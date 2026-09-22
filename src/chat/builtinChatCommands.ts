// The platform's own chat commands, registered the way an embedder's would be.
//
// Each one used to be a route of its own in routes/documentAgentEdit.ts
// (those routes still answer, for the clients that call them); here they are
// the same definitions and payloads behind the registry, so `/api/chat/commands`
// lists them and `/api/chat/commands/:trigger/run` starts them.
//
// None of the four posts its card from here: the app's chat client posts the
// "running" card with the metadata the run returns, and /summarize and
// /diagram post their finished card from their own last step.
import { AppError, NotFoundError } from '../middleware/error-handler';
import { documentTypeRepo } from '../repositories';
import type { DocumentTypeItem } from '../repositories';
import { buildDocumentAgentEditDefinition, buildDocumentTransformDefinition, DOCUMENT_AGENT_EDIT_PIPELINE_ID, DOCUMENT_TRANSFORM_PIPELINE_ID } from '../documents/documentAgentPipeline';
import { buildConversationSummarizeDefinition, CONVERSATION_SUMMARIZE_PIPELINE_ID, DEFAULT_HISTORY_LIMIT } from '../documents/conversationSummarizePipeline';
import { buildDiagramGenerateDefinition, DIAGRAM_GENERATE_PIPELINE_ID } from '../documents/diagramGeneratePipeline';
import { buildDeckGenerateDefinition, DECK_GENERATE_PIPELINE_ID, DECK_HISTORY_LIMIT } from '../documents/deckGeneratePipeline';
import { buildDeckFixture } from '../deck/deckFixture';
import { renderDeckPreview } from '../deck/preview';
import { randomUUID } from 'node:crypto';
import { registerChatCommandPipeline, type ChatCommandRunInput } from './chatCommandRegistry';
import {
  AGENT_LOOP_CARD_AGENT, CHAT_AGENT_LOOP_PIPELINE_ID, agentLoopCardLab,
  buildAgentLoopDefinition, type AgentLoopRequest, type LoopPlan,
} from './agentLoopPlanner';
import { PENDING_PLAN, requestFromInput, startAgentLoop } from './agentLoopRunner';
import { ClaudeCodeAgentClient } from '../pipeline/agent/ClaudeCodeAgentClient';
import type { AgentClient } from '../pipeline/agent/types';

export const HAIKU_CARD_AGENT = { userId: 'agent:haiku', displayName: 'Haiku' } as const;

/**
 * The client the /agent planner asks. Lazily the CLI-backed one, so nothing
 * is spawned in a process that never runs the command; settable so a test
 * can hand it a scripted planner instead of a real model.
 */
let plannerAgentClient: AgentClient | null | undefined;

export function setAgentLoopPlannerClient(client: AgentClient | null): void {
  plannerAgentClient = client;
}

export function getAgentLoopPlannerClient(): AgentClient | null {
  if (plannerAgentClient === undefined) plannerAgentClient = new ClaudeCodeAgentClient();
  return plannerAgentClient;
}

/** The parsed command and its plan, carried from `prepare` to the builders. */
export interface PreparedAgentLoop { request: AgentLoopRequest; plan: LoopPlan }

/** `prepare` stashed it under `extra.agentLoop`; every builder needs it. */
export function preparedAgentLoop(input: ChatCommandRunInput): PreparedAgentLoop {
  const prepared = input.extra?.agentLoop as PreparedAgentLoop | undefined;
  if (!prepared?.plan || !prepared.request) throw new AppError(500, '/agent was built without a plan');
  return prepared;
}

const str = (v: unknown): string | undefined => (typeof v === 'string' && v.trim() ? v.trim() : undefined);

/** The target of a transform as the pipeline's payload wants it. */
export interface TransformTarget { id: string; name: string; icon: string; fields: Array<{ name: string; sectionType?: string }> }

function targetFromType(t: DocumentTypeItem): TransformTarget {
  return {
    id: t.typeId, name: t.name, icon: t.icon ?? '',
    fields: (t.fields ?? []).slice(0, 40).map((f) => {
      const sectionType = (f as unknown as { sectionType?: unknown }).sectionType;
      return { name: f.name, ...(typeof sectionType === 'string' ? { sectionType } : {}) };
    }),
  };
}

function targetFromExtra(raw: unknown): TransformTarget | undefined {
  if (!raw || typeof raw !== 'object') return undefined;
  const t = raw as { id?: unknown; name?: unknown; icon?: unknown; fields?: unknown };
  const name = str(t.name);
  if (!name) return undefined;
  const fields = (Array.isArray(t.fields) ? t.fields : [])
    .map((f) => (f && typeof f === 'object' ? f as { name?: unknown; sectionType?: unknown } : null))
    .filter((f): f is { name?: unknown; sectionType?: unknown } => !!f && typeof f.name === 'string' && !!f.name)
    .slice(0, 40)
    .map((f) => ({ name: String(f.name), ...(typeof f.sectionType === 'string' ? { sectionType: f.sectionType } : {}) }));
  return { id: str(t.id) ?? '', name, icon: str(t.icon) ?? '', fields };
}

/**
 * The type a `/transform <type>` names: `extra.target` when the client already
 * has it, else the picked argument's id, else its label matched against the
 * type names (case-insensitive) through the same repository the
 * /api/document-types routes read.
 */
export async function resolveTransformTarget(input: ChatCommandRunInput, repo: Pick<typeof documentTypeRepo, 'get' | 'list'> = documentTypeRepo): Promise<TransformTarget> {
  const given = targetFromExtra(input.extra?.target);
  if (given) return given;
  const arg = input.argument;
  if (arg?.id) {
    const byId = await repo.get(arg.id);
    if (byId) return targetFromType(byId);
  }
  const label = (arg?.label ?? input.argumentText ?? '').trim().toLowerCase();
  if (!label) throw new AppError(400, '/transform needs a target type');
  const types = await repo.list(200);
  const match = types.find((t) => t.name.trim().toLowerCase() === label) ?? types.find((t) => t.typeId === label);
  if (!match) throw new NotFoundError(`no document type named "${arg?.label ?? input.argumentText}"`);
  return targetFromType(match);
}

/** `targetFieldList` as the transform prompt interpolates it. */
export function transformFieldList(fields: TransformTarget['fields']): string {
  return fields.length ? fields.map((f) => `- ${f.name}${f.sectionType ? ` (${f.sectionType})` : ''}`).join('\n') : '- (the type declares no fields; keep the source headings)';
}

/** `/deck` flags: `--preview` and `--from transcript|documents`. */
export interface DeckCommandArgs {
  /** The instruction with the flags removed. */
  prompt: string;
  /** Answer with a preview card and persist nothing. */
  preview: boolean;
  /** What the deck is built from, when the person said. */
  from?: 'transcript' | 'documents';
}

/**
 * Parses `/deck`'s own grammar. `--from #anything` and a bare `#doc` both mean
 * the referenced documents; `--from transcript` means the meeting's transcript.
 * Everything else stays in the instruction — the deck is about the words, and
 * an unrecognised flag must not silently vanish from what was asked for.
 */
export function parseDeckCommand(argumentText: string): DeckCommandArgs {
  let preview = false;
  let from: DeckCommandArgs['from'];
  const words = argumentText.trim().split(/\s+/).filter(Boolean);
  const kept: string[] = [];
  for (let index = 0; index < words.length; index += 1) {
    const word = words[index]!;
    const lowered = word.toLowerCase();
    if (lowered === '--preview') { preview = true; continue; }
    if (lowered === '--from' || lowered.startsWith('--from=')) {
      const value = (lowered.startsWith('--from=') ? lowered.slice('--from='.length) : (words[index + 1] ?? '')).toLowerCase();
      if (!lowered.startsWith('--from=')) index += 1;
      if (value.startsWith('transcript') || value.startsWith('recording') || value.startsWith('meeting')) from = 'transcript';
      else if (value) from = 'documents';
      continue;
    }
    kept.push(word);
  }
  return { prompt: kept.join(' ').trim(), preview, ...(from ? { from } : {}) };
}

/** The recording a `--from transcript` deck reads, when the client named one. */
function deckRecordingId(input: ChatCommandRunInput): string | undefined {
  const raw = input.extra?.recordingId ?? input.extra?.transcriptRecordingId;
  return typeof raw === 'string' && raw.trim() ? raw.trim() : undefined;
}

/**
 * The documents a `/deck` must cite: what the composer referenced with `#`
 * (`extra.documents`, the shape `ComposedMessage.documents` already has), minus
 * the new presentation itself. Nothing is inferred — a deck cites what the
 * person picked.
 */
export function deckSourcesFromInput(input: ChatCommandRunInput): Array<{ documentId: string; title: string }> {
  const raw = input.extra?.documents;
  if (!Array.isArray(raw)) return [];
  const out: Array<{ documentId: string; title: string }> = [];
  const seen = new Set<string>();
  for (const entry of raw) {
    if (!entry || typeof entry !== 'object') continue;
    const item = entry as { id?: unknown; documentId?: unknown; title?: unknown; name?: unknown };
    const documentId = str(item.documentId) ?? str(item.id);
    if (!documentId || documentId === input.documentId || seen.has(documentId)) continue;
    seen.add(documentId);
    out.push({ documentId, title: (str(item.title) ?? str(item.name) ?? documentId).slice(0, 160) });
    if (out.length >= 12) break;
  }
  return out;
}

/**
 * `/deck --preview`: the deck as it would be, before anything is written.
 *
 * It derives a spec from the named material and renders slide 1, and the card
 * carries that SVG itself — there is no preview handle, because a handle
 * addresses a stored revision and this stored nothing. The id it answers with
 * is `deck-preview:<uuid>`, which is not a run id and says so.
 */
export async function buildDeckPreviewCard(input: ChatCommandRunInput): Promise<Record<string, unknown>> {
  const args = parseDeckCommand(input.argumentText ?? '');
  const sources = deckSourcesFromInput(input);
  const recordingId = deckRecordingId(input);
  const documentId = args.from === 'transcript' ? undefined : sources[0]?.documentId;
  if (!documentId && !recordingId) {
    throw new AppError(400, '/deck --preview needs a document (reference one with #) or a recording to build from');
  }
  const result = await buildDeckFixture(
    { ...(documentId ? { documentId } : {}), ...(recordingId ? { recordingId } : {}), personId: input.requestedBy },
    args.prompt || undefined,
    {},
  );
  const preview = renderDeckPreview(result.spec);
  const first = preview.slides[0]!;
  return {
    kind: 'deck-preview',
    persisted: false,
    title: result.spec.title,
    specSource: result.source,
    ...(result.reason ? { specSourceReason: result.reason } : {}),
    slideCount: preview.slides.length,
    // What the slides themselves invite, then what the deck left unused.
    warnings: [...preview.warnings, ...result.warnings],
    transcriptIds: result.transcriptIds,
    thumbnail: { slideId: first.id, title: first.title, contentType: 'image/svg+xml', svg: first.svg },
    sources: sources.length ? sources : undefined,
    spec: result.spec,
    agent: HAIKU_CARD_AGENT,
  };
}

/**
 * The preview a `--preview` run rendered, handed from `start` to `card` — the
 * registry calls them in that order for the same input, and this is the only
 * seam between them. Keyed by requester and taken out on read, so a preview is
 * never shown to a second person or a second time.
 */
const deckPreviewCards = new Map<string, Record<string, unknown>>();

let registered = false;

/** Register the built-ins. Idempotent; called once from the chat-commands route module. */
export function registerBuiltinChatCommands(): void {
  if (registered) return;
  registered = true;

  // /doc <instruction> — edit the document beside the chat as a collaborator.
  registerChatCommandPipeline({
    trigger: '/doc',
    pipelineId: DOCUMENT_AGENT_EDIT_PIPELINE_ID,
    label: 'Edit the open document',
    hint: 'with Haiku, as a collaborator',
    description: 'Edit the document beside the chat: a light model plans the change, a big one waits for your approval, the edit lands as a collaborator.',
    requires: { document: true, argumentText: true },
    postCard: false,
    // The requester approves a big edit — set on the definition, since approvers are not templated.
    buildDefinition: (input) => buildDocumentAgentEditDefinition(undefined, { approverUserId: input.requestedBy }),
    triggerPayload: (input) => ({
      documentId: input.documentId, instruction: input.argumentText.trim().slice(0, 2000),
      requestedBy: input.requestedBy, requestedByName: input.requestedByName, chatChannel: input.channel,
    }),
    card: () => ({ agent: HAIKU_CARD_AGENT }),
  });

  // /transform <type> — rebuild the source page under a known type into the fresh page the client created.
  registerChatCommandPipeline({
    trigger: '/transform',
    pipelineId: DOCUMENT_TRANSFORM_PIPELINE_ID,
    label: 'Transform the open document',
    hint: 'into another type',
    description: 'Transform the source page into another known type: `documentId` is the new empty page, `extra.sourceDocumentId` the page being transformed, the argument names the type.',
    requires: { document: true, argument: true, extra: ['sourceDocumentId'] },
    postCard: false,
    prepare: async (input) => ({ ...input, extra: { ...input.extra, target: await resolveTransformTarget(input) } }),
    buildDefinition: (input) => buildDocumentTransformDefinition(undefined, { approverUserId: input.requestedBy }),
    triggerPayload: (input) => {
      const target = input.extra!.target as TransformTarget;
      return {
        documentId: input.documentId, sourceDocumentId: String(input.extra!.sourceDocumentId),
        requestedBy: input.requestedBy, requestedByName: input.requestedByName,
        targetName: target.name, targetId: target.id, targetIcon: target.icon,
        targetFieldList: transformFieldList(target.fields),
        instruction: `Transform into ${target.name}`,
      };
    },
    card: (input) => {
      const target = input.extra!.target as TransformTarget;
      return { sourceDocumentId: input.extra!.sourceDocumentId, targetName: target.name, targetId: target.id, agent: HAIKU_CARD_AGENT };
    },
  });

  // /summarize — read the conversation into the fresh page the client created.
  registerChatCommandPipeline({
    trigger: '/summarize',
    pipelineId: CONVERSATION_SUMMARIZE_PIPELINE_ID,
    label: 'Summarise this conversation',
    hint: 'into a new page',
    description: 'Summarise this conversation into a page: `documentId` is the new empty page; `extra.limit` caps how many messages are read.',
    requires: { document: true },
    postCard: false,
    triggerPayload: (input) => {
      const raw = input.extra?.limit;
      const limit = typeof raw === 'number' && Number.isFinite(raw) && raw > 0 ? Math.min(Math.floor(raw), 1000) : DEFAULT_HISTORY_LIMIT;
      return {
        documentId: input.documentId, chatChannel: input.channel, limit,
        requestedBy: input.requestedBy, requestedByName: input.requestedByName,
        instruction: `Summarise ${input.channel}`,
      };
    },
    buildDefinition: () => buildConversationSummarizeDefinition(),
    card: () => ({ agent: HAIKU_CARD_AGENT }),
  });

  // /agent <instruction> [--model x] [<N>k] — plan the instruction into steps and
  // run each of them as an agent in one cloud workspace, continuously, on a budget.
  registerChatCommandPipeline({
    trigger: '/agent',
    pipelineId: CHAT_AGENT_LOOP_PIPELINE_ID,
    label: 'Start an agent loop',
    hint: 'in a cloud computer · budgeted',
    description: 'Plan an instruction into agent steps and run them in a cloud workspace until each meets its acceptance. Leading /words are step hints, a trailing `200k` sets the context budget, `--model` picks the model.',
    requires: { argumentText: true },
    // The card IS the anchor the conversation gets: it opens the lab.
    postCard: true,
    // FAST. The planner is a 20-90s CLI call and must never run inside the
    // POST — see agentLoopRunner.ts. `prepare` only parses the grammar; the
    // plan arrives on the second pass, in `extra.agentLoop`.
    prepare: async (input) => {
      const prepared = input.extra?.agentLoop as PreparedAgentLoop | undefined;
      if (prepared?.plan) return input;   // the background pass, plan in hand
      const request = requestFromInput(input);
      return { ...input, extra: { ...input.extra, agentLoop: { request, plan: PENDING_PLAN } satisfies PreparedAgentLoop } };
    },
    // First pass: take the run, answer with a loop id, plan in the background.
    // Second pass (the planner calling back with a plan): decline, and let the
    // ordinary build-and-trigger path below start the real run.
    start: async (input) => {
      const prepared = input.extra?.agentLoop as PreparedAgentLoop | undefined;
      if (prepared?.plan && prepared.plan.source !== 'pending') return null;
      const { runId } = await startAgentLoop(input);
      return { runId };
    },
    buildDefinition: (input) => {
      const { plan, request } = preparedAgentLoop(input);
      return buildAgentLoopDefinition(plan, request);
    },
    triggerPayload: (input) => {
      const { plan, request } = preparedAgentLoop(input);
      return {
        // The organization the command was typed in. A step that creates
        // something an organization owns (a room, a page) has no other way to
        // know it, and a run without one used to fail at that step.
        ...(input.organizationId ? { organizationId: input.organizationId } : {}),
        instruction: request.instruction,
        hints: request.hints,
        contextBudgetTokens: request.contextBudgetTokens,
        model: request.model,
        plan,
        chatChannel: request.chatChannel,
        requestedBy: request.requestedBy,
        requestedByName: request.requestedByName,
        // Interpolated by a deck step's drafting prompt. Empty string rather
        // than absent, so the prompt reads as "no transcript" instead of
        // leaving a dangling template.
        deckTranscript: request.transcript ?? '(no transcript for this conversation)',
        deckPriorWork: '(this is the first step)',
        ...(request.sources?.length ? { deckSources: request.sources } : {}),
      };
    },
    card: (input) => {
      const { plan, request } = preparedAgentLoop(input);
      return { agent: AGENT_LOOP_CARD_AGENT, lab: agentLoopCardLab(plan, request) };
    },
  });

  // /diagram <what> — sketch a whiteboard into the fresh diagram document the client created.
  registerChatCommandPipeline({
    trigger: '/diagram',
    pipelineId: DIAGRAM_GENERATE_PIPELINE_ID,
    label: 'Draw a diagram',
    hint: 'from a description',
    description: 'Draw a diagram from a description into a new diagram document: `documentId` is the new document, the text after the command is what to draw; `extra.title` names it.',
    requires: { document: true, argumentText: true },
    postCard: false,
    buildDefinition: () => buildDiagramGenerateDefinition(),
    triggerPayload: (input) => {
      const prompt = input.argumentText.trim().slice(0, 2000);
      const title = (str(input.extra?.title) ?? prompt).slice(0, 120);
      return {
        documentId: input.documentId, title, prompt,
        requestedBy: input.requestedBy, requestedByName: input.requestedByName, chatChannel: input.channel,
        instruction: title,
      };
    },
    card: (input) => ({ title: (str(input.extra?.title) ?? input.argumentText.trim()).slice(0, 120), agent: HAIKU_CARD_AGENT }),
  });

  // /deck <what> [#doc …] [--from transcript|documents] [--preview] — build a
  // real presentation, grounded in the documents the composer referenced and
  // the meeting or conversation, into the fresh presentation document the
  // client created. `--preview` answers with the deck as it would be and writes
  // nothing.
  registerChatCommandPipeline({
    trigger: '/deck',
    pipelineId: DECK_GENERATE_PIPELINE_ID,
    label: 'Build a presentation',
    hint: 'from the referenced documents',
    description: 'Build a .pptx from the documents you reference with # and this conversation: `documentId` is the new presentation, `extra.documents` are the sources it must cite, the text after the command is what the deck is about. `--from transcript` reads the meeting recording in `extra.recordingId`; `--preview` answers with the slides and stores nothing.',
    requires: { document: true, argumentText: true },
    postCard: false,
    // `--preview` never reaches the pipeline: it derives a spec, renders slide 1
    // and answers with it. The id is `deck-preview:<uuid>` — not a run id, and
    // the card says `persisted: false` so nothing downstream mistakes it.
    start: async (input) => {
      if (!parseDeckCommand(input.argumentText ?? '').preview) return null;
      const card = await buildDeckPreviewCard(input);
      deckPreviewCards.set(input.requestedBy, card);
      return { runId: `deck-preview:${randomUUID()}` };
    },
    buildDefinition: () => buildDeckGenerateDefinition(),
    triggerPayload: (input) => {
      const args = parseDeckCommand(input.argumentText ?? '');
      const prompt = (args.prompt || input.argumentText.trim()).slice(0, 2000);
      const title = (str(input.extra?.title) ?? prompt).slice(0, 120);
      const recordingId = deckRecordingId(input);
      return {
        documentId: input.documentId, title, prompt,
        sources: deckSourcesFromInput(input),
        ...(recordingId ? { recordingId } : {}),
        ...(args.from ? { from: args.from } : {}),
        limit: DECK_HISTORY_LIMIT,
        chatChannel: input.channel,
        requestedBy: input.requestedBy, requestedByName: input.requestedByName,
        instruction: title,
      };
    },
    // The card names what the deck stands on, so the thread can see the sources
    // before the deck exists — and carries the rendered preview when the
    // command asked for one.
    card: (input) => {
      const args = parseDeckCommand(input.argumentText ?? '');
      const sources = deckSourcesFromInput(input);
      const preview = args.preview ? deckPreviewCards.get(input.requestedBy) : undefined;
      if (preview) deckPreviewCards.delete(input.requestedBy);
      const recordingId = deckRecordingId(input);
      return {
        title: ((str(input.extra?.title) ?? args.prompt) || input.argumentText.trim()).slice(0, 120),
        sources,
        sourceNames: sources.map((source) => source.title),
        ...(args.from ? { from: args.from } : {}),
        ...(recordingId ? { recordingId } : {}),
        agent: HAIKU_CARD_AGENT,
        ...(preview ?? {}),
      };
    },
  });
}
