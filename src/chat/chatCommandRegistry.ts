// Chat slash-commands that are PIPELINE RUNS — as a registry, not a route each.
//
// `/doc`, `/summarize` and `/diagram` each started life as a bespoke Express
// route that built a definition inline, triggered it through the bridge,
// registered the trigger with run persistence and answered 202. A project
// embedding this platform that wants `/search` or `/triage` backed by a run
// should not have to add a fourth copy of that. It registers a
// `ChatCommandPipeline` here — what the command needs, how its definition
// and trigger payload are built from the command — and gets:
//
//   GET  /api/chat/commands                 the commands a client can offer
//   POST /api/chat/commands/:trigger/run    one code path from command to run
//
// plus the card the conversation shows for the run, and (see runResult.ts)
// one convention for what the card opens when the run is done.
//
// Result convention — a run whose FINAL CONTEXT contains
//
//   result: { kind: 'panel',    panel: { kind, id, title?, params? } }
//         | { kind: 'document', documentId }
//         | { kind: 'link',     href, label }
//
// is what the chat card offers to open. A step returns it as part of its
// output (`{ status: 'completed', output: { result: {...} } }`; outputs merge
// into the context) and `GET /api/pipelines/:pipelineId/runs/:runId` lifts
// it to a top-level `result` on the run snapshot.
import { randomUUID } from 'crypto';
import { AppError } from '../middleware/error-handler';
import { getPipelineBridge } from '../pipeline/bridge-types';
import { getRunPersistenceService } from '../pipeline/runPersistence';
import { workGraphPlanFromDefinition } from '../pipeline/workGraphPlan';
import { workSourceAccountRepo } from '../repositories';
import { getSystemPipeline, registerSystemPipeline } from '../pipeline/systemPipelines';
import { postRunCardToChannel } from '../documents/chatRunCard';
import log from '../lib/logger';

export type { ChatCommandResult, ChatCommandPanelResult, ChatCommandDocumentResult, ChatCommandLinkResult } from '../pipeline/runResult';
export { parseRunResult, runResultFromContext } from '../pipeline/runResult';

export interface ChatCommandRunInput {
  /** The slash command as typed, with or without the slash (`/doc`, `doc`). */
  trigger: string;
  /** The conversation the command was typed in. */
  channel: string;
  /** The whole message, `/doc add a title`. */
  text: string;
  /** The message after the command, `add a title`. */
  argumentText: string;
  /** A picked argument, when the client offered a list (a document type, a person). */
  argument?: { id: string; label: string };
  /** The document the command concerns — the one beside the chat, or a fresh page the client created for the run. */
  documentId?: string;
  requestedBy: string;
  requestedByName: string;
  /**
   * The requester's verified organization claim. Without it the run cannot be
   * projected into their work graph at all — the source-account join that
   * makes a run person-owned is keyed on it — so a command dispatched with no
   * organization stays a run and is simply never shown on the graph.
   */
  organizationId?: string;
  /** Anything else the command's payload needs; validated by `requires.extra`. */
  extra?: Record<string, unknown>;
  /**
   * True when the client that sent the command posts the run's card itself
   * (the app does: it posts the returned `card`, which updates live and opens
   * the `result`). Merged into every trigger payload as `clientPostsCard`, so
   * a pipeline's own publish step (`postRunCardToChannel`) leaves the card to
   * the client instead of posting a second one. Runs started with no client
   * omit it and keep posting.
   */
  clientPostsCard?: boolean;
}

export interface ChatCommandRequires {
  /** `documentId` must be set. */
  document?: boolean;
  /** `argument` must be set with a non-empty `label` (a picked `{ id, label }`, or a typed name with an empty id). */
  argument?: boolean;
  /** `argumentText` must be non-empty (the free text after the command). */
  argumentText?: boolean;
  /** These `extra` keys must be present and non-empty. */
  extra?: string[];
}

export interface ChatCommandPipeline {
  /** '/doc', '/summarize', '/search' … Stored lowercase with the slash; matched either way. */
  trigger: string;
  /** Must be (or becomes, on first run) a registered system pipeline. */
  pipelineId: string;
  /** The long form: what the run does, for the menu's footer and the card. */
  description: string;
  /** What the menu row says — "Find people". Without one a client falls back to the description or the trigger. */
  label?: string;
  /** The row's second clause — "by what they know". */
  hint?: string;
  /**
   * Resolve anything asynchronous the command needs before the definition and
   * payload are built (a type looked up by name, a person by handle). Returns
   * the input the builders see; throw an `AppError` to refuse the command.
   */
  prepare?: (input: ChatCommandRunInput) => Promise<ChatCommandRunInput> | ChatCommandRunInput;
  /**
   * Answer the command WITHOUT starting its run yet, returning the id the
   * client should poll. For a command whose definition cannot be built
   * synchronously — `/agent` has to call a planner that takes tens of
   * seconds, and a composer cannot hold an HTTP request open for that.
   *
   * Returning `null` DECLINES, and the ordinary build-and-trigger path below
   * runs as usual. That is how `/agent` re-enters: its background planner
   * calls back through here with the finished plan in `extra`, the hook
   * declines the second time, and the real run is triggered by exactly the
   * same code (organization join, work-graph projection, persistence) as
   * every other command — rather than a second copy of it that drifts.
   */
  start?: (input: ChatCommandRunInput) => Promise<{ runId: string } | null>;
  /** The definition to run. May vary per input (an approver, a model); the FIRST build also registers the system pipeline. */
  buildDefinition: (input: ChatCommandRunInput) => Record<string, unknown>;
  triggerPayload: (input: ChatCommandRunInput) => Record<string, unknown>;
  /** Metadata for the chat card, merged with `{ kind: 'agent-run', command, instruction, runId, pipelineId }`. */
  card?: (input: ChatCommandRunInput) => Record<string, unknown>;
  /**
   * Post the card into the command's channel when the run starts. Default
   * true — an embedder's command gets its card for free. False means the
   * client posts it (it gets the metadata back), or the pipeline's own last
   * step does.
   */
  postCard?: boolean;
  requires?: ChatCommandRequires;
}

export type ChatCommandListing = Pick<ChatCommandPipeline, 'trigger' | 'pipelineId' | 'description' | 'label' | 'hint' | 'requires'>;

export interface ChatCommandRunOutput {
  runId: string;
  pipelineId: string;
  /** The card's metadata, ready to post or render: `{ kind: 'agent-run', command, instruction, runId, pipelineId, ...card }`. */
  card: Record<string, unknown>;
}

/** A command whose `requires` were not met: 400 with the requirements named. */
export class ChatCommandRequiresError extends AppError {
  constructor(message: string, public readonly requires: ChatCommandRequires) {
    super(400, message);
  }
}

/** A trigger nobody registered: 404 with the known ones. */
export class UnknownChatCommandError extends AppError {
  constructor(trigger: string, public readonly known: string[]) {
    super(404, `unknown chat command ${trigger}`);
  }
}

/** `/Doc` → `/doc`; `doc` → `/doc`. */
export function normalizeTrigger(trigger: string): string {
  const t = String(trigger ?? '').trim().toLowerCase();
  if (!t) return '';
  return t.startsWith('/') ? t : `/${t}`;
}

const REGISTRY = new Map<string, ChatCommandPipeline>();

/** Idempotent by trigger; a later registration replaces an earlier one. */
export function registerChatCommandPipeline(def: ChatCommandPipeline): void {
  const trigger = normalizeTrigger(def.trigger);
  if (!trigger || trigger === '/') throw new Error('a chat command needs a trigger');
  if (!def.pipelineId) throw new Error(`chat command ${trigger} needs a pipelineId`);
  REGISTRY.set(trigger, { ...def, trigger });
}

export function getChatCommandPipeline(trigger: string): ChatCommandPipeline | undefined {
  return REGISTRY.get(normalizeTrigger(trigger));
}

export function listChatCommandPipelines(): ChatCommandListing[] {
  return [...REGISTRY.values()].map(({ trigger, pipelineId, description, label, hint, requires }) => ({
    trigger, pipelineId, description,
    ...(label ? { label } : {}),
    ...(hint ? { hint } : {}),
    ...(requires ? { requires } : {}),
  }));
}

export function knownChatCommandTriggers(): string[] {
  return [...REGISTRY.keys()];
}

/** Test seam. */
export function clearChatCommandPipelines(): void {
  REGISTRY.clear();
}

const nonEmpty = (v: unknown): boolean => v !== undefined && v !== null && !(typeof v === 'string' && v.trim() === '');

/** Throws `ChatCommandRequiresError` naming what is missing. */
export function assertChatCommandRequires(def: ChatCommandPipeline, input: ChatCommandRunInput): void {
  const req = def.requires;
  if (!req) return;
  const missing: string[] = [];
  if (req.document && !nonEmpty(input.documentId)) missing.push('documentId');
  // A picked argument has an id; one typed as text may only have its label.
  if (req.argument && !(input.argument && nonEmpty(input.argument.label))) missing.push('argument');
  if (req.argumentText && !nonEmpty(input.argumentText)) missing.push('argumentText');
  for (const key of req.extra ?? []) if (!nonEmpty(input.extra?.[key])) missing.push(`extra.${key}`);
  if (missing.length) throw new ChatCommandRequiresError(`${def.trigger} needs ${missing.join(', ')}`, req);
}

/** What the card says after the command: the payload's `instruction` when it has one, else the text after the command. */
export function instructionFor(payload: Record<string, unknown>, input: ChatCommandRunInput): string {
  if (typeof payload.instruction === 'string' && payload.instruction.trim()) return payload.instruction.trim();
  const arg = input.argumentText?.trim();
  if (arg) return arg;
  return input.argument?.label ?? input.text.replace(/^\S+\s*/, '').trim();
}

/**
 * Command → run. Validates `requires`, runs `prepare`, makes sure the
 * pipeline is a registered system pipeline (so it lists and its steps have
 * names), triggers through the bridge exactly as the bespoke routes did,
 * registers the trigger with run persistence, posts the card when asked, and
 * hands back the card so the client can render it before the first event.
 */
export async function runChatCommand(rawInput: ChatCommandRunInput): Promise<ChatCommandRunOutput> {
  const trigger = normalizeTrigger(rawInput.trigger);
  const def = REGISTRY.get(trigger);
  if (!def) throw new UnknownChatCommandError(trigger || rawInput.trigger, knownChatCommandTriggers());
  const baseInput: ChatCommandRunInput = { ...rawInput, trigger, argumentText: rawInput.argumentText ?? '' };
  assertChatCommandRequires(def, baseInput);
  const input = def.prepare ? await def.prepare(baseInput) : baseInput;

  // A command that answers before its run exists does so here. `null` means
  // "not this time" and falls through to the path below unchanged.
  const started = def.start ? await def.start(input) : null;
  if (started) {
    const pendingPayload = { ...def.triggerPayload(input) };
    const pendingInstruction = instructionFor(pendingPayload, input);
    const pendingCard: Record<string, unknown> = {
      kind: 'agent-run', command: trigger, instruction: pendingInstruction,
      runId: started.runId, pipelineId: def.pipelineId,
      ...(input.documentId ? { documentId: input.documentId } : {}),
      ...(def.card ? def.card(input) : {}),
    };
    if (def.postCard !== false && input.clientPostsCard !== true) {
      const { kind: _k, command: _c, instruction: _i, runId: _r, pipelineId: _p, ...extra } = pendingCard;
      await postRunCardToChannel({
        channel: input.channel, runId: started.runId, pipelineId: def.pipelineId,
        command: trigger, instruction: pendingInstruction, extra,
      });
    }
    log.info({ runId: started.runId, pipelineId: def.pipelineId, trigger, channel: input.channel, userId: input.requestedBy }, 'chat command accepted; the command starts its own run');
    return { runId: started.runId, pipelineId: def.pipelineId, card: pendingCard };
  }

  const bridge = getPipelineBridge();
  if (!bridge?.trigger) throw new AppError(503, 'pipeline engine is not available');

  // Same create-only join the authenticated trigger route establishes, and for
  // the same reason: the projection consumer re-checks it before it will
  // accept a single lifecycle event for this person. A chat command that
  // skipped it produced runs whose events were dropped at ingest with no
  // error anywhere — the run happened and the graph never heard of it.
  if (input.organizationId) {
    await workSourceAccountRepo.ensureVerifiedPrincipal({
      source: 'pipeline', organizationId: input.organizationId, subject: input.requestedBy,
    });
  } else {
    log.warn({ trigger, userId: input.requestedBy }, 'chat command run has no organization; it will not reach the work graph');
  }

  const definition = def.buildDefinition(input);
  if (!getSystemPipeline(def.pipelineId)) {
    // The first run's build stands in for the pipeline on the definitions
    // pages; a build that varies per input (an approver) still names the same
    // steps, which is what the pages need.
    registerSystemPipeline({ id: def.pipelineId, build: () => def.buildDefinition(input) });
  }

  const payload = { ...def.triggerPayload(input) };
  if (payload.requestId === undefined) payload.requestId = randomUUID();
  if (payload.requestedBy === undefined) payload.requestedBy = input.requestedBy;
  if (payload.requestedByName === undefined) payload.requestedByName = input.requestedByName;
  if (payload.chatChannel === undefined) payload.chatChannel = input.channel;
  payload.clientPostsCard = input.clientPostsCard === true;

  const out = await Promise.resolve(bridge.trigger({
    pipelineId: def.pipelineId,
    definition: definition as never,
    triggerPayload: payload,
    triggeredBy: { userId: input.requestedBy },
  } as never));
  const runId = (out as { runId: string }).runId;
  // The graph shows this run as the steps the definition actually declares,
  // hanging off the conversation the command was typed in. A planner that fell
  // back to a single step therefore yields a single node — honestly.
  const workGraph = workGraphPlanFromDefinition(definition, input.channel);
  getRunPersistenceService()?.registerTrigger({
    runId,
    pipelineId: def.pipelineId,
    userId: input.requestedBy,
    triggerType: 'manual',
    at: new Date().toISOString(),
    ...(input.organizationId ? { organizationId: input.organizationId } : {}),
    ...(workGraph ? { workGraph } : {}),
  });

  const instruction = instructionFor(payload, input);
  const card: Record<string, unknown> = {
    kind: 'agent-run', command: trigger, instruction, runId, pipelineId: def.pipelineId,
    ...(input.documentId ? { documentId: input.documentId } : {}),
    ...(def.card ? def.card(input) : {}),
  };
  // The client that posts the card itself gets the metadata and no duplicate from here.
  if (def.postCard !== false && input.clientPostsCard !== true) {
    const { kind: _k, command: _c, instruction: _i, runId: _r, pipelineId: _p, ...extra } = card;
    await postRunCardToChannel({ channel: input.channel, runId, pipelineId: def.pipelineId, command: trigger, instruction, extra });
  }
  log.info({ runId, pipelineId: def.pipelineId, trigger, channel: input.channel, userId: input.requestedBy }, 'chat command run triggered');
  return { runId, pipelineId: def.pipelineId, card };
}
