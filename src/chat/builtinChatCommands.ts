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
import { registerChatCommandPipeline, type ChatCommandRunInput } from './chatCommandRegistry';

export const HAIKU_CARD_AGENT = { userId: 'agent:haiku', displayName: 'Haiku' } as const;

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

let registered = false;

/** Register the four built-ins. Idempotent; called once from the chat-commands route module. */
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
}
