// `document:create` — the step behind a planned `kind: 'document'`.
//
// Before this, a document step was an ordinary devbox agent told to "create a
// page". The agent has no way to create one — its tools are a shell in a
// scratch workspace — so it listed an empty directory, said DONE, and the run
// card said Done over a page that never existed. This is the write: a real
// page in the Documents explorer, created FOR the conversation the /agent
// command came from.
//
// What makes it the conversation's page, not only the requester's:
//   channel    the run's `chatChannel`, so the conversation's Documents tab
//              lists it (`d.channel === activeChannel`);
//   createdBy  the requester (and `createdByName`) — the person who asked,
//              never the agent;
//   editors    every member of that conversation (conversationMembers.ts):
//              both people in a DM, a room's members at creation time — so
//              `document:read` / `document:edit` admit all of them.
// And the run's card in the conversation becomes the document card
// (chatRunCard.ts `announceRunDocument`): one message, not two.
//
// Same write order as agenda:write — the row first, the body second, because
// they fail separately and a real, empty page beats a pretend full one.

import { randomUUID } from 'node:crypto';
import { PutCommand } from '@aws-sdk/lib-dynamodb';
import type { PipelineNode, StepHandlerHelpers, StepOutcome } from 'distributed-core/applications/pipeline';
import type { DescribedStepHandler } from '../pipeline/customStepHandlers';
import { docClient } from '../lib/aws-clients';
import log from '../lib/logger';
import { readPath } from '../pipeline/image/ScatterStepHandler';
import { AgentCollaborator, HAIKU_AGENT } from '../documents/AgentCollaborator';
import { applyOps, ensureCanvasMeta, BLOCK_KINDS, type BlockKind, type DocOp } from '../documents/documentAgentOps';
import { DOCUMENTS_TABLE } from '../deck/presentationDocuments';
import { conversationMembers } from '../chat/conversationMembers';
import { announceRunDocument, type RunDocumentCardInput, type RunDocumentCardOutcome } from '../documents/chatRunCard';
import { putRunDocumentInConversation, namesLine } from './conversationDocument';

export { namesLine };

export const DOCUMENT_CREATE_STEP_KIND = 'document:create';
export const CREATED_DOCUMENT_TYPE = 'page';
export const CREATED_DOCUMENT_ICON = '📄';
export const MAX_PAGE_BLOCKS = 40;
export const MAX_BLOCK_CHARS = 2000;

export interface PageBlock { kind: BlockKind; text: string; level?: number }
export interface PageSpec { title: string; blocks: PageBlock[] }

export interface DocumentCreateNodeData {
  type: typeof DOCUMENT_CREATE_STEP_KIND;
  /** Where the drafted page is: a context path (the draft step's text). */
  pageFrom?: string;
  /** The page inline — the fixture path, no model in the loop. */
  page?: PageSpec | string;
  /** The title the plan read off the step; the draft's own title wins. */
  title?: string;
  label?: string;
}

function str(value: unknown, limit = 200): string | undefined {
  if (typeof value !== 'string') return undefined;
  const trimmed = value.replace(/\s+/g, ' ').trim();
  return trimmed ? trimmed.slice(0, limit) : undefined;
}

/** The first JSON object in what the drafting agent said, or undefined. */
export function parsePageJson(text: string): unknown {
  const fenced = /```(?:json)?\s*([\s\S]*?)```/i.exec(text);
  for (const candidate of [fenced?.[1], text].filter((value): value is string => typeof value === 'string')) {
    const start = candidate.indexOf('{');
    const end = candidate.lastIndexOf('}');
    if (start < 0 || end <= start) continue;
    try { return JSON.parse(candidate.slice(start, end + 1)); } catch { continue; }
  }
  return undefined;
}

/**
 * A drafted page, validated. Unlike an agenda, an empty body is a valid page —
 * "a blank page titled X" is a thing people ask for. Unknown block kinds are
 * dropped, not guessed.
 */
export function validatePage(input: unknown, fallbackTitle: string): PageSpec | null {
  if (!input || typeof input !== 'object' || Array.isArray(input)) return null;
  const raw = input as { title?: unknown; blocks?: unknown };
  const title = str(raw.title, 120) ?? fallbackTitle;
  const blocks: PageBlock[] = [];
  for (const entry of Array.isArray(raw.blocks) ? raw.blocks.slice(0, MAX_PAGE_BLOCKS) : []) {
    if (!entry || typeof entry !== 'object') continue;
    const e = entry as { kind?: unknown; text?: unknown; level?: unknown };
    const kind = typeof e.kind === 'string' && (BLOCK_KINDS as readonly string[]).includes(e.kind) && e.kind !== 'table' && e.kind !== 'horizontalRule'
      ? e.kind as BlockKind
      : undefined;
    const text = typeof e.text === 'string' ? e.text.trim().slice(0, MAX_BLOCK_CHARS) : '';
    if (!kind || !text) continue;
    const level = kind === 'heading' && typeof e.level === 'number' && e.level >= 1 && e.level <= 3 ? Math.round(e.level) : undefined;
    blocks.push({ kind, text, ...(level ? { level } : {}) });
  }
  return { title, blocks };
}

/** The page as document operations: the title, then each block in order. */
export function pageOps(page: PageSpec): DocOp[] {
  return [
    { op: 'setTitle', text: page.title },
    ...page.blocks.map((b): DocOp => ({ op: 'appendBlock', kind: b.kind, text: b.text, ...(b.level ? { level: b.level } : {}) })),
  ];
}

export interface DocumentCreateStepHandlerOpts {
  documents?: { send: (command: unknown) => Promise<unknown> };
  writeBody?: (documentId: string, title: string, ops: DocOp[], requestedBy: string) => Promise<number>;
  members?: (channel: string, requester: string) => Promise<string[]>;
  names?: (userIds: string[]) => Promise<Map<string, string>>;
  announce?: (input: RunDocumentCardInput) => Promise<RunDocumentCardOutcome>;
  newDocumentId?: () => string;
  now?: () => Date;
}

/** The real write: join the document as the agent, apply the ops, leave. */
export async function writeThroughGateway(documentId: string, title: string, ops: DocOp[], requestedBy: string): Promise<number> {
  const agent = new AgentCollaborator({ documentId, identity: HAIKU_AGENT });
  try {
    await agent.join();
    agent.announce();
    ensureCanvasMeta(agent.doc, { id: documentId, title, createdBy: requestedBy });
    const result = applyOps(agent.doc, ops, 'agent');
    void agent.leave(Number(process.env.DOCUMENT_AGENT_PRESENCE_HOLD_MS) || 2000).catch(() => undefined);
    return result.applied;
  } catch (err) {
    await agent.leave(0).catch(() => undefined);
    throw err;
  }
}

export interface ConversationPageRow {
  documentId: string;
  title: string;
  requestedBy: string;
  requestedByName?: string;
  channel?: string;
  editors: string[];
  /** ISO time for createdAt / updatedAt. */
  at: string;
  /** The run that made it (`createdByRunId`). */
  runId: string;
}

/**
 * The page's row: owned by the requester, on the conversation, every member an
 * editor. Conditional on the id being new — a second write of the same id
 * (a repair re-run) throws ConditionalCheckFailedException instead of
 * clobbering the page.
 */
export async function putConversationPageRow(documents: { send: (command: unknown) => Promise<unknown> }, row: ConversationPageRow): Promise<void> {
  await documents.send(new PutCommand({
    TableName: DOCUMENTS_TABLE,
    Item: {
      documentId: row.documentId,
      title: row.title,
      type: CREATED_DOCUMENT_TYPE,
      icon: CREATED_DOCUMENT_ICON,
      status: 'draft',
      createdBy: row.requestedBy,
      ...(row.requestedByName ? { createdByName: row.requestedByName } : {}),
      ...(row.channel ? { channel: row.channel } : {}),
      editors: row.editors,
      createdAt: row.at,
      updatedAt: row.at,
      createdByRunId: row.runId,
    },
    ConditionExpression: 'attribute_not_exists(documentId)',
  }));
}

export function createDocumentCreateStepHandler(opts: DocumentCreateStepHandlerOpts = {}): DescribedStepHandler {
  const documents = opts.documents ?? (docClient as unknown as { send: (command: unknown) => Promise<unknown> });
  const writeBody = opts.writeBody ?? writeThroughGateway;
  const members = opts.members ?? ((channel: string, requester: string) => conversationMembers(channel, requester));
  const announce = opts.announce ?? ((input: RunDocumentCardInput) => announceRunDocument(input));
  const newDocumentId = opts.newDocumentId ?? (() => randomUUID());
  const now = opts.now ?? (() => new Date());

  return {
    kind: DOCUMENT_CREATE_STEP_KIND,
    description:
      'Creates the page a document step asked for, in the conversation the loop came from: shared with every member as an editor, and announced by turning the run\'s card into the document card.',
    reads: 'The drafted page JSON at the node\'s pageFrom (or inline), the requester and the run\'s chatChannel.',
    emits: 'documentId, title, channel, editors, the block count, whether the body landed, and what happened to the card.',
    async execute(node: PipelineNode, context: Record<string, unknown>, helpers: StepHandlerHelpers): Promise<StepOutcome> {
      const data = (node.data ?? {}) as unknown as DocumentCreateNodeData;
      const fallbackTitle = str(data.title, 120) ?? str(data.label, 120) ?? 'Untitled';

      const notices: string[] = [];
      const raw = data.page ?? (data.pageFrom ? readPath(context, data.pageFrom) : undefined);
      const parsed = typeof raw === 'string' ? parsePageJson(raw) : raw;
      let page = validatePage(parsed, fallbackTitle);
      if (!page) {
        // A draft that is not a page spec does not stop the page: the step's
        // deliverable is the page, and the title is already known.
        page = { title: fallbackTitle, blocks: [] };
        if (raw !== undefined) notices.push('The draft was not a page; the page was created with its title and no body.');
      }

      const requestedBy = str(readPath(context, '$.requestedBy')) ?? str(readPath(context, '$.trigger.requestedBy'));
      if (!requestedBy) return { status: 'failed', error: 'document: the run names no requester to create the page for' };
      const requestedByName = str(readPath(context, '$.requestedByName')) ?? str(readPath(context, '$.trigger.requestedByName'));
      const channel = str(readPath(context, '$.chatChannel'), 512) ?? str(readPath(context, '$.trigger.chatChannel'), 512);
      const editors = channel ? await members(channel, requestedBy) : [requestedBy];

      const documentId = newDocumentId();
      const at = now().toISOString();
      try {
        await putConversationPageRow(documents, {
          documentId, title: page.title, requestedBy, requestedByName, channel, editors, at, runId: helpers.runId,
        });
      } catch (err) {
        return { status: 'failed', error: `document: the page could not be created: ${err instanceof Error ? err.message : String(err)}` };
      }

      let applied = 0;
      let written: 'document' | 'row-only' = 'document';
      try {
        applied = await writeBody(documentId, page.title, pageOps(page), requestedBy);
      } catch (err) {
        written = 'row-only';
        notices.push(`The page exists but its body could not be written: ${err instanceof Error ? err.message : String(err)}`);
        log.warn({ documentId, err: String(err) }, 'document: the body could not be written through the collaboration channel');
      }

      // The row went in with the channel and the editors; what is left is the
      // card — the same function a repair of an old run calls
      // (conversationDocument.ts), so create time and repair cannot drift.
      let card: RunDocumentCardOutcome = { outcome: 'skipped' };
      if (channel) {
        const loopId = str(readPath(context, '$.loopId'));
        ({ card } = await putRunDocumentInConversation({
          documentId,
          title: page.title,
          icon: CREATED_DOCUMENT_ICON,
          channel,
          requester: { userId: requestedBy, ...(requestedByName ? { displayName: requestedByName } : {}) },
          runIds: [loopId, helpers.runId].filter((id): id is string => !!id),
          pipelineId: str(readPath(context, '$.pipelineId')) ?? 'chat-agent-loop',
          instruction: str(readPath(context, '$.instruction'), 160),
          editors,
          rowAttached: true,
        }, { ...(opts.names ? { names: opts.names } : {}), announce }));
        if (card.outcome === 'failed') notices.push(`The page was not announced in the conversation: ${card.error ?? 'unknown error'}`);
      }

      return {
        status: 'completed',
        output: {
          documentId,
          title: page.title,
          ...(channel ? { channel } : {}),
          editors,
          blockCount: page.blocks.length,
          written,
          applied,
          card: card.outcome,
          ...(notices.length ? { notices } : {}),
          artifacts: [{ artifactId: documentId, kind: 'page', title: page.title }],
          result: { kind: 'document', documentId },
        },
      };
    },
  };
}
