// A document an /agent run made, put into the conversation the run came from.
//
// "In the conversation" is two facts, and this is the one place both are
// written:
//   the row     `channel` = the run's chat channel (the conversation's
//               Documents tab lists `d.channel === activeChannel`) and
//               `editors` ⊇ the conversation's members, so everyone in it can
//               open and edit the page (document:read / document:edit);
//   the card    the run's card in the thread turns into the document card
//               ("<title> · Created by <name> · Open"), edited in place
//               (chatRunCard.ts `announceRunDocument`) — one message, not two.
//
// Three callers, one function:
//   create time  `document:create` (documentCreateStepHandler.ts) — the row
//                was just written with both, so only the card is left;
//   read time    a Done run card's status read (routes/index.ts getStoredRun
//                → runDocumentRepair.ts `scheduleRunDocumentRepair`);
//   backfill     POST /api/admin/agent-documents/repair (runDocumentRepair.ts
//                `repairAllRunDocuments`).
// Every step is idempotent: a row already on the channel with every member as
// an editor is not written, a card that is already this document's card is not
// edited, and a repair never posts a second card when it cannot find the first.

import { GetCommand, UpdateCommand } from '@aws-sdk/lib-dynamodb';
import { docClient } from '../lib/aws-clients';
import log from '../lib/logger';
import { DOCUMENTS_TABLE } from '../deck/presentationDocuments';
import { conversationMembers } from '../chat/conversationMembers';
import { announceRunDocument, type RunDocumentCardInput, type RunDocumentCardOutcome } from '../documents/chatRunCard';

type Send = (command: unknown) => Promise<unknown>;

export interface ConversationDocumentInput {
  documentId: string;
  title: string;
  icon?: string;
  channel: string;
  requester: { userId: string; displayName?: string };
  /** Every id the run's card may carry: the loop id the client was given, and the executor's. */
  runIds: string[];
  pipelineId: string;
  /** What was asked, for the card's run line. */
  instruction?: string;
  /** Appended to the card's run line (a repair says so). */
  note?: string;
  /** The editors, when the caller already read them (create time). */
  editors?: string[];
  /** The row was just written with `channel` and `editors` (create time): skip the row step. */
  rowAttached?: boolean;
  /** Post a document card when no run card is found. Create time: yes. A repair: never. */
  postIfNoCard?: boolean;
}

export interface ConversationDocumentOutcome {
  /**
   * `attached`: the row was moved onto the conversation now; `already`: it was;
   * `missing`: no such document; `elsewhere`: it belongs to another
   * conversation, and is left there.
   */
  document: 'attached' | 'already' | 'missing' | 'elsewhere';
  editors: string[];
  card: RunDocumentCardOutcome;
}

export interface ConversationDocumentDeps {
  documents?: { send: Send };
  members?: (channel: string, requester: string) => Promise<string[]>;
  names?: (userIds: string[]) => Promise<Map<string, string>>;
  announce?: (input: RunDocumentCardInput) => Promise<RunDocumentCardOutcome>;
}

/** "Alice", "Alice & Grace", "Alice, Grace & 3 others". */
export function namesLine(names: string[]): string {
  if (names.length <= 1) return names[0] ?? '';
  if (names.length === 2) return `${names[0]} & ${names[1]}`;
  const rest = names.length - 2;
  return `${names[0]}, ${names[1]} & ${rest === 1 ? '1 other' : `${rest} others`}`;
}

/** The line under the document card: what was asked, and for whom. */
export function runDocumentSummary(instruction: string | undefined, who: string, note?: string): string {
  return [instruction ? `“${instruction}”` : undefined, `Created by the agent for ${who}`, note].filter(Boolean).join(' · ');
}

async function profileNames(userIds: string[]): Promise<Map<string, string>> {
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const { profileRepo } = require('../repositories') as typeof import('../repositories');
  const profiles = await profileRepo.batchGetProfiles(userIds).catch(() => []);
  return new Map(profiles.filter((p) => p.displayName).map((p) => [p.userId, p.displayName]));
}

function editorList(raw: unknown): string[] {
  if (Array.isArray(raw)) return raw.filter((v): v is string => typeof v === 'string');
  if (raw instanceof Set) return [...raw].filter((v): v is string => typeof v === 'string');
  return [];
}

/**
 * Move the row onto the conversation: `channel` set (only when it has none or
 * already this one) and every member added to `editors`. Existing editors are
 * kept; `updatedAt` is not touched — sharing a page is not a change to it.
 */
async function attachRow(
  documents: { send: Send },
  documentId: string,
  channel: string,
  members: string[],
): Promise<{ document: ConversationDocumentOutcome['document']; editors: string[] }> {
  const res = await documents.send(new GetCommand({ TableName: DOCUMENTS_TABLE, Key: { documentId } })) as { Item?: Record<string, unknown> };
  const row = res?.Item;
  if (!row) return { document: 'missing', editors: [] };
  const current = editorList(row.editors);
  if (typeof row.channel === 'string' && row.channel && row.channel !== channel) return { document: 'elsewhere', editors: current };
  const editors = Array.from(new Set([...current, ...members]));
  if (row.channel === channel && editors.length === current.length) return { document: 'already', editors: current };
  await documents.send(new UpdateCommand({
    TableName: DOCUMENTS_TABLE,
    Key: { documentId },
    UpdateExpression: 'SET #channel = :channel, #editors = :editors',
    ConditionExpression: 'attribute_exists(documentId) AND (attribute_not_exists(#channel) OR #channel = :channel)',
    ExpressionAttributeNames: { '#channel': 'channel', '#editors': 'editors' },
    ExpressionAttributeValues: { ':channel': channel, ':editors': editors },
  }));
  return { document: 'attached', editors };
}

/** Put a run's document into its conversation: the row, then the card. The card step never throws. */
export async function putRunDocumentInConversation(
  input: ConversationDocumentInput,
  deps: ConversationDocumentDeps = {},
): Promise<ConversationDocumentOutcome> {
  const documents = deps.documents ?? (docClient as unknown as { send: Send });
  const members = deps.members ?? ((channel: string, requester: string) => conversationMembers(channel, requester));
  const names = deps.names ?? profileNames;
  const announce = deps.announce ?? ((card: RunDocumentCardInput) => announceRunDocument(card));

  const conversation = input.editors ?? await members(input.channel, input.requester.userId);
  let document: ConversationDocumentOutcome['document'] = 'already';
  let editors = conversation;
  if (!input.rowAttached) {
    ({ document, editors } = await attachRow(documents, input.documentId, input.channel, conversation));
    if (document === 'missing' || document === 'elsewhere') {
      log.info({ documentId: input.documentId, channel: input.channel, document }, 'run document not put into the conversation');
      return { document, editors, card: { outcome: 'skipped' } };
    }
  }

  const nameOf = await names(conversation).catch(() => new Map<string, string>());
  if (input.requester.displayName) nameOf.set(input.requester.userId, input.requester.displayName);
  const who = namesLine(conversation.map((id) => nameOf.get(id) ?? id));
  const card = await announce({
    channel: input.channel,
    runIds: input.runIds.filter(Boolean),
    pipelineId: input.pipelineId,
    documentId: input.documentId,
    title: input.title,
    ...(input.icon ? { icon: input.icon } : {}),
    requester: input.requester,
    summary: runDocumentSummary(input.instruction, who, input.note),
    ...(input.postIfNoCard === false ? { postIfNoCard: false } : {}),
  });
  return { document, editors, card };
}
