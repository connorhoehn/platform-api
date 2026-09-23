// Who may change a document's content — the one check every deck write path
// makes before it writes: the revision save, the composer's revise and
// revise-run routes, and the deck-revise pipeline's load and write steps.
//
// The route check answers 403 before anything starts. The pipeline re-checks
// with the run's requester at write time, so a queued run cannot land a
// revision after the person lost access (or a run started by someone who
// never had it cannot write at all).
//
// The rule itself is the `document:edit` policy in lib/authorize.ts: the
// row's owner (`createdBy`) or a user the row names in `editors`.

import { GetCommand } from '@aws-sdk/lib-dynamodb';
import { authorize, principalOf, type Decision } from '../lib/authorize';
import { tenantOf } from '../lib/tenant';
import { docClient } from '../lib/aws-clients';
import { tableName } from '../lib/ddb-table-name';
import { ForbiddenError } from '../middleware/error-handler';

/** What the check reads off a document row. Any row shape with these fields. */
export interface EditableDocumentRow {
  createdBy?: unknown;
  editors?: unknown;
  viewers?: unknown;
  /** The chat the document was shared into, when it was. */
  channel?: unknown;
}

export type DocumentRole = 'owner' | 'editor' | 'viewer' | 'stranger';

function ids(value: unknown): string[] {
  return Array.isArray(value) ? value.filter((entry): entry is string => typeof entry === 'string' && !!entry) : [];
}

/** How a user stands with a document — for logs and messages; the decision is `canEditDocument`. */
export function documentRoleOf(row: EditableDocumentRow | null | undefined, userId: string): DocumentRole {
  if (!row || !userId) return 'stranger';
  if (typeof row.createdBy === 'string' && row.createdBy === userId) return 'owner';
  if (ids(row.editors).includes(userId)) return 'editor';
  if (ids(row.viewers).includes(userId)) return 'viewer';
  return 'stranger';
}

/**
 * May `userId` change this document? A row that does not exist yet is not
 * someone else's document: the caller is about to create it (the save files it
 * with the caller as `createdBy`), so it is allowed here and the route decides
 * whether "absent" is a 404.
 */
export async function canEditDocument(
  row: EditableDocumentRow | null | undefined,
  user: { sub?: string; orgId?: string } | undefined,
  documentId: string,
): Promise<Decision> {
  if (!row) return { allow: true };
  let tenant;
  try { tenant = tenantOf(user?.orgId); } catch { return { allow: false, reason: 'the caller\'s organisation is not valid', status: 403 }; }
  return authorize({
    principal: principalOf(user, tenant),
    action: 'document:edit',
    resource: {
      type: 'document',
      id: documentId,
      attrs: { ownerId: typeof row.createdBy === 'string' ? row.createdBy : undefined, editorIds: ids(row.editors) },
    },
  });
}

/** The words a refused person sees. */
export function editRefusal(row: EditableDocumentRow | null | undefined, userId: string): string {
  return documentRoleOf(row, userId) === 'viewer'
    ? 'You can view this document but not change it. Ask its owner to make you an editor.'
    : 'Only this document\'s owner or an editor can change it.';
}

/**
 * A refusal thrown where a route cannot answer directly (a chat command's
 * `prepare`): 403 with `code: 'forbidden'` and the document named.
 */
export class DocumentEditForbiddenError extends ForbiddenError {
  public readonly code = 'forbidden';
  constructor(message: string, public readonly documentId: string, public readonly role: DocumentRole) {
    super(message);
  }
}

/** Throws `DocumentEditForbiddenError` unless `user` may change the document. */
export async function assertCanEditDocument(
  row: EditableDocumentRow | null | undefined,
  user: { sub?: string; orgId?: string } | undefined,
  documentId: string,
): Promise<void> {
  const decision = await canEditDocument(row, user, documentId);
  if (decision.allow) return;
  const userId = user?.sub ?? '';
  throw new DocumentEditForbiddenError(editRefusal(row, userId), documentId, documentRoleOf(row, userId));
}

const CHAT_MEMBERS_TABLE = tableName(process.env.DYNAMODB_CHAT_MEMBERS_TABLE || 'chat-members');

/** Is `userId` a current member of `channel`? Fails closed: an unreadable table is not permission. */
export type ChannelMemberLookup = (channel: string, userId: string) => Promise<boolean>;

export const isChannelMember: ChannelMemberLookup = async (channel, userId) => {
  if (!channel || !userId) return false;
  try {
    const res = await docClient.send(new GetCommand({ TableName: CHAT_MEMBERS_TABLE, Key: { channel, userId } }));
    return !!res.Item && res.Item.removedAt === undefined;
  } catch {
    return false;
  }
};

/**
 * May `user` read this document — its revision list, slides, spec and file
 * (NFR #135)? The `document:read` policy in lib/authorize.ts: owner, editor,
 * viewer, or a member of the channel the row names. A missing row is the
 * route's own 404. The membership table is read only when nothing on the row
 * already decides it, so the owner's read costs no extra lookup.
 */
export async function canReadDocument(
  row: EditableDocumentRow,
  user: { sub?: string; orgId?: string } | undefined,
  documentId: string,
  memberLookup: ChannelMemberLookup = isChannelMember,
): Promise<Decision> {
  let tenant;
  try { tenant = tenantOf(user?.orgId); } catch { return { allow: false, reason: 'document not found', status: 404 }; }
  const userId = user?.sub ?? '';
  const channel = typeof row.channel === 'string' ? row.channel : '';
  const role = documentRoleOf(row, userId);
  const member = role === 'stranger' && !!channel && !!userId ? await memberLookup(channel, userId) : false;
  return authorize({
    principal: principalOf(user, tenant),
    action: 'document:read',
    resource: {
      type: 'document',
      id: documentId,
      attrs: {
        ownerId: typeof row.createdBy === 'string' ? row.createdBy : undefined,
        editorIds: ids(row.editors),
        viewerIds: ids(row.viewers),
        isChannelMember: member,
      },
    },
  });
}
