// platform-api/src/documents/documentReadGate.ts
//
// NFR #154: the reader check for REST routes that hang off a document but are
// not the revision routes — today the comment routes. The rule is #135's
// `document:read` policy (`canReadDocument`: owner, editor, viewer, a member of
// the document's channel, a DM the owner filed it in). A scoped document grant
// (`req.documentGrant`) was already checked by the document-policy middleware.
//
//   no bearer at all          → 401 (even under SKIP_AUTH's dev-user fallback)
//   no read access / no row   → 404, the same answer as a missing document
//
// The row is the `crdt-documents` row; a document with no row but a lifecycle
// row is owned by the lifecycle `ownerId` (as documentAccess.ts). An allow is
// cached per (org, user, document) for 60 s — the gateway's
// DocumentPresenceAccess keeps the same window — so a Comments tab that lists,
// posts and re-lists costs one row read. A refusal is not cached: a document
// shared a moment ago must open on the next try.
//
// `assertCanEdit` is the same gate one step further, for routes that change a
// document through a run rather than a revision save — reviewing (accept /
// reject) a run's suggestions and asking the agent for an edit. The rule is
// #41's `document:edit` (owner, or a user in `editors`):
//
//   no bearer                 → 401
//   cannot even read it       → 404 (a stranger learns nothing)
//   can read but not edit     → 403 `DocumentEditForbiddenError` (#41's refusal)
//
// Allows are cached for the same 60 s under their own key; refusals are not.

import type { Request } from 'express';
import { canEditDocument, canReadDocument, DocumentEditForbiddenError, documentRoleOf, editRefusal, type EditableDocumentRow } from '../deck/documentEditAccess';
import { AppError, NotFoundError } from '../middleware/error-handler';
import log from '../lib/logger';

export interface DocumentReadGateDeps {
  getDocumentRow(documentId: string): Promise<EditableDocumentRow | null>;
  getLifecycleOwner(documentId: string): Promise<string | undefined>;
  canRead?: typeof canReadDocument;
  canEdit?: typeof canEditDocument;
  now?: () => number;
}

export interface DocumentReadGate {
  /** Throws 401/404 unless the caller may read. Returns the document owner, when known. */
  assertCanRead(req: Pick<Request, 'user' | 'documentGrant' | 'headers' | 'path'>, documentId: string): Promise<{ ownerId?: string }>;
  /** Throws 401/404 as assertCanRead, then 403 (`DocumentEditForbiddenError`) unless the caller may edit. */
  assertCanEdit(req: Pick<Request, 'user' | 'documentGrant' | 'headers' | 'path'>, documentId: string): Promise<{ ownerId?: string }>;
  /** The document owner without an access check (after assertCanRead). */
  ownerOf(documentId: string): Promise<string | undefined>;
}

export const DOCUMENT_READ_CACHE_MS = 60_000;
const CACHE_LIMIT = 5_000;

export function makeDocumentReadGate(deps: DocumentReadGateDeps): DocumentReadGate {
  const canRead = deps.canRead ?? canReadDocument;
  const canEdit = deps.canEdit ?? canEditDocument;
  const now = deps.now ?? Date.now;
  const allowed = new Map<string, { expires: number; ownerId?: string }>();
  const editAllowed = new Map<string, { expires: number; ownerId?: string }>();

  function remember(cache: Map<string, { expires: number; ownerId?: string }>, key: string, ownerId: string | undefined): void {
    if (cache.size >= CACHE_LIMIT) cache.delete(cache.keys().next().value as string);
    cache.set(key, { expires: now() + DOCUMENT_READ_CACHE_MS, ownerId });
  }

  async function effectiveRow(documentId: string): Promise<EditableDocumentRow | null> {
    const row = await deps.getDocumentRow(documentId);
    if (row) return row;
    const ownerId = await deps.getLifecycleOwner(documentId);
    return ownerId ? { createdBy: ownerId } : null;
  }

  function ownerField(row: EditableDocumentRow): string | undefined {
    return typeof row.createdBy === 'string' && row.createdBy ? row.createdBy : undefined;
  }

  return {
    async assertCanRead(req, documentId) {
      if (req.documentGrant) return {};
      if (!req.headers.authorization?.startsWith('Bearer ')) throw new AppError(401, 'Authorization required');
      const userId = req.user?.sub ?? '';
      const key = `${req.user?.orgId ?? ''}|${userId}|${documentId}`;
      const hit = allowed.get(key);
      if (hit && hit.expires > now()) return { ownerId: hit.ownerId };
      const row = await effectiveRow(documentId);
      const decision = row ? await canRead(row, req.user, documentId) : { allow: false as const };
      if (!row || !decision.allow) {
        log.warn({ documentId, userId, orgId: req.user?.orgId, route: req.path }, 'document read refused: the caller may not read this document');
        throw new NotFoundError('Document not found');
      }
      const ownerId = ownerField(row);
      remember(allowed, key, ownerId);
      return { ownerId };
    },
    async assertCanEdit(req, documentId) {
      // A scoped grant speaks for one document and names its operations; the
      // review route takes its document from the body, so check both here.
      if (req.documentGrant) {
        const g = req.documentGrant;
        if (g.documentId !== documentId) throw new NotFoundError('Document not found');
        if (!g.operations.includes('edit') && !g.operations.includes('manage')) {
          throw new DocumentEditForbiddenError('This session can view this document but not change it.', documentId, 'viewer');
        }
        return {};
      }
      if (!req.headers.authorization?.startsWith('Bearer ')) throw new AppError(401, 'Authorization required');
      const userId = req.user?.sub ?? '';
      const key = `${req.user?.orgId ?? ''}|${userId}|${documentId}`;
      const hit = editAllowed.get(key);
      if (hit && hit.expires > now()) return { ownerId: hit.ownerId };
      const row = await effectiveRow(documentId);
      const read = row ? await canRead(row, req.user, documentId) : { allow: false as const };
      if (!row || !read.allow) {
        log.warn({ documentId, userId, orgId: req.user?.orgId, route: req.path }, 'document edit refused: the caller may not read this document');
        throw new NotFoundError('Document not found');
      }
      const ownerId = ownerField(row);
      remember(allowed, key, ownerId);
      const edit = await canEdit(row, req.user, documentId);
      if (!edit.allow) {
        const role = documentRoleOf(row, userId);
        log.warn({ documentId, userId, orgId: req.user?.orgId, role, route: req.path }, 'document edit refused: the caller may read but not edit this document');
        throw new DocumentEditForbiddenError(editRefusal(row, userId), documentId, role);
      }
      remember(editAllowed, key, ownerId);
      return { ownerId };
    },
    async ownerOf(documentId) {
      const row = await effectiveRow(documentId);
      return row ? ownerField(row) : undefined;
    },
  };
}

let defaultGate: DocumentReadGate | null = null;

/** The process-wide gate over DynamoDB (lazy, so tests that mock it never touch AWS). */
export function documentReadGate(): DocumentReadGate {
  if (defaultGate) return defaultGate;
  /* eslint-disable @typescript-eslint/no-require-imports */
  const { GetCommand } = require('@aws-sdk/lib-dynamodb') as typeof import('@aws-sdk/lib-dynamodb');
  const { docClient } = require('../lib/aws-clients') as typeof import('../lib/aws-clients');
  const { DOCUMENTS_TABLE } = require('../deck/presentationDocuments') as typeof import('../deck/presentationDocuments');
  const repos = require('../repositories') as typeof import('../repositories');
  /* eslint-enable @typescript-eslint/no-require-imports */
  defaultGate = makeDocumentReadGate({
    // Only the four access fields: a deck row carries its revision list, and
    // the check has no use for it.
    getDocumentRow: async (id) => {
      const res = await docClient.send(new GetCommand({
        TableName: DOCUMENTS_TABLE,
        Key: { documentId: id },
        ProjectionExpression: 'documentId, createdBy, editors, viewers, #channel',
        ExpressionAttributeNames: { '#channel': 'channel' },
      }));
      return (res.Item as EditableDocumentRow | undefined) ?? null;
    },
    getLifecycleOwner: async (id) => (await repos.documentLifecycleRepo.get(id))?.ownerId,
  });
  return defaultGate;
}
