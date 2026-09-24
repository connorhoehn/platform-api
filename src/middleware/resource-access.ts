// platform-api/src/middleware/resource-access.ts
//
// Loop 29c (realtime-examples access sweep): the route-level wrappers over the
// access checks that already exist, so a route states its policy where it is
// registered instead of somewhere inside its handler. No new ACL model:
//
//   requireDocumentRead(idOf)  → documentReadGate().assertCanRead  (#135 document:read)
//   requireDocumentEdit(idOf)  → documentReadGate().assertCanEdit  (#41 document:edit)
//   requireChannelMember(idOf) → isChannelMember                   (#151 chat membership)
//
// Conventions (the ones #135/#154/#171 established):
//   no bearer                  → 401
//   no access / no such thing  → 404 (a stranger learns nothing)
//   read-only caller on a write → 403 `{ code: 'forbidden' }`
//
// Every wrapper carries `accessPolicy`, which the router guard test
// (src/__tests__/route-access-guard.test.ts) reads: a route with an id param
// must pass through a marked middleware or be listed in its allowlist with a
// reason.

import type { NextFunction, Request, RequestHandler, Response } from 'express';
import { documentReadGate } from '../documents/documentReadGate';
import { DocumentEditForbiddenError, isChannelMember } from '../deck/documentEditAccess';
import { AppError, NotFoundError } from './error-handler';

export type AccessMarkedHandler = RequestHandler & { accessPolicy: string };

/** Tag a middleware with the policy it enforces (read by the router guard test). */
export function markAccess(fn: RequestHandler, policy: string): AccessMarkedHandler {
  const marked = fn as AccessMarkedHandler;
  marked.accessPolicy = policy;
  return marked;
}

type IdOf = (req: Request) => unknown;
const fromParam = (name: string): IdOf => (req) => req.params?.[name];

function idFrom(idOf: IdOf | string, req: Request): string {
  const raw = (typeof idOf === 'string' ? fromParam(idOf) : idOf)(req);
  return typeof raw === 'string' ? raw.trim() : '';
}

/**
 * The caller may read the document `idOf` names (default `:documentId`). An
 * optional id (a body field the route does not require) passes when absent —
 * the route's own validation owns "missing".
 */
export function requireDocumentRead(idOf: IdOf | string = 'documentId', opts: { optional?: boolean } = {}): AccessMarkedHandler {
  return markAccess((req: Request, _res: Response, next: NextFunction) => {
    const documentId = idFrom(idOf, req);
    if (!documentId) { if (opts.optional) { next(); return; } next(new NotFoundError('Document not found')); return; }
    documentReadGate().assertCanRead(req, documentId).then(() => next(), next);
  }, 'document:read');
}

/** The caller may change the document `idOf` names: 401 / 404 / 403 `{ code: 'forbidden' }`. */
export function requireDocumentEdit(idOf: IdOf | string = 'documentId', opts: { optional?: boolean } = {}): AccessMarkedHandler {
  return markAccess((req: Request, res: Response, next: NextFunction) => {
    const documentId = idFrom(idOf, req);
    if (!documentId) { if (opts.optional) { next(); return; } next(new NotFoundError('Document not found')); return; }
    documentReadGate().assertCanEdit(req, documentId).then(() => next(), (err: unknown) => {
      if (err instanceof DocumentEditForbiddenError) {
        res.status(403).json({ error: err.message, code: err.code, documentId });
        return;
      }
      next(err);
    });
  }, 'document:edit');
}

/**
 * The caller is a member of the chat channel `idOf` names (a room's
 * `chat-members` row, a DM's `chat-conversations` row). Not a member → 404.
 */
export function requireChannelMember(idOf: IdOf | string, opts: { optional?: boolean; lookup?: typeof isChannelMember } = {}): AccessMarkedHandler {
  const lookup = opts.lookup ?? isChannelMember;
  return markAccess((req: Request, _res: Response, next: NextFunction) => {
    const channel = idFrom(idOf, req);
    if (!channel) { if (opts.optional) { next(); return; } next(new NotFoundError('Channel not found')); return; }
    if (!req.headers.authorization?.startsWith('Bearer ')) { next(new AppError(401, 'Authorization required')); return; }
    const userId = req.user?.sub ?? '';
    lookup(channel, userId).then((member) => {
      if (member) { next(); return; }
      next(new NotFoundError('Channel not found'));
    }, next);
  }, 'channel:member');
}

/** The body field `name`, as a string id, for the wrappers above. */
export const bodyId = (name: string): IdOf => (req) => (req.body as Record<string, unknown> | undefined)?.[name];
