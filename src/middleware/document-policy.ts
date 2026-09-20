import type { Request, Response, NextFunction } from 'express';
import type { DocumentGrantClaims, DocumentGrantOperation } from '@connorhoehn/realtime-modules/server';
import {
  createDocumentGrantVerifierFromEnv,
  documentGrantAllows,
} from '@connorhoehn/realtime-modules/server';

export type DocumentGrantVerifier = (token: string) => Promise<DocumentGrantClaims>;

/** Resolve REST verbs into the existing document grant vocabulary. Domain rules
 * (author-only comment edits, designated reviewers, lifecycle CAS) still apply. */
export function documentOperation(req: Pick<Request, 'method' | 'path' | 'body'>): DocumentGrantOperation {
  if (req.method === 'GET' || req.method === 'HEAD') return 'read';
  if (/\/lifecycle\/ownership\/?$/.test(req.path)) return 'manage';
  if (/\/lifecycle\/transitions\/?$/.test(req.path)) {
    return ['approve', 'request_changes'].includes(req.body?.action) ? 'comment' : 'edit';
  }
  if (/\/review-requests(?:\/[^/]+)?\/?$/.test(req.path)) {
    return req.method === 'PATCH' && ['approved', 'changes_requested'].includes(req.body?.state)
      && req.body?.dueDate === undefined ? 'comment' : 'manage';
  }
  if (/\/(comments|reviews|reactions)(\/|$)/.test(req.path)) return 'comment';
  if (/\/items\/[^/]+\/(ack|answer)\/?$/.test(req.path)) return 'comment';
  return 'edit';
}

/** The single-tenant deployment is an explicit boundary. A valid token from a
 * sibling tenant or for a different document is insufficient authority. */
export function createDocumentPolicy(options: {
  verify: DocumentGrantVerifier;
  tenantId: string;
  canWrite?: (documentId: string) => Promise<boolean>;
}) {
  return async (req: Request, res: Response, next: NextFunction): Promise<void> => {
    const bearer = req.headers.authorization;
    if (!bearer?.startsWith('Bearer ')) {
      res.status(401).json({ error: 'Document authorization required' });
      return;
    }
    let claims: DocumentGrantClaims;
    try {
      claims = await options.verify(bearer.slice(7));
    } catch {
      res.status(401).json({ error: 'Invalid or expired document authorization' });
      return;
    }
    if (!documentGrantAllows(claims, {
      tenantId: options.tenantId,
      documentId: req.params.documentId,
      operation: documentOperation(req),
    })) {
      res.status(403).json({ error: 'Document operation forbidden' });
      return;
    }
    // Workflow transitions retain their own owner/reviewer/CAS rules, including
    // the owner's explicit reopen. Content edits cannot bypass the lock through
    // a REST section/import/agent route after approval or finalization.
    if (documentOperation(req) === 'edit' && !/^\/lifecycle(?:\/|$)/.test(req.path)) {
      try {
        const canWrite = options.canWrite ?? (await import('../services/documentWritePolicy.js')).documentContentWritable;
        if (!await canWrite(claims.documentId)) {
          res.status(409).json({ error: 'Document content is locked', code: 'document_locked' });
          return;
        }
      } catch {
        res.status(503).json({ error: 'Document write policy is unavailable', code: 'document_policy_unavailable' });
        return;
      }
    }
    req.documentGrant = claims;
    req.user = { sub: claims.sub, orgId: claims.tenantId };
    next();
  };
}

/** Installed before every /documents/:documentId route, including formerly
 * anonymous workflow reads. Local fixture auth is available only outside prod. */
export async function requireDocumentPolicy(req: Request, res: Response, next: NextFunction): Promise<void> {
  const configured = Boolean(process.env.DOCUMENT_GRANT_ISSUER || process.env.DOCUMENT_GRANT_PUBLIC_KEY || process.env.DOCUMENT_GRANT_KEY_ID);
  if (!configured && process.env.NODE_ENV !== 'production' && process.env.DOCUMENT_AUTH_MODE !== 'grants') {
    next();
    return;
  }
  try {
    const tenantId = process.env.DOCUMENT_TENANT_ID;
    if (!tenantId) throw new Error('DOCUMENT_TENANT_ID is required');
    const verify = createDocumentGrantVerifierFromEnv(process.env);
    if (!verify) throw new Error('Document grant verifier is required');
    await createDocumentPolicy({ tenantId, verify })(req, res, next);
  } catch {
    res.status(503).json({ error: 'Document authorization is not configured' });
  }
}

/** Server lifecycle rules remain authoritative; a transport grant can narrow
 * their action list but never add a role or bypass compare-and-swap. */
export function filterDocumentLifecycleActions<T extends string>(req: Request, actions: T[]): T[] {
  if (!req.documentGrant) return actions;
  return actions.filter(action => req.documentGrant!.operations.includes(
    ['approve', 'request_changes'].includes(action) ? 'comment' : 'edit',
  ));
}
