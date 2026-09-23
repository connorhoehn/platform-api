// platform-api/src/documents/documentAccess.ts
//
// The access check the work and run-draft routes share. The rule is the
// `document:edit` policy (lib/authorize.ts, via deck/documentEditAccess.ts):
// the document row's owner (`createdBy`) or a user its `editors` name. The row
// is the `crdt-documents` row; a document with no row but a lifecycle row is
// owned by the lifecycle `ownerId`. A scoped document grant (`req.documentGrant`)
// was already checked for the operation by the document-policy middleware
// (app.ts) and is not re-checked here.

import type { Request } from 'express';
import { canEditDocument, documentRoleOf, editRefusal, type EditableDocumentRow } from '../deck/documentEditAccess';
import { AppError } from '../middleware/error-handler';

export interface DocumentAccessDeps {
  getDocumentRow(documentId: string): Promise<EditableDocumentRow | null>;
  getLifecycle(documentId: string): Promise<{ status?: string; ownerId?: string } | null>;
}

/** A refusal with a JSON body the route sends as-is. */
export class DocumentAccessError extends AppError {
  constructor(status: number, public readonly body: Record<string, unknown>) {
    super(status, String(body.error ?? 'refused'));
  }
}

export interface DocumentAccess {
  row: EditableDocumentRow | null;
  lifecycle: { status?: string; ownerId?: string } | null;
}

export const DOCUMENT_ID = /^[A-Za-z0-9_.:-]{1,200}$/;

/**
 * Resolve the document and, for `edit`, refuse anyone but its owner or an
 * editor. 404 when neither a document row nor a lifecycle row exists.
 */
export async function requireDocumentAccess(
  deps: DocumentAccessDeps,
  req: Pick<Request, 'user' | 'documentGrant'>,
  documentId: string,
  mode: 'read' | 'edit',
): Promise<DocumentAccess> {
  if (!DOCUMENT_ID.test(documentId)) throw new DocumentAccessError(400, { error: 'documentId is invalid' });
  const [row, lifecycle] = await Promise.all([deps.getDocumentRow(documentId), deps.getLifecycle(documentId)]);
  const effective: EditableDocumentRow | null = row ?? (lifecycle?.ownerId ? { createdBy: lifecycle.ownerId } : null);
  if (!effective) throw new DocumentAccessError(404, { error: 'document not found', code: 'document-not-found', documentId });
  if (mode === 'edit' && !req.documentGrant) {
    const decision = await canEditDocument(effective, req.user, documentId);
    if (!decision.allow) {
      const userId = req.user?.sub ?? '';
      throw new DocumentAccessError(403, {
        error: editRefusal(effective, userId),
        code: 'forbidden',
        documentId,
        role: documentRoleOf(effective, userId),
      });
    }
  }
  return { row, lifecycle };
}
