// platform-api/src/pipeline/runSourceAccess.ts
//
// The documents a run is told to read (`triggerPayload.sources`) must be ones
// the person starting it may read (`document:read`, canReadDocument). A step
// that reads a source joins the document as the agent — it does not check the
// requester — so without this a run could read any document by naming it.
//
// Two forms of one rule:
//   - request form, `assertRunSourcesReadable(req, sources)`: run drafts (PUT,
//     dispatch) and the generic trigger route, before anything starts. Every
//     document source is checked; the refusal is a 404 `source-unreadable`
//     naming EVERY failing id (ids only — the caller supplied them; a title
//     would describe a document they may not read).
//   - context form, `unreadableSourcesFor(user, ids)`: the deck steps re-check
//     as the run's `requestedBy`, so a user-defined definition that wires
//     `deck:read-sources` / `deck:draft-from-sources` cannot get around it.
//
// Source shapes: run-draft `{kind:'document', id}` and deck refs
// `{documentId, title}`; other kinds (transcripts, recordings) are not
// documents and are left to their own checks.

import type { Request } from 'express';
import { canReadDocument, type EditableDocumentRow } from '../deck/documentEditAccess';
import { makeDocumentReadGate, type DocumentReadGate } from '../documents/documentReadGate';
import { DocumentAccessError } from '../documents/documentAccess';
import { AppError, ValidationError } from '../middleware/error-handler';

/** More document sources than this in one request is refused outright. */
export const MAX_RUN_SOURCE_DOCUMENTS = 50;

function str(value: unknown): string | undefined {
  return typeof value === 'string' && value.trim() ? value.trim() : undefined;
}

/** The document ids a `sources` array names, in order, deduped. */
export function documentSourceIds(raw: unknown): string[] {
  if (!Array.isArray(raw)) return [];
  const out: string[] = [];
  for (const entry of raw) {
    if (!entry || typeof entry !== 'object') continue;
    const e = entry as { kind?: unknown; id?: unknown; documentId?: unknown };
    if (e.kind !== undefined && e.kind !== 'document') continue;
    const id = str(e.documentId) ?? str(e.id);
    if (id && !out.includes(id)) out.push(id);
  }
  return out;
}

export interface RunSourceAccessDeps {
  getDocumentRow(documentId: string): Promise<EditableDocumentRow | null>;
  getLifecycleOwner(documentId: string): Promise<string | undefined>;
  canRead?: typeof canReadDocument;
}

function defaultDeps(): RunSourceAccessDeps {
  /* eslint-disable @typescript-eslint/no-require-imports */
  const { getPresentationDocumentStore } = require('../deck/presentationDocuments') as typeof import('../deck/presentationDocuments');
  const repos = require('../repositories') as typeof import('../repositories');
  /* eslint-enable @typescript-eslint/no-require-imports */
  return {
    getDocumentRow: (id) => getPresentationDocumentStore().get(id) as Promise<EditableDocumentRow | null>,
    getLifecycleOwner: async (id) => (await repos.documentLifecycleRepo.get(id))?.ownerId,
  };
}

let depsOverride: RunSourceAccessDeps | null = null;
let gate: DocumentReadGate | null = null;
/** Tests: the rows and policy the default gate reads (null restores the real ones). */
export function setRunSourceAccessDepsForTests(deps: RunSourceAccessDeps | null): void {
  depsOverride = deps;
  gate = null;
}
function currentDeps(): RunSourceAccessDeps { return depsOverride ?? defaultDeps(); }
/** The rows and policy the context form reads (the platform's, or the tests' override). */
export function runSourceAccessDeps(): RunSourceAccessDeps { return currentDeps(); }
function defaultGate(): DocumentReadGate {
  if (!gate) {
    const deps = currentDeps();
    gate = makeDocumentReadGate({ getDocumentRow: deps.getDocumentRow, getLifecycleOwner: deps.getLifecycleOwner, ...(deps.canRead ? { canRead: deps.canRead } : {}) });
  }
  return gate;
}

export function sourceUnreadableError(ids: string[], what = 'the run was not prepared'): DocumentAccessError {
  const message = `Source ${ids.length === 1 ? 'document' : 'documents'} not readable: ${ids.join(', ')} — ${what}`;
  return new DocumentAccessError(404, { error: message, code: 'source-unreadable', sourceIds: ids });
}

type ReqLike = Pick<Request, 'user' | 'documentGrant' | 'headers' | 'path'>;

/**
 * Request form. Throws 404 `source-unreadable` (every failing id) unless the
 * caller may read every document source; a 401 and other errors pass through.
 */
export async function assertRunSourcesReadable(
  req: ReqLike,
  sources: unknown,
  opts: { assertOne?: (req: ReqLike, documentId: string) => Promise<unknown>; what?: string } = {},
): Promise<void> {
  const ids = documentSourceIds(sources);
  if (!ids.length) return;
  if (ids.length > MAX_RUN_SOURCE_DOCUMENTS) throw new ValidationError(`at most ${MAX_RUN_SOURCE_DOCUMENTS} source documents`);
  const assertOne = opts.assertOne ?? ((r: ReqLike, id: string) => defaultGate().assertCanRead(r, id));
  const unreadable: string[] = [];
  for (const id of ids) {
    try {
      await assertOne(req, id);
    } catch (err) {
      if (!(err instanceof AppError) || (err.status !== 404 && err.status !== 403)) throw err;
      unreadable.push(id);
    }
  }
  if (unreadable.length) throw sourceUnreadableError(unreadable, opts.what);
}

/**
 * Context form: the ids among `ids` that `user` may NOT read (a missing row
 * counts as unreadable). A row that cannot be read at all throws.
 */
export async function unreadableSourcesFor(
  user: { sub: string; orgId?: string },
  ids: readonly string[],
  deps: RunSourceAccessDeps = currentDeps(),
): Promise<string[]> {
  const canRead = deps.canRead ?? canReadDocument;
  const out: string[] = [];
  for (const id of ids) {
    let row = await deps.getDocumentRow(id);
    if (!row) {
      const owner = await deps.getLifecycleOwner(id);
      row = owner ? { createdBy: owner } : null;
    }
    const decision = row ? await canRead(row, user, id) : { allow: false };
    if (!decision.allow) out.push(id);
  }
  return out;
}
