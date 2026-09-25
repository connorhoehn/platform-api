// GET /api/document-search — the Documents page's ranked, paged, read-scoped
// search (realtime-examples NFR #230). The index and its data path are in
// documents/documentSearch.ts; this file only parses the query string.
//
//   ?q=release&sort=relevance|updated|name&kind=page,presentation
//   &owner=dev-connor&folder=<folderId>|unfiled&since=<epoch ms>&limit=30&cursor=<next>
//   &fresh=1 (skip the row cache — the list just changed)
//
// Not under /api/documents/:documentId — that mount's document-policy guard
// would read "document-search" as a document id.

import { Router, type Request, type Response } from 'express';
import { requireAuth } from '../middleware/auth';
import { perUserRateLimit } from '../middleware/rateLimit';
import { asyncHandler, ValidationError } from '../middleware/error-handler';
import { docClient } from '../lib/aws-clients';
import { withContext } from '../lib/logger';
import {
  DocumentSearchIndex,
  ddbSearchDeps,
  decodeCursor,
  type SearchKind,
  type SearchSort,
} from '../documents/documentSearch';

const log = withContext({ component: 'document-search' });
const KINDS: ReadonlySet<string> = new Set<SearchKind>(['page', 'presentation', 'diagram']);
const SORTS: ReadonlySet<string> = new Set<SearchSort>(['relevance', 'updated', 'name']);
/** A typist with a 200 ms debounce sends ~5/s at most; this is a runaway guard. */
export const DOCUMENT_SEARCH_PER_MINUTE = 240;

function list(value: unknown): string[] {
  return typeof value === 'string' ? value.split(',').map((s) => s.trim()).filter(Boolean).slice(0, 20) : [];
}

export function createDocumentSearchRouter(index = new DocumentSearchIndex(ddbSearchDeps(docClient))): Router {
  const router = Router();
  router.get('/', requireAuth, perUserRateLimit('document:search', DOCUMENT_SEARCH_PER_MINUTE), asyncHandler(async (req: Request, res: Response) => {
    const q = typeof req.query.q === 'string' ? req.query.q.slice(0, 200) : '';
    const sortRaw = typeof req.query.sort === 'string' ? req.query.sort : '';
    if (sortRaw && !SORTS.has(sortRaw)) throw new ValidationError('sort must be relevance, updated or name');
    const sort = (sortRaw || (q.trim() ? 'relevance' : 'updated')) as SearchSort;
    const kind = list(req.query.kind);
    if (kind.some((k) => !KINDS.has(k))) throw new ValidationError('kind must be page, presentation or diagram');
    const sinceRaw = typeof req.query.since === 'string' ? Number(req.query.since) : undefined;
    const limitRaw = typeof req.query.limit === 'string' ? Number(req.query.limit) : 30;
    const limit = Number.isFinite(limitRaw) ? Math.min(100, Math.max(1, Math.floor(limitRaw))) : 30;
    const started = Date.now();
    const result = await index.search(req.user ?? {}, {
      q,
      sort,
      filters: {
        ...(kind.length ? { kind: kind as SearchKind[] } : {}),
        ...(list(req.query.owner).length ? { owner: list(req.query.owner) } : {}),
        ...(typeof req.query.folder === 'string' && req.query.folder ? { folder: req.query.folder.slice(0, 200) } : {}),
        ...(sinceRaw !== undefined && Number.isFinite(sinceRaw) ? { since: sinceRaw } : {}),
      },
      offset: decodeCursor(typeof req.query.cursor === 'string' ? req.query.cursor : undefined),
      limit,
      fresh: req.query.fresh === '1',
    });
    const ms = Date.now() - started;
    res.setHeader('Server-Timing', `rows;dur=${result.timings.rowsMs}, access;dur=${result.timings.accessMs}, text;dur=${result.timings.textMs}, total;dur=${ms}`);
    log.debug({ event: 'document_search', ms, total: result.total, qLen: q.length, ...result.timings }, 'document search');
    const { timings: _timings, ...body } = result;
    res.status(200).json(body);
  }));
  return router;
}

export const documentSearchRouter = createDocumentSearchRouter();
