// platform-api/src/routes/documentWork.ts
//
// Work fields on a document (docs/design/documents-detail/PLAN.md §2.1).
//
//   GET   /api/documents/:documentId/work   → { work: DocumentWorkView }
//   PATCH /api/documents/:documentId/work   { expectedRevision, set?, criteria?, links? }
//         → 200 { work, applied, changed }   | 409 { error: 'work_conflict', fields, work }
//   GET   /api/document-work?scope=<scopeId> → { scope, rows, rollup }
//
// Reads backfill from the lifecycle row and write nothing. A PATCH is a
// read-modify-write conditional on the stored revision (retried up to 3
// times); it answers 409 only when a field it changes was changed after its
// `expectedRevision` (workBackfill.ts). A PATCH needs the `document:edit`
// policy (owner or editor), or a scoped grant the document-policy middleware
// already checked for `edit`.
//
// After each applied write: `doc:work_updated` on `doc-work:<documentId>` and
// on the scope channel (old and new scope when it moved; `workScopeChannel`:
// `doc-work-scope:<parentId>`, or `doc-work-scope:<orgId>:type:<type>`) — ids and
// enums only, never text, because gateway channels carry no per-document
// authorization; clients re-read over REST. Plus the durable
// `document.work.updated` (outbox → MQ) for a later work-graph projection.

import { Router, type Request, type Response, type NextFunction } from 'express';
import { requireAuth } from '../middleware/auth';
import {
  applyWorkPatch,
  backfillWork,
  parseWorkPatch,
  rollupWork,
  TYPE_SCOPE,
  workScopeChannel,
  WorkPatchError,
  type DocumentWork,
  type DocumentWorkView,
} from '../documents/workBackfill';
import { DocumentAccessError, requireDocumentAccess, type DocumentAccessDeps } from '../documents/documentAccess';
import type { RunDraft } from '../repositories/RunDraftRepository';
import type { RollupRecentRun } from '../pipeline/run-rollup';
import log from '../lib/logger';

export interface DocumentWorkDeps extends DocumentAccessDeps {
  work: {
    get(documentId: string): Promise<DocumentWork | null>;
    getMany(documentIds: readonly string[]): Promise<Map<string, DocumentWork | null>>;
    listByScope(scopeId: string): Promise<DocumentWork[]>;
    putIfRevision(next: DocumentWork, seenRevision: number): Promise<void>;
  };
  getLifecycleMany(documentIds: readonly string[]): Promise<Map<string, { status?: string; ownerId?: string } | null>>;
  drafts: { listForDocument(documentId: string): Promise<RunDraft[]> };
  /** The pipeline's rollup `recent` entries (≤ 5), for an active run's live status. */
  recentRuns(pipelineIds: readonly string[]): Promise<Map<string, RollupRecentRun[]>>;
  /** A dispatched run's recorded cost (undefined = none recorded). */
  runCost(pipelineId: string, runId: string): Promise<number | undefined>;
  publishSignal(channelId: string, eventName: string, payload: Record<string, unknown>): Promise<void>;
  publishDurable(eventName: string, payload: Record<string, unknown>): Promise<void>;
  now(): string;
}

const CAS_ATTEMPTS = 3;
const SCOPE_CONCURRENCY = 10;
const MAX_COST_LOOKUPS = 50;

export function isConflictError(err: unknown): boolean {
  const name = (err as { name?: string } | null)?.name;
  return name === 'WorkRevisionConflict';
}

type Handler = (req: Request, res: Response) => Promise<void>;

/** Sends the structured refusals this router throws; everything else goes to the error handler. */
export function handle(fn: Handler) {
  return (req: Request, res: Response, next: NextFunction): void => {
    fn(req, res).catch((err: unknown) => {
      if (err instanceof DocumentAccessError) { res.status(err.status).json(err.body); return; }
      if (err instanceof WorkPatchError) { res.status(400).json({ error: err.message, code: 'invalid_patch' }); return; }
      next(err);
    });
  };
}

function orgMismatch(row: { organizationId?: string } | null | undefined, orgId: string | undefined): boolean {
  return !!row?.organizationId && !!orgId && row.organizationId !== orgId;
}

async function mapLimit<T, R>(items: readonly T[], limit: number, fn: (item: T) => Promise<R>): Promise<R[]> {
  const out: R[] = new Array(items.length);
  let next = 0;
  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (next < items.length) {
      const i = next++;
      out[i] = await fn(items[i]!);
    }
  });
  await Promise.all(workers);
  return out;
}

/** What a list row shows about the run its document last dispatched. */
export interface ActiveRun {
  runId: string;
  pipelineId: string;
  draftId: string;
  /** The rollup's run status (`pending` when the rollup has not seen the run yet). */
  status: string;
  startedAt?: string;
  completedAt?: string;
  dispatchedAt?: string;
}

function latestDispatch(drafts: RunDraft[]): RunDraft | undefined {
  return drafts
    .filter((d) => d.status === 'dispatched' && d.runId)
    .sort((a, b) => (b.dispatchedAt ?? '').localeCompare(a.dispatchedAt ?? ''))[0];
}

const LIVE_RUN = new Set(['pending', 'running', 'awaiting_approval', 'paused_at_breakpoint']);

export function createDocumentWorkRouters(deps: DocumentWorkDeps = defaultDocumentWorkDeps()): { documentWorkRouter: Router; documentWorkListRouter: Router } {
  // Mounted at /documents/:documentId/work.
  const documentWorkRouter = Router({ mergeParams: true });
  // Mounted at /document-work.
  const documentWorkListRouter = Router();

  documentWorkRouter.get('/', requireAuth, handle(async (req, res) => {
    const documentId = String(req.params.documentId ?? '');
    const { lifecycle } = await requireDocumentAccess(deps, req, documentId, 'read');
    const row = await deps.work.get(documentId);
    if (orgMismatch(row, req.user?.orgId)) { res.status(404).json({ error: 'document not found', code: 'document-not-found', documentId }); return; }
    res.status(200).json({ work: backfillWork(documentId, row, lifecycle) });
  }));

  documentWorkRouter.patch('/', requireAuth, handle(async (req, res) => {
    const documentId = String(req.params.documentId ?? '');
    const patch = parseWorkPatch(req.body);
    const { lifecycle } = await requireDocumentAccess(deps, req, documentId, 'edit');
    const userId = req.user!.sub;
    const organizationId = req.user?.orgId;

    for (let attempt = 0; attempt < CAS_ATTEMPTS; attempt++) {
      const current = await deps.work.get(documentId);
      if (orgMismatch(current, organizationId)) { res.status(404).json({ error: 'document not found', code: 'document-not-found', documentId }); return; }
      const now = deps.now();
      const result = applyWorkPatch(documentId, current, patch, { userId, now, ...(organizationId ? { organizationId } : {}) });
      if (result.conflicts.length > 0) {
        res.status(409).json({
          error: 'work_conflict',
          message: 'Someone changed these fields after the revision this edit was made from. Re-read and retry.',
          fields: result.conflicts,
          work: backfillWork(documentId, current, lifecycle),
        });
        return;
      }
      if (result.changed.length === 0) {
        res.status(200).json({ work: backfillWork(documentId, current, lifecycle), applied: false, changed: [] });
        return;
      }
      try {
        await deps.work.putIfRevision(result.next, current?.revision ?? 0);
      } catch (err) {
        if (isConflictError(err)) continue;
        throw err;
      }
      emitWorkUpdated(deps, current, result.next, result.changed);
      res.status(200).json({ work: backfillWork(documentId, result.next, lifecycle), applied: true, changed: result.changed });
      return;
    }
    res.status(409).json({ error: 'work_busy', message: 'The work row kept changing under this edit; retry.' });
  }));

  documentWorkListRouter.get('/', requireAuth, handle(async (req, res) => {
    const scopeId = typeof req.query.scope === 'string' ? req.query.scope : '';
    if (!scopeId || scopeId.length > 200) { res.status(400).json({ error: 'scope is required (a parent id or type:<type>)' }); return; }
    const orgId = req.user?.orgId;
    // A type folder exists in every organisation: list only the caller's own org's rows.
    const typeScope = TYPE_SCOPE.test(scopeId);
    const rows = (await deps.work.listByScope(scopeId))
      .filter((r) => (typeScope ? (r.organizationId ?? '') === (orgId ?? '') : !orgMismatch(r, orgId)));
    const ids = rows.map((r) => r.documentId);
    const [lifecycles, draftsPerDoc] = await Promise.all([
      deps.getLifecycleMany(ids),
      mapLimit(ids, SCOPE_CONCURRENCY, (id) => deps.drafts.listForDocument(id)),
    ]);
    const dispatches = draftsPerDoc.map(latestDispatch);
    const pipelineIds = [...new Set(dispatches.filter((d): d is RunDraft => !!d).map((d) => d.pipelineId))];
    const recent = pipelineIds.length ? await deps.recentRuns(pipelineIds) : new Map<string, RollupRecentRun[]>();

    let agents = 0;
    const items = rows.map((row, i) => {
      const lifecycle = lifecycles.get(row.documentId) ?? null;
      const view: DocumentWorkView & { lifecycleStatus?: string; activeRun?: ActiveRun } = backfillWork(row.documentId, row, lifecycle);
      if (lifecycle?.status) view.lifecycleStatus = lifecycle.status;
      const drafts = draftsPerDoc[i] ?? [];
      agents += drafts.filter((d) => d.status === 'draft' || d.status === 'dispatching').length;
      const d = dispatches[i];
      if (d?.runId) {
        const entry = (recent.get(d.pipelineId) ?? []).find((r) => r.runId === d.runId);
        const status = entry?.status ?? 'pending';
        view.activeRun = {
          runId: d.runId, pipelineId: d.pipelineId, draftId: d.draftId, status,
          ...(entry?.startedAt ? { startedAt: entry.startedAt } : {}),
          ...(entry?.completedAt ? { completedAt: entry.completedAt } : {}),
          ...(d.dispatchedAt ? { dispatchedAt: d.dispatchedAt } : {}),
        };
        if (LIVE_RUN.has(status)) agents += 1;
      }
      return view;
    });
    items.sort((a, b) => (a.rank ?? '\uffff').localeCompare(b.rank ?? '\uffff') || a.documentId.localeCompare(b.documentId));

    const dispatched = draftsPerDoc.flat().filter((d) => d.status === 'dispatched' && d.runId).slice(0, MAX_COST_LOOKUPS);
    const costs = await mapLimit(dispatched, SCOPE_CONCURRENCY, (d) => deps.runCost(d.pipelineId, d.runId!).catch(() => undefined));
    const priced = costs.filter((c): c is number => typeof c === 'number');
    const spentUsd = priced.length ? priced.reduce((a, b) => a + b, 0) : undefined;

    res.status(200).json({ scope: scopeId, rows: items, rollup: rollupWork(items, agents, spentUsd) });
  }));

  return { documentWorkRouter, documentWorkListRouter };
}

function emitWorkUpdated(deps: DocumentWorkDeps, before: DocumentWork | null, after: DocumentWork, changed: string[]): void {
  const { documentId, revision } = after;
  const fail = (event: string) => (err: unknown) =>
    log.error({ event, documentId, err: err instanceof Error ? err.message : String(err) }, 'work signal publish failed');
  deps.publishSignal(`doc-work:${documentId}`, 'doc:work_updated', { documentId, revision, fields: changed })
    .catch(fail('doc:work_updated'));
  const org = after.organizationId ?? before?.organizationId;
  const channels = new Set([before?.scopeId, after.scopeId]
    .filter((s): s is string => !!s)
    .map((s) => workScopeChannel(s, org))
    .filter((c): c is string => !!c));
  for (const channel of channels) {
    deps.publishSignal(channel, 'doc:work_updated', {
      documentId,
      revision,
      fields: changed,
      ...(after.status ? { status: after.status } : {}),
      ...(after.rank ? { rank: after.rank } : {}),
      ...(after.scopeId ? { scopeId: after.scopeId } : {}),
    }).catch(fail('doc:work_updated'));
  }
  deps.publishDurable('document.work.updated', {
    documentId,
    revision,
    fields: changed,
    updatedBy: after.updatedBy ?? '',
    ...(after.organizationId ? { organizationId: after.organizationId } : {}),
    ...(after.status ? { status: after.status } : {}),
    ...(after.scopeId ? { scopeId: after.scopeId } : {}),
  }).catch(fail('document.work.updated'));
}

/** Production wiring, loaded lazily so tests that inject deps never touch AWS or the pipeline modules. */
export function defaultDocumentWorkDeps(): DocumentWorkDeps {
  /* eslint-disable @typescript-eslint/no-require-imports */
  const { documentWorkRepo } = require('../repositories/DocumentWorkRepository') as typeof import('../repositories/DocumentWorkRepository');
  const { runDraftRepo } = require('../repositories/RunDraftRepository') as typeof import('../repositories/RunDraftRepository');
  const repos = require('../repositories') as typeof import('../repositories');
  const { getPresentationDocumentStore } = require('../deck/presentationDocuments') as typeof import('../deck/presentationDocuments');
  const { pipelineRunRollupRepo } = require('../repositories/PipelineRunRollupRepository') as typeof import('../repositories/PipelineRunRollupRepository');
  const catalog = require('../lib/event-catalog') as typeof import('../lib/event-catalog');
  const { lifecycleMany } = require('../documents/lifecycleBatch') as typeof import('../documents/lifecycleBatch');
  const { runCostOf } = require('./pipelineEstimate') as typeof import('./pipelineEstimate');
  /* eslint-enable @typescript-eslint/no-require-imports */
  return {
    getDocumentRow: (id) => getPresentationDocumentStore().get(id),
    getLifecycle: async (id) => {
      const row = await repos.documentLifecycleRepo.get(id);
      return row ? { status: row.status, ownerId: row.ownerId } : null;
    },
    getLifecycleMany: (ids) => lifecycleMany(ids),
    work: documentWorkRepo,
    drafts: runDraftRepo,
    recentRuns: async (pipelineIds) => {
      const rows = await pipelineRunRollupRepo.getMany(pipelineIds);
      return new Map([...rows].map(([id, row]) => [id, row?.recent ?? []]));
    },
    runCost: (pipelineId, runId) => runCostOf(pipelineId, runId),
    publishSignal: (c, e, p) => catalog.publishSignal(c, e, p),
    publishDurable: (e, p) => catalog.publishDurable(e, p),
    now: () => new Date().toISOString(),
  };
}

let routers: ReturnType<typeof createDocumentWorkRouters> | null = null;
function lazy(): ReturnType<typeof createDocumentWorkRouters> {
  routers ??= createDocumentWorkRouters();
  return routers;
}

/** Mounted at /documents/:documentId/work. */
export const documentWorkRouter = Router({ mergeParams: true });
documentWorkRouter.use((req, res, next) => lazy().documentWorkRouter(req, res, next));
/** Mounted at /document-work. */
export const documentWorkListRouter = Router();
documentWorkListRouter.use((req, res, next) => lazy().documentWorkListRouter(req, res, next));
