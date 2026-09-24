// platform-api/src/routes/pipelineDLQ.ts
//
// Operator surface for the EventBus dead-letter queue, backed by
// distributed-core v0.11.0 T9 (lib-expansion-3) `InMemoryDeadLetterQueue`.
// The DLQ is populated by the bootstrap's `eventBusDeadLetterHandler`
// whenever a subscriber throws or a publish path fails on the pipeline
// EventBus. This route exposes that store for operator inspection +
// targeted redrive.
//
// DLQ persistence: when PIPELINE_DLQ_PERSISTENCE_ENABLED is on (default),
// every entry is also written to the pipeline-dlq DDB table (DLQRepository)
// and read back into memory at boot (deadLetterStore.ts). These routes read
// the in-memory queue only; a redrive drops the durable copy in bootstrap's
// redrive sink, and purge drops it here.
//
// Endpoints:
//   GET  /api/pipelines/dlq?errorKind=…           → paginated list (filters: sinceMs, errorKind)
//   GET  /api/pipelines/dlq/peek/:id              → single envelope by id
//   POST /api/pipelines/dlq/redrive               → re-publish selected entries via the EventBus
//   POST /api/pipelines/dlq/purge                 → drop selected entries
//
// Both POST routes accept `body.preview === true` for a no-op dry-run that
// returns `{wouldRedrive,notFound}` / `{wouldPurge,notFound}` summaries
// without mutating the DLQ or re-publishing to the bus. This lets an
// operator verify the id list matches what they intend before committing.
//
// Error-taxonomy filter: every entry response is enriched with a derived
// `errorKind` field (the `Error`/`NetworkError`/etc. class prefix parsed
// from `lastError`). The list endpoint accepts `?errorKind=<class>` to
// filter server-side via distributed-core's `failureKindMatches` regex —
// the gateway escapes the input and anchors it as `^<kind>:` so callers
// can not inject arbitrary regex.
//
// Auth: requireAuth (mounted in app.ts) + pipelineReadRateLimit GET budget
// (also in app.ts). The two POST routes attach `pipelineWriteRateLimit`
// directly here — 10 writes / min / user — to keep destructive blast radius
// bounded even when an operator script gets carried away. The library does
// NOT ship authz, the gateway is responsible for any role gating. Today the
// platform-api does not yet have an admin-role middleware in /api/pipelines, so
// these endpoints reuse the same Cognito-authenticated session as the rest of
// the pipeline routes; folding in an admin gate when one exists is a Phase 50
// follow-up.

import { Router, type Response } from 'express';
import type { BusEvent as DCBusEvent } from 'distributed-core';
import type { DeadLetterQueue } from 'distributed-core/messaging-queue';
import { asyncHandler } from '../middleware/error-handler';
import { assertItemRunAccess, filterVisibleRunItems } from '../pipeline/runAccess';
import { markAccess } from '../middleware/resource-access';
import type { Request, NextFunction } from 'express';
import { pipelineWriteRateLimit } from '../middleware/rateLimit';
import { getPipelineBridge } from './pipelineTriggers';
import { forgetDeadLetters, getDeadLetterStore } from '../pipeline/deadLetterStore';

export const pipelineDLQRouter = Router();

// One limiter instance shared across both destructive POST routes so they
// share a budget (a busy `redrive` user can't sidestep by switching to
// `purge` and vice versa). Lazy: createRateLimiter() does not touch Redis
// until the first request flows through.
const writeLimiter = pipelineWriteRateLimit();

function dlqOr503(res: Response): DeadLetterQueue<DCBusEvent> | null {
  const bridge = getPipelineBridge();
  if (!bridge || typeof bridge.getDLQ !== 'function') {
    res.status(503).json({ error: 'pipeline DLQ unavailable' });
    return null;
  }
  const dlq = bridge.getDLQ();
  if (!dlq) {
    res.status(503).json({ error: 'pipeline DLQ disabled' });
    return null;
  }
  return dlq;
}

function parseLimit(raw: unknown, def = 50, max = 200): number {
  if (typeof raw !== 'string') return def;
  const n = Number(raw);
  if (!Number.isFinite(n) || n < 0) return def;
  return Math.min(Math.floor(n), max);
}

function parseSinceMs(raw: unknown): number | undefined {
  if (typeof raw !== 'string') return undefined;
  const n = Number(raw);
  if (!Number.isFinite(n) || n < 0) return undefined;
  return n;
}

// Conservative shape: an error class name (e.g. `Error`, `NetworkError`,
// `RegistryConflictError`). Bounded length to keep the regex compile cheap;
// the inner whitelist also keeps the escape step a no-op for valid input.
const ERROR_KIND_RE = /^[A-Za-z][A-Za-z0-9_]{0,79}$/;

function parseErrorKind(raw: unknown): string | null {
  if (typeof raw !== 'string') return null;
  return ERROR_KIND_RE.test(raw) ? raw : null;
}

function escapeRegex(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

/**
 * Pull the error-class prefix out of a `lastError` formatted as
 * `${error.name}: ${error.message}` (the shape stored by
 * `eventBusDeadLetterHandler` in bootstrap.ts). Returns null when no
 * prefix is parseable so callers can render "unknown" without crashing.
 */
/**
 * Loop 29c (NFR #174): a dead letter belongs to the run its event names. Peek
 * takes read on that run; redrive / purge take the run's OWNER for every id
 * (a reader 403, anything else — unknown id, no run, a stranger's — 404).
 */
function dlqEntryAccess(mode: 'read' | 'owner') {
  return markAccess((req: Request, res: Response, next: NextFunction) => {
    void (async () => {
      const dlq = getPipelineBridge()?.getDLQ?.() ?? null;
      if (!dlq) { next(); return; } // the handler answers 503
      const ids = mode === 'read'
        ? [String((req.params as { id?: string }).id ?? '')]
        : (Array.isArray((req.body as { ids?: unknown } | undefined)?.ids) ? ((req.body as { ids: unknown[] }).ids.filter((x): x is string => typeof x === 'string')) : []);
      for (const id of ids) {
        const entry = await dlq.peek(id);
        // An unknown id is the handler's (peek 404, preview `notFound`, a no-op redrive).
        if (!entry) continue;
        try {
          await assertItemRunAccess(req, entry, mode);
        } catch (err) {
          if ((err as { status?: number }).status === 404) { res.status(404).json({ error: 'dlq entry not found', id }); return; }
          throw err;
        }
      }
      next();
    })().catch(next);
  }, mode === 'owner' ? 'run:owner' : 'run:read');
}

function deriveErrorKind(lastError: unknown): string | null {
  if (typeof lastError !== 'string') return null;
  const m = /^([A-Za-z][A-Za-z0-9_]{0,79}):/.exec(lastError);
  return m ? m[1] : null;
}

interface EnrichedDLQEntry {
  errorKind: string | null;
  [k: string]: unknown;
}

function enrichEntry(entry: unknown): EnrichedDLQEntry {
  const e = entry as { lastError?: unknown };
  return { ...(entry as object), errorKind: deriveErrorKind(e.lastError) };
}

function parseIdsBody(body: unknown): string[] | { error: string } {
  if (typeof body !== 'object' || body === null) {
    return { error: 'body.ids must be an array of strings' };
  }
  const ids = (body as { ids?: unknown }).ids;
  if (!Array.isArray(ids) || ids.some((x) => typeof x !== 'string')) {
    return { error: 'body.ids must be an array of strings' };
  }
  return ids as string[];
}

pipelineDLQRouter.get('/', asyncHandler(async (req, res) => {
  const dlq = dlqOr503(res);
  if (!dlq) return;
  const query = req.query as Record<string, unknown>;
  const limit = parseLimit(query.limit);
  const cursor = typeof query.cursor === 'string' ? query.cursor : undefined;
  const sinceMs = parseSinceMs(query.sinceMs);

  // ?errorKind=<class> — translates into a server-side regex against the
  // `lastError` prefix (the `${error.name}: ${error.message}` shape stored
  // by bootstrap). Reject malformed input rather than silently dropping the
  // filter, otherwise an operator typo returns the unfiltered firehose.
  let failureKindMatches: RegExp | undefined;
  if (typeof query.errorKind === 'string') {
    const kind = parseErrorKind(query.errorKind);
    if (!kind) {
      res.status(400).json({
        error: 'errorKind must match /^[A-Za-z][A-Za-z0-9_]{0,79}$/',
      });
      return;
    }
    failureKindMatches = new RegExp(`^${escapeRegex(kind)}:`);
  }

  const page = await dlq.list({
    limit,
    cursor,
    ...(sinceMs !== undefined ? { sinceMs } : {}),
    ...(failureKindMatches ? { failureKindMatches } : {}),
  });
  // Loop 29c (NFR #174): dead letters are every user's events — only those of
  // runs the caller may read (owner, or a reader of the run's document).
  const visible = await filterVisibleRunItems(req, page.items);
  res.status(200).json({
    ...page,
    items: visible.map((e) => enrichEntry(e)),
  });
}));

pipelineDLQRouter.get('/peek/:id', dlqEntryAccess('read'), asyncHandler(async (req, res) => {
  const dlq = dlqOr503(res);
  if (!dlq) return;
  const params = req.params as { id: string };
  const entry = await dlq.peek(params.id);
  if (!entry) {
    res.status(404).json({ error: 'dlq entry not found', id: params.id });
    return;
  }
  res.status(200).json(enrichEntry(entry));
}));

pipelineDLQRouter.post('/redrive', writeLimiter, dlqEntryAccess('owner'), asyncHandler(async (req, res) => {
  const dlq = dlqOr503(res);
  if (!dlq) return;
  const body = (req.body ?? {}) as { resetAttempts?: unknown; preview?: unknown };
  const parsed = parseIdsBody(req.body);
  if (!Array.isArray(parsed)) {
    res.status(400).json(parsed);
    return;
  }
  const resetAttempts = body.resetAttempts === true;

  if (body.preview === true) {
    const peeks = await Promise.all(parsed.map(async (id) => ({ id, entry: await dlq.peek(id) })));
    const wouldRedrive = peeks
      .filter((p) => p.entry !== null)
      .map((p) => enrichEntry(p.entry));
    const notFound = peeks
      .filter((p) => p.entry === null)
      .map((p) => ({ id: p.id }));
    res.status(200).json({ preview: true, resetAttempts, wouldRedrive, notFound });
    return;
  }

  const result = await dlq.redrive(parsed, { resetAttempts });
  res.status(200).json(result);
}));

pipelineDLQRouter.post('/purge', writeLimiter, dlqEntryAccess('owner'), asyncHandler(async (req, res) => {
  const dlq = dlqOr503(res);
  if (!dlq) return;
  const body = (req.body ?? {}) as { preview?: unknown };
  const parsed = parseIdsBody(req.body);
  if (!Array.isArray(parsed)) {
    res.status(400).json(parsed);
    return;
  }

  if (body.preview === true) {
    const peeks = await Promise.all(parsed.map(async (id) => ({ id, entry: await dlq.peek(id) })));
    const wouldPurge = peeks
      .filter((p) => p.entry !== null)
      .map((p) => enrichEntry(p.entry));
    const notFound = peeks
      .filter((p) => p.entry === null)
      .map((p) => ({ id: p.id }));
    res.status(200).json({ preview: true, wouldPurge, notFound });
    return;
  }

  const result = await dlq.purge(parsed);
  // PurgeResult is a count, not the ids; deleting a row that was never
  // there is a no-op, so forget every requested id.
  await forgetDeadLetters(getDeadLetterStore(), parsed);
  res.status(200).json(result);
}));
