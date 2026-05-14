import { Router } from 'express';
import { CORRELATION_ID_HEADER } from '@connorhoehn/service-runtime';
import { randomUUID } from 'crypto';
import { trace, SpanStatusCode, type Span } from '@opentelemetry/api';
import { asyncHandler, NotFoundError, ValidationError } from '../middleware/error-handler';
import { idempotency } from '../middleware/idempotency';
import { createRateLimiter } from '../middleware/rateLimit';
import { withContext } from '../lib/logger';
import {
  recordPipelineApproval,
  recordPipelineCancel,
  recordPipelineError,
  recordPipelineTrigger,
} from '../observability/metrics';
import { classifyPipelineError, type ErrorCategory } from '../pipeline/error-classification';
import { auditRepo, type AuditAction, type AuditDecision } from '../pipeline/audit-repository';
import { getPipelineBridge, type PendingApprovalRow } from '../pipeline/bridge-types';
import { definitionsRepo } from '../pipeline/definitions-repository';
import { getRunPersistenceService } from '../pipeline/runPersistence';
import { stubRunStore } from '../pipeline/stub-run-store';

export { classifyPipelineError, type ErrorCategory };
export {
  getPipelineBridge,
  setPipelineBridge,
  type BusEvent,
  type PipelineBridge,
  type PipelineBridgeMetrics,
  type PipelineRunSnapshot,
  type PendingApprovalRow,
} from '../pipeline/bridge-types';
export { stubRunStore, ACTIVE_STATUSES, type StubRunRecord, type StubRunStatus } from '../pipeline/stub-run-store';

const log = withContext({ route: 'pipelineTriggers' });
const tracer = trace.getTracer('platform-api');

function fireAndForgetAudit(
  event: Parameters<typeof auditRepo.record>[0],
): void {
  auditRepo.record(event).catch((err: unknown) => {
    const message = err instanceof Error ? err.message : String(err);
    log.warn(
      { action: event.action, pipelineId: event.pipelineId, runId: event.runId, err: message },
      'audit write failed',
    );
  });
}

async function withSpan<T>(
  name: string,
  attributes: Record<string, string | number | boolean | undefined>,
  fn: (span: Span) => Promise<T>,
): Promise<T> {
  return tracer.startActiveSpan(name, async (span) => {
    for (const [k, v] of Object.entries(attributes)) {
      if (v !== undefined) span.setAttribute(k, v);
    }
    try {
      const out = await fn(span);
      span.setStatus({ code: SpanStatusCode.OK });
      return out;
    } catch (err) {
      span.recordException(err as Error);
      span.setStatus({
        code: SpanStatusCode.ERROR,
        message: err instanceof Error ? err.message : String(err),
      });
      throw err;
    } finally {
      span.end();
    }
  });
}

// ---------------------------------------------------------------------------
// Per-user rate limit for POST /:pipelineId/runs.
//
// Defaults: 60 tokens capacity, 60 tokens / 60s refill (= 1 run/sec sustained
// with a 60-burst). Operators can tune via env without redeploying:
//
//   PIPELINE_RATELIMIT_CAPACITY        — burst size (tokens)
//   PIPELINE_RATELIMIT_REFILL_PER_MIN  — sustained tokens per 60s
//
// Mounted AFTER auth (so `req.user.sub` is populated) but BEFORE idempotency
// (so a 429 doesn't burn a fresh Idempotency-Key — the client can retry the
// same key once a token frees up). Rate-limit scope is per-userId; unauth'd
// flows fall back to req.ip.
// ---------------------------------------------------------------------------

const PIPELINE_RATELIMIT_CAPACITY = (() => {
  const raw = process.env.PIPELINE_RATELIMIT_CAPACITY;
  if (!raw) return 60;
  const n = Number(raw);
  return Number.isFinite(n) && n > 0 ? n : 60;
})();

const PIPELINE_RATELIMIT_REFILL_PER_MIN = (() => {
  const raw = process.env.PIPELINE_RATELIMIT_REFILL_PER_MIN;
  if (!raw) return 60;
  const n = Number(raw);
  return Number.isFinite(n) && n > 0 ? n : 60;
})();

const triggerRateLimiter = createRateLimiter({
  scope: 'pipeline-trigger',
  capacity: PIPELINE_RATELIMIT_CAPACITY,
  refillRate: PIPELINE_RATELIMIT_REFILL_PER_MIN,
  refillIntervalMs: 60_000,
  // Prefer the authed userId; fall back to ip for any pre-auth path that
  // might land here (defensive — `requireAuth` is global today).
  key: (req) => req.user?.sub ?? req.ip ?? 'anon',
});


// ---------------------------------------------------------------------------
// Request/response shapes.
// ---------------------------------------------------------------------------

export interface InlinePipelineDefinition {
  id: string;
  [key: string]: unknown;
}

export interface TriggerRunRequest {
  triggerPayload?: Record<string, unknown>;
  /**
   * Inline pipeline definition. Optional in Phase 1; required in Phase 4
   * (the distributed-core `createResource` call needs it inline so the
   * worker side doesn't have to read-after-write against pipeline storage).
   */
  definition?: InlinePipelineDefinition;
  /** Debug breakpoints: nodeIds where execution should pause before executing. */
  breakpoints?: string[];
}

export interface TriggerRunResponse {
  runId: string;
  pipelineId: string;
  triggeredBy: { userId: string; triggerType: string };
  at: string;
}

export interface ResolveApprovalRequest {
  stepId: string;
  decision: 'approve' | 'reject';
  comment?: string;
}

/** Structured 5xx response body when a bridge call throws. */
interface BridgeErrorBody {
  error: string;
  action: string;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Pull the correlation id (if any) off the request headers. */
function correlationIdOf(req: { headers: Record<string, unknown> }): string | undefined {
  const raw = req.headers[CORRELATION_ID_HEADER];
  if (typeof raw === 'string') return raw;
  if (Array.isArray(raw) && typeof raw[0] === 'string') return raw[0];
  return undefined;
}

/**
 * Standard error-tail for a failed bridge call: emit a Prometheus error
 * counter, write an audit row (fire-and-forget), log structured, and return
 * a 5xx with `{ error, action }`. Bridge-unavailable (null) → 503; everything
 * else → 500.
 */
function respondBridgeError(params: {
  res: { status(code: number): { json(body: BridgeErrorBody): unknown } };
  reqLog: ReturnType<typeof withContext>;
  err: unknown;
  action: string; // bridge method name (used in response body)
  auditAction: AuditAction;
  actorUserId: string;
  pipelineId: string;
  runId?: string;
  bridgeMissing?: boolean;
  details?: Record<string, unknown>;
}): void {
  const message = params.err instanceof Error ? params.err.message : String(params.err);
  const stack = params.err instanceof Error ? params.err.stack : undefined;
  const status = params.bridgeMissing ? 503 : 500;

  recordPipelineError(classifyPipelineError(message, stack));

  fireAndForgetAudit({
    action: params.auditAction,
    actorUserId: params.actorUserId,
    pipelineId: params.pipelineId,
    runId: params.runId,
    decision: 'failed',
    details: { error: message, ...(params.details ?? {}) },
  });

  params.reqLog.error(
    {
      action: params.action,
      auditAction: params.auditAction,
      pipelineId: params.pipelineId,
      runId: params.runId,
      status,
      err: message,
    },
    'pipeline bridge call failed',
  );

  params.res.status(status).json({ error: message, action: params.action });
}

// ---------------------------------------------------------------------------
// Triggers + run snapshot + history.
// `mergeParams` exposes :pipelineId from the parent mount.
// ---------------------------------------------------------------------------

export const pipelineTriggersRouter = Router({ mergeParams: true });

// POST /
pipelineTriggersRouter.post(
  '/',
  // Rate-limit BEFORE idempotency: a 429 must not consume an Idempotency-Key
  // entry, otherwise the client's retry would replay an empty cached response.
  triggerRateLimiter,
  idempotency({ scope: 'pipeline-trigger' }),
  asyncHandler<{ pipelineId: string }, TriggerRunResponse | BridgeErrorBody, TriggerRunRequest>(
    async (req, res) => {
      const { pipelineId } = req.params;
      const userId = req.user!.sub;
      const body = (req.body ?? {}) as TriggerRunRequest;
      const reqLog = log.child({
        correlationId: correlationIdOf(req as unknown as { headers: Record<string, unknown> }),
        userId,
        pipelineId,
      });

      if (body.definition && body.definition.id !== pipelineId) {
        throw new ValidationError(
          `definition.id (${body.definition.id}) does not match :pipelineId (${pipelineId})`,
        );
      }

      // Server-side definition resolution. The frontend used to ship the
      // definition inline from localStorage, which produced "No trigger node
      // in definition" failures whenever the local copy was stale or absent
      // (definitions edited in another browser/tab, or pipelines created
      // server-side that never made it into local storage). DDB is the
      // canonical store — fall back to it whenever the body lacks one.
      let resolvedDefinition = body.definition;
      if (!resolvedDefinition) {
        const fromDb = await definitionsRepo.get(userId, pipelineId);
        if (fromDb) resolvedDefinition = fromDb;
      }

      const bridge = getPipelineBridge();

      let runId: string;
      if (bridge?.trigger) {
        try {
          const out = await withSpan(
            'pipeline.trigger',
            { 'pipeline.id': pipelineId, 'pipeline.user_id': userId },
            async () =>
              Promise.resolve(
                bridge!.trigger!({
                  pipelineId,
                  definition: resolvedDefinition,
                  triggerPayload: body.triggerPayload,
                  triggeredBy: { userId },
                  breakpoints: body.breakpoints,
                }),
              ),
          );
          runId = out.runId;
          // Pre-populate the run cache so terminal events have a pipelineId
          // even if the framework rejects the definition before publishing
          // pipeline:run:started (e.g. "No trigger node in definition").
          getRunPersistenceService()?.registerTrigger({
            runId,
            pipelineId,
            userId,
            triggerType: 'manual',
            at: new Date().toISOString(),
          });
        } catch (err) {
          respondBridgeError({
            res,
            reqLog,
            err,
            action: 'trigger',
            auditAction: 'pipeline.trigger',
            actorUserId: userId,
            pipelineId,
          });
          return;
        }
      } else {
        runId = randomUUID();
        reqLog.info(
          { runId, action: 'trigger.stub' },
          'pipeline bridge not wired; synthesized runId',
        );
      }

      const at = new Date().toISOString();

      if (!bridge?.trigger) {
        stubRunStore.put({
          runId,
          pipelineId,
          userId,
          status: 'pending',
          triggeredAt: at,
          updatedAt: at,
          triggerPayload: body.triggerPayload,
        });
      }

      // Success-path observability.
      recordPipelineTrigger();
      fireAndForgetAudit({
        action: 'pipeline.trigger',
        actorUserId: userId,
        pipelineId,
        runId,
      });
      reqLog.info({ runId, action: 'trigger.create' }, 'pipeline triggered');

      const response: TriggerRunResponse = {
        runId,
        pipelineId,
        triggeredBy: { userId, triggerType: 'manual' },
        at,
      };

      res.status(202).json(response);
    },
  ),
);

// GET / — list past runs for a pipeline (paginated).
//
// Query: ?limit=NN&cursor=<runId>   (limit clamped to 1..100, default 20)
//
// When the bridge exposes `listRuns()` we forward to it; otherwise we read
// from the in-memory stubRunStore (filtered to the calling user).
pipelineTriggersRouter.get(
  '/',
  asyncHandler<
    { pipelineId: string },
    unknown,
    unknown,
    { limit?: string; cursor?: string }
  >(async (req, res) => {
    const { pipelineId } = req.params;
    const userId = req.user!.sub;
    const reqLog = log.child({
      correlationId: correlationIdOf(req as unknown as { headers: Record<string, unknown> }),
      userId,
      pipelineId,
    });

    const rawLimit = req.query.limit;
    let limit = 20;
    if (rawLimit !== undefined) {
      const n = parseInt(String(rawLimit), 10);
      if (!Number.isFinite(n) || n < 1 || n > 100) {
        throw new ValidationError('limit must be an integer between 1 and 100');
      }
      limit = n;
    }
    const cursor = req.query.cursor ? String(req.query.cursor) : undefined;

    const bridge = getPipelineBridge();
    if (bridge?.listRuns) {
      try {
        const out = await withSpan(
          'pipeline.listRuns',
          { 'pipeline.id': pipelineId, 'pipeline.limit': limit },
          async () => Promise.resolve(bridge!.listRuns!(pipelineId, { limit, cursor })),
        );
        res.json(out);
        return;
      } catch (err) {
        respondBridgeError({
          res: res as unknown as {
            status(code: number): { json(body: BridgeErrorBody): unknown };
          },
          reqLog,
          err,
          action: 'listRuns',
          auditAction: 'pipeline.trigger',
          actorUserId: userId,
          pipelineId,
        });
        return;
      }
    }
    const out = stubRunStore.listForPipeline(pipelineId, userId, { limit, cursor });
    res.json(out);
  }),
);

// GET /:runId — run snapshot
pipelineTriggersRouter.get(
  '/:runId',
  asyncHandler<{ pipelineId: string; runId: string }>(async (req, res) => {
    const { runId, pipelineId } = req.params;
    const userId = req.user!.sub;
    const reqLog = log.child({
      correlationId: correlationIdOf(req as unknown as { headers: Record<string, unknown> }),
      userId,
      pipelineId,
      runId,
    });

    const bridge = getPipelineBridge();
    if (!bridge) {
      throw new NotFoundError('run not found');
    }

    let snap;
    try {
      snap = await withSpan(
        'pipeline.getRun',
        { 'pipeline.id': pipelineId, 'pipeline.run_id': runId },
        async () => Promise.resolve(bridge!.getRun(runId)),
      );
    } catch (err) {
      respondBridgeError({
        res: res as unknown as {
          status(code: number): { json(body: BridgeErrorBody): unknown };
        },
        reqLog,
        err,
        action: 'getRun',
        auditAction: 'pipeline.trigger',
        actorUserId: userId,
        pipelineId,
        runId,
      });
      return;
    }

    if (!snap) {
      throw new NotFoundError('run not found');
    }
    res.json(snap);
  }),
);

// GET /:runId/history?fromVersion=0
pipelineTriggersRouter.get(
  '/:runId/history',
  asyncHandler<{ pipelineId: string; runId: string }, unknown, unknown, { fromVersion?: string }>(
    async (req, res) => {
      const { runId, pipelineId } = req.params;
      const userId = req.user!.sub;
      const reqLog = log.child({
        correlationId: correlationIdOf(req as unknown as { headers: Record<string, unknown> }),
        userId,
        pipelineId,
        runId,
      });

      const rawFromVersion = req.query.fromVersion;
      const fromVersion = rawFromVersion ? parseInt(String(rawFromVersion), 10) : 0;
      if (!Number.isFinite(fromVersion) || fromVersion < 0) {
        throw new ValidationError('fromVersion must be a non-negative integer');
      }
      const bridge = getPipelineBridge();
      if (!bridge) {
        // Per handoff contract: [] when bus has no walFilePath configured.
        res.json([]);
        return;
      }

      try {
        const events = await withSpan(
          'pipeline.getHistory',
          { 'pipeline.id': pipelineId, 'pipeline.run_id': runId, 'pipeline.from_version': fromVersion },
          async () => Promise.resolve(bridge!.getHistory(runId, fromVersion)),
        );
        res.json(events ?? []);
      } catch (err) {
        respondBridgeError({
          res: res as unknown as {
            status(code: number): { json(body: BridgeErrorBody): unknown };
          },
          reqLog,
          err,
          action: 'getHistory',
          auditAction: 'pipeline.trigger',
          actorUserId: userId,
          pipelineId,
          runId,
        });
      }
    },
  ),
);

// ---------------------------------------------------------------------------
// Active runs — GET /api/pipelines/runs/active
// Mounted separately from the runs router (no :pipelineId in the path).
// ---------------------------------------------------------------------------

export const pipelineActiveRunsRouter = Router();

pipelineActiveRunsRouter.get(
  '/',
  asyncHandler(async (req, res) => {
    const userId = req.user!.sub;
    const reqLog = log.child({
      correlationId: correlationIdOf(req as unknown as { headers: Record<string, unknown> }),
      userId,
    });

    const bridge = getPipelineBridge();
    if (bridge?.listActiveRuns) {
      try {
        const runs = await withSpan(
          'pipeline.listActiveRuns',
          { 'pipeline.user_id': userId },
          async () => Promise.resolve(bridge!.listActiveRuns!()),
        );
        res.json({ runs });
        return;
      } catch (err) {
        respondBridgeError({
          res: res as unknown as {
            status(code: number): { json(body: BridgeErrorBody): unknown };
          },
          reqLog,
          err,
          action: 'listActiveRuns',
          auditAction: 'pipeline.trigger',
          actorUserId: userId,
          pipelineId: '*',
        });
        return;
      }
    }
    const runs = stubRunStore.listActive(userId);
    res.json({ runs });
  }),
);

// ---------------------------------------------------------------------------
// Cancel — POST /api/pipelines/:runId/cancel
// Returns 200 with the updated record on success, 404 when the run doesn't
// exist (or isn't owned by the caller), 409 when the run is already in a
// terminal state.
// ---------------------------------------------------------------------------

export const pipelineCancelRouter = Router({ mergeParams: true });

interface CancelRunRequest {
  reason?: string;
}

pipelineCancelRouter.post(
  '/',
  idempotency({ scope: 'pipeline-cancel' }),
  asyncHandler<{ runId: string }, unknown, CancelRunRequest>(async (req, res) => {
    const { runId } = req.params;
    const userId = req.user!.sub;
    const body = (req.body ?? {}) as CancelRunRequest;
    const reqLog = log.child({
      correlationId: correlationIdOf(req as unknown as { headers: Record<string, unknown> }),
      userId,
      runId,
    });

    if (body.reason !== undefined && typeof body.reason !== 'string') {
      throw new ValidationError('reason must be a string when present');
    }

    const bridge = getPipelineBridge();
    if (bridge?.cancelRun) {
      // Best-effort pipelineId derivation for audit/metrics: if the bridge
      // can hand us the snapshot we read it AFTER the cancel so we know
      // which pipeline this belonged to. Falls back to '*' on failure.
      let pipelineIdForAudit = '*';

      try {
        await withSpan(
          'pipeline.cancelRun',
          { 'pipeline.run_id': runId, 'pipeline.user_id': userId },
          async () => Promise.resolve(bridge!.cancelRun!(runId, userId)),
        );
      } catch (err) {
        respondBridgeError({
          res: res as unknown as {
            status(code: number): { json(body: BridgeErrorBody): unknown };
          },
          reqLog,
          err,
          action: 'cancelRun',
          auditAction: 'pipeline.cancel',
          actorUserId: userId,
          pipelineId: pipelineIdForAudit,
          runId,
          details: { reason: body.reason },
        });
        return;
      }

      // Bridge owns the truth in Phase 4; we surface the same shape as the
      // stub branch by reading back via getRun. The PipelineBridge interface
      // declares `getRun` non-optional, but partial test bridges (and older
      // phase-1 bridges) may omit it — defensively pull it via dynamic
      // accessor so we behave the same as before.
      const getRunFn = bridge.getRun?.bind(bridge);
      if (getRunFn) {
        let snap;
        try {
          snap = await withSpan(
            'pipeline.getRun',
            { 'pipeline.run_id': runId },
            async () => Promise.resolve(getRunFn(runId)),
          );
        } catch (err) {
          respondBridgeError({
            res: res as unknown as {
              status(code: number): { json(body: BridgeErrorBody): unknown };
            },
            reqLog,
            err,
            action: 'getRun',
            auditAction: 'pipeline.cancel',
            actorUserId: userId,
            pipelineId: pipelineIdForAudit,
            runId,
          });
          return;
        }

        if (!snap) {
          throw new NotFoundError('run not found');
        }
        if (typeof snap.pipelineId === 'string') pipelineIdForAudit = snap.pipelineId;

        recordPipelineCancel();
        fireAndForgetAudit({
          action: 'pipeline.cancel',
          actorUserId: userId,
          pipelineId: pipelineIdForAudit,
          runId,
          decision: 'cancelled',
          details: body.reason ? { reason: body.reason } : undefined,
        });
        reqLog.info({ runId, pipelineId: pipelineIdForAudit, action: 'cancel' }, 'pipeline cancelled');

        res.json(snap);
        return;
      }

      // Bridge doesn't expose getRun; we still emit metrics + audit on the
      // fact that cancelRun returned successfully.
      recordPipelineCancel();
      fireAndForgetAudit({
        action: 'pipeline.cancel',
        actorUserId: userId,
        pipelineId: pipelineIdForAudit,
        runId,
        decision: 'cancelled',
        details: body.reason ? { reason: body.reason } : undefined,
      });
      reqLog.info({ runId, action: 'cancel' }, 'pipeline cancelled');

      res.json({ runId, status: 'canceled' });
      return;
    }

    // Stub branch — no bridge wired.
    const updated = stubRunStore.cancel(runId, userId, body.reason);
    if (!updated) {
      // Distinguish missing-vs-terminal so the MCP layer can surface a
      // useful error. We probe the store again to figure out which case.
      const existing = stubRunStore.get(runId);
      if (!existing || existing.userId !== userId) {
        throw new NotFoundError('run not found');
      }
      throw new ValidationError(
        `run ${runId} is in terminal state '${existing.status}' and cannot be canceled`,
      );
    }

    recordPipelineCancel();
    fireAndForgetAudit({
      action: 'pipeline.cancel',
      actorUserId: userId,
      pipelineId: updated.pipelineId,
      runId,
      decision: 'cancelled',
      details: body.reason ? { reason: body.reason } : undefined,
    });
    reqLog.info(
      { runId, pipelineId: updated.pipelineId, action: 'cancel.stub' },
      'pipeline cancelled (stub)',
    );

    res.json(updated);
  }),
);

// ---------------------------------------------------------------------------
// Pending approvals queue — GET /api/pipelines/approvals
//
// Surfaces `bridge.getPendingApprovals()` to the frontend so the approvals
// inbox can render across all runs without per-run polling. Mounted at the
// static `/pipelines/approvals` path (NOT under `:pipelineId` and NOT under
// `:runId/approvals`) — see routes/index.ts ordering note.
//
// Behaviour:
//   - Bridge wired + getPendingApprovals defined → forward.
//   - Bridge null OR getPendingApprovals undefined → return { approvals: [] }
//     with HTTP 200 (an empty queue is a valid answer in stub mode; we
//     deliberately do NOT 503 since the frontend treats this as data, not
//     a health probe).
//   - Optional `?userId=` query param: when present, filter to rows where
//     the requesting user appears in `approvers`. Default is no filter —
//     the frontend can do its own visual filtering across teammates.
//   - Each returned row is enriched with `runContext` (the run's accumulated
//     context map) and `runSteps` (a stepId → { output } projection of the
//     run's steps map) so the FE approval card can render upstream LLM
//     responses without an N+1 per-row `getPipelineRun` round-trip. The
//     enrichment is best-effort: if `bridge.getRun(runId)` throws or returns
//     null we just emit the row without the enrichment fields rather than
//     failing the whole queue request.
// ---------------------------------------------------------------------------

/**
 * Wider row returned by GET /api/pipelines/approvals — extends the bridge's
 * canonical `PendingApprovalRow` with the run-side context that the approval
 * card uses to render "what am I being asked to review". Both are optional:
 * when the bridge can't surface a run snapshot (older bridge / run already
 * evicted) the row still includes the queue-level fields and the FE falls
 * back to its empty-state banner.
 */
export interface PendingApprovalRowWithContext extends PendingApprovalRow {
  /**
   * The run's accumulated context map at the moment of the queue read. Same
   * shape as `PipelineRunSnapshot['context']`. Omitted if `bridge.getRun`
   * throws / returns null.
   */
  runContext?: Record<string, unknown>;
  /**
   * Per-step output projection — `{ [stepId]: { output } }`. We project to
   * `{ output }` only (rather than the full StepExecution) to keep the payload
   * narrow; the only field the approval card consumes today is each step's
   * `output.response` (LLM steps) and other free-form output values.
   */
  runSteps?: Record<string, { output?: unknown }>;
}

export const pipelinePendingApprovalsRouter = Router();

pipelinePendingApprovalsRouter.get(
  '/',
  asyncHandler<unknown, { approvals: PendingApprovalRowWithContext[] } | BridgeErrorBody, unknown, { userId?: string }>(
    async (req, res) => {
      const userId = req.user?.sub ?? 'anon';
      const reqLog = log.child({
        correlationId: correlationIdOf(req as unknown as { headers: Record<string, unknown> }),
        userId,
      });

      const bridge = getPipelineBridge();
      if (!bridge || !bridge.getPendingApprovals) {
        res.json({ approvals: [] });
        return;
      }

      let rows: PendingApprovalRow[];
      try {
        rows = await withSpan(
          'pipeline.getPendingApprovals',
          { 'pipeline.user_id': userId },
          async () => Promise.resolve(bridge!.getPendingApprovals!()),
        );
      } catch (err) {
        respondBridgeError({
          res: res as unknown as {
            status(code: number): { json(body: BridgeErrorBody): unknown };
          },
          reqLog,
          err,
          action: 'getPendingApprovals',
          auditAction: 'pipeline.approve',
          actorUserId: userId,
          pipelineId: '*',
        });
        return;
      }

      const filterUserId = req.query.userId ? String(req.query.userId) : undefined;
      const filtered = filterUserId
        ? rows.filter((row) =>
            row.approvers.some(
              (a) => a.type === 'user' && (a.value === filterUserId || a.userId === filterUserId),
            ),
          )
        : rows;

      // Enrich each row with the run snapshot's context + step outputs so
      // the approval card has the upstream LLM response inline. De-dupe by
      // runId so a single queue read doing the rare N approvals on N
      // distinct runs only does N getRun calls (and fan-in approvals on the
      // same runId only do 1).
      const approvals: PendingApprovalRowWithContext[] = await enrichWithRunContext(
        filtered,
        bridge,
        reqLog,
      );

      res.json({ approvals });
    },
  ),
);

/**
 * Best-effort hydration of `runContext` + `runSteps` onto each pending-approval
 * row. Failures per-row are swallowed (logged at warn) — the approval queue
 * itself must keep rendering even if one run snapshot is unavailable.
 */
async function enrichWithRunContext(
  rows: PendingApprovalRow[],
  bridge: NonNullable<ReturnType<typeof getPipelineBridge>>,
  reqLog: { warn: (obj: Record<string, unknown>, msg: string) => void },
): Promise<PendingApprovalRowWithContext[]> {
  if (rows.length === 0) return [];
  // Multiple approval rows on the same run share one snapshot fetch.
  const runIds = Array.from(new Set(rows.map((r) => r.runId)));
  const snapshots = new Map<
    string,
    { context?: Record<string, unknown>; steps?: Record<string, { output?: unknown }> }
  >();
  await Promise.all(
    runIds.map(async (runId) => {
      try {
        const snap = await Promise.resolve(bridge.getRun(runId));
        if (!snap) return;
        // Project steps → { output } only; the FE doesn't read other fields
        // on this surface and we'd rather not ship unbounded approval+llm
        // metadata over the wire on the queue endpoint.
        const stepsProjection: Record<string, { output?: unknown }> = {};
        const rawSteps = (snap as { steps?: Record<string, { output?: unknown }> }).steps;
        if (rawSteps && typeof rawSteps === 'object') {
          for (const [stepId, step] of Object.entries(rawSteps)) {
            if (step && typeof step === 'object' && 'output' in step) {
              stepsProjection[stepId] = { output: step.output };
            }
          }
        }
        snapshots.set(runId, {
          context: (snap as { context?: Record<string, unknown> }).context,
          steps: stepsProjection,
        });
      } catch (err) {
        reqLog.warn(
          { runId, err: err instanceof Error ? err.message : String(err) },
          'getRun for pending-approval enrichment failed; row will omit runContext/runSteps',
        );
      }
    }),
  );
  return rows.map((row) => {
    const snap = snapshots.get(row.runId);
    if (!snap) return row;
    return {
      ...row,
      ...(snap.context ? { runContext: snap.context } : {}),
      ...(snap.steps ? { runSteps: snap.steps } : {}),
    };
  });
}

// ---------------------------------------------------------------------------
// Approvals — POST /api/pipelines/:runId/approvals
// Mounted separately from the runs router so the path matches the spec.
// ---------------------------------------------------------------------------

export const pipelineApprovalsRouter = Router({ mergeParams: true });

pipelineApprovalsRouter.post(
  '/',
  idempotency({ scope: 'pipeline-approval' }),
  asyncHandler<{ runId: string }, unknown, ResolveApprovalRequest>(async (req, res) => {
    const { runId } = req.params;
    const userId = req.user!.sub;
    const body = (req.body ?? {}) as ResolveApprovalRequest;
    const reqLog = log.child({
      correlationId: correlationIdOf(req as unknown as { headers: Record<string, unknown> }),
      userId,
      runId,
    });

    if (!body.stepId || typeof body.stepId !== 'string') {
      throw new ValidationError('stepId is required');
    }
    if (body.decision !== 'approve' && body.decision !== 'reject') {
      throw new ValidationError("decision must be 'approve' or 'reject'");
    }
    if (body.comment !== undefined && typeof body.comment !== 'string') {
      throw new ValidationError('comment must be a string when present');
    }

    const bridge = getPipelineBridge();
    if (!bridge) {
      reqLog.info(
        {
          runId,
          stepId: body.stepId,
          decision: body.decision,
          action: 'approval.stub',
        },
        'pipeline bridge not wired; accepted approval (stub)',
      );
      res.status(204).end();
      return;
    }

    let pipelineIdForAudit = '*';
    if (bridge.getRun) {
      try {
        const snap = await Promise.resolve(bridge.getRun(runId));
        if (snap && typeof snap.pipelineId === 'string') {
          pipelineIdForAudit = snap.pipelineId;
        }
      } catch (err) {
        reqLog.warn(
          { runId, err: err instanceof Error ? err.message : String(err) },
          'getRun for approval audit-context lookup failed; continuing',
        );
      }
    }

    // Authorization: if bridge exposes getPendingApprovals, verify the
    // requesting user is listed as a user-type approver for this step.
    // Role-type entries cannot be validated at the gateway (no role resolver),
    // so they always pass through. Older bridges without getPendingApprovals
    // also pass through — the bridge enforces its own rules.
    //
    // In dev (SKIP_AUTH=true) the approver UUID baked into the pipeline
    // definition is rarely the operator's own dev userId, so the check
    // below would 403 every legitimate dev approval. Bypass it.
    if (bridge.getPendingApprovals && process.env.SKIP_AUTH !== 'true') {
      let rows: PendingApprovalRow[] = [];
      try {
        rows = await Promise.resolve(bridge.getPendingApprovals());
      } catch (err) {
        reqLog.warn(
          { runId, err: err instanceof Error ? err.message : String(err) },
          'getPendingApprovals for authz lookup failed; continuing',
        );
      }
      const pending = rows.find((r) => r.runId === runId && r.stepId === body.stepId);
      if (pending) {
        const userApprovers = pending.approvers.filter((a) => a.type === 'user');
        if (userApprovers.length > 0) {
          const isAuthorized = userApprovers.some(
            (a) => a.value === userId || a.userId === userId,
          );
          if (!isAuthorized) {
            reqLog.warn(
              { runId, stepId: body.stepId, action: 'approval.authz.denied' },
              'user not in approvers list — 403',
            );
            res.status(403).json({ error: 'not authorized to resolve this approval step' });
            return;
          }
        }
      }
    }

    try {
      await withSpan(
        'pipeline.resolveApproval',
        {
          'pipeline.run_id': runId,
          'pipeline.step_id': body.stepId,
          'pipeline.decision': body.decision,
        },
        async () =>
          Promise.resolve(
            bridge!.resolveApproval(runId, body.stepId, userId, body.decision, body.comment),
          ),
      );
    } catch (err) {
      respondBridgeError({
        res: res as unknown as {
          status(code: number): { json(body: BridgeErrorBody): unknown };
        },
        reqLog,
        err,
        action: 'resolveApproval',
        auditAction: body.decision === 'approve' ? 'pipeline.approve' : 'pipeline.reject',
        actorUserId: userId,
        pipelineId: pipelineIdForAudit,
        runId,
        details: { stepId: body.stepId, decision: body.decision },
      });
      return;
    }

    // Success-path observability.
    recordPipelineApproval();
    const auditAction: AuditAction =
      body.decision === 'approve' ? 'pipeline.approve' : 'pipeline.reject';
    const auditDecision: AuditDecision =
      body.decision === 'approve' ? 'approved' : 'rejected';
    fireAndForgetAudit({
      action: auditAction,
      actorUserId: userId,
      pipelineId: pipelineIdForAudit,
      runId,
      decision: auditDecision,
      details: body.comment ? { stepId: body.stepId, comment: body.comment } : { stepId: body.stepId },
    });
    reqLog.info(
      {
        runId,
        stepId: body.stepId,
        decision: body.decision,
        pipelineId: pipelineIdForAudit,
        action: 'approval.resolve',
      },
      'pipeline approval resolved',
    );

    res.status(204).end();
  }),
);

// ---------------------------------------------------------------------------
// Debug breakpoint resolve — POST /api/pipelines/:pipelineId/runs/:runId/breakpoint/:stepId/resolve
//
// Resumes execution at a paused breakpoint. Body: { action?: 'continue' | 'resume_all' }
// Returns 204 on success, 503 when bridge not wired, 400 on bad input.
// ---------------------------------------------------------------------------

export const pipelineBreakpointRouter = Router({ mergeParams: true });

interface ResolveBreakpointRequest {
  action?: 'continue' | 'step_over' | 'resume_all';
}

pipelineBreakpointRouter.post(
  '/',
  asyncHandler<
    { pipelineId: string; runId: string; stepId: string },
    unknown,
    ResolveBreakpointRequest
  >(async (req, res) => {
    const { runId, stepId, pipelineId } = req.params;
    const userId = req.user!.sub;
    const body = (req.body ?? {}) as ResolveBreakpointRequest;
    const reqLog = log.child({
      correlationId: correlationIdOf(req as unknown as { headers: Record<string, unknown> }),
      userId,
      runId,
      stepId,
      pipelineId,
    });

    const action = body.action ?? 'continue';
    if (action !== 'continue' && action !== 'step_over' && action !== 'resume_all') {
      throw new ValidationError("action must be 'continue', 'step_over', or 'resume_all'");
    }

    const bridge = getPipelineBridge();
    if (!bridge?.resolveBreakpoint) {
      respondBridgeError({
        res: res as unknown as {
          status(code: number): { json(body: BridgeErrorBody): unknown };
        },
        reqLog,
        err: new Error('pipeline bridge not wired'),
        action: 'resolveBreakpoint',
        auditAction: 'pipeline.breakpoint.resolve',
        actorUserId: userId,
        pipelineId,
        runId,
        bridgeMissing: true,
      });
      return;
    }

    try {
      bridge.resolveBreakpoint(runId, stepId, action);
    } catch (err) {
      respondBridgeError({
        res: res as unknown as {
          status(code: number): { json(body: BridgeErrorBody): unknown };
        },
        reqLog,
        err,
        action: 'resolveBreakpoint',
        auditAction: 'pipeline.breakpoint.resolve',
        actorUserId: userId,
        pipelineId,
        runId,
      });
      return;
    }

    reqLog.info(
      { runId, stepId, action, pipelineId },
      'breakpoint resolved',
    );

    fireAndForgetAudit({
      action: 'pipeline.breakpoint.resolve',
      actorUserId: userId,
      pipelineId,
      runId,
      outcome: 'success',
      details: { stepId, breakpointAction: action },
    });

    res.status(204).end();
  }),
);
