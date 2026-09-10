// platform-api/src/pipeline/createBridge.ts
//
// Wraps a live distributed-core `PipelineModule` instance behind the
// `PipelineBridge` shape that platform-api routes consume. This is the boundary
// where Phase-4 wiring meets the existing route stub paths — when a bridge is
// installed, every route's `if (bridge?.X)` branch hits this implementation;
// otherwise the in-memory stubRunStore stays in charge.
//
// Exposes the six surfaces locked with the distributed-core sibling:
//   1. getRun        → PipelineModule.getRun
//   2. getHistory    → PipelineModule.getHistory
//   3. listActiveRuns→ PipelineModule.listActiveRuns (mapped to PipelineRunSnapshot)
//   4. getMetrics    → PipelineModule.getMetrics() — full pass-through of the
//                       dashboard fields the module exposes (runsStarted/-
//                       Completed/-Failed/-Active, runsAwaitingApproval,
//                       avgDurationMs, llmTokensIn/Out, avgFirstTokenLatencyMs
//                       on v0.3.7+, asOf). Missing fields stay missing so the
//                       route can map them to `null`.
//   5. getPendingApprovals → PipelineModule.getPendingApprovals
//   6. cancelRun     → PipelineModule.cancelRun
// + trigger          → PipelineModule.createResource (for pipelineTriggers POST)
// + resolveApproval  → PipelineModule.resolveApproval
//
// `pipeline.run.reassigned` (the 6th locked surface, an event not a method)
// is consumed via `module.getEventBus().subscribe(...)` from the gateway-side
// bridge in src/pipeline-bridge/pipeline-bridge.js — not here.

import type {
  PublicBusEvent,
  BusEvent as DCBusEvent,
} from 'distributed-core';
import type {
  PipelineModule,
  StepExecution,
} from 'distributed-core/applications/pipeline';
import type {
  Envelope,
  LeaseLike,
  DLQEntryLike,
  QueueInspector,
  DeadLetterQueue,
} from 'distributed-core/messaging-queue';
import { wrap, InMemoryQueueInspector } from 'distributed-core/messaging-queue';
// hub#2226 — see bridge-types.ts for why these aren't imported from
// distributed-core (the v0.14.2 pin predates the type re-exports).
import { normalizeRunStatus } from '@connorhoehn/pipeline-runtime';
import type { ContextPatch, PatchResult } from './bridge-types';
import type {
  BusEvent,
  PipelineBridge,
  PipelineBridgeMetrics,
  PipelineRunSnapshot,
  PendingApprovalRow,
} from '../routes/pipelineTriggers';
import { classifyPipelineError } from '../routes/pipelineTriggers';
import { classifyRetryability } from '../pipeline/error-classification';
import type {
  PipelineRunRepository,
  PipelineRunItem,
  StepSummary,
} from '../repositories/PipelineRunRepository';
import { readAttempts } from '../repositories/PipelineRunRepository';

// ---------------------------------------------------------------------------
// T1 (lib-expansion-3) boundary adapters — keep the wider pipeline shape
// untouched. We wrap PipelineRunSnapshot only at the Inspector / DLQ surfaces
// where operator-grade introspection wants the standard Envelope contract.
// ---------------------------------------------------------------------------

/**
 * Wrap a `PipelineRunSnapshot` in a T1 `Envelope` for inspector consumption.
 * The envelope id is the run id (stable, lexicographically reasonable). The
 * `enqueuedAtMs` is derived from `startedAt` when present so age-of-oldest
 * pending matches the dashboard expectation; falls back to `Date.now()` when
 * absent (a snapshot without a startedAt is effectively just-now).
 */
export function asEnvelope(run: PipelineRunSnapshot): Envelope<PipelineRunSnapshot> {
  const startedAtRaw = typeof run.startedAt === 'string' ? run.startedAt : undefined;
  const startedAtMs = startedAtRaw ? new Date(startedAtRaw).getTime() : NaN;
  const enqueuedAtMs = Number.isFinite(startedAtMs) ? startedAtMs : Date.now();
  return wrap(run, {
    id: run.runId,
    now: () => enqueuedAtMs,
    attemptCount: 0,
  });
}

/**
 * Adapt a hypothetical lease (PipelineRunSnapshot + lease metadata) to the
 * T13 LeaseLike shape. We don't actually have leases for the in-process
 * single-node executor today — this exists for symmetry with the inspector
 * contract and as the seam Phase 4 multi-node will wire through. Until then
 * the inspector's `inflight` provider returns an empty array.
 */
export function leaseAsLeaseLike(
  run: PipelineRunSnapshot,
  workerId: string,
  leasedAtMs: number,
  leaseExpiresAtMs: number,
): LeaseLike<PipelineRunSnapshot> {
  return {
    envelope: asEnvelope(run),
    workerId,
    leasedAtMs,
    leaseExpiresAtMs,
  };
}

/** Coerce a value to a finite number, or return `undefined` (so the route
 *  surfaces it as `null`). Strings/NaN/Infinity are NOT silently parsed — the
 *  bridge's job is pass-through, not numeric salvage. */
function asFiniteNumber(v: unknown): number | undefined {
  return typeof v === 'number' && Number.isFinite(v) ? v : undefined;
}

/**
 * Map persisted `stepsSummary` (Record<nodeId, StepSummary>) to the
 * `Record<nodeId, StepExecution>` shape consumed by frontend run views.
 * Synthesises `nodeId` from the key, propagates `attempts[]` (or back-fills a
 * single synthetic attempt for legacy rows via `readAttempts`).
 */
function summariesToStepExecutions(
  summaries: Record<string, StepSummary> | undefined,
  runId: string,
): Record<string, StepExecution> {
  if (!summaries) return {};
  const out: Record<string, StepExecution> = {};
  for (const [nodeId, summary] of Object.entries(summaries)) {
    const attempts = readAttempts(summary, runId, nodeId);
    out[nodeId] = {
      nodeId,
      status: (summary.status ?? 'completed') as StepExecution['status'],
      startedAt: summary.startedAt,
      completedAt: summary.completedAt,
      durationMs: summary.durationMs,
      output: summary.output,
      error: summary.error,
      ...(attempts.length > 0 ? { attempts } : {}),
    } as StepExecution;
  }
  return out;
}

function parseStoredError(stored: string): { message: string; nodeId: string; errorCategory: string; retryability: string } {
  const colonIdx = stored.indexOf(': ');
  const nodeId = colonIdx > 0 ? stored.slice(0, colonIdx) : 'unknown';
  const message = colonIdx > 0 ? stored.slice(colonIdx + 2) : stored;
  const errorCategory = classifyPipelineError(message);
  const retryability = classifyRetryability(errorCategory, message);
  return { message, nodeId, errorCategory, retryability };
}

// PipelineModule.getRun returns a `PipelineRun` shape from distributed-core's
// internal types. That shape uses `id` (not `runId`) as the run identifier,
// while the `applicationData` envelope produced by `listActiveRuns` uses
// `runId`. Both arrive here, so we accept either spelling and project to the
// `runId`-keyed `PipelineRunSnapshot` the platform-api routes consume.
//
// Returns a SHALLOW COPY rather than mutating the input — the live
// `PipelineRun` from `module.getRun()` is the executor's own object, and
// downstream callers (HTTP serialization, JSON.stringify) must not be able
// to mutate executor state through the snapshot reference.
//
// Error classification: distributed-core emits `PipelineRunError` with
// `message` and `stack` but no structured category. We add `errorCategory`
// + `retryability` here so operators can triage failures (NetworkError vs
// TimeoutError vs LLMError) without parsing stack traces.
/**
 * Map a persisted `PipelineRunItem` onto the wire snapshot.
 *
 * Exported because the run-DETAIL route needs the exact same mapping for its
 * repository fallback: `bridge.getRun()` only ever sees runs the CURRENT
 * process executed, so after a restart every earlier run 404'd from detail
 * while still listing fine (the list path has always read the repository).
 * Two mappings would drift, and this one already carries the legacy-status
 * folding that keeps list and detail agreeing.
 */
export function runItemToSnapshot(item: PipelineRunItem): PipelineRunSnapshot {
  return {
    runId: item.runId,
    pipelineId: item.pipelineId,
    // Fold legacy spellings onto the canonical union. New writes are already
    // canonical (runPersistence), but rows persisted before that fix still
    // carry `succeeded`/`canceled`, and this endpoint must agree with the
    // detail endpoint for every run — historical ones included.
    status: normalizeRunStatus(item.status),
    startedAt: item.startedAt,
    completedAt: item.completedAt,
    // Hub#1282: surface interruptedAt so the UI can render a "Resume" affordance.
    ...(item.interruptedAt ? { interruptedAt: item.interruptedAt } : {}),
    steps: summariesToStepExecutions(item.stepsSummary, item.runId),
    error: item.error ? parseStoredError(item.error) : undefined,
    // Approval rejection detail — who rejected, when, and why. Carried on the
    // LIST shape too, so the Runs table can name the rejector without opening
    // the run (and without a live executor, which historical runs don't have).
    ...(item.rejection ? { rejection: item.rejection } : {}),
    // DLQ redrive lineage, both directions.
    ...(item.redriveOf ? { redriveOf: item.redriveOf } : {}),
    ...(item.redrivenBy && item.redrivenBy.length > 0 ? { redrivenBy: item.redrivenBy } : {}),
    triggeredBy: {
      userId: item.triggeredBy === 'system' ? undefined : item.triggeredBy,
      triggerType: item.triggerType ?? 'manual',
      payload: {},
    },
    triggerType: item.triggerType,
  };
}

function toRunSnapshot(run: unknown): PipelineRunSnapshot | null {
  if (!run || typeof run !== 'object') return null;
  const r = run as Record<string, unknown>;

  // Accept either `runId` (snapshot/applicationData shape) or `id` (live
  // PipelineRun shape from PipelineExecutor.getCurrentRun()).
  const runId =
    typeof r['runId'] === 'string'
      ? (r['runId'] as string)
      : typeof r['id'] === 'string'
        ? (r['id'] as string)
        : undefined;
  if (runId === undefined || typeof r['pipelineId'] !== 'string') return null;

  // Shallow copy so we never mutate the executor's live run object.
  const snap: Record<string, unknown> = { ...r, runId };

  // Classify error if present (on the copy, never the input).
  const error = r['error'] as { message?: string; stack?: string; nodeId?: string } | undefined;
  if (error && typeof error.message === 'string') {
    const errorCategory = classifyPipelineError(error.message, error.stack);
    const retryability = classifyRetryability(errorCategory, error.message);
    snap['error'] = { ...error, errorCategory, retryability };
  }

  return snap as unknown as PipelineRunSnapshot;
}

export function createBridge(
  module: PipelineModule,
  dlq: DeadLetterQueue<DCBusEvent> | null = null,
  pipelineRunRepository?: PipelineRunRepository,
): PipelineBridge {
  // Snapshot helper for the inspector — same shape the route's
  // `listActiveRuns` returns, but extracted here so the inspector closure
  // and the bridge method stay in sync.
  function listActiveRunSnapshots(): PipelineRunSnapshot[] {
    const resources = module.listActiveRuns();
    const out: PipelineRunSnapshot[] = [];
    for (const r of resources) {
      const ad = (r as { applicationData?: unknown }).applicationData;
      const snap = toRunSnapshot(ad);
      if (snap) out.push(snap);
    }
    return out;
  }

  // Inspector is a singleton over the bridge — providers are called fresh on
  // every inspector method so it always sees current state. `inflight` and
  // `dlq` providers return empty until the matching wiring (single-node has
  // no leases today; the DLQ wiring lands in the next adoption step).
  let inspector: QueueInspector<PipelineRunSnapshot> | null = null;

  return {
    async trigger({ pipelineId, definition, triggerPayload, triggeredBy, breakpoints }) {
      const resource = await module.createResource({
        applicationData: {
          definition,
          triggerPayload,
          triggeredBy,
          pipelineId,
          breakpoints,
        },
      });
      const data = (resource.applicationData ?? {}) as Record<string, unknown>;
      const runId = data['runId'];
      if (typeof runId !== 'string') {
        throw new Error('PipelineModule.createResource did not return a runId');
      }
      return { runId };
    },

    getRun(runId: string): PipelineRunSnapshot | null {
      return toRunSnapshot(module.getRun(runId));
    },

    async getHistory(runId: string, fromVersion: number): Promise<BusEvent[]> {
      // Per the cross-repo contract: `getHistory` returns [] (no throw) when
      // the underlying EventBus has no walFilePath configured. PipelineModule
      // honors that already; pass-through. platform-api's `BusEvent` extends
      // DC's `PublicBusEvent`, so a structural cast is enough — DC's wider
      // internal envelope (`id`, `sourceNodeId`, `version`, `v`) is dropped
      // by the route, but the runtime payload carries them harmlessly.
      const events = (await module.getHistory(runId, fromVersion)) as Array<PublicBusEvent>;
      return events as BusEvent[];
    },

    async resolveApproval(runId, stepId, userId, decision, comment) {
      module.resolveApproval(runId, stepId, userId, decision, comment);
    },

    listActiveRuns(): PipelineRunSnapshot[] {
      return listActiveRunSnapshots();
    },

    cancelRun(runId: string): void {
      module.cancelRun(runId);
    },

    resolveBreakpoint(runId: string, stepId: string, action: 'continue' | 'step_over' | 'resume_all' = 'continue'): void {
      module.resolveBreakpoint(runId, stepId, action);
    },

    // hub#2223: restart a previously-failed run from `fromNodeId`. The
    // PipelineModule enforces the resumable-state contract via
    // `InvalidRunStateError`; we surface the same error here so the route can
    // map it to a 4xx. Returns the terminal `PipelineRun` snapshot.
    //
    // Structural call against the module — `PipelineModule.resumeFromStep`
    // shipped in distributed-core hub#2222 (post-v0.14.2). We cast through
    // `unknown` so this code compiles against the v0.14.2 type surface that's
    // currently pinned; at runtime the method is present on the actual
    // module instance once the pin is bumped to the post-2222 release.
    async resumeFromStep(
      runId: string,
      fromNodeId: string,
      options?: { breakpoints?: string[]; allowCompleted?: boolean },
    ): Promise<unknown> {
      const m = module as unknown as {
        resumeFromStep?: (
          runId: string,
          fromNodeId: string,
          options?: { breakpoints?: string[]; allowCompleted?: boolean },
        ) => Promise<unknown>;
      };
      if (typeof m.resumeFromStep !== 'function') {
        throw new Error(
          'PipelineModule.resumeFromStep is unavailable. Bump distributed-core to a version that includes hub#2222.',
        );
      }
      return m.resumeFromStep(runId, fromNodeId, options);
    },

    // hub#2226: edit-and-continue. Both methods structural-cast against the
    // module so this code compiles against pre-2226 type pins; at runtime the
    // method must exist on the actual module instance once the pin is bumped
    // to a version including the executor commit (eee7ab2).
    async patchPausedContext(
      runId: string,
      stepId: string,
      patch: ContextPatch,
      appliedBy?: string,
    ): Promise<PatchResult> {
      const m = module as unknown as {
        patchPausedContext?: (
          runId: string,
          stepId: string,
          patch: ContextPatch,
          appliedBy?: string,
        ) => Promise<PatchResult>;
      };
      if (typeof m.patchPausedContext !== 'function') {
        throw new Error(
          'PipelineModule.patchPausedContext is unavailable. Bump distributed-core to a version that includes hub#2226.',
        );
      }
      return m.patchPausedContext(runId, stepId, patch, appliedBy);
    },

    async revertContextPatch(
      runId: string,
      stepId: string,
      patchId: string,
    ): Promise<void> {
      const m = module as unknown as {
        revertContextPatch?: (
          runId: string,
          stepId: string,
          patchId: string,
        ) => Promise<void>;
      };
      if (typeof m.revertContextPatch !== 'function') {
        throw new Error(
          'PipelineModule.revertContextPatch is unavailable. Bump distributed-core to a version that includes hub#2226.',
        );
      }
      return m.revertContextPatch(runId, stepId, patchId);
    },

    getPendingApprovals(): PendingApprovalRow[] {
      return module.getPendingApprovals() as unknown as PendingApprovalRow[];
    },

    async getMetrics(): Promise<PipelineBridgeMetrics> {
      const m = (await module.getMetrics()) as unknown as Record<string, unknown>;
      // Forward every dashboard-relevant field the module emits. Each numeric
      // field is coerced to a finite number; if the module doesn't track it,
      // the field is omitted (route maps absent → `null`). `runsAwaitingApproval`
      // keeps its legacy "default to 0" behavior so callers (pipelineHealth)
      // that read it as a plain count don't regress.
      const runsAwaitingApprovalRaw = asFiniteNumber(m['runsAwaitingApproval']);
      const out: PipelineBridgeMetrics = {
        runsAwaitingApproval: runsAwaitingApprovalRaw ?? 0,
      };
      const runsStarted = asFiniteNumber(m['runsStarted']);
      if (runsStarted !== undefined) out.runsStarted = runsStarted;
      const runsCompleted = asFiniteNumber(m['runsCompleted']);
      if (runsCompleted !== undefined) out.runsCompleted = runsCompleted;
      const runsFailed = asFiniteNumber(m['runsFailed']);
      if (runsFailed !== undefined) out.runsFailed = runsFailed;
      const runsActive = asFiniteNumber(m['runsActive']);
      if (runsActive !== undefined) out.runsActive = runsActive;
      const avgDurationMs = asFiniteNumber(m['avgDurationMs']);
      if (avgDurationMs !== undefined) out.avgDurationMs = avgDurationMs;
      const llmTokensIn = asFiniteNumber(m['llmTokensIn']);
      if (llmTokensIn !== undefined) out.llmTokensIn = llmTokensIn;
      const llmTokensOut = asFiniteNumber(m['llmTokensOut']);
      if (llmTokensOut !== undefined) out.llmTokensOut = llmTokensOut;
      // distributed-core v0.3.7+: average first-token latency across LLM steps.
      const avgFirstTokenLatencyMs = asFiniteNumber(m['avgFirstTokenLatencyMs']);
      if (avgFirstTokenLatencyMs !== undefined) {
        out.avgFirstTokenLatencyMs = avgFirstTokenLatencyMs;
      }
      const asOf = m['asOf'];
      if (typeof asOf === 'string') out.asOf = asOf;
      return out;
    },

    // Pass-through of PipelineModule.getDashboardData(). The module synthesises
    // the payload from its own getMetrics() snapshot (chart shapes, summary
    // KPIs, alerts) — the bridge does no shaping. Errors propagate; the route
    // wraps the call and tags the response source.
    async getDashboardData() {
      return module.getDashboardData();
    },

    getInspector(): QueueInspector<PipelineRunSnapshot> {
      if (inspector) return inspector;
      inspector = new InMemoryQueueInspector<
        PipelineRunSnapshot,
        LeaseLike<PipelineRunSnapshot>,
        DLQEntryLike<PipelineRunSnapshot>
      >({
        pending:  () => listActiveRunSnapshots().map(asEnvelope),
        inflight: () => [],
        // PipelineRunSnapshot inspector and BusEvent DLQ are different
        // shapes; the DLQ surface is exposed separately via getDLQ().
        dlq:      () => [],
      });
      return inspector;
    },

    getDLQ(): DeadLetterQueue<DCBusEvent> | null {
      return dlq;
    },

    async listRuns(
      pipelineId: string,
      opts: { limit?: number; cursor?: string } = {},
    ): Promise<{ runs: PipelineRunSnapshot[]; nextCursor?: string }> {
      if (!pipelineRunRepository) {
        // Fallback to empty when repository not wired
        return { runs: [], nextCursor: undefined };
      }

      const limit = opts.limit ?? 50;

      // Query DDB for historical runs
      const items: PipelineRunItem[] = await pipelineRunRepository.listByPipeline(pipelineId, {
        limit,
      });

      const runs: PipelineRunSnapshot[] = items.map(runItemToSnapshot);

      return { runs, nextCursor: undefined }; // Pagination via cursor NYI
    },
  };
}
