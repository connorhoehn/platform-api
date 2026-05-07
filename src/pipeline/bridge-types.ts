import type {
  PipelineRunSnapshot as DCPipelineRunSnapshot,
  PendingApprovalRow as DCPendingApprovalRow,
  PublicBusEvent,
  BusEvent as DCBusEvent,
  QueueInspector,
  DeadLetterQueue,
  ApplicationModuleDashboardData,
} from 'distributed-core';

// hub#2226: local mirror of the distributed-core types added in commit
// `eee7ab2` (post-v0.14.2). The currently-pinned tag predates these
// declarations, so we redeclare the structural shape here so the bridge
// signatures compile against today's pin. When the pin advances to a
// release that re-exports them from distributed-core's index, switch back
// to `import type { ContextPatch, PatchResult } from 'distributed-core'`.
export interface JsonPatchOperation {
  op: 'add' | 'remove' | 'replace' | 'move' | 'copy' | 'test';
  path: string;
  value?: unknown;
  from?: string;
}
export type ContextPatch =
  | { mode: 'merge'; values: Record<string, unknown> }
  | { mode: 'jsonPatch'; ops: JsonPatchOperation[] };
export interface PatchResult {
  patchId: string;
  before: Record<string, unknown>;
  after: Record<string, unknown>;
  validation: { ok: true } | { ok: false; reason: string };
}

export interface PipelineRunSnapshot extends Omit<Partial<DCPipelineRunSnapshot>, 'status'> {
  runId: string;
  pipelineId?: string;
  status?: string;
  [k: string]: unknown;
}

export interface BusEvent<T = unknown> extends Partial<PublicBusEvent<T>> {
  type: string;
  payload?: T;
  version?: number;
  at?: string;
  [k: string]: unknown;
}

export interface PipelineBridge {
  trigger?(args: {
    pipelineId: string;
    definition?: unknown;
    triggerPayload?: unknown;
    triggeredBy: { userId: string };
    breakpoints?: string[];
  }): Promise<{ runId: string }>;
  resolveBreakpoint?(
    runId: string,
    stepId: string,
    action?: 'continue' | 'step_over' | 'resume_all',
  ): void;
  /**
   * hub#2223: restart a previously-failed run from `fromNodeId` without
   * re-running its succeeded upstream steps. Delegates to
   * `PipelineModule.resumeFromStep`. Returns the terminal `PipelineRun`
   * snapshot after the resumed dispatch loop settles. Throws when the run
   * is unknown to this node, or when the run is not in a resumable state.
   */
  resumeFromStep?(
    runId: string,
    fromNodeId: string,
    options?: { breakpoints?: string[]; allowCompleted?: boolean },
  ): Promise<unknown>;
  /**
   * hub#2226: edit-and-continue. Apply a patch to the run's accumulated
   * context while the run is paused at `stepId`'s breakpoint. Returns the
   * `PatchResult` from the executor — pre/post snapshots and validator
   * outcome. When `validation.ok === false` the patch was NOT applied;
   * the route surfaces a 422 with the reason. Throws
   * `InvalidRunStateError` when the run isn't paused at `stepId`, and
   * `PatchValidationError` for malformed payloads (incl. `mode: 'jsonPatch'`
   * which is rejected in v1).
   */
  patchPausedContext?(
    runId: string,
    stepId: string,
    patch: ContextPatch,
    appliedBy?: string,
  ): Promise<PatchResult>;
  /**
   * hub#2226: revert a previously-applied paused-run patch. Only legal
   * while the run is still paused at the same `stepId` — once execution
   * resumes past `pausedAtNodeId` patches become permanent and this
   * throws `InvalidRunStateError`.
   */
  revertContextPatch?(
    runId: string,
    stepId: string,
    patchId: string,
  ): Promise<void>;
  getRun(runId: string): Promise<PipelineRunSnapshot | null> | PipelineRunSnapshot | null;
  getHistory(runId: string, fromVersion: number): Promise<BusEvent[]> | BusEvent[];
  resolveApproval(
    runId: string,
    stepId: string,
    userId: string,
    decision: 'approve' | 'reject',
    comment?: string,
  ): Promise<void> | void;
  listRuns?(
    pipelineId: string,
    opts: { limit: number; cursor?: string },
  ): Promise<{ runs: PipelineRunSnapshot[]; nextCursor?: string }>
    | { runs: PipelineRunSnapshot[]; nextCursor?: string };
  listActiveRuns?(): Promise<PipelineRunSnapshot[]> | PipelineRunSnapshot[];
  cancelRun?(runId: string, userId: string): Promise<void> | void;
  getPendingApprovals?(): Promise<PendingApprovalRow[]> | PendingApprovalRow[];
  getLastEventAt?(): number | null;
  getMetrics?(): Promise<PipelineBridgeMetrics> | PipelineBridgeMetrics;
  /**
   * Pass-through of `ApplicationModule.getDashboardData()` (distributed-core
   * `src/applications/types.ts:74`). Returns the module's own ready-to-render
   * dashboard payload — chart definitions, summary KPIs, and alerts — so the
   * frontend can render a uniform module-driven observability section instead
   * of hand-rolling the same shape from `getMetrics()`.
   */
  getDashboardData?(): Promise<ApplicationModuleDashboardData> | ApplicationModuleDashboardData;
  getInspector?(): QueueInspector<PipelineRunSnapshot>;
  getDLQ?(): DeadLetterQueue<DCBusEvent> | null;
}

export interface PipelineBridgeMetrics {
  runsStarted?: number;
  runsCompleted?: number;
  runsFailed?: number;
  runsActive?: number;
  runsAwaitingApproval?: number;
  avgDurationMs?: number;
  llmTokensIn?: number;
  llmTokensOut?: number;
  avgFirstTokenLatencyMs?: number;
  asOf?: string;
  [k: string]: unknown;
}

export type PendingApprovalRow = DCPendingApprovalRow;

let bridge: PipelineBridge | null = null;

export function setPipelineBridge(b: PipelineBridge | null): void {
  bridge = b;
}

export function getPipelineBridge(): PipelineBridge | null {
  return bridge;
}
