// platform-api/src/pipeline/run-rollup.ts
//
// The last-run rollup: one derived row per pipeline that answers "what did
// this pipeline do lately" without listing its runs. Pure functions only —
// the repository persists them and the subscriber feeds them. The rules:
//
//   - `recent` holds the newest ≤ RECENT_LIMIT runs by startedAt, one entry
//     per runId. A run already there is UPDATED in place; a run not there is
//     INSERTED. That is what makes at-least-once delivery safe: a replayed
//     event finds its run and changes nothing.
//   - A status only moves forward: a terminal status never reverts to a live
//     one, and an entry never takes an event older than the one it has.
//   - `updatedAt` is the newest event time the row absorbed. The repository's
//     conditional put compares against it, so an older event can never
//     overwrite a newer row from the other replica.
//   - `approvals` is the set of approval gates still waiting, keyed by
//     (runId, stepId): `requested` adds a key, `recorded` removes it, and any
//     non-live status for the run drops all of its keys. `pendingApprovals` is
//     its size — the count every replica answers alike, instead of the
//     executor's in-memory queue on whichever replica took the request.
//   - `decisions` is the set of approval decisions taken on a replica that
//     does not own the run, keyed (runId, stepId, decisionId) — the shared
//     record the owner (or, when the owner is dead, whoever resumes the run)
//     applies to its executor (NFR #115). A `recorded` for the gate, or a
//     terminal status for the run, releases them; `interrupted` / `stuck` do
//     NOT, because that is exactly the run the resume path will re-ask.

export type RollupRunStatus =
  | 'pending' | 'running' | 'awaiting_approval' | 'paused_at_breakpoint'
  | 'completed' | 'failed' | 'cancelled' | 'rejected'
  | 'interrupted' | 'stuck';

export const RECENT_LIMIT = 5;

const TERMINAL: ReadonlySet<string> = new Set(['completed', 'failed', 'cancelled', 'rejected']);
const LIVE: ReadonlySet<string> = new Set(['pending', 'running', 'awaiting_approval', 'paused_at_breakpoint']);
const BAD: ReadonlySet<string> = new Set(['failed', 'interrupted', 'stuck']);

export function isTerminalRollupStatus(s: string): boolean { return TERMINAL.has(s); }
export function isLiveRollupStatus(s: string): boolean { return LIVE.has(s); }

/** Run-table spellings folded onto the rollup vocabulary. */
export function normalizeRunStatus(raw: unknown): RollupRunStatus | null {
  if (typeof raw !== 'string') return null;
  switch (raw) {
    case 'succeeded': return 'completed';
    case 'canceled': return 'cancelled';
    case 'pending': case 'running': case 'awaiting_approval': case 'paused_at_breakpoint':
    case 'completed': case 'failed': case 'cancelled': case 'rejected':
    case 'interrupted': case 'stuck':
      return raw;
    default:
      return null;
  }
}

export interface RollupRecentRun {
  runId: string;
  status: RollupRunStatus;
  startedAt: string;
  completedAt?: string;
  /** The event time that set `status` — the per-entry ordering guard. */
  at: string;
  /** The model the run's trigger chose for its agent steps (runOverrides.ts); absent = the steps' own. */
  model?: string;
}

/** One approval gate still waiting on a run of this pipeline. */
export interface RollupPendingApproval {
  runId: string;
  stepId: string;
  /** When it was requested. */
  at: string;
}

/**
 * One approval decision taken on a replica that does not own the run, kept
 * on the row until the owner's executor records it (NFR #115).
 */
export interface RollupApprovalDecision {
  decisionId: string;
  runId: string;
  stepId: string;
  userId: string;
  decision: 'approve' | 'reject';
  comment?: string;
  /** When it was taken. */
  at: string;
}

/** Decisions kept per row; the oldest go first when a row would exceed it. */
export const MAX_DECISIONS = 100;

/**
 * One run control (cancel, breakpoint resolve, resume-from-step) taken on a
 * replica that does not own the run, kept on the row until the replica that
 * holds the run applies it and releases it — or the run ends (NFR #120).
 */
export type RollupRunControl = {
  controlId: string;
  runId: string;
  userId: string;
  /** When it was taken. */
  at: string;
} & (
  | { kind: 'cancel'; reason?: string }
  | { kind: 'breakpoint'; stepId: string; action: 'continue' | 'step_over' | 'resume_all' }
  | { kind: 'resume-from-step'; fromNodeId: string; breakpoints?: string[]; allowCompleted?: boolean }
);

/** Controls kept per row; the oldest go first when a row would exceed it. */
export const MAX_CONTROLS = 100;

export interface PipelineRunRollup {
  pipelineId: string;
  /** '' when the pipeline has never run (the sentinel row that stops the backfill probe). */
  lastRunId: string;
  /** startedAt of the newest run; '' for the sentinel. */
  lastRunAt: string;
  lastRunStatus: RollupRunStatus | null;
  /** Newest first, ≤ RECENT_LIMIT. */
  recent: RollupRecentRun[];
  /** recent entries in failed | interrupted | stuck. */
  failedOfRecent: number;
  /** recent entries still pending | running | awaiting_approval | paused_at_breakpoint. */
  runningCount: number;
  /**
   * Distinct runs this row has absorbed. Exact while events arrive in order;
   * a replay older than the RECENT_LIMIT newest runs may count once more.
   */
  runCount: number;
  /** Approval gates still waiting, keyed by (runId, stepId). Absent on rows written before it existed. */
  approvals?: RollupPendingApproval[];
  /** `approvals.length` — shared across replicas because it lives on this row. */
  pendingApprovals: number;
  /** Decisions waiting for the run's owner to apply them. Absent on rows written before it existed. */
  decisions?: RollupApprovalDecision[];
  /** Run controls waiting for the replica that holds the run to apply them. Absent on rows written before it existed. */
  controls?: RollupRunControl[];
  /**
   * The newest event time the row absorbed — or one millisecond past the
   * previous value when a change carried no newer time, so every write moves
   * the version the repository's conditional put compares against.
   */
  updatedAt: string;
}

export interface RunRollupEvent {
  pipelineId: string;
  runId: string;
  status: RollupRunStatus;
  /** Event time (ISO). Orders events for one run and rows across replicas. */
  at: string;
  /** Known for `started`; a terminal event on an unknown run falls back to `at`. */
  startedAt?: string;
  /** `started` only: the trigger's model override, carried onto the recent entry. */
  model?: string;
  completedAt?: string;
  /** An approval gate on this run: `pending: true` = requested, `false` = recorded. */
  approval?: { stepId: string; pending: boolean };
  /**
   * A decision taken on a replica that does not own the run. A decision-only
   * event touches `decisions` and nothing else: it never inserts or moves the
   * run's `recent` entry (the owner's lifecycle events do that).
   */
  decision?: RollupApprovalDecision;
  /**
   * A run control taken on a replica that does not hold the run. Like a
   * decision, a control-only event touches `controls` and nothing else.
   */
  control?: RollupRunControl;
  /** The holder applied (or refused) a control: release it from the row. */
  controlReleased?: { controlId: string };
}

function byStartedAtDesc(a: RollupRecentRun, b: RollupRecentRun): number {
  if (a.startedAt === b.startedAt) return a.runId < b.runId ? 1 : a.runId > b.runId ? -1 : 0;
  return a.startedAt < b.startedAt ? 1 : -1;
}

function derive(pipelineId: string, recent: RollupRecentRun[], runCount: number, updatedAt: string, approvals: RollupPendingApproval[] = [], decisions: RollupApprovalDecision[] = [], controls: RollupRunControl[] = []): PipelineRunRollup {
  const sorted = [...recent].sort(byStartedAtDesc).slice(0, RECENT_LIMIT);
  const newest = sorted[0];
  return {
    pipelineId,
    lastRunId: newest?.runId ?? '',
    lastRunAt: newest?.startedAt ?? '',
    lastRunStatus: newest?.status ?? null,
    recent: sorted,
    failedOfRecent: sorted.filter((r) => BAD.has(r.status)).length,
    runningCount: sorted.filter((r) => LIVE.has(r.status)).length,
    runCount,
    approvals,
    pendingApprovals: approvals.length,
    ...(decisions.length > 0 ? { decisions } : {}),
    ...(controls.length > 0 ? { controls } : {}),
    updatedAt,
  };
}

/** The controls still waiting for a run, oldest first. */
export function controlsFor(row: PipelineRunRollup | null, runId: string): RollupRunControl[] {
  return (row?.controls ?? []).filter((c) => c.runId === runId);
}

/**
 * The control set after one event. Returns the SAME array when nothing
 * changes. A terminal run releases all of its controls; a release event
 * removes one; a control event adds one unless its controlId is already
 * there. `interrupted` / `stuck` keep them: the replica that resumes the run
 * reads the row and applies them.
 */
function nextControls(list: RollupRunControl[], event: RunRollupEvent): RollupRunControl[] {
  if (TERMINAL.has(event.status) && !event.control && !event.controlReleased) {
    const kept = list.filter((c) => c.runId !== event.runId);
    return kept.length === list.length ? list : kept;
  }
  if (event.controlReleased) {
    const kept = list.filter((c) => c.controlId !== event.controlReleased!.controlId);
    return kept.length === list.length ? list : kept;
  }
  const taken = event.control;
  if (!taken) return list;
  if (list.some((c) => c.controlId === taken.controlId)) return list;
  const next = [...list, taken];
  return next.length > MAX_CONTROLS ? next.slice(next.length - MAX_CONTROLS) : next;
}

/** The decisions still waiting for a run's gate, oldest first. */
export function decisionsFor(row: PipelineRunRollup | null, runId: string, stepId: string): RollupApprovalDecision[] {
  return (row?.decisions ?? []).filter((d) => d.runId === runId && d.stepId === stepId);
}

/**
 * The decision set after one event. Returns the SAME array when nothing
 * changes. A `recorded` gate releases the decisions taken for it; a terminal
 * run releases all of its decisions; a decision event adds one unless its
 * decisionId is already there. `interrupted` / `stuck` keep them: the run
 * that resumes will ask for the gate again and pick them up.
 */
function nextDecisions(list: RollupApprovalDecision[], event: RunRollupEvent, runEnded: boolean): RollupApprovalDecision[] {
  if (runEnded || TERMINAL.has(event.status)) {
    const kept = list.filter((d) => d.runId !== event.runId);
    return kept.length === list.length ? list : kept;
  }
  if (event.approval && !event.approval.pending) {
    const kept = list.filter((d) => !(d.runId === event.runId && d.stepId === event.approval!.stepId));
    return kept.length === list.length ? list : kept;
  }
  const taken = event.decision;
  if (!taken) return list;
  if (list.some((d) => d.decisionId === taken.decisionId)) return list;
  const next = [...list, taken];
  return next.length > MAX_DECISIONS ? next.slice(next.length - MAX_DECISIONS) : next;
}

/** One millisecond past `iso`, so a changed row always carries a new version. */
function plusOneMs(iso: string): string {
  const ms = Date.parse(iso);
  return Number.isFinite(ms) ? new Date(ms + 1).toISOString() : iso;
}

/**
 * The approval set after one event. Returns the SAME array when nothing
 * changes. A run leaving the live statuses (terminal, interrupted, stuck)
 * releases every gate it held: the executor that held them is gone or done.
 */
function nextApprovals(list: RollupPendingApproval[], event: RunRollupEvent): RollupPendingApproval[] {
  if (!LIVE.has(event.status)) {
    const kept = list.filter((a) => a.runId !== event.runId);
    return kept.length === list.length ? list : kept;
  }
  const gate = event.approval;
  if (!gate) return list;
  const idx = list.findIndex((a) => a.runId === event.runId && a.stepId === gate.stepId);
  if (gate.pending) {
    return idx >= 0 ? list : [...list, { runId: event.runId, stepId: gate.stepId, at: event.at }];
  }
  return idx < 0 ? list : list.filter((_, i) => i !== idx);
}

/** The row a pipeline gets before any run: nothing recent, so the probe is not repeated. */
export function emptyRollup(pipelineId: string, at: string): PipelineRunRollup {
  return derive(pipelineId, [], 0, at);
}

/**
 * The row after one event. Returns the SAME object when the event changes
 * nothing (a replay, or an event older than what the entry already has), so a
 * caller can skip the write.
 */
export function applyRunEvent(existing: PipelineRunRollup | null, event: RunRollupEvent): PipelineRunRollup {
  const base = existing ?? emptyRollup(event.pipelineId, event.at);
  const approvalsBefore = base.approvals ?? [];
  const decisionsBefore = base.decisions ?? [];
  const controlsBefore = base.controls ?? [];
  const idx = base.recent.findIndex((r) => r.runId === event.runId);
  const recent = base.recent.slice();
  let runCount = base.runCount;
  const cur = idx >= 0 ? recent[idx] : undefined;
  // A version that always moves: the newest event time, or one past the
  // previous value when this event is not newer but still changes the row.
  const stamp = () => {
    const at = maxIso(base.updatedAt, event.at);
    return existing && at === existing.updatedAt ? plusOneMs(at) : at;
  };

  // A decision taken elsewhere: recorded on the row, nothing else moves. A
  // run that already ended takes none (its owner cannot apply it).
  if (event.decision) {
    if (cur !== undefined && TERMINAL.has(cur.status)) return existing ?? base;
    const decisions = nextDecisions(decisionsBefore, event, false);
    if (decisions === decisionsBefore) return existing ?? base;
    return derive(base.pipelineId, recent, runCount, stamp(), approvalsBefore, decisions, controlsBefore);
  }
  // A run control taken elsewhere, or released by its holder: the row's
  // `controls` move, nothing else. A run that already ended takes none.
  if (event.control || event.controlReleased) {
    // (A resume-from-step is for a run that ended: it is the one control a terminal run takes.)
    if (event.control && event.control.kind !== 'resume-from-step' && cur !== undefined && TERMINAL.has(cur.status)) return existing ?? base;
    const controls = nextControls(controlsBefore, event);
    if (controls === controlsBefore) return existing ?? base;
    return derive(base.pipelineId, recent, runCount, stamp(), approvalsBefore, decisionsBefore, controls);
  }

  // A replayed `requested` that lands after its `recorded` is older than the
  // entry and must not re-open the gate; a release (non-live status) always
  // applies.
  const older = cur !== undefined && event.at < cur.at;
  const approvals = older && LIVE.has(event.status) ? approvalsBefore : nextApprovals(approvalsBefore, event);
  const approvalsChanged = approvals !== approvalsBefore;
  const decisions = older && LIVE.has(event.status) ? decisionsBefore : nextDecisions(decisionsBefore, event, false);
  const decisionsChanged = decisions !== decisionsBefore;
  const controls = older && LIVE.has(event.status) ? controlsBefore : nextControls(controlsBefore, event);
  const controlsChanged = controls !== controlsBefore;
  const sideChanged = approvalsChanged || decisionsChanged || controlsChanged;
  // One gate recorded while another on the same run still waits: the run is
  // still awaiting approval.
  const status: RollupRunStatus =
    event.approval && !event.approval.pending && approvals.some((a) => a.runId === event.runId)
      ? 'awaiting_approval'
      : event.status;
  const touched = () => derive(base.pipelineId, recent, runCount, stamp(), approvals, decisions, controls);

  if (cur !== undefined) {
    // Terminal is final: a late `running`, `interrupted` or `stuck` for a run
    // that already finished is stale news, whatever its timestamp says.
    const regress = TERMINAL.has(cur.status) && !TERMINAL.has(status);
    const same = cur.status === status && !(event.completedAt && !cur.completedAt);
    if (older || regress || same) {
      // Nothing new — but a started event can still fill a missing startedAt.
      if (event.startedAt && cur.startedAt === cur.at && event.startedAt < cur.startedAt) {
        recent[idx] = { ...cur, startedAt: event.startedAt };
        return touched();
      }
      return sideChanged ? touched() : existing ?? base;
    }
    recent[idx] = {
      ...cur,
      status,
      at: event.at,
      ...(event.startedAt && event.startedAt < cur.startedAt ? { startedAt: event.startedAt } : {}),
      ...(event.completedAt ? { completedAt: event.completedAt } : TERMINAL.has(status) ? { completedAt: event.at } : {}),
      ...(event.model && !cur.model ? { model: event.model } : {}),
    };
  } else {
    const startedAt = event.startedAt ?? event.at;
    // A run older than everything we keep, arriving after the window is full,
    // is history: it must not evict a newer run.
    if (recent.length >= RECENT_LIMIT && startedAt < recent[recent.length - 1].startedAt) {
      return sideChanged ? touched() : existing ?? base;
    }
    recent.push({
      runId: event.runId,
      status,
      startedAt,
      at: event.at,
      ...(event.completedAt ? { completedAt: event.completedAt } : TERMINAL.has(status) ? { completedAt: event.at } : {}),
      ...(event.model ? { model: event.model } : {}),
    });
    runCount += 1;
  }
  return touched();
}

function maxIso(a: string, b: string): string { return a > b ? a : b; }

/** What the run table says, shaped as a rollup — the backfill for a pipeline with no row yet. */
export interface RunRowLike {
  runId: string;
  status: unknown;
  startedAt?: string;
  completedAt?: string;
  lastEventAt?: string;
  interruptedAt?: string;
  stuckAt?: string;
  runOverrides?: { model?: string };
}

export function rollupFromRuns(pipelineId: string, runs: readonly RunRowLike[], at: string, totalHint?: number): PipelineRunRollup {
  const recent: RollupRecentRun[] = [];
  for (const run of runs) {
    if (typeof run.runId !== 'string' || !run.runId) continue;
    const status = (run.stuckAt ? 'stuck' : normalizeRunStatus(run.status)) ?? 'running';
    const startedAt = typeof run.startedAt === 'string' && run.startedAt ? run.startedAt : at;
    recent.push({
      runId: run.runId,
      status,
      startedAt,
      at: run.completedAt ?? run.interruptedAt ?? run.stuckAt ?? run.lastEventAt ?? startedAt,
      ...(run.completedAt ? { completedAt: run.completedAt } : {}),
      ...(typeof run.runOverrides?.model === 'string' ? { model: run.runOverrides.model } : {}),
    });
  }
  const sorted = recent.sort(byStartedAtDesc).slice(0, RECENT_LIMIT);
  const updatedAt = sorted.reduce((m, r) => (r.at > m ? r.at : m), sorted.length ? sorted[0].at : at);
  // The run table does not record approval gates (run persistence never
  // writes `awaiting_approval`), so a backfilled row starts with none; the
  // subscriber adds them as they are requested.
  return derive(pipelineId, sorted, Math.max(totalHint ?? 0, recent.length), updatedAt);
}
