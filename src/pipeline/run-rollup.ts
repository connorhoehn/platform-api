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
}

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
  /** The newest event time the row absorbed. */
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
  completedAt?: string;
}

function byStartedAtDesc(a: RollupRecentRun, b: RollupRecentRun): number {
  if (a.startedAt === b.startedAt) return a.runId < b.runId ? 1 : a.runId > b.runId ? -1 : 0;
  return a.startedAt < b.startedAt ? 1 : -1;
}

function derive(pipelineId: string, recent: RollupRecentRun[], runCount: number, updatedAt: string): PipelineRunRollup {
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
    updatedAt,
  };
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
  const idx = base.recent.findIndex((r) => r.runId === event.runId);
  const recent = base.recent.slice();
  let runCount = base.runCount;

  if (idx >= 0) {
    const cur = recent[idx];
    const older = event.at < cur.at;
    // Terminal is final: a late `running`, `interrupted` or `stuck` for a run
    // that already finished is stale news, whatever its timestamp says.
    const regress = TERMINAL.has(cur.status) && !TERMINAL.has(event.status);
    const same = cur.status === event.status && !(event.completedAt && !cur.completedAt);
    if (older || regress || same) {
      // Nothing new — but a started event can still fill a missing startedAt.
      if (event.startedAt && cur.startedAt === cur.at && event.startedAt < cur.startedAt) {
        recent[idx] = { ...cur, startedAt: event.startedAt };
        return derive(base.pipelineId, recent, runCount, maxIso(base.updatedAt, event.at));
      }
      return existing ?? base;
    }
    recent[idx] = {
      ...cur,
      status: event.status,
      at: event.at,
      ...(event.startedAt && event.startedAt < cur.startedAt ? { startedAt: event.startedAt } : {}),
      ...(event.completedAt ? { completedAt: event.completedAt } : TERMINAL.has(event.status) ? { completedAt: event.at } : {}),
    };
  } else {
    const startedAt = event.startedAt ?? event.at;
    // A run older than everything we keep, arriving after the window is full,
    // is history: it must not evict a newer run.
    if (recent.length >= RECENT_LIMIT && startedAt < recent[recent.length - 1].startedAt) {
      return existing ?? base;
    }
    recent.push({
      runId: event.runId,
      status: event.status,
      startedAt,
      at: event.at,
      ...(event.completedAt ? { completedAt: event.completedAt } : TERMINAL.has(event.status) ? { completedAt: event.at } : {}),
    });
    runCount += 1;
  }
  return derive(base.pipelineId, recent, runCount, maxIso(base.updatedAt, event.at));
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
    });
  }
  const sorted = recent.sort(byStartedAtDesc).slice(0, RECENT_LIMIT);
  const updatedAt = sorted.reduce((m, r) => (r.at > m ? r.at : m), sorted.length ? sorted[0].at : at);
  return derive(pipelineId, sorted, Math.max(totalHint ?? 0, recent.length), updatedAt);
}
