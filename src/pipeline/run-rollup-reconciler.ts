// platform-api/src/pipeline/run-rollup-reconciler.ts
//
// NFR #220: the rollup's `runCount` is counted once per run by a durable claim
// (run-rollup-subscriber.ts). A claim taken whose row write was then lost (a
// crash between the two, a claim that could not be released) leaves the count
// one short, and rows written before the claims existed carry old drift. This
// recomputes the count from the run table and corrects the row. It runs as a
// cluster sweep task, so one replica (the sweep leader) does it.
//
// Race-free by construction: a row is only touched once it has been quiet for
// `quietMs`, and the target counts only runs started at or before that cutoff.
// Every such run's first event has already landed (the row absorbed nothing
// newer), and a run starting now is outside the target — if its `new` count
// lands after the correction it adds its own 1 on top of a count that did not
// include it. The write is the rollup's compare-and-set, so any event landing
// mid-check makes the correction lose and wait for the next sweep.

import { withContext } from '../lib/logger';
import { RollupVersionConflict, type PipelineRunRollupRepository } from '../repositories/PipelineRunRollupRepository';
import type { PipelineRunRollup } from './run-rollup';

const log = withContext({ service: 'run-rollup-reconciler' });

export interface RunCountReconcileDeps {
  repo: Pick<PipelineRunRollupRepository, 'listAll' | 'putIfUnchanged'>;
  /** Runs of `pipelineId` started at or before `cutoff` (ISO). */
  countRuns: (pipelineId: string, cutoff: string) => Promise<number>;
  now?: () => number;
  /** How long a row must have absorbed nothing before it is checked. Default 30 s. */
  quietMs?: number;
  /**
   * pipelineId → the row version last found correct. A row still at that
   * version is not recounted: counting reads every run row of the pipeline,
   * and DynamoDB Local holds each in heap. Default: this module's map.
   */
  verified?: Map<string, string>;
}

const verifiedRows = new Map<string, string>();

export interface RunCountReconcileResult {
  checked: number;
  corrected: Array<{ pipelineId: string; from: number; to: number }>;
  busy: number;
  lost: number;
}

function plusOneMs(iso: string): string {
  const t = Date.parse(iso);
  return Number.isFinite(t) ? new Date(t + 1).toISOString() : iso;
}

export async function reconcileRunCounts(deps: RunCountReconcileDeps): Promise<RunCountReconcileResult> {
  const now = (deps.now ?? Date.now)();
  const quietMs = deps.quietMs ?? 30_000;
  const result: RunCountReconcileResult = { checked: 0, corrected: [], busy: 0, lost: 0 };
  const verified = deps.verified ?? verifiedRows;
  const rows = await deps.repo.listAll();
  for (const row of rows) {
    // The sentinel of a pipeline that never ran has nothing to count.
    if (!row.lastRunId) continue;
    const last = Date.parse(row.updatedAt);
    if (!Number.isFinite(last) || last > now - quietMs) { result.busy += 1; continue; }
    if (verified.get(row.pipelineId) === row.updatedAt) continue;
    result.checked += 1;
    const cutoff = new Date(now - quietMs).toISOString();
    const actual = await deps.countRuns(row.pipelineId, cutoff);
    if (actual === row.runCount) { verified.set(row.pipelineId, row.updatedAt); continue; }
    const next: PipelineRunRollup = { ...row, runCount: actual, updatedAt: plusOneMs(row.updatedAt) };
    try {
      await deps.repo.putIfUnchanged(next, row);
      verified.set(row.pipelineId, next.updatedAt);
      result.corrected.push({ pipelineId: row.pipelineId, from: row.runCount, to: actual });
      log.info({ pipelineId: row.pipelineId, from: row.runCount, to: actual }, 'rollup runCount corrected from the run table');
    } catch (err) {
      if (!(err instanceof RollupVersionConflict)) throw err;
      result.lost += 1; // an event landed mid-check; the next sweep retries
    }
  }
  return result;
}
