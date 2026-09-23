// platform-api/src/pipeline/run-rollup-read.ts
//
// The list route's read path for rollups: one BatchGetItem over the page's
// ids, and a bounded, non-destructive backfill for definitions that have no
// row yet. Nothing is rewritten: a row that exists is returned as is, and a
// missing one is computed from the run table (its newest 5 runs), written
// with a put that yields to any newer row, and returned. At most
// BACKFILL_PER_REQUEST are probed per request, oldest-updated definitions
// first; the rest come back `null` and heal on a later request. A pipeline
// with no runs gets the sentinel row so the probe is not repeated every load.

import { rollupFromRuns, emptyRollup, type PipelineRunRollup, type RunRowLike } from './run-rollup';
import type { PipelineRunRollupRepository } from '../repositories/PipelineRunRollupRepository';
import { withContext } from '../lib/logger';

const log = withContext({ service: 'run-rollup-read' });

export const BACKFILL_PER_REQUEST = 10;
/**
 * Rows read per probed pipeline. The run table's sort key is the runId (a
 * random UUID), so "most recent first" from `listByPipeline` is runId order,
 * not time order: the probe reads a window and `rollupFromRuns` keeps the
 * newest five by startedAt. A pipeline with more runs than this may seed
 * with a slightly stale window; the subscriber makes it exact from the next
 * event on.
 */
export const RECENT_RUNS_PER_PROBE = 25;

export interface RollupReadDeps {
  rollups: Pick<PipelineRunRollupRepository, 'getMany' | 'putIfNotNewer'>;
  /** The run table, newest first. `null` = no run table wired: missing rows stay null. */
  listRuns: ((pipelineId: string, limit: number) => Promise<RunRowLike[]>) | null;
  now?: () => string;
}

export interface RollupCandidate { id: string; updatedAt?: string }

/** `id → rollup | null` for every candidate. */
export async function loadRollups(candidates: readonly RollupCandidate[], deps: RollupReadDeps): Promise<Map<string, PipelineRunRollup | null>> {
  const ids = candidates.map((c) => c.id);
  const found = await deps.rollups.getMany(ids);
  if (!deps.listRuns) return found;

  const missing = candidates
    .filter((c) => !found.get(c.id))
    .sort((a, b) => (a.updatedAt ?? '') < (b.updatedAt ?? '') ? -1 : (a.updatedAt ?? '') > (b.updatedAt ?? '') ? 1 : a.id < b.id ? -1 : 1)
    .slice(0, BACKFILL_PER_REQUEST);

  const at = (deps.now ?? (() => new Date().toISOString()))();
  await Promise.all(missing.map(async ({ id }) => {
    try {
      const runs = await deps.listRuns!(id, RECENT_RUNS_PER_PROBE);
      const rollup = runs.length ? rollupFromRuns(id, runs, at) : emptyRollup(id, at);
      const wrote = await deps.rollups.putIfNotNewer(rollup);
      if (wrote) found.set(id, rollup);
      else {
        // The subscriber got there first; its row is the truth.
        const again = await deps.rollups.getMany([id]);
        found.set(id, again.get(id) ?? rollup);
      }
    } catch (err) {
      log.warn({ pipelineId: id, err: String(err) }, 'rollup backfill failed; returned null');
      found.set(id, null);
    }
  }));
  return found;
}
