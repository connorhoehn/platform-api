// platform-api/src/routes/pipelineEstimate.ts
//
// GET /api/pipelines/:pipelineId/estimate?model=haiku|sonnet|opus|<model id>
//
// A cost and duration range from THIS pipeline's own completed runs — the
// last five the rollup row remembers (`recent`, run-rollup.ts) — and nothing
// else: never another pipeline's runs, never a made-up number
// (docs/design/documents-detail/PLAN.md §2.3, §4 risk 2).
//
//   { pipelineId,
//     basis: { runs, costRuns, model?, repriced },
//     costUsd: { low, high } | null,
//     durationMs: { low, high } | null,
//     confidence: 'none' | 'low' | 'medium',
//     reason?: 'no-completed-runs' | 'no-cost-recorded' }
//
// Cost per run is `costOfRun` (agentLoops.ts, imported, not copied). With `model`, a run that
// recorded per-model token usage is repriced as if every token had gone to
// that model (`computeCost`, llm-pricing.ts — whose table is flagged there as
// unverified list prices, so the UI says "estimate"). A run with only a total
// cannot be repriced and keeps its own cost; `repriced` is true only when
// every priced run was repriced. Duration is `completedAt − startedAt` off the
// rollup entry. No writes: two replicas are trivially safe.

import { Router, type Request, type Response } from 'express';
import { asyncHandler } from '../middleware/error-handler';
import type { RollupRecentRun } from '../pipeline/run-rollup';
import type { PipelineRunItem } from '../repositories/PipelineRunRepository';
import { computeCost, isPricedModel } from '../pipeline/llm-pricing';
import { costOfRun } from './agentLoops';

export type EstimateConfidence = 'none' | 'low' | 'medium';

export interface PipelineEstimate {
  pipelineId: string;
  basis: { runs: number; costRuns: number; model?: string; repriced: boolean };
  costUsd: { low: number; high: number } | null;
  durationMs: { low: number; high: number } | null;
  confidence: EstimateConfidence;
  reason?: 'no-completed-runs' | 'no-cost-recorded';
}

export const ESTIMATE_RUNS = 5;
const MODEL = /^[A-Za-z0-9._:-]{1,100}$/;

type RunCostRow = Pick<PipelineRunItem, 'runId' | 'estimatedCostUsd' | 'tokenUsage'>;

/** Recorded cost of one run: `costOfRun` (agentLoops.ts), the same function the agent-loop views use. */
export function recordedCost(run: Pick<PipelineRunItem, 'estimatedCostUsd' | 'tokenUsage'>): number | undefined {
  return costOfRun(run);
}

/** The run's tokens as if all of them had gone to `model`; undefined when it recorded no per-model usage. */
export function repricedCost(run: Pick<PipelineRunItem, 'tokenUsage'>, model: string): number | undefined {
  const usage = run.tokenUsage;
  if (!usage || Object.keys(usage).length === 0) return undefined;
  let tokensIn = 0;
  let tokensOut = 0;
  for (const u of Object.values(usage)) { tokensIn += u.tokensIn ?? 0; tokensOut += u.tokensOut ?? 0; }
  if (tokensIn + tokensOut === 0) return undefined;
  return computeCost(model, tokensIn, tokensOut);
}

function range(values: number[]): { low: number; high: number } | null {
  if (values.length === 0) return null;
  return { low: Math.min(...values), high: Math.max(...values) };
}

function confidenceOf(n: number): EstimateConfidence {
  if (n === 0) return 'none';
  return n >= 4 ? 'medium' : 'low';
}

/** Pure: the estimate from the rollup's recent entries and those runs' rows. */
export function estimateFromRuns(
  pipelineId: string,
  recent: readonly RollupRecentRun[],
  runs: ReadonlyMap<string, RunCostRow | null>,
  model?: string,
): PipelineEstimate {
  const completed = recent
    .filter((r) => r.status === 'completed' && r.completedAt)
    .sort((a, b) => (b.completedAt ?? '').localeCompare(a.completedAt ?? ''))
    .slice(0, ESTIMATE_RUNS);
  const durations: number[] = [];
  const costs: number[] = [];
  let repricedAll = true;
  for (const entry of completed) {
    const ms = Date.parse(entry.completedAt!) - Date.parse(entry.startedAt);
    if (Number.isFinite(ms) && ms >= 0) durations.push(ms);
    const row = runs.get(entry.runId);
    if (!row) continue;
    const repriced = model ? repricedCost(row, model) : undefined;
    const cost = repriced ?? recordedCost(row);
    if (cost === undefined) continue;
    if (model && repriced === undefined) repricedAll = false;
    costs.push(cost);
  }
  const n = completed.length;
  const estimate: PipelineEstimate = {
    pipelineId,
    basis: { runs: n, costRuns: costs.length, ...(model ? { model } : {}), repriced: !!model && costs.length > 0 && repricedAll },
    costUsd: range(costs),
    durationMs: range(durations),
    confidence: confidenceOf(n),
  };
  if (n === 0) estimate.reason = 'no-completed-runs';
  else if (costs.length === 0) estimate.reason = 'no-cost-recorded';
  return estimate;
}

export interface PipelineEstimateDeps {
  recent(pipelineId: string): Promise<RollupRecentRun[] | null>;
  runs(pipelineId: string, runIds: readonly string[]): Promise<Map<string, RunCostRow | null>>;
}

export function createPipelineEstimateRouter(deps: PipelineEstimateDeps = defaultEstimateDeps()): Router {
  const router = Router({ mergeParams: true });
  router.get('/', asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const pipelineId = String((req.params as { pipelineId?: string }).pipelineId ?? '');
    if (!pipelineId) { res.status(400).json({ error: 'pipelineId is required' }); return; }
    const rawModel = req.query.model;
    // The same list a run's model override is checked against (runOverrides.ts):
    // a model the pricing table does not know would be priced at a made-up rate.
    if (rawModel !== undefined && (typeof rawModel !== 'string' || !MODEL.test(rawModel) || !isPricedModel(rawModel))) {
      res.status(400).json({ error: 'model must be haiku, sonnet, opus or a priced model id' });
      return;
    }
    const model = typeof rawModel === 'string' && rawModel ? rawModel : undefined;
    const recent = (await deps.recent(pipelineId)) ?? [];
    const wanted = recent.filter((r) => r.status === 'completed').map((r) => r.runId);
    const runs = wanted.length ? await deps.runs(pipelineId, wanted) : new Map<string, RunCostRow | null>();
    res.status(200).json(estimateFromRuns(pipelineId, recent, runs, model));
  }));
  return router;
}

/** A dispatched run's recorded cost — the Work header's "$ spent". */
export async function runCostOf(pipelineId: string, runId: string): Promise<number | undefined> {
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const { pipelineRunRepo } = require('../repositories') as typeof import('../repositories');
  const row = await pipelineRunRepo.get(pipelineId, runId);
  return row ? recordedCost(row) : undefined;
}

function defaultEstimateDeps(): PipelineEstimateDeps {
  return {
    recent: async (pipelineId) => {
      // eslint-disable-next-line @typescript-eslint/no-require-imports
      const { pipelineRunRollupRepo } = require('../repositories/PipelineRunRollupRepository') as typeof import('../repositories/PipelineRunRollupRepository');
      const row = await pipelineRunRollupRepo.get(pipelineId);
      return row?.recent ?? null;
    },
    runs: async (pipelineId, runIds) => {
      // eslint-disable-next-line @typescript-eslint/no-require-imports
      const { pipelineRunRepo } = require('../repositories') as typeof import('../repositories');
      const rows = await Promise.all(runIds.map((id) => pipelineRunRepo.get(pipelineId, id)));
      return new Map(runIds.map((id, i) => [id, rows[i] ?? null]));
    },
  };
}

let router: Router | null = null;
/** Mounted at /pipelines/:pipelineId/estimate. */
export const pipelineEstimateRouter = Router({ mergeParams: true });
pipelineEstimateRouter.use((req, res, next) => { router ??= createPipelineEstimateRouter(); router(req, res, next); });
