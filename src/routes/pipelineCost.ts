// social-api/src/routes/pipelineCost.ts
//
// GET /api/pipelines/:pipelineId/cost — aggregate LLM cost for all persisted
// runs of a pipeline. Reads from PipelineRunRepository and sums tokenUsage /
// estimatedCostUsd across runs.

import { Router, type Request } from 'express';
import { PipelineRunRepository, type TokenUsageByModel } from '../repositories/PipelineRunRepository';
import { getDynamoClient } from '../lib/ddb-client';
import { asyncHandler } from '../middleware/error-handler';

export const pipelineCostRouter = Router({ mergeParams: true });

export interface PipelineCostResponse {
  totalCostUsd: number;
  totalTokensIn: number;
  totalTokensOut: number;
  byModel: Record<string, { tokensIn: number; tokensOut: number; costUsd: number }>;
  runCount: number;
}

pipelineCostRouter.get('/', asyncHandler(async (req: Request<{ pipelineId: string }>, res) => {
  const { pipelineId } = req.params;
  if (!pipelineId) {
    res.status(400).json({ error: 'pipelineId is required' });
    return;
  }

  const repo = new PipelineRunRepository(getDynamoClient());
  const runs = await repo.listByPipeline(pipelineId, { limit: 200 });

  let totalCostUsd = 0;
  let totalTokensIn = 0;
  let totalTokensOut = 0;
  const byModel: Record<string, { tokensIn: number; tokensOut: number; costUsd: number }> = {};
  let runCount = 0;

  for (const run of runs) {
    if (!run.tokenUsage) continue;
    runCount++;

    for (const [model, usage] of Object.entries(run.tokenUsage as Record<string, TokenUsageByModel>)) {
      totalTokensIn += usage.tokensIn;
      totalTokensOut += usage.tokensOut;

      if (!byModel[model]) {
        byModel[model] = { tokensIn: 0, tokensOut: 0, costUsd: 0 };
      }
      byModel[model].tokensIn += usage.tokensIn;
      byModel[model].tokensOut += usage.tokensOut;
    }

    if (run.estimatedCostUsd) {
      totalCostUsd += run.estimatedCostUsd;
    }
  }

  // Compute per-model cost from the aggregated tokens using the same pricing logic
  const { computeCost } = await import('../pipeline/llm-pricing');
  for (const [model, agg] of Object.entries(byModel)) {
    agg.costUsd = Math.round(computeCost(model, agg.tokensIn, agg.tokensOut) * 1_000_000) / 1_000_000;
  }

  // Round total cost
  totalCostUsd = Math.round(totalCostUsd * 1_000_000) / 1_000_000;

  const body: PipelineCostResponse = {
    totalCostUsd,
    totalTokensIn,
    totalTokensOut,
    byModel,
    runCount,
  };

  res.json(body);
}));
