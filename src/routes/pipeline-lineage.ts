// platform-api/src/routes/pipeline-lineage.ts
//
// GET /api/pipeline/runs/:runId/image-lineage
// Returns the image artifact DAG for a single pipeline run so the UI can
// render a lineage graph: source images on the left, terminal crops/saves
// on the right, parent edges connecting them.
//
// Auth: requireAuth (same guard as all pipeline routes).
// The pipelineId is resolved by scanning the run across all pipelines
// owned by the authenticated user. To keep this cheap, the route accepts
// an optional `?pipelineId=<id>` query param; when present it skips the
// scan and uses it directly (matches the run detail page URL shape which
// already carries both IDs).

import { Router } from 'express';
import type { Request, Response } from 'express';
import { requireAuth } from '../middleware/auth';
import { asyncHandler, NotFoundError } from '../middleware/error-handler';
import { PipelineRunRepository, type ImageLineageEntry } from '../repositories/PipelineRunRepository';
import { getDynamoClient } from '../lib/ddb-client';
import { requireRunAccess } from '../pipeline/runAccess';

export const pipelineLineageRouter = Router({ mergeParams: true });

export interface ImageLineageResponse {
  entries: ImageLineageEntry[];
}

// Lazy-init repository (matches pattern from other pipeline routes)
let runRepo: PipelineRunRepository | null = null;
function getRunRepo(): PipelineRunRepository {
  if (!runRepo) runRepo = new PipelineRunRepository(getDynamoClient());
  return runRepo;
}

/**
 * GET /api/pipeline/runs/:runId/image-lineage
 *
 * Query params:
 *   pipelineId  — required. The pipeline that owns this run.
 *
 * Response: { entries: ImageLineageEntry[] }
 */
pipelineLineageRouter.get(
  '/',
  requireAuth,
  // Loop 29c (NFR #173): the run's owner or a reader of its document; pipelineId is a query param.
  requireRunAccess('read', (req) => req.params.runId, (req) => (req.query as Record<string, unknown>).pipelineId),
  asyncHandler(async (req: Request<{ runId: string }>, res: Response) => {
    const { runId } = req.params;
    const pipelineId = (req.query as Record<string, string>)['pipelineId'];

    if (!pipelineId) {
      res.status(400).json({ error: 'pipelineId query param is required' });
      return;
    }

    const entries = await getRunRepo().getRunImageLineage(pipelineId, runId);

    if (entries === null) {
      throw new NotFoundError(`Run ${runId} not found for pipeline ${pipelineId}`);
    }

    const body: ImageLineageResponse = { entries };
    res.json(body);
  }),
);
