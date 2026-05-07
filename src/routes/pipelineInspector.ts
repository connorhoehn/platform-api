// social-api/src/routes/pipelineInspector.ts
//
// Operator introspection endpoints for the pipeline run-queue, backed by
// distributed-core v0.11.0 T13 (lib-expansion-3) `QueueInspector`. Reads
// pass through to `bridge.getInspector()` — when the bridge isn't wired
// yet, every endpoint returns 503 so the dashboard surfaces a recognizable
// "inspector not available" state rather than 404'ing.
//
// Endpoints:
//   GET /api/pipelines/inspector/pending?limit=&cursor=  → paginated active runs
//   GET /api/pipelines/inspector/inflight?limit=         → leased entries (always [] today)
//   GET /api/pipelines/inspector/summary                 → aggregate snapshot
//   GET /api/pipelines/inspector/peek/:runId             → single envelope by id
//
// Auth: all endpoints sit behind the existing `requireAuth` mount in app.ts
// + `pipelineReadRateLimit` applied at the `/api/pipelines` mount in app.ts
// for GETs. No additional middleware here.

import { Router, type Response } from 'express';
import type { QueueInspector } from 'distributed-core';
import { asyncHandler } from '../middleware/error-handler';
import { getPipelineBridge } from './pipelineTriggers';
import type { PipelineRunSnapshot } from './pipelineTriggers';

export const pipelineInspectorRouter = Router();

function inspectorOr503(
  res: Response,
): QueueInspector<PipelineRunSnapshot> | null {
  const bridge = getPipelineBridge();
  if (!bridge || typeof bridge.getInspector !== 'function') {
    res.status(503).json({ error: 'pipeline inspector unavailable' });
    return null;
  }
  return bridge.getInspector();
}

function parseLimit(raw: unknown, def = 50, max = 200): number {
  if (typeof raw !== 'string') return def;
  const n = Number(raw);
  if (!Number.isFinite(n) || n < 0) return def;
  return Math.min(Math.floor(n), max);
}

pipelineInspectorRouter.get('/pending', asyncHandler(async (req, res) => {
  const inspector = inspectorOr503(res);
  if (!inspector) return;
  const query = req.query as Record<string, unknown>;
  const limit = parseLimit(query.limit);
  const cursor = typeof query.cursor === 'string' ? query.cursor : undefined;
  const page = await inspector.listPending({ limit, cursor });
  res.status(200).json(page);
}));

pipelineInspectorRouter.get('/inflight', asyncHandler(async (req, res) => {
  const inspector = inspectorOr503(res);
  if (!inspector) return;
  const query = req.query as Record<string, unknown>;
  const limit = parseLimit(query.limit);
  const items = await inspector.listInflight({ limit });
  res.status(200).json({ items });
}));

pipelineInspectorRouter.get('/summary', asyncHandler(async (_req, res) => {
  const inspector = inspectorOr503(res);
  if (!inspector) return;
  const summary = await inspector.summary();
  res.status(200).json(summary);
}));

pipelineInspectorRouter.get('/peek/:runId', asyncHandler(async (req, res) => {
  const inspector = inspectorOr503(res);
  if (!inspector) return;
  const params = req.params as { runId: string };
  const env = await inspector.peekPending(params.runId);
  if (!env) {
    res.status(404).json({ error: 'run not in pending queue', runId: params.runId });
    return;
  }
  res.status(200).json(env);
}));
