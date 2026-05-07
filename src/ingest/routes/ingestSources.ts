// platform-api/src/ingest/routes/ingestSources.ts
//
// REST endpoints for ingest source configuration and lifecycle.
//
// Endpoints (mounted at /api/ingest/sources in routes/index.ts):
//   POST   /                  — create a new source
//   GET    /                  — list all sources
//   GET    /:id               — get source by ID
//   PUT    /:id               — update source config
//   DELETE /:id               — delete source
//   POST   /:id/sync          — trigger manual sync
//   POST   /:id/pause         — pause source
//   POST   /:id/resume        — resume source
//   GET    /:id/stats         — get sync stats

import { Router, type Request, type Response } from 'express';
import { randomUUID } from 'crypto';
import { asyncHandler, ValidationError, NotFoundError } from '../../middleware/error-handler';
import { withContext } from '../../lib/logger';
import { ingestSourceRepo } from '../repositories';
import { getIngestEngine } from '../engine-singleton';
import type { IngestSource } from '../types';

const log = withContext({ route: 'ingestSources' });

export const ingestSourcesRouter = Router();

// Create
ingestSourcesRouter.post(
  '/',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const body = (req.body ?? {}) as Record<string, unknown>;

    if (typeof body.name !== 'string' || !body.name) {
      throw new ValidationError('name is required');
    }
    if (typeof body.type !== 'string' || !body.type) {
      throw new ValidationError('type is required');
    }
    if (typeof body.config !== 'object' || body.config === null || Array.isArray(body.config)) {
      throw new ValidationError('config is required and must be an object');
    }

    const now = new Date().toISOString();
    const source: IngestSource = {
      sourceId: randomUUID(),
      name: body.name,
      type: body.type as IngestSource['type'],
      config: body.config as IngestSource['config'],
      syncSchedule: typeof body.syncSchedule === 'string' ? body.syncSchedule : null,
      mappingRules: Array.isArray(body.mappingRules) ? body.mappingRules : [],
      defaultPipelineId: typeof body.defaultPipelineId === 'string' ? body.defaultPipelineId : null,
      status: 'active',
      lastSyncAt: null,
      lastSyncStats: null,
      createdAt: now,
      updatedAt: now,
    };

    await ingestSourceRepo.putSource(source);
    log.info({ sourceId: source.sourceId, type: source.type, action: 'source.create' }, 'source created');
    res.status(201).json(source);
  }),
);

// List all
ingestSourcesRouter.get(
  '/',
  asyncHandler(async (_req: Request, res: Response): Promise<void> => {
    const sources = await ingestSourceRepo.listAll();
    res.json({ sources });
  }),
);

// Get by ID
ingestSourcesRouter.get(
  '/:id',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const source = await ingestSourceRepo.getById(req.params.id);
    if (!source) {
      throw new NotFoundError(`source ${req.params.id} not found`);
    }
    res.json(source);
  }),
);

// Update
ingestSourcesRouter.put(
  '/:id',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const sourceId = req.params.id;
    const existing = await ingestSourceRepo.getById(sourceId);
    if (!existing) {
      throw new NotFoundError(`source ${sourceId} not found`);
    }

    const body = (req.body ?? {}) as Record<string, unknown>;
    const updates: Partial<Omit<IngestSource, 'sourceId'>> = {
      updatedAt: new Date().toISOString(),
    };

    if (typeof body.name === 'string') updates.name = body.name;
    if (typeof body.type === 'string') updates.type = body.type as IngestSource['type'];
    if (typeof body.config === 'object' && body.config !== null && !Array.isArray(body.config)) {
      updates.config = body.config as IngestSource['config'];
    }
    if (body.syncSchedule === null || typeof body.syncSchedule === 'string') {
      updates.syncSchedule = body.syncSchedule;
    }
    if (Array.isArray(body.mappingRules)) {
      updates.mappingRules = body.mappingRules;
    }
    if (body.defaultPipelineId === null || typeof body.defaultPipelineId === 'string') {
      updates.defaultPipelineId = body.defaultPipelineId;
    }
    if (
      typeof body.pipelineByDocType === 'object' &&
      body.pipelineByDocType !== null &&
      !Array.isArray(body.pipelineByDocType)
    ) {
      const raw = body.pipelineByDocType as Record<string, unknown>;
      const cleaned: Record<string, string> = {};
      for (const [typeId, pipelineId] of Object.entries(raw)) {
        if (typeof pipelineId === 'string' && pipelineId.length > 0) {
          cleaned[typeId] = pipelineId;
        }
      }
      updates.pipelineByDocType = cleaned;
    }

    await ingestSourceRepo.updateSource(sourceId, updates);
    const updated = await ingestSourceRepo.getById(sourceId);
    log.info({ sourceId, action: 'source.update' }, 'source updated');
    res.json(updated);
  }),
);

// Delete
ingestSourcesRouter.delete(
  '/:id',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const sourceId = req.params.id;
    const existing = await ingestSourceRepo.getById(sourceId);
    if (!existing) {
      throw new NotFoundError(`source ${sourceId} not found`);
    }

    await ingestSourceRepo.deleteSource(sourceId);
    log.info({ sourceId, action: 'source.delete' }, 'source deleted');
    res.status(204).end();
  }),
);

// Trigger manual sync
ingestSourcesRouter.post(
  '/:id/sync',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const sourceId = req.params.id;
    const existing = await ingestSourceRepo.getById(sourceId);
    if (!existing) {
      throw new NotFoundError(`source ${sourceId} not found`);
    }

    const engine = getIngestEngine();
    if (!engine) {
      res.status(503).json({ error: 'ingest engine not available' });
      return;
    }

    // Fire-and-forget — the sync runs asynchronously; progress is reported via events.
    void engine.triggerSync(sourceId, 'manual').catch((err) => {
      log.error(
        { err: err instanceof Error ? err.message : String(err), sourceId, action: 'source.sync' },
        'manual sync failed',
      );
    });

    log.info({ sourceId, action: 'source.sync' }, 'manual sync triggered');
    res.status(202).json({ status: 'sync_triggered', sourceId });
  }),
);

// Pause
ingestSourcesRouter.post(
  '/:id/pause',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const sourceId = req.params.id;
    const existing = await ingestSourceRepo.getById(sourceId);
    if (!existing) {
      throw new NotFoundError(`source ${sourceId} not found`);
    }

    await ingestSourceRepo.updateSource(sourceId, {
      status: 'paused',
      updatedAt: new Date().toISOString(),
    });

    log.info({ sourceId, action: 'source.pause' }, 'source paused');
    res.json({ status: 'paused', sourceId });
  }),
);

// Resume
ingestSourcesRouter.post(
  '/:id/resume',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const sourceId = req.params.id;
    const existing = await ingestSourceRepo.getById(sourceId);
    if (!existing) {
      throw new NotFoundError(`source ${sourceId} not found`);
    }

    await ingestSourceRepo.updateSource(sourceId, {
      status: 'active',
      updatedAt: new Date().toISOString(),
    });

    log.info({ sourceId, action: 'source.resume' }, 'source resumed');
    res.json({ status: 'active', sourceId });
  }),
);

// Get sync stats
ingestSourcesRouter.get(
  '/:id/stats',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const sourceId = req.params.id;
    const source = await ingestSourceRepo.getById(sourceId);
    if (!source) {
      throw new NotFoundError(`source ${sourceId} not found`);
    }

    res.json({
      sourceId,
      lastSyncAt: source.lastSyncAt,
      lastSyncStats: source.lastSyncStats,
      status: source.status,
    });
  }),
);
