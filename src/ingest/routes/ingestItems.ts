// platform-api/src/ingest/routes/ingestItems.ts
//
// REST endpoints for ingest item inspection and manual intervention.
//
// Endpoints (mounted at /api/ingest/items in routes/index.ts):
//   GET    /              — list items (query: sourceId, status, limit)
//   GET    /:id           — get item detail
//   POST   /:id/retry     — retry failed item
//   POST   /:id/skip      — skip item

import { Router, type Request, type Response } from 'express';
import { asyncHandler, ValidationError, NotFoundError } from '../../middleware/error-handler';
import { withContext } from '../../lib/logger';
import { ingestItemRepo } from '../repositories';
import type { IngestItemStatus } from '../types';

const log = withContext({ route: 'ingestItems' });

export const ingestItemsRouter = Router();

// List items
ingestItemsRouter.get(
  '/',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const sourceId = typeof req.query.sourceId === 'string' ? req.query.sourceId : undefined;
    const status = typeof req.query.status === 'string' ? req.query.status as IngestItemStatus : undefined;
    const limit = typeof req.query.limit === 'string' ? Math.min(parseInt(req.query.limit, 10) || 100, 500) : 100;

    let items;
    if (sourceId) {
      items = await ingestItemRepo.listBySource(sourceId, status);
    } else if (status) {
      items = await ingestItemRepo.listByStatus(status, limit);
    } else {
      // No filter — use listByStatus without filter (scan). For now, use a
      // broad scan via listByStatus with a default. A dedicated listAll would
      // be added for production scale.
      items = await ingestItemRepo.listByStatus('queued', limit);
      // Actually we want all statuses — fall back to listing by each known status.
      // This is acceptable at demo/dev scale; production would use a GSI or scan.
      const statuses: IngestItemStatus[] = ['queued', 'mapping', 'materializing', 'triggering', 'done', 'failed'];
      items = [];
      for (const s of statuses) {
        const batch = await ingestItemRepo.listByStatus(s, limit - items.length);
        items.push(...batch);
        if (items.length >= limit) break;
      }
      items = items.slice(0, limit);
    }

    res.json({ items });
  }),
);

// Get item detail
ingestItemsRouter.get(
  '/:id',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const item = await ingestItemRepo.getById(req.params.id);
    if (!item) {
      throw new NotFoundError(`ingest item ${req.params.id} not found`);
    }
    res.json(item);
  }),
);

// Retry failed item
ingestItemsRouter.post(
  '/:id/retry',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const ingestId = req.params.id;
    const item = await ingestItemRepo.getById(ingestId);
    if (!item) {
      throw new NotFoundError(`ingest item ${ingestId} not found`);
    }

    if (item.status !== 'failed') {
      throw new ValidationError(`item ${ingestId} is not in failed status (current: ${item.status})`);
    }

    await ingestItemRepo.updateStatus(ingestId, 'queued', {
      error: null,
      attempts: item.attempts + 1,
    });

    log.info({ ingestId, attempts: item.attempts + 1, action: 'item.retry' }, 'item retried');
    res.json({ status: 'queued', ingestId, attempts: item.attempts + 1 });
  }),
);

// Skip item
ingestItemsRouter.post(
  '/:id/skip',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const ingestId = req.params.id;
    const item = await ingestItemRepo.getById(ingestId);
    if (!item) {
      throw new NotFoundError(`ingest item ${ingestId} not found`);
    }

    await ingestItemRepo.updateStatus(ingestId, 'done', {
      error: 'skipped by user',
    });

    log.info({ ingestId, action: 'item.skip' }, 'item skipped');
    res.json({ status: 'done', ingestId, skipped: true });
  }),
);

// Re-map item — operator override of the LLM mapper. Sets a new typeId and
// re-queues the item for materialization. AGENTIC_ETL_VISION §B Tier 2 (the
// "Re-map" action on the needs-review queue).
ingestItemsRouter.post(
  '/:id/remap',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const ingestId = req.params.id;
    const body = (req.body ?? {}) as Record<string, unknown>;
    if (typeof body.typeId !== 'string' || !body.typeId) {
      throw new ValidationError('typeId is required');
    }
    const item = await ingestItemRepo.getById(ingestId);
    if (!item) {
      throw new NotFoundError(`ingest item ${ingestId} not found`);
    }

    await ingestItemRepo.updateStatus(ingestId, 'materializing', {
      mappedTypeId: body.typeId,
      mappingMethod: 'rule',
      mappingConfidence: 1,
      error: null,
    });

    log.info(
      { ingestId, typeId: body.typeId, action: 'item.remap' },
      'item remapped by operator',
    );
    res.json({ status: 'materializing', ingestId, mappedTypeId: body.typeId });
  }),
);
