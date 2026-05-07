// platform-api/src/ingest/routes/ingestDLQ.ts
//
// REST endpoints for the ingest DeadLetterQueue (T9 / library-expansion-3
// from distributed-core). Per AGENTIC_ETL_VISION §F + slice I, operators
// need to inspect + replay failed items so failures don't rot silently.
//
// Endpoints (mounted at /api/ingest/dlq):
//   GET    /              — list DLQ entries (paginated)
//   POST   /:id/replay    — re-enqueue an entry; removed from the DLQ on
//                            successful redrive.
//   DELETE /:id           — purge an entry without replay.

import { Router, type Request, type Response } from 'express';
import { asyncHandler, NotFoundError } from '../../middleware/error-handler';
import { withContext } from '../../lib/logger';
import { getIngestDLQ } from '../dlq-singleton';

const log = withContext({ route: 'ingestDLQ' });

export const ingestDLQRouter = Router();

/**
 * Maximum entries returned per `GET /`. Keeps responses bounded; operators
 * can paginate via `cursor`.
 */
const DLQ_LIST_PAGE_SIZE = 100;

ingestDLQRouter.get(
  '/',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const dlq = getIngestDLQ();
    if (!dlq) {
      res.status(503).json({ error: 'ingest DLQ not initialized' });
      return;
    }

    const cursor = typeof req.query.cursor === 'string' ? req.query.cursor : undefined;
    const limit = typeof req.query.limit === 'string'
      ? Math.min(parseInt(req.query.limit, 10) || DLQ_LIST_PAGE_SIZE, DLQ_LIST_PAGE_SIZE)
      : DLQ_LIST_PAGE_SIZE;

    const page = await dlq.list({ limit, cursor });
    res.json({
      items: page.items,
      nextCursor: page.nextCursor ?? null,
    });
  }),
);

ingestDLQRouter.post(
  '/:id/replay',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const dlq = getIngestDLQ();
    if (!dlq) {
      res.status(503).json({ error: 'ingest DLQ not initialized' });
      return;
    }

    const id = req.params.id;
    const result = await dlq.redrive([id]);
    if (result.redriven === 0) {
      throw new NotFoundError(`DLQ entry ${id} not found or already redriven`);
    }

    log.info({ id, action: 'dlq.replay' }, 'DLQ entry replayed');
    res.json({ ok: true, id, redriven: result.redriven });
  }),
);

ingestDLQRouter.delete(
  '/:id',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const dlq = getIngestDLQ();
    if (!dlq) {
      res.status(503).json({ error: 'ingest DLQ not initialized' });
      return;
    }

    const id = req.params.id;
    const result = await dlq.purge([id]);
    if (result.purged === 0) {
      throw new NotFoundError(`DLQ entry ${id} not found`);
    }

    log.info({ id, action: 'dlq.purge' }, 'DLQ entry purged');
    res.json({ ok: true, id, purged: result.purged });
  }),
);
