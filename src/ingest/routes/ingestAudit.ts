// social-api/src/ingest/routes/ingestAudit.ts
//
// Read-side route for the ingest audit log (hub#2214 — slice 2/3 of the
// audit-viewer feature; foundation slice was hub#2213). Operators ask
// "why did this file get mapped to type X?" / "who paused the source
// last Tuesday?" by hitting GET /api/ingest/audit with filters.
//
// Slice 3/3 (hub#2215) instruments the ingest engine + write-back driver
// to actually populate the table; until that lands the page renders an
// empty state.

import { Router, type Request, type Response } from 'express';
import { asyncHandler } from '../../middleware/error-handler';
import { withContext } from '../../lib/logger';
import { ingestAuditRepo } from '../repositories';
import type { IngestAuditEventType } from '../types';

const log = withContext({ route: 'ingestAudit' });

export const ingestAuditRouter = Router();

const VALID_EVENT_TYPES: IngestAuditEventType[] = [
  'mapping.decided',
  'source.created',
  'source.updated',
  'source.paused',
  'source.resumed',
  'writeback.attempted',
  'writeback.failed',
];

ingestAuditRouter.get(
  '/',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const since = typeof req.query.since === 'string' ? req.query.since : undefined;
    const until = typeof req.query.until === 'string' ? req.query.until : undefined;
    const targetId = typeof req.query.targetId === 'string' ? req.query.targetId : undefined;
    const actor = typeof req.query.actor === 'string' ? req.query.actor : undefined;
    const eventType = typeof req.query.eventType === 'string'
      && (VALID_EVENT_TYPES as string[]).includes(req.query.eventType)
      ? (req.query.eventType as IngestAuditEventType)
      : undefined;
    const limitParam = typeof req.query.limit === 'string' ? parseInt(req.query.limit, 10) : NaN;
    const limit = Number.isFinite(limitParam) && limitParam > 0 ? limitParam : 100;

    // targetId is the most selective filter — short-circuit to the GSI
    // path when present. actor is the second-most selective; also GSI.
    let entries;
    if (targetId) {
      entries = await ingestAuditRepo.listByTarget(targetId, limit);
    } else if (actor) {
      entries = await ingestAuditRepo.listByActor(actor, limit);
    } else {
      entries = await ingestAuditRepo.listAll({ limit, since, until, eventType });
    }

    log.info(
      { count: entries.length, targetId, actor, eventType, action: 'audit.list' },
      'audit list served',
    );
    res.json({ entries, nextCursor: null });
  }),
);
