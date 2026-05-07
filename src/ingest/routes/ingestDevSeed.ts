// social-api/src/ingest/routes/ingestDevSeed.ts
//
// Dev-only seed-fake-content endpoint. AGENTIC_ETL_VISION slice J — operators
// (and devs evaluating the UI) can hit a button instead of dropping to a shell
// to run scripts/seed-ingest-mocks.sh. Seeded items go through the normal
// repos so the UI surfaces (sync monitor, items page, dashboard cards) light
// up end-to-end.
//
// Production gate: rejects 403 unless `process.env.NODE_ENV !== 'production'`.
// The route is mounted at /api/ingest/dev/seed-fake-content.

import { Router, type Request, type Response } from 'express';
import { randomUUID } from 'crypto';
import { asyncHandler, ValidationError } from '../../middleware/error-handler';
import { withContext } from '../../lib/logger';
import { ingestItemRepo } from '../repositories';
import type { IngestItem } from '../types';

const log = withContext({ route: 'ingestDevSeed' });

export const ingestDevSeedRouter = Router();

/**
 * Cap the per-call seed count so an operator clicking "100" doesn't accidentally
 * stress the dev DDB. Generous enough for realistic UI evaluation.
 */
const MAX_SEED_COUNT = 100;

/**
 * Synthesised file paths so seeded items aren't all `/seed/sample-1.json` —
 * the UI's per-source filter, status grouping, and event log all read more
 * lifelike when filenames vary in extension and prefix.
 */
const SAMPLE_PATHS = [
  '/seed/research/notes.md',
  '/seed/research/whitepaper.pdf',
  '/seed/contracts/agreement-q3.docx',
  '/seed/contracts/nda-2026.pdf',
  '/seed/invoices/inv-1042.json',
  '/seed/invoices/inv-1099.json',
  '/seed/tickets/INC-42.json',
  '/seed/tickets/INC-87.json',
  '/seed/transcripts/sales-call.txt',
  '/seed/transcripts/standup-2026-05-06.md',
] as const;

const SAMPLE_STATUSES: Array<IngestItem['status']> = [
  'queued',
  'mapping',
  'materializing',
  'done',
  'done',
  'failed',
];

ingestDevSeedRouter.post(
  '/seed-fake-content',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    if (process.env.NODE_ENV === 'production') {
      res.status(403).json({ error: 'seed-fake-content is disabled in production' });
      return;
    }

    const body = (req.body ?? {}) as Record<string, unknown>;
    const sourceId = typeof body.sourceId === 'string' && body.sourceId
      ? body.sourceId
      : 'src-dev-seed';
    const requested = typeof body.count === 'number' ? body.count : 5;
    if (!Number.isFinite(requested) || requested <= 0) {
      throw new ValidationError('count must be a positive number');
    }
    const count = Math.min(Math.floor(requested), MAX_SEED_COUNT);

    const now = new Date().toISOString();
    const created: string[] = [];
    for (let i = 0; i < count; i++) {
      const status = SAMPLE_STATUSES[i % SAMPLE_STATUSES.length];
      const path = SAMPLE_PATHS[i % SAMPLE_PATHS.length];
      const ingestId = `seed-${randomUUID()}`;
      const item: IngestItem = {
        ingestId,
        sourceId,
        filePath: path,
        fileHash: `seed-hash-${i}`,
        mimeType: path.endsWith('.json') ? 'application/json' : 'text/plain',
        fileSize: 1024 + i * 64,
        status,
        mappedTypeId: status !== 'queued' ? 'doctype-research' : null,
        mappingConfidence: status === 'mapping' ? 0.55 : status === 'done' ? 0.95 : null,
        mappingMethod: status === 'queued' ? null : 'llm-match',
        documentId: status === 'done' ? `doc-${ingestId}` : null,
        pipelineRunId: null,
        error: status === 'failed' ? 'simulated transient error' : null,
        attempts: status === 'failed' ? 1 : 0,
        createdAt: now,
        updatedAt: now,
      };
      await ingestItemRepo.put(item);
      created.push(ingestId);
    }

    log.info(
      { sourceId, count, action: 'dev.seed.fake-content' },
      'seeded fake ingest content',
    );
    res.json({ ok: true, sourceId, count, ingestIds: created });
  }),
);
