// social-api/src/ingest/routes/ingestConflicts.ts
//
// Backend endpoints for the operator write-back conflict queue.
//
// Mounted at /api/sync/conflicts (frontend hook reads from there). Stub-only
// today — slice F (write-back engine + WriteBackChange events) hasn't landed,
// so no real conflicts exist yet. The route persists resolutions to an
// in-memory store so the Resolved tab on ConflictsPage has something to
// render. Once slice F lands, swap the in-memory store for a DDB-backed
// repository (see hub#2187 follow-up).
//
// Endpoints:
//   GET    /                  → { conflicts: SyncConflict[] }
//   POST   /:id/resolve       → { conflict: SyncConflict }
//   POST   /:id/discard       → { conflict: SyncConflict }

import { Router, type Request, type Response } from 'express';
import { asyncHandler, ValidationError } from '../../middleware/error-handler';
import { withContext } from '../../lib/logger';

const log = withContext({ route: 'ingestConflicts' });

interface ConflictField {
  field: string;
  sourceValue: unknown;
  platformValue: unknown;
  mergedValue?: unknown;
}

interface SyncConflict {
  conflictId: string;
  sourceId: string;
  documentId: string;
  documentTypeId: string;
  sourceRef: string;
  summary: string;
  fields: ConflictField[];
  status: 'pending' | 'resolved' | 'discarded';
  platformUpdatedAt: string;
  sourceUpdatedAt: string;
  detectedAt: string;
  resolvedAt?: string;
  resolution?: 'source' | 'platform' | 'merged';
  auditNote?: string;
}

interface ConflictResolution {
  resolution: 'source' | 'platform' | 'merged';
  mergedFields?: Record<string, unknown>;
  auditNote?: string;
}

/**
 * In-memory conflict store. Pending conflicts are seeded by slice F's
 * write-back engine (not yet landed); resolved entries land here when an
 * operator clicks Resolve / Discard so the Resolved tab has data.
 */
const conflicts = new Map<string, SyncConflict>();

export const ingestConflictsRouter = Router();

ingestConflictsRouter.get(
  '/',
  asyncHandler(async (_req: Request, res: Response): Promise<void> => {
    const all = Array.from(conflicts.values()).sort(
      (a, b) => b.detectedAt.localeCompare(a.detectedAt),
    );
    res.json({ conflicts: all });
  }),
);

ingestConflictsRouter.post(
  '/:id/resolve',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const id = req.params.id;
    const body = (req.body ?? {}) as Partial<ConflictResolution>;
    if (body.resolution !== 'source' && body.resolution !== 'platform' && body.resolution !== 'merged') {
      throw new ValidationError('resolution must be one of: source, platform, merged');
    }

    // Slice F isn't landed yet, so we accept resolutions for ANY id and
    // record a synthetic pending entry first. Once slice F populates pending
    // conflicts, this branch becomes a normal lookup-and-update.
    const existing = conflicts.get(id) ?? {
      conflictId: id,
      sourceId: 'unknown',
      documentId: 'unknown',
      documentTypeId: 'unknown',
      sourceRef: '',
      summary: 'Resolution recorded before write-back engine landed.',
      fields: [],
      status: 'pending' as const,
      platformUpdatedAt: new Date().toISOString(),
      sourceUpdatedAt: new Date().toISOString(),
      detectedAt: new Date().toISOString(),
    };

    const resolved: SyncConflict = {
      ...existing,
      status: 'resolved',
      resolvedAt: new Date().toISOString(),
      resolution: body.resolution,
      auditNote: typeof body.auditNote === 'string' ? body.auditNote : undefined,
    };
    conflicts.set(id, resolved);

    log.info({ id, resolution: body.resolution, action: 'conflict.resolve' }, 'conflict resolved');
    res.json({ conflict: resolved });
  }),
);

ingestConflictsRouter.post(
  '/:id/discard',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const id = req.params.id;
    const existing = conflicts.get(id) ?? {
      conflictId: id,
      sourceId: 'unknown',
      documentId: 'unknown',
      documentTypeId: 'unknown',
      sourceRef: '',
      summary: 'Discard recorded before write-back engine landed.',
      fields: [],
      status: 'pending' as const,
      platformUpdatedAt: new Date().toISOString(),
      sourceUpdatedAt: new Date().toISOString(),
      detectedAt: new Date().toISOString(),
    };

    const discarded: SyncConflict = {
      ...existing,
      status: 'discarded',
      resolvedAt: new Date().toISOString(),
    };
    conflicts.set(id, discarded);

    log.info({ id, action: 'conflict.discard' }, 'conflict discarded');
    res.json({ conflict: discarded });
  }),
);

/**
 * Test-only seed. Lets unit tests inject a pending conflict without spinning
 * up the write-back engine. Not exported from the package barrel.
 */
export function _seedConflictForTest(c: SyncConflict): void {
  conflicts.set(c.conflictId, c);
}

/** Test-only reset. */
export function _clearConflictsForTest(): void {
  conflicts.clear();
}
