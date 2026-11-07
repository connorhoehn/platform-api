// platform-api/src/routes/media.ts
//
// The catalog's HTTP surface. POST is what the host walker calls with each
// batch — it starts one `media-catalog` run per batch and answers 202 with
// the run id, so the walker never waits on a model. The GETs are what the
// catalog page reads. Every read and write is scoped to the caller's
// organisation, and batches are capped per user (rate and size).

import { Router, type Request, type Response } from 'express';
import { asyncHandler, ValidationError, NotFoundError, ForbiddenError } from '../middleware/error-handler';
import { mediaCatalogRepo } from '../repositories';
import { perUserRateLimit } from '../middleware/rateLimit';
import { skipAuthEnabled } from '../lib/insecureDevMode';
import { startMediaCatalogRun, type MediaBatch, type MediaProbe } from '../media/mediaCatalogPipeline';

export const mediaRouter = Router();

/** More than this per batch and one run holds the executor for minutes; the walker chunks. */
export const MAX_BATCH_FILES = 100;
/** A thumbnail is a ≤512px JPEG; anything bigger is not a thumbnail. */
export const MAX_THUMBNAIL_BYTES = 350 * 1024;
/**
 * All thumbnails of one batch together. The walker's default batch (40 files
 * at ~30 KB) is about 1.2 MB; the old ceiling was 100 × 350 KB, a 35 MB body
 * any user could post as often as they liked (security review, 6 November 2026).
 */
export const MAX_BATCH_THUMBNAIL_BYTES = 8 * 1024 * 1024;
/** Batches one user may post per minute. The walker waits out a 429's Retry-After. */
export const MEDIA_BATCHES_PER_MINUTE = 20;

/**
 * The caller's organisation; every catalog read and write is scoped to it.
 * The insecure dev fallback mirrors lobbyAccess' `default-org`.
 */
function callerOrg(req: Request): string {
  const org = req.user?.orgId ?? (skipAuthEnabled() ? 'default-org' : '');
  if (!org) throw new ForbiddenError('the media catalog needs an organisation on your account');
  return org;
}

const batchLimiter = perUserRateLimit('media:batch', MEDIA_BATCHES_PER_MINUTE);

const HEX64 = /^[0-9a-f]{64}$/;

export function validateBatch(body: unknown): MediaBatch {
  const b = (body ?? {}) as Partial<MediaBatch>;
  if (typeof b.batchId !== 'string' || !b.batchId) throw new ValidationError('batchId is required');
  if (typeof b.root !== 'string' || !b.root) throw new ValidationError('root is required');
  if (!Array.isArray(b.files) || b.files.length === 0) throw new ValidationError('files must be a non-empty array');
  if (b.files.length > MAX_BATCH_FILES) throw new ValidationError(`at most ${MAX_BATCH_FILES} files per batch`);
  let thumbnailBytes = 0;
  const files: MediaProbe[] = b.files.map((raw, i) => {
    const f = (raw ?? {}) as Partial<MediaProbe>;
    const at = `files[${i}]`;
    if (typeof f.fileId !== 'string' || !HEX64.test(f.fileId)) throw new ValidationError(`${at}.fileId must be a sha256 hex`);
    if (typeof f.path !== 'string' || !f.path) throw new ValidationError(`${at}.path is required`);
    if (typeof f.modifiedAt !== 'string' || Number.isNaN(Date.parse(f.modifiedAt))) throw new ValidationError(`${at}.modifiedAt must be an ISO date`);
    if (f.capturedAt != null && (typeof f.capturedAt !== 'string' || Number.isNaN(Date.parse(f.capturedAt)))) throw new ValidationError(`${at}.capturedAt must be an ISO date or null`);
    if (f.dhash != null && !/^[0-9a-f]{16}$/.test(String(f.dhash))) throw new ValidationError(`${at}.dhash must be 16 hex chars or null`);
    const kind = f.kind === 'image' || f.kind === 'video' ? f.kind : 'other';
    let thumbnail: MediaProbe['thumbnail'] = null;
    if (f.thumbnail && typeof f.thumbnail === 'object') {
      const t = f.thumbnail as NonNullable<MediaProbe['thumbnail']>;
      if (typeof t.base64 !== 'string' || !t.base64) throw new ValidationError(`${at}.thumbnail.base64 is required when a thumbnail is sent`);
      const bytes = Buffer.byteLength(t.base64, 'base64');
      if (bytes > MAX_THUMBNAIL_BYTES) throw new ValidationError(`${at}.thumbnail exceeds ${MAX_THUMBNAIL_BYTES} bytes`);
      thumbnailBytes += bytes;
      if (thumbnailBytes > MAX_BATCH_THUMBNAIL_BYTES) throw new ValidationError(`the batch's thumbnails exceed ${MAX_BATCH_THUMBNAIL_BYTES} bytes; send smaller batches`);
      thumbnail = { base64: t.base64, mimeType: typeof t.mimeType === 'string' ? t.mimeType : 'image/jpeg', width: Number(t.width) || 0, height: Number(t.height) || 0 };
    }
    return {
      path: f.path,
      relPath: typeof f.relPath === 'string' ? f.relPath : f.path,
      fileId: f.fileId,
      sizeBytes: Number(f.sizeBytes) || 0,
      mimeType: typeof f.mimeType === 'string' ? f.mimeType : 'application/octet-stream',
      kind,
      width: typeof f.width === 'number' ? f.width : null,
      height: typeof f.height === 'number' ? f.height : null,
      capturedAt: f.capturedAt ?? null,
      modifiedAt: f.modifiedAt,
      cameraId: typeof f.cameraId === 'string' && f.cameraId ? f.cameraId : null,
      dhash: f.dhash ?? null,
      thumbnail,
    };
  });
  return { batchId: b.batchId, root: b.root, files };
}

mediaRouter.post('/catalog/batches', batchLimiter, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const orgId = callerOrg(req);
  const batch = validateBatch(req.body);
  const userId = req.user?.sub ?? 'media-walker';
  const runId = await startMediaCatalogRun(batch, userId, orgId);
  res.status(202).json({ runId, batchId: batch.batchId, files: batch.files.length });
}));

mediaRouter.get('/catalog', asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const orgId = callerOrg(req);
  const limit = Math.min(Math.max(Number(req.query.limit) || 60, 1), 500);
  const cursor = typeof req.query.cursor === 'string' ? req.query.cursor : null;
  const page = await mediaCatalogRepo.listRecent(orgId, { limit, cursor });
  res.json(page);
}));

mediaRouter.get('/catalog/bursts/:burstId', asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const frames = await mediaCatalogRepo.listBurst(callerOrg(req), String(req.params.burstId));
  if (frames.length === 0) throw new NotFoundError('burst not found');
  res.json({ burstId: req.params.burstId, frames });
}));

mediaRouter.get('/catalog/files/:fileId', asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const row = await mediaCatalogRepo.getFile(callerOrg(req), String(req.params.fileId));
  if (!row) throw new NotFoundError('file not found');
  res.json(row);
}));
