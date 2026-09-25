// platform-api/src/routes/mediaEdits.ts
//
// A catalog photo's edits (realtime-examples docs/design/media-photo-editor/PLAN.md §2),
// mounted at /api/media/edits. Every route with a :fileId is guarded by
// `requirePhotoRead` (404 to anyone who cannot read the photo).
//
//   GET  /edits?fileIds=a,b        HEAD rows for the filmstrip (≤100; unreadable ids are left out)
//   GET  /edits/:fileId            { head, versions (newest first, ≤50), next, channel }
//   POST /edits/:fileId/versions   Keep: { ops, parentVersion, summary, source, suggestionId, runId? }
//                                  → 201 { version } / 200 { version, replay:true } / 409 { currentVersion }
//   POST /edits/:fileId/restore    { version, parentVersion } → a NEW version with that version's ops
//   PUT  /edits/:fileId/pick       { pick: 'pick' | 'reject' | null }
//   PUT  /edits/:fileId/masks      { masks: [{ maskId, label, kind, subjects?, polygons }] }
//                                  → each rasterized to an 8-bit PNG, stored by content hash
//   POST /edits/:fileId/render     { version } → renders through the image:adjust-layer handler
//
// A Keep or Restore publishes `photo:edit_kept` on the photo's channel
// (`photo:<base64url id>`), so every viewer re-reads the new version live.

import { createHash } from 'crypto';
import { Router, type Request, type Response } from 'express';
import sharp from 'sharp';
import { asyncHandler, ValidationError, AppError, NotFoundError } from '../middleware/error-handler';
import { mediaCatalogRepo } from '../repositories';
import {
  MediaEditRepository, PhotoEditConflict, MAX_MASKS, MAX_OPS,
  type PhotoEditHead, type PhotoEditVersionItem, type PhotoMaskRecord,
} from '../repositories/MediaEditRepository';
import { ADJUST_LAYER_KINDS, createImageAdjustLayerStepHandler } from '../pipeline/image/ImageAdjustLayerStepHandler';
import { getImageBlobStore } from '../pipeline/image/visionServices';
import { decidePhotoAccess, FILE_ID, photoChannelOf, requirePhotoRead } from '../media/photoAccess';
import { publishSignal } from '../lib/event-catalog';
import log from '../lib/logger';

export const mediaEditRepo = new MediaEditRepository();
const ID = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$/;
const MASK_KINDS = new Set(['people', 'subject', 'sky', 'brush']);

function userOf(req: Request): string {
  return req.user?.sub ?? 'unknown';
}

/** Ops as the pipeline accepts them: known kinds, finite numbers, bounded. */
export function validateOps(raw: unknown): Array<Record<string, unknown>> {
  if (!Array.isArray(raw)) throw new ValidationError('ops must be an array');
  if (raw.length > MAX_OPS) throw new ValidationError(`at most ${MAX_OPS} ops`);
  return raw.map((op, i) => {
    if (!op || typeof op !== 'object') throw new ValidationError(`ops[${i}] must be an object`);
    const o = op as Record<string, unknown>;
    if (typeof o.kind !== 'string' || !ADJUST_LAYER_KINDS.has(o.kind)) throw new ValidationError(`ops[${i}].kind is not an adjust-layer kind`);
    if (o.maskId !== undefined && (typeof o.maskId !== 'string' || !ID.test(o.maskId))) throw new ValidationError(`ops[${i}].maskId is invalid`);
    for (const [k, v] of Object.entries(o)) {
      if (k === 'kind' || k === 'maskId' || k === 'channel') continue;
      if (k === 'points') {
        if (!Array.isArray(v) || v.length > 16 || !v.every((p) => Array.isArray(p) && p.length === 2 && p.every((n) => Number.isFinite(n)))) {
          throw new ValidationError(`ops[${i}].points must be up to 16 [x, y] pairs`);
        }
        continue;
      }
      if (typeof v !== 'number' || !Number.isFinite(v)) throw new ValidationError(`ops[${i}].${k} must be a finite number`);
    }
    if (o.kind === 'curve' && !['rgb', 'r', 'g', 'b'].includes(String(o.channel))) throw new ValidationError(`ops[${i}].channel must be rgb, r, g or b`);
    return o;
  });
}

function publicMask(m: PhotoMaskRecord) {
  return { maskId: m.maskId, label: m.label, kind: m.kind, ...(m.subjects ? { subjects: m.subjects } : {}), polygons: m.polygons, blobHash: m.blobHash, origin: m.origin };
}

function publicHead(h: PhotoEditHead) {
  return { ...h, masks: h.masks.map(publicMask) };
}

async function announceKept(fileId: string, v: PhotoEditVersionItem): Promise<void> {
  try {
    await publishSignal(photoChannelOf(fileId), 'photo:edit_kept', {
      fileId,
      version: v.version,
      parentVersion: v.parentVersion,
      source: v.source,
      suggestionId: v.suggestionId ?? null,
      createdBy: v.createdBy,
      at: v.createdAt,
    });
  } catch (err) {
    log.error({ event: 'photo:edit_kept', err: err instanceof Error ? err.message : String(err) }, 'signal publish failed');
  }
}

/** Rasterize normalized polygons to an 8-bit PNG (white = inside) at the photo's aspect. */
export async function rasterizeMask(polygons: Array<Array<[number, number]>>, width: number, height: number): Promise<Buffer> {
  const pts = (poly: Array<[number, number]>) => poly.map(([x, y]) => `${(x * width).toFixed(1)},${(y * height).toFixed(1)}`).join(' ');
  const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}"><rect width="100%" height="100%" fill="#000"/>${
    polygons.map((p) => `<polygon points="${pts(p)}" fill="#fff"/>`).join('')}</svg>`;
  return sharp(Buffer.from(svg)).greyscale().png({ compressionLevel: 9 }).toBuffer();
}

export function createMediaEditsRouter(repo: MediaEditRepository = mediaEditRepo): Router {
  const r = Router();
  const guard = requirePhotoRead();

  r.get('/', asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const ids = String(req.query.fileIds ?? '').split(',').map((s) => s.trim()).filter((s) => FILE_ID.test(s));
    if (ids.length === 0) throw new ValidationError('fileIds is required');
    if (ids.length > 100) throw new ValidationError('at most 100 fileIds');
    const user = userOf(req);
    const readable = (await Promise.all(ids.map(async (id) => ((await decidePhotoAccess(id, user)).allow ? id : null)))).filter((x): x is string => !!x);
    const heads = await repo.getHeads(readable);
    res.json({ heads: heads.map(publicHead) });
  }));

  r.get('/:fileId', guard, asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const fileId = String(req.params.fileId);
    const before = Number(req.query.before) || undefined;
    const [head, page] = await Promise.all([repo.getHead(fileId), repo.listVersions(fileId, { limit: Number(req.query.limit) || 50, before })]);
    res.json({ head: publicHead(head), versions: page.versions, next: page.next, channel: photoChannelOf(fileId) });
  }));

  r.post('/:fileId/versions', guard, asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const fileId = String(req.params.fileId);
    const b = (req.body ?? {}) as Record<string, unknown>;
    const ops = validateOps(b.ops);
    if (b.parentVersion === null) b.parentVersion = 0; // "edited the original"
    if (typeof b.parentVersion !== 'number' || !Number.isInteger(b.parentVersion) || b.parentVersion < 0) throw new ValidationError('parentVersion must be the version you edited (0 = the original)');
    if (typeof b.suggestionId !== 'string' || !ID.test(b.suggestionId)) throw new ValidationError('suggestionId is required (one per proposal; a replay answers the same version)');
    const source = b.source === 'assistant' ? 'assistant' : 'user';
    const summary = typeof b.summary === 'string' ? b.summary.slice(0, 200) : '';
    try {
      const out = await repo.appendVersion({
        fileId, ops, parentVersion: b.parentVersion, summary, source,
        suggestionId: b.suggestionId,
        ...(typeof b.runId === 'string' && ID.test(b.runId) ? { runId: b.runId } : {}),
        userId: userOf(req),
      });
      if (!out.replay) await announceKept(fileId, out.version);
      res.status(out.replay ? 200 : 201).json({ head: publicHead(await repo.getHead(fileId)), version: out.version, replay: out.replay });
    } catch (err) {
      if (err instanceof PhotoEditConflict) { res.status(409).json({ error: 'version_conflict', currentVersion: err.currentVersion }); return; }
      throw err;
    }
  }));

  r.post('/:fileId/restore', guard, asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const fileId = String(req.params.fileId);
    const b = (req.body ?? {}) as Record<string, unknown>;
    if (typeof b.version !== 'number' || !Number.isInteger(b.version) || b.version < 0) throw new ValidationError('version is required (0 = the original)');
    if (b.parentVersion === null) b.parentVersion = 0;
    if (typeof b.parentVersion !== 'number' || !Number.isInteger(b.parentVersion)) throw new ValidationError('parentVersion is required');
    const from = b.version === 0 ? null : await repo.getVersion(fileId, b.version);
    if (b.version !== 0 && !from) throw new NotFoundError('version not found');
    try {
      const out = await repo.appendVersion({
        fileId,
        ops: from?.ops ?? [],
        parentVersion: b.parentVersion,
        summary: b.version === 0 ? 'Restored the original' : `Restored v${b.version}${from?.summary ? ` · ${from.summary}` : ''}`.slice(0, 200),
        source: 'restore',
        restoredFrom: b.version,
        ...(typeof b.suggestionId === 'string' && ID.test(b.suggestionId) ? { suggestionId: b.suggestionId } : {}),
        userId: userOf(req),
      });
      if (!out.replay) await announceKept(fileId, out.version);
      res.status(out.replay ? 200 : 201).json({ head: publicHead(await repo.getHead(fileId)), version: out.version, replay: out.replay });
    } catch (err) {
      if (err instanceof PhotoEditConflict) { res.status(409).json({ error: 'version_conflict', currentVersion: err.currentVersion }); return; }
      throw err;
    }
  }));

  r.put('/:fileId/pick', guard, asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const pick = (req.body ?? {}).pick;
    if (pick !== 'pick' && pick !== 'reject' && pick !== null) throw new ValidationError("pick must be 'pick', 'reject' or null");
    await repo.setPick(String(req.params.fileId), pick, userOf(req));
    res.json({ pick });
  }));

  r.put('/:fileId/masks', guard, asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const fileId = String(req.params.fileId);
    const raw = (req.body ?? {}).masks;
    if (!Array.isArray(raw) || raw.length > MAX_MASKS) throw new ValidationError(`masks must be an array of at most ${MAX_MASKS}`);
    const row = await mediaCatalogRepo.getFile(fileId);
    const aspect = row?.width && row?.height ? row.width / row.height : 1.5;
    const w = aspect >= 1 ? 512 : Math.round(512 * aspect);
    const h = aspect >= 1 ? Math.round(512 / aspect) : 512;
    const store = getImageBlobStore();
    const masks: PhotoMaskRecord[] = [];
    for (const [i, m] of raw.entries()) {
      const at = `masks[${i}]`;
      if (!m || typeof m !== 'object') throw new ValidationError(`${at} must be an object`);
      if (typeof m.maskId !== 'string' || !ID.test(m.maskId)) throw new ValidationError(`${at}.maskId is invalid`);
      if (typeof m.label !== 'string' || !m.label || m.label.length > 60) throw new ValidationError(`${at}.label is required`);
      if (!MASK_KINDS.has(m.kind)) throw new ValidationError(`${at}.kind must be people, subject, sky or brush`);
      const polys = m.polygons;
      if (!Array.isArray(polys) || polys.length === 0 || polys.length > 8
        || !polys.every((p: unknown) => Array.isArray(p) && p.length >= 3 && p.length <= 200
          && p.every((pt: unknown) => Array.isArray(pt) && pt.length === 2 && pt.every((n) => typeof n === 'number' && n >= 0 && n <= 1)))) {
        throw new ValidationError(`${at}.polygons must be 1–8 polygons of 3–200 normalized [x, y] points`);
      }
      const png = await rasterizeMask(polys, w, h);
      const blobHash = createHash('sha256').update(png).digest('hex');
      const imageId = `mask-${blobHash}`;
      if (!(await store.getMetadata(imageId))) {
        await store.put(imageId, png, { mimeType: 'image/png', sizeBytes: png.length, width: w, height: h, uploadedAt: new Date().toISOString(), uploadedBy: userOf(req) });
      }
      masks.push({
        maskId: m.maskId, label: m.label, kind: m.kind,
        ...(Number.isInteger(m.subjects) && m.subjects > 0 ? { subjects: m.subjects } : {}),
        polygons: polys, blobHash, origin: m.origin === 'brush' ? 'brush' : 'seed',
      });
    }
    await repo.setMasks(fileId, masks, userOf(req));
    res.json({ masks: masks.map(publicMask) });
  }));

  r.post('/:fileId/render', guard, asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const fileId = String(req.params.fileId);
    const version = Number((req.body ?? {}).version);
    const v = await repo.getVersion(fileId, version);
    if (!v) throw new NotFoundError('version not found');
    if (v.renderedImageId) { res.json({ renderedImageId: v.renderedImageId }); return; }
    const row = await mediaCatalogRepo.getFile(fileId) as (Awaited<ReturnType<typeof mediaCatalogRepo.getFile>> & { previewImageId?: string | null }) | null;
    const source = row?.previewImageId ?? row?.imageId;
    if (!source) throw new AppError(422, 'the photo has no stored pixels to render');
    const head = await repo.getHead(fileId);
    const masks = Object.fromEntries(head.masks.map((m) => [m.maskId, `mask-${m.blobHash}`]));
    const handler = createImageAdjustLayerStepHandler({ imageBlobStore: getImageBlobStore() });
    const outcome = await handler.execute(
      { id: 'render', type: 'image:adjust-layer', data: { type: 'image:adjust-layer', layers: v.ops, masks } } as never,
      { imageId: source },
      { runId: `photo-render-${fileId.slice(0, 12)}-v${version}`, stepId: 'render', emit: async () => {} } as never,
    );
    if (outcome.status !== 'completed') throw new AppError(500, (outcome as { error?: string }).error ?? 'render failed');
    const imageId = String((outcome.output as Record<string, unknown>).imageId);
    await repo.setRendered(fileId, version, imageId);
    res.json({ renderedImageId: imageId });
  }));

  return r;
}
