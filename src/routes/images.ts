// platform-api/src/routes/images.ts
//
// HTTP surface for the image:vision-analyze step's data plane.
//
// Endpoints:
//   POST /api/images
//     Body: raw image bytes (up to 5 MB before resize)
//     Content-Type: image/jpeg | image/png | image/webp
//     Server-side: re-encode + EXIF strip + dimension extract via Sharp;
//                  reject if final bytes exceed IMAGE_BLOB_SIZE_CAP_BYTES.
//     Response: { imageId, mimeType, width, height, sizeBytes }
//
//   GET /api/images/:imageId
//     Returns raw bytes with Content-Type from metadata.
//     404 when not found, 500 on store failure.
//
//   GET /api/images/:imageId/metadata
//     Returns { imageId, mimeType, sizeBytes, width, height, uploadedAt }
//     Cheaper than the body fetch — for canvas thumbnails / listings.
//
// Mounted from src/routes/index.ts under `/api/images`. Auth is
// applied by the `requireAuth` middleware in app.ts (mounted before
// the `/api/*` router) — no per-route auth logic here.
//
// Code parity with platform-api/src/routes/judgments.ts.

import { Router, raw } from 'express';
import { randomUUID } from 'crypto';
import sharp from 'sharp';
import { asyncHandler, ValidationError } from '../middleware/error-handler';
import { withContext } from '../lib/logger';
import { getImageBlobStore } from '../pipeline/image/visionServices';
import { IMAGE_BLOB_SIZE_CAP_BYTES } from '../pipeline/image/ImageBlobStore';
import type { ImageMeta } from '../pipeline/image/types';

const log = withContext({ route: 'images' });

/** Raw-body cap BEFORE server-side resize. Larger than the storage cap
 *  to give the resize step room to bring a 4MP phone photo down. */
const RAW_UPLOAD_CAP_BYTES = 5 * 1024 * 1024;

/** Long-edge cap for server-side resize. Matches Anthropic's
 *  `width * height / 750` token-cost sweet spot. */
const RESIZE_LONG_EDGE = 1568;

/** MIME types we accept on the upload route. */
const ALLOWED_MIME = new Set(['image/jpeg', 'image/png', 'image/webp']);

interface UploadResponse {
  imageId: string;
  mimeType: string;
  sizeBytes: number;
  width: number;
  height: number;
}

interface MetadataResponse extends ImageMeta {
  imageId: string;
}

export const imagesRouter = Router({ mergeParams: true });

// ---------------------------------------------------------------------------
// POST /api/images — raw-body upload
// ---------------------------------------------------------------------------

imagesRouter.post(
  '/',
  raw({ type: () => true, limit: RAW_UPLOAD_CAP_BYTES }),
  asyncHandler<
    Record<string, never>,
    UploadResponse | { error: string }
  >(async (req, res) => {
    const userId = req.user?.sub ?? 'anonymous';
    const contentType = (req.headers['content-type'] ?? '').split(';')[0]?.trim().toLowerCase();
    if (!contentType || !ALLOWED_MIME.has(contentType)) {
      throw new ValidationError(`Content-Type must be one of ${[...ALLOWED_MIME].join(', ')} — got '${contentType ?? '(none)'}'`);
    }
    const body = req.body;
    if (!Buffer.isBuffer(body) || body.byteLength === 0) {
      throw new ValidationError('request body is empty or not a Buffer (use Content-Type: image/* + raw bytes)');
    }

    const reqLog = log.child({ userId, contentType, inputBytes: body.byteLength });

    // Re-encode via Sharp:
    //   - Strips EXIF + ICC + other metadata (privacy + security; EXIF
    //     can carry GPS coordinates)
    //   - Decodes + re-encodes invalidates polyglot files (JPEG+JS
    //     hybrid, embedded SVG)
    //   - Resizes to long-edge 1568 (Anthropic sweet spot)
    //   - Detects + extracts dimensions
    // Throws on unsupported / corrupted input — caller gets a clean 400.
    let processed: Buffer;
    let metadata: sharp.Metadata;
    try {
      const pipeline = sharp(body, { limitInputPixels: 25_000_000 })
        .rotate()                                    // honor EXIF orientation, then strip below
        .resize({
          width: RESIZE_LONG_EDGE,
          height: RESIZE_LONG_EDGE,
          fit: 'inside',
          withoutEnlargement: true,
        });

      // Re-encode to the same family format. Always JPEG quality 85 for
      // image/jpeg input; PNG/WebP keep their format.
      const targetMime = contentType;
      if (targetMime === 'image/jpeg') {
        processed = await pipeline.jpeg({ quality: 85, mozjpeg: true }).toBuffer();
      } else if (targetMime === 'image/png') {
        processed = await pipeline.png({ compressionLevel: 8 }).toBuffer();
      } else {
        processed = await pipeline.webp({ quality: 85 }).toBuffer();
      }
      metadata = await sharp(processed).metadata();
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      reqLog.warn({ err: msg }, 'image decode/re-encode failed');
      throw new ValidationError(`could not decode image: ${msg}`);
    }

    if (processed.byteLength > IMAGE_BLOB_SIZE_CAP_BYTES) {
      reqLog.warn(
        { processedBytes: processed.byteLength, cap: IMAGE_BLOB_SIZE_CAP_BYTES },
        'image exceeds storage cap after resize',
      );
      res.status(413).json({
        error: `image still exceeds storage cap (${IMAGE_BLOB_SIZE_CAP_BYTES} bytes) after server-side resize. Pre-compress on the client.`,
      });
      return;
    }

    const imageId = randomUUID();
    const meta: ImageMeta = {
      mimeType: contentType,
      sizeBytes: processed.byteLength,
      width: metadata.width ?? 0,
      height: metadata.height ?? 0,
      uploadedAt: new Date().toISOString(),
      uploadedBy: userId,
    };

    try {
      await getImageBlobStore().put(imageId, processed, meta);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      reqLog.error({ err: msg, imageId }, 'image blob store put failed');
      res.status(500).json({ error: `image storage failed: ${msg}` });
      return;
    }

    reqLog.info({ imageId, outputBytes: processed.byteLength, width: meta.width, height: meta.height }, 'image uploaded');
    res.status(201).json({
      imageId,
      mimeType: meta.mimeType,
      sizeBytes: meta.sizeBytes,
      width: meta.width,
      height: meta.height,
    });
  }),
);

// ---------------------------------------------------------------------------
// GET /api/images/:imageId — raw bytes
// ---------------------------------------------------------------------------

imagesRouter.get(
  '/:imageId',
  asyncHandler<
    { imageId: string },
    Buffer | { error: string }
  >(async (req, res) => {
    const { imageId } = req.params;
    if (!imageId) {
      throw new ValidationError('imageId is required');
    }
    const userId = req.user?.sub ?? 'anonymous';
    const reqLog = log.child({ userId, imageId });

    const row = await getImageBlobStore().get(imageId);
    if (!row) {
      reqLog.info('image not found');
      res.status(404).json({ error: 'image not found' });
      return;
    }

    res.status(200)
      .setHeader('Content-Type', row.meta.mimeType)
      .setHeader('Content-Length', String(row.meta.sizeBytes))
      .setHeader('Cache-Control', 'private, max-age=3600')
      .send(row.body);
  }),
);

// ---------------------------------------------------------------------------
// GET /api/images/:imageId/metadata — JSON metadata only
// ---------------------------------------------------------------------------

imagesRouter.get(
  '/:imageId/metadata',
  asyncHandler<
    { imageId: string },
    MetadataResponse | { error: string }
  >(async (req, res) => {
    const { imageId } = req.params;
    if (!imageId) {
      throw new ValidationError('imageId is required');
    }
    const userId = req.user?.sub ?? 'anonymous';
    const reqLog = log.child({ userId, imageId });

    const meta = await getImageBlobStore().getMetadata(imageId);
    if (!meta) {
      reqLog.info('image metadata not found');
      res.status(404).json({ error: 'image not found' });
      return;
    }

    res.status(200).json({ imageId, ...meta });
  }),
);
