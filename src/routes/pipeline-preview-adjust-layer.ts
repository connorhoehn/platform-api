// platform-api/src/routes/pipeline-preview-adjust-layer.ts
//
// POST /api/pipeline/preview/adjust-layer
//
// Live preview for the image:adjust-layer node config panel. Applies a
// supplied layer stack to a stored image and returns the result as a data URL.
//
// Differences from the full ImageAdjustLayerStepHandler path:
//   - No blob-store write — preview is ephemeral, base64 in the response.
//   - No audit log, no run record, no pipeline:step:image:produced event.
//   - Hard cap on input image size (5 MB raw — after blob-store fetch).
//   - Rate limit: 30 calls/min per authed user (client-side debounce keeps
//     steady-state well under 2/s; this is a safety ceiling).
//
// Auth: requireAuth (same as every other /api/pipeline/* route).

import { Router } from 'express';
import sharp from 'sharp';
import { asyncHandler, ValidationError, NotFoundError } from '../middleware/error-handler';
import { createRateLimiter } from '../middleware/rateLimit';
import { requireAuth } from '../middleware/auth';
import { getImageBlobStore } from '../pipeline/image/visionServices';
import type { AdjustLayer } from '../pipeline/image/ImageAdjustLayerStepHandler';

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/** 5 MB ceiling on the raw image bytes we'll run through Sharp. */
const INPUT_SIZE_CAP_BYTES = 5 * 1024 * 1024;

/** Valid layer kind discriminators. */
const VALID_KINDS = new Set(['brightness', 'contrast', 'saturation', 'hue', 'blur']);

// ---------------------------------------------------------------------------
// Rate limiter — 30 calls/min per authed user.
// ---------------------------------------------------------------------------

const previewAdjustRateLimit = createRateLimiter({
  capacity: 30,
  refillRate: 30,
  refillIntervalMs: 60_000,
  scope: 'pipeline:preview:adjust-layer',
  key: (req) => req.user?.sub ?? req.ip ?? 'anon',
});

// ---------------------------------------------------------------------------
// Layer validation
// ---------------------------------------------------------------------------

/** Returns an error string on invalid input; undefined on success. */
function validateLayers(raw: unknown): string | undefined {
  if (!Array.isArray(raw)) return 'layers must be an array';
  for (let i = 0; i < raw.length; i++) {
    const layer = raw[i] as Record<string, unknown> | null | undefined;
    if (!layer || typeof layer !== 'object') {
      return `layers[${i}]: must be an object`;
    }
    if (typeof layer['kind'] !== 'string' || !VALID_KINDS.has(layer['kind'])) {
      return `layers[${i}]: unknown kind '${String(layer['kind'])}'`;
    }
    const kind = layer['kind'] as string;
    if (kind === 'hue') {
      if (typeof layer['degrees'] !== 'number') {
        return `layers[${i}] (hue): degrees must be a number`;
      }
    } else if (kind === 'blur') {
      if (typeof layer['radiusPx'] !== 'number') {
        return `layers[${i}] (blur): radiusPx must be a number`;
      }
    } else {
      if (typeof layer['value'] !== 'number') {
        return `layers[${i}] (${kind}): value must be a number`;
      }
    }
  }
  return undefined;
}

// ---------------------------------------------------------------------------
// Sharp pipeline builder — extracted so it's testable in isolation.
// ---------------------------------------------------------------------------

export async function applyAdjustLayers(
  sourceBytes: Buffer,
  layers: AdjustLayer[],
): Promise<{ data: Buffer; width: number; height: number }> {
  let pipeline = sharp(sourceBytes);

  for (const layer of layers) {
    switch (layer.kind) {
      case 'brightness':
        pipeline = pipeline.modulate({ brightness: layer.value });
        break;
      case 'contrast':
        pipeline = pipeline.linear(layer.value, -(layer.value - 1) * 128);
        break;
      case 'saturation':
        pipeline = pipeline.modulate({ saturation: layer.value });
        break;
      case 'hue':
        pipeline = pipeline.modulate({ hue: layer.degrees });
        break;
      case 'blur':
        pipeline = pipeline.blur(layer.radiusPx);
        break;
    }
  }

  const result = await pipeline.png().toBuffer({ resolveWithObject: true });
  return {
    data: result.data,
    width: result.info.width,
    height: result.info.height,
  };
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

interface PreviewAdjustLayerRequestBody {
  imageId?: string;
  layers?: AdjustLayer[];
}

interface PreviewAdjustLayerResponse {
  dataUrl: string;
  width: number;
  height: number;
  mimeType: string;
}

export const pipelinePreviewAdjustLayerRouter = Router({ mergeParams: true });

pipelinePreviewAdjustLayerRouter.post(
  '/',
  requireAuth,
  previewAdjustRateLimit,
  asyncHandler<
    Record<string, never>,
    PreviewAdjustLayerResponse | { error: string }
  >(async (req, res) => {
    const body = req.body as PreviewAdjustLayerRequestBody | undefined;

    // ── Validate request body ─────────────────────────────────────────────
    const { imageId, layers } = body ?? {};

    if (!imageId || typeof imageId !== 'string') {
      throw new ValidationError('imageId is required');
    }

    const layerValidationError = validateLayers(layers);
    if (layerValidationError) {
      throw new ValidationError(layerValidationError);
    }

    const safeL = (layers ?? []) as AdjustLayer[];

    // ── Fetch source image bytes ──────────────────────────────────────────
    const store = getImageBlobStore();
    const source = await store.get(imageId);

    if (!source) {
      throw new NotFoundError(`image '${imageId}' not found`);
    }

    if (source.body.byteLength > INPUT_SIZE_CAP_BYTES) {
      res.status(413).json({ error: `source image exceeds ${INPUT_SIZE_CAP_BYTES} byte preview limit` });
      return;
    }

    // ── Apply layers via Sharp ────────────────────────────────────────────
    let outData: Buffer;
    let width: number;
    let height: number;

    try {
      const result = await applyAdjustLayers(source.body, safeL);
      outData = result.data;
      width = result.width;
      height = result.height;
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      throw new ValidationError(`Sharp transform failed: ${msg}`);
    }

    // ── Encode as data URL ────────────────────────────────────────────────
    const mimeType = 'image/png';
    const dataUrl = `data:${mimeType};base64,${outData.toString('base64')}`;

    res.status(200).json({ dataUrl, width, height, mimeType });
  }),
);
