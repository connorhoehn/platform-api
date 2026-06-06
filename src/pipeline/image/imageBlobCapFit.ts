// platform-api/src/pipeline/image/imageBlobCapFit.ts
//
// Shared helper that compresses raw image bytes to fit within the
// DDB inline cap (IMAGE_BLOB_SIZE_CAP_BYTES = 350 KB).
//
// Strategy (mirrors the DDB cap contract):
//   1. Sharp resize to fit-inside 512×512 + PNG compressionLevel:9.
//   2. If still over cap, re-encode as JPEG quality:80 (loses alpha).
//   3. If still over cap → throw with a clear message so callers can
//      surface a non-throwing failure path.
//
// Returns `{ bytes, width, height, mimeType }` on success.
// Throws `Error` (message: 'image:generate: generated image too large
// after compression') when all strategies fail.
//
// Used by: ImageGenerateStepHandler, ImageVariationStepHandler,
// ImageEditPromptStepHandler.

import sharp from 'sharp';
import { IMAGE_BLOB_SIZE_CAP_BYTES } from './ImageBlobStore';

const CAP_FIT_MAX_DIMENSION = 512;

export interface CapFitResult {
  bytes: Buffer;
  width: number;
  height: number;
  mimeType: string;
}

/**
 * Compress `raw` so it fits within IMAGE_BLOB_SIZE_CAP_BYTES.
 * `hint` is a label for error messages (e.g. 'image:generate').
 *
 * Throws when all strategies are exhausted.
 */
export async function capFitImage(raw: Buffer, hint: string): Promise<CapFitResult> {
  // ── Attempt 1: resize to 512×512-inside + PNG compressionLevel:9 ─────────
  const attempt1 = await sharp(raw)
    .resize({
      width: CAP_FIT_MAX_DIMENSION,
      height: CAP_FIT_MAX_DIMENSION,
      fit: 'inside',
      withoutEnlargement: true,
    })
    .png({ compressionLevel: 9 })
    .toBuffer({ resolveWithObject: true });

  if (attempt1.data.byteLength <= IMAGE_BLOB_SIZE_CAP_BYTES) {
    return {
      bytes: attempt1.data,
      width: attempt1.info.width,
      height: attempt1.info.height,
      mimeType: 'image/png',
    };
  }

  // ── Attempt 2: JPEG quality:80 ─────────────────────────────────────────────
  const attempt2 = await sharp(raw)
    .resize({
      width: CAP_FIT_MAX_DIMENSION,
      height: CAP_FIT_MAX_DIMENSION,
      fit: 'inside',
      withoutEnlargement: true,
    })
    .jpeg({ quality: 80 })
    .toBuffer({ resolveWithObject: true });

  if (attempt2.data.byteLength <= IMAGE_BLOB_SIZE_CAP_BYTES) {
    return {
      bytes: attempt2.data,
      width: attempt2.info.width,
      height: attempt2.info.height,
      mimeType: 'image/jpeg',
    };
  }

  // ── All strategies failed ──────────────────────────────────────────────────
  throw new Error(
    `${hint}: generated image too large after compression (${attempt2.data.byteLength}B > ${IMAGE_BLOB_SIZE_CAP_BYTES}B cap)`,
  );
}
