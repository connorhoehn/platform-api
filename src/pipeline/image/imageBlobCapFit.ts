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

export interface CapFitOptions {
  /** Largest edge to resize-inside before compressing. Default 512. */
  maxDimension?: number;
  /** Byte ceiling. Default IMAGE_BLOB_SIZE_CAP_BYTES (350KB). */
  capBytes?: number;
}

/**
 * Compress `raw` so it fits within the byte cap. `hint` is a label for
 * error messages (e.g. 'image:generate').
 *
 * Strategy: PNG at maxDimension → JPEG q80 → progressively step the JPEG
 * quality down (65, 50, 35) before giving up. The quality-ladder lets a
 * larger maxDimension (e.g. an upscaled 1024) still land under the cap
 * instead of throwing the moment q80 is too big.
 *
 * Throws when all strategies are exhausted.
 */
export async function capFitImage(
  raw: Buffer,
  hint: string,
  opts: CapFitOptions = {},
): Promise<CapFitResult> {
  const maxDimension = opts.maxDimension ?? CAP_FIT_MAX_DIMENSION;
  const capBytes = opts.capBytes ?? IMAGE_BLOB_SIZE_CAP_BYTES;
  const resize = {
    width: maxDimension,
    height: maxDimension,
    fit: 'inside' as const,
    withoutEnlargement: true,
  };

  // ── Attempt 1: PNG compressionLevel:9 ──────────────────────────────────────
  const attempt1 = await sharp(raw)
    .resize(resize)
    .png({ compressionLevel: 9 })
    .toBuffer({ resolveWithObject: true });

  if (attempt1.data.byteLength <= capBytes) {
    return {
      bytes: attempt1.data,
      width: attempt1.info.width,
      height: attempt1.info.height,
      mimeType: 'image/png',
    };
  }

  // ── Attempts 2..n: JPEG quality ladder ─────────────────────────────────────
  let last = attempt1.data.byteLength;
  for (const quality of [80, 65, 50, 35]) {
    const attempt = await sharp(raw)
      .resize(resize)
      .jpeg({ quality })
      .toBuffer({ resolveWithObject: true });
    last = attempt.data.byteLength;
    if (attempt.data.byteLength <= capBytes) {
      return {
        bytes: attempt.data,
        width: attempt.info.width,
        height: attempt.info.height,
        mimeType: 'image/jpeg',
      };
    }
  }

  // ── All strategies failed ──────────────────────────────────────────────────
  throw new Error(
    `${hint}: image too large after compression (${last}B > ${capBytes}B cap)`,
  );
}
