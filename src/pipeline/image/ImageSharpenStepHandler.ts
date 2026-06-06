// platform-api/src/pipeline/image/ImageSharpenStepHandler.ts
//
// Custom step handler for `node.data.type === 'image:sharpen'`.
// Applies unsharp masking via Sharp's `.sharpen()` to a source image and
// writes the result to the ImageBlobStore as a new imageId.
//
// === Sharpen parameters ===
//   sigma — standard deviation of the Gaussian kernel. Range: 0.5-3.0.
//           Values below 0.5 are clamped to 0.5; above 3.0 to 3.0.
//           sigma=0 is treated as a passthrough (no-op re-encode) — don't
//           fail, just skip the sharpen call. Default: 1.0.
//   m1    — 'flat' area sharpening amount. Default: 1.0.
//   m2    — 'jagged' area sharpening amount. Default: 2.0.
//
// Sharp's .sharpen({ sigma, m1, m2 }) maps to libvips usm with those
// weights. sigma=1.0, m1=1.0, m2=2.0 is a mild, universally safe preset.
//
// Non-throwing: bad source, missing blob, Sharp error → status:'failed'.

import { randomUUID } from 'crypto';
import sharp from 'sharp';
import type {
  PipelineNode,
  StepHandler,
  StepHandlerHelpers,
  StepOutcome,
} from 'distributed-core/applications/pipeline';
import type { ImageBlobStore } from './ImageBlobStore';
import type { ImageMeta } from './types';
import { readPath } from './ScatterStepHandler';

export interface ImageSharpenNodeData {
  type: 'image:sharpen';
  /**
   * JSONPath to the source imageId in the context.
   * Default fallback chain: $.scatter.parentImageId → $.trigger.imageRef → $.trigger.imageId
   */
  sourceImagePath?: string;
  /**
   * Gaussian sigma. Range 0.5-3.0, clamped. Default: 1.0.
   * sigma=0 is a no-op pass-through (re-encodes without sharpening).
   */
  sigma?: number;
  /** Flat-area sharpening weight. Default: 1.0. */
  m1?: number;
  /** Jagged-area sharpening weight. Default: 2.0. */
  m2?: number;
}

export interface ImageSharpenStepHandlerOpts {
  imageBlobStore: ImageBlobStore;
}

const DEFAULT_SOURCE_PATH = '$.scatter.parentImageId';
const FALLBACK_SOURCE_PATHS = ['$.trigger.imageRef', '$.trigger.imageId'];

const SIGMA_MIN = 0.5;
const SIGMA_MAX = 3.0;

function clampSigma(s: number): number {
  return Math.min(SIGMA_MAX, Math.max(SIGMA_MIN, s));
}

export function createImageSharpenStepHandler(
  opts: ImageSharpenStepHandlerOpts,
): StepHandler {
  return {
    kind: 'image:sharpen',
    async execute(
      node: PipelineNode,
      context: Record<string, unknown>,
      helpers: StepHandlerHelpers,
    ): Promise<StepOutcome> {
      const data = node.data as unknown as ImageSharpenNodeData;
      const sourcePath = data.sourceImagePath ?? DEFAULT_SOURCE_PATH;
      const sigmaRaw = data.sigma ?? 1.0;
      const m1 = data.m1 ?? 1.0;
      const m2 = data.m2 ?? 2.0;

      // sigma=0 is an explicit no-op signal; other values are clamped.
      const sigma = sigmaRaw === 0 ? 0 : clampSigma(sigmaRaw);

      // ── Resolve source imageId ─────────────────────────────────────────
      let imageId = readPath(context, sourcePath);
      if (typeof imageId !== 'string' || !imageId) {
        for (const fb of FALLBACK_SOURCE_PATHS) {
          const candidate = readPath(context, fb);
          if (typeof candidate === 'string' && candidate) {
            imageId = candidate;
            break;
          }
        }
      }
      if (typeof imageId !== 'string' || !imageId) {
        return {
          status: 'failed',
          error: `image:sharpen: failed to resolve source imageId at '${sourcePath}' (or fallbacks)`,
        };
      }

      // ── Fetch source bytes ─────────────────────────────────────────────
      const source = await opts.imageBlobStore.get(imageId as string);
      if (!source) {
        return {
          status: 'failed',
          error: `image:sharpen: source image '${imageId}' not found in blob store`,
        };
      }

      // ── Apply Sharp sharpen ────────────────────────────────────────────
      let outBytes: Buffer;
      let outInfo: { width: number; height: number; format: string };
      try {
        let pipeline = sharp(source.body);

        // sigma=0 → no-op: re-encode without sharpening.
        if (sigma !== 0) {
          pipeline = pipeline.sharpen({ sigma, m1, m2 });
        }

        const result = await pipeline.png().toBuffer({ resolveWithObject: true });
        outBytes = result.data;
        outInfo = {
          width: result.info.width,
          height: result.info.height,
          format: result.info.format,
        };
      } catch (err) {
        return {
          status: 'failed',
          error: `image:sharpen: Sharp transform failed: ${
            err instanceof Error ? err.message : String(err)
          }`,
        };
      }

      // ── Persist result ─────────────────────────────────────────────────
      const newImageId = randomUUID();
      const mimeType = 'image/png';
      const meta: ImageMeta = {
        mimeType,
        sizeBytes: outBytes.length,
        width: outInfo.width,
        height: outInfo.height,
        uploadedAt: new Date().toISOString(),
      };
      try {
        await opts.imageBlobStore.put(newImageId, outBytes, meta);
      } catch (err) {
        return {
          status: 'failed',
          error: `image:sharpen: blob-store put failed: ${
            err instanceof Error ? err.message : String(err)
          }`,
        };
      }

      // ── Emit image:produced ────────────────────────────────────────────
      try {
        await helpers.emit('pipeline:step:image:produced', {
          runId: helpers.runId,
          stepId: helpers.stepId,
          imageId: newImageId,
          width: outInfo.width,
          height: outInfo.height,
          mimeType,
          at: new Date().toISOString(),
        });
      } catch {
        // never let a bad subscriber kill the step
      }

      return {
        status: 'completed',
        output: {
          imageId: newImageId,
          parentImageId: imageId,
          width: outInfo.width,
          height: outInfo.height,
          mimeType,
          materialized: true,
        },
      };
    },
  };
}
