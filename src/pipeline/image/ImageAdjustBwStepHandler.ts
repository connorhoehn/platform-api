// platform-api/src/pipeline/image/ImageAdjustBwStepHandler.ts
//
// Custom step handler (Phase 3) for `node.data.type === 'image:adjust-bw'`.
// Converts a source image to black-and-white via one of three algorithms,
// optionally applies a contrast adjustment, and writes the result to the
// ImageBlobStore as a new imageId.
//
// === Algorithm notes ===
// - 'luminosity' (default): Sharp's `.grayscale()` — ITU-R BT.709 weighted
//   luminance. Best perceptual result for photos.
// - 'desaturate': `.modulate({ saturation: 0 })` — drives chrominance to zero
//   but stays in RGB colour-space. The result looks the same as grayscale on
//   screen but the pixel values differ from BT.709 luminance.
// - 'average': `.removeAlpha().toColourspace('b-w')` — equal-weight average
//   of R+G+B channels (libvips 'b-w' colourspace).
//
// Contrast is applied via a linear tone-curve after grayscale conversion:
//   output = contrast * input + -(contrast - 1) * 128
// This maps 128 (mid-grey) to itself and stretches/compresses around it.
// When contrast === 1.0 the adjustment is skipped (identity).
//
// Non-throwing: bad sourceImagePath, missing blob, Sharp error → status:'failed'.

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

export type BwAlgorithm = 'luminosity' | 'average' | 'desaturate';

interface ImageAdjustBwNodeData {
  type: 'image:adjust-bw';
  /**
   * JSONPath to the source imageId in the context.
   * Default fallback chain: $.scatter.parentImageId → $.trigger.imageRef → $.trigger.imageId
   */
  sourceImagePath?: string;
  /** Greyscale conversion algorithm. Default: 'luminosity'. */
  algorithm?: BwAlgorithm;
  /**
   * Linear contrast multiplier, 0.5–2.0. Default: 1.0 (no adjustment).
   * Applied as: output = contrast * input + -(contrast - 1) * 128
   */
  contrast?: number;
}

export interface ImageAdjustBwStepHandlerOpts {
  imageBlobStore: ImageBlobStore;
}

const DEFAULT_SOURCE_PATH = '$.scatter.parentImageId';
const FALLBACK_SOURCE_PATHS = ['$.trigger.imageRef', '$.trigger.imageId'];

export function createImageAdjustBwStepHandler(
  opts: ImageAdjustBwStepHandlerOpts,
): StepHandler {
  return {
    kind: 'image:adjust-bw',
    async execute(
      node: PipelineNode,
      context: Record<string, unknown>,
      helpers: StepHandlerHelpers,
    ): Promise<StepOutcome> {
      const data = node.data as unknown as ImageAdjustBwNodeData;
      const sourcePath = data.sourceImagePath ?? DEFAULT_SOURCE_PATH;
      const algorithm: BwAlgorithm = data.algorithm ?? 'luminosity';
      const contrast = data.contrast ?? 1.0;

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
          error: `image:adjust-bw: failed to resolve source imageId at '${sourcePath}' (or fallbacks)`,
        };
      }

      // ── Fetch source bytes ─────────────────────────────────────────────
      const source = await opts.imageBlobStore.get(imageId as string);
      if (!source) {
        return {
          status: 'failed',
          error: `image:adjust-bw: source image '${imageId}' not found in blob store`,
        };
      }

      // ── Apply Sharp transformation ─────────────────────────────────────
      let outBytes: Buffer;
      let outInfo: { width: number; height: number; format: string };
      try {
        let pipeline = sharp(source.body);

        switch (algorithm) {
          case 'luminosity':
            pipeline = pipeline.grayscale();
            break;
          case 'desaturate':
            pipeline = pipeline.modulate({ saturation: 0 });
            break;
          case 'average':
            pipeline = pipeline.removeAlpha().toColourspace('b-w');
            break;
          default: {
            // TypeScript exhaustiveness guard; runtime defence for bad node data.
            const _: never = algorithm;
            void _;
            pipeline = pipeline.grayscale();
          }
        }

        // Apply contrast via linear tone-curve only when non-trivially != 1.0.
        if (Math.abs(contrast - 1.0) > 1e-9) {
          pipeline = pipeline.linear(contrast, -(contrast - 1) * 128);
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
          error: `image:adjust-bw: Sharp transform failed: ${
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
          error: `image:adjust-bw: blob-store put failed: ${
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
