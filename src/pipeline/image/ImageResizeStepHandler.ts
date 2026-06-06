// platform-api/src/pipeline/image/ImageResizeStepHandler.ts
//
// Custom step handler for `node.data.type === 'image:resize'`.
// Resizes a source image via Sharp's resize API, writes the result to the
// ImageBlobStore as a new imageId, and emits `pipeline:step:image:produced`.
//
// === Resize semantics ===
// Uses Sharp's built-in fit modes:
//   cover   — fills the target box, cropping excess (aspect-preserving crop)
//   contain — fits within the target box, padding with background colour
//   fill    — stretches to exact dimensions, ignoring aspect ratio
//   inside  — (default) scales down so the entire image fits within box
//   outside — scales up/down so the entire image covers the box dimensions
//
// `withoutEnlargement` (default true) prevents Sharp from upscaling when
// the source is already smaller than the target.
//
// `background` (default '#ffffff') is used for `contain` padding when the
// source aspect ratio doesn't match the target box. Ignored by other fits.
//
// Both `width` and `height` may be omitted together only if one imposes a
// real constraint (Sharp requires at least one); the handler fails early
// with a clear message when both are absent.
//
// Non-throwing: bad config, missing source, Sharp error → status:'failed'.

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

export interface ImageResizeNodeData {
  type: 'image:resize';
  /**
   * JSONPath to the source imageId in the context.
   * Default fallback chain: $.scatter.parentImageId → $.trigger.imageRef → $.trigger.imageId
   */
  sourceImagePath?: string;
  /** Target width in pixels. At least one of width or height must be provided. */
  width?: number;
  /** Target height in pixels. At least one of width or height must be provided. */
  height?: number;
  /**
   * Sharp fit mode. Controls how the image is scaled to the target box.
   * Default: 'inside' (scale down to fit, never crop, no upscale by default).
   */
  fit?: 'cover' | 'contain' | 'fill' | 'inside' | 'outside';
  /**
   * When true (default), prevent enlarging images smaller than the target
   * dimensions. Has no effect for 'cover' fit which always fills the target.
   */
  withoutEnlargement?: boolean;
  /**
   * Background colour for 'contain' padding. CSS hex or named colour.
   * Default: '#ffffff'. Ignored by other fit modes.
   */
  background?: string;
}

export interface ImageResizeStepHandlerOpts {
  imageBlobStore: ImageBlobStore;
}

const DEFAULT_SOURCE_PATH = '$.scatter.parentImageId';
const FALLBACK_SOURCE_PATHS = ['$.trigger.imageRef', '$.trigger.imageId'];

export function createImageResizeStepHandler(
  opts: ImageResizeStepHandlerOpts,
): StepHandler {
  return {
    kind: 'image:resize',
    async execute(
      node: PipelineNode,
      context: Record<string, unknown>,
      helpers: StepHandlerHelpers,
    ): Promise<StepOutcome> {
      const data = node.data as unknown as ImageResizeNodeData;
      const sourcePath = data.sourceImagePath ?? DEFAULT_SOURCE_PATH;
      const fit = data.fit ?? 'inside';
      const withoutEnlargement = data.withoutEnlargement ?? true;
      const background = data.background ?? '#ffffff';

      // ── Validate config ────────────────────────────────────────────────
      if (data.width === undefined && data.height === undefined) {
        return {
          status: 'failed',
          error: 'image:resize: both width and height are omitted — at least one must be provided',
        };
      }

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
          error: `image:resize: failed to resolve source imageId at '${sourcePath}' (or fallbacks)`,
        };
      }

      // ── Fetch source bytes ─────────────────────────────────────────────
      const source = await opts.imageBlobStore.get(imageId as string);
      if (!source) {
        return {
          status: 'failed',
          error: `image:resize: source image '${imageId}' not found in blob store`,
        };
      }

      // ── Apply Sharp resize ─────────────────────────────────────────────
      let outBytes: Buffer;
      let outInfo: { width: number; height: number; format: string };
      try {
        const result = await sharp(source.body)
          .resize({
            width: data.width,
            height: data.height,
            fit,
            withoutEnlargement,
            background,
          })
          .png()
          .toBuffer({ resolveWithObject: true });
        outBytes = result.data;
        outInfo = {
          width: result.info.width,
          height: result.info.height,
          format: result.info.format,
        };
      } catch (err) {
        return {
          status: 'failed',
          error: `image:resize: Sharp resize failed: ${
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
          error: `image:resize: blob-store put failed: ${
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
