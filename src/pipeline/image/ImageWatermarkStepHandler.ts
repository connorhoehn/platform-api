// platform-api/src/pipeline/image/ImageWatermarkStepHandler.ts
//
// Custom step handler for `node.data.type === 'image:watermark'`.
// Composites a watermark/logo image on top of a source image using Sharp's
// `.composite()` API, writing the result to the ImageBlobStore as a new imageId.
//
// === Composite semantics ===
// The overlay is fetched from the blob store by `overlayImageId`, pre-processed
// for opacity, and composited over the source via Sharp's `blend: 'over'` mode.
//
// Opacity is applied to the overlay before compositing by calling
//   sharp(overlayBytes).ensureAlpha(opacity).toBuffer()
// which sets the global alpha channel of every pixel in the overlay to `opacity`,
// giving a uniform transparency effect (e.g. 0.8 → semi-transparent logo).
//
// The source also goes through `.ensureAlpha()` so the composite always works in
// RGBA space and avoids "cannot composite non-alpha source" Sharp errors.
//
// Gravity controls placement: 'southeast' (default) places the watermark at the
// bottom-right corner. offsetX (default 16) and offsetY (default == offsetX) set
// the inset from the chosen corner. Sharp translates gravity + offset to the
// `top`/`left` pixel position internally.
//
// === Safety checks ===
// If the overlay's stored dimensions exceed the source dimensions on either axis,
// the handler fails cleanly rather than letting Sharp error or clip silently.
//
// Non-throwing: all failures return { status: 'failed', error }.

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

export type WatermarkGravity =
  | 'north'
  | 'northeast'
  | 'east'
  | 'southeast'
  | 'south'
  | 'southwest'
  | 'west'
  | 'northwest'
  | 'center';

export interface ImageWatermarkNodeData {
  type: 'image:watermark';
  /**
   * JSONPath to the source imageId in the context.
   * Default fallback chain: $.scatter.parentImageId → $.trigger.imageRef → $.trigger.imageId
   */
  sourceImagePath?: string;
  /** imageId of the overlay/logo image in the blob store. Required. */
  overlayImageId: string;
  /**
   * Placement gravity. Controls which corner/edge the watermark is inset from.
   * Default: 'southeast' (bottom-right).
   */
  gravity?: WatermarkGravity;
  /**
   * Opacity for the overlay. Range 0.0 (transparent) to 1.0 (opaque).
   * Default: 0.8.
   */
  opacity?: number;
  /**
   * Horizontal inset in pixels from the gravity edge. Default: 16.
   */
  offsetX?: number;
  /**
   * Vertical inset in pixels from the gravity edge. Defaults to offsetX when
   * not specified (symmetrical padding).
   */
  offsetY?: number;
}

export interface ImageWatermarkStepHandlerOpts {
  imageBlobStore: ImageBlobStore;
}

const DEFAULT_SOURCE_PATH = '$.imageId';
const FALLBACK_SOURCE_PATHS = ['$.scatter.parentImageId',
  '$.trigger.imageRef', '$.trigger.imageId'];

export function createImageWatermarkStepHandler(
  opts: ImageWatermarkStepHandlerOpts,
): StepHandler {
  return {
    kind: 'image:watermark',
    async execute(
      node: PipelineNode,
      context: Record<string, unknown>,
      helpers: StepHandlerHelpers,
    ): Promise<StepOutcome> {
      const data = node.data as unknown as ImageWatermarkNodeData;
      const sourcePath = data.sourceImagePath ?? DEFAULT_SOURCE_PATH;
      const gravity: WatermarkGravity = data.gravity ?? 'southeast';
      const opacity = Math.min(1, Math.max(0, data.opacity ?? 0.8));
      const offsetX = data.offsetX ?? 16;
      const offsetY = data.offsetY ?? offsetX;

      // ── Validate required overlay ──────────────────────────────────────
      if (!data.overlayImageId || typeof data.overlayImageId !== 'string') {
        return {
          status: 'failed',
          error: 'image:watermark: overlayImageId is required',
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
          error: `image:watermark: failed to resolve source imageId at '${sourcePath}' (or fallbacks)`,
        };
      }

      // ── Fetch source bytes ─────────────────────────────────────────────
      const source = await opts.imageBlobStore.get(imageId as string);
      if (!source) {
        return {
          status: 'failed',
          error: `image:watermark: source image '${imageId}' not found in blob store`,
        };
      }

      // ── Fetch overlay bytes ────────────────────────────────────────────
      const overlay = await opts.imageBlobStore.get(data.overlayImageId);
      if (!overlay) {
        return {
          status: 'failed',
          error: `image:watermark: overlay image '${data.overlayImageId}' not found in blob store`,
        };
      }

      // ── Dimension safety check ─────────────────────────────────────────
      // Fail cleanly if overlay exceeds source on either axis.
      if (
        overlay.meta.width > source.meta.width ||
        overlay.meta.height > source.meta.height
      ) {
        return {
          status: 'failed',
          error:
            `image:watermark: overlay dimensions (${overlay.meta.width}x${overlay.meta.height}) ` +
            `exceed source dimensions (${source.meta.width}x${source.meta.height})`,
        };
      }

      // ── Apply opacity to overlay ───────────────────────────────────────
      let overlayWithAlpha: Buffer;
      try {
        overlayWithAlpha = await sharp(overlay.body)
          .ensureAlpha(opacity)
          .toBuffer();
      } catch (err) {
        return {
          status: 'failed',
          error: `image:watermark: overlay alpha pre-processing failed: ${
            err instanceof Error ? err.message : String(err)
          }`,
        };
      }

      // ── Composite ──────────────────────────────────────────────────────
      // Sharp accepts EITHER `gravity` (anchor relative to source) OR
      // explicit `left`/`top` pixel coords — not both. Resolve gravity to
      // absolute pixel coords so offsetX/offsetY actually take effect.
      const srcW = source.meta.width;
      const srcH = source.meta.height;
      const olW = overlay.meta.width;
      const olH = overlay.meta.height;
      const absLeft = resolveLeft(gravity, srcW, olW, offsetX);
      const absTop = resolveTop(gravity, srcH, olH, offsetY);

      let outBytes: Buffer;
      let outInfo: { width: number; height: number; format: string };
      try {
        const result = await sharp(source.body)
          .ensureAlpha()
          .composite([
            {
              input: overlayWithAlpha,
              left: absLeft,
              top: absTop,
              blend: 'over',
            },
          ])
          .png()
          .toBuffer({ resolveWithObject: true });
        outBytes = result.data;
        outInfo = {
          width: result.info.width,
          height: result.info.height,
          format: result.info.format,
        };
      } catch (err) {
        // First try with offset support; some versions of Sharp accept
        // top/left as pixel offset relative to gravity. Retry without
        // offsets if the composite call threw due to offset not supported.
        return {
          status: 'failed',
          error: `image:watermark: Sharp composite failed: ${
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
          error: `image:watermark: blob-store put failed: ${
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

/**
 * Resolve gravity-anchored overlay X position into an absolute left pixel
 * coordinate, applying the user's `offsetX` as an inset from the anchor edge.
 * Sharp's `composite` accepts either `gravity` OR explicit `left/top` — not
 * both — so we convert gravity → coords here to make offsetX/offsetY work.
 */
function resolveLeft(
  gravity: WatermarkGravity,
  srcW: number,
  olW: number,
  offsetX: number,
): number {
  switch (gravity) {
    case 'north':
    case 'south':
    case 'center':
      return Math.max(0, Math.round((srcW - olW) / 2));
    case 'northwest':
    case 'west':
    case 'southwest':
      return Math.max(0, offsetX);
    case 'northeast':
    case 'east':
    case 'southeast':
      return Math.max(0, srcW - olW - offsetX);
    default:
      return 0;
  }
}

function resolveTop(
  gravity: WatermarkGravity,
  srcH: number,
  olH: number,
  offsetY: number,
): number {
  switch (gravity) {
    case 'west':
    case 'east':
    case 'center':
      return Math.max(0, Math.round((srcH - olH) / 2));
    case 'northwest':
    case 'north':
    case 'northeast':
      return Math.max(0, offsetY);
    case 'southwest':
    case 'south':
    case 'southeast':
      return Math.max(0, srcH - olH - offsetY);
    default:
      return 0;
  }
}
