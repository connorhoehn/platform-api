// platform-api/src/pipeline/image/ImageAdjustLayerStepHandler.ts
//
// Custom step handler (Phase 3) for `node.data.type === 'image:adjust-layer'`.
// Applies an ordered array of adjustment layers to a source image using
// Sharp's chainable API, then writes the result to the ImageBlobStore.
//
// === Layer kinds and their Sharp equivalents ===
//   brightness → .modulate({ brightness: value })  — 1.0 passthrough
//   contrast   → .linear(value, -(value-1)*128)    — 1.0 passthrough
//   saturation → .modulate({ saturation: value })  — 1.0 passthrough
//   hue        → .modulate({ hue: degrees })        — 0 passthrough
//   blur       → .blur(radiusPx)                    — sigma in pixels
//
//   Photo-editor kinds (realtime-examples media photo editor, lane E2):
//   exposure, temperature, tint, highlights, shadows, whites, blacks,
//   vibrance, curve, vignette → per-pixel on `raw()` bytes (adjustPixels.ts,
//   the same math as the editor's live preview), then
//   sharpen → .sharpen({ sigma }) and noise-reduction → .median(size).
//   Any layer may carry `maskId` (resolved through `node.data.masks`) or
//   `maskImageId`: it then applies inside that mask only (8-bit, blended).
//   When a list uses only the five legacy kinds, the Sharp chain above is
//   used unchanged (existing pipelines render byte-identical).
//
// Empty layers array is valid: image is re-encoded as PNG (new imageId)
// without any transformation. This keeps the step contract uniform so
// downstream steps always see a fresh imageId in output.
//
// Unknown layer kinds cause the step to fail early (non-throwing contract).
//
// Non-throwing: bad source, blob-store error, Sharp error → status:'failed'.

import type { DescribedStepHandler } from '../customStepHandlers';
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
import { applyPixelLayers, type PixelLayer } from './adjustPixels';

/**
 * The photo-editor path: every per-pixel layer on raw bytes (whole photo
 * first, then each mask's layers blended through that mask), then the
 * spatial ones (sharpen, noise reduction, blur) through Sharp. The order is
 * the live preview's fixed order, not list order.
 */
export async function renderPhotoLayers(
  input: Buffer,
  layers: readonly AdjustLayer[],
  masks: Record<string, string>,
  store: Pick<ImageBlobStore, 'get'>,
): Promise<{ data: Buffer; info: { width: number; height: number; format: string } }> {
  const raw = await sharp(input).removeAlpha().raw().toBuffer({ resolveWithObject: true });
  const { width, height, channels } = raw.info;
  const pixels = raw.data;
  const colour = layers.filter((l) => l.kind !== 'blur' && l.kind !== 'sharpen' && l.kind !== 'noise-reduction');
  const groups = new Map<string, PixelLayer[]>();
  for (const l of colour) {
    const maskImage = l.maskImageId ?? (l.maskId ? masks[l.maskId] : '');
    if (l.maskId && !maskImage) throw new Error(`mask '${l.maskId}' has no image`);
    const key = maskImage ?? '';
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key)!.push(l as PixelLayer);
  }
  const whole = groups.get('');
  if (whole) applyPixelLayers(pixels, width, height, channels, whole);
  for (const [maskImageId, group] of groups) {
    if (!maskImageId) continue;
    const blob = await store.get(maskImageId);
    if (!blob) throw new Error(`mask image '${maskImageId}' not found`);
    const mask = await sharp(blob.body).resize(width, height, { fit: 'fill' }).greyscale().raw().toBuffer();
    applyPixelLayers(pixels, width, height, channels, group, new Uint8Array(mask.buffer, mask.byteOffset, width * height));
  }
  let out = sharp(pixels, { raw: { width, height, channels } });
  for (const l of layers) {
    if (l.maskId || l.maskImageId) continue; // spatial ops are whole-photo only
    if (l.kind === 'sharpen' && l.amount > 0) out = out.sharpen({ sigma: 0.5 + (Math.min(l.amount, 150) / 150) * 1.5 });
    else if (l.kind === 'noise-reduction' && l.amount > 0) out = out.median(l.amount >= 60 ? 5 : 3);
    else if (l.kind === 'blur' && l.radiusPx > 0) out = out.blur(Math.max(0.3, l.radiusPx));
  }
  const result = await out.png().toBuffer({ resolveWithObject: true });
  return { data: result.data, info: { width: result.info.width, height: result.info.height, format: result.info.format } };
}

// ── Layer union type ───────────────────────────────────────────────────────

export type AdjustLayer = (
  | { kind: 'brightness'; value: number }
  | { kind: 'contrast'; value: number }
  | { kind: 'saturation'; value: number }
  | { kind: 'hue'; degrees: number }
  | { kind: 'blur'; radiusPx: number }
  | { kind: 'exposure'; stops: number }
  | { kind: 'highlights' | 'shadows' | 'whites' | 'blacks' | 'vibrance' | 'tint' | 'vignette'; amount: number }
  | { kind: 'temperature'; kelvin: number }
  | { kind: 'curve'; channel: 'rgb' | 'r' | 'g' | 'b'; points: Array<[number, number]> }
  | { kind: 'sharpen'; amount: number }
  | { kind: 'noise-reduction'; amount: number }
) & { maskId?: string; maskImageId?: string };

const LEGACY_KINDS = new Set(['brightness', 'contrast', 'saturation', 'hue', 'blur']);
export const ADJUST_LAYER_KINDS = new Set([
  ...LEGACY_KINDS, 'exposure', 'highlights', 'shadows', 'whites', 'blacks', 'vibrance', 'tint', 'vignette',
  'temperature', 'curve', 'sharpen', 'noise-reduction',
]);

interface ImageAdjustLayerNodeData {
  type: 'image:adjust-layer';
  /**
   * JSONPath to the source imageId in the context.
   * Default fallback chain: $.scatter.parentImageId → $.trigger.imageRef → $.trigger.imageId
   */
  sourceImagePath?: string;
  /** Ordered list of layer adjustments to apply. */
  layers: AdjustLayer[];
  /** maskId → imageId of its 8-bit mask, for layers that name a `maskId`. */
  masks?: Record<string, string>;
}

export interface ImageAdjustLayerStepHandlerOpts {
  imageBlobStore: ImageBlobStore;
}

const DEFAULT_SOURCE_PATH = '$.imageId';
const FALLBACK_SOURCE_PATHS = ['$.scatter.parentImageId',
  '$.trigger.imageRef', '$.trigger.imageId'];

export function createImageAdjustLayerStepHandler(
  opts: ImageAdjustLayerStepHandlerOpts,
): DescribedStepHandler {
  return {
    kind: 'image:adjust-layer',
    description:
      'Applies stacked colour and filter adjustments — brightness, contrast, saturation, hue and the rest — in the order the node lists them.',
    reads: 'The source image at the node\'s path, falling back to the scatter parent and the trigger\'s imageRef/imageId.',
    emits:
      'A NEW imageId for the result, plus parentImageId, width, height and mimeType.',
    async execute(
      node: PipelineNode,
      context: Record<string, unknown>,
      helpers: StepHandlerHelpers,
    ): Promise<StepOutcome> {
      const data = node.data as unknown as ImageAdjustLayerNodeData;
      const sourcePath = data.sourceImagePath ?? DEFAULT_SOURCE_PATH;
      const layers: AdjustLayer[] = Array.isArray(data.layers) ? data.layers : [];

      // ── Resolve source imageId ───────────────────────────────────────
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
          error: `image:adjust-layer: failed to resolve source imageId at '${sourcePath}' (or fallbacks)`,
        };
      }

      // ── Validate layer kinds before fetching bytes ───────────────────
      for (const layer of layers) {
        if (!layer || typeof layer !== 'object' || !ADJUST_LAYER_KINDS.has(layer.kind)) {
          return {
            status: 'failed',
            error: `image:adjust-layer: unknown layer kind '${(layer as { kind?: unknown })?.kind ?? layer}'`,
          };
        }
      }

      // ── Fetch source bytes ───────────────────────────────────────────
      const source = await opts.imageBlobStore.get(imageId as string);
      if (!source) {
        return {
          status: 'failed',
          error: `image:adjust-layer: source image '${imageId}' not found in blob store`,
        };
      }

      // ── Apply layers in order ────────────────────────────────────────
      let outBytes: Buffer;
      let outInfo: { width: number; height: number; format: string };
      const legacyOnly = layers.every((l) => LEGACY_KINDS.has(l.kind) && !l.maskId && !l.maskImageId);
      if (!legacyOnly) {
        try {
          const out = await renderPhotoLayers(source.body, layers, data.masks ?? {}, opts.imageBlobStore);
          outBytes = out.data;
          outInfo = out.info;
        } catch (err) {
          return {
            status: 'failed',
            error: `image:adjust-layer: ${err instanceof Error ? err.message : String(err)}`,
          };
        }
      } else try {
        let pipeline = sharp(source.body);

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
            default:
              break;
          }
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
          error: `image:adjust-layer: Sharp transform failed: ${
            err instanceof Error ? err.message : String(err)
          }`,
        };
      }

      // ── Persist result ───────────────────────────────────────────────
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
          error: `image:adjust-layer: blob-store put failed: ${
            err instanceof Error ? err.message : String(err)
          }`,
        };
      }

      // ── Emit image:produced ──────────────────────────────────────────
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
