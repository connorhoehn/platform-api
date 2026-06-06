// platform-api/src/pipeline/image/ImageCropStepHandler.ts
//
// Custom step handler (DC-21) that dispatches `node.data.type === 'image:crop'`.
// Reads the source image from ImageBlobStore, extracts a bounding-box
// region via Sharp, writes the resulting buffer back to the blob store as
// a new imageId, and emits `pipeline:step:image:produced` so the canvas
// can render an inline thumbnail before the step's terminal completion
// event arrives.
//
// === Coordinate space ===
// All bbox coordinates are interpreted in POST-RESIZE pixel space — the
// upload route (`routes/images.ts`) resizes every uploaded image to a
// 1568px long edge before storing, so the meta.width/meta.height already
// reflect the resized geometry. Vision steps see the resized buffer and
// emit bboxes in that same coordinate space. There is no "original
// image" stored anywhere; downstream consumers operate on the resized
// representation end to end.
//
// === Scatter-arm pattern ===
// When this handler runs inside a scatter arm (the common case), the per-arm
// context carries `scatter.item` — the crop spec. The handler reads source
// image + bbox via configurable JSONPaths so it works equally for:
//   - Vision-driven scatter: bbox comes from `$.scatter.item.bbox`.
//   - Standalone use: source + bbox baked into node.data as literal config.
//
// === Output shape ===
// Output is `{ imageId, parentImageId, bbox, width, height, mimeType }`.
// Downstream handlers (image:adjust-bw, image:save, ...) consume `imageId`
// from `context.steps.<thisNodeId>.output.imageId`.
//
// Non-throwing: bad bbox, missing source, Sharp failures → `status:'failed'`.

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

interface ImageCropNodeData {
  type: 'image:crop';
  /**
   * JSONPath to the source image identifier in the context. Default:
   * `$.scatter.parentImageId` (set by the scatter handler's arm context).
   * Fallback: `$.trigger.imageRef` for non-scatter runs.
   */
  sourceImagePath?: string;
  /**
   * JSONPath to the bbox object in the context. Default: `$.scatter.item.bbox`.
   * Bbox shape: `{ x, y, width, height }` integer pixels in post-resize space.
   */
  bboxPath?: string;
  /**
   * Literal bbox baked into the node config. When present, this is used
   * VERBATIM and `bboxPath` is ignored — frozen-run pipelines (see
   * `freezeRun.ts` on the wsg-frontend side) hardcode the detection
   * result into each cloned CropNode so re-runs are deterministic. The
   * shape is the same canonical `{x,y,width,height}` integer-pixels in
   * post-resize coordinate space.
   */
  bbox?: { x: number; y: number; width: number; height: number };
  /**
   * When false, skip persisting the cropped buffer — output carries the
   * computed bbox but no new imageId. Useful for "preview only" pipelines
   * where downstream consumers don't need a stored crop. Default: true.
   */
  materialize?: boolean;
}

interface BBox {
  x: number;
  y: number;
  width: number;
  height: number;
}

export interface ImageCropStepHandlerOpts {
  imageBlobStore: ImageBlobStore;
}

// Source-image resolution convention:
//   `$.imageId` is the top-level imageId merged into context by the executor's
//   `accumulateContext` after every image handler returns `{imageId, ...}`.
//   That makes `$.imageId` the *most-recent* produced imageId, which is what
//   downstream transforms (crop/adjust/save) want by default. Inside a
//   scatter arm before any crop has run, this still resolves to the source
//   image because image-source's output already populated it.
const DEFAULT_SOURCE_PATH = '$.imageId';
const FALLBACK_SOURCE_PATHS = [
  '$.scatter.parentImageId',
  '$.trigger.imageRef',
  '$.trigger.imageId',
];
// Scatter writes the per-arm item at `context.scatter.item` AND under the
// configured itemAlias (default 'item'). Crop reads the bbox from the item
// — accepts either `{bbox: {...}}` nested or `{x,y,width,height}` flat form
// via coerceBBox.
const DEFAULT_BBOX_PATH = '$.scatter.item';

export function createImageCropStepHandler(
  opts: ImageCropStepHandlerOpts,
): StepHandler {
  return {
    kind: 'image:crop',
    async execute(
      node: PipelineNode,
      context: Record<string, unknown>,
      helpers: StepHandlerHelpers,
    ): Promise<StepOutcome> {
      const data = node.data as unknown as ImageCropNodeData;
      const sourcePath = data.sourceImagePath ?? DEFAULT_SOURCE_PATH;
      const bboxPath = data.bboxPath ?? DEFAULT_BBOX_PATH;
      const materialize = data.materialize ?? true;

      // ── Resolve source imageId ─────────────────────────────────────────
      let imageId = readPath(context, sourcePath);
      if (typeof imageId !== 'string' || !imageId) {
        // Try fallback paths so this handler "just works" without explicit
        // wiring in the simple single-image trigger case.
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
          error: `image:crop: failed to resolve source imageId at '${sourcePath}' (or fallbacks)`,
        };
      }

      // ── Resolve bbox ───────────────────────────────────────────────────
      // Literal `data.bbox` wins over `bboxPath` resolution — frozen-run
      // pipelines hardcode the detection result into the node config, so
      // the handler must honor the literal even when no scatter context
      // exists at execution time.
      const bbox = coerceBBox(data.bbox) ?? coerceBBox(readPath(context, bboxPath));
      if (!bbox) {
        return {
          status: 'failed',
          error: data.bbox
            ? `image:crop: literal data.bbox is malformed (expected {x,y,width,height}, got: ${JSON.stringify(data.bbox)})`
            : `image:crop: bbox at '${bboxPath}' is missing or malformed (expected {x,y,width,height})`,
        };
      }

      // ── Fetch source bytes ─────────────────────────────────────────────
      const source = await opts.imageBlobStore.get(imageId);
      if (!source) {
        return {
          status: 'failed',
          error: `image:crop: source image '${imageId}' not found in blob store`,
        };
      }

      // ── Clamp bbox to image bounds ─────────────────────────────────────
      // Vision models occasionally emit bboxes that nudge slightly past the
      // edge; clamping is safer than failing the whole arm.
      const clamped = clampBBox(bbox, source.meta.width, source.meta.height);
      if (clamped.width <= 0 || clamped.height <= 0) {
        return {
          status: 'failed',
          error: `image:crop: bbox ${JSON.stringify(bbox)} has zero area after clamping to image ${source.meta.width}x${source.meta.height}`,
        };
      }

      // ── Sharp extract ──────────────────────────────────────────────────
      let cropBytes: Buffer;
      let croppedMeta: { width: number; height: number; format: string };
      try {
        const pipeline = sharp(source.body).extract({
          left: clamped.x,
          top: clamped.y,
          width: clamped.width,
          height: clamped.height,
        });
        const result = await pipeline.toBuffer({ resolveWithObject: true });
        cropBytes = result.data;
        croppedMeta = {
          width: result.info.width,
          height: result.info.height,
          format: result.info.format,
        };
      } catch (err) {
        return {
          status: 'failed',
          error: `image:crop: Sharp extract failed: ${
            err instanceof Error ? err.message : String(err)
          }`,
        };
      }

      const mimeType = formatToMime(croppedMeta.format, source.meta.mimeType);

      // ── Persist or skip ────────────────────────────────────────────────
      let cropImageId: string | undefined;
      if (materialize) {
        cropImageId = randomUUID();
        const cropMeta: ImageMeta = {
          mimeType,
          sizeBytes: cropBytes.length,
          width: croppedMeta.width,
          height: croppedMeta.height,
          uploadedAt: new Date().toISOString(),
        };
        try {
          await opts.imageBlobStore.put(cropImageId, cropBytes, cropMeta);
        } catch (err) {
          return {
            status: 'failed',
            error: `image:crop: blob-store put failed: ${
              err instanceof Error ? err.message : String(err)
            }`,
          };
        }

        // Emit the image-produced event so the UI can render inline before
        // step.completed fires.
        try {
          await helpers.emit('pipeline:step:image:produced', {
            runId: helpers.runId,
            stepId: helpers.stepId,
            imageId: cropImageId,
            width: croppedMeta.width,
            height: croppedMeta.height,
            mimeType,
            bbox: {
              x: clamped.x,
              y: clamped.y,
              width: clamped.width,
              height: clamped.height,
            },
            at: new Date().toISOString(),
          });
        } catch {
          // never let a bad subscriber kill the step
        }
      }

      return {
        status: 'completed',
        output: {
          imageId: cropImageId,
          parentImageId: imageId,
          bbox: clamped,
          width: croppedMeta.width,
          height: croppedMeta.height,
          mimeType,
          materialized: materialize,
        },
      };
    },
  };
}

/**
 * Coerce arbitrary input to a BBox if possible. Accepts the canonical
 * `{x,y,width,height}` shape, plus alternates the vision step sometimes
 * emits (`{left,top,width,height}`, `{x1,y1,x2,y2}` corner form).
 *
 * Also unwraps a `.bbox` sub-key when present so callers can default
 * `bboxPath` to `$.scatter.item` and have the coercer handle BOTH
 * `{label, bbox: {x,y,w,h}}` nested vision schemas AND
 * `{x,y,w,h,label}` flat vision schemas transparently.
 */
function coerceBBox(raw: unknown): BBox | null {
  if (!raw || typeof raw !== 'object') return null;
  const r = raw as Record<string, unknown>;
  const num = (v: unknown): number | null =>
    typeof v === 'number' && Number.isFinite(v) ? Math.round(v) : null;

  // Canonical flat shape.
  const x = num(r['x']);
  const y = num(r['y']);
  const w = num(r['width']);
  const h = num(r['height']);
  if (x !== null && y !== null && w !== null && h !== null) {
    return { x, y, width: w, height: h };
  }

  // {left, top, width, height} variant.
  const left = num(r['left']);
  const top = num(r['top']);
  if (left !== null && top !== null && w !== null && h !== null) {
    return { x: left, y: top, width: w, height: h };
  }

  // {x1, y1, x2, y2} corner form (common in detection models).
  const x1 = num(r['x1']);
  const y1 = num(r['y1']);
  const x2 = num(r['x2']);
  const y2 = num(r['y2']);
  if (x1 !== null && y1 !== null && x2 !== null && y2 !== null) {
    return {
      x: Math.min(x1, x2),
      y: Math.min(y1, y2),
      width: Math.abs(x2 - x1),
      height: Math.abs(y2 - y1),
    };
  }

  // Nested-bbox unwrap: try `.bbox` once. Lets callers default the bbox
  // path to `$.scatter.item` and have it work whether the vision schema
  // emits the bbox flat or under a `bbox` sub-key.
  if (r['bbox'] && typeof r['bbox'] === 'object') {
    return coerceBBox(r['bbox']);
  }

  return null;
}

/**
 * Clamp the bbox to the image bounds so Sharp's `.extract()` doesn't
 * throw on a 1-px overshoot.
 */
function clampBBox(b: BBox, imgW: number, imgH: number): BBox {
  const x = Math.max(0, Math.min(b.x, imgW));
  const y = Math.max(0, Math.min(b.y, imgH));
  const width = Math.max(0, Math.min(b.width, imgW - x));
  const height = Math.max(0, Math.min(b.height, imgH - y));
  return { x, y, width, height };
}

/**
 * Sharp returns the output format string (`jpeg` / `png` / `webp`). Map
 * back to a mime type matching ImageBlobStore's stored shape. Falls
 * back to the source's mime when Sharp's format is something unexpected.
 */
function formatToMime(format: string, fallback: string): string {
  switch (format) {
    case 'jpeg':
    case 'jpg':
      return 'image/jpeg';
    case 'png':
      return 'image/png';
    case 'webp':
      return 'image/webp';
    case 'gif':
      return 'image/gif';
    default:
      return fallback;
  }
}
