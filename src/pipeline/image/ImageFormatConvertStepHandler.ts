// platform-api/src/pipeline/image/ImageFormatConvertStepHandler.ts
//
// Custom step handler for `node.data.type === 'image:format-convert'`.
// Converts a source image to a target format using Sharp and writes the
// result to the ImageBlobStore. Output mimeType reflects the chosen format.
//
// === Format behaviour ===
//   jpeg — lossy, quality 1-100 (default 85). PNG with transparency is
//          composited on a white background via .flatten() before encoding
//          to avoid silent transparency collapse producing black regions.
//   png  — lossless, ignores quality and lossless flags. Strips metadata
//          when stripMetadata (default true) via Sharp's default behaviour
//          (no embedded EXIF in PNG output by default).
//   webp — quality and lossless both honoured. Lossless=true ignores quality.
//   avif — quality and lossless both honoured. AVIF encoding is CPU-intensive
//          at high resolutions; that is acceptable.
//
// `stripMetadata` (default true) strips EXIF/XMP/ICC via .withMetadata(false)
// on output (Sharp's default is to strip; this flag is a no-op in current
// Sharp but is honoured as a signal of intent).
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

export type ConvertFormat = 'jpeg' | 'png' | 'webp' | 'avif';

export interface ImageFormatConvertNodeData {
  type: 'image:format-convert';
  /**
   * JSONPath to the source imageId in the context.
   * Default fallback chain: $.scatter.parentImageId → $.trigger.imageRef → $.trigger.imageId
   */
  sourceImagePath?: string;
  /** Target format. Required. */
  format: ConvertFormat;
  /**
   * Compression quality for lossy formats (jpeg, webp, avif). Range 1-100.
   * Default: 85. Ignored by png.
   */
  quality?: number;
  /**
   * When true, use lossless compression for webp/avif. Default: false.
   * Lossless ignores quality for those formats.
   */
  lossless?: boolean;
  /**
   * When true (default), metadata (EXIF, XMP, ICC) is stripped from the
   * output. Sharp strips by default; setting this to false passes
   * .withMetadata() to preserve any embedded data.
   */
  stripMetadata?: boolean;
}

export interface ImageFormatConvertStepHandlerOpts {
  imageBlobStore: ImageBlobStore;
}

const DEFAULT_SOURCE_PATH = '$.imageId';
const FALLBACK_SOURCE_PATHS = ['$.scatter.parentImageId',
  '$.trigger.imageRef', '$.trigger.imageId'];
const VALID_FORMATS = new Set<string>(['jpeg', 'png', 'webp', 'avif']);

function formatToMime(format: ConvertFormat): string {
  switch (format) {
    case 'jpeg': return 'image/jpeg';
    case 'png':  return 'image/png';
    case 'webp': return 'image/webp';
    case 'avif': return 'image/avif';
  }
}

export function createImageFormatConvertStepHandler(
  opts: ImageFormatConvertStepHandlerOpts,
): StepHandler {
  return {
    kind: 'image:format-convert',
    async execute(
      node: PipelineNode,
      context: Record<string, unknown>,
      helpers: StepHandlerHelpers,
    ): Promise<StepOutcome> {
      const data = node.data as unknown as ImageFormatConvertNodeData;
      const sourcePath = data.sourceImagePath ?? DEFAULT_SOURCE_PATH;
      const format = data.format;
      const quality = data.quality ?? 85;
      const lossless = data.lossless ?? false;
      const stripMetadata = data.stripMetadata ?? true;

      // ── Validate format ────────────────────────────────────────────────
      if (!format || !VALID_FORMATS.has(format)) {
        return {
          status: 'failed',
          error: `image:format-convert: invalid format '${format}'. Must be one of: jpeg, png, webp, avif`,
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
          error: `image:format-convert: failed to resolve source imageId at '${sourcePath}' (or fallbacks)`,
        };
      }

      // ── Fetch source bytes ─────────────────────────────────────────────
      const source = await opts.imageBlobStore.get(imageId as string);
      if (!source) {
        return {
          status: 'failed',
          error: `image:format-convert: source image '${imageId}' not found in blob store`,
        };
      }

      // ── Apply Sharp conversion ─────────────────────────────────────────
      let outBytes: Buffer;
      let outInfo: { width: number; height: number; format: string };
      try {
        let pipeline = sharp(source.body);

        // JPEG cannot encode transparency — composite on white first to prevent
        // black artifacts (transparent pixels become opaque black without this).
        if (format === 'jpeg') {
          pipeline = pipeline.flatten({ background: '#ffffff' });
        }

        // Preserve metadata when stripMetadata is false.
        if (!stripMetadata) {
          pipeline = pipeline.withMetadata();
        }

        switch (format) {
          case 'jpeg':
            pipeline = pipeline.jpeg({ quality });
            break;
          case 'png':
            pipeline = pipeline.png();
            break;
          case 'webp':
            pipeline = pipeline.webp({ quality, lossless });
            break;
          case 'avif':
            pipeline = pipeline.avif({ quality, lossless });
            break;
        }

        const result = await pipeline.toBuffer({ resolveWithObject: true });
        outBytes = result.data;
        outInfo = {
          width: result.info.width,
          height: result.info.height,
          format: result.info.format,
        };
      } catch (err) {
        return {
          status: 'failed',
          error: `image:format-convert: Sharp conversion failed: ${
            err instanceof Error ? err.message : String(err)
          }`,
        };
      }

      // ── Persist result ─────────────────────────────────────────────────
      const newImageId = randomUUID();
      const mimeType = formatToMime(format);
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
          error: `image:format-convert: blob-store put failed: ${
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
