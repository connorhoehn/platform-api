// platform-api/src/pipeline/image/ImageGeometricTransformStepHandler.ts
//
// Custom step handler for `node.data.type === 'image:transform'`.
// Applies an ordered array of geometric operations (rotate / flip / flop)
// to a source image using Sharp's chainable API.
//
// === Operation semantics ===
//   rotate  — Sharp .rotate(degrees, { background }) — positive clockwise.
//             When `trimAfter` is true, .trim() is called after rotation to
//             remove any padded background introduced by a non-orthogonal
//             rotation angle.
//   flip    — Vertical reflection (top↔bottom). Sharp: .flip()
//   flop    — Horizontal reflection (left↔right). Sharp: .flop()
//
// An empty operations array is valid — re-encodes the source as PNG
// (new imageId) without transformation. This keeps step output uniform.
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

// ── Operation union type ───────────────────────────────────────────────────

export type GeometricOp =
  | { op: 'rotate'; degrees: number; background?: string; trimAfter?: boolean }
  | { op: 'flip' }
  | { op: 'flop' };

export interface ImageGeometricTransformNodeData {
  type: 'image:transform';
  /**
   * JSONPath to the source imageId in the context.
   * Default fallback chain: $.scatter.parentImageId → $.trigger.imageRef → $.trigger.imageId
   */
  sourceImagePath?: string;
  /**
   * Ordered list of geometric operations to apply. Empty array is valid —
   * produces a re-encoded PNG with a new imageId and no transformation.
   */
  operations: GeometricOp[];
}

export interface ImageGeometricTransformStepHandlerOpts {
  imageBlobStore: ImageBlobStore;
}

const DEFAULT_SOURCE_PATH = '$.imageId';
const FALLBACK_SOURCE_PATHS = ['$.scatter.parentImageId',
  '$.trigger.imageRef', '$.trigger.imageId'];
const VALID_OPS = new Set(['rotate', 'flip', 'flop']);

export function createImageGeometricTransformStepHandler(
  opts: ImageGeometricTransformStepHandlerOpts,
): StepHandler {
  return {
    kind: 'image:transform',
    async execute(
      node: PipelineNode,
      context: Record<string, unknown>,
      helpers: StepHandlerHelpers,
    ): Promise<StepOutcome> {
      const data = node.data as unknown as ImageGeometricTransformNodeData;
      const sourcePath = data.sourceImagePath ?? DEFAULT_SOURCE_PATH;
      const operations: GeometricOp[] = Array.isArray(data.operations) ? data.operations : [];

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
          error: `image:transform: failed to resolve source imageId at '${sourcePath}' (or fallbacks)`,
        };
      }

      // ── Validate operations before fetching bytes ──────────────────────
      for (const op of operations) {
        if (!op || typeof op !== 'object' || !VALID_OPS.has((op as { op?: unknown }).op as string)) {
          return {
            status: 'failed',
            error: `image:transform: unknown operation '${(op as { op?: unknown })?.op ?? op}'`,
          };
        }
      }

      // ── Fetch source bytes ─────────────────────────────────────────────
      const source = await opts.imageBlobStore.get(imageId as string);
      if (!source) {
        return {
          status: 'failed',
          error: `image:transform: source image '${imageId}' not found in blob store`,
        };
      }

      // ── Apply operations in order ──────────────────────────────────────
      let outBytes: Buffer;
      let outInfo: { width: number; height: number; format: string };
      try {
        let pipeline = sharp(source.body);

        for (const op of operations) {
          switch (op.op) {
            case 'rotate': {
              pipeline = pipeline.rotate(op.degrees, {
                background: op.background ?? '#000000',
              });
              if (op.trimAfter) {
                pipeline = pipeline.trim();
              }
              break;
            }
            case 'flip':
              pipeline = pipeline.flip();
              break;
            case 'flop':
              pipeline = pipeline.flop();
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
          error: `image:transform: Sharp transform failed: ${
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
          error: `image:transform: blob-store put failed: ${
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
