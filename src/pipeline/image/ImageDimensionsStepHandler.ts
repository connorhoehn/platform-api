// platform-api/src/pipeline/image/ImageDimensionsStepHandler.ts
//
// `image:dimensions` — reads the dimensions of a source image already in
// the blob store and writes them into the pipeline context so downstream
// steps (notably image:generate) can size themselves off a real image
// instead of assuming 512×512.
//
// Output — the executor merges a step's output into context.steps[nodeId]
// DIRECTLY (not under `.output`), so downstream binds read
// `{{steps.<thisNodeId>.genWidth}}` etc. (see VisionPipelineE2E:
// $.steps.source.imageId). Fields:
//   width        – source image width  (px, exact)
//   height       – source image height (px, exact)
//   aspectRatio  – width / height (4dp)
//   genWidth     – width snapped to a valid generation size (multiple of
//   genHeight      16, ≤ maxEdge, aspect preserved) — bind image:generate's
//                  width/height to these, NOT the raw dimensions, since
//                  diffusion models reject arbitrary sizes.
//
// Source imageId resolution mirrors the other image steps:
//   data.sourceImagePath (JSONPath) → fallbacks → fail.

import type { DescribedStepHandler } from '../customStepHandlers';
import type {
  PipelineNode,
  StepHandler,
  StepHandlerHelpers,
  StepOutcome,
} from 'distributed-core/applications/pipeline';
import type { ImageBlobStore } from './ImageBlobStore';
import { readPath } from './ScatterStepHandler';
import { snapGenerationDims, aspectRatio } from './dimensionSnap';

interface ImageDimensionsNodeData {
  type: 'image:dimensions';
  /** JSONPath to the source imageId. Default '$.imageId' + fallbacks. */
  sourceImagePath?: string;
  /** Largest edge for the snapped genWidth/genHeight. Default 1024. */
  maxEdge?: number;
}

const DEFAULT_SOURCE_PATH = '$.imageId';
const FALLBACK_SOURCE_PATHS = [
  '$.scatter.parentImageId',
  '$.trigger.imageRef',
  '$.trigger.imageId',
];

export interface ImageDimensionsStepHandlerOpts {
  imageBlobStore: ImageBlobStore;
}

export function createImageDimensionsStepHandler(
  opts: ImageDimensionsStepHandlerOpts,
): DescribedStepHandler {
  return {
    kind: 'image:dimensions',
    description:
      'Reads the source image\'s size without changing it. Also reports the nearest size the generators accept, so a generate step downstream can match the original\'s shape instead of guessing.',
    reads: 'The source image at the node\'s path, falling back to the scatter parent and the trigger\'s imageRef/imageId.',
    emits:
      'imageId unchanged, width, height, aspectRatio, and genWidth/genHeight — the snapped pair for a generator.',
    async execute(
      node: PipelineNode,
      context: Record<string, unknown>,
      _helpers: StepHandlerHelpers,
    ): Promise<StepOutcome> {
      const data = node.data as unknown as ImageDimensionsNodeData;
      const sourcePath = data.sourceImagePath ?? DEFAULT_SOURCE_PATH;

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
          error: `image:dimensions: failed to resolve source imageId at '${sourcePath}' (or fallbacks)`,
        };
      }

      const meta = await opts.imageBlobStore.getMetadata(imageId);
      if (!meta) {
        return {
          status: 'failed',
          error: `image:dimensions: source image '${imageId}' not found in blob store`,
        };
      }

      const snapped = snapGenerationDims(meta.width, meta.height, {
        ...(data.maxEdge !== undefined ? { maxEdge: data.maxEdge } : {}),
      });

      return {
        status: 'completed',
        output: {
          imageId,
          width: meta.width,
          height: meta.height,
          aspectRatio: aspectRatio(meta.width, meta.height),
          genWidth: snapped.width,
          genHeight: snapped.height,
        },
      };
    },
  };
}
