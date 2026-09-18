// platform-api/src/pipeline/image/ImageSourceStepHandler.ts
//
// Custom step handler (DC-21) that dispatches `node.data.type === 'image-source'`.
// Acts as the explicit entry point for image-input pipelines: resolves an
// `imageId` from one of three sources (trigger payload, JSONPath into the
// run context, or a literal config value) and emits a `pipeline:step:image:produced`
// event so the canvas renders the source thumbnail before any downstream
// vision/crop step fires.
//
// Why ship it as a node, not implicit-from-trigger:
//   1. Makes the graph self-documenting — the user can see "image enters here"
//      on the canvas instead of inferring it from the trigger's payload shape.
//   2. Carries the source imageId forward at a stable path
//      (`steps.<id>.output.imageId`) regardless of how the trigger phrased it,
//      so downstream vision/crop nodes don't need trigger-shape coupling.
//   3. Renders a thumbnail on the canvas (free visual confirmation that the
//      right image is bound) the same way crop / vision nodes do.
//
// Non-throwing: missing/invalid resolution → `{status:'failed', error:...}`.

import type { DescribedStepHandler } from '../customStepHandlers';
import type {
  PipelineNode,
  StepHandler,
  StepHandlerHelpers,
  StepOutcome,
} from 'distributed-core/applications/pipeline';
import type { ImageBlobStore } from './ImageBlobStore';
import { readPath } from './ScatterStepHandler';

interface ImageSourceNodeData {
  type: 'image-source';
  /**
   * Resolution mode:
   *   - 'trigger' (default): read from the trigger payload at `triggerPath`
   *     (default `$.trigger.imageRef`).
   *   - 'path': read from anywhere in the context via `imagePath`.
   *   - 'literal': use the literal `imageId` field — useful for frozen-graph
   *     pipelines where a specific image is baked in.
   */
  mode?: 'trigger' | 'path' | 'literal';
  /** When mode === 'trigger', the JSONPath into the trigger payload. */
  triggerPath?: string;
  /** When mode === 'path', the JSONPath into the run context. */
  imagePath?: string;
  /** When mode === 'literal', the imageId to bind. */
  imageId?: string;
}

export interface ImageSourceStepHandlerOpts {
  imageBlobStore: ImageBlobStore;
}

const DEFAULT_TRIGGER_PATH = '$.trigger.imageRef';

export function createImageSourceStepHandler(
  opts: ImageSourceStepHandlerOpts,
): DescribedStepHandler {
  return {
    kind: 'image-source',
    description:
      'Brings an existing image into the run by its id, so the steps after it have pixels to work on. The usual first step after a trigger that carried an upload.',
    reads: 'An imageId from the node or from the trigger payload.',
    emits:
      'imageId, width and height. No new image is made — it names one that already exists.',
    async execute(
      node: PipelineNode,
      context: Record<string, unknown>,
      helpers: StepHandlerHelpers,
    ): Promise<StepOutcome> {
      const data = node.data as unknown as ImageSourceNodeData;
      const mode = data.mode ?? 'trigger';

      let imageId: unknown;
      switch (mode) {
        case 'literal':
          imageId = data.imageId;
          break;
        case 'path':
          if (!data.imagePath) {
            return {
              status: 'failed',
              error: `image-source: mode='path' requires 'imagePath'`,
            };
          }
          imageId = readPath(context, data.imagePath);
          break;
        case 'trigger':
        default:
          imageId = readPath(context, data.triggerPath ?? DEFAULT_TRIGGER_PATH);
          break;
      }

      if (typeof imageId !== 'string' || !imageId) {
        return {
          status: 'failed',
          error: `image-source: failed to resolve imageId (mode=${mode}, got: ${JSON.stringify(imageId)})`,
        };
      }

      // Fetch metadata (NOT bytes — the source image already lives in the
      // blob store; downstream handlers fetch it themselves).
      const meta = await opts.imageBlobStore.getMetadata(imageId);
      if (!meta) {
        return {
          status: 'failed',
          error: `image-source: imageId '${imageId}' not found in blob store`,
        };
      }

      // Emit image:produced so the canvas renders the source thumbnail
      // inline — same protocol the crop/adjust steps use, so the UI's
      // node-body image renderer doesn't need a special case.
      try {
        await helpers.emit('pipeline:step:image:produced', {
          runId: helpers.runId,
          stepId: helpers.stepId,
          imageId,
          width: meta.width,
          height: meta.height,
          mimeType: meta.mimeType,
          at: new Date().toISOString(),
        });
      } catch {
        // never let a bad subscriber kill the step
      }

      return {
        status: 'completed',
        output: {
          imageId,
          width: meta.width,
          height: meta.height,
          mimeType: meta.mimeType,
        },
      };
    },
  };
}
