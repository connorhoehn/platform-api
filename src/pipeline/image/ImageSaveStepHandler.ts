// platform-api/src/pipeline/image/ImageSaveStepHandler.ts
//
// Custom step handler (Phase 3) for `node.data.type === 'image:save'`.
// Terminal "mark as saved" node — reads the source imageId from the blob
// store and emits `pipeline:step:image:produced` so the canvas renders the
// final image inline. Does NOT write a new image; the source bytes already
// live in the store. The existing imageId is propagated in the step output
// for downstream consumption.
//
// === Filename templating ===
// `filenameTemplate` supports simple `{{key.path}}` substitution. The
// `key.path` is resolved via `readPath()` against the run context. Missing
// values render as an empty string (no error). Example:
//   'crop-{{scatter.item.label}}-{{scatter.index}}.jpg'
// resolves 'scatter.item.label' and 'scatter.index' from context.
//
// Non-throwing: missing source, blob-store error → status:'failed'.

import type {
  PipelineNode,
  StepHandler,
  StepHandlerHelpers,
  StepOutcome,
} from 'distributed-core/applications/pipeline';
import type { ImageBlobStore } from './ImageBlobStore';
import { readPath } from './ScatterStepHandler';

interface ImageSaveNodeData {
  type: 'image:save';
  /**
   * JSONPath to the source imageId in the context.
   * Default fallback chain: $.scatter.parentImageId → $.trigger.imageRef → $.trigger.imageId
   */
  sourceImagePath?: string;
  /**
   * Optional filename template. Supports `{{path.to.value}}` tokens resolved
   * against the run context via `readPath`. Missing values render as ''.
   * Example: 'crop-{{scatter.item.label}}-{{scatter.index}}.jpg'
   */
  filenameTemplate?: string;
}

export interface ImageSaveStepHandlerOpts {
  imageBlobStore: ImageBlobStore;
}

const DEFAULT_SOURCE_PATH = '$.scatter.parentImageId';
const FALLBACK_SOURCE_PATHS = ['$.trigger.imageRef', '$.trigger.imageId'];

/**
 * Resolve all `{{key.path}}` tokens in a template string against the context.
 * Missing values render as empty string. Not a full template engine — simple
 * token substitution only.
 */
export function resolveTemplate(
  template: string,
  context: Record<string, unknown>,
): string {
  return template.replace(/\{\{([^}]+)\}\}/g, (_match, path: string) => {
    const trimmed = path.trim();
    const value = readPath(context, trimmed);
    if (value === null || value === undefined) return '';
    return String(value);
  });
}

export function createImageSaveStepHandler(
  opts: ImageSaveStepHandlerOpts,
): StepHandler {
  return {
    kind: 'image:save',
    async execute(
      node: PipelineNode,
      context: Record<string, unknown>,
      helpers: StepHandlerHelpers,
    ): Promise<StepOutcome> {
      const data = node.data as unknown as ImageSaveNodeData;
      const sourcePath = data.sourceImagePath ?? DEFAULT_SOURCE_PATH;

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
          error: `image:save: failed to resolve source imageId at '${sourcePath}' (or fallbacks)`,
        };
      }

      // ── Verify source exists ───────────────────────────────────────────
      const meta = await opts.imageBlobStore.getMetadata(imageId as string);
      if (!meta) {
        return {
          status: 'failed',
          error: `image:save: source image '${imageId}' not found in blob store`,
        };
      }

      // ── Resolve filename template ──────────────────────────────────────
      const filename: string | undefined = data.filenameTemplate
        ? resolveTemplate(data.filenameTemplate, context)
        : undefined;

      // ── Emit image:produced ────────────────────────────────────────────
      // Signal the canvas that this imageId is the final output.
      // The bytes already exist in the store; we're just announcing them.
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
          ...(filename !== undefined ? { filename } : {}),
          width: meta.width,
          height: meta.height,
          mimeType: meta.mimeType,
        },
      };
    },
  };
}
