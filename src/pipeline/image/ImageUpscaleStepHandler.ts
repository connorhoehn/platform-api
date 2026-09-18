// platform-api/src/pipeline/image/ImageUpscaleStepHandler.ts
//
// `image:upscale` — super-resolves a source image via a self-hosted
// stable-diffusion-x4-upscaler server (no cloud SDKs). Reads a source
// imageId from context, sends its bytes to the upscale server, stores
// the result, and emits pipeline:step:image:produced.
//
// Pairs with image:generate (generate small, then upscale) and
// image:dimensions. Output: { imageId, width, height, sourceImageId }.
//
// Storage note: the x4 model returns ~4× the input (a 512 source →
// 2048). The inline blob store caps at 350KB / a max edge, so the stored
// result is cap-fit to `maxEdge` (default 1024 — still markedly sharper
// than the 512 source). Persisting the full 2048 needs the chunked
// spillover store (second DDB table) — tracked as a follow-up.

import type { DescribedStepHandler } from '../customStepHandlers';
import { randomUUID } from 'crypto';
import type {
  PipelineNode,
  StepHandler,
  StepHandlerHelpers,
  StepOutcome,
} from 'distributed-core/applications/pipeline';
import type { ImageBlobStore } from './ImageBlobStore';
import type { ImageMeta } from './types';
import { readPath } from './ScatterStepHandler';
import { resolveTemplate } from './ImageSaveStepHandler';
import { capFitImage } from './imageBlobCapFit';
import { UpscaleThrottlingError, type UpscaleClient } from './UpscaleClient';
import { createUpscaleClient } from '../factories/UpscaleClientFactory';

interface ImageUpscaleNodeData {
  type: 'image:upscale';
  /** JSONPath to the source imageId. Default '$.imageId' + fallbacks. */
  sourceImagePath?: string;
  /** Optional prompt to guide the upscaler (supports {{path}}). */
  prompt?: string;
  /** Largest stored edge after upscaling. Default 1024 (fits the blob cap). */
  maxEdge?: number;
}

const DEFAULT_SOURCE_PATH = '$.imageId';
const FALLBACK_SOURCE_PATHS = [
  '$.scatter.parentImageId',
  '$.trigger.imageRef',
  '$.trigger.imageId',
];
const DEFAULT_STORE_MAX_EDGE = 1024;

export interface ImageUpscaleStepHandlerOpts {
  imageBlobStore: ImageBlobStore;
  /** Inject an UpscaleClient (tests). Defaults to the self-hosted client. */
  upscaleClient?: UpscaleClient;
}

export function createImageUpscaleStepHandler(
  opts: ImageUpscaleStepHandlerOpts,
): DescribedStepHandler {
  return {
    kind: 'image:upscale',
    description:
      'Runs 4x super-resolution on the source image, self-hosted rather than through a provider.',
    reads: 'The source image at the node\'s path, falling back to the scatter parent and the trigger\'s imageRef/imageId.',
    emits:
      'A NEW imageId for the result, plus parentImageId, width, height and mimeType.',
    async execute(
      node: PipelineNode,
      context: Record<string, unknown>,
      helpers: StepHandlerHelpers,
    ): Promise<StepOutcome> {
      const data = node.data as unknown as ImageUpscaleNodeData;
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
          error: `image:upscale: failed to resolve source imageId at '${sourcePath}' (or fallbacks)`,
        };
      }

      const source = await opts.imageBlobStore.get(imageId);
      if (!source) {
        return {
          status: 'failed',
          error: `image:upscale: source image '${imageId}' not found in blob store`,
        };
      }

      // ── Upscale via the self-hosted provider ───────────────────────────
      const client = opts.upscaleClient ?? createUpscaleClient();
      let upscaledBytes: Buffer;
      try {
        const result = await client.upscale({
          imageB64: source.body.toString('base64'),
          ...(data.prompt ? { prompt: resolveTemplate(data.prompt, context) } : {}),
        });
        upscaledBytes = Buffer.from(result.imageB64, 'base64');
      } catch (err) {
        if (err instanceof UpscaleThrottlingError) {
          return { status: 'failed', error: 'rate limited; retry after backoff' };
        }
        return {
          status: 'failed',
          error: `image:upscale: ${client.provider} invocation failed: ${
            err instanceof Error ? err.message : String(err)
          }`,
        };
      }

      // ── Cap-fit + store ────────────────────────────────────────────────
      // Use the store's declared capability so a volume-backed store can
      // keep the full 2048 upscale while a DDB-backed store still caps
      // at its inline limit. data.maxEdge overrides both (operator pin).
      let fitted: Awaited<ReturnType<typeof capFitImage>>;
      try {
        fitted = await capFitImage(upscaledBytes, 'image:upscale', {
          maxDimension: data.maxEdge ?? opts.imageBlobStore.maxStoredEdge ?? DEFAULT_STORE_MAX_EDGE,
          capBytes: opts.imageBlobStore.capBytes,
        });
      } catch (err) {
        return { status: 'failed', error: err instanceof Error ? err.message : String(err) };
      }

      const newImageId = randomUUID();
      const meta: ImageMeta = {
        mimeType: fitted.mimeType,
        sizeBytes: fitted.bytes.byteLength,
        width: fitted.width,
        height: fitted.height,
        uploadedAt: new Date().toISOString(),
      };

      try {
        await opts.imageBlobStore.put(newImageId, fitted.bytes, meta);
      } catch (err) {
        return {
          status: 'failed',
          error: `image:upscale: blob-store put failed: ${
            err instanceof Error ? err.message : String(err)
          }`,
        };
      }

      try {
        await helpers.emit('pipeline:step:image:produced', {
          runId: helpers.runId,
          stepId: helpers.stepId,
          imageId: newImageId,
          width: fitted.width,
          height: fitted.height,
          mimeType: fitted.mimeType,
          at: new Date().toISOString(),
        });
      } catch {
        // never let a bad subscriber kill the step
      }

      return {
        status: 'completed',
        output: {
          imageId: newImageId,
          sourceImageId: imageId,
          width: fitted.width,
          height: fitted.height,
          mimeType: fitted.mimeType,
        },
      };
    },
  };
}
