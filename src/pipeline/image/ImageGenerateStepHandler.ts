// platform-api/src/pipeline/image/ImageGenerateStepHandler.ts
//
// Custom step handler for `node.data.type === 'image:generate'`.
// Text-to-image via a pluggable provider (ImageGenClient).
//
// === Provider ===
// Selected by ImageGenClientFactory: a self-hosted FLUX server when
// FLUX_ENDPOINT_URL is set (the live image path — Bedrock has no active
// image-output model on this account), else the legacy Bedrock client.
// `data.model` / IMAGE_GENERATE_MODEL pass through to whichever provider
// is active.
//
// === Output ===
// numImages === 1 → single-image output + emit.
// numImages  > 1 → fan-out output (fanOut.items) + one emit per image.
//
// === 350KB DDB cap ===
// Generated images are base64-decoded, then cap-fitted via
// `capFitImage()` (resize-inside-512 + PNG-compress → JPEG fallback)
// before hitting the blob store.
//
// === Failure modes (non-throwing) ===
// throttling (429/503/Bedrock)  → status:'failed', error:'rate limited; retry after backoff'
// empty images array            → status:'failed', error:'no images returned (content policy?)'
// any provider error            → status:'failed', error:e.message

import { randomUUID } from 'crypto';
import type { BedrockRuntimeClient } from '@aws-sdk/client-bedrock-runtime';
import type {
  PipelineNode,
  StepHandler,
  StepHandlerHelpers,
  StepOutcome,
} from 'distributed-core/applications/pipeline';
import type { ImageBlobStore } from './ImageBlobStore';
import type { ImageMeta } from './types';
import { resolveTemplate } from './ImageSaveStepHandler';
import { capFitImage } from './imageBlobCapFit';
import {
  ImageGenThrottlingError,
  type ImageGenClient,
} from './ImageGenClient';
import { BedrockImageGenClient } from './BedrockImageGenClient';
import { createImageGenClient } from '../factories/ImageGenClientFactory';

interface ImageGenerateNodeData {
  type: 'image:generate';
  /** Prompt text; supports {{path}} interpolation via resolveTemplate. */
  prompt: string;
  /** Provider-specific model id (FLUX checkpoint / Bedrock modelId). Optional. */
  model?: string;
  /** Output width in pixels. Default: 512. */
  width?: number;
  /** Output height in pixels. Default: 512. */
  height?: number;
  /** Optional random seed for reproducible generation. */
  seed?: number;
  /** Number of images to generate. Default: 1. */
  numImages?: number;
}

export interface ImageGenerateStepHandlerOpts {
  imageBlobStore: ImageBlobStore;
  /** Inject an ImageGenClient directly (preferred for tests). */
  imageGenClient?: ImageGenClient;
  /**
   * Legacy seam: inject a raw BedrockRuntimeClient. Wrapped in a
   * BedrockImageGenClient. Kept so existing tests keep working.
   */
  bedrockClient?: BedrockRuntimeClient;
}

export function createImageGenerateStepHandler(
  opts: ImageGenerateStepHandlerOpts,
): StepHandler {
  return {
    kind: 'image:generate',
    async execute(
      node: PipelineNode,
      context: Record<string, unknown>,
      helpers: StepHandlerHelpers,
    ): Promise<StepOutcome> {
      const data = node.data as unknown as ImageGenerateNodeData;

      if (!data.prompt) {
        return { status: 'failed', error: 'image:generate: prompt is required' };
      }

      const prompt = resolveTemplate(data.prompt, context);
      const width = data.width ?? 512;
      const height = data.height ?? 512;
      const numImages = data.numImages ?? 1;

      // ── Generate via the selected provider ────────────────────────────────
      // Provider seam (ImageGenClient): FLUX self-hosted server by default
      // when FLUX_ENDPOINT_URL is set, else Bedrock. Tests inject a client
      // directly; the legacy `bedrockClient` opt is still honored.
      const client: ImageGenClient =
        opts.imageGenClient ??
        (opts.bedrockClient
          ? new BedrockImageGenClient(opts.bedrockClient)
          : createImageGenClient());

      let images: string[];
      try {
        const result = await client.generate({
          prompt,
          width,
          height,
          numImages,
          ...(data.seed !== undefined ? { seed: data.seed } : {}),
          ...(data.model !== undefined ? { model: data.model } : {}),
        });
        images = result.images;
      } catch (err) {
        if (err instanceof ImageGenThrottlingError) {
          return { status: 'failed', error: 'rate limited; retry after backoff' };
        }
        return {
          status: 'failed',
          error: `image:generate: ${client.provider} invocation failed: ${
            err instanceof Error ? err.message : String(err)
          }`,
        };
      }

      if (images.length === 0) {
        return {
          status: 'failed',
          error: 'image:generate: no images returned (content policy?)',
        };
      }

      // ── Process each image ─────────────────────────────────────────────────
      const imageIds: string[] = [];

      for (const b64 of images) {
        const rawBytes = Buffer.from(b64, 'base64');
        let fitted: Awaited<ReturnType<typeof capFitImage>>;
        try {
          fitted = await capFitImage(rawBytes, 'image:generate');
        } catch (err) {
          return {
            status: 'failed',
            error: err instanceof Error ? err.message : String(err),
          };
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
            error: `image:generate: blob-store put failed: ${
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

        imageIds.push(newImageId);
      }

      // ── Single-image output ────────────────────────────────────────────────
      if (numImages === 1) {
        const id = imageIds[0]!;
        const stored = await opts.imageBlobStore.get(id);
        return {
          status: 'completed',
          output: {
            imageId: id,
            width: stored?.meta.width ?? width,
            height: stored?.meta.height ?? height,
            mimeType: stored?.meta.mimeType ?? 'image/png',
          },
        };
      }

      // ── Fan-out output (numImages > 1) ────────────────────────────────────
      return {
        status: 'completed',
        output: { imageIds },
        fanOut: {
          items: imageIds.map((id) => ({ imageId: id })),
          itemAlias: 'generatedImage',
          maxConcurrency: 4,
        },
      };
    },
  };
}
