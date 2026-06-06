// platform-api/src/pipeline/image/ImageGenerateStepHandler.ts
//
// Custom step handler for `node.data.type === 'image:generate'`.
// Text-to-image via AWS Bedrock Titan Image Generator v2.
//
// === Model ===
// Default: `amazon.titan-image-generator-v2` (configurable via
// `data.model` or `IMAGE_GENERATE_MODEL` env var).
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
// ThrottlingException          → status:'failed', error:'rate limited; retry after backoff'
// empty images array           → status:'failed', error:'no images returned (content policy?)'
// ContentFilterException / any → status:'failed', error:e.message
//
// AWS region: PIPELINE_IMAGE_AWS_REGION → AWS_REGION → 'us-east-1'.

import { randomUUID } from 'crypto';
import {
  BedrockRuntimeClient,
  InvokeModelCommand,
} from '@aws-sdk/client-bedrock-runtime';
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

interface ImageGenerateNodeData {
  type: 'image:generate';
  /** Prompt text; supports {{path}} interpolation via resolveTemplate. */
  prompt: string;
  /**
   * Bedrock model ID. Default: `amazon.titan-image-generator-v2`.
   * Also overridable via IMAGE_GENERATE_MODEL env var (data wins).
   */
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

interface BedrockImageGenerateResponse {
  images?: string[];   // base64-encoded PNG strings
  error?: string;
}

export interface ImageGenerateStepHandlerOpts {
  imageBlobStore: ImageBlobStore;
  /** Inject a custom BedrockRuntimeClient (tests). Defaults to a real one. */
  bedrockClient?: BedrockRuntimeClient;
}

const DEFAULT_MODEL = 'amazon.titan-image-generator-v2';
const THROTTLING_NAMES = new Set(['ThrottlingException', 'TooManyRequestsException']);

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
      const modelId =
        data.model ??
        process.env['IMAGE_GENERATE_MODEL'] ??
        DEFAULT_MODEL;
      const width = data.width ?? 512;
      const height = data.height ?? 512;
      const numImages = data.numImages ?? 1;

      // ── Build Bedrock request body ────────────────────────────────────────
      const imageGenConfig: Record<string, unknown> = {
        width,
        height,
        numberOfImages: numImages,
      };
      if (data.seed !== undefined) {
        imageGenConfig['seed'] = data.seed;
      }

      const bedrockBody = JSON.stringify({
        taskType: 'TEXT_IMAGE',
        textToImageParams: { text: prompt },
        imageGenerationConfig: imageGenConfig,
      });

      // ── Invoke Bedrock ────────────────────────────────────────────────────
      const region =
        process.env['PIPELINE_IMAGE_AWS_REGION'] ??
        process.env['AWS_REGION'] ??
        'us-east-1';

      const client =
        opts.bedrockClient ??
        new BedrockRuntimeClient({ region });

      let responseBody: BedrockImageGenerateResponse;
      try {
        const command = new InvokeModelCommand({
          modelId,
          body: bedrockBody,
          contentType: 'application/json',
          accept: 'application/json',
        });
        const response = await client.send(command);
        const bodyText = Buffer.from(response.body).toString('utf-8');
        responseBody = JSON.parse(bodyText) as BedrockImageGenerateResponse;
      } catch (err) {
        const name = (err as Record<string, unknown>)['name'];
        if (typeof name === 'string' && THROTTLING_NAMES.has(name)) {
          return { status: 'failed', error: 'rate limited; retry after backoff' };
        }
        return {
          status: 'failed',
          error: `image:generate: Bedrock invocation failed: ${
            err instanceof Error ? err.message : String(err)
          }`,
        };
      }

      const images = responseBody.images ?? [];
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
