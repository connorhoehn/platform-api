// platform-api/src/pipeline/image/ImageVariationStepHandler.ts
//
// Custom step handler for `node.data.type === 'image:variation'`.
// Generates variations of a source image via AWS Bedrock Nova Canvas.
//
// === Model ===
// Default: `amazon.nova-canvas-v1:0`.
//
// === Source resolution ===
// Same fallback chain as other image handlers:
//   $.scatter.parentImageId → $.trigger.imageRef → $.trigger.imageId
// Or specify `data.sourceImagePath` explicitly.
//
// === Output ===
// Always fan-out (even numVariations=1) so callers get a consistent
// `fanOut.items` shape. Each arm carries `{ imageId }`.
//
// === 350KB DDB cap ===
// Generated images are cap-fitted via `capFitImage()` before blob-store put.
//
// === Failure modes (non-throwing) ===
// Missing source → status:'failed'
// ThrottlingException → status:'failed', error:'rate limited; retry after backoff'
// Empty images array → status:'failed', error:'no images returned (content policy?)'
// Other Bedrock error → status:'failed', error:e.message
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
import { readPath } from './ScatterStepHandler';
import { capFitImage } from './imageBlobCapFit';

interface ImageVariationNodeData {
  type: 'image:variation';
  /** JSONPath to the source imageId. Default: fallback chain. */
  sourceImagePath?: string;
  /** Number of variations to generate. Default: 2. */
  numVariations?: number;
  /**
   * Similarity strength (0.2–1.0). Controls how close the variation is
   * to the source. Maps to Nova Canvas `similarityStrength`. Default: 0.7.
   */
  similarity?: number;
  /** Bedrock model ID. Default: 'amazon.nova-canvas-v1:0'. */
  model?: string;
}

interface BedrockVariationResponse {
  images?: string[];
}

export interface ImageVariationStepHandlerOpts {
  imageBlobStore: ImageBlobStore;
  /** Inject a custom BedrockRuntimeClient (tests). */
  bedrockClient?: BedrockRuntimeClient;
}

const DEFAULT_MODEL = 'amazon.nova-canvas-v1:0';
const DEFAULT_SOURCE_PATH = '$.imageId';
const FALLBACK_SOURCE_PATHS = ['$.scatter.parentImageId',
  '$.trigger.imageRef', '$.trigger.imageId'];
const THROTTLING_NAMES = new Set(['ThrottlingException', 'TooManyRequestsException']);

export function createImageVariationStepHandler(
  opts: ImageVariationStepHandlerOpts,
): StepHandler {
  return {
    kind: 'image:variation',
    async execute(
      node: PipelineNode,
      context: Record<string, unknown>,
      helpers: StepHandlerHelpers,
    ): Promise<StepOutcome> {
      const data = node.data as unknown as ImageVariationNodeData;
      const sourcePath = data.sourceImagePath ?? DEFAULT_SOURCE_PATH;
      const numVariations = data.numVariations ?? 2;
      const similarityStrength = data.similarity ?? 0.7;
      const modelId = data.model ?? DEFAULT_MODEL;

      // ── Resolve source imageId ─────────────────────────────────────────────
      let sourceId = readPath(context, sourcePath);
      if (typeof sourceId !== 'string' || !sourceId) {
        for (const fb of FALLBACK_SOURCE_PATHS) {
          const candidate = readPath(context, fb);
          if (typeof candidate === 'string' && candidate) {
            sourceId = candidate;
            break;
          }
        }
      }
      if (typeof sourceId !== 'string' || !sourceId) {
        return {
          status: 'failed',
          error: `image:variation: failed to resolve source imageId at '${sourcePath}' (or fallbacks)`,
        };
      }

      // ── Fetch source bytes ─────────────────────────────────────────────────
      const source = await opts.imageBlobStore.get(sourceId as string);
      if (!source) {
        return {
          status: 'failed',
          error: `image:variation: source image '${sourceId}' not found in blob store`,
        };
      }

      // ── Build Bedrock request body ─────────────────────────────────────────
      const bedrockBody = JSON.stringify({
        taskType: 'IMAGE_VARIATION',
        imageVariationParams: {
          images: [source.body.toString('base64')],
          similarityStrength,
          numberOfImages: numVariations,
        },
      });

      // ── Invoke Bedrock ─────────────────────────────────────────────────────
      const region =
        process.env['PIPELINE_IMAGE_AWS_REGION'] ??
        process.env['AWS_REGION'] ??
        'us-east-1';

      const client =
        opts.bedrockClient ??
        new BedrockRuntimeClient({ region });

      let responseBody: BedrockVariationResponse;
      try {
        const command = new InvokeModelCommand({
          modelId,
          body: bedrockBody,
          contentType: 'application/json',
          accept: 'application/json',
        });
        const response = await client.send(command);
        const bodyText = Buffer.from(response.body).toString('utf-8');
        responseBody = JSON.parse(bodyText) as BedrockVariationResponse;
      } catch (err) {
        const name = (err as Record<string, unknown>)['name'];
        if (typeof name === 'string' && THROTTLING_NAMES.has(name)) {
          return { status: 'failed', error: 'rate limited; retry after backoff' };
        }
        return {
          status: 'failed',
          error: `image:variation: Bedrock invocation failed: ${
            err instanceof Error ? err.message : String(err)
          }`,
        };
      }

      const images = responseBody.images ?? [];
      if (images.length === 0) {
        return {
          status: 'failed',
          error: 'image:variation: no images returned (content policy?)',
        };
      }

      // ── Process each variation ─────────────────────────────────────────────
      const imageIds: string[] = [];

      for (const b64 of images) {
        const rawBytes = Buffer.from(b64, 'base64');
        let fitted: Awaited<ReturnType<typeof capFitImage>>;
        try {
          fitted = await capFitImage(rawBytes, 'image:variation');
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
            error: `image:variation: blob-store put failed: ${
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

      // ── Fan-out output ─────────────────────────────────────────────────────
      return {
        status: 'completed',
        output: { imageIds, parentImageId: sourceId },
        fanOut: {
          items: imageIds.map((id) => ({ imageId: id })),
          itemAlias: 'generatedImage',
          maxConcurrency: 4,
        },
      };
    },
  };
}
