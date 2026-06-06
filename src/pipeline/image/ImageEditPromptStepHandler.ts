// platform-api/src/pipeline/image/ImageEditPromptStepHandler.ts
//
// Custom step handler for `node.data.type === 'image:edit-prompt'`.
// Inpainting via AWS Bedrock Nova Canvas.
//
// === Masking strategy ===
//   - `maskImagePath` provided → INPAINTING with explicit PNG mask image
//     (white pixels = area to paint). The mask image must already be in
//     the blob store keyed by its imageId.
//   - `maskImagePath` absent → INPAINTING with `maskPrompt` (Nova Canvas
//     auto-generates the mask from a text description). Prompt text is
//     used as both `maskPrompt` and `text`.
//
// === Model ===
// Default: `amazon.nova-canvas-v1:0`.
//
// === Source resolution ===
// Same fallback chain as other image handlers:
//   $.scatter.parentImageId → $.trigger.imageRef → $.trigger.imageId
//
// === Output ===
// Single-image output: `{ imageId, parentImageId, width, height,
// mimeType, materialized: true }` + emit `pipeline:step:image:produced`.
//
// === 350KB DDB cap ===
// Generated image cap-fitted via `capFitImage()` before blob-store put.
//
// === Failure modes (non-throwing) ===
// Missing source/mask → status:'failed'
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
import { resolveTemplate } from './ImageSaveStepHandler';
import { capFitImage } from './imageBlobCapFit';

interface ImageEditPromptNodeData {
  type: 'image:edit-prompt';
  /** JSONPath to the source imageId. Default: fallback chain. */
  sourceImagePath?: string;
  /**
   * Optional imageId of the mask PNG (white = area to paint).
   * When present, resolves the mask from the blob store.
   * When absent, Nova Canvas uses `maskPrompt` (text-driven mask).
   */
  maskImagePath?: string;
  /**
   * Edit instruction. Supports {{path}} interpolation.
   * Used as `text` in all cases, and as `maskPrompt` when no explicit
   * mask image is provided.
   */
  prompt: string;
  /** Bedrock model ID. Default: 'amazon.nova-canvas-v1:0'. */
  model?: string;
}

interface BedrockEditResponse {
  images?: string[];
}

export interface ImageEditPromptStepHandlerOpts {
  imageBlobStore: ImageBlobStore;
  /** Inject a custom BedrockRuntimeClient (tests). */
  bedrockClient?: BedrockRuntimeClient;
}

const DEFAULT_MODEL = 'amazon.nova-canvas-v1:0';
const DEFAULT_SOURCE_PATH = '$.scatter.parentImageId';
const FALLBACK_SOURCE_PATHS = ['$.trigger.imageRef', '$.trigger.imageId'];
const THROTTLING_NAMES = new Set(['ThrottlingException', 'TooManyRequestsException']);

export function createImageEditPromptStepHandler(
  opts: ImageEditPromptStepHandlerOpts,
): StepHandler {
  return {
    kind: 'image:edit-prompt',
    async execute(
      node: PipelineNode,
      context: Record<string, unknown>,
      helpers: StepHandlerHelpers,
    ): Promise<StepOutcome> {
      const data = node.data as unknown as ImageEditPromptNodeData;
      const sourcePath = data.sourceImagePath ?? DEFAULT_SOURCE_PATH;
      const modelId = data.model ?? DEFAULT_MODEL;

      if (!data.prompt) {
        return { status: 'failed', error: 'image:edit-prompt: prompt is required' };
      }

      const prompt = resolveTemplate(data.prompt, context);

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
          error: `image:edit-prompt: failed to resolve source imageId at '${sourcePath}' (or fallbacks)`,
        };
      }

      // ── Fetch source bytes ─────────────────────────────────────────────────
      const source = await opts.imageBlobStore.get(sourceId as string);
      if (!source) {
        return {
          status: 'failed',
          error: `image:edit-prompt: source image '${sourceId}' not found in blob store`,
        };
      }

      const srcB64 = source.body.toString('base64');

      // ── Resolve mask (optional) ────────────────────────────────────────────
      let maskImageId: string | undefined;
      if (data.maskImagePath) {
        const resolvedMask = readPath(context, data.maskImagePath);
        maskImageId =
          typeof resolvedMask === 'string' && resolvedMask
            ? resolvedMask
            : data.maskImagePath; // treat as literal imageId if not a valid path
      }

      // ── Build Bedrock request body ─────────────────────────────────────────
      let bedrockBody: string;

      if (maskImageId) {
        // Explicit mask image
        const maskBlob = await opts.imageBlobStore.get(maskImageId);
        if (!maskBlob) {
          return {
            status: 'failed',
            error: `image:edit-prompt: mask image '${maskImageId}' not found in blob store`,
          };
        }
        const maskB64 = maskBlob.body.toString('base64');
        bedrockBody = JSON.stringify({
          taskType: 'INPAINTING',
          inPaintingParams: {
            image: srcB64,
            maskImage: maskB64,
            text: prompt,
          },
        });
      } else {
        // Text-driven mask via maskPrompt
        bedrockBody = JSON.stringify({
          taskType: 'INPAINTING',
          inPaintingParams: {
            image: srcB64,
            maskPrompt: prompt,
            text: prompt,
          },
        });
      }

      // ── Invoke Bedrock ─────────────────────────────────────────────────────
      const region =
        process.env['PIPELINE_IMAGE_AWS_REGION'] ??
        process.env['AWS_REGION'] ??
        'us-east-1';

      const client =
        opts.bedrockClient ??
        new BedrockRuntimeClient({ region });

      let responseBody: BedrockEditResponse;
      try {
        const command = new InvokeModelCommand({
          modelId,
          body: bedrockBody,
          contentType: 'application/json',
          accept: 'application/json',
        });
        const response = await client.send(command);
        const bodyText = Buffer.from(response.body).toString('utf-8');
        responseBody = JSON.parse(bodyText) as BedrockEditResponse;
      } catch (err) {
        const name = (err as Record<string, unknown>)['name'];
        if (typeof name === 'string' && THROTTLING_NAMES.has(name)) {
          return { status: 'failed', error: 'rate limited; retry after backoff' };
        }
        return {
          status: 'failed',
          error: `image:edit-prompt: Bedrock invocation failed: ${
            err instanceof Error ? err.message : String(err)
          }`,
        };
      }

      const images = responseBody.images ?? [];
      if (images.length === 0) {
        return {
          status: 'failed',
          error: 'image:edit-prompt: no images returned (content policy?)',
        };
      }

      // ── Process result image ───────────────────────────────────────────────
      const rawBytes = Buffer.from(images[0]!, 'base64');
      let fitted: Awaited<ReturnType<typeof capFitImage>>;
      try {
        fitted = await capFitImage(rawBytes, 'image:edit-prompt');
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
          error: `image:edit-prompt: blob-store put failed: ${
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
          parentImageId: sourceId,
          width: fitted.width,
          height: fitted.height,
          mimeType: fitted.mimeType,
          materialized: true,
        },
      };
    },
  };
}
