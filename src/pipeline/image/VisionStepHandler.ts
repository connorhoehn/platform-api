// platform-api/src/pipeline/image/VisionStepHandler.ts
//
// Custom step handler (DC-21) that dispatches `node.data.type === 'image:vision-analyze'`.
// Lifecycle per call:
//   1. Resolve imageId from context via jsonpath (imageRefPath config).
//   2. Fetch image bytes + metadata from ImageBlobStore.
//   3. Render the user prompt with context substitution.
//   4. Emit pipeline:image:vision:started.
//   5. Call VisionClient.analyze() with abortSignal threaded through.
//   6. Persist VisionResult.
//   7. Emit pipeline:image:vision:analyzed.
//   8. Return StepOutcome to the executor.
//
// Failure mode: non-throwing (parity with judge audit rule #1). Image
// fetch failure, malformed model output, model refusal, or aborted
// fetch surface as `pipeline:image:vision:failed` events + the step
// returns `{status:'failed', error: ...}` with usage populated so cost
// telemetry stays honest.
//
// Code parity with platform-api/src/pipeline/judge/JudgeStepHandler.ts.
//
// === Refactor note ===
// This handler is now a thin shim over `createVisionLikeHandler` from
// visionHandlerFactory.ts. All five vision-family handlers share the same
// lifecycle; the factory eliminates ~250 LOC of duplication. The public
// API (`createVisionStepHandler`, `VisionStepHandlerOpts`,
// `VisionResultRepositoryLike`) is unchanged so external callers (bootstrap,
// registerVisionHandlers) don't break.

import type { DescribedStepHandler } from '../customStepHandlers';
import type { StepHandler } from 'distributed-core/applications/pipeline';
import type { VisionClientLike } from './VisionClient';
import type { ImageBlobStore } from './ImageBlobStore';
import type { ImageVisionAnalyzeStepConfig } from './types';
import { createVisionLikeHandler } from './visionHandlerFactory';

export interface VisionResultRepositoryLike {
  persistVisionResult(result: import('./types').VisionResult): Promise<void>;
}

export interface VisionStepHandlerOpts {
  visionClient: VisionClientLike;
  imageBlobStore: ImageBlobStore;
  repository?: VisionResultRepositoryLike;
  defaultModel?: string;
  /**
   * Resolved at handler-construction time. The executor passes `runId` +
   * `stepId` per call via the helpers; we still need `pipelineId` to
   * route persistence — passed via this lookup.
   */
  pipelineIdLookup: (runId: string) => string | undefined;
}

const DEFAULT_MODEL = 'claude-sonnet-4-6';
const DEFAULT_MAX_OUTPUT_TOKENS = 2048;

/**
 * Thin alias over `createVisionLikeHandler` for the `image:vision-analyze`
 * step type. Preserves the existing public API so bootstrap + tests are
 * unaffected by the factory refactor.
 */
export function createVisionStepHandler(opts: VisionStepHandlerOpts): DescribedStepHandler {
  const defaultModel = opts.defaultModel ?? DEFAULT_MODEL;

  // Spread rather than wrapped: the factory returns a plain object and the
  // catalog reads these three fields straight off it.
  return {
    description:
      'The general-purpose vision step: asks a multimodal model the node\'s own question about an image. Unlike classify, tag and extract-text, which each fix their own schema, this one takes whatever outputSchema the node supplies — give it one to get structured output, leave it off for prose.',
    reads: 'The image at imageRefPath — a string imageId, or an object carrying one — plus the node\'s prompt and optional outputSchema.',
    emits:
      'caption always, extracted when an outputSchema was given, plus the model id and token counts. The output is an object, so it merges into the root context as well as sitting under this step\'s id.',
    ...createVisionLikeHandler<ImageVisionAnalyzeStepConfig>(
    {
      kind: 'image:vision-analyze',
      defaultModel,
      defaultMaxOutputTokens: DEFAULT_MAX_OUTPUT_TOKENS,
      buildSchema: (data) => data.outputSchema,
      buildUserPrompt: (data) => data.userPrompt,
      buildSystemPrompt: (data) => data.systemPrompt ?? '',
      getImageRefPath: (data) => data.imageRefPath,
      getModel: (data) => data.model,
      getMaxOutputTokens: (data) => data.maxOutputTokens,
      getSeedRegions: (data) => data.seedRegions,
    },
    {
      visionClient: opts.visionClient,
      imageBlobStore: opts.imageBlobStore,
      repository: opts.repository,
      pipelineIdLookup: opts.pipelineIdLookup,
      },
    ),
  };
}
