// platform-api/src/pipeline/image/ImageVisionDescribeStepHandler.ts
//
// Step handler for `image:vision-describe`. A cheap per-arm descriptor
// that produces a freeform caption — no structured JSON output schema.
// Delegates all lifecycle management to `createVisionLikeHandler`.
//
// Default model: claude-haiku-4-5-20251001 (description is a high-volume,
// low-cost operation; Haiku is sufficient).
// Default maxOutputTokens: 512 (sufficient for a paragraph-level description).
//
// The node data carries a `userPrompt` field directly (unlike classify/tag
// which synthesize a default prompt internally). This lets authors craft
// arbitrary description queries per node without configuring a label set.

import type { DescribedStepHandler } from '../customStepHandlers';
import type { StepHandler } from 'distributed-core/applications/pipeline';
import { createVisionLikeHandler, type VisionLikeHandlerOpts } from './visionHandlerFactory';

export interface ImageVisionDescribeNodeData {
  type: 'image:vision-describe';
  /** Anthropic model id. Default: 'claude-haiku-4-5-20251001'. */
  model?: string;
  /** JSONPath reference to the image (e.g. '$.trigger.imageRef'). */
  imageRefPath: string;
  /** User-authored prompt; supports {{context.*}} substitution. */
  userPrompt: string;
  /** Optional override for the system prompt. */
  systemPrompt?: string;
  /** Max output tokens. Default: 512. */
  maxOutputTokens?: number;
}

const DEFAULT_MODEL = 'claude-haiku-4-5-20251001';
const DEFAULT_MAX_OUTPUT_TOKENS = 512;

export function createImageVisionDescribeStepHandler(
  opts: VisionLikeHandlerOpts,
): DescribedStepHandler {
  // Spread rather than wrapped: the factory returns a plain object, and the
  // catalog reads these three fields straight off it.
  return {
    description:
      'Asks a vision model to answer the node\'s own prompt about the image. The one vision step with no output schema, so the answer is prose — use classify or tag when something downstream has to branch on it.',
    reads: 'The image at imageRefPath — a string imageId, or an object carrying one.',
    emits:
      'the caption only: no extracted field, because there is no schema. Plus the model id and token counts.',
    ...createVisionLikeHandler<ImageVisionDescribeNodeData>(
    {
      kind: 'image:vision-describe',
      defaultModel: DEFAULT_MODEL,
      defaultMaxOutputTokens: DEFAULT_MAX_OUTPUT_TOKENS,
      getImageRefPath: (data) => data.imageRefPath,
      getModel: (data) => data.model,
      getMaxOutputTokens: (data) => data.maxOutputTokens,
      // Caption-only — no output schema.
      buildSchema: () => undefined,
      buildSystemPrompt: (data) => data.systemPrompt ?? '',
      buildUserPrompt: (data) => data.userPrompt,
    },
    opts,
    ),
  };
}
