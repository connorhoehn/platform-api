// platform-api/src/pipeline/image/ImageVisionClassifyStepHandler.ts
//
// Step handler for `image:vision-classify`. Classifies an image into one
// of a fixed label set using the `classification-enum` schema preset.
// Delegates all lifecycle management to `createVisionLikeHandler`.
//
// Default model: claude-haiku-4-5-20251001 (cheap; classification is
// a low-complexity task that doesn't need Sonnet-level reasoning).
// Default maxOutputTokens: 256 (label + confidence + one-sentence rationale
// fit comfortably within this budget).

import type { DescribedStepHandler } from '../customStepHandlers';
import type { StepHandler } from 'distributed-core/applications/pipeline';
import { createVisionLikeHandler, type VisionLikeHandlerOpts } from './visionHandlerFactory';
import { buildPresetSchema } from './visionSchemaPresets';

export interface ImageVisionClassifyNodeData {
  type: 'image:vision-classify';
  /** Anthropic model id. Default: 'claude-haiku-4-5-20251001'. */
  model?: string;
  /** JSONPath reference to the image (e.g. '$.trigger.imageRef'). */
  imageRefPath: string;
  /** The fixed set of allowed label values. At least one required. */
  labels: string[];
  /** Optional override for the system prompt. */
  systemPrompt?: string;
  /** Max output tokens. Default: 256. */
  maxOutputTokens?: number;
}

const DEFAULT_MODEL = 'claude-haiku-4-5-20251001';
const DEFAULT_MAX_OUTPUT_TOKENS = 256;

export function createImageVisionClassifyStepHandler(
  opts: VisionLikeHandlerOpts,
): DescribedStepHandler {
  // Spread rather than wrapped: the factory returns a plain object, and the
  // catalog reads these three fields straight off it.
  return {
    description:
      'Asks a vision model which ONE of the node\'s labels the image belongs to. The label set goes into the prompt and is enforced by an enum schema, so the answer is always one of them.',
    reads: 'The image at imageRefPath — a string imageId, or an object carrying one.',
    emits:
      'extracted, holding the chosen label and its confidence, plus the caption, the model id and the token counts. The output is an object, so it merges into the root context as well as sitting under this step\'s id.',
    ...createVisionLikeHandler<ImageVisionClassifyNodeData>(
    {
      kind: 'image:vision-classify',
      defaultModel: DEFAULT_MODEL,
      defaultMaxOutputTokens: DEFAULT_MAX_OUTPUT_TOKENS,
      getImageRefPath: (data) => data.imageRefPath,
      getModel: (data) => data.model,
      getMaxOutputTokens: (data) => data.maxOutputTokens,
      buildSchema: (data) =>
        buildPresetSchema('classification-enum', { labels: data.labels }),
      buildSystemPrompt: (data) =>
        data.systemPrompt ?? 'You are a precise image classification assistant.',
      buildUserPrompt: (data) => {
        const labelsStr = data.labels.join(', ');
        return (
          `Classify this image as one of: ${labelsStr}. ` +
          'Return the chosen label with a confidence 0-1 and a one-sentence rationale, ' +
          'plus a brief caption.'
        );
      },
    },
    opts,
    ),
  };
}
