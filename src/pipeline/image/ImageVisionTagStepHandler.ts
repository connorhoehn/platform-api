// platform-api/src/pipeline/image/ImageVisionTagStepHandler.ts
//
// Step handler for `image:vision-tag`. Extracts a ranked list of tags
// from an image using the `multi-label-tags` schema preset.
// Delegates all lifecycle management to `createVisionLikeHandler`.
//
// Default model: claude-haiku-4-5-20251001 (tagging is a repetitive,
// high-volume task suited to the faster, cheaper model).
// Default maxOutputTokens: 512 (up to 10 tags + caption fit here easily).

import type { DescribedStepHandler } from '../customStepHandlers';
import type { StepHandler } from 'distributed-core/applications/pipeline';
import { createVisionLikeHandler, type VisionLikeHandlerOpts } from './visionHandlerFactory';
import { buildPresetSchema } from './visionSchemaPresets';

export interface ImageVisionTagNodeData {
  type: 'image:vision-tag';
  /** Anthropic model id. Default: 'claude-haiku-4-5-20251001'. */
  model?: string;
  /** JSONPath reference to the image (e.g. '$.trigger.imageRef'). */
  imageRefPath: string;
  /** Optional curated list of candidate tags to bias selection toward. */
  candidateTags?: string[];
  /** Maximum number of tags to return. Default: 10. */
  maxTags?: number;
  /** Optional override for the system prompt. */
  systemPrompt?: string;
  /** Max output tokens. Default: 512. */
  maxOutputTokens?: number;
}

const DEFAULT_MODEL = 'claude-haiku-4-5-20251001';
const DEFAULT_MAX_OUTPUT_TOKENS = 512;
const DEFAULT_MAX_TAGS = 10;

export function createImageVisionTagStepHandler(
  opts: VisionLikeHandlerOpts,
): DescribedStepHandler {
  // Spread rather than wrapped: the factory returns a plain object, and the
  // catalog reads these three fields straight off it.
  return {
    description:
      'Asks a vision model for up to N descriptive tags with confidence scores, ordered most to least confident. A candidate list on the node is offered to the model as a preference, not a restriction.',
    reads: 'The image at imageRefPath — a string imageId, or an object carrying one.',
    emits:
      'extracted, holding the ranked tags, plus the caption, the model id and the token counts. The output is an object, so it merges into the root context as well as sitting under this step\'s id.',
    ...createVisionLikeHandler<ImageVisionTagNodeData>(
    {
      kind: 'image:vision-tag',
      defaultModel: DEFAULT_MODEL,
      defaultMaxOutputTokens: DEFAULT_MAX_OUTPUT_TOKENS,
      getImageRefPath: (data) => data.imageRefPath,
      getModel: (data) => data.model,
      getMaxOutputTokens: (data) => data.maxOutputTokens,
      buildSchema: (data) =>
        buildPresetSchema('multi-label-tags', {
          maxTags: data.maxTags ?? DEFAULT_MAX_TAGS,
        }),
      buildSystemPrompt: (data) => data.systemPrompt ?? '',
      buildUserPrompt: (data) => {
        const maxTags = data.maxTags ?? DEFAULT_MAX_TAGS;
        const lines = [
          `Tag this image with up to ${maxTags} descriptive tags.`,
          'For each tag include a confidence score from 0 to 1.',
          'Order tags from most to least confident.',
          'Also include a brief caption describing the image overall.',
        ];
        if (data.candidateTags && data.candidateTags.length > 0) {
          lines.push(
            `Prefer tags from this candidate list when applicable: ${data.candidateTags.join(', ')}.`,
          );
        }
        return lines.join(' ');
      },
    },
    opts,
    ),
  };
}
