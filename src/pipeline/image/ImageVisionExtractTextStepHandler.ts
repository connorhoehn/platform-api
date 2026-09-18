// platform-api/src/pipeline/image/ImageVisionExtractTextStepHandler.ts
//
// Step handler for `image:vision-extract-text` (OCR). Transcribes all
// visible text from an image using the `ocr-records` schema preset.
// Delegates all lifecycle management to `createVisionLikeHandler`.
//
// Default model: claude-sonnet-4-6 (OCR accuracy matters more here than
// speed — Sonnet handles complex layouts, mixed scripts, and low-contrast
// text far better than Haiku).
// Default maxOutputTokens: 4096 (dense documents can produce large
// transcriptions; cap high to avoid truncation artefacts).

import type { DescribedStepHandler } from '../customStepHandlers';
import type { StepHandler } from 'distributed-core/applications/pipeline';
import { createVisionLikeHandler, type VisionLikeHandlerOpts } from './visionHandlerFactory';
import { buildPresetSchema } from './visionSchemaPresets';

export interface ImageVisionExtractTextNodeData {
  type: 'image:vision-extract-text';
  /** Anthropic model id. Default: 'claude-sonnet-4-6'. */
  model?: string;
  /** JSONPath reference to the image (e.g. '$.trigger.imageRef'). */
  imageRefPath: string;
  /**
   * Optional system prompt override. When omitted the default OCR
   * system prompt is used: verbatim transcription, preserve line breaks,
   * no interpretation or summarisation.
   */
  systemPrompt?: string;
  /** Max output tokens. Default: 4096. */
  maxOutputTokens?: number;
}

const DEFAULT_MODEL = 'claude-sonnet-4-6';
const DEFAULT_MAX_OUTPUT_TOKENS = 4096;

const DEFAULT_SYSTEM_PROMPT = [
  'You are an OCR assistant.',
  'Transcribe all visible text verbatim.',
  'Preserve line breaks.',
  'Do not interpret or summarize.',
].join(' ');

const DEFAULT_USER_PROMPT = 'Transcribe all visible text in this image.';

export function createImageVisionExtractTextStepHandler(
  opts: VisionLikeHandlerOpts,
): DescribedStepHandler {
  // Spread rather than wrapped: the factory returns a plain object, and the
  // catalog reads these three fields straight off it.
  return {
    description:
      'Transcribes the visible text in the image — OCR by way of the vision model rather than a dedicated engine.',
    reads: 'The image at imageRefPath — a string imageId, or an object carrying one.',
    emits:
      'extracted, holding the transcribed text, plus the caption, the model id and the token counts. The output is an object, so it merges into the root context as well as sitting under this step\'s id.',
    ...createVisionLikeHandler<ImageVisionExtractTextNodeData>(
    {
      kind: 'image:vision-extract-text',
      defaultModel: DEFAULT_MODEL,
      defaultMaxOutputTokens: DEFAULT_MAX_OUTPUT_TOKENS,
      getImageRefPath: (data) => data.imageRefPath,
      getModel: (data) => data.model,
      getMaxOutputTokens: (data) => data.maxOutputTokens,
      buildSchema: () => buildPresetSchema('ocr-records'),
      buildSystemPrompt: (data) => data.systemPrompt ?? DEFAULT_SYSTEM_PROMPT,
      buildUserPrompt: () => DEFAULT_USER_PROMPT,
    },
    opts,
    ),
  };
}
