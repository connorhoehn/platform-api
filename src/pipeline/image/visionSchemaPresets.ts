// platform-api/src/pipeline/image/visionSchemaPresets.ts
//
// Preset JSON Schemas for the chained vision handlers. Each preset
// produces a valid JSON Schema object that is forwarded to the vision
// client as `outputSchema`. The client's buildVisionSystemPrompt
// appends the schema stanza so the model returns structured JSON.
//
// `buildPresetSchema` returns `undefined` for caption-only presets —
// the caller passes `undefined` to VisionAnalyzeInput.outputSchema which
// leaves the client in caption mode (no schema instructions emitted).

export type VisionSchemaPreset =
  | 'detection-regions'      // { regions: [{label, confidence, bbox}], caption }
  | 'multi-label-tags'       // { tags: [{tag, confidence}], caption }
  | 'classification-enum'    // built dynamically from node.data.labels
  | 'ocr-records'            // { fullText, lines: [{text, confidence, region}], language }
  | 'caption-only'           // pass-through; no extracted JSON
  | 'face-detection'         // { faces: [{bbox, confidence, age?, expression?, gaze?}] }
  | 'color-palette'          // { dominant: [{hex, percentage}], temperature }
  | 'image-quality-rating';  // { focus, exposure, composition, overallScore, rationale }

export interface BuildPresetSchemaOptions {
  /** Required for 'classification-enum' — the allowed label values. */
  labels?: string[];
  /** Max number of tags for 'multi-label-tags'. Default 10. */
  maxTags?: number;
}

/**
 * Build a JSON Schema object for the given preset, or `undefined` for
 * caption-only (no schema → caption mode, same as omitting `outputSchema`).
 */
export function buildPresetSchema(
  preset: VisionSchemaPreset,
  options?: BuildPresetSchemaOptions,
): Record<string, unknown> | undefined {
  switch (preset) {
    case 'caption-only':
      return undefined;

    case 'classification-enum': {
      const labels = options?.labels ?? [];
      return {
        type: 'object',
        required: ['label', 'confidence', 'caption'],
        properties: {
          label: {
            type: 'string',
            enum: labels,
          },
          confidence: {
            type: 'number',
            minimum: 0,
            maximum: 1,
          },
          rationale: {
            type: 'string',
          },
          caption: {
            type: 'string',
          },
        },
        additionalProperties: false,
      };
    }

    case 'multi-label-tags': {
      const maxTags = options?.maxTags ?? 10;
      return {
        type: 'object',
        required: ['tags', 'caption'],
        properties: {
          tags: {
            type: 'array',
            maxItems: maxTags,
            items: {
              type: 'object',
              required: ['tag', 'confidence'],
              properties: {
                tag: { type: 'string' },
                confidence: { type: 'number', minimum: 0, maximum: 1 },
              },
              additionalProperties: false,
            },
          },
          caption: { type: 'string' },
        },
        additionalProperties: false,
      };
    }

    case 'ocr-records': {
      return {
        type: 'object',
        required: ['fullText', 'lines', 'caption'],
        properties: {
          fullText: { type: 'string' },
          lines: {
            type: 'array',
            items: {
              type: 'object',
              required: ['text', 'confidence'],
              properties: {
                text: { type: 'string' },
                confidence: { type: 'number', minimum: 0, maximum: 1 },
                region: {
                  type: 'object',
                  properties: {
                    x: { type: 'number' },
                    y: { type: 'number' },
                    width: { type: 'number' },
                    height: { type: 'number' },
                  },
                  additionalProperties: false,
                },
              },
              additionalProperties: false,
            },
          },
          language: { type: 'string' },
          caption: { type: 'string' },
        },
        additionalProperties: false,
      };
    }

    case 'detection-regions': {
      return {
        type: 'object',
        required: ['regions', 'caption'],
        properties: {
          regions: {
            type: 'array',
            items: {
              type: 'object',
              required: ['label', 'confidence', 'bbox'],
              properties: {
                label: { type: 'string' },
                confidence: { type: 'number', minimum: 0, maximum: 1 },
                bbox: {
                  type: 'object',
                  required: ['x', 'y', 'width', 'height'],
                  properties: {
                    x: { type: 'number' },
                    y: { type: 'number' },
                    width: { type: 'number' },
                    height: { type: 'number' },
                  },
                  additionalProperties: false,
                },
              },
              additionalProperties: false,
            },
          },
          caption: { type: 'string' },
        },
        additionalProperties: false,
      };
    }

    case 'face-detection': {
      return {
        type: 'object',
        required: ['faces'],
        properties: {
          faces: {
            type: 'array',
            items: {
              type: 'object',
              required: ['bbox', 'confidence'],
              properties: {
                bbox: {
                  type: 'object',
                  required: ['x', 'y', 'width', 'height'],
                  properties: {
                    x: { type: 'number', minimum: 0, maximum: 1 },
                    y: { type: 'number', minimum: 0, maximum: 1 },
                    width: { type: 'number', minimum: 0, maximum: 1 },
                    height: { type: 'number', minimum: 0, maximum: 1 },
                  },
                  additionalProperties: false,
                },
                confidence: { type: 'number', minimum: 0, maximum: 1 },
                age: { type: 'string' },
                expression: { type: 'string' },
                gaze: { type: 'string' },
              },
              additionalProperties: false,
            },
          },
        },
        additionalProperties: false,
      };
    }

    case 'color-palette': {
      return {
        type: 'object',
        required: ['dominant', 'temperature'],
        properties: {
          dominant: {
            type: 'array',
            maxItems: 5,
            items: {
              type: 'object',
              required: ['hex', 'percentage'],
              properties: {
                hex: { type: 'string' },
                percentage: { type: 'number', minimum: 0, maximum: 100 },
              },
              additionalProperties: false,
            },
          },
          temperature: {
            type: 'string',
            enum: ['warm', 'cool', 'neutral'],
          },
        },
        additionalProperties: false,
      };
    }

    case 'image-quality-rating': {
      return {
        type: 'object',
        required: ['focus', 'exposure', 'composition', 'overallScore', 'rationale'],
        properties: {
          focus: { type: 'integer', minimum: 0, maximum: 10 },
          exposure: { type: 'integer', minimum: 0, maximum: 10 },
          composition: { type: 'integer', minimum: 0, maximum: 10 },
          overallScore: { type: 'integer', minimum: 0, maximum: 10 },
          rationale: { type: 'string' },
        },
        additionalProperties: false,
      };
    }

    default: {
      // TypeScript exhaustiveness guard — should never reach here at runtime.
      const _exhaustive: never = preset;
      throw new Error(`Unknown VisionSchemaPreset: ${String(_exhaustive)}`);
    }
  }
}

// ---------------------------------------------------------------------------
// Default system prompts
// ---------------------------------------------------------------------------
//
// The bench UI and node configs prefill the system-prompt textarea from this
// map when a preset is selected. Handler defaults still live with each
// handler — this map is the canonical author-facing copy.

export const PRESET_DEFAULT_PROMPTS: Record<VisionSchemaPreset, string> = {
  'caption-only':
    'You are an image captioning assistant. Describe the image in one concise sentence.',
  'classification-enum':
    'You are a precise image classification assistant.',
  'multi-label-tags':
    'You are an image tagging assistant. Return up to 10 short descriptive tags with confidence scores.',
  'ocr-records':
    'You are an OCR assistant. Extract all readable text from the image preserving line order.',
  'detection-regions':
    'You are an object detection assistant. Identify salient objects and return labeled bounding boxes with confidence scores.',
  'face-detection':
    'You are a face detection vision model. Identify all human faces in the image and return their bounding boxes (normalized [0,1] coordinates) with optional age/expression/gaze attributes when clearly visible. Return JSON conforming to the schema. Do NOT detect non-human faces.',
  'color-palette':
    'Analyze the dominant colors in this image. Return the top 5 colors as hex codes with their approximate percentage coverage. Classify the overall temperature as warm/cool/neutral.',
  'image-quality-rating':
    "Rate this image's technical and aesthetic quality on focus (sharpness), exposure (lighting), composition (framing/balance) — each 0-10. Provide an overall score 0-10 weighted as 30/30/40. Add one sentence rationale.",
};

/**
 * Convenience accessor — returns the canonical default system prompt for the
 * given preset. Mirrored by the ui-components bench picker.
 */
export function getPresetDefaultPrompt(preset: VisionSchemaPreset): string {
  return PRESET_DEFAULT_PROMPTS[preset];
}
