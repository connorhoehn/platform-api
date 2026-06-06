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
  | 'detection-regions'    // { regions: [{label, confidence, bbox}], caption }
  | 'multi-label-tags'     // { tags: [{tag, confidence}], caption }
  | 'classification-enum'  // built dynamically from node.data.labels
  | 'ocr-records'          // { fullText, lines: [{text, confidence, region}], language }
  | 'caption-only';        // pass-through; no extracted JSON

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

    default: {
      // TypeScript exhaustiveness guard — should never reach here at runtime.
      const _exhaustive: never = preset;
      throw new Error(`Unknown VisionSchemaPreset: ${String(_exhaustive)}`);
    }
  }
}
