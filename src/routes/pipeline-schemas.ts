// platform-api/src/routes/pipeline-schemas.ts
//
// GET /api/pipeline/output-schemas
//
// Exposes the output JSON Schema for each image-pipeline node kind so the
// editor's scatter path picker can offer a schema tree instead of a raw
// text box. Schemas are build-time constants (no DB reads).
//
// === Why null for some kinds? ===
// Node kinds whose output schema depends on per-node configuration (e.g.
// `image:vision-classify` whose enum values come from `node.data.labels`)
// return null here. The editor synthesises those schemas client-side from
// the live node data, and the picker falls back to plain JsonPathInput when
// no schema is available.
//
// === Auth ===
// No auth guard: schemas are structural metadata, not user data. Consistent
// with the other read-only introspection endpoints (/api/pipelines/health,
// /api/pipelines/validate) that skip auth on non-sensitive payloads.

import { Router } from 'express';
import { buildPresetSchema } from '../pipeline/image/visionSchemaPresets';

export const pipelineSchemasRouter = Router();

// Crop and resize both produce an image reference object with dimensions.
const IMAGE_REF_SCHEMA = {
  type: 'object',
  required: ['imageId', 'width', 'height', 'mimeType'],
  properties: {
    imageId: { type: 'string', description: 'Stable identifier for the produced image.' },
    parentImageId: { type: 'string', description: 'Source image identifier (present on crop/resize).' },
    width: { type: 'number', description: 'Output width in pixels.' },
    height: { type: 'number', description: 'Output height in pixels.' },
    mimeType: { type: 'string', description: 'MIME type, e.g. image/jpeg.' },
    bbox: {
      type: 'object',
      description: 'Crop bounding box, present when the handler is image:crop.',
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
} as const;

// Hardcoded map: node kind → output schema (or null for dynamic schemas).
// Extend this map when new image-producing handlers are added.
const OUTPUT_SCHEMAS: Record<string, Record<string, unknown> | null> = {
  // Vision handlers whose schema is fixed at build time.
  'image:vision-analyze': buildPresetSchema('detection-regions') ?? null,
  'image:vision-tag': buildPresetSchema('multi-label-tags') ?? null,
  'image:vision-extract-text': buildPresetSchema('ocr-records') ?? null,

  // Dynamic schema: `labels` array comes from node.data, not this route.
  // The picker synthesises a classification-enum schema client-side when it
  // has access to the live node data.
  'image:vision-classify': null,

  // Caption-only — no structured extracted field; schema is just { caption }.
  'image:vision-describe': {
    type: 'object',
    required: ['caption'],
    properties: {
      caption: { type: 'string', description: 'Free-text image description.' },
    },
    additionalProperties: false,
  },

  // Sharp transform handlers.
  'image:crop': IMAGE_REF_SCHEMA,
  'image:resize': IMAGE_REF_SCHEMA,

  // Format conversion — same image-ref shape, mimeType reflects target format.
  'image:format-convert': IMAGE_REF_SCHEMA,

  // Adjustment handlers write the adjusted image to a new imageId.
  'image:adjust-bw': IMAGE_REF_SCHEMA,
  'image:adjust-layer': IMAGE_REF_SCHEMA,
  'image:sharpen': IMAGE_REF_SCHEMA,
  'image:geometric-transform': IMAGE_REF_SCHEMA,
  'image:watermark': IMAGE_REF_SCHEMA,

  // Source / save — these nodes are pipeline boundary points. Their output
  // schema documents what downstream steps can read.
  'image:source': {
    type: 'object',
    required: ['imageId'],
    properties: {
      imageId: { type: 'string', description: 'Resolved imageId for the source image.' },
      width: { type: 'number' },
      height: { type: 'number' },
      mimeType: { type: 'string' },
    },
    additionalProperties: false,
  },
  'image:save': {
    type: 'object',
    required: ['imageId', 'storageKey'],
    properties: {
      imageId: { type: 'string' },
      storageKey: { type: 'string', description: 'S3 key or equivalent storage path.' },
      width: { type: 'number' },
      height: { type: 'number' },
      mimeType: { type: 'string' },
    },
    additionalProperties: false,
  },

  // Variation / generation handlers.
  'image:variation': IMAGE_REF_SCHEMA,
  'image:generate': {
    type: 'object',
    required: ['imageId'],
    properties: {
      imageId: { type: 'string' },
      width: { type: 'number' },
      height: { type: 'number' },
      mimeType: { type: 'string' },
      prompt: { type: 'string', description: 'The prompt used to generate the image.' },
    },
    additionalProperties: false,
  },

  // Edit-prompt handler (inpainting / instructed edits).
  'image:edit-prompt': IMAGE_REF_SCHEMA,
};

export interface PipelineOutputSchemasResponse {
  schemas: Record<string, Record<string, unknown> | null>;
}

pipelineSchemasRouter.get('/', (_req, res) => {
  const response: PipelineOutputSchemasResponse = { schemas: OUTPUT_SCHEMAS };
  res.json(response);
});
