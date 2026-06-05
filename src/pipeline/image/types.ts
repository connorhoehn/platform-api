// platform-api/src/pipeline/image/types.ts
//
// Single source of truth for the image-vision step data model. Shared by:
//   - VisionClient (spawns claude -p with image content block)
//   - VisionStepHandler (executes the step, emits events)
//   - VisionResultRepository (DDB persistence)
//   - ImageBlobStore (DDB blob store for image bytes)
//   - ui-components/pipelines/vision (renders VisionResult — already shipped)
//
// Server-side authoritative subset of the ui-components VisionResult
// shape. The ui-components type has many optional fields the consumer-
// side renderer handles gracefully; this type carries only what a v1
// Claude vision call actually produces.
//
// Code parity with platform-api/src/pipeline/judge/types.ts.

export type VisionMode = 'caption' | 'extract';
export type VisionFailureReason =
  | 'image-fetch'
  | 'input-resolution'
  | 'malformed'
  | 'aborted'
  | 'refusal'
  | 'unknown';

/**
 * Node-data shape on the pipeline graph. Matches the DC-21
 * customStepHandlers dispatch key on `data.type === 'image:vision-analyze'`.
 */
export interface ImageVisionAnalyzeStepConfig {
  /** Discriminator that the executor's customStepHandlers map keys on. */
  type: 'image:vision-analyze';
  /** Anthropic model id. Falls back to global default. */
  model?: string;
  /** Optional override of the canned vision system prompt. */
  systemPrompt?: string;
  /** User prompt; supports {{context.…}} substitution. */
  userPrompt: string;
  /**
   * JSONPath-shaped reference to the image. Examples:
   *   '$.trigger.imageRef'        — image came in via webhook trigger
   *   '$.steps.<id>.output.imageRef' — image is a previous step's output
   *
   * Whatever the path resolves to must be a string (imageId) or an
   * object with `imageId` field.
   */
  imageRefPath: string;
  /**
   * Optional JSON Schema (as a plain object). When supplied, the prompt
   * is augmented with schema instructions and the result's `extracted`
   * field carries the parsed/validated object. When omitted, mode is
   * 'caption' and the result carries only `caption` + `rawText`.
   */
  outputSchema?: Record<string, unknown>;
  /** Maximum tokens the vision call may emit. Default: 2048. */
  maxOutputTokens?: number;
}

/**
 * Input shape consumed by `VisionClient.analyze()`. Resolved by the
 * `VisionStepHandler` from the node's `ImageVisionAnalyzeStepConfig` +
 * the fetched image bytes.
 */
export interface VisionAnalyzeInput {
  model: string;
  systemPrompt: string;
  userPrompt: string;
  /** Raw image bytes (base64-encoded inline when sent to the CLI). */
  imageBytes: Buffer;
  imageMimeType: string;
  /** Optional JSON Schema for structured-output mode. */
  outputSchema?: Record<string, unknown>;
  maxOutputTokens: number;
  /** Cancellation hook from the executor; SIGTERM → SIGKILL the child. */
  abortSignal?: AbortSignal;
}

/**
 * Anthropic usage subset (mirror of judge AnthropicUsage). `image_tokens`
 * is a synthetic field we compute from the image dimensions when the
 * CLI doesn't surface it directly; provider-specific calculation.
 */
export interface AnthropicVisionUsage {
  input_tokens: number;
  output_tokens: number;
  image_tokens?: number;
  cache_creation_input_tokens?: number;
  cache_read_input_tokens?: number;
}

/**
 * The structured vision payload. Carries everything a downstream
 * consumer needs to render, audit, or roll up.
 *
 * v1 is intentionally lean — only the fields the Claude CLI vision
 * call actually produces. The ui-components VisionResult type has many
 * more optional fields (people[], objects[], colors, exposure, etc.)
 * for richer providers that ship structured detection alongside
 * caption; those land here when a real producer fills them.
 */
export interface VisionResult {
  visionId: string;
  runId: string;
  stepId: string;
  pipelineId: string;
  imageId: string;
  /** Caption / freeform analysis text. Always present. */
  caption: string;
  /**
   * Parsed object conforming to the step's `outputSchema`, when one
   * was supplied. Omitted in pure-caption mode.
   */
  extracted?: unknown;
  /** Verbatim model response — useful for audit + re-parse on schema bumps. */
  rawModelText?: string;
  model: string;
  tokensIn: number;
  tokensOut: number;
  imageTokens?: number;
  cacheReadTokens?: number;
  cacheCreationTokens?: number;
  latencyMs: number;
  costUsd: number;
  /** True when the system/user prefix was served from cache (CLI: always false). */
  cacheHit: boolean;
  /** ISO-8601 wall-clock for the vision call. */
  analyzedAt: string;
}

/**
 * Image metadata stored alongside the raw bytes in the
 * `pipeline-images` DDB table. Mirror of ImageBlobStore's row shape.
 */
export interface ImageMeta {
  mimeType: string;
  sizeBytes: number;
  width: number;
  height: number;
  uploadedAt: string;
  uploadedBy?: string;
}
