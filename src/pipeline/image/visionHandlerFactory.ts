// platform-api/src/pipeline/image/visionHandlerFactory.ts
//
// Generic factory that produces any vision-family StepHandler from a
// handler-specific configuration object. All five handlers in the
// `image:vision-*` family use this factory:
//
//   image:vision-analyze       — VisionStepHandler (existing, migrated)
//   image:vision-classify      — ImageVisionClassifyStepHandler
//   image:vision-describe      — ImageVisionDescribeStepHandler
//   image:vision-tag           — ImageVisionTagStepHandler
//   image:vision-extract-text  — ImageVisionExtractTextStepHandler
//
// === Lifecycle (identical across all variants) ===
//   1.  Resolve imageId from context via imageRefPath.
//   2.  Fetch image bytes from ImageBlobStore.
//   3.  Build outputSchema via the handler's buildSchema(nodeData) callback.
//   4.  Render the user prompt with {{context.*}} substitution.
//   5.  Emit pipeline:image:vision:started.
//   6.  Call visionClient.analyze() and stream token events.
//   7.  On failure: emit pipeline:image:vision:failed, return { status:'failed' }.
//   8.  On success: persist via repository, emit pipeline:image:vision:analyzed,
//       return { status:'completed', output: VisionResult }.
//
// === Handler-specific concerns ===
// Each handler supplies:
//   - `kind`               — the dispatch key (e.g. 'image:vision-classify').
//   - `defaultModel`       — model to use when node data omits one.
//   - `defaultMaxOutputTokens` — token cap when node data omits one.
//   - `buildSchema`        — (data) => outputSchema | undefined.
//   - `buildUserPrompt`    — (data) => string (may override per call).
//   - `buildSystemPrompt`  — (data) => string (optional; falls back to '').
//   - `getImageRefPath`    — (data) => string (locates imageRefPath on the node).
//
// Non-throwing per audit rule #1.

import { randomUUID } from 'crypto';
import type {
  PipelineNode,
  StepHandler,
  StepHandlerHelpers,
  StepOutcome,
} from 'distributed-core/applications/pipeline';
import { withContext } from '../../lib/logger';
import { computeVisionCost } from './computeVisionCost';
import type { ImageBlobStore } from './ImageBlobStore';
import type { VisionClientLike } from './VisionClient';
import type {
  ImageMeta,
  VisionAnalyzeInput,
  VisionFailureReason,
  VisionResult,
} from './types';
import type { VisionResultRepositoryLike } from './VisionStepHandler';

const log = withContext({ handler: 'visionHandlerFactory' });

/**
 * Permissive emit cast — mirrors the pattern in VisionStepHandler.
 * DC's PipelineEventMap can't be augmented here; same bridge as existing handler.
 */
type LooseEmit = (event: string, payload: unknown) => Promise<void>;

// ─── Handler descriptor ────────────────────────────────────────────────

/**
 * All the handler-specific knobs. Pass one of these to
 * `createVisionLikeHandler` to get a fully wired StepHandler back.
 */
export interface VisionHandlerDescriptor<TNodeData extends { type: string }> {
  /** The dispatch key — must equal node.data.type. */
  kind: string;
  /** Default Anthropic model id when node data omits one. */
  defaultModel: string;
  /** Default max output tokens when node data omits one. */
  defaultMaxOutputTokens: number;
  /**
   * Called once per execute() to derive the outputSchema (or undefined for
   * caption-only). Called after node.data is cast to TNodeData.
   */
  buildSchema(data: TNodeData): Record<string, unknown> | undefined;
  /**
   * Called once per execute() to derive the user prompt string. The string
   * may contain {{context.*}} placeholders — the factory renders them.
   */
  buildUserPrompt(data: TNodeData): string;
  /**
   * Called once per execute() to derive the system prompt. Return '' to
   * use the client's default vision system prompt.
   */
  buildSystemPrompt(data: TNodeData): string;
  /**
   * Extract the imageRefPath from node data. Most handlers store this as
   * `data.imageRefPath`; override only if the field has a different name.
   */
  getImageRefPath(data: TNodeData): string;
  /**
   * Extract the per-call model override from node data (optional field).
   * Return undefined to fall back to descriptor.defaultModel.
   */
  getModel?(data: TNodeData): string | undefined;
  /**
   * Extract the per-call maxOutputTokens override (optional field).
   * Return undefined to fall back to descriptor.defaultMaxOutputTokens.
   */
  getMaxOutputTokens?(data: TNodeData): number | undefined;
}

// ─── Shared opts (injected at construction time) ───────────────────────

export interface VisionLikeHandlerOpts {
  visionClient: VisionClientLike;
  imageBlobStore: ImageBlobStore;
  repository?: VisionResultRepositoryLike;
  pipelineIdLookup: (runId: string) => string | undefined;
}

// ─── Factory ──────────────────────────────────────────────────────────

/**
 * Produces a fully-wired StepHandler for any vision-family step type.
 *
 * @param descriptor  Handler-specific knobs (kind, defaults, builders).
 * @param opts        Shared infrastructure (client, blob store, repo, id lookup).
 */
export function createVisionLikeHandler<TNodeData extends { type: string }>(
  descriptor: VisionHandlerDescriptor<TNodeData>,
  opts: VisionLikeHandlerOpts,
): StepHandler {
  return {
    kind: descriptor.kind,

    async execute(
      node: PipelineNode,
      context: Record<string, unknown>,
      helpers: StepHandlerHelpers,
    ): Promise<StepOutcome> {
      const emit = helpers.emit as unknown as LooseEmit;
      const data = node.data as unknown as TNodeData;
      const pipelineId = opts.pipelineIdLookup(helpers.runId) ?? 'unknown';
      const visionId = randomUUID();
      const stepAt = new Date().toISOString();

      const model =
        (descriptor.getModel ? descriptor.getModel(data) : undefined) ??
        descriptor.defaultModel;

      const maxOutputTokens =
        (descriptor.getMaxOutputTokens ? descriptor.getMaxOutputTokens(data) : undefined) ??
        descriptor.defaultMaxOutputTokens;

      // ── 1. Resolve imageId from context ──────────────────────────────
      let imageId: string;
      try {
        imageId = resolveImageIdFromContext(context, descriptor.getImageRefPath(data));
      } catch (err) {
        await emit('pipeline:image:vision:failed', {
          runId: helpers.runId,
          stepId: node.id,
          pipelineId,
          reason: 'input-resolution' satisfies VisionFailureReason,
          error: (err as Error).message,
          at: stepAt,
        });
        return { status: 'failed', error: `vision input resolution: ${(err as Error).message}` };
      }

      // ── 2. Fetch image bytes ─────────────────────────────────────────
      let imageRow: { body: Buffer; meta: ImageMeta } | null;
      try {
        imageRow = await opts.imageBlobStore.get(imageId);
      } catch (err) {
        await emit('pipeline:image:vision:failed', {
          runId: helpers.runId,
          stepId: node.id,
          pipelineId,
          imageId,
          reason: 'image-fetch' satisfies VisionFailureReason,
          error: (err as Error).message,
          at: stepAt,
        });
        return { status: 'failed', error: `vision image-fetch: ${(err as Error).message}` };
      }
      if (!imageRow) {
        await emit('pipeline:image:vision:failed', {
          runId: helpers.runId,
          stepId: node.id,
          pipelineId,
          imageId,
          reason: 'image-fetch' satisfies VisionFailureReason,
          error: `imageId ${imageId} not found in blob store`,
          at: stepAt,
        });
        return { status: 'failed', error: `vision image-fetch: ${imageId} not found` };
      }

      // ── 3. Build schema + prompts ────────────────────────────────────
      const outputSchema = descriptor.buildSchema(data);
      const userPrompt = renderUserPrompt(descriptor.buildUserPrompt(data), context);
      const systemPrompt = descriptor.buildSystemPrompt(data);

      // ── 4. Emit started ──────────────────────────────────────────────
      await emit('pipeline:image:vision:started', {
        runId: helpers.runId,
        stepId: node.id,
        pipelineId,
        imageId,
        visionModel: model,
        hasSchema: !!outputSchema,
        at: stepAt,
      });

      // ── 5. Call the vision client + stream tokens to the bus ─────────
      const input: VisionAnalyzeInput = {
        model,
        systemPrompt,
        userPrompt,
        imageBytes: imageRow.body,
        imageMimeType: imageRow.meta.mimeType,
        ...(outputSchema ? { outputSchema } : {}),
        maxOutputTokens,
        ...(helpers.abortSignal ? { abortSignal: helpers.abortSignal } : {}),
      };

      let tokenSeq = 0;
      const callResult = await opts.visionClient.analyze(input, (delta) => {
        tokenSeq += 1;
        emit('pipeline:image:vision:token', {
          runId: helpers.runId,
          stepId: node.id,
          pipelineId,
          imageId,
          seq: tokenSeq,
          delta,
        }).catch((err: unknown) => {
          log.debug({ err }, 'pipeline:image:vision:token emit error (ignored)');
        });
      });

      // ── 6. Handle non-success paths ──────────────────────────────────
      if (!callResult.output) {
        const costUsd = computeVisionCost(model, callResult.usage);
        const stderrSnippet = callResult.rawText?.trim().slice(0, 400) ?? '';
        const reasonLabel = callResult.failureReason ?? 'unknown';
        const composedError = stderrSnippet
          ? `${reasonLabel}: ${stderrSnippet}`
          : reasonLabel === 'unknown'
            ? 'vision call produced no output'
            : reasonLabel;
        await emit('pipeline:image:vision:failed', {
          runId: helpers.runId,
          stepId: node.id,
          pipelineId,
          imageId,
          reason: mapFailureReason(callResult.failureReason),
          error: composedError,
          tokensIn: callResult.usage.input_tokens,
          tokensOut: callResult.usage.output_tokens,
          costUsd,
          latencyMs: callResult.latencyMs,
          at: stepAt,
        });
        return { status: 'failed', error: `vision ${composedError}` };
      }

      // ── 7. Compose final VisionResult + persist ──────────────────────
      const costUsd = computeVisionCost(model, callResult.usage);
      const result: VisionResult = {
        visionId,
        runId: helpers.runId,
        stepId: node.id,
        pipelineId,
        imageId,
        caption: callResult.output.caption,
        ...(callResult.output.extracted !== null ? { extracted: callResult.output.extracted } : {}),
        rawModelText: callResult.rawText,
        model,
        tokensIn: callResult.usage.input_tokens,
        tokensOut: callResult.usage.output_tokens,
        ...(callResult.usage.image_tokens ? { imageTokens: callResult.usage.image_tokens } : {}),
        ...(callResult.usage.cache_read_input_tokens ? { cacheReadTokens: callResult.usage.cache_read_input_tokens } : {}),
        ...(callResult.usage.cache_creation_input_tokens ? { cacheCreationTokens: callResult.usage.cache_creation_input_tokens } : {}),
        latencyMs: callResult.latencyMs,
        costUsd,
        cacheHit: callResult.cacheHit,
        analyzedAt: stepAt,
      };

      if (opts.repository) {
        try {
          await opts.repository.persistVisionResult(result);
        } catch (err) {
          // Persistence failure must not fail the step.
          console.warn('VisionResultRepository.persistVisionResult failed', err);
        }
      }

      // ── 8. Emit analyzed event ───────────────────────────────────────
      await emit('pipeline:image:vision:analyzed', {
        runId: helpers.runId,
        stepId: node.id,
        pipelineId,
        visionId,
        imageId,
        visionModel: model,
        result,
        usage: {
          inputTokens: callResult.usage.input_tokens,
          outputTokens: callResult.usage.output_tokens,
          ...(callResult.usage.image_tokens ? { imageTokens: callResult.usage.image_tokens } : {}),
        },
        costUsd,
        latencyMs: callResult.latencyMs,
        at: stepAt,
      });

      // ── 8a. Also emit the generic produced-image event so the canvas
      // renders an inline thumbnail of the source image on the vision
      // node body (mirrors crop / adjust / save handlers). The vision
      // step does not materialize a NEW image — the thumbnail it surfaces
      // is the source image it analyzed.
      try {
        await emit('pipeline:step:image:produced', {
          runId: helpers.runId,
          stepId: helpers.stepId,
          imageId,
          width: imageRow.meta.width,
          height: imageRow.meta.height,
          mimeType: imageRow.meta.mimeType,
          at: stepAt,
        });
      } catch {
        // never let a subscriber kill the step
      }

      return {
        status: 'completed',
        output: result,
        sourceHandle: 'out',
      };
    },
  };
}

// ─── Shared internal helpers ───────────────────────────────────────────

/**
 * Resolves the imageId from context via a dotted jsonpath. The value at
 * the path can be either a string (the imageId itself) or an object with
 * an `imageId` field. Throws when the path is unresolvable.
 *
 * Exported so VisionStepHandler can import it instead of carrying its own copy.
 */
export function resolveImageIdFromContext(
  context: Record<string, unknown>,
  path: string,
): string {
  if (!path.startsWith('$.')) {
    throw new Error(`imageRefPath must start with $.: got '${path}'`);
  }
  const parts = path.slice(2).split('.');
  let cursor: unknown = context;
  for (const part of parts) {
    if (cursor == null || typeof cursor !== 'object') {
      throw new Error(`imageRefPath ${path} dereferenced through a non-object at .${part}`);
    }
    cursor = (cursor as Record<string, unknown>)[part];
  }
  if (cursor == null) {
    throw new Error(`imageRefPath ${path} resolved to null/undefined`);
  }
  if (typeof cursor === 'string') return cursor;
  if (
    typeof cursor === 'object' &&
    typeof (cursor as Record<string, unknown>)['imageId'] === 'string'
  ) {
    return (cursor as Record<string, unknown>)['imageId'] as string;
  }
  throw new Error(`imageRefPath ${path} did not resolve to a string or {imageId} object`);
}

/**
 * Lightweight {{context.foo}} substitution. Mirrors the LLMNode
 * prompt-rendering convention.
 */
export function renderUserPrompt(
  template: string,
  context: Record<string, unknown>,
): string {
  return template.replace(/\{\{\s*context\.([\w.]+)\s*\}\}/g, (_match, path: string) => {
    try {
      const parts = path.split('.');
      let cursor: unknown = context;
      for (const part of parts) {
        if (cursor == null || typeof cursor !== 'object') return _match;
        cursor = (cursor as Record<string, unknown>)[part];
      }
      if (cursor == null) return _match;
      return typeof cursor === 'string' ? cursor : JSON.stringify(cursor);
    } catch {
      return _match;
    }
  });
}

/** Translate VisionClient failureReason to the event-catalog enum. */
function mapFailureReason(
  reason: 'malformed' | 'refusal' | 'aborted' | 'unknown' | undefined,
): VisionFailureReason {
  switch (reason) {
    case 'malformed': return 'malformed';
    case 'aborted':   return 'aborted';
    case 'refusal':   return 'refusal';
    case 'unknown':
    default:          return 'unknown';
  }
}
