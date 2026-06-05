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

import { randomUUID } from 'crypto';
import type {
  PipelineNode,
  StepHandler,
  StepHandlerHelpers,
  StepOutcome,
} from 'distributed-core/applications/pipeline';
import { withContext } from '../../lib/logger';
import { VisionClient } from './VisionClient';

const log = withContext({ handler: 'VisionStepHandler' });
import { computeVisionCost } from './computeVisionCost';
import type {
  ImageBlobStore,
} from './ImageBlobStore';
import type {
  ImageMeta,
  ImageVisionAnalyzeStepConfig,
  VisionAnalyzeInput,
  VisionFailureReason,
  VisionResult,
} from './types';

export interface VisionResultRepositoryLike {
  persistVisionResult(result: VisionResult): Promise<void>;
}

export interface VisionStepHandlerOpts {
  visionClient: VisionClient;
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
 * Permissive emit cast for the `pipeline:image:vision:*` event family.
 *
 * The wire contract for these events is owned by event-catalog (see
 * `@connorhoehn/event-catalog/pipeline` — `pipelineImageVisionStarted`,
 * `pipelineImageVisionAnalyzed`, `pipelineImageVisionFailed`).
 * DC's `PipelineEventMap` is a TypeScript `type` alias (not an
 * interface) so it can't be augmented from here. The cast is purely a
 * TS-side bridge; the runtime EventBus accepts arbitrary string topics.
 * When DC next bumps its types to include these entries, the cast can
 * drop without any payload shape change. Same posture as judge.
 */
type LooseEmit = (event: string, payload: unknown) => Promise<void>;

export function createVisionStepHandler(opts: VisionStepHandlerOpts): StepHandler {
  const defaultModel = opts.defaultModel ?? DEFAULT_MODEL;

  return {
    kind: 'image:vision-analyze',
    async execute(
      node: PipelineNode,
      context: Record<string, unknown>,
      helpers: StepHandlerHelpers,
    ): Promise<StepOutcome> {
      const emit = helpers.emit as unknown as LooseEmit;
      const config = (node.data as unknown as ImageVisionAnalyzeStepConfig);
      const pipelineId = opts.pipelineIdLookup(helpers.runId) ?? 'unknown';
      const visionId = randomUUID();
      // Hoist once — reused for started, analyzed, and failed events so
      // we avoid repeated Date allocations on the hot path.
      const stepAt = new Date().toISOString();
      const model = config.model ?? defaultModel;
      const maxOutputTokens = config.maxOutputTokens ?? DEFAULT_MAX_OUTPUT_TOKENS;

      // ── 1. Resolve imageId from context ──────────────────────────────
      let imageId: string;
      try {
        imageId = resolveImageIdFromContext(context, config.imageRefPath);
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

      // ── 3. Render user prompt with context substitution ──────────────
      const userPrompt = renderUserPrompt(config.userPrompt, context);
      const systemPrompt = config.systemPrompt ?? '';

      // ── 4. Emit started ──────────────────────────────────────────────
      await emit('pipeline:image:vision:started', {
        runId: helpers.runId,
        stepId: node.id,
        pipelineId,
        imageId,
        visionModel: model,
        hasSchema: !!config.outputSchema,
        at: stepAt,
      });

      // ── 5. Call the vision client + stream tokens to the bus ─────────
      const input: VisionAnalyzeInput = {
        model,
        systemPrompt,
        userPrompt,
        imageBytes: imageRow.body,
        imageMimeType: imageRow.meta.mimeType,
        ...(config.outputSchema ? { outputSchema: config.outputSchema } : {}),
        maxOutputTokens,
        ...(helpers.abortSignal ? { abortSignal: helpers.abortSignal } : {}),
      };

      // Each token delta from the CLI fires an event. We fire-and-forget so
      // a slow subscriber can't backpressure the model loop; the in-process
      // EventBus dispatches inline. Tokens stream during the model's text
      // response phase (after the Read tool returns the image) — exactly
      // mirrors how the LLM node renders incremental output.
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

      // ── 6. Handle non-success paths (non-throwing per rule #1) ───────
      if (!callResult.output) {
        const costUsd = computeVisionCost(model, callResult.usage);
        // The CLI's stderr (e.g. "API Error: 401 Invalid authentication
        // credentials" when claude auth expires) lands in
        // callResult.rawText. Forward it on the failed event so the
        // frontend can surface the actual cause instead of opaque
        // "vision unknown". Cap at 400 chars to keep the event payload
        // bus-friendly + the UI footer readable.
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
        return {
          status: 'failed',
          error: `vision ${composedError}`,
        };
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
          // Persistence failure must not fail the step — the result is
          // already on the event bus and observers can re-derive.
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

      // Step output carries the full VisionResult so it lands in
      // run.stepsSummary and the runs panel can render via the existing
      // `<VisionResultViewer>` composite (already shipped in
      // ui-components, already wired via classifyStepOutput).
      return {
        status: 'completed',
        output: result,
        sourceHandle: 'out',
      };
    },
  };
}

// ─── Internal helpers ──────────────────────────────────────────────────

/**
 * Resolves the imageId from context via jsonpath. The value at the
 * path can be either a string (the imageId itself) or an object with
 * an `imageId` field. Throws when the path is unresolvable.
 *
 * Examples:
 *   '$.trigger.imageRef'                  → 'img_abc'
 *   '$.trigger.imageRef.imageId'          → 'img_abc'
 *   '$.steps.upload.output.imageId'       → 'img_abc'
 */
function resolveImageIdFromContext(context: Record<string, unknown>, path: string): string {
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
  if (typeof cursor === 'object' && typeof (cursor as Record<string, unknown>)['imageId'] === 'string') {
    return (cursor as Record<string, unknown>)['imageId'] as string;
  }
  throw new Error(`imageRefPath ${path} did not resolve to a string or {imageId} object`);
}

/**
 * Lightweight {{context.foo}} substitution in the user prompt. Mirrors
 * the existing LLMNode prompt-rendering convention so vision prompts
 * feel consistent with text LLM prompts in the editor.
 */
function renderUserPrompt(template: string, context: Record<string, unknown>): string {
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

/** Translate VisionClient's failureReason to the event-catalog enum. */
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
