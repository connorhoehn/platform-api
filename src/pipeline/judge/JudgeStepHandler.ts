// platform-api/src/pipeline/judge/JudgeStepHandler.ts
//
// Custom step handler (DC-21) that dispatches `node.data.type === 'llm:judge'`.
// Lifecycle per call:
//   1. Resolve candidate/reference inputs from context via jsonpath.
//   2. Render the rubric prompt from criteria + scales.
//   3. Call JudgeClient.judge() with abortSignal threaded through.
//   4. Normalize per-criterion scores into 0..1.
//   5. Aggregate to overallScore via aggregateScore().
//   6. Persist JudgmentResult.
//   7. Emit pipeline:judge:scored on the executor's EventBus.
//   8. Return StepOutcome to the executor.
//
// Failure mode: per audit rule #1, judge failures are non-throwing. A
// missing API key, malformed model output, model refusal, or aborted
// fetch surface as `pipeline:judge:failed` events + the step returns
// `{status:'failed', error: ...}` with `usage` populated so cost
// accounting stays honest.

import { randomUUID } from 'crypto';
import type {
  PipelineNode,
  StepHandler,
  StepHandlerHelpers,
  StepOutcome,
} from 'distributed-core/applications/pipeline';
import { JudgeClient, DEFAULT_JUDGE_SYSTEM_PROMPT } from './JudgeClient';
import { aggregateScore, normalizeCriterionScore } from './aggregateScore';
import { computeJudgeCost, computeCacheHitRatio } from './computeJudgeCost';
import type {
  CriterionScore,
  JudgeInput,
  JudgmentResult,
  LlmJudgeStepConfig,
} from './types';

export interface JudgmentRepositoryLike {
  persistJudgment(result: JudgmentResult): Promise<void>;
}

export interface JudgeBudgetGuardLike {
  /** True when the daily budget is exhausted; the handler skips the call. */
  isExhausted(pipelineId: string, modelCostUsdEstimate: number): Promise<boolean>;
  /** Called after a successful call to debit the running total. */
  record(pipelineId: string, actualCostUsd: number): Promise<void>;
}

export interface JudgeStepHandlerOpts {
  judgeClient: JudgeClient;
  repository?: JudgmentRepositoryLike;
  budgetGuard?: JudgeBudgetGuardLike;
  defaultModel?: string;
  /**
   * Resolved at handler-construction time. The executor passes `runId` +
   * `stepId` per call via the helpers; we still need `pipelineId` to
   * route persistence / budget — passed via this lookup.
   */
  pipelineIdLookup: (runId: string) => string | undefined;
}

const DEFAULT_MODEL = 'claude-sonnet-4-6';

/**
 * Permissive emit cast for the `pipeline:judge:*` event family. DC's
 * `PipelineEventMap` doesn't yet declare these (they will land in
 * event-catalog in wave 3); until then, we publish through the EventBus
 * with a string event name. The DC EventBus accepts arbitrary string
 * topics at runtime — only the TS surface is strict.
 */
type LooseEmit = (event: string, payload: unknown) => Promise<void>;

export function createJudgeStepHandler(opts: JudgeStepHandlerOpts): StepHandler {
  const defaultModel = opts.defaultModel ?? DEFAULT_MODEL;

  return {
    kind: 'llm:judge',
    async execute(
      node: PipelineNode,
      context: Record<string, unknown>,
      helpers: StepHandlerHelpers,
    ): Promise<StepOutcome> {
      const emit = helpers.emit as unknown as LooseEmit;
      const config = (node.data as unknown as LlmJudgeStepConfig);
      const pipelineId = opts.pipelineIdLookup(helpers.runId) ?? 'unknown';
      const judgmentId = randomUUID();
      const judgedAt = new Date().toISOString();
      const model = config.model ?? defaultModel;
      const maxOutputTokens = config.maxOutputTokens ?? 2048;

      // ── 1. Resolve inputs ────────────────────────────────────────────
      let candidateText: string;
      let referenceText: string | undefined;
      try {
        candidateText = resolveJsonPath(context, config.input.candidate);
        if (config.input.reference) {
          referenceText = resolveJsonPath(context, config.input.reference);
        }
      } catch (err) {
        await emit('pipeline:judge:failed', {
          runId: helpers.runId,
          stepId: node.id,
          reason: 'input-resolution',
          error: (err as Error).message,
          at: judgedAt,
        });
        return { status: 'failed', error: `judge input resolution: ${(err as Error).message}` };
      }

      // ── 2. Budget check (cheap estimate before paying for the call) ──
      if (opts.budgetGuard) {
        const estCost = estimateCallCost(model, candidateText, referenceText);
        if (await opts.budgetGuard.isExhausted(pipelineId, estCost)) {
          await emit('pipeline:judge:budget-exceeded', {
            pipelineId,
            runId: helpers.runId,
            stepId: node.id,
            at: judgedAt,
        });
          // Skip, not fail — judge is signal, not a correctness gate
          return { status: 'completed', output: { skipped: 'budget-exhausted' }, sourceHandle: 'out' };
        }
      }

      // ── 3. Build rubric prompt + emit started ────────────────────────
      const rubricPrompt = renderRubricPrompt(config);
      const systemPrompt = config.systemPrompt ?? DEFAULT_JUDGE_SYSTEM_PROMPT;

      await emit('pipeline:judge:started', {
        runId: helpers.runId,
        stepId: node.id,
        pipelineId,
        rubricId: config.rubric.id,
        rubricVersion: config.rubric.version,
        judgeModel: model,
        mode: config.mode,
        at: judgedAt,
        });

      // ── 4. Call the judge ────────────────────────────────────────────
      const input: JudgeInput = {
        model,
        systemPrompt,
        rubricPrompt,
        candidateText,
        referenceText,
        maxOutputTokens,
        abortSignal: helpers.abortSignal,
      };
      const callResult = await opts.judgeClient.judge(input);

      // ── 5. Handle non-success paths (non-throwing per rule #1) ───────
      if (!callResult.output) {
        const costUsd = computeJudgeCost(model, callResult.usage);
        if (opts.budgetGuard && costUsd > 0) {
          await opts.budgetGuard.record(pipelineId, costUsd);
        }
        await emit('pipeline:judge:failed', {
          runId: helpers.runId,
          stepId: node.id,
          reason: callResult.failureReason ?? 'unknown',
          error: callResult.failureReason ?? 'judge call produced no tool output',
          at: judgedAt,
          tokensIn: callResult.usage.input_tokens,
          tokensOut: callResult.usage.output_tokens,
          costUsd,
          latencyMs: callResult.latencyMs,
        });
        return {
          status: 'failed',
          error: `judge ${callResult.failureReason ?? 'failed'}`,
        };
      }

      // ── 6. Normalize criterion scores + aggregate ────────────────────
      const criteriaScores: CriterionScore[] = [];
      for (const rawCrit of callResult.output.criteria) {
        const def = config.rubric.criteria.find((c) => c.id === rawCrit.criterionId);
        if (!def) {
          // Model invented a criterionId not in the rubric — skip with a warning
          continue;
        }
        const normalized = normalizeCriterionScore(rawCrit.score, def.scale);
        if (Number.isNaN(normalized)) continue;
        const failThreshold = def.failThreshold ?? 0.5;
        criteriaScores.push({
          criterionId: rawCrit.criterionId,
          score: normalized,
          rawScore: rawCrit.score,
          rationale: rawCrit.rationale,
          passed: normalized >= failThreshold,
        });
      }

      const { overallScore, overallVerdict, overallPassed } = aggregateScore(
        config.rubric,
        criteriaScores,
        config.passThresholdOverride,
      );

      // ── 7. Compose final JudgmentResult + persist ────────────────────
      const costUsd = computeJudgeCost(model, callResult.usage);
      const result: JudgmentResult = {
        judgmentId,
        runId: helpers.runId,
        stepId: node.id,
        pipelineId,
        rubricId: config.rubric.id,
        rubricVersion: config.rubric.version,
        rubricSnapshot: config.rubric,
        mode: config.mode,
        overallScore,
        overallVerdict,
        overallPassed,
        confidence: callResult.output.confidence,
        criteria: criteriaScores,
        rawModelText: callResult.rawText,
        model,
        tokensIn: callResult.usage.input_tokens,
        tokensOut: callResult.usage.output_tokens,
        cacheReadTokens: callResult.usage.cache_read_input_tokens,
        cacheCreationTokens: callResult.usage.cache_creation_input_tokens,
        latencyMs: callResult.latencyMs,
        costUsd,
        cacheHit: callResult.cacheHit,
        judgedAt,
      };

      if (opts.repository) {
        try {
          await opts.repository.persistJudgment(result);
        } catch (err) {
          // Persistence failure must not fail the step — the score is
          // already on the event bus and observers can re-derive.
          console.warn('JudgmentRepository.persistJudgment failed', err);
        }
      }
      if (opts.budgetGuard) {
        await opts.budgetGuard.record(pipelineId, costUsd);
      }

      const cacheHitRatio = computeCacheHitRatio(callResult.usage);

      // ── 8. Emit scored event ─────────────────────────────────────────
      await emit('pipeline:judge:scored', {
        runId: helpers.runId,
        stepId: node.id,
        pipelineId,
        judgmentId,
        rubricId: config.rubric.id,
        judgeModel: model,
        result,
        usage: {
          inputTokens: callResult.usage.input_tokens,
          outputTokens: callResult.usage.output_tokens,
          cacheReadTokens: callResult.usage.cache_read_input_tokens ?? 0,
          cacheCreationTokens: callResult.usage.cache_creation_input_tokens ?? 0,
        },
        costUsd,
        cacheHitRatio,
        latencyMs: callResult.latencyMs,
        at: judgedAt,
        });

      // Step output carries the full JudgmentResult so it lands in
      // run.stepsSummary and the replay path can render via the
      // `<JudgmentResultViewer result={step.output as JudgmentResult} />`
      // composite. Aggregate score also surfaces on context for
      // downstream conditional nodes.
      return {
        status: 'completed',
        output: result,
        sourceHandle: overallPassed ? 'out' : 'fail',
      };
    },
  };
}

// ─── Internal helpers ──────────────────────────────────────────────────

/**
 * Lightweight jsonpath resolver. Supports the subset we actually use:
 *   $.steps.<stepId>.output.response
 *   $.triggerPayload.candidate
 *   $.steps.qwen.output.caption
 *
 * Returns the resolved value as a string (JSON-stringified when the
 * target is an object). Throws when a path can't be walked.
 */
function resolveJsonPath(context: Record<string, unknown>, path: string): string {
  if (!path.startsWith('$.')) {
    throw new Error(`jsonpath must start with $.: got '${path}'`);
  }
  const parts = path.slice(2).split('.');
  let cursor: unknown = context;
  for (const part of parts) {
    if (cursor == null || typeof cursor !== 'object') {
      throw new Error(`jsonpath ${path} dereferenced through a non-object at .${part}`);
    }
    cursor = (cursor as Record<string, unknown>)[part];
  }
  if (cursor == null) {
    throw new Error(`jsonpath ${path} resolved to null/undefined`);
  }
  if (typeof cursor === 'string') return cursor;
  return JSON.stringify(cursor);
}

/**
 * Build the rubric prompt the judge sees. Per criterion: id, prompt,
 * scale, anchors. Kept terse — the model already knows what each scale
 * means and we don't want to balloon cached tokens.
 */
function renderRubricPrompt(config: LlmJudgeStepConfig): string {
  const lines: string[] = ['Rubric:'];
  for (const c of config.rubric.criteria) {
    lines.push(`- ${c.id} [${c.scale}]: ${c.prompt}`);
    if (c.anchors && c.anchors.length > 0) {
      lines.push(`  anchors: ${c.anchors.join(' | ')}`);
    }
  }
  lines.push('');
  lines.push(`Mode: ${config.mode}`);
  lines.push('Score every criterion using the rubric id exactly as given above.');
  return lines.join('\n');
}

/**
 * Pre-call cost estimate for budget gating. Rough — uses 4 chars/token
 * heuristic. The actual debit (`budgetGuard.record`) happens with real
 * usage after the call.
 */
function estimateCallCost(
  model: string,
  candidateText: string,
  referenceText: string | undefined,
): number {
  const charCount = candidateText.length + (referenceText?.length ?? 0);
  const approxInputTokens = Math.ceil(charCount / 4);
  // Assume 500 output tokens, no cache hit for the estimate (conservative)
  return computeJudgeCost(model, {
    input_tokens: approxInputTokens,
    output_tokens: 500,
  });
}
