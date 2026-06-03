// platform-api/src/pipeline/judge/types.ts
//
// Single source of truth for the LLM-as-judge data model. Shared by:
//   - JudgeClient (Anthropic SDK call shape, tool input_schema)
//   - JudgeStepHandler (executes the step, emits events)
//   - JudgmentRepository (DDB persistence)
//   - aggregateScore (pure function: criteria[] → overall)
//   - ui-components/pipelines/judge (renders JudgmentResult)
//
// Where the types overlap with the executor's `StepHandler` /
// `StepOutcome` shape, we import those from distributed-core directly;
// don't re-declare them.

export type JudgeScale = 'binary' | '1-5' | '1-10' | 'pass-fail';
export type JudgmentAggregation = 'weighted-mean' | 'min' | 'all-pass';
export type JudgeMode = 'absolute' | 'comparison';
export type JudgmentVerdict = 'pass' | 'fail' | 'inconclusive';

/**
 * A single rubric criterion the judge model scores. `id` is stable across
 * rubric versions so per-criterion failure-rate analytics can roll up.
 */
export interface JudgmentCriterion {
  id: string;
  prompt: string;
  scale: JudgeScale;
  /**
   * Weighting for `weighted-mean` aggregation. Ignored for `min` /
   * `all-pass`. Defaults to 1.0 if omitted.
   */
  weight?: number;
  /**
   * Optional pass gate at the criterion level — `criterionScore.passed` is
   * derived from this when present. Falls back to >= 0.5 on the normalized
   * 0..1 score when omitted.
   */
  failThreshold?: number;
  /** Optional anchor labels for `1-5` / `1-10` scales (e.g. `['terrible','poor','ok','good','excellent']`). */
  anchors?: string[];
}

export interface JudgmentRubric {
  id: string;
  version: number;
  criteria: JudgmentCriterion[];
  /**
   * How per-criterion scores roll up to `overallScore`:
   *   - `weighted-mean`: sum(score*weight) / sum(weight)
   *   - `min`: min(score)
   *   - `all-pass`: 1 if every criterion passed, 0 otherwise
   *
   * Computed server-side from the model's per-criterion scores. NEVER
   * asked of the model — that would collapse auditability.
   */
  overall: JudgmentAggregation;
  /**
   * Rubric-level pass gate on `overallScore`. Step config can override
   * per-step (see `LlmJudgeStepConfig.passThresholdOverride`).
   */
  passThreshold?: number;
}

export interface LlmJudgeStepConfig {
  /** Discriminator that the executor's customStepHandlers map keys on. */
  type: 'llm:judge';
  /** Anthropic model id. Falls back to pipeline default, then to global default. */
  model?: string;
  /** Optional override of the canned judge system prompt. */
  systemPrompt?: string;
  /** Rubric, inline. v2 will swap to `{ rubricRef }` lookup. */
  rubric: JudgmentRubric;
  mode: JudgeMode;
  /**
   * JSONPath-shaped declarative mapping from run context to the artifact
   * being judged. `candidate` is the primary; `reference` is required for
   * `mode: 'comparison'`. `contextFields` becomes substitutable template
   * vars in the user-prompt.
   */
  input: {
    candidate: string;
    reference?: string;
    contextFields?: Record<string, string>;
  };
  /** Maximum tokens the judge call may emit. Default: 2048. */
  maxOutputTokens?: number;
  /** Per-step pass threshold (overrides rubric.passThreshold). */
  passThresholdOverride?: number;
}

export interface CriterionScore {
  criterionId: string;
  /** Normalized 0..1 for aggregation. */
  score: number;
  /** Verbatim score from the model (might be 1-5, "pass", etc. — pre-normalization). */
  rawScore: number | string;
  rationale: string;
  /** Derived from `criterion.failThreshold` (or default 0.5) and `score`. */
  passed: boolean;
}

/**
 * The structured judgment payload. Carries everything a downstream
 * consumer needs to render, audit, or roll up.
 */
export interface JudgmentResult {
  judgmentId: string;
  runId: string;
  stepId: string;
  pipelineId: string;
  rubricId: string;
  rubricVersion: number;
  /** Pinned rubric snapshot — historical record never reinterprets when rubric edits later. */
  rubricSnapshot: JudgmentRubric;
  mode: JudgeMode;
  overallScore: number;
  overallVerdict: JudgmentVerdict;
  overallPassed: boolean;
  /** Optional model self-reported confidence. */
  confidence?: number;
  criteria: CriterionScore[];
  /** Verbatim model response — useful for audit + re-parse on schema bumps. */
  rawModelText?: string;
  model: string;
  tokensIn: number;
  tokensOut: number;
  /** Anthropic prompt-caching tokens (when cache hit). */
  cacheReadTokens?: number;
  cacheCreationTokens?: number;
  latencyMs: number;
  costUsd: number;
  /** True when the rubric+system+tool prefix was served from cache. */
  cacheHit: boolean;
  /** ISO-8601 wall-clock for the judge call. */
  judgedAt: string;
}

/**
 * Input shape consumed by `JudgeClient.judge()`. Resolved by the
 * `JudgeStepHandler` from the node's `LlmJudgeStepConfig` + run context.
 */
export interface JudgeInput {
  model: string;
  systemPrompt: string;
  rubricPrompt: string;
  candidateText: string;
  /** Optional in `absolute` mode; required in `comparison` mode (v2). */
  referenceText?: string;
  maxOutputTokens: number;
  /** Cancellation hook from the executor; passed into Anthropic SDK. */
  abortSignal?: AbortSignal;
}

/**
 * Anthropic SDK usage object — used by cost-tracking. Subset of the
 * SDK's `Usage` type so we don't drag the SDK into shared type-graphs.
 */
export interface AnthropicUsage {
  input_tokens: number;
  output_tokens: number;
  cache_creation_input_tokens?: number;
  cache_read_input_tokens?: number;
}

/**
 * The raw tool-use payload the judge model produces. Always validated +
 * normalized before becoming a `JudgmentResult`.
 */
export interface RawJudgmentToolOutput {
  /** Per-criterion scores. The model uses criterion.id, NEVER prompt text. */
  criteria: Array<{ criterionId: string; score: number; rationale: string }>;
  /** Optional model-side overall narrative; not used in aggregation. */
  overallNotes?: string;
  /** Optional self-reported confidence 0..1. */
  confidence?: number;
}
