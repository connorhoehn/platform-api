// platform-api/src/pipeline/judge/ — barrel.
//
// `llm:judge` step type (DC-21 customStepHandler). Anthropic SDK with
// tool-use for constrained structured output, prompt caching for repeat
// rubric calls.
//
// Wire by calling `createJudgeStepHandler({...})` and adding the result
// to the `customStepHandlers` Map passed to `PipelineModule` config.

export { createJudgeStepHandler } from './JudgeStepHandler';
export type {
  JudgeStepHandlerOpts,
  JudgmentRepositoryLike,
  JudgeBudgetGuardLike,
} from './JudgeStepHandler';

export { JudgeClient, DEFAULT_JUDGE_SYSTEM_PROMPT, extractJsonObject } from './JudgeClient';
export type { JudgeClientCallResult, JudgeClientOpts } from './JudgeClient';

export {
  aggregateScore,
  normalizeCriterionScore,
} from './aggregateScore';
export type { AggregationResult } from './aggregateScore';

export {
  computeJudgeCost,
  computeCacheHitRatio,
  getJudgePricing,
} from './computeJudgeCost';

export { JudgmentRepository } from './JudgmentRepository';
export type {
  StoredJudgment,
  ListJudgmentsOptions,
  ListJudgmentsResult,
  JudgmentRepositoryOptions,
} from './JudgmentRepository';

export { JudgeBudgetGuard } from './JudgeBudgetGuard';
export type { JudgeBudgetGuardOptions } from './JudgeBudgetGuard';

export type {
  AnthropicUsage,
  CriterionScore,
  JudgeInput,
  JudgeMode,
  JudgeScale,
  JudgmentAggregation,
  JudgmentCriterion,
  JudgmentResult,
  JudgmentRubric,
  JudgmentVerdict,
  LlmJudgeStepConfig,
  RawJudgmentToolOutput,
} from './types';
