// platform-api/src/pipeline/runOverrides.ts
//
// Trigger-level overrides for a run's agent steps: the `model` and the
// `contextBudgetTokens` a caller chose for THIS run (a run draft's Model and
// Budget, docs/design/documents-detail/PLAN.md WS-F), on top of what each
// step's own config says.
//
// THE CONTRACT
//   - `triggerPayload.model` — must be a model the pricing table prices
//     (`isPricedModel`, the same list `GET /api/pipelines/:id/estimate`
//     accepts). Anything else is refused with 400 at the trigger (and at a run
//     draft's dispatch, before the draft moves), never silently dropped.
//   - `triggerPayload.contextBudgetTokens` — a positive integer. It is capped
//     at the platform maximum (`PIPELINE_MAX_CONTEXT_BUDGET_TOKENS`, default
//     2 000 000), and in each agent step at that step's own configured budget:
//     an override can lower a step's cap, never raise it.
//   - Absent → every step runs on its own config, exactly as before.
//
// `triggerPipelineRun` validates the payload and stamps the result into it as
// `runOverrides` (a caller-sent `runOverrides` is discarded, so it can only
// ever hold validated values). The run context IS the trigger payload, but
// step outputs merge into it — an agent step's output carries its own `model`
// — so steps read the stamped `runOverrides`, never the bare `model` key.

import { ValidationError } from '../middleware/error-handler';
import { isPricedModel, PRICED_MODEL_FAMILIES } from './llm-pricing';

export interface RunOverrides {
  /** The model every agent step of the run uses. */
  model?: string;
  /** The run-level budget after the platform cap. */
  contextBudgetTokens?: number;
  /** What the caller asked for, when the platform cap lowered it. */
  requestedContextBudgetTokens?: number;
}

const DEFAULT_MAX_CONTEXT_BUDGET_TOKENS = 2_000_000;

/** The platform ceiling on any run's context budget. */
export function maxContextBudgetTokens(env: NodeJS.ProcessEnv = process.env): number {
  const raw = Number(env.PIPELINE_MAX_CONTEXT_BUDGET_TOKENS);
  return Number.isInteger(raw) && raw > 0 ? raw : DEFAULT_MAX_CONTEXT_BUDGET_TOKENS;
}

/**
 * The validated overrides a trigger payload asks for, or undefined when it
 * asks for none. Throws ValidationError (→ 400) for an unpriced model or a
 * budget that is not a positive integer.
 */
export function resolveRunOverrides(payload: Record<string, unknown> | undefined, max = maxContextBudgetTokens()): RunOverrides | undefined {
  if (!payload) return undefined;
  const out: RunOverrides = {};
  const model = payload.model;
  if (model !== undefined && model !== null && model !== '') {
    if (typeof model !== 'string' || !isPricedModel(model)) {
      throw new ValidationError(`model ${JSON.stringify(model)} is not a priced model: use ${PRICED_MODEL_FAMILIES.join(', ')} or a priced model id`);
    }
    out.model = model.trim();
  }
  const budget = payload.contextBudgetTokens;
  if (budget !== undefined && budget !== null) {
    if (typeof budget !== 'number' || !Number.isInteger(budget) || budget <= 0) {
      throw new ValidationError('contextBudgetTokens must be a positive integer');
    }
    out.contextBudgetTokens = Math.min(budget, max);
    if (budget > max) out.requestedContextBudgetTokens = budget;
  }
  return out.model !== undefined || out.contextBudgetTokens !== undefined ? out : undefined;
}

/** The payload with `runOverrides` replaced by the validated overrides (or removed). */
export function stampRunOverrides(payload: Record<string, unknown>, max = maxContextBudgetTokens()): Record<string, unknown> {
  const overrides = resolveRunOverrides(payload, max);
  const { runOverrides: _discarded, ...rest } = payload;
  return overrides ? { ...rest, runOverrides: overrides } : rest;
}

/** The stamped overrides in a run context (or trigger payload), re-checked. */
export function runOverridesFrom(context: Record<string, unknown> | undefined): RunOverrides | undefined {
  const raw = context?.runOverrides;
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return undefined;
  const r = raw as Record<string, unknown>;
  const out: RunOverrides = {};
  if (typeof r.model === 'string' && isPricedModel(r.model)) out.model = r.model;
  if (typeof r.contextBudgetTokens === 'number' && Number.isInteger(r.contextBudgetTokens) && r.contextBudgetTokens > 0) {
    out.contextBudgetTokens = Math.min(r.contextBudgetTokens, maxContextBudgetTokens());
  }
  if (typeof r.requestedContextBudgetTokens === 'number' && Number.isFinite(r.requestedContextBudgetTokens)) {
    out.requestedContextBudgetTokens = r.requestedContextBudgetTokens;
  }
  return out.model !== undefined || out.contextBudgetTokens !== undefined ? out : undefined;
}

/**
 * What an agent step actually runs with. The override wins for the model; for
 * the budget it is the lower of the override and the step's own budget (the
 * override alone when the step sets none). No override → the step's config.
 */
export function effectiveAgentSettings(
  step: { model?: string; contextBudgetTokens?: number },
  context: Record<string, unknown>,
  defaultModel: string,
): { model: string; contextBudgetTokens?: number } {
  const overrides = runOverridesFrom(context);
  const stepBudget = Number.isFinite(step.contextBudgetTokens) && (step.contextBudgetTokens as number) > 0
    ? (step.contextBudgetTokens as number)
    : undefined;
  const model = overrides?.model ?? (step.model?.trim() || defaultModel);
  const override = overrides?.contextBudgetTokens;
  const contextBudgetTokens = override !== undefined
    ? (stepBudget !== undefined ? Math.min(stepBudget, override) : override)
    : stepBudget;
  return { model, ...(contextBudgetTokens !== undefined ? { contextBudgetTokens } : {}) };
}
