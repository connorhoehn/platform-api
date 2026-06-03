// platform-api/src/pipeline/judge/computeJudgeCost.ts
//
// Cost computation for judge calls dispatched through the Claude Code
// CLI. The CLI's stream-json `result` event reports input_tokens +
// output_tokens — no cache tiers are surfaced via `claude -p` today, so
// we map directly onto the simple input/output pricing in `llm-pricing.ts`.
//
// If/when the CLI grows cache-tier reporting, this module gets the cache
// fields back (mirror the Anthropic SDK's `cache_read_input_tokens` +
// `cache_creation_input_tokens` keys to stay forward-compatible with
// `types.AnthropicUsage`).
//
// Centralized here so the executor-audit subscriber, the budget guard,
// and the JudgmentRepository all compute cost identically.

import { computeCost as computeLlmCost, getPricing } from '../llm-pricing';
import type { AnthropicUsage } from './types';

/** Compute total USD cost from a CLI usage block. */
export function computeJudgeCost(model: string, usage: AnthropicUsage): number {
  const input = usage.input_tokens ?? 0;
  const output = usage.output_tokens ?? 0;
  return computeLlmCost(model, input, output);
}

/**
 * Cache-hit ratio for observability. Always 0 today (CLI doesn't surface
 * cache tiers). Kept as an exported function so call sites stay stable
 * if/when the CLI grows cache reporting.
 */
export function computeCacheHitRatio(usage: AnthropicUsage): number {
  const cacheRead = usage.cache_read_input_tokens ?? 0;
  const cacheCreate = usage.cache_creation_input_tokens ?? 0;
  const newInput = usage.input_tokens ?? 0;
  const total = newInput + cacheRead + cacheCreate;
  return total === 0 ? 0 : cacheRead / total;
}

/** Backwards-compat accessor — pricing lookup delegates to `llm-pricing`. */
export function getJudgePricing(model: string) {
  return getPricing(model);
}
