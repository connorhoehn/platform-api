// platform-api/src/pipeline/judge/computeJudgeCost.ts
//
// Cache-tier-aware cost computation for judge calls. Extends the simple
// input/output table in `llm-pricing.ts` with the two extra tiers
// Anthropic prompt caching introduces:
//
//   - cache_read_input_tokens   ← typically 10% of input price
//   - cache_creation_input_tokens ← typically 125% of input price
//
// Hardcoded per Anthropic's published pricing (per Mtok), 2026-06:
//   sonnet-4-6:  $3 / $0.30 / $3.75 / $15
//   opus-4-7:    $15 / $1.50 / $18.75 / $75
//   haiku-4-5:   $0.80 / $0.08 / $1.00 / $4
//
// Centralized here so the executor-audit subscriber, the budget guard,
// and the JudgmentRepository all compute cost identically.

import type { AnthropicUsage } from './types';

export interface CacheTierPricing {
  /** USD per million NEW input tokens (uncached). */
  inputPerMTok: number;
  /** USD per million CACHE READ tokens. ~10% of input. */
  cacheReadPerMTok: number;
  /** USD per million CACHE WRITE tokens (creating the cache entry). ~125% of input. */
  cacheWritePerMTok: number;
  /** USD per million output tokens. */
  outputPerMTok: number;
}

const JUDGE_PRICING: Record<string, CacheTierPricing> = {
  'claude-sonnet-4-6': {
    inputPerMTok: 3,
    cacheReadPerMTok: 0.3,
    cacheWritePerMTok: 3.75,
    outputPerMTok: 15,
  },
  'claude-opus-4-7': {
    inputPerMTok: 15,
    cacheReadPerMTok: 1.5,
    cacheWritePerMTok: 18.75,
    outputPerMTok: 75,
  },
  'claude-haiku-4-5': {
    inputPerMTok: 0.8,
    cacheReadPerMTok: 0.08,
    cacheWritePerMTok: 1.0,
    outputPerMTok: 4,
  },
};

const DEFAULT_PRICING: CacheTierPricing = {
  inputPerMTok: 3,
  cacheReadPerMTok: 0.3,
  cacheWritePerMTok: 3.75,
  outputPerMTok: 15,
};

export function getJudgePricing(model: string): CacheTierPricing {
  const lower = model.toLowerCase();
  if (JUDGE_PRICING[lower]) return JUDGE_PRICING[lower];
  for (const [key, pricing] of Object.entries(JUDGE_PRICING)) {
    if (lower.startsWith(key)) return pricing;
  }
  return DEFAULT_PRICING;
}

/**
 * Compute total USD cost from an Anthropic usage block. Treats
 * `input_tokens` as NEW (uncached) input — that's how the SDK reports
 * usage when cache_creation/cache_read are also present.
 */
export function computeJudgeCost(model: string, usage: AnthropicUsage): number {
  const p = getJudgePricing(model);
  const newInput = usage.input_tokens ?? 0;
  const cacheRead = usage.cache_read_input_tokens ?? 0;
  const cacheCreate = usage.cache_creation_input_tokens ?? 0;
  const output = usage.output_tokens ?? 0;
  return (
    (newInput / 1_000_000) * p.inputPerMTok +
    (cacheRead / 1_000_000) * p.cacheReadPerMTok +
    (cacheCreate / 1_000_000) * p.cacheWritePerMTok +
    (output / 1_000_000) * p.outputPerMTok
  );
}

/**
 * Cache-hit ratio for observability: what fraction of total input
 * tokens came from cache. 0 when no cache activity, 1 when the entire
 * prompt prefix was served from cache.
 */
export function computeCacheHitRatio(usage: AnthropicUsage): number {
  const newInput = usage.input_tokens ?? 0;
  const cacheRead = usage.cache_read_input_tokens ?? 0;
  const cacheCreate = usage.cache_creation_input_tokens ?? 0;
  const total = newInput + cacheRead + cacheCreate;
  return total === 0 ? 0 : cacheRead / total;
}
