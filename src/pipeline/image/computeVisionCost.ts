// platform-api/src/pipeline/image/computeVisionCost.ts
//
// Cost computation for vision calls dispatched through the Anthropic
// Messages API (direct fetch — see VisionClient.ts). The HTTP response
// reports input_tokens + output_tokens directly in the usage block,
// plus optional cache_read_input_tokens + cache_creation_input_tokens.
//
// Vision token counts include the image content; Anthropic's documented
// formula is roughly `width * height / 750` tokens per image. We don't
// recompute that here — we trust the input_tokens value from the API
// response (which already includes image tokens) and forward it as-is
// to the simple pricing in `llm-pricing.ts`. The `image_tokens` field
// in AnthropicVisionUsage is for observability only — it's a synthetic
// breakdown when the client wants to surface "of N input tokens, M
// came from the image" in the UI, computed downstream.
//
// Code parity with platform-api/src/pipeline/judge/computeJudgeCost.ts.

import { computeCost as computeLlmCost, getPricing } from '../llm-pricing';
import type { AnthropicVisionUsage } from './types';

/** Compute total USD cost from an Anthropic Messages API usage block. */
export function computeVisionCost(model: string, usage: AnthropicVisionUsage): number {
  const input = usage.input_tokens ?? 0;
  const output = usage.output_tokens ?? 0;
  return computeLlmCost(model, input, output);
}

/**
 * Cache-hit ratio for observability. Computed from cache_read_input_tokens
 * vs total input. Same formula as judge — kept here so call sites can
 * compute it without importing from the judge module.
 */
export function computeVisionCacheHitRatio(usage: AnthropicVisionUsage): number {
  const cacheRead = usage.cache_read_input_tokens ?? 0;
  const cacheCreate = usage.cache_creation_input_tokens ?? 0;
  const newInput = usage.input_tokens ?? 0;
  const total = newInput + cacheRead + cacheCreate;
  return total === 0 ? 0 : cacheRead / total;
}

/** Backwards-compat accessor — pricing lookup delegates to `llm-pricing`. */
export function getVisionPricing(model: string) {
  return getPricing(model);
}
