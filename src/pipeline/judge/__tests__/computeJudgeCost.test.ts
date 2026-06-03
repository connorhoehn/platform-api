import { describe, it, expect } from '@jest/globals';
import { computeJudgeCost, computeCacheHitRatio, getJudgePricing } from '../computeJudgeCost';

// Cost is delegated to llm-pricing.ts via simple input/output rates.
// The CLI doesn't surface cache tiers today so this module is thin.

describe('computeJudgeCost — basic tier mapping', () => {
  it('matches Sonnet 4.6 input/output rates', () => {
    expect(computeJudgeCost('claude-sonnet-4-6', { input_tokens: 1_000_000, output_tokens: 0 })).toBeCloseTo(3, 4);
    expect(computeJudgeCost('claude-sonnet-4-6', { input_tokens: 0, output_tokens: 1_000_000 })).toBeCloseTo(15, 4);
  });

  it('combines input and output', () => {
    // 1M input ($3) + 200K output ($3) = $6
    expect(computeJudgeCost('claude-sonnet-4-6', { input_tokens: 1_000_000, output_tokens: 200_000 })).toBeCloseTo(6, 4);
  });

  it('unknown model falls back to default pricing', () => {
    expect(getJudgePricing('totally-unknown').inputPerMTok).toBeGreaterThan(0);
  });
});

describe('computeCacheHitRatio', () => {
  it('is 0 when CLI does not report cache tokens (always for now)', () => {
    expect(computeCacheHitRatio({ input_tokens: 100, output_tokens: 50 })).toBe(0);
  });

  it('forward-compat: if cache fields appear later, ratio is computed', () => {
    // The CLI doesn't emit these today but the function stays type-stable.
    const r = computeCacheHitRatio({
      input_tokens: 200,
      output_tokens: 0,
      cache_read_input_tokens: 800,
      cache_creation_input_tokens: 0,
    });
    expect(r).toBeCloseTo(0.8, 4);
  });
});
