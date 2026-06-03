import { describe, it, expect } from '@jest/globals';
import { computeJudgeCost, computeCacheHitRatio, getJudgePricing } from '../computeJudgeCost';

describe('computeJudgeCost — Sonnet 4.6', () => {
  it('charges $3/Mtok input + $15/Mtok output', () => {
    const cost = computeJudgeCost('claude-sonnet-4-6', { input_tokens: 1_000_000, output_tokens: 0 });
    expect(cost).toBeCloseTo(3, 4);
    const out = computeJudgeCost('claude-sonnet-4-6', { input_tokens: 0, output_tokens: 1_000_000 });
    expect(out).toBeCloseTo(15, 4);
  });

  it('charges 10% of input for cache reads', () => {
    const cost = computeJudgeCost('claude-sonnet-4-6', {
      input_tokens: 0,
      output_tokens: 0,
      cache_read_input_tokens: 1_000_000,
    });
    expect(cost).toBeCloseTo(0.3, 4);
  });

  it('charges 125% of input for cache writes', () => {
    const cost = computeJudgeCost('claude-sonnet-4-6', {
      input_tokens: 0,
      output_tokens: 0,
      cache_creation_input_tokens: 1_000_000,
    });
    expect(cost).toBeCloseTo(3.75, 4);
  });

  it('combines all four tiers correctly', () => {
    const cost = computeJudgeCost('claude-sonnet-4-6', {
      input_tokens: 1_000_000,         // $3
      output_tokens: 200_000,           // $3
      cache_read_input_tokens: 500_000, // $0.15
      cache_creation_input_tokens: 100_000, // $0.375
    });
    expect(cost).toBeCloseTo(3 + 3 + 0.15 + 0.375, 3);
  });
});

describe('computeJudgeCost — Opus / Haiku', () => {
  it('Opus 4.7 is 5× Sonnet on input', () => {
    expect(computeJudgeCost('claude-opus-4-7', { input_tokens: 1_000_000, output_tokens: 0 })).toBeCloseTo(15, 4);
  });
  it('Haiku 4.5 is ~3.75× cheaper than Sonnet on input', () => {
    expect(computeJudgeCost('claude-haiku-4-5', { input_tokens: 1_000_000, output_tokens: 0 })).toBeCloseTo(0.8, 4);
  });
  it('unknown model falls back to default (Sonnet-like)', () => {
    expect(getJudgePricing('totally-unknown-model').inputPerMTok).toBeGreaterThan(0);
  });
  it('prefix-matches model variants', () => {
    expect(getJudgePricing('claude-sonnet-4-6-20260101').inputPerMTok).toBe(3);
  });
});

describe('computeCacheHitRatio', () => {
  it('is 0 when no cache activity', () => {
    expect(computeCacheHitRatio({ input_tokens: 100, output_tokens: 50 })).toBe(0);
  });
  it('is the cache-read fraction of total input', () => {
    const r = computeCacheHitRatio({
      input_tokens: 200,
      output_tokens: 0,
      cache_read_input_tokens: 800,
      cache_creation_input_tokens: 0,
    });
    expect(r).toBeCloseTo(0.8, 4);
  });
});
