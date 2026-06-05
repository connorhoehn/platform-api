// platform-api/src/pipeline/image/__tests__/computeVisionCost.test.ts
//
// Cost-math coverage for the vision pricing helpers. The functions are
// thin wrappers around llm-pricing but they drive the `costUsd` that
// lands on the analyzed event, in the DB row, and on the canvas footer
// — a silent miscalculation shows up as "$0.000" or an absurd number
// on every run.

import {
  computeVisionCost,
  computeVisionCacheHitRatio,
  getVisionPricing,
} from '../computeVisionCost';

describe('computeVisionCost', () => {
  it('returns a finite USD value for a known model + non-zero tokens', () => {
    const cost = computeVisionCost('claude-sonnet-4-6', {
      input_tokens: 1200,
      output_tokens: 200,
    });
    expect(Number.isFinite(cost)).toBe(true);
    expect(cost).toBeGreaterThan(0);
  });

  it('scales linearly with input tokens', () => {
    const lo = computeVisionCost('claude-sonnet-4-6', { input_tokens: 100, output_tokens: 0 });
    const hi = computeVisionCost('claude-sonnet-4-6', { input_tokens: 1000, output_tokens: 0 });
    expect(hi).toBeCloseTo(lo * 10, 6);
  });

  it('scales linearly with output tokens', () => {
    const lo = computeVisionCost('claude-sonnet-4-6', { input_tokens: 0, output_tokens: 100 });
    const hi = computeVisionCost('claude-sonnet-4-6', { input_tokens: 0, output_tokens: 1000 });
    expect(hi).toBeCloseTo(lo * 10, 6);
  });

  it('zero tokens → zero cost', () => {
    expect(computeVisionCost('claude-sonnet-4-6', { input_tokens: 0, output_tokens: 0 })).toBe(0);
  });

  it('treats unknown model as zero (no throw)', () => {
    const cost = computeVisionCost('made-up-model-xyz', {
      input_tokens: 100,
      output_tokens: 100,
    });
    // llm-pricing returns 0 for unknown models — verify we don't blow up.
    expect(Number.isFinite(cost)).toBe(true);
    expect(cost).toBeGreaterThanOrEqual(0);
  });

  it('handles undefined token fields by treating them as zero', () => {
    // Anthropic usage occasionally omits fields entirely on partial
    // responses. The cost helper should tolerate undefined.
    const cost = computeVisionCost('claude-sonnet-4-6', {
      input_tokens: undefined as unknown as number,
      output_tokens: undefined as unknown as number,
    });
    expect(cost).toBe(0);
  });
});

describe('computeVisionCacheHitRatio', () => {
  it('returns 0 when no tokens at all', () => {
    expect(computeVisionCacheHitRatio({ input_tokens: 0, output_tokens: 0 })).toBe(0);
  });

  it('returns 0 when no cache reads', () => {
    expect(
      computeVisionCacheHitRatio({ input_tokens: 500, output_tokens: 100 }),
    ).toBe(0);
  });

  it('returns 1 when every input token came from cache', () => {
    expect(
      computeVisionCacheHitRatio({
        input_tokens: 0,
        output_tokens: 100,
        cache_read_input_tokens: 1200,
      }),
    ).toBe(1);
  });

  it('computes a partial cache-hit ratio correctly', () => {
    // 800 cache-read out of 1000 total input tokens = 0.8
    expect(
      computeVisionCacheHitRatio({
        input_tokens: 200,
        output_tokens: 50,
        cache_read_input_tokens: 800,
      }),
    ).toBeCloseTo(0.8, 6);
  });

  it('counts cache_creation against the total but not as a hit', () => {
    // The denominator includes creation tokens (they ARE input cost),
    // the numerator counts ONLY reads. 100 read + 100 created + 100 new
    // → 100 / 300 ≈ 0.333.
    expect(
      computeVisionCacheHitRatio({
        input_tokens: 100,
        output_tokens: 0,
        cache_read_input_tokens: 100,
        cache_creation_input_tokens: 100,
      }),
    ).toBeCloseTo(1 / 3, 6);
  });
});

describe('getVisionPricing', () => {
  it('returns a pricing object for known models', () => {
    const p = getVisionPricing('claude-sonnet-4-6');
    expect(p).toBeDefined();
  });

  it('returns null/undefined (not throw) for unknown models', () => {
    // The underlying llm-pricing returns null for unknown — we forward.
    expect(() => getVisionPricing('totally-fake-model-xyz')).not.toThrow();
  });
});
