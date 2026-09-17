// social-api/src/pipeline/__tests__/llm-pricing.test.ts

import { describe, it, expect } from '@jest/globals';
import { getPricing, computeCost } from '../llm-pricing';

describe('getPricing()', () => {
  it('returns known pricing for exact model names', () => {
    expect(getPricing('claude-3-haiku').inputPerMTok).toBe(0.25);
    expect(getPricing('claude-3-sonnet').inputPerMTok).toBe(3);
    expect(getPricing('claude-3-opus').inputPerMTok).toBe(15);
  });

  it('is case-insensitive', () => {
    expect(getPricing('Claude-3-Haiku').inputPerMTok).toBe(0.25);
    expect(getPricing('CLAUDE-3-OPUS').inputPerMTok).toBe(15);
  });

  it('matches by prefix (versioned model strings)', () => {
    // e.g. "claude-3-haiku-20240307" should match "claude-3-haiku"
    expect(getPricing('claude-3-haiku-20240307').inputPerMTok).toBe(0.25);
    expect(getPricing('claude-3-sonnet-20240229').inputPerMTok).toBe(3);
  });

  it('falls back to default pricing for unknown models', () => {
    const pricing = getPricing('gpt-4o');
    expect(pricing.inputPerMTok).toBe(1);
    expect(pricing.outputPerMTok).toBe(5);
  });
});

describe('computeCost()', () => {
  it('computes cost for known model correctly', () => {
    // claude-3-haiku: $0.25/MTok in, $1.25/MTok out
    // 1M in + 1M out → $0.25 + $1.25 = $1.50
    expect(computeCost('claude-3-haiku', 1_000_000, 1_000_000)).toBeCloseTo(1.5, 6);
  });

  it('computes zero cost for zero tokens', () => {
    expect(computeCost('claude-3-opus', 0, 0)).toBe(0);
  });

  it('uses default pricing for unknown model', () => {
    // $1/MTok in, $5/MTok out → 500k in + 200k out = $0.5 + $1.0 = $1.5
    expect(computeCost('unknown-model', 500_000, 200_000)).toBeCloseTo(1.5, 6);
  });

  it('handles fractional token counts (sub-million)', () => {
    // claude-3-haiku, 100 tokens in + 50 out → tiny cost
    const cost = computeCost('claude-3-haiku', 100, 50);
    expect(cost).toBeGreaterThan(0);
    expect(cost).toBeLessThan(0.001);
  });
});

describe('getPricing() — model families', () => {
  it('prices a bare family name, which is what an agent step reports', () => {
    // The miss that left every agent step on the unknown-model default:
    // `haiku` does not START with `claude-3-haiku`, so prefix matching failed
    // and the step was priced at $1/$5 regardless of which model ran.
    expect(getPricing('haiku')).toEqual(getPricing('claude-3-haiku'));
    expect(getPricing('sonnet')).toEqual(getPricing('claude-3-sonnet'));
    expect(getPricing('opus')).toEqual(getPricing('claude-3-opus'));
  });

  it('prices a Claude 5 id as its tier rather than as unknown', () => {
    expect(getPricing('claude-opus-5').inputPerMTok).toBe(15);
    expect(getPricing('claude-sonnet-5').inputPerMTok).toBe(3);
    expect(getPricing('claude-haiku-4-5').inputPerMTok).toBe(0.25);
  });

  it('prices a Bedrock inference profile as its tier', () => {
    expect(getPricing('us.anthropic.claude-opus-4-8').inputPerMTok).toBe(15);
  });

  it('is case-insensitive about the family too', () => {
    expect(getPricing('HAIKU')).toEqual(getPricing('haiku'));
  });

  it('still falls back for a model from no known family', () => {
    expect(getPricing('some-inhouse-model').inputPerMTok).toBe(1);
  });
});
