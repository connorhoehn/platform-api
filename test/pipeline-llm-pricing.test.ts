// test/pipeline-llm-pricing.test.ts
//
// Tests for getPricing and computeCost — pure functions with no external deps.

import { getPricing, computeCost } from '../src/pipeline/llm-pricing';

// ─── getPricing ───────────────────────────────────────────────────────────────

describe('getPricing — known models', () => {
    test('claude-3-haiku exact match', () => {
        const p = getPricing('claude-3-haiku');
        expect(p.inputPerMTok).toBe(0.25);
        expect(p.outputPerMTok).toBe(1.25);
    });

    test('claude-3-sonnet exact match', () => {
        const p = getPricing('claude-3-sonnet');
        expect(p.inputPerMTok).toBe(3);
        expect(p.outputPerMTok).toBe(15);
    });

    test('claude-sonnet-4-6 exact match', () => {
        const p = getPricing('claude-sonnet-4-6');
        expect(p.inputPerMTok).toBe(3);
        expect(p.outputPerMTok).toBe(15);
    });

    test('claude-3-opus exact match', () => {
        const p = getPricing('claude-3-opus');
        expect(p.inputPerMTok).toBe(15);
        expect(p.outputPerMTok).toBe(75);
    });
});

describe('getPricing — case insensitivity', () => {
    test('uppercase model name is normalized', () => {
        const p = getPricing('CLAUDE-3-HAIKU');
        expect(p.inputPerMTok).toBe(0.25);
    });

    test('mixed case model name is normalized', () => {
        const p = getPricing('Claude-3-Opus');
        expect(p.inputPerMTok).toBe(15);
    });
});

describe('getPricing — prefix matching', () => {
    test('versioned haiku model maps to haiku pricing', () => {
        const p = getPricing('claude-3-haiku-20240307');
        expect(p.inputPerMTok).toBe(0.25);
    });

    test('versioned sonnet model maps to sonnet pricing', () => {
        const p = getPricing('claude-3-sonnet-20240229');
        expect(p.inputPerMTok).toBe(3);
    });

    test('versioned opus model maps to opus pricing', () => {
        const p = getPricing('claude-3-opus-20240229');
        expect(p.inputPerMTok).toBe(15);
    });
});

describe('getPricing — unknown model fallback', () => {
    test('completely unknown model returns default pricing', () => {
        const p = getPricing('gpt-4');
        expect(p.inputPerMTok).toBe(1);
        expect(p.outputPerMTok).toBe(5);
    });

    test('empty string returns default pricing', () => {
        const p = getPricing('');
        expect(p.inputPerMTok).toBe(1);
        expect(p.outputPerMTok).toBe(5);
    });
});

// ─── computeCost ──────────────────────────────────────────────────────────────

describe('computeCost', () => {
    test('1M input + 0 output tokens for haiku', () => {
        const cost = computeCost('claude-3-haiku', 1_000_000, 0);
        expect(cost).toBeCloseTo(0.25);
    });

    test('0 input + 1M output tokens for haiku', () => {
        const cost = computeCost('claude-3-haiku', 0, 1_000_000);
        expect(cost).toBeCloseTo(1.25);
    });

    test('1M input + 1M output for opus', () => {
        const cost = computeCost('claude-3-opus', 1_000_000, 1_000_000);
        expect(cost).toBeCloseTo(90); // 15 + 75
    });

    test('zero tokens gives zero cost', () => {
        expect(computeCost('claude-3-sonnet', 0, 0)).toBe(0);
    });

    test('typical request: 500 input + 200 output tokens for sonnet', () => {
        const cost = computeCost('claude-3-sonnet', 500, 200);
        // (500/1M)*3 + (200/1M)*15 = 0.0015 + 0.003 = 0.0045
        expect(cost).toBeCloseTo(0.0045, 6);
    });

    test('unknown model uses default pricing', () => {
        const cost = computeCost('unknown-model', 1_000_000, 1_000_000);
        expect(cost).toBeCloseTo(6); // 1 + 5
    });
});
