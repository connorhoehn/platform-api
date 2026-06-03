import { describe, it, expect } from '@jest/globals';
import { aggregateScore, normalizeCriterionScore } from '../aggregateScore';
import type { CriterionScore, JudgmentRubric } from '../types';

function crit(id: string, score: number, passed = score >= 0.5): CriterionScore {
  return { criterionId: id, score, rawScore: score, rationale: '', passed };
}

const rubricBase: JudgmentRubric = {
  id: 'r-1',
  version: 1,
  criteria: [
    { id: 'a', prompt: '', scale: '1-5', weight: 2 },
    { id: 'b', prompt: '', scale: '1-5', weight: 1 },
    { id: 'c', prompt: '', scale: '1-5', weight: 1 },
  ],
  overall: 'weighted-mean',
  passThreshold: 0.7,
};

describe('aggregateScore — weighted-mean', () => {
  it('weights criterion scores by rubric weights', () => {
    // a (w2) = 1.0, b (w1) = 0.5, c (w1) = 0.0
    // weighted mean = (1.0*2 + 0.5*1 + 0.0*1) / 4 = 2.5/4 = 0.625
    const r = aggregateScore(rubricBase, [crit('a', 1), crit('b', 0.5), crit('c', 0)]);
    expect(r.overallScore).toBeCloseTo(0.625, 4);
    expect(r.overallPassed).toBe(false);
    expect(r.overallVerdict).toBe('fail');
  });

  it('treats missing weights as 1.0', () => {
    const rubric = { ...rubricBase, criteria: rubricBase.criteria.map((c) => ({ ...c, weight: undefined })) };
    const r = aggregateScore(rubric, [crit('a', 1), crit('b', 0.5), crit('c', 0)]);
    expect(r.overallScore).toBeCloseTo(0.5, 4);
  });
});

describe('aggregateScore — min', () => {
  it('returns the lowest criterion score', () => {
    const r = aggregateScore(
      { ...rubricBase, overall: 'min' },
      [crit('a', 1), crit('b', 0.3), crit('c', 0.8)],
    );
    expect(r.overallScore).toBe(0.3);
    expect(r.overallPassed).toBe(false);
  });
});

describe('aggregateScore — all-pass', () => {
  it('returns 1 when every criterion passed', () => {
    const r = aggregateScore(
      { ...rubricBase, overall: 'all-pass' },
      [crit('a', 0.8, true), crit('b', 0.9, true), crit('c', 0.7, true)],
    );
    expect(r.overallScore).toBe(1);
    expect(r.overallPassed).toBe(true);
  });
  it('returns 0 when any criterion failed', () => {
    const r = aggregateScore(
      { ...rubricBase, overall: 'all-pass' },
      [crit('a', 0.8, true), crit('b', 0.4, false), crit('c', 0.7, true)],
    );
    expect(r.overallScore).toBe(0);
    expect(r.overallPassed).toBe(false);
  });
});

describe('aggregateScore — passThreshold override', () => {
  it('per-step override beats rubric default', () => {
    // weighted-mean = 0.625; rubric.passThreshold = 0.7 → fail
    // override to 0.5 → pass
    const r = aggregateScore(rubricBase, [crit('a', 1), crit('b', 0.5), crit('c', 0)], 0.5);
    expect(r.overallPassed).toBe(true);
  });
});

describe('aggregateScore — edge cases', () => {
  it('empty criteria → inconclusive', () => {
    const r = aggregateScore(rubricBase, []);
    expect(r.overallVerdict).toBe('inconclusive');
    expect(r.overallScore).toBe(0);
    expect(r.overallPassed).toBe(false);
  });
  it('clamps overall to 0..1', () => {
    // craft a rubric where weights are 0 — protects against NaN/Inf
    const r = aggregateScore({ ...rubricBase, criteria: [] }, [crit('a', 1)]);
    expect(r.overallScore).toBeGreaterThanOrEqual(0);
    expect(r.overallScore).toBeLessThanOrEqual(1);
  });
});

describe('normalizeCriterionScore', () => {
  it('binary: numeric 1 → 1, 0 → 0', () => {
    expect(normalizeCriterionScore(1, 'binary')).toBe(1);
    expect(normalizeCriterionScore(0, 'binary')).toBe(0);
  });
  it('binary / pass-fail: string forms accepted', () => {
    expect(normalizeCriterionScore('pass', 'pass-fail')).toBe(1);
    expect(normalizeCriterionScore('fail', 'pass-fail')).toBe(0);
    expect(normalizeCriterionScore('true', 'binary')).toBe(1);
    expect(normalizeCriterionScore('false', 'binary')).toBe(0);
  });
  it('1-5 maps to 0..1 by step', () => {
    expect(normalizeCriterionScore(1, '1-5')).toBe(0);
    expect(normalizeCriterionScore(3, '1-5')).toBe(0.5);
    expect(normalizeCriterionScore(5, '1-5')).toBe(1);
  });
  it('1-10 maps to 0..1', () => {
    expect(normalizeCriterionScore(1, '1-10')).toBeCloseTo(0, 4);
    expect(normalizeCriterionScore(10, '1-10')).toBe(1);
  });
  it('out of range clamps', () => {
    expect(normalizeCriterionScore(7, '1-5')).toBe(1);
    expect(normalizeCriterionScore(-2, '1-5')).toBe(0);
    expect(normalizeCriterionScore(99, '1-10')).toBe(1);
  });
  it('non-numeric on numeric scale → NaN', () => {
    expect(Number.isNaN(normalizeCriterionScore('huh', '1-5'))).toBe(true);
  });
});
