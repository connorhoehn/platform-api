// platform-api/src/pipeline/judge/aggregateScore.ts
//
// Pure aggregation: given a rubric + per-criterion scores, compute the
// overall score, verdict, and pass flag. Server-side computation is
// load-bearing — letting the model do the math collapses auditability
// (the model could hallucinate an overall score that contradicts its own
// criterion breakdown). See the architecture audit, constraint #2.
//
// Three modes:
//   weighted-mean   sum(score * weight) / sum(weight)
//   min             min(score)
//   all-pass        1 if every criterion passed, 0 otherwise
//
// Pure / no side effects. Heavily unit-tested.

import type {
  CriterionScore,
  JudgmentRubric,
  JudgmentVerdict,
} from './types';

export interface AggregationResult {
  overallScore: number;
  overallVerdict: JudgmentVerdict;
  overallPassed: boolean;
}

export function aggregateScore(
  rubric: JudgmentRubric,
  criteria: CriterionScore[],
  passThresholdOverride?: number,
): AggregationResult {
  if (criteria.length === 0) {
    return { overallScore: 0, overallVerdict: 'inconclusive', overallPassed: false };
  }

  let overallScore: number;
  switch (rubric.overall) {
    case 'weighted-mean': {
      let weightedSum = 0;
      let weightTotal = 0;
      for (const c of criteria) {
        const def = rubric.criteria.find((d) => d.id === c.criterionId);
        const w = def?.weight ?? 1;
        weightedSum += c.score * w;
        weightTotal += w;
      }
      overallScore = weightTotal === 0 ? 0 : weightedSum / weightTotal;
      break;
    }
    case 'min': {
      overallScore = Math.min(...criteria.map((c) => c.score));
      break;
    }
    case 'all-pass': {
      overallScore = criteria.every((c) => c.passed) ? 1 : 0;
      break;
    }
    default: {
      const _exhaustive: never = rubric.overall;
      throw new Error(`Unknown aggregation: ${_exhaustive as string}`);
    }
  }

  // Clamp to 0..1 — defensive against floating-point drift.
  overallScore = Math.max(0, Math.min(1, overallScore));

  const passThreshold = passThresholdOverride ?? rubric.passThreshold ?? 0.7;
  const overallPassed = overallScore >= passThreshold;

  // Verdict shorthand for downstream UI / alerts. `inconclusive` is
  // reserved for parse/malformed-response paths upstream; aggregateScore
  // returns pass/fail based on the threshold.
  const overallVerdict: JudgmentVerdict = overallPassed ? 'pass' : 'fail';

  return { overallScore, overallVerdict, overallPassed };
}

/**
 * Normalize a raw model score (1-5, "pass", etc.) into the 0..1 space
 * the aggregator works in. Pure / table-driven.
 *
 * - `binary`: 0 or 1 → 0 or 1 (string "pass"/"fail" also accepted).
 * - `1-5`: 1..5 → 0, 0.25, 0.5, 0.75, 1.
 * - `1-10`: 1..10 → 0, 0.111…, …, 1.
 * - `pass-fail`: same as binary but the model emits the string form.
 *
 * Out-of-range values clamp; non-numeric on numeric scales returns NaN
 * upstream (handler treats as a malformed criterion → step fails).
 */
export function normalizeCriterionScore(
  raw: number | string,
  scale: 'binary' | '1-5' | '1-10' | 'pass-fail',
): number {
  if (scale === 'binary' || scale === 'pass-fail') {
    if (typeof raw === 'string') {
      const lower = raw.trim().toLowerCase();
      if (lower === 'pass' || lower === 'true' || lower === '1') return 1;
      if (lower === 'fail' || lower === 'false' || lower === '0') return 0;
      return NaN;
    }
    return raw >= 0.5 ? 1 : 0;
  }
  if (scale === '1-5') {
    if (typeof raw !== 'number' || Number.isNaN(raw)) return NaN;
    const clamped = Math.max(1, Math.min(5, raw));
    return (clamped - 1) / 4;
  }
  if (scale === '1-10') {
    if (typeof raw !== 'number' || Number.isNaN(raw)) return NaN;
    const clamped = Math.max(1, Math.min(10, raw));
    return (clamped - 1) / 9;
  }
  return NaN;
}
