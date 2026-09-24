// GET /api/pipelines/:pipelineId/estimate (PLAN §2.3): a range only from the
// pipeline's own completed runs, `null` with a reason when there are none,
// repriced through `computeCost` when a model is chosen.

// Loop 29c: run access has its own suite (runAccess.test.ts); these tests
// exercise the route behaviour behind it, so the gate passes here.
jest.mock('../../pipeline/runAccess', () => {
  const actual = jest.requireActual('../../pipeline/runAccess');
  const pass = (policy: string) => Object.assign((_q: unknown, _s: unknown, n: () => void) => n(), { accessPolicy: policy });
  return {
    ...actual,
    requireRunAccess: () => pass('run:read'),
    requirePipelineVisible: () => pass('pipeline:owner'),
    assertRunAccess: async () => ({ role: 'owner' }),
    assertItemRunAccess: async () => undefined,
    canSeeRun: async () => true,
    filterVisibleRunItems: async (_r: unknown, items: unknown[]) => items,
  };
});

jest.mock('../../lib/aws-clients', () => ({ docClient: {}, ddbClient: {} }));

import { createPipelineEstimateRouter, estimateFromRuns } from '../pipelineEstimate';
import { computeCost } from '../../pipeline/llm-pricing';
import type { RollupRecentRun } from '../../pipeline/run-rollup';
import { call, serve } from './helpers/workFakes';

const run = (runId: string, status: RollupRecentRun['status'], startedAt: string, completedAt?: string): RollupRecentRun => ({
  runId, status, startedAt, ...(completedAt ? { completedAt } : {}), at: completedAt ?? startedAt,
});

const RECENT: RollupRecentRun[] = [
  run('r1', 'completed', '2026-09-23T10:00:00.000Z', '2026-09-23T10:00:40.000Z'),
  run('r2', 'completed', '2026-09-23T11:00:00.000Z', '2026-09-23T11:01:30.000Z'),
  run('r3', 'completed', '2026-09-23T12:00:00.000Z', '2026-09-23T12:01:00.000Z'),
  run('r4', 'failed', '2026-09-23T13:00:00.000Z', '2026-09-23T13:00:05.000Z'),
  run('r5', 'running', '2026-09-23T14:00:00.000Z'),
];

const ROWS = new Map([
  ['r1', { runId: 'r1', tokenUsage: { 'claude-3-haiku': { tokensIn: 100_000, tokensOut: 10_000 } } }],
  ['r2', { runId: 'r2', tokenUsage: { 'claude-3-haiku': { tokensIn: 300_000, tokensOut: 20_000 } } }],
  ['r3', { runId: 'r3', estimatedCostUsd: 0.05 }],
]);

describe('estimateFromRuns', () => {
  it('no runs gives null with reason and confidence none', () => {
    expect(estimateFromRuns('p', [], new Map())).toEqual({
      pipelineId: 'p', basis: { runs: 0, costRuns: 0, repriced: false }, costUsd: null, durationMs: null, confidence: 'none', reason: 'no-completed-runs',
    });
    // Runs that never completed do not count.
    expect(estimateFromRuns('p', [RECENT[3]!, RECENT[4]!], new Map()).costUsd).toBeNull();
  });

  it('three completed runs give a range with low confidence', () => {
    const e = estimateFromRuns('p', RECENT, ROWS);
    expect(e.confidence).toBe('low');
    expect(e.basis).toEqual({ runs: 3, costRuns: 3, repriced: false });
    expect(e.durationMs).toEqual({ low: 40_000, high: 90_000 });
    const c1 = computeCost('claude-3-haiku', 100_000, 10_000);
    const c2 = computeCost('claude-3-haiku', 300_000, 20_000);
    expect(e.costUsd!.low).toBeCloseTo(Math.min(c1, c2, 0.05));
    expect(e.costUsd!.high).toBeCloseTo(Math.max(c1, c2, 0.05));
    expect(e.reason).toBeUndefined();
  });

  it('repricing a chosen model uses computeCost over each run\'s tokens; a total-only run cannot be repriced', () => {
    const e = estimateFromRuns('p', RECENT, ROWS, 'opus');
    const o1 = computeCost('opus', 100_000, 10_000);
    const o2 = computeCost('opus', 300_000, 20_000);
    expect(e.costUsd!.high).toBeCloseTo(Math.max(o1, o2, 0.05));
    expect(e.basis).toEqual({ runs: 3, costRuns: 3, model: 'opus', repriced: false });
    const only = estimateFromRuns('p', RECENT.slice(0, 2), ROWS, 'opus');
    expect(only.basis.repriced).toBe(true);
    expect(only.costUsd).toEqual({ low: o1, high: o2 });
  });

  it('completed runs with no recorded cost give a duration and a null cost with a reason', () => {
    const e = estimateFromRuns('p', RECENT.slice(0, 2), new Map([['r1', { runId: 'r1' }], ['r2', { runId: 'r2' }]]));
    expect(e.costUsd).toBeNull();
    expect(e.durationMs).toEqual({ low: 40_000, high: 90_000 });
    expect(e.reason).toBe('no-cost-recorded');
  });

  it('five runs give medium confidence and only the five newest count', () => {
    const many = Array.from({ length: 7 }, (_, i) => run(`m${i}`, 'completed', `2026-09-2${i}T00:00:00.000Z`, `2026-09-2${i}T00:00:0${i + 1}.000Z`));
    const e = estimateFromRuns('p', many, new Map());
    expect(e.basis.runs).toBe(5);
    expect(e.confidence).toBe('medium');
    expect(e.durationMs).toEqual({ low: 3000, high: 7000 });
  });
});

describe('GET /api/pipelines/:pipelineId/estimate', () => {
  it('answers with history, without history, and refuses a bad model', async () => {
    const reads: string[][] = [];
    const router = createPipelineEstimateRouter({
      recent: async (id) => (id === 'deck-revise' ? RECENT : null),
      runs: async (_id, ids) => { reads.push([...ids]); return new Map(ids.map((i) => [i, ROWS.get(i) ?? null])); },
    });
    const app = await serve([['/api/pipelines/:pipelineId/estimate', router]]);
    try {
      const withHistory = await call(app.url, 'GET', '/api/pipelines/deck-revise/estimate?model=sonnet', 'dev-connor');
      expect(withHistory.status).toBe(200);
      expect(withHistory.body).toMatchObject({ pipelineId: 'deck-revise', confidence: 'low', basis: { runs: 3, model: 'sonnet' } });
      expect(reads).toEqual([['r1', 'r2', 'r3']]);
      const none = await call(app.url, 'GET', '/api/pipelines/brand-new/estimate', 'dev-connor');
      expect(none.body).toMatchObject({ pipelineId: 'brand-new', costUsd: null, durationMs: null, confidence: 'none', reason: 'no-completed-runs' });
      expect(reads).toHaveLength(1);
      expect((await call(app.url, 'GET', '/api/pipelines/p/estimate?model=a%20b', 'dev-connor')).status).toBe(400);
    } finally {
      await app.close();
    }
  });
});
