// NFR #220: the sweep leader's runCount reconciler.
import { reconcileRunCounts } from '../run-rollup-reconciler';
import { RollupVersionConflict } from '../../repositories/PipelineRunRollupRepository';
import { applyRunEvent, type PipelineRunRollup } from '../run-rollup';

jest.mock('../../lib/logger', () => ({
  withContext: () => ({ info: jest.fn(), warn: jest.fn(), error: jest.fn(), debug: jest.fn() }),
}));

const NOW = Date.parse('2026-09-24T10:00:00.000Z');
const row = (updatedAt: string, runCount: number): PipelineRunRollup => ({
  ...applyRunEvent(null, { pipelineId: 'p', runId: 'r1', status: 'completed', at: updatedAt, startedAt: updatedAt }),
  runCount,
  updatedAt,
});

function store(rows: PipelineRunRollup[]) {
  const table = new Map(rows.map((r) => [r.pipelineId, r]));
  return {
    table,
    listAll: async () => [...table.values()],
    putIfUnchanged: async (next: PipelineRunRollup, seen: PipelineRunRollup | null) => {
      if (table.get(next.pipelineId)?.updatedAt !== seen?.updatedAt) throw new RollupVersionConflict(next.pipelineId);
      table.set(next.pipelineId, next);
    },
  };
}

describe('reconcileRunCounts', () => {
  it('corrects an injected drift from the run table, counting only runs before the quiet cutoff', async () => {
    const repo = store([row('2026-09-24T09:58:00.000Z', 57)]);
    const cutoffs: string[] = [];
    const out = await reconcileRunCounts({ repo, now: () => NOW, countRuns: async (_p, cutoff) => { cutoffs.push(cutoff); return 60; } });
    expect(out.corrected).toEqual([{ pipelineId: 'p', from: 57, to: 60 }]);
    expect(repo.table.get('p')?.runCount).toBe(60);
    expect(cutoffs).toEqual(['2026-09-24T09:59:30.000Z']);
  });

  it('leaves a busy row alone and loses to an event landing mid-check', async () => {
    const busy = store([row('2026-09-24T09:59:50.000Z', 3)]);
    expect((await reconcileRunCounts({ repo: busy, now: () => NOW, countRuns: async () => 9 })).busy).toBe(1);
    expect(busy.table.get('p')?.runCount).toBe(3);

    const raced = store([row('2026-09-24T09:50:00.000Z', 3)]);
    const out = await reconcileRunCounts({ repo: raced, now: () => NOW, countRuns: async () => {
      raced.table.set('p', row('2026-09-24T09:59:59.000Z', 4));
      return 9;
    } });
    expect(out.lost).toBe(1);
    expect(raced.table.get('p')?.runCount).toBe(4);
  });
});
