// The list route's rollup read: one batch get, a bounded oldest-first
// backfill from the run table, and the sentinel for a pipeline with no runs.

import { loadRollups, BACKFILL_PER_REQUEST } from '../run-rollup-read';
import { emptyRollup, type PipelineRunRollup } from '../run-rollup';

jest.mock('../../lib/logger', () => ({
  withContext: () => ({ info: jest.fn(), warn: jest.fn(), error: jest.fn() }),
}));

function fakeRollups(seed: PipelineRunRollup[] = []) {
  const rows = new Map(seed.map((r) => [r.pipelineId, r]));
  const getMany = jest.fn(async (ids: readonly string[]) => new Map(ids.map((id) => [id, rows.get(id) ?? null])));
  const putIfNotNewer = jest.fn(async (next: PipelineRunRollup) => {
    const cur = rows.get(next.pipelineId);
    if (cur && cur.updatedAt >= next.updatedAt) return false;
    rows.set(next.pipelineId, next);
    return true;
  });
  return { rows, getMany, putIfNotNewer };
}

describe('loadRollups', () => {
  it('returns stored rows and backfills the missing ones from the run table, sentinel when there are no runs', async () => {
    const rollups = fakeRollups([emptyRollup('has-row', '2026-09-23T13:00:00.000Z')]);
    const listRuns = jest.fn(async (id: string) => (id === 'ran' ? [{ runId: 'r1', status: 'completed', startedAt: '2026-09-23T12:00:00.000Z', completedAt: '2026-09-23T12:01:00.000Z' }] : []));
    const out = await loadRollups([{ id: 'has-row' }, { id: 'ran' }, { id: 'never' }], { rollups, listRuns, now: () => '2026-09-23T14:00:00.000Z' });
    expect(out.get('has-row')!.updatedAt).toBe('2026-09-23T13:00:00.000Z');
    expect(out.get('ran')).toMatchObject({ lastRunId: 'r1', lastRunStatus: 'completed', runCount: 1 });
    expect(out.get('never')).toMatchObject({ lastRunId: '', lastRunStatus: null, recent: [] });
    expect(listRuns).toHaveBeenCalledTimes(2);
    expect(listRuns).not.toHaveBeenCalledWith('has-row', expect.anything());
    expect(rollups.putIfNotNewer).toHaveBeenCalledTimes(2);
  });

  it('probes at most BACKFILL_PER_REQUEST missing ids per request, oldest-updated first; the rest are null', async () => {
    const rollups = fakeRollups();
    const listRuns = jest.fn(async () => []);
    const candidates = Array.from({ length: BACKFILL_PER_REQUEST + 5 }, (_, i) => ({ id: `p${i}`, updatedAt: `2026-09-${String(30 - i).padStart(2, '0')}T00:00:00.000Z` }));
    const out = await loadRollups(candidates, { rollups, listRuns });
    expect(listRuns).toHaveBeenCalledTimes(BACKFILL_PER_REQUEST);
    const probed = listRuns.mock.calls.map((c) => (c as unknown[])[0]).sort();
    // The oldest-updated are the highest indexes.
    expect(probed).toEqual(candidates.slice(5).map((c) => c.id).sort());
    expect(out.get('p0')).toBeNull();
    expect(out.get(`p${BACKFILL_PER_REQUEST + 4}`)).not.toBeNull();
  });

  it('a subscriber row that lands first wins over the backfill', async () => {
    const rollups = fakeRollups();
    const live = { ...emptyRollup('p', '2026-09-23T15:00:00.000Z'), lastRunId: 'live' };
    rollups.putIfNotNewer.mockImplementationOnce(async () => { rollups.rows.set('p', live); return false; });
    const out = await loadRollups([{ id: 'p' }], { rollups, listRuns: async () => [], now: () => '2026-09-23T14:00:00.000Z' });
    expect(out.get('p')!.lastRunId).toBe('live');
  });

  it('with no run table wired, missing rows stay null and nothing is written', async () => {
    const rollups = fakeRollups();
    const out = await loadRollups([{ id: 'p' }], { rollups, listRuns: null });
    expect(out.get('p')).toBeNull();
    expect(rollups.putIfNotNewer).not.toHaveBeenCalled();
  });

  it('a failing probe answers null for that id and leaves the others alone', async () => {
    const rollups = fakeRollups();
    const listRuns = jest.fn(async (id: string) => { if (id === 'bad') throw new Error('ddb down'); return []; });
    const out = await loadRollups([{ id: 'bad' }, { id: 'ok' }], { rollups, listRuns });
    expect(out.get('bad')).toBeNull();
    expect(out.get('ok')).not.toBeNull();
  });
});
