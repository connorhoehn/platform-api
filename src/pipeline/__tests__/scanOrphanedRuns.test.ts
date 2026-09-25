// social-api/src/pipeline/__tests__/scanOrphanedRuns.test.ts
//
// Hub#1282: bootstrap orphan-scan transitions stale `running` runs to
// `interrupted`. Tests use a stub repository that captures put() args.

import { describe, it, expect, jest } from '@jest/globals';
import { scanOrphanedRuns } from '../scanOrphanedRuns';
import type {
  PipelineRunRepository,
  PipelineRunItem,
} from '../../repositories/PipelineRunRepository';

function stubRepo(running: PipelineRunItem[], opts: { listShouldThrow?: boolean; putShouldThrow?: (item: PipelineRunItem) => boolean } = {}) {
  const put = jest.fn(async (item: PipelineRunItem) => {
    if (opts.putShouldThrow?.(item)) throw new Error('ddb write failed');
  });
  const listByStatus = jest.fn(async (status: string) => {
    if (opts.listShouldThrow) throw new Error('ddb list failed');
    return status === 'running' ? running : [];
  });
  return {
    repo: { put, listByStatus } as unknown as PipelineRunRepository,
    put,
    listByStatus,
  };
}

function makeRun(overrides: Partial<PipelineRunItem>): PipelineRunItem {
  return {
    pipelineId: 'p1',
    runId: 'r-' + Math.random().toString(36).slice(2, 8),
    status: 'running',
    triggeredBy: 'u1',
    triggerType: 'manual',
    startedAt: new Date().toISOString(),
    ...overrides,
  };
}

describe('scanOrphanedRuns (hub#1282)', () => {
  const NOW = Date.parse('2026-05-06T12:00:00.000Z');

  it('transitions a recent running run to interrupted with timestamp', async () => {
    const recentStart = new Date(NOW - 30 * 60 * 1000).toISOString(); // 30 min ago
    const run = makeRun({ runId: 'r-recent', startedAt: recentStart });
    const { repo, put } = stubRepo([run]);

    const result = await scanOrphanedRuns({
      repository: repo,
      now: () => NOW,
    });

    expect(result).toEqual({ scanned: 1, transitioned: 1, skippedOutsideWindow: 0 });
    expect(put).toHaveBeenCalledTimes(1);
    const updated = put.mock.calls[0][0] as PipelineRunItem;
    expect(updated.status).toBe('interrupted');
    expect(updated.interruptedAt).toBe('2026-05-06T12:00:00.000Z');
    expect(updated.runId).toBe('r-recent');
    // Other fields preserved
    expect(updated.startedAt).toBe(recentStart);
    expect(updated.triggeredBy).toBe('u1');
  });

  it('NFR loop 18: an interrupted run reaches the rollup row and the bus as pipeline:run:orphaned', async () => {
    const recentStart = new Date(NOW - 30 * 60 * 1000).toISOString();
    const run = makeRun({ runId: 'r-recent', startedAt: recentStart, ownerNodeId: 'node-a' });
    const { repo } = stubRepo([run]);
    const applyResolved = jest.fn(async (_resolved: unknown) => null);
    const publish = jest.fn(async (_event: string, _payload: unknown) => undefined);

    const result = await scanOrphanedRuns({ repository: repo, now: () => NOW, rollup: { applyResolved }, publish });

    expect(result.transitioned).toBe(1);
    expect(applyResolved).toHaveBeenCalledWith({
      pipelineId: 'p1', runId: 'r-recent', status: 'interrupted', at: '2026-05-06T12:00:00.000Z', startedAt: recentStart,
    });
    expect(publish).toHaveBeenCalledWith('pipeline:run:orphaned', { runId: 'r-recent', previousOwner: 'node-a', at: '2026-05-06T12:00:00.000Z', pipelineId: 'p1' });
  });

  it('NFR loop 18: a rollup or bus failure is logged; the run-table transition stands', async () => {
    const run = makeRun({ runId: 'r-recent', startedAt: new Date(NOW - 30 * 60 * 1000).toISOString() });
    const { repo, put } = stubRepo([run]);
    const result = await scanOrphanedRuns({
      repository: repo,
      now: () => NOW,
      rollup: { applyResolved: jest.fn(async () => { throw new Error('ddb cas gave up'); }) },
      publish: jest.fn(async () => { throw new Error('bus down'); }),
    });
    expect(result.transitioned).toBe(1);
    expect(put).toHaveBeenCalledTimes(1);
  });

  it('NFR loop 18: a row finalized from terminal evidence reaches the rollup as that terminal status, with no bus event', async () => {
    const recentStart = new Date(NOW - 30 * 60 * 1000).toISOString();
    const run = makeRun({ runId: 'r-done', startedAt: recentStart });
    const { repo } = stubRepo([run]);
    const applyResolved = jest.fn(async (_resolved: unknown) => null);
    const publish = jest.fn(async (_event: string, _payload: unknown) => undefined);

    const result = await scanOrphanedRuns({
      repository: repo,
      now: () => NOW,
      terminalEvidence: async () => ({ status: 'completed', at: '2026-05-06T11:45:00.000Z', source: 'audit', detail: 'pipeline.run.completed row' }),
      rollup: { applyResolved },
      publish,
    });

    expect(result.transitioned).toBe(0);
    expect(applyResolved).toHaveBeenCalledTimes(1);
    expect(applyResolved.mock.calls[0][0]).toMatchObject({ pipelineId: 'p1', runId: 'r-done', status: 'completed', startedAt: recentStart });
    expect(publish).not.toHaveBeenCalled();
  });

  it('skips runs outside the resume window (older than 4h default)', async () => {
    const oldStart = new Date(NOW - 5 * 60 * 60 * 1000).toISOString(); // 5h ago
    const recentStart = new Date(NOW - 1 * 60 * 60 * 1000).toISOString(); // 1h ago
    const oldRun = makeRun({ runId: 'r-old', startedAt: oldStart });
    const recentRun = makeRun({ runId: 'r-fresh', startedAt: recentStart });
    const { repo, put } = stubRepo([oldRun, recentRun]);

    const result = await scanOrphanedRuns({
      repository: repo,
      now: () => NOW,
    });

    expect(result).toEqual({ scanned: 2, transitioned: 1, skippedOutsideWindow: 1 });
    expect(put).toHaveBeenCalledTimes(1);
    expect((put.mock.calls[0][0] as PipelineRunItem).runId).toBe('r-fresh');
  });

  it('honours a custom resumeWindowMs', async () => {
    const start = new Date(NOW - 10 * 60 * 1000).toISOString(); // 10 min ago
    const run = makeRun({ runId: 'r-10min', startedAt: start });
    const { repo, put } = stubRepo([run]);

    // 5-minute window: the run is too old.
    const result = await scanOrphanedRuns({
      repository: repo,
      now: () => NOW,
      resumeWindowMs: 5 * 60 * 1000,
    });

    expect(result).toEqual({ scanned: 1, transitioned: 0, skippedOutsideWindow: 1 });
    expect(put).not.toHaveBeenCalled();
  });

  it('returns zero counts when nothing is in running status', async () => {
    const { repo, put } = stubRepo([]);
    const result = await scanOrphanedRuns({ repository: repo, now: () => NOW });
    expect(result).toEqual({ scanned: 0, transitioned: 0, skippedOutsideWindow: 0 });
    expect(put).not.toHaveBeenCalled();
  });

  it('survives a per-row put() failure and continues processing', async () => {
    const r1 = makeRun({ runId: 'r-fails', startedAt: new Date(NOW - 60_000).toISOString() });
    const r2 = makeRun({ runId: 'r-ok', startedAt: new Date(NOW - 60_000).toISOString() });
    const { repo, put } = stubRepo([r1, r2], {
      putShouldThrow: (item) => item.runId === 'r-fails',
    });

    const result = await scanOrphanedRuns({ repository: repo, now: () => NOW });

    // The failing put still counted as "attempted but transitioned=false";
    // the implementation only increments transitioned on success.
    expect(result.scanned).toBe(2);
    expect(result.transitioned).toBe(1);
    expect(put).toHaveBeenCalledTimes(2);
  });

  it('returns zeros (not throws) when listByStatus fails', async () => {
    const { repo, put } = stubRepo([], { listShouldThrow: true });
    const result = await scanOrphanedRuns({ repository: repo, now: () => NOW });
    expect(result).toEqual({ scanned: 0, transitioned: 0, skippedOutsideWindow: 0 });
    expect(put).not.toHaveBeenCalled();
  });

  it('skips rows with malformed startedAt', async () => {
    const bad = makeRun({ runId: 'r-bad', startedAt: 'not-a-date' });
    const good = makeRun({ runId: 'r-good', startedAt: new Date(NOW - 60_000).toISOString() });
    const { repo, put } = stubRepo([bad, good]);
    const result = await scanOrphanedRuns({ repository: repo, now: () => NOW });
    expect(result.scanned).toBe(2);
    expect(result.transitioned).toBe(1);
    expect((put.mock.calls[0][0] as PipelineRunItem).runId).toBe('r-good');
  });
});
