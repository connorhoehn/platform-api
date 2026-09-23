// The rollup merge rules (PLAN §2.2): keyed by runId, status only forward,
// older events ignored, ≤ 5 recent, derived counters.

import { applyRunEvent, emptyRollup, rollupFromRuns, RECENT_LIMIT, type PipelineRunRollup } from '../run-rollup';

const t = (s: number) => `2026-09-23T14:00:${String(s).padStart(2, '0')}.000Z`;

describe('applyRunEvent', () => {
  it('a started event on no row makes a running row', () => {
    const row = applyRunEvent(null, { pipelineId: 'p', runId: 'r1', status: 'running', at: t(1), startedAt: t(1) });
    expect(row).toMatchObject({ pipelineId: 'p', lastRunId: 'r1', lastRunAt: t(1), lastRunStatus: 'running', runningCount: 1, failedOfRecent: 0, runCount: 1, updatedAt: t(1) });
  });

  it('completed updates the same run in place; a replay of it returns the same object', () => {
    const a = applyRunEvent(null, { pipelineId: 'p', runId: 'r1', status: 'running', at: t(1), startedAt: t(1) });
    const b = applyRunEvent(a, { pipelineId: 'p', runId: 'r1', status: 'completed', at: t(5), completedAt: t(5) });
    expect(b.recent).toHaveLength(1);
    expect(b).toMatchObject({ lastRunStatus: 'completed', runningCount: 0, runCount: 1, updatedAt: t(5) });
    expect(b.recent[0].completedAt).toBe(t(5));
    const c = applyRunEvent(b, { pipelineId: 'p', runId: 'r1', status: 'completed', at: t(5), completedAt: t(5) });
    expect(c).toBe(b);
  });

  it('a terminal status never regresses to a live one, whatever the timestamp says', () => {
    const done = applyRunEvent(null, { pipelineId: 'p', runId: 'r1', status: 'completed', at: t(5), startedAt: t(1) });
    expect(applyRunEvent(done, { pipelineId: 'p', runId: 'r1', status: 'running', at: t(9) })).toBe(done);
    expect(applyRunEvent(done, { pipelineId: 'p', runId: 'r1', status: 'interrupted', at: t(9) })).toBe(done);
    expect(applyRunEvent(done, { pipelineId: 'p', runId: 'r1', status: 'stuck', at: t(9) })).toBe(done);
  });

  it('an event older than the entry is ignored', () => {
    const failed = applyRunEvent(null, { pipelineId: 'p', runId: 'r1', status: 'failed', at: t(5), startedAt: t(1) });
    expect(applyRunEvent(failed, { pipelineId: 'p', runId: 'r1', status: 'cancelled', at: t(4) })).toBe(failed);
  });

  it('interrupted and stuck can still finish', () => {
    const stuck = applyRunEvent(null, { pipelineId: 'p', runId: 'r1', status: 'stuck', at: t(3), startedAt: t(1) });
    expect(stuck.failedOfRecent).toBe(1);
    const done = applyRunEvent(stuck, { pipelineId: 'p', runId: 'r1', status: 'completed', at: t(7) });
    expect(done.lastRunStatus).toBe('completed');
    expect(done.failedOfRecent).toBe(0);
  });

  it('a terminal event for a run never seen started uses the event time as startedAt and completedAt', () => {
    const row = applyRunEvent(null, { pipelineId: 'p', runId: 'r9', status: 'failed', at: t(8) });
    expect(row.recent[0]).toMatchObject({ runId: 'r9', startedAt: t(8), completedAt: t(8), status: 'failed' });
    // …and a late `started` then corrects startedAt without touching the status.
    const fixed = applyRunEvent(row, { pipelineId: 'p', runId: 'r9', status: 'running', at: t(2), startedAt: t(2) });
    expect(fixed.recent[0]).toMatchObject({ startedAt: t(2), status: 'failed' });
  });

  it('keeps the newest RECENT_LIMIT runs and counts every distinct run', () => {
    let row: PipelineRunRollup | null = null;
    for (let i = 1; i <= 8; i++) row = applyRunEvent(row, { pipelineId: 'p', runId: `r${i}`, status: i % 3 === 0 ? 'failed' : 'completed', at: t(i * 5), startedAt: t(i * 5) });
    expect(row!.recent.map((r) => r.runId)).toEqual(['r8', 'r7', 'r6', 'r5', 'r4']);
    expect(row!.recent).toHaveLength(RECENT_LIMIT);
    expect(row!.runCount).toBe(8);
    expect(row!.failedOfRecent).toBe(1); // r6
    expect(row!.lastRunId).toBe('r8');
  });

  it('a run older than the whole window, arriving late, does not evict a newer run', () => {
    let row: PipelineRunRollup | null = null;
    for (let i = 10; i <= 14; i++) row = applyRunEvent(row, { pipelineId: 'p', runId: `r${i}`, status: 'completed', at: t(i), startedAt: t(i) });
    const same = applyRunEvent(row, { pipelineId: 'p', runId: 'r1', status: 'completed', at: t(1), startedAt: t(1) });
    expect(same).toBe(row);
  });

  it('the pill inputs: failedOfRecent counts failed | interrupted | stuck; runningCount counts live statuses', () => {
    let row: PipelineRunRollup | null = null;
    row = applyRunEvent(row, { pipelineId: 'p', runId: 'a', status: 'running', at: t(1), startedAt: t(1) });
    row = applyRunEvent(row, { pipelineId: 'p', runId: 'b', status: 'awaiting_approval', at: t(2), startedAt: t(2) });
    row = applyRunEvent(row, { pipelineId: 'p', runId: 'c', status: 'interrupted', at: t(3), startedAt: t(3) });
    row = applyRunEvent(row, { pipelineId: 'p', runId: 'd', status: 'rejected', at: t(4), startedAt: t(4) });
    expect(row).toMatchObject({ runningCount: 2, failedOfRecent: 1, lastRunStatus: 'rejected' });
  });
});

describe('rollupFromRuns (the read-side backfill)', () => {
  it('shapes run rows newest first, folding table spellings and stuckAt', () => {
    const row = rollupFromRuns('p', [
      { runId: 'r3', status: 'succeeded', startedAt: t(30), completedAt: t(35) },
      { runId: 'r2', status: 'running', startedAt: t(20), lastEventAt: t(21), stuckAt: t(29) },
      { runId: 'r1', status: 'canceled', startedAt: t(10), completedAt: t(12) },
    ], t(40));
    expect(row.recent.map((r) => [r.runId, r.status])).toEqual([['r3', 'completed'], ['r2', 'stuck'], ['r1', 'cancelled']]);
    expect(row).toMatchObject({ lastRunId: 'r3', lastRunStatus: 'completed', failedOfRecent: 1, runningCount: 0, runCount: 3, updatedAt: t(35) });
  });
  it('the sentinel for a pipeline with no runs', () => {
    expect(emptyRollup('p', t(1))).toEqual({ pipelineId: 'p', lastRunId: '', lastRunAt: '', lastRunStatus: null, recent: [], failedOfRecent: 0, runningCount: 0, runCount: 0, approvals: [], pendingApprovals: 0, updatedAt: t(1) });
  });

});

describe('approval gates on the row (NFR loop 18: the count every replica answers)', () => {
  const started = () => applyRunEvent(null, { pipelineId: 'p', runId: 'r1', status: 'running', at: t(1), startedAt: t(1) });

  it('requested adds a (runId, stepId) key and marks the run awaiting; a replay adds nothing', () => {
    const a = applyRunEvent(started(), { pipelineId: 'p', runId: 'r1', status: 'awaiting_approval', at: t(2), approval: { stepId: 's1', pending: true } });
    expect(a.pendingApprovals).toBe(1);
    expect(a.approvals).toEqual([{ runId: 'r1', stepId: 's1', at: t(2) }]);
    expect(a.lastRunStatus).toBe('awaiting_approval');
    expect(a.runningCount).toBe(1);
    const again = applyRunEvent(a, { pipelineId: 'p', runId: 'r1', status: 'awaiting_approval', at: t(2), approval: { stepId: 's1', pending: true } });
    expect(again).toBe(a);
  });

  it('recorded removes its key; the run stays awaiting while another gate on it waits', () => {
    let row = started();
    row = applyRunEvent(row, { pipelineId: 'p', runId: 'r1', status: 'awaiting_approval', at: t(2), approval: { stepId: 's1', pending: true } });
    row = applyRunEvent(row, { pipelineId: 'p', runId: 'r1', status: 'awaiting_approval', at: t(3), approval: { stepId: 's2', pending: true } });
    expect(row.pendingApprovals).toBe(2);
    row = applyRunEvent(row, { pipelineId: 'p', runId: 'r1', status: 'running', at: t(4), approval: { stepId: 's1', pending: false } });
    expect(row.pendingApprovals).toBe(1);
    expect(row.lastRunStatus).toBe('awaiting_approval');
    row = applyRunEvent(row, { pipelineId: 'p', runId: 'r1', status: 'running', at: t(5), approval: { stepId: 's2', pending: false } });
    expect(row.pendingApprovals).toBe(0);
    expect(row.lastRunStatus).toBe('running');
  });

  it('a replayed `requested` older than the `recorded` does not re-open the gate', () => {
    let row = started();
    row = applyRunEvent(row, { pipelineId: 'p', runId: 'r1', status: 'awaiting_approval', at: t(2), approval: { stepId: 's1', pending: true } });
    row = applyRunEvent(row, { pipelineId: 'p', runId: 'r1', status: 'running', at: t(4), approval: { stepId: 's1', pending: false } });
    const late = applyRunEvent(row, { pipelineId: 'p', runId: 'r1', status: 'awaiting_approval', at: t(2), approval: { stepId: 's1', pending: true } });
    expect(late).toBe(row);
    expect(late.pendingApprovals).toBe(0);
  });

  it('a run leaving the live statuses releases every gate it held (terminal, interrupted, stuck)', () => {
    for (const status of ['completed', 'failed', 'cancelled', 'rejected', 'interrupted', 'stuck'] as const) {
      let row = started();
      row = applyRunEvent(row, { pipelineId: 'p', runId: 'r1', status: 'awaiting_approval', at: t(2), approval: { stepId: 's1', pending: true } });
      row = applyRunEvent(row, { pipelineId: 'p', runId: 'r1', status: 'awaiting_approval', at: t(3), approval: { stepId: 's2', pending: true } });
      const done = applyRunEvent(row, { pipelineId: 'p', runId: 'r1', status, at: t(6) });
      expect(done.pendingApprovals).toBe(0);
      expect(done.approvals).toEqual([]);
      expect(done.lastRunStatus).toBe(status);
    }
  });

  it('gates of one run do not touch another run of the same pipeline', () => {
    let row = started();
    row = applyRunEvent(row, { pipelineId: 'p', runId: 'r2', status: 'running', at: t(2), startedAt: t(2) });
    row = applyRunEvent(row, { pipelineId: 'p', runId: 'r1', status: 'awaiting_approval', at: t(3), approval: { stepId: 's1', pending: true } });
    row = applyRunEvent(row, { pipelineId: 'p', runId: 'r2', status: 'completed', at: t(4), completedAt: t(4) });
    expect(row.pendingApprovals).toBe(1);
    expect(row.recent.find((r) => r.runId === 'r1')!.status).toBe('awaiting_approval');
  });

  it('a row written before the field existed reads as no gates', () => {
    const legacy = { ...started() } as PipelineRunRollup & { approvals?: unknown };
    delete legacy.approvals;
    const row = applyRunEvent(legacy as PipelineRunRollup, { pipelineId: 'p', runId: 'r1', status: 'awaiting_approval', at: t(2), approval: { stepId: 's1', pending: true } });
    expect(row.pendingApprovals).toBe(1);
  });
});
