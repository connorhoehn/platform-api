// The rollup subscriber: exactly-once per bus event id, idempotent per runId,
// and safe with two replicas sharing one table (compare-and-set on updatedAt).

import type { EventBus } from 'distributed-core';
import type { PipelineEventMap } from 'distributed-core/applications/pipeline';
import { RunRollupSubscriber, getRunRollupSubscriber, setRunRollupSubscriber } from '../run-rollup-subscriber';
import { RollupVersionConflict } from '../../repositories/PipelineRunRollupRepository';
import { emptyRollup, type PipelineRunRollup } from '../run-rollup';

jest.mock('../../lib/logger', () => ({
  withContext: () => ({ info: jest.fn(), warn: jest.fn(), error: jest.fn(), debug: jest.fn() }),
}));

type Handler = (event: { id?: string; payload: unknown }) => void | Promise<void>;

class StubBus {
  private handlers = new Map<string, Handler[]>();
  private nextId = 1;
  subscribe(eventName: string, handler: Handler): string {
    const list = this.handlers.get(eventName) ?? [];
    list.push(handler);
    this.handlers.set(eventName, list);
    return `sub-${this.nextId++}`;
  }
  unsubscribe(): void {}
  async emit(eventName: keyof PipelineEventMap, payload: unknown, id?: string): Promise<void> {
    for (const h of this.handlers.get(eventName) ?? []) await h({ id, payload });
  }
}

/** One DynamoDB table, shared by every "replica"; conditional put semantics only. */
class SharedTable {
  rows = new Map<string, PipelineRunRollup>();
  puts = 0;
  /** Delay before a put lands, to interleave two replicas. */
  putDelayMs = 0;
  async get(pipelineId: string): Promise<PipelineRunRollup | null> {
    const row = this.rows.get(pipelineId);
    return row ? JSON.parse(JSON.stringify(row)) : null;
  }
  async putIfUnchanged(next: PipelineRunRollup, seen: PipelineRunRollup | null): Promise<void> {
    if (this.putDelayMs) await new Promise((r) => setTimeout(r, this.putDelayMs));
    const cur = this.rows.get(next.pipelineId);
    if (seen === null ? cur !== undefined : !cur || cur.updatedAt !== seen.updatedAt) throw new RollupVersionConflict(next.pipelineId);
    this.rows.set(next.pipelineId, JSON.parse(JSON.stringify(next)));
    this.puts += 1;
  }
}

const t = (s: number) => `2026-09-23T14:00:${String(s).padStart(2, '0')}.000Z`;

function replica(table: SharedTable, lookup?: (runId: string) => Promise<string | null>) {
  const bus = new StubBus();
  const sub = new RunRollupSubscriber({
    eventBus: bus as unknown as EventBus<PipelineEventMap>,
    repo: table,
    lookupPipelineId: lookup ?? (async () => null),
  });
  sub.start();
  return { bus, sub };
}

describe('RunRollupSubscriber', () => {
  it('started → completed produces one row and moves the status, with no request from anyone', async () => {
    const table = new SharedTable();
    const { bus, sub } = replica(table);
    await bus.emit('pipeline:run:started', { runId: 'r1', pipelineId: 'deck-revise', triggeredBy: {}, at: t(1) }, 'e1');
    await sub.idle();
    expect(table.rows.get('deck-revise')).toMatchObject({ lastRunId: 'r1', lastRunStatus: 'running', runningCount: 1 });
    await bus.emit('pipeline:run:completed', { runId: 'r1', durationMs: 4000, at: t(5) }, 'e2');
    await sub.idle();
    expect(table.rows.get('deck-revise')).toMatchObject({ lastRunId: 'r1', lastRunStatus: 'completed', runningCount: 0, updatedAt: t(5) });
    expect(table.rows.get('deck-revise')!.recent[0].completedAt).toBe(t(5));
  });

  it('the same completed event delivered twice (same id) is dropped before any read; a copy with a new id changes nothing', async () => {
    const table = new SharedTable();
    const { bus, sub } = replica(table);
    await bus.emit('pipeline:run:started', { runId: 'r1', pipelineId: 'p', triggeredBy: {}, at: t(1) }, 'e1');
    await bus.emit('pipeline:run:completed', { runId: 'r1', durationMs: 1, at: t(5) }, 'e2');
    await sub.idle();
    const putsAfterFirst = table.puts;
    const before = JSON.stringify(table.rows.get('p'));
    await bus.emit('pipeline:run:completed', { runId: 'r1', durationMs: 1, at: t(5) }, 'e2');
    await bus.emit('pipeline:run:completed', { runId: 'r1', durationMs: 1, at: t(5) }, 'e2-outbox-replay');
    await sub.idle();
    expect(table.puts).toBe(putsAfterFirst);
    expect(JSON.stringify(table.rows.get('p'))).toBe(before);
    expect(sub.stats.duplicates).toBe(1);
    expect(sub.stats.unchanged).toBe(1);
  });

  it('a late started after completed cannot regress the row', async () => {
    const table = new SharedTable();
    const { bus, sub } = replica(table);
    await bus.emit('pipeline:run:started', { runId: 'r1', pipelineId: 'p', triggeredBy: {}, at: t(1) }, 'a');
    await bus.emit('pipeline:run:completed', { runId: 'r1', durationMs: 1, at: t(5) }, 'b');
    await bus.emit('pipeline:run:started', { runId: 'r1', pipelineId: 'p', triggeredBy: {}, at: t(1) }, 'c');
    await sub.idle();
    expect(table.rows.get('p')!.lastRunStatus).toBe('completed');
  });

  it('rejected completes as rejected; failed, cancelled, orphaned and stuck map to their statuses', async () => {
    const table = new SharedTable();
    const { bus, sub } = replica(table);
    for (const [i, run] of ['a', 'b', 'c', 'd', 'e'].entries()) {
      await bus.emit('pipeline:run:started', { runId: run, pipelineId: 'p', triggeredBy: {}, at: t(i + 1) }, `s-${run}`);
    }
    await bus.emit('pipeline:run:completed', { runId: 'a', durationMs: 1, at: t(10), status: 'rejected' }, 'x1');
    await bus.emit('pipeline:run:failed', { runId: 'b', error: { message: 'boom' }, at: t(11) }, 'x2');
    await bus.emit('pipeline:run:cancelled', { runId: 'c', at: t(12) }, 'x3');
    await bus.emit('pipeline:run:orphaned', { runId: 'd', previousOwner: 'n1', at: t(13) }, 'x4');
    await bus.emit('pipeline:run:stuck', { runId: 'e', lastHeartbeatAt: null, at: t(14) }, 'x5');
    await sub.idle();
    const row = table.rows.get('p')!;
    const byRun = Object.fromEntries(row.recent.map((r) => [r.runId, r.status]));
    expect(byRun).toEqual({ a: 'rejected', b: 'failed', c: 'cancelled', d: 'interrupted', e: 'stuck' });
    expect(row.failedOfRecent).toBe(3);
    expect(row.runningCount).toBe(0);
  });

  it('a terminal event for a run this process never saw is resolved through the run table lookup, else skipped', async () => {
    const table = new SharedTable();
    const lookup = jest.fn(async (runId: string) => (runId === 'known' ? 'p' : null));
    const { bus, sub } = replica(table, lookup);
    await bus.emit('pipeline:run:completed', { runId: 'known', durationMs: 1, at: t(3) }, 'k');
    await bus.emit('pipeline:run:completed', { runId: 'ghost', durationMs: 1, at: t(4) }, 'g');
    await sub.idle();
    expect(table.rows.get('p')).toMatchObject({ lastRunId: 'known', lastRunStatus: 'completed' });
    expect(sub.stats.unresolved).toBe(1);
    expect(lookup).toHaveBeenCalledTimes(2);
  });

  it('two replicas: the run starts on A and is finalised on B; both events land in one row', async () => {
    const table = new SharedTable();
    const lookup = async (runId: string) => (runId === 'r1' ? 'p' : null);
    const a = replica(table, lookup);
    const b = replica(table, lookup);
    await a.bus.emit('pipeline:run:started', { runId: 'r1', pipelineId: 'p', triggeredBy: {}, at: t(1) }, 'e1');
    await a.sub.idle();
    // B never saw the start (it is the leader's orphan scan finishing the run).
    await b.bus.emit('pipeline:run:completed', { runId: 'r1', durationMs: 1, at: t(6) }, 'e2');
    await b.sub.idle();
    const row = table.rows.get('p')!;
    expect(row.recent).toHaveLength(1);
    expect(row).toMatchObject({ lastRunStatus: 'completed', updatedAt: t(6) });
    // A's copy of the same outbox delivery: a no-op, not a second entry or a regression.
    await a.bus.emit('pipeline:run:completed', { runId: 'r1', durationMs: 1, at: t(6) }, 'e2');
    await a.sub.idle();
    expect(table.rows.get('p')!.recent).toHaveLength(1);
    expect(table.puts).toBe(2);
  });

  it('two replicas writing the same pipeline at once: the loser re-reads and merges, nothing is lost', async () => {
    const table = new SharedTable();
    table.putDelayMs = 5;
    const a = replica(table);
    const b = replica(table);
    await Promise.all([
      a.bus.emit('pipeline:run:started', { runId: 'ra', pipelineId: 'p', triggeredBy: {}, at: t(1) }, 'sa'),
      b.bus.emit('pipeline:run:started', { runId: 'rb', pipelineId: 'p', triggeredBy: {}, at: t(2) }, 'sb'),
    ]);
    await Promise.all([a.sub.idle(), b.sub.idle()]);
    const row = table.rows.get('p')!;
    expect(row.recent.map((r) => r.runId).sort()).toEqual(['ra', 'rb']);
    expect(row.runCount).toBe(2);
    expect(a.sub.stats.conflicts + b.sub.stats.conflicts).toBeGreaterThanOrEqual(1);
  });

  it('start() twice subscribes once; stop() unsubscribes', async () => {
    const table = new SharedTable();
    const bus = new StubBus();
    const unsub = jest.spyOn(bus, 'unsubscribe');
    const sub = new RunRollupSubscriber({ eventBus: bus as unknown as EventBus<PipelineEventMap>, repo: table, lookupPipelineId: async () => null });
    sub.start(); sub.start();
    await bus.emit('pipeline:run:started', { runId: 'r', pipelineId: 'p', triggeredBy: {}, at: t(1) }, 'one');
    await sub.idle();
    expect(table.puts).toBe(1);
    sub.stop();
    expect(unsub).toHaveBeenCalledTimes(9);
  });

  it('approval requested on replica A and recorded on replica B: one count, on the row, for both (NFR loop 18)', async () => {
    const table = new SharedTable();
    const a = replica(table);
    const b = replica(table, async (runId) => (runId === 'r1' ? 'p' : null)); // B never saw r1 start: runId-index lookup
    await a.bus.emit('pipeline:run:started', { runId: 'r1', pipelineId: 'p', triggeredBy: {}, at: t(1) }, 's');
    await a.bus.emit('pipeline:approval:requested', { runId: 'r1', stepId: 'gate', approvers: [], at: t(2) }, 'q');
    await a.sub.idle();
    expect(table.rows.get('p')).toMatchObject({ pendingApprovals: 1, lastRunStatus: 'awaiting_approval' });
    // The same `requested` delivered again (outbox replay) changes nothing.
    await a.bus.emit('pipeline:approval:requested', { runId: 'r1', stepId: 'gate', approvers: [], at: t(2) }, 'q2');
    await a.sub.idle();
    expect(table.rows.get('p')!.pendingApprovals).toBe(1);
    await b.bus.emit('pipeline:approval:recorded', { runId: 'r1', stepId: 'gate', userId: 'u', decision: 'approve', at: t(3) }, 'd');
    await b.sub.idle();
    expect(table.rows.get('p')).toMatchObject({ pendingApprovals: 0, approvals: [], lastRunStatus: 'running' });
    expect(b.sub.stats.unresolved).toBe(0);
  });

  it('the process-wide instance: applyResolved from the sweep, then the bus copy of the same transition needs no lookup', async () => {
    const table = new SharedTable();
    const lookup = jest.fn(async () => null);
    const leader = replica(table, lookup);
    setRunRollupSubscriber(leader.sub);
    expect(getRunRollupSubscriber()).toBe(leader.sub);
    await table.putIfUnchanged({ ...emptyRollup('p', t(1)), lastRunId: 'r1', lastRunAt: t(1), lastRunStatus: 'running', runningCount: 1, runCount: 1, recent: [{ runId: 'r1', status: 'running', startedAt: t(1), at: t(1) }] }, null);
    await getRunRollupSubscriber()!.applyResolved({ pipelineId: 'p', runId: 'r1', status: 'interrupted', at: t(5), startedAt: t(1) });
    expect(table.rows.get('p')).toMatchObject({ lastRunStatus: 'interrupted', failedOfRecent: 1, runningCount: 0 });
    await leader.bus.emit('pipeline:run:orphaned', { runId: 'r1', previousOwner: 'node-a', at: t(5) }, 'o');
    await leader.sub.idle();
    expect(lookup).not.toHaveBeenCalled();
    expect(leader.sub.stats.unchanged).toBe(1);
    expect(table.puts).toBe(2);
    leader.sub.stop();
    expect(getRunRollupSubscriber()).toBeNull();
  });
});
