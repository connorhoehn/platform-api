// NFR #120 — a run control (cancel, breakpoint resolve, resume-from-step)
// taken on the replica that does not hold the run reaches the one that does,
// once, over one shared pubsub; a dead owner's control is applied by the
// replica that resumes the run.

import type { EventBus } from 'distributed-core';
import type { PipelineEventMap } from 'distributed-core/applications/pipeline';
import { RunControlForwarder, RUN_CONTROLS_TOPIC, type ClusterPubSubLike } from '../run-control-forwarder';
import { RunRollupSubscriber } from '../run-rollup-subscriber';
import { RollupVersionConflict } from '../../repositories/PipelineRunRollupRepository';
import type { PipelineRunRollup, RollupRunControl } from '../run-rollup';

jest.mock('../../lib/logger', () => ({
  withContext: () => ({ info: jest.fn(), warn: jest.fn(), error: jest.fn(), debug: jest.fn() }),
}));

type Handler = (event: { id?: string; payload: unknown }) => void | Promise<void>;

/** One replica's in-process dc EventBus. */
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

/** The cluster pubsub every replica shares: a publish reaches every subscriber, the publisher included. */
class SharedPubSub implements ClusterPubSubLike {
  private subs = new Map<string, { topic: string; handler: (topic: string, payload: unknown) => void }>();
  private nextId = 1;
  published: unknown[] = [];
  failPublish = false;
  subscribe(topic: string, handler: (topic: string, payload: unknown) => void): string {
    const id = `ps-${this.nextId++}`;
    this.subs.set(id, { topic, handler });
    return id;
  }
  unsubscribe(id: string): boolean { return this.subs.delete(id); }
  async publish(topic: string, payload: unknown): Promise<void> {
    if (this.failPublish) throw new Error('redis down');
    this.published.push(payload);
    for (const s of this.subs.values()) if (s.topic === topic) s.handler(topic, JSON.parse(JSON.stringify(payload)));
  }
}

/** One DynamoDB table shared by every replica; conditional put only. */
class SharedTable {
  rows = new Map<string, PipelineRunRollup>();
  async get(pipelineId: string): Promise<PipelineRunRollup | null> {
    const row = this.rows.get(pipelineId);
    return row ? JSON.parse(JSON.stringify(row)) : null;
  }
  async putIfUnchanged(next: PipelineRunRollup, seen: PipelineRunRollup | null): Promise<void> {
    const cur = this.rows.get(next.pipelineId);
    if (seen === null ? cur !== undefined : !cur || cur.updatedAt !== seen.updatedAt) throw new RollupVersionConflict(next.pipelineId);
    this.rows.set(next.pipelineId, JSON.parse(JSON.stringify(next)));
  }
}

/** The executor side of one replica: the runs it holds and the controls it applied. */
class StubRuns {
  runs = new Map<string, { status: string }>();
  cancelled: string[] = [];
  breakpoints: Array<{ runId: string; stepId: string; action: string }> = [];
  resumed: Array<{ runId: string; fromNodeId: string; options?: unknown }> = [];
  failResume = false;
  hold(runId: string, status = 'running'): void { this.runs.set(runId, { status }); }
  getRun(runId: string) { return this.runs.get(runId) ?? null; }
  cancelRun(runId: string): void {
    if (!this.runs.has(runId)) return; // dc: silent no-op for an unknown run
    this.cancelled.push(runId);
    this.runs.set(runId, { status: 'cancelled' });
  }
  resolveBreakpoint(runId: string, stepId: string, action: 'continue' | 'step_over' | 'resume_all'): void {
    if (!this.runs.has(runId)) return;
    this.breakpoints.push({ runId, stepId, action });
    this.runs.set(runId, { status: 'running' });
  }
  async resumeFromStep(runId: string, fromNodeId: string, options?: unknown): Promise<unknown> {
    if (this.failResume) throw new Error('InvalidRunStateError: not resumable');
    this.resumed.push({ runId, fromNodeId, options });
    this.runs.set(runId, { status: 'running' });
    return { runId };
  }
}

const t = (s: number) => `2026-09-23T17:00:${String(s).padStart(2, '0')}.000Z`;

function replica(nodeId: string, table: SharedTable, pubsub: SharedPubSub) {
  const bus = new StubBus();
  const rollup = new RunRollupSubscriber({
    eventBus: bus as unknown as EventBus<PipelineEventMap>,
    repo: table,
    lookupPipelineId: async (runId) => (runId.startsWith('r') ? 'deck-revise' : null),
  });
  rollup.start();
  const runs = new StubRuns();
  const forwarder = new RunControlForwarder({ nodeId, pubsub, runs, eventBus: bus as unknown as EventBus<PipelineEventMap>, rollup, repo: table });
  forwarder.start();
  const settle = async () => { await rollup.idle(); await forwarder.idle(); await rollup.idle(); };
  return { bus, rollup, runs, forwarder, settle };
}

const cancel = (n: number, over: Partial<{ runId: string; userId: string; reason: string }> = {}): RollupRunControl & { pipelineId: string } => ({
  kind: 'cancel', controlId: `c${n}`, pipelineId: 'deck-revise', runId: 'r1', userId: 'alice', at: t(10 + n), ...over,
});

describe('RunControlForwarder — two replicas on one bus', () => {
  it('a cancel taken on B is applied by A, the holder, exactly once, acknowledged to B, and released from the row', async () => {
    const table = new SharedTable();
    const pubsub = new SharedPubSub();
    const A = replica('node-a', table, pubsub);
    const B = replica('node-b', table, pubsub);
    await A.bus.emit('pipeline:run:started', { runId: 'r1', pipelineId: 'deck-revise', startedAt: t(1) }, 'e1');
    await A.settle();
    A.runs.hold('r1');

    expect(A.forwarder.holdsRun('r1')).toBe(true);
    expect(B.forwarder.holdsRun('r1')).toBe(false);

    const outcome = await B.forwarder.forward(cancel(1, { reason: 'operator' }), { awaitAppliedMs: 500 });
    await A.settle(); await B.settle();

    expect(A.runs.cancelled).toEqual(['r1']);
    expect(B.runs.cancelled).toEqual([]);
    expect(outcome.recorded).toBe(true);
    expect(outcome.applied).toMatchObject({ type: 'applied', controlId: 'c1', nodeId: 'node-a', ok: true });
    expect(outcome.appliedInMs).toBeGreaterThanOrEqual(0);
    expect(A.forwarder.stats).toMatchObject({ applied: 1, duplicates: 0 });
    expect(B.forwarder.stats).toMatchObject({ forwarded: 1, notHere: 1, applied: 0 });
    // The holder released it: the row no longer carries the control.
    expect((await table.get('deck-revise'))?.controls ?? []).toEqual([]);
    // What crossed the wire: the control (once) and the acknowledgement (once).
    expect(pubsub.published.map((m) => (m as { type: string }).type)).toEqual(['control', 'applied']);
  });

  it('a duplicate control (same controlId, retried on either replica) is ignored by the row and the holder', async () => {
    const table = new SharedTable();
    const pubsub = new SharedPubSub();
    const A = replica('node-a', table, pubsub);
    const B = replica('node-b', table, pubsub);
    await A.bus.emit('pipeline:run:started', { runId: 'r1', pipelineId: 'deck-revise', startedAt: t(1) }, 'e1');
    await A.settle();
    A.runs.hold('r1');

    await B.forwarder.forward(cancel(1), { awaitAppliedMs: 200 });
    await A.settle(); await B.settle();
    // The retry lands on A's route this time (A does not hold it any more: cancelled).
    A.runs.hold('r1'); // pretend the executor still lists it live so the duplicate reaches apply()
    const retry = await A.forwarder.forward(cancel(1), { awaitAppliedMs: 200 });
    await A.settle(); await B.settle();

    expect(A.runs.cancelled).toEqual(['r1']);
    expect(A.forwarder.stats.duplicates).toBe(1);
    expect(retry.applied).toMatchObject({ ok: true, duplicate: true, nodeId: 'node-a' });
    expect((await table.get('deck-revise'))?.controls ?? []).toEqual([]);
  });

  it('owner dead: the cancel stays on the row through `interrupted`, and the replica that resumes the run applies it on its `resumed`', async () => {
    const table = new SharedTable();
    const pubsub = new SharedPubSub();
    const A = replica('node-a', table, pubsub);
    const B = replica('node-b', table, pubsub);
    await A.bus.emit('pipeline:run:started', { runId: 'r1', pipelineId: 'deck-revise', startedAt: t(1) }, 'e1');
    await A.settle();
    A.runs.hold('r1');
    A.forwarder.stop(); // the owner is gone before the control arrives
    A.runs.runs.clear();

    const outcome = await B.forwarder.forward(cancel(1), { awaitAppliedMs: 100 });
    await B.settle();
    expect(outcome.applied).toBeNull();
    expect(outcome.recorded).toBe(true);
    expect((await table.get('deck-revise'))?.controls).toEqual([expect.objectContaining({ controlId: 'c1', kind: 'cancel', runId: 'r1' })]);

    // The leader writes `interrupted` (orphan scan): the control stays.
    await B.rollup.applyResolved({ pipelineId: 'deck-revise', runId: 'r1', status: 'interrupted', at: t(20) });
    expect((await table.get('deck-revise'))?.controls).toHaveLength(1);

    // B resumes the run: its executor emits `resumed`; the recorded cancel is applied at once.
    B.runs.hold('r1');
    await B.bus.emit('pipeline:run:resumed', { runId: 'r1', pipelineId: 'deck-revise' }, 'e2');
    await B.settle();
    expect(B.runs.cancelled).toEqual(['r1']);
    expect(B.forwarder.stats).toMatchObject({ pickedUp: 1, applied: 1 });
    expect((await table.get('deck-revise'))?.controls ?? []).toEqual([]);

    // A second `resumed` (replay) applies nothing.
    await B.bus.emit('pipeline:run:resumed', { runId: 'r1', pipelineId: 'deck-revise' }, 'e3');
    await B.settle();
    expect(B.runs.cancelled).toEqual(['r1']);
  });

  it('a breakpoint resolve taken on B is applied by A when the run is paused there; a recorded one waits for the pause', async () => {
    const table = new SharedTable();
    const pubsub = new SharedPubSub();
    const A = replica('node-a', table, pubsub);
    const B = replica('node-b', table, pubsub);
    await A.bus.emit('pipeline:run:started', { runId: 'r1', pipelineId: 'deck-revise', startedAt: t(1) }, 'e1');
    await A.settle();
    A.runs.hold('r1', 'paused_at_breakpoint');

    const live = await B.forwarder.forward({ kind: 'breakpoint', controlId: 'b1', pipelineId: 'deck-revise', runId: 'r1', userId: 'alice', at: t(11), stepId: 'write', action: 'continue' }, { awaitAppliedMs: 300 });
    await A.settle(); await B.settle();
    expect(A.runs.breakpoints).toEqual([{ runId: 'r1', stepId: 'write', action: 'continue' }]);
    expect(live.applied).toMatchObject({ ok: true, nodeId: 'node-a' });

    // Owner gone; the resolve is recorded and applied when the resumed run pauses at that step.
    A.forwarder.stop();
    A.runs.runs.clear();
    const later = await B.forwarder.forward({ kind: 'breakpoint', controlId: 'b2', pipelineId: 'deck-revise', runId: 'r1', userId: 'alice', at: t(12), stepId: 'check', action: 'step_over' }, { awaitAppliedMs: 50 });
    expect(later.applied).toBeNull();
    B.runs.hold('r1', 'running');
    await B.bus.emit('pipeline:run:resumed', { runId: 'r1' }, 'e2');
    await B.settle();
    expect(B.runs.breakpoints).toEqual([]); // not paused yet: nothing applied
    B.runs.hold('r1', 'paused_at_breakpoint');
    await B.bus.emit('pipeline:step:breakpoint', { runId: 'r1', stepId: 'other' }, 'e3');
    await B.settle();
    expect(B.runs.breakpoints).toEqual([]); // paused at another step: still waiting
    await B.bus.emit('pipeline:step:breakpoint', { runId: 'r1', stepId: 'check' }, 'e4');
    await B.settle();
    expect(B.runs.breakpoints).toEqual([{ runId: 'r1', stepId: 'check', action: 'step_over' }]);
    expect((await table.get('deck-revise'))?.controls ?? []).toEqual([]);
  });

  it('resume-from-step reaches the replica that ran the failed run; with no such replica nothing is acknowledged and the route releases it', async () => {
    const table = new SharedTable();
    const pubsub = new SharedPubSub();
    const A = replica('node-a', table, pubsub);
    const B = replica('node-b', table, pubsub);
    await A.bus.emit('pipeline:run:started', { runId: 'r1', pipelineId: 'deck-revise', startedAt: t(1) }, 'e1');
    await A.rollup.applyResolved({ pipelineId: 'deck-revise', runId: 'r1', status: 'failed', at: t(5) });
    await A.settle();
    A.runs.hold('r1', 'failed'); // finished here: A can rewind it, nobody else can

    expect(A.forwarder.knowsRun('r1')).toBe(true);
    expect(A.forwarder.holdsRun('r1')).toBe(false);
    const resume = { kind: 'resume-from-step' as const, controlId: 's1', pipelineId: 'deck-revise', runId: 'r1', userId: 'alice', at: t(11), fromNodeId: 'write', breakpoints: ['check'] };
    const outcome = await B.forwarder.forward(resume, { awaitAppliedMs: 300 });
    await A.settle(); await B.settle();
    expect(A.runs.resumed).toEqual([{ runId: 'r1', fromNodeId: 'write', options: { breakpoints: ['check'] } }]);
    expect(outcome.applied).toMatchObject({ ok: true, nodeId: 'node-a', kind: 'resume-from-step' });

    // A refuses (not resumable): the acknowledgement says so, the control is not released by the holder.
    A.runs.failResume = true;
    const refused = await B.forwarder.forward({ ...resume, controlId: 's2', at: t(12) }, { awaitAppliedMs: 300 });
    await A.settle(); await B.settle();
    expect(refused.applied).toMatchObject({ ok: false, error: expect.stringContaining('not resumable') });

    // Nobody has it: no acknowledgement; the route's release empties the row.
    A.forwarder.stop();
    const nobody = await B.forwarder.forward({ ...resume, controlId: 's3', at: t(13) }, { awaitAppliedMs: 50 });
    expect(nobody.applied).toBeNull();
    await B.forwarder.release('deck-revise', 'r1', 's3');
    await B.settle();
    expect(((await table.get('deck-revise'))?.controls ?? []).map((c) => c.controlId)).toEqual(['s2']);
  });

  it('a run that already ended takes no control, a failed publish still leaves the row as the record, and a malformed message is dropped', async () => {
    const table = new SharedTable();
    const pubsub = new SharedPubSub();
    const A = replica('node-a', table, pubsub);
    const B = replica('node-b', table, pubsub);
    await A.bus.emit('pipeline:run:started', { runId: 'r1', pipelineId: 'deck-revise', startedAt: t(1) }, 'e1');
    await A.rollup.applyResolved({ pipelineId: 'deck-revise', runId: 'r1', status: 'completed', at: t(5) });
    await A.settle();
    const ended = await B.forwarder.forward(cancel(1), { awaitAppliedMs: 0 });
    expect(ended.recorded).toBe(true); // the row simply did not change
    expect((await table.get('deck-revise'))?.controls ?? []).toEqual([]);

    await A.bus.emit('pipeline:run:started', { runId: 'r2', pipelineId: 'deck-revise', startedAt: t(6) }, 'e2');
    await A.settle();
    pubsub.failPublish = true;
    const unpublished = await B.forwarder.forward(cancel(2, { runId: 'r2' }), { awaitAppliedMs: 0 });
    expect(unpublished.recorded).toBe(true);
    expect(unpublished.applied).toBeNull();
    expect((await table.get('deck-revise'))?.controls).toEqual([expect.objectContaining({ controlId: 'c2', runId: 'r2' })]);
    pubsub.failPublish = false;

    A.runs.hold('r2');
    await pubsub.publish(RUN_CONTROLS_TOPIC, { v: 1, type: 'control', kind: 'cancel', runId: 'r2' });
    await pubsub.publish(RUN_CONTROLS_TOPIC, { v: 1, type: 'control', kind: 'teleport', controlId: 'x', runId: 'r2', userId: 'u', pipelineId: 'deck-revise', at: t(7), fromReplica: 'node-b' });
    await A.settle();
    expect(A.runs.cancelled).toEqual([]);
    expect(A.forwarder.stats).toMatchObject({ received: 1, notHere: 1, applied: 0 }); // the ended-run cancel was heard and not for A; the malformed ones were dropped
  });
});
