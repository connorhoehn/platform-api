// NFR #115: an approval decision taken on the replica that does not own the
// run reaches the owner — over the cluster pubsub while the owner lives, off
// the rollup row when the run is resumed elsewhere — and is applied once.

import type { EventBus } from 'distributed-core';
import type { PipelineEventMap } from 'distributed-core/applications/pipeline';
import { ApprovalForwarder, APPROVAL_DECISIONS_TOPIC, type ClusterPubSubLike } from '../approval-forwarder';
import { RunRollupSubscriber } from '../run-rollup-subscriber';
import { RollupVersionConflict } from '../../repositories/PipelineRunRollupRepository';
import type { PipelineRunRollup } from '../run-rollup';

jest.mock('../../lib/logger', () => ({
  withContext: () => ({ info: jest.fn(), warn: jest.fn(), error: jest.fn(), debug: jest.fn() }),
}));

type Handler = (event: { id?: string; payload: unknown }) => void | Promise<void>;

/** One replica's in-process dc EventBus (per process, like the real one). */
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
  subscribe(topic: string, handler: (topic: string, payload: unknown) => void): string {
    const id = `ps-${this.nextId++}`;
    this.subs.set(id, { topic, handler });
    return id;
  }
  unsubscribe(id: string): boolean { return this.subs.delete(id); }
  async publish(topic: string, payload: unknown): Promise<void> {
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

/** The executor side of one replica: the gates it holds and the decisions it took. */
class StubRuns {
  gates = new Set<string>();
  resolved: Array<{ runId: string; stepId: string; userId: string; decision: string; comment?: string }> = [];
  hold(runId: string, stepId: string): void { this.gates.add(`${runId}/${stepId}`); }
  getPendingApprovals() {
    return [...this.gates].map((k) => { const [runId, stepId] = k.split('/'); return { runId, stepId }; });
  }
  async resolveApproval(runId: string, stepId: string, userId: string, decision: 'approve' | 'reject', comment?: string): Promise<void> {
    this.resolved.push({ runId, stepId, userId, decision, ...(comment !== undefined ? { comment } : {}) });
    // The executor releases the gate; the real one also emits `recorded`.
    this.gates.delete(`${runId}/${stepId}`);
  }
}

const t = (s: number) => `2026-09-23T16:00:${String(s).padStart(2, '0')}.000Z`;

function replica(nodeId: string, table: SharedTable, pubsub: SharedPubSub) {
  const bus = new StubBus();
  const rollup = new RunRollupSubscriber({
    eventBus: bus as unknown as EventBus<PipelineEventMap>,
    repo: table,
    lookupPipelineId: async (runId) => (runId.startsWith('r') ? 'deck-revise' : null),
  });
  rollup.start();
  const runs = new StubRuns();
  const forwarder = new ApprovalForwarder({
    nodeId,
    pubsub,
    runs,
    eventBus: bus as unknown as EventBus<PipelineEventMap>,
    rollup,
    repo: table,
  });
  forwarder.start();
  const settle = async () => { await rollup.idle(); await forwarder.idle(); await rollup.idle(); };
  return { bus, rollup, runs, forwarder, settle };
}

const decision = (n: number, over: Partial<{ userId: string; decision: 'approve' | 'reject'; stepId: string; comment: string }> = {}) => ({
  decisionId: `d${n}`, pipelineId: 'deck-revise', runId: 'r1', stepId: 'gate', userId: 'alice', decision: 'approve' as const, at: t(10 + n), ...over,
});

describe('ApprovalForwarder — two replicas on one bus', () => {
  it('a decision taken on B is applied by A, the owner, exactly once; the row holds it until A records the gate', async () => {
    const table = new SharedTable();
    const pubsub = new SharedPubSub();
    const A = replica('node-a', table, pubsub);
    const B = replica('node-b', table, pubsub);
    // A owns r1 and its executor asks for the gate.
    await A.bus.emit('pipeline:run:started', { runId: 'r1', pipelineId: 'deck-revise', triggeredBy: {}, at: t(1) }, 'e1');
    await A.bus.emit('pipeline:approval:requested', { runId: 'r1', stepId: 'gate', approvers: [], at: t(2) }, 'e2');
    A.runs.hold('r1', 'gate');
    await A.settle(); await B.settle();
    expect(table.rows.get('deck-revise')?.pendingApprovals).toBe(1);
    expect(B.forwarder.holdsGate('r1', 'gate')).toBe(false);

    // B takes the decision.
    await B.forwarder.forward(decision(1, { comment: 'ship it' }));
    await A.settle(); await B.settle();

    expect(A.runs.resolved).toEqual([{ runId: 'r1', stepId: 'gate', userId: 'alice', decision: 'approve', comment: 'ship it' }]);
    expect(B.runs.resolved).toEqual([]);
    expect(A.forwarder.stats).toMatchObject({ received: 1, applied: 1, duplicates: 0 });
    expect(B.forwarder.stats).toMatchObject({ forwarded: 1, received: 1, notHere: 1, applied: 0 });
    // Recorded on the shared row before the wire, so every replica's badge sees it.
    expect(table.rows.get('deck-revise')?.decisions).toEqual([expect.objectContaining({ decisionId: 'd1', runId: 'r1', stepId: 'gate' })]);

    // A's executor records the gate: the row releases both the gate and the decision.
    await A.bus.emit('pipeline:approval:recorded', { runId: 'r1', stepId: 'gate', userId: 'alice', decision: 'approve', at: t(12) }, 'e3');
    await A.settle();
    const row = table.rows.get('deck-revise')!;
    expect(row.pendingApprovals).toBe(0);
    expect(row.decisions).toBeUndefined();
    expect(row.lastRunStatus).toBe('running');
  });

  it('a duplicate decision (same decisionId, retried on either replica) is ignored by the row and the owner', async () => {
    const table = new SharedTable();
    const pubsub = new SharedPubSub();
    const A = replica('node-a', table, pubsub);
    const B = replica('node-b', table, pubsub);
    await A.bus.emit('pipeline:run:started', { runId: 'r1', pipelineId: 'deck-revise', triggeredBy: {}, at: t(1) }, 'e1');
    await A.bus.emit('pipeline:approval:requested', { runId: 'r1', stepId: 'gate', approvers: [], at: t(2) }, 'e2');
    A.runs.hold('r1', 'gate');
    await A.settle();

    await B.forwarder.forward(decision(1));
    // The executor released the gate on the first apply; a second copy of the
    // same decision must not be handed to it again — hold the gate again to
    // prove the dedupe is the forwarder's, not the executor's.
    await A.settle(); await B.settle();
    A.runs.hold('r1', 'gate');
    await B.forwarder.forward(decision(1));
    await A.forwarder.forward(decision(1));
    await A.settle(); await B.settle();

    expect(A.runs.resolved).toHaveLength(1);
    expect(A.forwarder.stats).toMatchObject({ applied: 1, duplicates: 2 });
    expect(table.rows.get('deck-revise')?.decisions).toHaveLength(1);
    // A second, distinct decision on the same gate is a new id and applies.
    await B.forwarder.forward(decision(2, { userId: 'bob' }));
    await A.settle(); await B.settle();
    expect(A.runs.resolved).toHaveLength(2);
    expect(table.rows.get('deck-revise')?.decisions?.map((d) => d.decisionId)).toEqual(['d1', 'd2']);
  });

  it('owner dead: the decision stays on the row through `interrupted`, and the replica that resumes the run applies it when its executor asks for the gate', async () => {
    const table = new SharedTable();
    const pubsub = new SharedPubSub();
    const A = replica('node-a', table, pubsub);
    const B = replica('node-b', table, pubsub);
    await A.bus.emit('pipeline:run:started', { runId: 'r1', pipelineId: 'deck-revise', triggeredBy: {}, at: t(1) }, 'e1');
    await A.bus.emit('pipeline:approval:requested', { runId: 'r1', stepId: 'gate', approvers: [], at: t(2) }, 'e2');
    A.runs.hold('r1', 'gate');
    await A.settle();
    // A dies: nothing it holds answers any more.
    A.forwarder.stop();
    A.rollup.stop();
    A.runs.gates.clear();

    await B.forwarder.forward(decision(1, { decision: 'reject' }));
    await B.settle();
    expect(B.forwarder.stats).toMatchObject({ forwarded: 1, notHere: 1, applied: 0 });
    expect(table.rows.get('deck-revise')?.decisions).toHaveLength(1);

    // The sweep leader marks the run interrupted: the gate is released, the decision is not.
    await B.rollup.applyResolved({ pipelineId: 'deck-revise', runId: 'r1', status: 'interrupted', at: t(20) });
    let row = table.rows.get('deck-revise')!;
    expect(row.pendingApprovals).toBe(0);
    expect(row.decisions).toHaveLength(1);

    // B resumes the run from its checkpoint; its executor re-asks the gate.
    await B.bus.emit('pipeline:run:resumed', { runId: 'r1', pipelineId: 'deck-revise', at: t(21) }, 'e3');
    B.runs.hold('r1', 'gate');
    await B.bus.emit('pipeline:approval:requested', { runId: 'r1', stepId: 'gate', approvers: [], at: t(22) }, 'e4');
    await B.settle();

    expect(B.runs.resolved).toEqual([{ runId: 'r1', stepId: 'gate', userId: 'alice', decision: 'reject' }]);
    expect(B.forwarder.stats).toMatchObject({ applied: 1, pickedUp: 1 });
    // The executor records it; the row is clean.
    await B.bus.emit('pipeline:approval:recorded', { runId: 'r1', stepId: 'gate', userId: 'alice', decision: 'reject', at: t(23) }, 'e5');
    await B.settle();
    row = table.rows.get('deck-revise')!;
    expect(row.decisions).toBeUndefined();
    expect(row.pendingApprovals).toBe(0);
  });

  it('a run that already ended takes no decision, and a failed publish still leaves the row as the record', async () => {
    const table = new SharedTable();
    const pubsub = new SharedPubSub();
    const B = replica('node-b', table, pubsub);
    await B.rollup.applyResolved({ pipelineId: 'deck-revise', runId: 'r1', status: 'completed', at: t(5), startedAt: t(1), completedAt: t(5) });
    await B.forwarder.forward(decision(1));
    expect(table.rows.get('deck-revise')?.decisions).toBeUndefined();

    await B.rollup.applyResolved({ pipelineId: 'deck-revise', runId: 'r2', status: 'awaiting_approval', at: t(7), startedAt: t(6), approval: { stepId: 'gate', pending: true } });
    pubsub.publish = async () => { throw new Error('redis down'); };
    await expect(B.forwarder.forward({ ...decision(2), runId: 'r2' })).resolves.toMatchObject({ decisionId: 'd2', fromNodeId: 'node-b' });
    expect(table.rows.get('deck-revise')?.decisions).toEqual([expect.objectContaining({ decisionId: 'd2', runId: 'r2' })]);
    expect(pubsub.published.filter((m) => (m as { runId: string }).runId === 'r2')).toHaveLength(0);
  });

  it('a malformed message on the topic is dropped', async () => {
    const table = new SharedTable();
    const pubsub = new SharedPubSub();
    const A = replica('node-a', table, pubsub);
    A.runs.hold('r1', 'gate');
    await pubsub.publish(APPROVAL_DECISIONS_TOPIC, { v: 1, runId: 'r1', stepId: 'gate' });
    await pubsub.publish(APPROVAL_DECISIONS_TOPIC, 'nope');
    await A.settle();
    expect(A.runs.resolved).toEqual([]);
    expect(A.forwarder.stats).toMatchObject({ received: 0, applied: 0 });
  });
});
