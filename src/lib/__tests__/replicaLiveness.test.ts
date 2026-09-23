// NFR #26 / #11 — boot sweeps under two replicas.
//
// Two "replicas" share one LeaseStore (the DDB table in production). The
// checks: exactly one of them sweeps; a planning loop or running run owned by
// the live replica is never failed/interrupted; one owned by a dead or
// restarted incarnation is; leadership moves when the leader stops.

import {
  ClusterSweeper,
  ReplicaHeartbeat,
  _resetLocalReplicaForTests,
  createMemoryLeaseStore,
  isOwnerLive,
  setLocalReplica,
} from '../replicaLiveness';
import { failOrphanedLoops, pendingRow, PENDING_PLAN } from '../../chat/agentLoopRunner';
import { scanOrphanedRuns } from '../../pipeline/scanOrphanedRuns';
import type { PipelineRunItem } from '../../repositories/PipelineRunRepository';

const LOOP_PIPELINE = 'chat-agent-loop';

afterEach(() => _resetLocalReplicaForTests());

function planningLoop(loopId: string, plannerNodeId: string | undefined, startedAt: Date): PipelineRunItem {
  const row = pendingRow(
    loopId,
    { instruction: 'do it', requestedBy: 'dev-frank', chatChannel: 'c1' } as never,
    startedAt,
  ) as PipelineRunItem;
  const ctx = row.checkpoint!.context as Record<string, unknown>;
  if (plannerNodeId) ctx.plannerNodeId = plannerNodeId;
  else delete ctx.plannerNodeId;
  ctx.plan = PENDING_PLAN;
  return row;
}

describe('ClusterSweeper — two contenders on one lease', () => {
  it('only one replica sweeps per tick; the other takes over when it stops', async () => {
    const store = createMemoryLeaseStore();
    const runs = { a: 0, b: 0 };
    const a = new ClusterSweeper({ store, ownerId: 'A', tasks: [{ name: 't', run: async () => { runs.a++; } }] });
    const b = new ClusterSweeper({ store, ownerId: 'B', tasks: [{ name: 't', run: async () => { runs.b++; } }] });

    // Both boot at once.
    const [ra, rb] = await Promise.all([a.tick(), b.tick()]);
    expect([ra, rb].filter(Boolean)).toHaveLength(1);
    for (let i = 0; i < 5; i++) await Promise.all([a.tick(), b.tick()]);
    expect(runs.a + runs.b).toBe(6);
    expect(Math.min(runs.a, runs.b)).toBe(0);

    const leader = runs.a ? a : b;
    const follower = runs.a ? b : a;
    await leader.stop();
    expect(await follower.tick()).toBe(true);
    await follower.stop();
  });
});

describe('isOwnerLive — incarnation, not just node id', () => {
  it('live lease + booted before the work → live; restarted or expired → dead', async () => {
    let now = 1_000_000;
    const clock = () => now;
    const store = createMemoryLeaseStore(clock);
    const hbA = new ReplicaHeartbeat({ store, nodeId: 'node-a', bootedAtMs: now, ttlMs: 30_000, now: clock });
    await hbA.start();

    expect(await isOwnerLive(store, 'node-a', now + 5, clock)).toBe(true);
    // Work that started before this incarnation booted belongs to a dead one.
    expect(await isOwnerLive(store, 'node-a', now - 5, clock)).toBe(false);
    expect(await isOwnerLive(store, 'node-b', now + 5, clock)).toBe(false);
    expect(await isOwnerLive(store, undefined, now + 5, clock)).toBe(false);

    await hbA.stop(); // graceful stop releases the row
    expect(await isOwnerLive(store, 'node-a', now + 5, clock)).toBe(false);

    // Crash (no release): the row lingers until its TTL, then reads as dead.
    store.rows.set('platform-replica#node-a', { owner: `node-a@${now}`, expiresAt: now + 30_000 });
    expect(await isOwnerLive(store, 'node-a', now + 5, clock)).toBe(true);
    now += 31_000;
    expect(await isOwnerLive(store, 'node-a', now - 30_000, clock)).toBe(false);
  });

  it('a failing store read keeps the loop (unknown is not dead)', async () => {
    const store = createMemoryLeaseStore();
    store.read = async () => { throw new Error('ddb down'); };
    expect(await isOwnerLive(store, 'node-a', Date.now())).toBe(true);
  });
});

describe('rolling restart with two replicas', () => {
  it('B booting fails none of A\'s planning loops or runs; a dead incarnation\'s are swept once', async () => {
    const store = createMemoryLeaseStore();
    const t0 = Date.now();

    // Replica A is up and planning a loop + executing a run.
    const hbA = new ReplicaHeartbeat({ store, nodeId: 'node-a', bootedAtMs: t0 - 60_000 });
    await hbA.start();
    // Replica C crashed earlier: its lease is gone, its work is orphaned.
    const liveLoop = planningLoop('loop-live', 'node-a', new Date(t0 - 10_000));
    const deadLoop = planningLoop('loop-dead', 'node-c', new Date(t0 - 10_000));
    const legacyLoop = planningLoop('loop-legacy', undefined, new Date(t0 - 10_000));
    const liveRun = { pipelineId: 'p', runId: 'run-live', status: 'running', triggeredBy: 'u', triggerType: 'manual', startedAt: new Date(t0 - 5_000).toISOString(), ownerNodeId: 'node-a' } as PipelineRunItem;
    const deadRun = { ...liveRun, runId: 'run-dead', ownerNodeId: 'node-c' } as PipelineRunItem;

    const rows = new Map<string, PipelineRunItem>([liveLoop, deadLoop, legacyLoop, liveRun, deadRun].map((r) => [r.runId, r]));
    const repo = {
      listByStatus: async (status: string) => [...rows.values()].filter((r) => r.status === status),
      put: async (item: PipelineRunItem) => { rows.set(item.runId, item); },
    };
    let loopSweeps = 0;
    let scanSweeps = 0;

    // Replica B boots (and A is sweeping too): both run the cluster sweeper.
    setLocalReplica('node-b', t0);
    const tasks = [
      {
        name: 'orphaned-agent-loops',
        run: async () => {
          loopSweeps++;
          await failOrphanedLoops({
            listPending: async () => (await repo.listByStatus('pending')).filter((r) => r.pipelineId === LOOP_PIPELINE),
            putRun: repo.put,
            isPlannerLive: (owner, since) => isOwnerLive(store, owner, since),
          });
        },
      },
      {
        name: 'orphan-scan',
        run: async () => {
          scanSweeps++;
          await scanOrphanedRuns({
            repository: repo as never,
            isOwnerLive: (item) => isOwnerLive(store, item.ownerNodeId, Date.parse(item.resumedAt ?? item.startedAt)),
          });
        },
      },
    ];
    const sweeperA = new ClusterSweeper({ store, ownerId: 'A', tasks });
    const sweeperB = new ClusterSweeper({ store, ownerId: 'B', tasks });
    await Promise.all([sweeperA.tick(), sweeperB.tick()]);
    await Promise.all([sweeperA.tick(), sweeperB.tick()]);

    // Each sweep ran once per tick cluster-wide, not once per replica.
    expect(loopSweeps).toBe(2);
    expect(scanSweeps).toBe(2);

    expect(rows.get('loop-live')!.status).toBe('pending');
    expect(rows.get('run-live')!.status).toBe('running');
    expect(rows.get('loop-dead')!.status).toBe('failed');
    expect(rows.get('loop-legacy')!.status).toBe('failed');
    expect(rows.get('run-dead')!.status).toBe('interrupted');

    await Promise.all([sweeperA.stop(), sweeperB.stop(), hbA.stop()]);
  });

  it('pendingRow stamps the planning replica', () => {
    setLocalReplica('node-z', 1);
    const row = pendingRow('l1', { instruction: 'x', requestedBy: 'u', chatChannel: 'c' } as never, new Date());
    expect((row.checkpoint!.context as Record<string, unknown>).plannerNodeId).toBe('node-z');
  });
});
