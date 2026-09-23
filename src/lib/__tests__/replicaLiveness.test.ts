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
  ownerLiveness,
  REPLICA_LEASE_TTL_MS,
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

describe('NFR #59 — resume follows the owner\'s lease, not the sweep interval', () => {
  it('the default lease is 10 s (was 30 s)', () => {
    expect(REPLICA_LEASE_TTL_MS).toBe(10_000);
  });

  it('ownerLiveness says when a live peer\'s lease lapses; none for ourselves or the dead', async () => {
    const now = 2_000_000;
    const store = createMemoryLeaseStore(() => now);
    store.rows.set('platform-replica#node-a', { owner: `node-a@${now - 100}`, expiresAt: now + 7_000 });
    expect(await ownerLiveness(store, 'node-a', now, () => now)).toEqual({ live: true, recheckAt: now + 7_000 });
    expect(await ownerLiveness(store, 'node-b', now, () => now)).toEqual({ live: false });
    setLocalReplica('node-self', now - 10);
    expect(await ownerLiveness(store, 'node-self', now, () => now)).toEqual({ live: true });
  });

  it('wakeAt runs only the named task at the lease expiry, and a follower claims within a third of a TTL', async () => {
    jest.useFakeTimers();
    try {
      const store = createMemoryLeaseStore();
      const runs: string[] = [];
      const tasks = [
        { name: 'orphan-scan-and-resume', run: async () => { runs.push('orphan'); } },
        { name: 'deck-blob-sweep', run: async () => { runs.push('deck'); } },
      ];
      const leader = new ClusterSweeper({ store, ownerId: 'A', tasks, intervalMs: 60_000, ttlMs: 9_000 });
      leader.start();
      await jest.advanceTimersByTimeAsync(0);
      expect(runs).toEqual(['orphan', 'deck']);
      leader.wakeAt(Date.now() + 5_000, 'orphan-scan-and-resume');
      leader.wakeAt(Date.now() + 8_000, 'orphan-scan-and-resume'); // later: ignored
      await jest.advanceTimersByTimeAsync(5_000);
      expect(runs).toEqual(['orphan', 'deck', 'orphan']);
      await jest.advanceTimersByTimeAsync(5_000);
      expect(runs).toHaveLength(3); // no second wake, interval not yet due

      await leader.stop();
      // A leader that died without releasing: its lease lapses 9 s from now.
      store.rows.set('platform-sweep-leader', { owner: 'A-dead', expiresAt: Date.now() + 9_000 });
      const follower = new ClusterSweeper({ store, ownerId: 'B', tasks: [{ name: 'orphan-scan-and-resume', run: async () => { runs.push('B'); } }], intervalMs: 60_000, ttlMs: 9_000 });
      follower.start();
      await jest.advanceTimersByTimeAsync(0);
      expect(runs).not.toContain('B'); // A's lease still held
      await jest.advanceTimersByTimeAsync(12_000); // lease gone + at most one claim retry (3 s)
      expect(runs).toContain('B');
      await follower.stop();
    } finally {
      jest.useRealTimers();
    }
  });
});
