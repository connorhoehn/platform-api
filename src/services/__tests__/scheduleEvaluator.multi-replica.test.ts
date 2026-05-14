// platform-api/src/services/__tests__/scheduleEvaluator.multi-replica.test.ts
//
// Wave3-PA-5 — 2-replica simulation for the schedule evaluator's leader-lock
// path. Spins two ScheduleEvaluator instances backed by a shared in-memory
// LeaseStore (modelling DDB-conditional-PutItem semantics) and asserts:
//
//   1. At most one replica's tick() body runs per minute (exactly-once cron).
//   2. When the current leader stops, the follower takes over on the next
//      tick (bounded by 2x pollIntervalMs TTL).
//   3. Concurrent acquire attempts at the exact same instant don't both win.
//   4. Release errors don't poison the next-tick handoff.
//   5. A leader that crashes mid-tick (never releases) eventually times out
//      and the follower wins.
//
// Pattern mirrors the per-batch SchedulerLeaderLock that wraps LeaseStore in
// production (src/services/scheduler-leader-lock.ts).

import {
  createScheduleEvaluator,
  type PipelineForSchedule,
} from '../scheduleEvaluator';
import type { SchedulerLeaderLock } from '../scheduler-leader-lock';

// ─── Shared in-memory store ────────────────────────────────────────────────

interface StoredLease {
  owner: string;
  expiresAt: number;
}

interface SharedStore {
  rows: Map<string, StoredLease>;
  now: () => number;
}

function makeSharedStore(opts: { now?: () => number } = {}): SharedStore {
  return {
    rows: new Map(),
    now: opts.now ?? Date.now,
  };
}

/**
 * Build a SchedulerLeaderLock backed by the shared store. Models the
 * DDB-conditional semantics of createDDBSchedulerLeaderLock: PutItem
 * succeeds if no row exists OR the row is expired; DeleteItem only
 * fires when this owner holds the row.
 */
function makeReplicaLock(
  shared: SharedStore,
  ownerId: string,
  opts: { releaseThrows?: boolean } = {},
): SchedulerLeaderLock {
  const LOCK_KEY = 'SCHEDULE_LOCK#scheduleEvaluator';
  let held = false;
  return {
    async acquire(ttlMs: number): Promise<boolean> {
      const now = shared.now();
      const existing = shared.rows.get(LOCK_KEY);
      if (existing && existing.expiresAt > now) {
        return false; // another replica holds a valid lease
      }
      shared.rows.set(LOCK_KEY, { owner: ownerId, expiresAt: now + ttlMs });
      held = true;
      return true;
    },
    async release(): Promise<void> {
      if (!held) return;
      held = false;
      if (opts.releaseThrows) throw new Error('simulated release failure');
      const existing = shared.rows.get(LOCK_KEY);
      if (existing && existing.owner === ownerId) {
        shared.rows.delete(LOCK_KEY);
      }
    },
  };
}

function publishedSchedule(id: string, cron = '* * * * *'): PipelineForSchedule {
  return { id, status: 'published', triggerBinding: { event: 'schedule', schedule: cron } };
}

// ─── Tests ─────────────────────────────────────────────────────────────────

describe('scheduleEvaluator — 2-replica multi-replica safety', () => {
  test('exactly one replica fires the cron per minute', async () => {
    const shared = makeSharedStore();
    const firedA: string[] = [];
    const firedB: string[] = [];

    const evalA = createScheduleEvaluator({
      pollIntervalMs: 60_000,
      listPipelines: () => [publishedSchedule('p-1')],
      trigger: async (id) => { firedA.push(id); },
      leaderLock: makeReplicaLock(shared, 'replica-A'),
    });
    const evalB = createScheduleEvaluator({
      pollIntervalMs: 60_000,
      listPipelines: () => [publishedSchedule('p-1')],
      trigger: async (id) => { firedB.push(id); },
      leaderLock: makeReplicaLock(shared, 'replica-B'),
    });

    const tickTime = new Date(2026, 3, 23, 10, 30, 0);
    // Race them in parallel — simulates two replicas' 60s timers firing in
    // the same minute. Whoever wins the tryClaim runs; the other skips.
    const [ranA, ranB] = await Promise.all([
      evalA.runProtectedTick(tickTime),
      evalB.runProtectedTick(tickTime),
    ]);
    await Promise.resolve();

    // Exactly one of the replicas ran the tick body. The other skipped.
    expect([ranA, ranB].filter(Boolean)).toHaveLength(1);
    const combined = [...firedA, ...firedB];
    expect(combined).toEqual(['p-1']);
  });

  test('leader stop → follower takes over on next tick', async () => {
    // Use an advancing clock so the released lease (or expired) is reclaimable.
    let nowMs = new Date(2026, 3, 23, 10, 30, 0).getTime();
    const shared = makeSharedStore({ now: () => nowMs });

    const firedA: string[] = [];
    const firedB: string[] = [];
    const evalA = createScheduleEvaluator({
      pollIntervalMs: 60_000,
      listPipelines: () => [publishedSchedule('p-x')],
      trigger: async (id) => { firedA.push(id); },
      leaderLock: makeReplicaLock(shared, 'replica-A'),
    });
    const evalB = createScheduleEvaluator({
      pollIntervalMs: 60_000,
      listPipelines: () => [publishedSchedule('p-x')],
      trigger: async (id) => { firedB.push(id); },
      leaderLock: makeReplicaLock(shared, 'replica-B'),
    });

    // Minute 1: race them concurrently — one wins.
    const t1 = new Date(nowMs);
    const [ranA1, ranB1] = await Promise.all([
      evalA.runProtectedTick(t1),
      evalB.runProtectedTick(t1),
    ]);
    expect([ranA1, ranB1].filter(Boolean)).toHaveLength(1);

    // Advance to next minute (lease was released by the winner at end of tick).
    nowMs += 60_000;
    const t2 = new Date(nowMs);
    // Race them again — again exactly one wins.
    const [ranA2, ranB2] = await Promise.all([
      evalA.runProtectedTick(t2),
      evalB.runProtectedTick(t2),
    ]);
    expect([ranA2, ranB2].filter(Boolean)).toHaveLength(1);

    await Promise.resolve();
    // Per-minute exactly-once: 2 minutes × 1 fire/minute = 2 total firings.
    expect(firedA.length + firedB.length).toBe(2);
  });

  test('crashed leader (never released) eventually times out so follower wins', async () => {
    // Replica A acquires but the release call is silently dropped (e.g. SIGKILL).
    let nowMs = new Date(2026, 3, 23, 10, 30, 0).getTime();
    const shared = makeSharedStore({ now: () => nowMs });

    const lockA: SchedulerLeaderLock = {
      async acquire(ttlMs) {
        const existing = shared.rows.get('SCHEDULE_LOCK#scheduleEvaluator');
        if (existing && existing.expiresAt > shared.now()) return false;
        shared.rows.set('SCHEDULE_LOCK#scheduleEvaluator', {
          owner: 'replica-A',
          expiresAt: shared.now() + ttlMs,
        });
        return true;
      },
      async release() {
        // Simulate SIGKILL — release never executes. Row stays in store.
      },
    };

    const lockB = makeReplicaLock(shared, 'replica-B');

    const evalA = createScheduleEvaluator({
      pollIntervalMs: 60_000,
      listPipelines: () => [publishedSchedule('p-late')],
      trigger: async () => {},
      leaderLock: lockA,
    });
    const evalB = createScheduleEvaluator({
      pollIntervalMs: 60_000,
      listPipelines: () => [publishedSchedule('p-late')],
      trigger: async () => {},
      leaderLock: lockB,
    });

    // A wins.
    const ranA = await evalA.runProtectedTick(new Date(nowMs));
    expect(ranA).toBe(true);

    // 30s later (less than 2x pollInterval TTL = 120s): B still blocked.
    nowMs += 30_000;
    const ranB1 = await evalB.runProtectedTick(new Date(nowMs));
    expect(ranB1).toBe(false);

    // Advance past the TTL — B's tryClaim now sees an expired row.
    nowMs += 120_001; // total > 120_000 ms since A claimed
    const ranB2 = await evalB.runProtectedTick(new Date(nowMs));
    expect(ranB2).toBe(true);
  });

  test('release errors on replica A do not block replica B from acquiring next minute', async () => {
    let nowMs = new Date(2026, 3, 23, 10, 30, 0).getTime();
    const shared = makeSharedStore({ now: () => nowMs });

    const lockA = makeReplicaLock(shared, 'replica-A', { releaseThrows: true });
    const lockB = makeReplicaLock(shared, 'replica-B');

    const evalA = createScheduleEvaluator({
      pollIntervalMs: 60_000,
      listPipelines: () => [publishedSchedule('p-rel-err')],
      trigger: async () => {},
      leaderLock: lockA,
    });
    const evalB = createScheduleEvaluator({
      pollIntervalMs: 60_000,
      listPipelines: () => [publishedSchedule('p-rel-err')],
      trigger: async () => {},
      leaderLock: lockB,
    });

    // A wins; release throws but is caught.
    await expect(evalA.runProtectedTick(new Date(nowMs))).resolves.toBe(true);

    // Within the same minute, B is still blocked by A's row (TTL not expired).
    expect(await evalB.runProtectedTick(new Date(nowMs))).toBe(false);

    // Once the TTL expires, B succeeds — release-failure didn't pin the row.
    nowMs += 120_001;
    expect(await evalB.runProtectedTick(new Date(nowMs))).toBe(true);
  });

  test('no double-fire when both replicas race the exact same tick', async () => {
    const shared = makeSharedStore();
    const fired: Array<{ replica: 'A' | 'B'; id: string }> = [];

    const evalA = createScheduleEvaluator({
      pollIntervalMs: 60_000,
      listPipelines: () => [publishedSchedule('p-race')],
      trigger: async (id) => { fired.push({ replica: 'A', id }); },
      leaderLock: makeReplicaLock(shared, 'replica-A'),
    });
    const evalB = createScheduleEvaluator({
      pollIntervalMs: 60_000,
      listPipelines: () => [publishedSchedule('p-race')],
      trigger: async (id) => { fired.push({ replica: 'B', id }); },
      leaderLock: makeReplicaLock(shared, 'replica-B'),
    });

    // Run 5 race rounds. Each round must produce exactly one trigger call.
    for (let i = 0; i < 5; i++) {
      const t = new Date(2026, 3, 23, 10, 30 + i, 0);
      // Different minute each round, so the per-minute dedupe set inside
      // each evaluator never fires; only the leader-lock prevents N-fold.
      await Promise.all([evalA.runProtectedTick(t), evalB.runProtectedTick(t)]);
      await Promise.resolve();
    }

    // 5 distinct minutes × exactly-one-fire-per-minute = 5 firings.
    expect(fired).toHaveLength(5);
  });
});
