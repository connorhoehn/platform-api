// platform-api/src/services/scheduler-fenced-leadership.ts
//
// ROADMAP P2.1 (lane P1, 2026-10-14) — single-flight schedule evaluation
// across replicas on distributed-core 0.48's FencedLease.
//
// Why the old per-tick lock was not enough
// ----------------------------------------
// `scheduler-leader-lock.ts` claimed a DynamoDB row at the start of each tick
// and released it at the end. Two replicas whose 60 s timers are offset by a
// few seconds therefore BOTH win, one after the other, in the same minute;
// each replica's in-memory `fired` set only dedupes its own fires, so every
// matching cron pipeline ran twice. A failover in the middle of a minute had
// the same effect.
//
// What this does
// --------------
// 1. Leadership is one FencedLease on `<key>` held continuously (renewed every
//    leaseMs/3). Only the replica whose lease is live evaluates schedules. A
//    lease that cannot prove ownership (renewal refused, reply past the
//    deadline, connection lost) fences permanently; the next tick on any
//    replica builds a fresh lease and takes over once Redis expires the key,
//    so failover is bounded by leaseMs + one tick.
// 2. Every fire is claimed at the side effect with one atomic script:
//    "the lease key still names me, and `<key>:fired:<pipeline>:<minute>` was
//    not set yet". The marker outlives the leader, so a successor that ticks
//    in the minute its predecessor already fired skips it, and a stale leader
//    that wakes after its key was taken is refused (-1) even if its local
//    view still reports live. That is the "no double fire across a failover"
//    guarantee; it does not depend on clocks or timer alignment.
//
// Missed minutes are not replayed: a minute that falls entirely inside a
// failover gap does not fire (at-most-once per pipeline per minute, the same
// guarantee cron gives).

import { randomUUID } from 'node:crypto';
import {
  createBoundedRedis,
  FencedLease,
  LeaseHeldError,
  type BoundedRedis,
  type LeaseRedis,
} from 'distributed-core/cluster/leases';
import log from '../lib/logger';
import {
  recordSchedulerLeaderAcquired,
  recordSchedulerLeaderError,
  recordSchedulerLeaderSkipped,
} from '../observability/metrics';

/** KEYS[1]=lease key, KEYS[2]=fire marker; ARGV[1]=owner, ARGV[2]=marker value, ARGV[3]=marker ttl ms → 1 claimed, 0 already fired, -1 not the owner. */
export const SCHEDULE_CLAIM_SCRIPT =
  "if redis.call('GET',KEYS[1]) ~= ARGV[1] then return -1 end\n" +
  "if redis.call('SET',KEYS[2],ARGV[2],'NX','PX',ARGV[3]) then return 1 end\n" +
  'return 0';

export interface SchedulerLeadership {
  /** True when this replica holds a live lease, acquiring a fresh one when the key is free. Never throws. */
  ensureLeader(): Promise<boolean>;
  /** Claim the single fire of `pipelineId` for `minuteEpoch`. False when already fired anywhere or this replica no longer owns the lease. Never throws. */
  claimFire(pipelineId: string, minuteEpoch: number): Promise<boolean>;
  /** True while the current lease is live (no Redis round trip). */
  isLeader(): boolean;
  /** Release the lease (compare-and-delete) so a peer takes over without waiting for expiry. */
  stop(): Promise<void>;
}

export interface FencedSchedulerLeadershipOptions {
  /** The Redis port, or a getter that may return null while Redis is unreachable. */
  redis: LeaseRedis | (() => Promise<LeaseRedis | null>);
  /** Lease key. Default `platform-api:schedule-evaluator:leader`. */
  key?: string;
  /** Default 30 000. Failover is bounded by this plus one poll interval. */
  leaseMs?: number;
  /** Fire-marker lifetime. Default 15 minutes (must exceed any tick skew). */
  markerTtlMs?: number;
  /** Owner prefix for operators (hostname-pid). A random suffix keeps each lease unique. */
  ownerPrefix?: string;
  /** Monotonic ms for the lease deadline; tests pass Date.now under fake timers. */
  now?: () => number;
}

export function createFencedSchedulerLeadership(
  opts: FencedSchedulerLeadershipOptions,
): SchedulerLeadership {
  const key = opts.key ?? 'platform-api:schedule-evaluator:leader';
  const leaseMs = opts.leaseMs ?? 30_000;
  const markerTtlMs = opts.markerTtlMs ?? 15 * 60_000;
  const ownerPrefix = opts.ownerPrefix ?? `${process.env.HOSTNAME ?? 'localhost'}-${process.pid}`;
  const getRedis = typeof opts.redis === 'function'
    ? opts.redis
    : (() => { const r = opts.redis as LeaseRedis; return async () => r; })();

  let lease: FencedLease | null = null;
  let leaseRedis: LeaseRedis | null = null;
  let acquiring: Promise<boolean> | null = null;
  let stopped = false;

  const dropLease = (): void => {
    const old = lease;
    lease = null;
    leaseRedis = null;
    // A fenced lease never revives; hand the key back if it is still ours.
    if (old) void old.release();
  };

  async function acquire(): Promise<boolean> {
    if (lease?.isLive()) return true;
    if (lease) dropLease();
    const redis = await getRedis().catch(() => null);
    if (!redis || stopped) return false;
    const next = new FencedLease({
      redis,
      key,
      leaseMs,
      owner: `${ownerPrefix}:${randomUUID()}`,
      ...(opts.now ? { now: opts.now } : {}),
      logger: { warn: (message, error) => log.warn({ err: error instanceof Error ? error.message : error }, message) },
    });
    try {
      await next.acquire();
    } catch (err) {
      if (err instanceof LeaseHeldError) {
        recordSchedulerLeaderSkipped();
      } else {
        recordSchedulerLeaderError();
        log.warn({ err: err instanceof Error ? err.message : String(err) },
          '[scheduler-leadership] acquire failed (treating as non-leader)');
      }
      void next.release();
      return false;
    }
    if (stopped) { void next.release(); return false; }
    lease = next;
    leaseRedis = redis;
    recordSchedulerLeaderAcquired();
    next.onFence((event) => {
      if (event.reason !== 'released') {
        log.warn({ reason: event.reason, message: event.message }, '[scheduler-leadership] lease fenced; another replica may take over');
      }
    });
    log.info({ owner: next.owner, token: next.token }, '[scheduler-leadership] acquired schedule leadership');
    return next.isLive();
  }

  return {
    async ensureLeader(): Promise<boolean> {
      if (stopped) return false;
      if (lease?.isLive()) return true;
      if (!acquiring) acquiring = acquire().finally(() => { acquiring = null; });
      return acquiring;
    },

    async claimFire(pipelineId: string, minuteEpoch: number): Promise<boolean> {
      const current = lease;
      const redis = leaseRedis;
      if (!current || !redis || !current.isLive()) return false;
      try {
        const reply = await redis.command(
          'EVAL', SCHEDULE_CLAIM_SCRIPT, '2',
          key, `${key}:fired:${pipelineId}:${minuteEpoch}`,
          current.owner, `${current.owner}#${current.token}`, String(markerTtlMs),
        );
        const n = Number(reply);
        if (n === -1) current.fence('renew-refused', 'fire claim found the leader key owned by someone else');
        return n === 1;
      } catch (err) {
        recordSchedulerLeaderError();
        log.warn({ pipelineId, err: err instanceof Error ? err.message : String(err) },
          '[scheduler-leadership] fire claim failed; skipping this fire');
        return false;
      }
    },

    isLeader(): boolean {
      return lease?.isLive() ?? false;
    },

    async stop(): Promise<void> {
      stopped = true;
      await acquiring?.catch(() => false);
      const old = lease;
      lease = null;
      leaseRedis = null;
      if (old) await old.release();
    },
  };
}

/**
 * A lazily opened BoundedRedis for the lease: every reply is bounded, and a
 * connection that missed a deadline is destroyed, so a stalled Redis fences
 * the lease instead of leaving a renewal pending forever. A destroyed or lost
 * connection is replaced on the next call (at most one attempt per
 * `retryMs`); while Redis is unreachable the getter returns null and no
 * replica leads, which is the safe failure for single-flight.
 */
export function createLeaseRedisGetter(
  url: string,
  opts: { retryMs?: number; timeoutMs?: number } = {},
): { get: () => Promise<LeaseRedis | null>; close: () => Promise<void> } {
  const retryMs = opts.retryMs ?? 5_000;
  let current: BoundedRedis | null = null;
  let opening: Promise<BoundedRedis | null> | null = null;
  let lastAttempt = 0;
  let closed = false;

  const open = async (): Promise<BoundedRedis | null> => {
    lastAttempt = Date.now();
    try {
      const next = await createBoundedRedis(url, {
        timeoutMs: opts.timeoutMs ?? 2_000,
        onError: (role, err) => log.debug({ role, err: err instanceof Error ? err.message : String(err) },
          '[scheduler-leadership] redis connection error'),
      });
      if (closed) { await next.quit(); return null; }
      current = next;
      return next;
    } catch (err) {
      log.warn({ err: err instanceof Error ? err.message : String(err) },
        '[scheduler-leadership] redis unreachable; no replica leads until it returns');
      return null;
    }
  };

  return {
    async get() {
      if (closed) return null;
      if (current?.isReady) return current;
      if (opening) return opening;
      if (Date.now() - lastAttempt < retryMs) return null;
      const stale = current;
      current = null;
      if (stale) await stale.quit().catch(() => undefined);
      opening = open().finally(() => { opening = null; });
      return opening;
    },
    async close() {
      closed = true;
      await opening?.catch(() => null);
      const c = current;
      current = null;
      if (c) await c.quit().catch(() => undefined);
    },
  };
}
