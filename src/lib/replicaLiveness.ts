// Replica liveness + cluster-wide sweep leadership (NFR backlog #26, #11).
//
// Every platform-api replica used to run its boot-time sweeps — the orphaned
// agent-loop sweep, the running→interrupted orphan scan and its resume, the
// audit-TTL sweep — as if it were the only process alive. With two replicas a
// rolling restart made the NEW pod fail the planning loops and interrupt (then
// re-execute) the runs the OTHER, perfectly healthy pod was in the middle of.
//
// Two distributed-core primitives fix that, both `LeasedKVLock` over the DDB
// `LeaseStore` adapter the outbox drainer and scheduler already use
// (`ddb-lease-store.ts`, social-outbox table), so no new infrastructure:
//
//   1. A per-replica LIVENESS lease, key `platform-replica#<nodeId>`, owner
//      `<nodeId>@<bootedAtMs>`. The owner string names the INCARNATION, not
//      just the node: node ids are stable across restarts (identity file), so
//      "node X is alive" is not the same as "the process that started this
//      run is alive". A row is live only while the lease is unexpired AND its
//      incarnation booted no later than the work it is asked about started.
//   2. A single SWEEP-LEADER lease, key `platform-sweep-leader`, that gates
//      every sweep so exactly one replica sweeps per interval cluster-wide.
//
// Why not dc's Raft/`ClusterLeaderElection`: the pipeline cluster in local dev
// and in the chart is not guaranteed to be multi-member (each pod may run a
// one-node cluster over its own gossip seed list), so an election over it would
// elect every pod. The DDB conditional-write lease is shared by construction.

import { LeasedKVLock } from 'distributed-core';
import type { LeaseStore } from 'distributed-core';
import log from './logger';

export const REPLICA_KEY_PREFIX = 'platform-replica#';
export const SWEEP_LEADER_KEY = 'platform-sweep-leader';

/** This process's incarnation — set once at bootstrap, read by row writers. */
let localReplica: { nodeId: string; bootedAtMs: number } | null = null;

export function setLocalReplica(nodeId: string, bootedAtMs = Date.now()): void {
  localReplica = { nodeId, bootedAtMs };
}

export function getLocalReplica(): { nodeId: string; bootedAtMs: number } | null {
  return localReplica;
}

/** Test-only reset. */
export function _resetLocalReplicaForTests(): void {
  localReplica = null;
}

function incarnationOwner(nodeId: string, bootedAtMs: number): string {
  return `${nodeId}@${bootedAtMs}`;
}

function parseIncarnation(owner: string): { nodeId: string; bootedAtMs: number } | null {
  const at = owner.lastIndexOf('@');
  if (at <= 0) return null;
  const bootedAtMs = Number(owner.slice(at + 1));
  if (!Number.isFinite(bootedAtMs)) return null;
  return { nodeId: owner.slice(0, at), bootedAtMs };
}

/**
 * Holds this replica's liveness lease. A crashed predecessor with the same
 * node id may still hold an unexpired row, in which case `start()` loses the
 * claim; the retry timer keeps trying until the old lease lapses (≤ ttlMs).
 */
export class ReplicaHeartbeat {
  private readonly lock: LeasedKVLock;
  private retry: NodeJS.Timeout | null = null;

  constructor(
    private readonly opts: { store: LeaseStore; nodeId: string; bootedAtMs: number; ttlMs?: number; now?: () => number },
  ) {
    this.lock = new LeasedKVLock({
      store: opts.store,
      lockKey: `${REPLICA_KEY_PREFIX}${opts.nodeId}`,
      ownerId: incarnationOwner(opts.nodeId, opts.bootedAtMs),
      ttlMs: opts.ttlMs ?? 30_000,
      ...(opts.now ? { now: opts.now } : {}),
      onLeaseLost: () => log.warn({ nodeId: opts.nodeId }, 'replica liveness lease lost; re-claiming'),
    });
  }

  async start(): Promise<boolean> {
    const claim = async (): Promise<boolean> => {
      try {
        return await this.lock.start();
      } catch (err) {
        log.warn({ err: String(err) }, 'replica liveness claim failed');
        return false;
      }
    };
    const won = await claim();
    const every = Math.max(1_000, Math.floor((this.opts.ttlMs ?? 30_000) / 3));
    this.retry = setInterval(() => {
      if (!this.lock.isLeader()) void claim();
    }, every);
    this.retry.unref?.();
    return won;
  }

  isHeld(): boolean {
    return this.lock.isLeader();
  }

  async stop(): Promise<void> {
    if (this.retry) clearInterval(this.retry);
    this.retry = null;
    await this.lock.stop();
  }
}

/**
 * Is the process that took on work at `sinceMs` on node `ownerNodeId` still
 * running? True only when that node's liveness lease is unexpired AND its
 * current incarnation booted at or before `sinceMs` — a node that restarted
 * after the work began is a different process that knows nothing of it.
 *
 * `ownerNodeId` missing → false: a row written before ownership was stamped
 * keeps the old single-replica meaning (its owner is presumed gone).
 */
export async function isOwnerLive(
  store: LeaseStore,
  ownerNodeId: string | undefined,
  sinceMs: number,
  now: () => number = Date.now,
): Promise<boolean> {
  if (!ownerNodeId) return false;
  const self = localReplica;
  if (self && self.nodeId === ownerNodeId) {
    // Ourselves: no store round trip. Alive iff we were already running.
    return Number.isFinite(sinceMs) && sinceMs >= self.bootedAtMs;
  }
  let row: { owner: string; expiresAt: number } | null;
  try {
    row = await store.read(`${REPLICA_KEY_PREFIX}${ownerNodeId}`);
  } catch (err) {
    // Cannot tell → do not sweep. A missed sweep is retried next interval;
    // failing a live loop is not undoable.
    log.warn({ ownerNodeId, err: String(err) }, 'replica liveness read failed; treating owner as live');
    return true;
  }
  if (!row || row.expiresAt <= now()) return false;
  const inc = parseIncarnation(row.owner);
  if (!inc || inc.nodeId !== ownerNodeId) return false;
  return Number.isFinite(sinceMs) && sinceMs >= inc.bootedAtMs;
}

export interface SweepTask {
  name: string;
  run: () => Promise<void>;
}

/**
 * Runs `tasks` every `intervalMs` on whichever replica holds the sweep-leader
 * lease. A non-leader tries to take the lease each tick, so leadership moves
 * within one TTL of the leader dying. The first tick runs immediately.
 */
export class ClusterSweeper {
  private readonly lock: LeasedKVLock;
  private timer: NodeJS.Timeout | null = null;
  private running = false;
  /** Ticks this process actually swept on (observable for tests / metrics). */
  sweeps = 0;

  constructor(
    private readonly opts: {
      store: LeaseStore;
      tasks: SweepTask[];
      intervalMs?: number;
      ttlMs?: number;
      ownerId?: string;
      lockKey?: string;
      now?: () => number;
    },
  ) {
    this.lock = new LeasedKVLock({
      store: opts.store,
      lockKey: opts.lockKey ?? SWEEP_LEADER_KEY,
      ttlMs: opts.ttlMs ?? 30_000,
      ...(opts.ownerId ? { ownerId: opts.ownerId } : {}),
      ...(opts.now ? { now: opts.now } : {}),
      onLeaseLost: () => log.warn('sweep leadership lost'),
    });
  }

  get ownerId(): string {
    return this.lock.ownerId;
  }

  isLeader(): boolean {
    return this.lock.isLeader();
  }

  /** One pass: take/keep the lease, then run every task if we hold it. */
  async tick(): Promise<boolean> {
    if (this.running) return false;
    this.running = true;
    try {
      if (!this.lock.isLeader()) {
        let won = false;
        try {
          won = await this.lock.start();
        } catch (err) {
          log.warn({ err: String(err) }, 'sweep leader claim failed');
        }
        if (!won) return false;
        log.info({ owner: this.lock.ownerId }, 'sweep leadership acquired');
      }
      for (const task of this.opts.tasks) {
        if (!this.lock.isLeader()) break;
        try {
          await task.run();
        } catch (err) {
          log.warn({ task: task.name, err: String(err) }, 'cluster sweep task failed');
        }
      }
      this.sweeps += 1;
      return true;
    } finally {
      this.running = false;
    }
  }

  start(): void {
    void this.tick();
    this.timer = setInterval(() => void this.tick(), this.opts.intervalMs ?? 60_000);
    this.timer.unref?.();
  }

  async stop(): Promise<void> {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    await this.lock.stop();
  }
}

/**
 * Process-local LeaseStore with the DDB adapter's semantics (claim when absent
 * or expired; renew/release only by owner). Used by tests and by bootstrap in
 * the jest environment, where there is no DDB to lease against.
 */
export function createMemoryLeaseStore(now: () => number = Date.now): LeaseStore & {
  rows: Map<string, { owner: string; expiresAt: number }>;
} {
  const rows = new Map<string, { owner: string; expiresAt: number }>();
  return {
    rows,
    async tryClaim(key, value) {
      const cur = rows.get(key);
      if (cur && cur.expiresAt >= now()) return false;
      rows.set(key, { ...value });
      return true;
    },
    async renewIfOwner(key, owner, newExpiresAt) {
      const cur = rows.get(key);
      if (!cur || cur.owner !== owner) throw new Error('lease not owned');
      cur.expiresAt = newExpiresAt;
    },
    async releaseIfOwner(key, owner) {
      if (rows.get(key)?.owner === owner) rows.delete(key);
    },
    async read(key) {
      const cur = rows.get(key);
      return cur ? { ...cur } : null;
    },
  };
}
