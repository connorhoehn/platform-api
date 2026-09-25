// A cap on the deck's model calls in flight across every platform replica.
//
// Twenty people asking for a deck edit at once used to be twenty model calls
// at once (realtime-examples Loop 35 ppt): each replica called the model for
// every run it owned, so the fleet's load on the model grew with the number of
// runs, not with what the model can take. Now every deck model call — a
// revise, a fixture draft (after the single-flight, so identical drafts still
// share one call) — holds a permit from distributed-core's RedisSemaphore on
// the main Redis:
//
//   - at most DECK_MODEL_CONCURRENCY calls in flight across all replicas
//     (default 4);
//   - a caller that finds every slot taken waits in line, first come first
//     served, and is told its place (the revise run shows "Waiting for a model
//     slot" on its step);
//   - a caller that waits longer than DECK_MODEL_SLOT_WAIT_MS (default 120 s)
//     gives up with ModelSlotTimeoutError — a clear "the agent is busy", not a
//     model error;
//   - a permit is a TTL'd lease (DECK_MODEL_PERMIT_TTL_MS, default 15 s)
//     renewed while held, so a pod that dies mid-call frees its slots within
//     the TTL.
//
// No Redis: the same limit per process (a local counter), so a single-node dev
// setup still has backpressure. DECK_MODEL_CONCURRENCY=0 turns the cap off.
//
// Metrics (/internal/metrics): deck_model_calls_in_flight (this replica),
// deck_model_calls_waiting (this replica), deck_model_slots_held (the whole
// fleet, as Redis last reported it), deck_model_slot_wait_ms_total.
//
// Pools (NFR Loop 39): every model caller, not only the deck, takes a slot —
// each purpose from its own pool (its own semaphore and limit), so a flood of
// deck revises cannot starve the /agent planner or a change verdict. `deck`
// keeps DECK_MODEL_CONCURRENCY and the `deck-model` semaphore; every other
// purpose reads MODEL_CONCURRENCY_<PURPOSE> (planner 2, verdict 2, drafter 2,
// document 2, pipeline 4, agent 4, vision 2, other 2; 0 = off) and uses `model-<purpose>`.
// MODEL_SLOT_WAIT_MS_<PURPOSE> overrides the wait. Per-pool gauges:
// model_calls_in_flight{purpose}, model_calls_waiting{purpose},
// model_slots_held{purpose}, model_slot_wait_ms_total{purpose}.

import os from 'os';
import { RedisSemaphore, SemaphoreTimeoutError, type SemaphoreRedisClient } from 'distributed-core';
import { getRedisClient } from '../lib/redis-client';
import { getRegistry } from '../observability/metrics';
import log from '../lib/logger';

export type ModelPurpose = 'deck' | 'planner' | 'verdict' | 'drafter' | 'document' | 'pipeline' | 'agent' | 'vision' | 'other';
export const MODEL_PURPOSES: readonly ModelPurpose[] = ['deck', 'planner', 'verdict', 'drafter', 'document', 'pipeline', 'agent', 'vision', 'other'];

const DEFAULT_LIMITS: Record<ModelPurpose, number> = { deck: 4, planner: 2, verdict: 2, drafter: 2, document: 2, pipeline: 4, agent: 4, vision: 2, other: 2 };

export interface ModelSlotWait {
  /** 0-based place in line (0 = next). */
  position: number;
  /** Slots in use across the fleet when this was measured. */
  held: number;
  waitedMs: number;
}

export interface ModelSlotOptions {
  /** Told when the call has to wait, and again when its place in line moves. Must not throw. */
  onWaiting?: (wait: ModelSlotWait) => void;
  /** Told once the call has its slot, if it had to wait. */
  onGranted?: (waitedMs: number) => void;
  signal?: AbortSignal;
  /** Which pool the call draws from (default `deck`). */
  pool?: ModelPurpose;
}

/** `withModelSlot` with the options bound: runs one call holding a slot. */
export type ModelSlotRunner = <T>(fn: () => Promise<T>) => Promise<T>;

export class ModelSlotTimeoutError extends Error {
  constructor(readonly waitedMs: number, readonly position: number, readonly limit: number) {
    super(`no model slot came free within ${Math.round(waitedMs / 1000)} s (${position + 1} in line, ${limit} slots)`);
    this.name = 'ModelSlotTimeoutError';
  }
}

const num = (value: string | undefined, fallback: number) => {
  const n = Number(value);
  return value !== undefined && value !== '' && Number.isFinite(n) ? n : fallback;
};

export function modelSlotConfig(pool: ModelPurpose = 'deck') {
  const P = pool.toUpperCase();
  const limit = pool === 'deck'
    ? num(process.env.DECK_MODEL_CONCURRENCY, DEFAULT_LIMITS.deck)
    : num(process.env[`MODEL_CONCURRENCY_${P}`], DEFAULT_LIMITS[pool] ?? 2);
  const waitMs = pool === 'deck'
    ? num(process.env.DECK_MODEL_SLOT_WAIT_MS, 120_000)
    : num(process.env[`MODEL_SLOT_WAIT_MS_${P}`], 120_000);
  return {
    limit: Math.max(0, Math.floor(limit)),
    waitMs,
    ttlMs: num(process.env.DECK_MODEL_PERMIT_TTL_MS, 15_000),
    pollMs: num(process.env.DECK_MODEL_SLOT_POLL_MS, 250),
  };
}

/** Every pool's limit, for /internal/metrics and the load check. */
export function modelSlotLimits(): Record<ModelPurpose, number> {
  return Object.fromEntries(MODEL_PURPOSES.map((p) => [p, modelSlotConfig(p).limit])) as Record<ModelPurpose, number>;
}

const SERVICE = process.env.WSG_SERVICE_NAME || 'platform-api';
const NODE = process.env.WSG_NODE_ID || os.hostname();
const LABELS = { service: SERVICE, node_id: NODE };

interface PoolState {
  inFlight: number;
  waiting: number;
  shared: { semaphore: RedisSemaphore; limit: number; client: unknown } | null;
  local: { limit: number; held: number; line: Array<() => void> } | null;
}
const pools = new Map<ModelPurpose, PoolState>();
function poolState(pool: ModelPurpose): PoolState {
  let st = pools.get(pool);
  if (!st) { st = { inFlight: 0, waiting: 0, shared: null, local: null }; pools.set(pool, st); }
  return st;
}

function gauges(pool: ModelPurpose) {
  const registry = getRegistry();
  const labels = { ...LABELS, purpose: pool };
  const deck = pool === 'deck';
  const noop = { set: (_v: number) => undefined, inc: (_v: number) => undefined };
  return {
    inFlight: registry.gauge('model_calls_in_flight', labels, 'Model calls this replica has in flight, by purpose (each holds a slot from its pool).'),
    waiting: registry.gauge('model_calls_waiting', labels, 'Model calls on this replica waiting for a slot, by purpose.'),
    held: registry.gauge('model_slots_held', labels, 'Model slots held across every replica, by purpose, as Redis last reported it.'),
    waitTotal: registry.counter('model_slot_wait_ms_total', labels, 'Milliseconds model calls spent waiting for a slot, by purpose.'),
    deckInFlight: deck ? registry.gauge('deck_model_calls_in_flight', LABELS, 'Deck model calls this replica has in flight (each holds a model slot).') : noop,
    deckWaiting: deck ? registry.gauge('deck_model_calls_waiting', LABELS, 'Deck model calls on this replica waiting for a model slot.') : noop,
    deckHeld: deck ? registry.gauge('deck_model_slots_held', LABELS, 'Model slots held across every replica, as Redis last reported it.') : noop,
    deckWaitTotal: deck ? registry.counter('deck_model_slot_wait_ms_total', LABELS, 'Milliseconds deck model calls spent waiting for a slot.') : noop,
  };
}

/** node-redis → the one command the semaphore needs. */
function evalClient(client: { sendCommand(args: string[]): Promise<unknown> }): SemaphoreRedisClient {
  return { eval: (script, numkeys, ...args) => client.sendCommand(['EVAL', script, String(numkeys), ...args.map(String)]) };
}

async function semaphore(pool: ModelPurpose, limit: number, ttlMs: number, pollMs: number): Promise<RedisSemaphore | null> {
  const st = poolState(pool);
  let client = await getRedisClient().catch(() => null);
  if (!client && st.shared) {
    // A reconnect in progress: give it a moment rather than dropping to a
    // per-process cap, which would let the fleet exceed the limit.
    await new Promise((resolve) => setTimeout(resolve, 500));
    client = await getRedisClient().catch(() => null);
  }
  if (!client) return null;
  if (st.shared && st.shared.limit === limit && st.shared.client === client) return st.shared.semaphore;
  st.shared = {
    limit,
    client,
    semaphore: new RedisSemaphore(evalClient(client as unknown as { sendCommand(args: string[]): Promise<unknown> }), pool === 'deck' ? 'deck-model' : `model-${pool}`, {
      limit, ttlMs, pollMs, nodeId: NODE, keyPrefix: 'platform:sem:',
    }),
  };
  return st.shared.semaphore;
}

/** The same limit on this process only, for a deployment with no Redis. */
async function localSlot(pool: ModelPurpose, limit: number, waitMs: number, onWaiting: (w: ModelSlotWait) => void): Promise<{ release: () => void; waitedMs: number }> {
  const st = poolState(pool);
  if (!st.local || st.local.limit !== limit) st.local = { limit, held: 0, line: [] };
  const slots = st.local;
  const release = () => {
    slots.held -= 1;
    slots.line.shift()?.();
  };
  if (slots.held < limit) { slots.held += 1; return { release, waitedMs: 0 }; }
  const started = Date.now();
  onWaiting({ position: slots.line.length, held: slots.held, waitedMs: 0 });
  return new Promise((resolve, reject) => {
    const go = () => { clearTimeout(timer); slots.held += 1; resolve({ release, waitedMs: Math.max(1, Date.now() - started) }); };
    const timer = setTimeout(() => {
      const at = slots.line.indexOf(go);
      if (at >= 0) slots.line.splice(at, 1);
      reject(new ModelSlotTimeoutError(Date.now() - started, Math.max(0, at), limit));
    }, waitMs);
    slots.line.push(go);
  });
}

export interface ModelSlotLease {
  waitedMs: number;
  /** Idempotent. */
  release(): Promise<void>;
}

/**
 * Takes one slot from `opts.pool` (waiting in line if the pool is full) and
 * hands back the lease; the caller releases it when its call ends. For a
 * streaming caller that cannot wrap its call in one function.
 */
export async function acquireModelSlot(opts: ModelSlotOptions = {}): Promise<ModelSlotLease> {
  const pool = opts.pool ?? 'deck';
  const cfg = modelSlotConfig(pool);
  if (cfg.limit <= 0) return { waitedMs: 0, release: async () => undefined };
  const st = poolState(pool);
  const g = gauges(pool);
  const safe = <A>(cb: ((a: A) => void) | undefined) => (a: A) => { try { cb?.(a); } catch { /* progress only */ } };
  const onWaiting = safe(opts.onWaiting);
  const onGranted = safe(opts.onGranted);
  let isWaiting = false;
  const startWaiting = (w: ModelSlotWait) => {
    if (!isWaiting) { isWaiting = true; st.waiting += 1; g.waiting.set(st.waiting); g.deckWaiting.set(st.waiting); }
    g.held.set(w.held); g.deckHeld.set(w.held);
    onWaiting(w);
  };
  const stopWaiting = () => { if (isWaiting) { isWaiting = false; st.waiting -= 1; g.waiting.set(st.waiting); g.deckWaiting.set(st.waiting); } };

  let releaseFn: () => Promise<void> | void;
  let waitedMs = 0;
  const sem = await semaphore(pool, cfg.limit, cfg.ttlMs, cfg.pollMs);
  try {
    if (sem) {
      const permit = await sem.acquire({ timeoutMs: cfg.waitMs, onWaiting: startWaiting, ...(opts.signal ? { signal: opts.signal } : {}) })
        .catch((err: unknown) => {
          if (err instanceof SemaphoreTimeoutError) throw new ModelSlotTimeoutError(err.waitedMs, err.position, cfg.limit);
          throw err;
        });
      waitedMs = permit.waitedMs;
      releaseFn = () => permit.release();
    } else {
      ({ release: releaseFn, waitedMs } = await localSlot(pool, cfg.limit, cfg.waitMs, startWaiting));
    }
  } finally {
    stopWaiting();
  }
  if (waitedMs > 0) { g.waitTotal.inc(waitedMs); g.deckWaitTotal.inc(waitedMs); onGranted(waitedMs); }
  st.inFlight += 1;
  g.inFlight.set(st.inFlight); g.deckInFlight.set(st.inFlight);
  // The fleet-wide count, for the log line that proves the cap (and the gauge).
  const fleet = sem ? await sem.status().catch(() => null) : null;
  if (fleet) { g.held.set(fleet.held); g.deckHeld.set(fleet.held); }
  log.info({ node: NODE, purpose: pool, inFlight: st.inFlight, held: fleet?.held ?? st.inFlight, waiting: fleet?.waiting, waitedMs, limit: cfg.limit, shared: Boolean(sem) }, 'model slot: granted');
  let released = false;
  return {
    waitedMs,
    async release() {
      if (released) return;
      released = true;
      st.inFlight -= 1;
      g.inFlight.set(st.inFlight); g.deckInFlight.set(st.inFlight);
      await Promise.resolve(releaseFn()).catch(() => undefined);
      if (sem) sem.status().then((s) => { g.held.set(s.held); g.deckHeld.set(s.held); }).catch(() => undefined);
    },
  };
}

/**
 * Runs `fn` (one model call) holding a model slot. Waiting for the slot does
 * not count against the call's own timeout: `fn` starts its clock when it runs.
 */
export async function withModelSlot<T>(fn: () => Promise<T>, opts: ModelSlotOptions = {}): Promise<T> {
  const lease = await acquireModelSlot(opts);
  try {
    return await fn();
  } finally {
    await lease.release();
  }
}

/** Tests: forget the shared semaphores and the local counters. */
export function resetModelSlots(): void { pools.clear(); }
