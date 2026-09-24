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

import os from 'os';
import { RedisSemaphore, SemaphoreTimeoutError, type SemaphoreRedisClient } from 'distributed-core';
import { getRedisClient } from '../lib/redis-client';
import { getRegistry } from '../observability/metrics';
import log from '../lib/logger';

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

export function modelSlotConfig() {
  return {
    limit: Math.max(0, Math.floor(num(process.env.DECK_MODEL_CONCURRENCY, 4))),
    waitMs: num(process.env.DECK_MODEL_SLOT_WAIT_MS, 120_000),
    ttlMs: num(process.env.DECK_MODEL_PERMIT_TTL_MS, 15_000),
    pollMs: num(process.env.DECK_MODEL_SLOT_POLL_MS, 250),
  };
}

const LABELS = { service: process.env.WSG_SERVICE_NAME || 'platform-api', node_id: process.env.WSG_NODE_ID || os.hostname() };
const NODE = LABELS.node_id;
let inFlight = 0;
let waiting = 0;
function gauges() {
  const registry = getRegistry();
  return {
    inFlight: registry.gauge('deck_model_calls_in_flight', LABELS, 'Deck model calls this replica has in flight (each holds a model slot).'),
    waiting: registry.gauge('deck_model_calls_waiting', LABELS, 'Deck model calls on this replica waiting for a model slot.'),
    held: registry.gauge('deck_model_slots_held', LABELS, 'Model slots held across every replica, as Redis last reported it.'),
    waitTotal: registry.counter('deck_model_slot_wait_ms_total', LABELS, 'Milliseconds deck model calls spent waiting for a slot.'),
  };
}

/** node-redis → the one command the semaphore needs. */
function evalClient(client: { sendCommand(args: string[]): Promise<unknown> }): SemaphoreRedisClient {
  return { eval: (script, numkeys, ...args) => client.sendCommand(['EVAL', script, String(numkeys), ...args.map(String)]) };
}

let shared: { semaphore: RedisSemaphore; limit: number; client: unknown } | null = null;
let local: { limit: number; held: number; line: Array<() => void> } | null = null;

async function semaphore(limit: number, ttlMs: number, pollMs: number): Promise<RedisSemaphore | null> {
  let client = await getRedisClient().catch(() => null);
  if (!client && shared) {
    // A reconnect in progress: give it a moment rather than dropping to a
    // per-process cap, which would let the fleet exceed the limit.
    await new Promise((resolve) => setTimeout(resolve, 500));
    client = await getRedisClient().catch(() => null);
  }
  if (!client) return null;
  if (shared && shared.limit === limit && shared.client === client) return shared.semaphore;
  shared = {
    limit,
    client,
    semaphore: new RedisSemaphore(evalClient(client as unknown as { sendCommand(args: string[]): Promise<unknown> }), 'deck-model', {
      limit, ttlMs, pollMs, nodeId: NODE, keyPrefix: 'platform:sem:',
    }),
  };
  return shared.semaphore;
}

/** The same limit on this process only, for a deployment with no Redis. */
async function localSlot(limit: number, waitMs: number, onWaiting: (w: ModelSlotWait) => void): Promise<{ release: () => void; waitedMs: number }> {
  if (!local || local.limit !== limit) local = { limit, held: 0, line: [] };
  const slots = local;
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

/**
 * Runs `fn` (one model call) holding a model slot. Waiting for the slot does
 * not count against the call's own timeout: `fn` starts its clock when it runs.
 */
export async function withModelSlot<T>(fn: () => Promise<T>, opts: ModelSlotOptions = {}): Promise<T> {
  const cfg = modelSlotConfig();
  if (cfg.limit <= 0) return fn();
  const g = gauges();
  const safe = <A>(cb: ((a: A) => void) | undefined) => (a: A) => { try { cb?.(a); } catch { /* progress only */ } };
  const onWaiting = safe(opts.onWaiting);
  const onGranted = safe(opts.onGranted);
  let isWaiting = false;
  const startWaiting = (w: ModelSlotWait) => {
    if (!isWaiting) { isWaiting = true; waiting += 1; g.waiting.set(waiting); }
    g.held.set(w.held);
    onWaiting(w);
  };
  const stopWaiting = () => { if (isWaiting) { isWaiting = false; waiting -= 1; g.waiting.set(waiting); } };

  let release: () => Promise<void> | void;
  let waitedMs = 0;
  const sem = await semaphore(cfg.limit, cfg.ttlMs, cfg.pollMs);
  try {
    if (sem) {
      const permit = await sem.acquire({ timeoutMs: cfg.waitMs, onWaiting: startWaiting, ...(opts.signal ? { signal: opts.signal } : {}) })
        .catch((err: unknown) => {
          if (err instanceof SemaphoreTimeoutError) throw new ModelSlotTimeoutError(err.waitedMs, err.position, cfg.limit);
          throw err;
        });
      waitedMs = permit.waitedMs;
      release = () => permit.release();
    } else {
      ({ release, waitedMs } = await localSlot(cfg.limit, cfg.waitMs, startWaiting));
    }
  } finally {
    stopWaiting();
  }
  if (waitedMs > 0) { g.waitTotal.inc(waitedMs); onGranted(waitedMs); }
  inFlight += 1;
  g.inFlight.set(inFlight);
  // The fleet-wide count, for the log line that proves the cap (and the gauge).
  const fleet = sem ? await sem.status().catch(() => null) : null;
  if (fleet) g.held.set(fleet.held);
  log.info({ node: NODE, inFlight, held: fleet?.held ?? inFlight, waiting: fleet?.waiting, waitedMs, limit: cfg.limit, shared: Boolean(sem) }, 'deck model slot: granted');
  try {
    return await fn();
  } finally {
    inFlight -= 1;
    g.inFlight.set(inFlight);
    await Promise.resolve(release()).catch(() => undefined);
    if (sem) sem.status().then((s) => g.held.set(s.held)).catch(() => undefined);
  }
}

/** Tests: forget the shared semaphore and the local counter. */
export function resetModelSlots(): void { shared = null; local = null; inFlight = 0; waiting = 0; }
