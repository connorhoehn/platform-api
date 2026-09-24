/**
 * One model call per identical deck draft, across replicas (realtime-examples
 * NFR #162).
 *
 * `POST /api/deck/fixture` asks the model for a deck from named material. Ten
 * callers asking for the same material at once made ten model calls (live,
 * 2026-09-24: p50 19.7 s / p95 24.1 s, 11.2 s of pod CPU, ten different
 * drafts). The key is a hash of the exact prompt and model, so a draft is only
 * shared when the model would have been asked the same thing.
 *
 * - Same replica: callers share one promise.
 * - Across replicas: the first to `SET <lock> NX PX` runs; it writes the answer
 *   to `<result>` for a short grace window and drops the lock. The others poll
 *   `<result>`; if the lock goes away with no result (the runner died or
 *   failed), a waiter tries the lock itself.
 * - The result lives only `resultTtlMs` (default 15 s): enough for the callers
 *   that raced, short enough that "draft it again" a little later gets a new
 *   draft. Nothing here is a cache of drafts.
 * - No Redis: same-replica sharing only, exactly as before otherwise.
 */
import { createHash, randomUUID } from 'node:crypto';
import log from '../lib/logger';

/** The subset of a node-redis client this needs. */
export interface SingleFlightStore {
  set(key: string, value: string, opts: { NX?: boolean; PX?: number }): Promise<string | null>;
  get(key: string): Promise<string | null>;
  del(key: string): Promise<number>;
}

export interface SingleFlightOptions {
  store: () => Promise<SingleFlightStore | null>;
  /** Namespace for the Redis keys. */
  prefix: string;
  /** How long a runner may hold the lock (its own timeout plus headroom). */
  lockTtlMs: number;
  resultTtlMs?: number;
  pollMs?: number;
  /** A waiter gives up and runs itself after this long. */
  maxWaitMs?: number;
  sleep?: (ms: number) => Promise<void>;
}

export type SingleFlightRole = 'ran' | 'joined-local' | 'joined-remote';

export function contentKey(...parts: string[]): string {
  const h = createHash('sha256');
  for (const part of parts) h.update(part).update('\u0000');
  return h.digest('hex');
}

export function createSingleFlight<T>(opts: SingleFlightOptions) {
  const inProcess = new Map<string, Promise<T>>();
  const resultTtlMs = opts.resultTtlMs ?? 15_000;
  const pollMs = opts.pollMs ?? 250;
  const maxWaitMs = opts.maxWaitMs ?? opts.lockTtlMs + 5_000;
  const sleep = opts.sleep ?? ((ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms)));
  const lockKey = (key: string) => `${opts.prefix}:${key}:lock`;
  const resultKey = (key: string) => `${opts.prefix}:${key}:result`;

  async function acrossReplicas(key: string, run: () => Promise<T>, onRole: (role: SingleFlightRole) => void): Promise<T> {
    let store: SingleFlightStore | null = null;
    try { store = await opts.store(); } catch { store = null; }
    if (!store) { onRole('ran'); return run(); }
    const owner = randomUUID();
    const started = Date.now();
    for (;;) {
      let got: string | null = null;
      try {
        const ready = await store.get(resultKey(key));
        if (ready) { onRole('joined-remote'); return JSON.parse(ready) as T; }
        got = await store.set(lockKey(key), owner, { NX: true, PX: opts.lockTtlMs });
      } catch (err) {
        log.warn({ err: err instanceof Error ? err.message : String(err) }, 'single-flight: store unavailable, running alone');
        onRole('ran');
        return run();
      }
      if (got === 'OK') {
        onRole('ran');
        try {
          const value = await run();
          await store.set(resultKey(key), JSON.stringify(value), { PX: resultTtlMs }).catch(() => null);
          return value;
        } finally {
          // Only our own lock: after a lockTtl overrun another runner may hold it.
          const holder = await store.get(lockKey(key)).catch(() => null);
          if (holder === owner) await store.del(lockKey(key)).catch(() => 0);
        }
      }
      // Someone else runs it: wait for the answer, or for their lock to go.
      while (Date.now() - started < maxWaitMs) {
        await sleep(pollMs);
        let ready: string | null = null;
        let holder: string | null = null;
        try {
          ready = await store.get(resultKey(key));
          if (!ready) holder = await store.get(lockKey(key));
        } catch { break; }
        if (ready) { onRole('joined-remote'); return JSON.parse(ready) as T; }
        if (!holder) break; // the runner failed or died: try the lock again
      }
      if (Date.now() - started >= maxWaitMs) {
        log.warn({ key: key.slice(0, 12) }, 'single-flight: waited too long, running alone');
        onRole('ran');
        return run();
      }
    }
  }

  /** `run()` once for `key` among everyone asking at the same time. */
  return function singleFlight(key: string, run: () => Promise<T>, onRole: (role: SingleFlightRole) => void = () => {}): Promise<T> {
    const pending = inProcess.get(key);
    if (pending) { onRole('joined-local'); return pending; }
    const p = acrossReplicas(key, run, onRole).finally(() => inProcess.delete(key));
    inProcess.set(key, p);
    return p;
  };
}
