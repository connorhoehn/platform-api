/**
 * One graph build's read memo (NFR #16, realtime-examples).
 *
 * A single `/graph` answer reads the same rows from several places: the
 * pipeline ACL pass reads every run the graph names, then the activity layer
 * reads every one of them again, and each run's facts fetched its pipeline's
 * definition — 304 `pipeline-runs` Queries and 135 `pipeline-definitions` Gets
 * for one build of a 250-node day (measured 2026-09-23). Inside
 * `withBuildMemo`, `memoOnce` answers a key read earlier in the same build
 * from that read. Nothing outlives the build: the next request reads fresh,
 * on every replica, so there is no invalidation to get wrong.
 */
import { AsyncLocalStorage } from 'node:async_hooks';

const store = new AsyncLocalStorage<Map<string, Promise<unknown>>>();

/** Runs `build` with a read memo — the enclosing build's, when there is one. */
export function withBuildMemo<T>(build: () => Promise<T>): Promise<T> {
  return store.getStore() ? build() : store.run(new Map(), build);
}

/** `load()` once per key per build; outside a build, every call loads. A failed read is not remembered. */
export function memoOnce<T>(key: string, load: () => Promise<T>): Promise<T> {
  const memo = store.getStore();
  if (!memo) return load();
  const known = memo.get(key) as Promise<T> | undefined;
  if (known) return known;
  const pending = load();
  memo.set(key, pending);
  pending.catch(() => memo.delete(key));
  return pending;
}

/** Whether a build memo is active (for `rows`-style batch readers). */
export function buildMemoActive(): boolean {
  return !!store.getStore();
}

/** Whether this build already holds (or is reading) `key`. */
export function memoHas(key: string): boolean {
  return !!store.getStore()?.has(key);
}

const fullReads = new AsyncLocalStorage<true>();

/**
 * Runs `build` with every read unprojected — the whole row, as a build read it
 * before NFR #157 — so an operator can check a projected build against the
 * full one (`x-graph-full-read: 1` on the graph route).
 */
export function withFullReads<T>(build: () => Promise<T>): Promise<T> {
  return fullReads.run(true, build);
}

/** Whether the enclosing build asked for whole rows. */
export function fullReadsRequested(): boolean {
  return fullReads.getStore() === true;
}
