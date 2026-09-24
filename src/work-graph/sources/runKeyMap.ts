/**
 * Which pipeline a run id belongs to, kept in the cache Redis (realtime-examples NFR #147).
 *
 * `pipeline-runs` is keyed `(pipelineId, runId)` and the work graph names a run
 * by its id alone, so every run in a build cost one Query on the `runId-index`
 * GSI — 177 per build of a 250-node day, ~1 s of wall time. A run's key never
 * changes once written, so the pair can be remembered: `wg:runkey:<table>:<runId>`
 * holds the pipeline id, and a build reads every remembered run with
 * `BatchGetItem` (100 keys a call) instead of a Query each. Only the KEY is
 * remembered — the row itself is read fresh every build, on every replica, so
 * a run that changes on another replica shows in this replica's next build.
 *
 * A key is filled from the index Query that found exactly one row. What the
 * map cannot see by itself is a SECOND row claiming a remembered id (the index
 * would have answered "ambiguous"). Two things bound that: every write through
 * `PipelineRunRepository` calls `noteWrite`, which drops a key that names a
 * different pipeline (so the next read goes back to the index and sees both
 * rows), and the TTL bounds writes that bypass the repository (seed scripts).
 *
 * allkeys-lru Redis: a shortcut, never the truth. Unreachable → every read
 * goes to the index, exactly as before.
 */
import type { RedisClientType } from 'redis';

import { getRedisCacheClient } from '../../lib/redis-client';
import log from '../../lib/logger';

export const RUN_KEY_TTL_S = 15 * 60;

type CacheClient = Pick<RedisClientType, 'mGet' | 'get' | 'set' | 'del'>;

const keyOf = (table: string, runId: string): string => `wg:runkey:${table}:${runId}`;

export interface RunKeyCounters { hits: number; misses: number; filled: number; dropped: number; unavailable: number }
export const runKeyCounters: RunKeyCounters = { hits: 0, misses: 0, filled: 0, dropped: 0, unavailable: 0 };

export interface RunKeyStore {
  /** Remembered `runId → pipelineId` for the asked ids (absent when not remembered). */
  get(table: string, runIds: readonly string[]): Promise<Map<string, string>>;
  /** Remember keys an index read proved unique. */
  fill(table: string, entries: ReadonlyArray<readonly [runId: string, pipelineId: string]>): Promise<void>;
  /** Forget keys (a remembered key whose row is gone). */
  drop(table: string, runIds: readonly string[]): Promise<void>;
}

export class RedisRunKeyStore implements RunKeyStore {
  constructor(private readonly cache: () => Promise<CacheClient | null> = getRedisCacheClient) {}

  async get(table: string, runIds: readonly string[]): Promise<Map<string, string>> {
    const found = new Map<string, string>();
    if (!runIds.length) return found;
    const cache = await this.cache().catch(() => null);
    if (!cache) {
      runKeyCounters.unavailable += 1;
      return found;
    }
    try {
      const values = await cache.mGet(runIds.map((id) => keyOf(table, id)));
      runIds.forEach((id, index) => {
        const value = values[index];
        if (typeof value === 'string' && value) found.set(id, value);
      });
      runKeyCounters.hits += found.size;
      runKeyCounters.misses += runIds.length - found.size;
    } catch (err) {
      log.warn({ err: (err as Error).message }, 'run key map read failed; reading the runId index');
    }
    return found;
  }

  async fill(table: string, entries: ReadonlyArray<readonly [string, string]>): Promise<void> {
    if (!entries.length) return;
    const cache = await this.cache().catch(() => null);
    if (!cache) return;
    await Promise.all(entries.map(([runId, pipelineId]) => cache.set(keyOf(table, runId), pipelineId, { EX: RUN_KEY_TTL_S })))
      .then(() => { runKeyCounters.filled += entries.length; })
      .catch((err) => log.warn({ err: (err as Error).message }, 'run key map fill failed'));
  }

  async drop(table: string, runIds: readonly string[]): Promise<void> {
    if (!runIds.length) return;
    const cache = await this.cache().catch(() => null);
    if (!cache) return;
    await cache.del(runIds.map((id) => keyOf(table, id)))
      .then(() => { runKeyCounters.dropped += runIds.length; })
      .catch(() => undefined);
  }

  /**
   * A run row was written. A remembered key naming a different pipeline means
   * a second row now claims this id: drop it, so the next read asks the index
   * (which sees both rows and discloses nothing). Never throws, never blocks
   * the write it follows.
   */
  async noteWrite(table: string, pipelineId: unknown, runId: unknown): Promise<void> {
    if (typeof pipelineId !== 'string' || typeof runId !== 'string') return;
    try {
      const cache = await this.cache().catch(() => null);
      if (!cache) return;
      const known = await cache.get(keyOf(table, runId));
      if (known && known !== pipelineId) await this.drop(table, [runId]);
    } catch {
      // The TTL bounds anything this misses.
    }
  }
}

/** One per process; the map itself lives in the shared cache Redis. */
export const runKeyStore = new RedisRunKeyStore();
