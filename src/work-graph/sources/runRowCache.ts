/**
 * Settled run rows, as a graph build reads them, kept in the shared cache
 * Redis (realtime-examples NFR #16 / Loop 32 (activity)).
 *
 * The pipeline ACL was the slowest phase of a warm work-graph build, and it
 * was not the decision: that is in memory (`triggeredBy` against the owner
 * and viewer). It was the read behind it — the day's run rows by key, one
 * `BatchGetItem` per 100 runs (240 runs ≈ 50–80 ms p50 on the dev stack),
 * which the activity layer then reuses through the build memo. Every build
 * paid it again for rows that had not changed: a run that finished minutes
 * ago is the same row on every build.
 *
 * So a finished run's projected row is kept here, shared by every replica and
 * every viewer; only a live (or just-finished) run is read from DynamoDB.
 *
 * What keeps it from going stale:
 *   - only SETTLED rows are kept — a terminal status whose last write is
 *     `SETTLE_MS` old; a run still being written (and the finalize/evidence
 *     writes that follow a terminal event) is always read fresh;
 *   - every write that can change a settled row (`PipelineRunRepository`:
 *     terminal put, resume claim, review verdict, delete) and every run
 *     lifecycle event on the pipeline bus puts a short HOLD on the key, after
 *     the write: a fill that raced the write finds the hold (fills are `NX`)
 *     and lands nothing, and a fill that landed first is overwritten;
 *   - `ROW_TTL_S` bounds any writer that goes around both (a seed writing
 *     DynamoDB directly).
 * `x-graph-full-read: 1` builds never read or fill it (the drift check).
 * Authorization is unchanged: grants and memberships are read per build as
 * before, and the decision is made from the row exactly as it was.
 */

import type { RedisClientType } from 'redis';
import { isTerminalRunStatus } from '@connorhoehn/pipeline-runtime';
import { getRedisCacheClient } from '../../lib/redis-client';
import log from '../../lib/logger';

type Row = Record<string, unknown>;
type CacheClient = Pick<RedisClientType, 'mGet' | 'set'>;

/** How long a settled row is kept. */
export const ROW_TTL_S = 10 * 60;
/** How long a written run's key refuses fills. */
export const HOLD_TTL_S = 30;
/** A terminal row younger than this is still being written (finalize, evidence). */
export const SETTLE_MS = 60_000;
const HOLD = '__hold';

export interface RunRowCacheCounters { hits: number; misses: number; filled: number; held: number; unavailable: number }
export const runRowCacheCounters: RunRowCacheCounters = { hits: 0, misses: 0, filled: 0, held: 0, unavailable: 0 };

const keyOf = (table: string, runId: string): string => `wg:runrow:v1:${table}:${runId}`;

function lastWrite(row: Row): number {
  return Math.max(0, ...['completedAt', 'lastEventAt', 'interruptedAt', 'resumedAt', 'startedAt']
    .map((name) => (typeof row[name] === 'string' ? Date.parse(row[name] as string) : NaN))
    .filter(Number.isFinite));
}

/** A row nobody is writing any more: terminal, and its last write `SETTLE_MS` old. */
export function isSettledRunRow(row: Row, now: number): boolean {
  if (typeof row.status !== 'string' || row.status === 'index' || !isTerminalRunStatus(row.status as never)) return false;
  const at = lastWrite(row);
  return at > 0 && now - at >= SETTLE_MS;
}

export class RunRowCache {
  constructor(
    private readonly cache: () => Promise<CacheClient | null> = getRedisCacheClient,
    private readonly now: () => number = Date.now,
  ) {}

  /** The cached rows among `runIds` (absent: not cached, held, or the cache is down). */
  async get(table: string, runIds: readonly string[]): Promise<Map<string, Row>> {
    const found = new Map<string, Row>();
    if (!runIds.length) return found;
    const cache = await this.cache().catch(() => null);
    if (!cache) { runRowCacheCounters.unavailable += 1; return found; }
    try {
      const values = await cache.mGet(runIds.map((id) => keyOf(table, id)));
      runIds.forEach((id, index) => {
        const value = values[index];
        if (typeof value !== 'string' || value === HOLD) return;
        try {
          const row = JSON.parse(value) as Row;
          if (row && row.runId === id) found.set(id, row);
        } catch { /* a bad value is a miss */ }
      });
      runRowCacheCounters.hits += found.size;
      runRowCacheCounters.misses += runIds.length - found.size;
    } catch (err) {
      log.warn({ err: (err as Error).message }, 'run row cache read failed; reading DynamoDB');
    }
    return found;
  }

  /** Keeps the settled rows among `rows`; never over a hold or a newer fill. */
  async fill(table: string, rows: Iterable<Row>): Promise<void> {
    const now = this.now();
    const settled = [...rows].filter((row) => typeof row.runId === 'string' && isSettledRunRow(row, now));
    if (!settled.length) return;
    const cache = await this.cache().catch(() => null);
    if (!cache) return;
    await Promise.all(settled.map((row) => cache.set(keyOf(table, row.runId as string), JSON.stringify(row), { EX: ROW_TTL_S, NX: true })))
      .then(() => { runRowCacheCounters.filled += settled.length; })
      .catch((err) => log.warn({ err: (err as Error).message }, 'run row cache fill failed'));
  }

  /**
   * A run was written (or its lifecycle moved): drop what is kept and refuse
   * fills for `HOLD_TTL_S`. Called AFTER the write. Never throws.
   */
  async hold(table: string, runIds: readonly string[]): Promise<void> {
    const ids = runIds.filter((id) => typeof id === 'string' && id.length > 0);
    if (!ids.length) return;
    try {
      const cache = await this.cache().catch(() => null);
      if (!cache) return;
      await Promise.all(ids.map((id) => cache.set(keyOf(table, id), HOLD, { EX: HOLD_TTL_S })));
      runRowCacheCounters.held += ids.length;
    } catch {
      // ROW_TTL_S bounds anything this misses.
    }
  }
}

/** One per process; the rows live in the shared cache Redis. */
export const runRowCache = new RunRowCache();

/** The run lifecycle events that can move a settled row (a resume, a re-finish). */
export const RUN_ROW_HOLD_EVENTS = [
  'pipeline:run:started', 'pipeline:run:resumed', 'pipeline:run:completed', 'pipeline:run:failed',
  'pipeline:run:cancelled', 'pipeline:run:orphaned', 'pipeline:run:stuck',
] as const;

interface RunEventBus {
  subscribe(name: string, handler: (event: { payload?: { runId?: unknown } }) => unknown): string;
}

/**
 * Holds a run's key on each of its lifecycle events, on the replica that
 * observes it (the cache is shared, so one replica's hold covers them all).
 * Belt and braces over the repository's own holds: a writer that goes around
 * the repository still announces its transition on the bus.
 */
export function startRunRowCacheInvalidation(eventBus: RunEventBus, table: string, cache: RunRowCache = runRowCache): string[] {
  return RUN_ROW_HOLD_EVENTS.map((name) => eventBus.subscribe(name, (event) => {
    const runId = event?.payload?.runId;
    if (typeof runId === 'string') void cache.hold(table, [runId]);
  }));
}
