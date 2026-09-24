import { describe, it, expect } from '@jest/globals';
import { BatchGetCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { RunRowLocator, projectGraphRunRow } from '../runRowLocator';
import { RedisRunKeyStore } from '../runKeyMap';
import { RunRowCache, SETTLE_MS, isSettledRunRow, startRunRowCacheInvalidation } from '../runRowCache';
import { withBuildMemo } from '../../buildMemo';

type Row = Record<string, unknown>;
const NOW = Date.parse('2026-09-24T12:00:00.000Z');
const old = new Date(NOW - SETTLE_MS - 1_000).toISOString();

/** The cache Redis, in memory, honouring NX. */
function memoryRedis() {
  const values = new Map<string, string>();
  const client = {
    mGet: async (keys: string[]) => keys.map((key) => values.get(key) ?? null),
    get: async (key: string) => values.get(key) ?? null,
    set: async (key: string, value: string, opts?: { NX?: boolean }) => {
      if (opts?.NX && values.has(key)) return null;
      values.set(key, value);
      return 'OK';
    },
    del: async (keys: string | string[]) => { for (const key of [keys].flat()) values.delete(key); return 1; },
  };
  return { values, client: async () => client as never };
}

function runsTable(rows: Row[]) {
  const batches: string[][] = [];
  const client = {
    send: async (command: unknown) => {
      if (!(command instanceof BatchGetCommand)) throw new Error('only keyed reads expected');
      const [[name, request]] = Object.entries(command.input.RequestItems ?? {}) as [[string, { Keys: Row[] }]];
      batches.push(request.Keys.map((key) => key.runId as string));
      return { Responses: { [name]: request.Keys.flatMap((key) => rows.filter((row) => row.runId === key.runId).map((row) => ({ ...row }))) } };
    },
  } as unknown as DynamoDBDocumentClient;
  return { client, batches, rows };
}

const flush = () => new Promise((resolve) => setImmediate(resolve));

describe('run row cache (Loop 32 (activity))', () => {
  it('keeps only settled terminal rows', () => {
    expect(isSettledRunRow({ status: 'completed', completedAt: old }, NOW)).toBe(true);
    expect(isSettledRunRow({ status: 'running', startedAt: old }, NOW)).toBe(false);
    expect(isSettledRunRow({ status: 'completed', completedAt: new Date(NOW - 5_000).toISOString() }, NOW)).toBe(false);
    expect(isSettledRunRow({ status: 'index', completedAt: old }, NOW)).toBe(false);
  });

  function setup() {
    const redis = memoryRedis();
    const t = runsTable([
      { pipelineId: 'p', runId: 'done', status: 'completed', triggeredBy: 'u', completedAt: old, definitionSnapshot: { big: true } },
      { pipelineId: 'p', runId: 'live', status: 'running', triggeredBy: 'u', startedAt: old },
    ]);
    const keys = new RedisRunKeyStore(redis.client);
    const cache = new RunRowCache(redis.client, () => NOW);
    redis.values.set('wg:runkey:runs:done', 'p');
    redis.values.set('wg:runkey:runs:live', 'p');
    return { redis, t, cache, locator: new RunRowLocator(() => NOW, keys, cache) };
  }

  it('a build reads a settled run from the cache the next time, a live one always from DynamoDB, and the same row either way', async () => {
    const { t, locator } = setup();
    const first = await withBuildMemo(() => locator.rows(t.client, 'runs', ['done', 'live']));
    await flush();
    const second = await withBuildMemo(() => locator.rows(t.client, 'runs', ['done', 'live']));
    expect(t.batches).toEqual([['done', 'live'], ['live']]);
    // The projected row a keyed read returns (this stand-in table does not project).
    expect(second.get('done')).toEqual(projectGraphRunRow(first.get('done') as Row));
    expect(second.get('done')).not.toHaveProperty('definitionSnapshot');
  });

  it('a write holds the key: the next build reads DynamoDB, and a racing fill lands nothing', async () => {
    const { t, locator, cache } = setup();
    await withBuildMemo(() => locator.rows(t.client, 'runs', ['done']));
    await flush();
    t.rows[0]!.review = { decision: 'accept' };
    await cache.hold('runs', ['done']);
    const after = await withBuildMemo(() => locator.rows(t.client, 'runs', ['done']));
    await flush();
    expect(after.get('done')?.review).toEqual({ decision: 'accept' });
    expect(t.batches).toEqual([['done'], ['done']]);
    // The fill after that read found the hold: still read fresh.
    await withBuildMemo(() => locator.rows(t.client, 'runs', ['done']));
    expect(t.batches).toHaveLength(3);
  });

  it('a lifecycle event on the bus holds the run', async () => {
    const { t, locator, cache, redis } = setup();
    await withBuildMemo(() => locator.rows(t.client, 'runs', ['done']));
    await flush();
    const handlers = new Map<string, (event: { payload?: { runId?: unknown } }) => unknown>();
    startRunRowCacheInvalidation({ subscribe: (name, handler) => { handlers.set(name, handler); return name; } }, 'runs', cache);
    handlers.get('pipeline:run:resumed')!({ payload: { runId: 'done' } });
    await flush();
    expect(redis.values.get('wg:runrow:v1:runs:done')).toBe('__hold');
  });

  it('a full-read build (the drift check) never reads or fills the cache', async () => {
    const { t, cache, redis } = setup();
    const locator = new RunRowLocator(() => NOW, new RedisRunKeyStore(redis.client), cache);
    await locator.rows(t.client, 'runs', ['done']);
    await flush();
    expect([...redis.values.keys()].filter((key) => key.startsWith('wg:runrow'))).toEqual([]);
  });
});
