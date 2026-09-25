import { describe, it, expect } from '@jest/globals';
import { BatchGetCommand, QueryCommand, ScanCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { PIPELINE_RUNS_RUN_ID_INDEX, RunRowLocator } from '../runRowLocator';
import { RedisRunKeyStore } from '../runKeyMap';

type Row = Record<string, unknown>;

function missingIndex(): Error {
  const err = new Error('The table does not have the specified index: runId-index');
  err.name = 'ValidationException';
  return err;
}

/** A tiny pipeline-runs table: Scan pages two rows at a time; Query the runId index when it exists. */
function table(rows: Row[], opts: { index?: boolean; unprocessedOnce?: boolean } = {}) {
  const calls: string[] = [];
  const state = { index: opts.index ?? true, unprocessed: opts.unprocessedOnce ?? false };
  const client = {
    send: async (command: unknown) => {
      if (command instanceof BatchGetCommand) {
        calls.push('batch');
        const [[name, request]] = Object.entries(command.input.RequestItems ?? {}) as [[string, { Keys: Row[] }]];
        let keys = request.Keys;
        let unprocessed: Row[] = [];
        if (state.unprocessed && keys.length > 1) {
          state.unprocessed = false;
          unprocessed = keys.slice(1);
          keys = keys.slice(0, 1);
        }
        const items = keys.flatMap((key) => rows.filter((row) => row.pipelineId === key.pipelineId && row.runId === key.runId));
        return { Responses: { [name]: items }, ...(unprocessed.length ? { UnprocessedKeys: { [name]: { Keys: unprocessed } } } : {}) };
      }
      if (command instanceof QueryCommand) {
        calls.push('query');
        if (!state.index || command.input.IndexName !== PIPELINE_RUNS_RUN_ID_INDEX) throw missingIndex();
        const runId = command.input.ExpressionAttributeValues?.[':r'];
        return { Items: rows.filter((row) => row.runId === runId) };
      }
      if (command instanceof ScanCommand) {
        calls.push('scan');
        const input = command.input;
        const wanted = new Set(Object.values(input.ExpressionAttributeValues ?? {}));
        const start = Number((input.ExclusiveStartKey as { at?: number } | undefined)?.at ?? 0);
        const page = rows.slice(start, start + 2);
        return {
          Items: page.filter((row) => wanted.has(row.runId)),
          ScannedCount: page.length,
          ...(start + 2 < rows.length ? { LastEvaluatedKey: { at: start + 2 } } : {}),
        };
      }
      throw new Error('unexpected command');
    },
  } as unknown as DynamoDBDocumentClient;
  return { client, calls, rows, state };
}

describe('RunRowLocator', () => {
  it('reads each run through the runId index, fresh each time, and never scans', async () => {
    const t = table([
      { pipelineId: 'p1', runId: 'r1', status: 'running' },
      { pipelineId: 'p1', runId: 'r2' },
      { pipelineId: 'p2', runId: 'r3' },
    ]);
    const locator = new RunRowLocator();
    const first = await locator.rows(t.client, 'runs', ['r3', 'r1', 'missing', 'r1']);
    expect(first.get('r3')?.pipelineId).toBe('p2');
    expect(first.get('missing')).toBeNull();
    expect(t.calls).toEqual(['query', 'query', 'query']); // one per distinct id

    t.rows[0]!.status = 'completed';
    const second = await locator.rows(t.client, 'runs', ['r1']);
    expect(second.get('r1')?.status).toBe('completed');
    expect(t.calls).not.toContain('scan');
  });

  it("skips an agent loop's person-day pointer row, on the index and on the Scan", async () => {
    const rows = [
      { pipelineId: 'chat-agent-loop', runId: 'loop-1', status: 'completed', triggeredBy: 'u1' },
      { pipelineId: 'loops-by-person#u1#2026-09-23', runId: 'loop-1', status: 'index', triggeredBy: 'u1' },
    ];
    for (const index of [true, false]) {
      const found = await new RunRowLocator().rows(table(rows, { index }).client, 'runs', ['loop-1']);
      expect(found.get('loop-1')?.pipelineId).toBe('chat-agent-loop');
    }
    const onlyPointer = await new RunRowLocator().rows(table([rows[1]!]).client, 'runs', ['loop-1']);
    expect(onlyPointer.get('loop-1')).toBeNull();
  });

  it('treats two rows claiming one run id as no row', async () => {
    const t = table([{ pipelineId: 'p1', runId: 'dup' }, { pipelineId: 'p2', runId: 'dup' }]);
    expect((await new RunRowLocator().rows(t.client, 'runs', ['dup'])).get('dup')).toBeNull();
    const s = table([{ pipelineId: 'p1', runId: 'dup' }, { pipelineId: 'p2', runId: 'dup' }], { index: false });
    expect((await new RunRowLocator().rows(s.client, 'runs', ['dup'])).get('dup')).toBeNull();
  });

  it('falls back to the paged Scan on a table without the index, and stops asking for it until the recheck', async () => {
    let clock = 1_000;
    const t = table([{ pipelineId: 'p1', runId: 'r1' }, { pipelineId: 'p1', runId: 'r2' }, { pipelineId: 'p2', runId: 'r3' }], { index: false });
    const locator = new RunRowLocator(() => clock);
    const first = await locator.rows(t.client, 'runs', ['r3', 'r1']);
    expect(first.get('r3')?.pipelineId).toBe('p2');
    expect(first.get('r1')?.pipelineId).toBe('p1');
    expect(t.calls[0]).toBe('query');
    expect(t.calls.filter((c) => c === 'scan').length).toBeGreaterThan(0);
    expect(locator.usesIndex('runs')).toBe(false);

    t.calls.length = 0;
    await locator.rows(t.client, 'runs', ['r2']);
    expect(t.calls.every((c) => c === 'scan')).toBe(true);

    // The GSI is added to the live table; after the recheck interval the index is used again.
    t.state.index = true;
    clock += 5 * 60_000;
    t.calls.length = 0;
    expect((await locator.rows(t.client, 'runs', ['r2'])).get('r2')?.pipelineId).toBe('p1');
    expect(t.calls).toEqual(['query']);
    expect(locator.usesIndex('runs')).toBe(true);
  });

  it('an error that is not a missing index is not swallowed into a Scan', async () => {
    const client = { send: async () => { const e = new Error('throughput exceeded'); e.name = 'ProvisionedThroughputExceededException'; throw e; } } as unknown as DynamoDBDocumentClient;
    await expect(new RunRowLocator().rows(client, 'runs', ['r1'])).rejects.toThrow(/throughput/);
  });
});

describe('RunRowLocator inside one graph build (NFR #16)', () => {
  it('reads each run once across the ACL pass and the activity layer, and fresh in the next build', async () => {
    const { withBuildMemo } = await import('../../buildMemo.js');
    const rows = [{ pipelineId: 'p', runId: 'r1' }, { pipelineId: 'p', runId: 'r2' }];
    const t = table(rows);
    const locator = new RunRowLocator();
    await withBuildMemo(async () => {
      const acl = await locator.rows(t.client, 'pipeline-runs', ['r1', 'r2']);
      const activity = await locator.rows(t.client, 'pipeline-runs', ['r2', 'r1']);
      expect(activity.get('r1')).toEqual(acl.get('r1'));
      expect(activity.get('r2')).toEqual(rows[1]);
    });
    expect(t.calls.filter((call) => call === 'query')).toHaveLength(2);
    await withBuildMemo(() => locator.rows(t.client, 'pipeline-runs', ['r1']));
    expect(t.calls.filter((call) => call === 'query')).toHaveLength(3);
  });
});

/** An in-memory stand-in for the cache Redis. */
function memoryCache() {
  const values = new Map<string, string>();
  const client = {
    mGet: async (keys: string[]) => keys.map((key) => values.get(key) ?? null),
    get: async (key: string) => values.get(key) ?? null,
    set: async (key: string, value: string) => { values.set(key, value); return 'OK'; },
    del: async (keys: string | string[]) => { for (const key of [keys].flat()) values.delete(key); return 1; },
  };
  return { values, store: new RedisRunKeyStore(async () => client as never) };
}

const flush = () => new Promise((resolve) => setImmediate(resolve));

describe('RunRowLocator with remembered run keys (NFR #147)', () => {
  it('reads a remembered run by its table key in one BatchGetItem, fresh, and the rest through the index', async () => {
    const t = table([
      { pipelineId: 'p1', runId: 'r1', status: 'running' },
      { pipelineId: 'p2', runId: 'r2' },
      { pipelineId: 'p2', runId: 'r3' },
    ]);
    const { store, values } = memoryCache();
    const locator = new RunRowLocator(Date.now, store);
    const first = await locator.rows(t.client, 'runs', ['r1', 'r2', 'missing']);
    expect(t.calls).toEqual(['query', 'query', 'query']);
    expect(first.get('missing')).toBeNull();
    await flush();
    expect([...values.keys()].sort()).toEqual(['wg:runkey:runs:r1', 'wg:runkey:runs:r2']);

    t.calls.length = 0;
    t.rows[0]!.status = 'completed';
    const second = await locator.rows(t.client, 'runs', ['r1', 'r2', 'r3']);
    expect(second.get('r1')?.status).toBe('completed');
    expect(second.get('r3')?.pipelineId).toBe('p2');
    expect(t.calls).toEqual(['batch', 'query']);
  });

  it('retries UnprocessedKeys', async () => {
    const t = table([{ pipelineId: 'p', runId: 'a' }, { pipelineId: 'p', runId: 'b' }], { unprocessedOnce: true });
    const { store, values } = memoryCache();
    values.set('wg:runkey:runs:a', 'p');
    values.set('wg:runkey:runs:b', 'p');
    const found = await new RunRowLocator(Date.now, store).rows(t.client, 'runs', ['a', 'b']);
    expect(found.get('a')?.runId).toBe('a');
    expect(found.get('b')?.runId).toBe('b');
    expect(t.calls).toEqual(['batch', 'batch']);
  });

  it('a remembered key whose row is gone goes back to the index and is forgotten', async () => {
    const t = table([{ pipelineId: 'p2', runId: 'moved' }]);
    const { store, values } = memoryCache();
    values.set('wg:runkey:runs:gone', 'p1');
    values.set('wg:runkey:runs:moved', 'p1');
    const found = await new RunRowLocator(Date.now, store).rows(t.client, 'runs', ['gone', 'moved']);
    expect(found.get('gone')).toBeNull();
    expect(found.get('moved')?.pipelineId).toBe('p2');
    await flush();
    expect(values.get('wg:runkey:runs:gone')).toBeUndefined();
    expect(values.get('wg:runkey:runs:moved')).toBe('p2');
  });

  it('never remembers an ambiguous id, and a write naming another pipeline drops a remembered key', async () => {
    const t = table([{ pipelineId: 'p1', runId: 'dup' }, { pipelineId: 'p2', runId: 'dup' }]);
    const { store, values } = memoryCache();
    const locator = new RunRowLocator(Date.now, store);
    expect((await locator.rows(t.client, 'runs', ['dup'])).get('dup')).toBeNull();
    await flush();
    expect(values.size).toBe(0);

    values.set('wg:runkey:runs:solo', 'p1');
    await store.noteWrite('runs', 'p1', 'solo');
    expect(values.get('wg:runkey:runs:solo')).toBe('p1');
    await store.noteWrite('runs', 'p9', 'solo');
    expect(values.get('wg:runkey:runs:solo')).toBeUndefined();
  });

  it('never takes an agent loop pointer row for the run, even under a remembered key', async () => {
    const t = table([{ pipelineId: 'loops-by-person#u1', runId: 'loop', status: 'index' }, { pipelineId: 'chat-agent-loop', runId: 'loop' }]);
    const { store, values } = memoryCache();
    values.set('wg:runkey:runs:loop', 'loops-by-person#u1');
    const found = await new RunRowLocator(Date.now, store).rows(t.client, 'runs', ['loop']);
    expect(found.get('loop')?.pipelineId).toBe('chat-agent-loop');
  });

  it('reads through the index when the cache is unreachable', async () => {
    const t = table([{ pipelineId: 'p', runId: 'r' }]);
    const locator = new RunRowLocator(Date.now, new RedisRunKeyStore(async () => null));
    expect((await locator.rows(t.client, 'runs', ['r'])).get('r')?.pipelineId).toBe('p');
    expect(t.calls).toEqual(['query']);
  });
});

describe('RunRowLocator projection inside a graph build (NFR #157)', () => {
  const keys = {
    get: async () => new Map([['r1', 'p']]),
    fill: async () => undefined,
    drop: async () => undefined,
  };
  const recording = () => {
    const inputs: Array<Record<string, unknown>> = [];
    const client = {
      send: async (command: unknown) => {
        const input = (command as BatchGetCommand).input;
        inputs.push(input.RequestItems!['pipeline-runs'] as Record<string, unknown>);
        return { Responses: { 'pipeline-runs': [{ pipelineId: 'p', runId: 'r1', status: 'completed' }] } };
      },
    } as unknown as DynamoDBDocumentClient;
    return { inputs, client };
  };

  it('a build reads remembered runs without the definition snapshot or the resume checkpoint', async () => {
    const { withBuildMemo } = await import('../../buildMemo.js');
    const { inputs, client } = recording();
    await withBuildMemo(() => new RunRowLocator(Date.now, keys).rows(client, 'pipeline-runs', ['r1']));
    const projection = String(inputs[0]!.ProjectionExpression);
    const names = Object.values(inputs[0]!.ExpressionAttributeNames as Record<string, string>);
    expect(projection).toContain('#ck.#ctx.#plan');
    expect(names).toEqual(expect.arrayContaining(['stepsSummary', 'triggeredBy', 'checkpoint', 'plan']));
    expect(names).not.toContain('definitionSnapshot');
  });

  it('whole rows outside a build, and inside one that asked for a full read', async () => {
    const { withBuildMemo, withFullReads } = await import('../../buildMemo.js');
    const { inputs, client } = recording();
    await new RunRowLocator(Date.now, keys).rows(client, 'pipeline-runs', ['r1']);
    await withFullReads(() => withBuildMemo(() => new RunRowLocator(Date.now, keys).rows(client, 'pipeline-runs', ['r1'])));
    expect(inputs.map((input) => input.ProjectionExpression)).toEqual([undefined, undefined]);
  });
});
