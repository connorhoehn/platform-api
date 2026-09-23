import { describe, it, expect } from '@jest/globals';
import { QueryCommand, ScanCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { PIPELINE_RUNS_RUN_ID_INDEX, RunRowLocator } from '../runRowLocator';

type Row = Record<string, unknown>;

function missingIndex(): Error {
  const err = new Error('The table does not have the specified index: runId-index');
  err.name = 'ValidationException';
  return err;
}

/** A tiny pipeline-runs table: Scan pages two rows at a time; Query the runId index when it exists. */
function table(rows: Row[], opts: { index?: boolean } = {}) {
  const calls: string[] = [];
  const state = { index: opts.index ?? true };
  const client = {
    send: async (command: unknown) => {
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
