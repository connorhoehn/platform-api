import { describe, it, expect } from '@jest/globals';
import { BatchGetCommand, ScanCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { RunRowLocator } from '../runRowLocator';

type Row = Record<string, unknown>;

/** A tiny pipeline-runs table: Scan pages two rows at a time, BatchGet by key. */
function table(rows: Row[]) {
  const calls: string[] = [];
  const client = {
    send: async (command: unknown) => {
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
      if (command instanceof BatchGetCommand) {
        calls.push('get');
        const [name, request] = Object.entries(command.input.RequestItems ?? {})[0]!;
        const keys = (request.Keys ?? []) as Row[];
        return {
          Responses: {
            [name]: rows.filter((row) => keys.some((key) => key.pipelineId === row.pipelineId && key.runId === row.runId)),
          },
        };
      }
      throw new Error('unexpected command');
    },
  } as unknown as DynamoDBDocumentClient;
  return { client, calls, rows };
}

describe('RunRowLocator', () => {
  it('scans for a run the first time and reads it by key after that, fresh each time', async () => {
    const t = table([
      { pipelineId: 'p1', runId: 'r1', status: 'running' },
      { pipelineId: 'p1', runId: 'r2' },
      { pipelineId: 'p2', runId: 'r3' },
    ]);
    const locator = new RunRowLocator();
    const first = await locator.rows(t.client, 'runs', ['r3', 'r1', 'missing']);
    expect(first.get('r3')?.pipelineId).toBe('p2');
    expect(first.get('missing')).toBeNull();
    expect(t.calls).toEqual(['scan', 'scan']); // paged past the first page
    expect(locator.known).toBe(2);

    // The row changed; the key did not. A keyed read returns the row as it is now.
    t.rows[0]!.status = 'completed';
    t.calls.length = 0;
    const second = await locator.rows(t.client, 'runs', ['r1', 'r3']);
    expect(t.calls).toEqual(['get']);
    expect(second.get('r1')?.status).toBe('completed');
  });

  it('forgets a key whose row is gone and looks for it again', async () => {
    const t = table([{ pipelineId: 'p1', runId: 'r1' }]);
    const locator = new RunRowLocator();
    await locator.rows(t.client, 'runs', ['r1']);
    t.rows.length = 0;
    t.calls.length = 0;
    expect((await locator.rows(t.client, 'runs', ['r1'])).get('r1')).toBeNull();
    expect(t.calls).toEqual(['get', 'scan']);
    expect(locator.known).toBe(0);
  });

  it('treats two rows claiming one run id as no row, and never remembers either', async () => {
    const t = table([{ pipelineId: 'p1', runId: 'dup' }, { pipelineId: 'p2', runId: 'dup' }]);
    const locator = new RunRowLocator();
    expect((await locator.rows(t.client, 'runs', ['dup'])).get('dup')).toBeNull();
    expect(locator.known).toBe(0);
  });

  it('holds a bounded number of keys, oldest out first', async () => {
    const t = table([{ pipelineId: 'p', runId: 'a' }, { pipelineId: 'p', runId: 'b' }, { pipelineId: 'p', runId: 'c' }]);
    const locator = new RunRowLocator(2);
    await locator.rows(t.client, 'runs', ['a', 'b', 'c']);
    expect(locator.known).toBe(2);
    t.calls.length = 0;
    await locator.rows(t.client, 'runs', ['a']);
    expect(t.calls[0]).toBe('scan');
  });
});
