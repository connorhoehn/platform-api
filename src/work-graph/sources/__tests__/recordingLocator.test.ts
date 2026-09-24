import { describe, it, expect } from '@jest/globals';
import { GetCommand, QueryCommand, ScanCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { RecordingLocator, RECORDINGS_RECORDING_ID_INDEX, parseRecordingResourceId } from '../recordingLocator';
import { withBuildMemo } from '../../buildMemo';

type Row = Record<string, unknown>;

function table(rows: Row[]) {
  const calls: string[] = [];
  const client = {
    send: async (command: unknown) => {
      if (command instanceof QueryCommand) {
        const id = command.input.ExpressionAttributeValues?.[':id'];
        const attribute = command.input.IndexName === RECORDINGS_RECORDING_ID_INDEX ? 'recordingId' : 'callId';
        calls.push(`query:${attribute}`);
        return { Items: rows.filter((row) => row[attribute] === id) };
      }
      if (command instanceof GetCommand) {
        calls.push('get');
        const key = command.input.Key as Row;
        return { Item: rows.find((row) => row.callId === key.callId && row.recordingId === key.recordingId) };
      }
      if (command instanceof ScanCommand) {
        calls.push('scan');
        const id = command.input.ExpressionAttributeValues?.[':id'];
        return { Items: rows.filter((row) => row.transcriptId === id) };
      }
      throw new Error('unexpected command');
    },
  } as unknown as DynamoDBDocumentClient;
  return { client, calls, rows };
}

function memoryCache() {
  const values = new Map<string, string>();
  const client = {
    get: async (key: string) => values.get(key) ?? null,
    set: async (key: string, value: string) => { values.set(key, value); return 'OK'; },
    del: async (keys: string | string[]) => { for (const key of [keys].flat()) values.delete(key); return 1; },
  };
  return { values, cache: async () => client as never };
}

const seeded = [
  { callId: 'c1', recordingId: 'r1', transcriptId: 't1', startedBy: 'u1' },
  { callId: 'c2', recordingId: 'r2', startedBy: 'u2' },
  { callId: 'c3', recordingId: 'r3a' },
  { callId: 'c3', recordingId: 'r3b' },
];

describe('RecordingLocator (NFR #148)', () => {
  it('parses the graph resource ids', () => {
    expect(parseRecordingResourceId('transcript:t1')).toEqual({ kind: 'transcript', id: 't1' });
    expect(parseRecordingResourceId('bare')).toEqual({ id: 'bare' });
  });

  it('finds a meeting by its callId and a recording by the recordingId index, with no Scan', async () => {
    const t = table(seeded);
    const locator = new RecordingLocator(memoryCache().cache);
    expect((await locator.find(t.client, 'rec', 'meeting:c1'))?.recordingId).toBe('r1');
    expect((await locator.find(t.client, 'rec', 'recording:r2'))?.callId).toBe('c2');
    expect(await locator.find(t.client, 'rec', 'meeting:c3')).toBeNull(); // two recordings: ambiguous
    expect(await locator.find(t.client, 'rec', 'meeting:nope')).toBeNull();
    expect(t.calls).not.toContain('scan');
  });

  it("finds a transcript addressed by its recording id, then one with its own id by a remembered key", async () => {
    const t = table(seeded);
    const { cache, values } = memoryCache();
    const locator = new RecordingLocator(cache);
    expect((await locator.find(t.client, 'rec', 'transcript:r2'))?.callId).toBe('c2');
    expect(t.calls).toEqual(['query:recordingId']);

    t.calls.length = 0;
    expect((await locator.find(t.client, 'rec', 'transcript:t1'))?.callId).toBe('c1');
    expect(t.calls).toEqual(['query:recordingId', 'scan']); // cold: one Scan fills the memory
    expect(values.get('wg:rec-tid:rec:t1')).toBe(JSON.stringify({ callId: 'c1', recordingId: 'r1' }));

    t.calls.length = 0;
    t.rows[0]!.startedBy = 'u9';
    const warm = await locator.find(t.client, 'rec', 'transcript:t1');
    expect(warm?.startedBy).toBe('u9'); // the row is read fresh
    expect(t.calls).toEqual(['query:recordingId', 'get']);
  });

  it('remembers a missing transcript briefly, and a recording write forgets it', async () => {
    const t = table(seeded);
    const { cache, values } = memoryCache();
    const locator = new RecordingLocator(cache);
    expect(await locator.find(t.client, 'rec', 'transcript:t-new')).toBeNull();
    expect(values.get('wg:rec-tid:rec:t-new')).toBe('-');
    t.calls.length = 0;
    expect(await locator.find(t.client, 'rec', 'transcript:t-new')).toBeNull();
    expect(t.calls).not.toContain('scan');

    t.rows.push({ callId: 'c9', recordingId: 'r9', transcriptId: 't-new' });
    await locator.noteWrite('rec', { recordingId: 'r9', transcriptId: 't-new' });
    expect((await locator.find(t.client, 'rec', 'transcript:t-new'))?.callId).toBe('c9');
  });

  it('a remembered key whose row no longer carries the transcript id is not believed', async () => {
    const t = table([{ callId: 'c1', recordingId: 'r1' }]);
    const { cache, values } = memoryCache();
    values.set('wg:rec-tid:rec:t1', JSON.stringify({ callId: 'c1', recordingId: 'r1' }));
    expect(await new RecordingLocator(cache).find(t.client, 'rec', 'transcript:t1')).toBeNull();
    expect(t.calls).toContain('scan');
  });

  it('reads each id once per build', async () => {
    const t = table(seeded);
    const locator = new RecordingLocator(memoryCache().cache);
    await withBuildMemo(async () => {
      await locator.find(t.client, 'rec', 'meeting:c1');
      await locator.find(t.client, 'rec', 'meeting:c1');
    });
    expect(t.calls).toEqual(['query:callId']);
  });
});
