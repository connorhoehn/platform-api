// The rollup table's conditional writes and the chunked batch read, against a
// fake document client that honours the three condition expressions used.

import { PipelineRunRollupRepository, RollupVersionConflict } from '../PipelineRunRollupRepository';
import { emptyRollup, type PipelineRunRollup } from '../../pipeline/run-rollup';

jest.mock('../../lib/aws-clients', () => ({ docClient: {}, ddbClient: {} }));

interface Cmd { constructor: { name: string }; input: Record<string, any> }

class FakeDoc {
  rows = new Map<string, PipelineRunRollup>();
  batchCalls: number[] = [];
  async send(cmd: Cmd): Promise<any> {
    const name = cmd.constructor.name;
    const input = cmd.input;
    if (name === 'GetCommand') {
      const row = this.rows.get(input.Key.pipelineId);
      return { Item: row ? { ...row } : undefined };
    }
    if (name === 'BatchGetCommand') {
      const table = Object.keys(input.RequestItems)[0];
      const keys: Array<{ pipelineId: string }> = input.RequestItems[table].Keys;
      this.batchCalls.push(keys.length);
      return { Responses: { [table]: keys.map((k) => this.rows.get(k.pipelineId)).filter(Boolean).map((r) => ({ ...r })) } };
    }
    if (name === 'PutCommand') {
      const item = input.Item as PipelineRunRollup;
      const cur = this.rows.get(item.pipelineId);
      const cond: string = input.ConditionExpression;
      const vals = input.ExpressionAttributeValues ?? {};
      let ok = true;
      if (cond === 'attribute_not_exists(pipelineId)') ok = cur === undefined;
      else if (cond === 'updatedAt = :seen') ok = cur !== undefined && cur.updatedAt === vals[':seen'];
      else if (cond === 'attribute_not_exists(pipelineId) OR updatedAt < :at') ok = cur === undefined || cur.updatedAt < vals[':at'];
      else throw new Error(`unexpected condition ${cond}`);
      if (!ok) { const e = new Error('cond'); (e as any).name = 'ConditionalCheckFailedException'; throw e; }
      this.rows.set(item.pipelineId, { ...item });
      return {};
    }
    throw new Error(`unexpected ${name}`);
  }
}

function repo(doc: FakeDoc) {
  return new PipelineRunRollupRepository(doc as any, 'pipeline-run-rollups', async () => undefined);
}

describe('PipelineRunRollupRepository', () => {
  it('putIfUnchanged: creates when no row was seen, then CASes on updatedAt', async () => {
    const doc = new FakeDoc();
    const r = repo(doc);
    const v1 = emptyRollup('p', '2026-01-01T00:00:01.000Z');
    await r.putIfUnchanged(v1, null);
    await expect(r.putIfUnchanged(v1, null)).rejects.toBeInstanceOf(RollupVersionConflict);
    const v2 = { ...v1, updatedAt: '2026-01-01T00:00:02.000Z' };
    await r.putIfUnchanged(v2, v1);
    // A writer that still holds v1 loses.
    await expect(r.putIfUnchanged({ ...v1, updatedAt: '2026-01-01T00:00:03.000Z' }, v1)).rejects.toBeInstanceOf(RollupVersionConflict);
    expect((await r.get('p'))!.updatedAt).toBe('2026-01-01T00:00:02.000Z');
  });

  it('putIfNotNewer: the backfill yields to a newer row and reports it', async () => {
    const doc = new FakeDoc();
    const r = repo(doc);
    expect(await r.putIfNotNewer(emptyRollup('p', '2026-01-01T00:00:05.000Z'))).toBe(true);
    expect(await r.putIfNotNewer(emptyRollup('p', '2026-01-01T00:00:04.000Z'))).toBe(false);
    expect(await r.putIfNotNewer(emptyRollup('p', '2026-01-01T00:00:06.000Z'))).toBe(true);
    expect((await r.get('p'))!.updatedAt).toBe('2026-01-01T00:00:06.000Z');
  });

  it('getMany: every id answered, misses as null, chunked at 100 keys, duplicates collapsed', async () => {
    const doc = new FakeDoc();
    const r = repo(doc);
    doc.rows.set('a', emptyRollup('a', 'x'));
    doc.rows.set('z', emptyRollup('z', 'x'));
    const ids = ['a', 'a', ...Array.from({ length: 148 }, (_, i) => `m${i}`), 'z'];
    const out = await r.getMany(ids);
    expect(out.size).toBe(150);
    expect(out.get('a')).toMatchObject({ pipelineId: 'a' });
    expect(out.get('z')).toMatchObject({ pipelineId: 'z' });
    expect(out.get('m3')).toBeNull();
    expect(doc.batchCalls).toEqual([100, 50]);
  });

  it('getMany with nothing asked does not touch the client', async () => {
    const doc = new FakeDoc();
    expect((await repo(doc).getMany([])).size).toBe(0);
    expect(doc.batchCalls).toEqual([]);
  });
});
