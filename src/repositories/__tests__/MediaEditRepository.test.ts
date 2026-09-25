import { BatchGetCommand, GetCommand, QueryCommand, TransactWriteCommand, UpdateCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';

jest.mock('../../lib/aws-clients', () => ({ docClient: { send: jest.fn() }, ddbClient: { send: jest.fn() } }));
jest.mock('../../lib/ddb-table-name', () => ({ tableName: (name: string) => `test-${name}` }));

import { MediaEditRepository, PhotoEditConflict, versionSk } from '../MediaEditRepository';

const failed = () => Object.assign(new Error('cancelled'), { name: 'TransactionCanceledException' });

/** One table keyed fileId|sk; evaluates exactly the conditions the repository writes. */
function fakeTable() {
  const items = new Map<string, Record<string, any>>();
  const key = (k: Record<string, any>) => `${k.fileId}|${k.sk}`;
  const cond = (row: Record<string, any> | undefined, input: Record<string, any>): boolean => {
    const expr: string = input.ConditionExpression ?? '';
    const v = input.ExpressionAttributeValues ?? {};
    if (!expr) return true;
    if (expr === 'attribute_not_exists(sk)') return !row;
    if (expr === 'attribute_not_exists(sk) OR currentVersion = :cur') return !row || row.currentVersion === v[':cur'];
    if (expr === 'attribute_exists(sk) AND attribute_not_exists(renderedImageId)') return !!row && row.renderedImageId === undefined;
    throw new Error(`unknown condition ${expr}`);
  };
  const update = (k: Record<string, any>, input: Record<string, any>) => {
    const row = items.get(key(k)) ?? { ...k };
    const v = input.ExpressionAttributeValues;
    for (const part of String(input.UpdateExpression).replace(/^SET /, '').split(/,(?![^(]*\))/)) {
      const [lhs, rhs] = part.split('=').map((s) => s.trim());
      const m = /^if_not_exists\((\w+), (:\w+)\)$/.exec(rhs!);
      row[lhs!] = m ? (row[m[1]!] ?? v[m[2]!]) : structuredClone(v[rhs!]);
    }
    items.set(key(k), row);
  };
  const client = {
    async send(command: any) {
      const input = command.input;
      if (command instanceof GetCommand) return { Item: structuredClone(items.get(key(input.Key))) };
      if (command instanceof BatchGetCommand) {
        const t = Object.keys(input.RequestItems)[0]!;
        return { Responses: { [t]: input.RequestItems[t].Keys.map((k: any) => items.get(key(k))).filter(Boolean) } };
      }
      if (command instanceof QueryCommand) {
        const v = input.ExpressionAttributeValues;
        let list = [...items.values()].filter((i) => i.fileId === v[':f'] && (v[':p'] ? i.sk.startsWith(v[':p']) : i.sk >= v[':lo'] && i.sk <= v[':hi']));
        list.sort((a, b) => a.sk.localeCompare(b.sk));
        if (input.ScanIndexForward === false) list = list.reverse();
        const page = list.slice(0, input.Limit);
        return { Items: structuredClone(page), ...(list.length > input.Limit ? { LastEvaluatedKey: { fileId: v[':f'], sk: page[page.length - 1].sk } } : {}) };
      }
      if (command instanceof UpdateCommand) {
        if (!cond(items.get(key(input.Key)), input)) throw Object.assign(new Error('cc'), { name: 'ConditionalCheckFailedException' });
        update(input.Key, input);
        return {};
      }
      if (command instanceof TransactWriteCommand) {
        await new Promise((r) => setImmediate(r));
        for (const t of input.TransactItems) {
          const op = t.Put ?? t.Update;
          const k = t.Put ? { fileId: t.Put.Item.fileId, sk: t.Put.Item.sk } : t.Update.Key;
          if (!cond(items.get(key(k)), op)) throw failed();
        }
        for (const t of input.TransactItems) {
          if (t.Put) items.set(key(t.Put.Item), structuredClone(t.Put.Item));
          else update(t.Update.Key, t.Update);
        }
        return {};
      }
      throw new Error(`unexpected ${command.constructor.name}`);
    },
  } as unknown as DynamoDBDocumentClient;
  return { client, items };
}

const F = 'a'.repeat(64);
const newRepo = () => {
  const t = fakeTable();
  return { ...t, repo: new MediaEditRepository(t.client, 'test-media-edits', async () => {}) };
};
const keep = (over: Record<string, unknown> = {}) => ({
  fileId: F, ops: [{ kind: 'exposure', stops: 0.15 }], summary: 'Brighter', source: 'user' as const, userId: 'dev-connor', ...over,
});

describe('MediaEditRepository (media photo editor)', () => {
  it('a Keep writes v1 as its own item and moves HEAD; a stale parent is a conflict', async () => {
    const { repo, items } = newRepo();
    const out = await repo.appendVersion(keep({ parentVersion: 0, suggestionId: 's1' }));
    expect(out).toMatchObject({ replay: false, version: { version: 1, parentVersion: null } });
    expect(items.has(`${F}|${versionSk(1)}`)).toBe(true);
    expect((await repo.getHead(F)).currentVersion).toBe(1);
    await expect(repo.appendVersion(keep({ parentVersion: 0, suggestionId: 's2' }))).rejects.toBeInstanceOf(PhotoEditConflict);
    expect((await repo.getHead(F)).currentVersion).toBe(1);
  });

  it('a replayed suggestion answers the version it wrote and writes nothing', async () => {
    const { repo, items } = newRepo();
    await repo.appendVersion(keep({ parentVersion: 0, suggestionId: 's1' }));
    const size = items.size;
    const again = await repo.appendVersion(keep({ parentVersion: 0, suggestionId: 's1' }));
    expect(again).toMatchObject({ replay: true, version: { version: 1 } });
    expect(items.size).toBe(size);
  });

  it('the same suggestion kept twice at once lands once', async () => {
    const { repo } = newRepo();
    const [a, b] = await Promise.all([
      repo.appendVersion(keep({ parentVersion: 0, suggestionId: 'dup' })),
      repo.appendVersion(keep({ parentVersion: 0, suggestionId: 'dup' })),
    ]);
    expect([a.version.version, b.version.version]).toEqual([1, 1]);
    expect([a.replay, b.replay].sort()).toEqual([false, true]);
    expect((await repo.listVersions(F)).versions).toHaveLength(1);
  });

  it('20 racing unconditioned appends land as v1..v20, HEAD at 20', async () => {
    const { repo } = newRepo();
    const out = await Promise.all(Array.from({ length: 20 }, (_v, i) => repo.appendVersion(keep({ suggestionId: `r${i}` }))));
    expect(out.map((o) => o.version.version).sort((a, b) => a - b)).toEqual(Array.from({ length: 20 }, (_v, i) => i + 1));
    expect((await repo.getHead(F)).currentVersion).toBe(20);
  });

  it('pages versions newest first by one Query each', async () => {
    const { repo } = newRepo();
    for (let i = 0; i < 5; i++) await repo.appendVersion(keep({ parentVersion: i, suggestionId: `p${i}` }));
    const first = await repo.listVersions(F, { limit: 2 });
    expect(first.versions.map((v) => v.version)).toEqual([5, 4]);
    const second = await repo.listVersions(F, { limit: 2, before: first.next! });
    expect(second.versions.map((v) => v.version)).toEqual([3, 2]);
  });

  it('masks and pick live on HEAD without touching versions; a render is set once', async () => {
    const { repo } = newRepo();
    await repo.setMasks(F, [{ maskId: 'people', label: 'People', kind: 'people', subjects: 2, polygons: [[[0, 0], [1, 0], [1, 1]]], blobHash: 'b'.repeat(64), origin: 'seed' }], 'u');
    await repo.setPick(F, 'pick', 'u');
    let head = await repo.getHead(F);
    expect(head).toMatchObject({ currentVersion: 0, pick: 'pick', masks: [{ maskId: 'people' }] });
    await repo.appendVersion(keep({ parentVersion: 0, suggestionId: 'm' }));
    head = await repo.getHead(F);
    expect(head).toMatchObject({ currentVersion: 1, pick: 'pick', masks: [{ maskId: 'people' }] });
    await repo.setRendered(F, 1, 'img-1');
    await repo.setRendered(F, 1, 'img-2');
    expect((await repo.getVersion(F, 1))?.renderedImageId).toBe('img-1');
  });
});
