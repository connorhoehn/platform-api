// NFR #188: one item per revision, a transactional CAS append, a single-Query
// page, and the copy → verify → remove-array migration.

jest.mock('../../lib/aws-clients', () => ({ docClient: { send: jest.fn() }, ddbClient: { send: jest.fn() } }));
jest.mock('../../lib/ddb-table-name', () => ({ tableName: (name: string) => `test-${name}` }));
jest.mock('../deckReviseEvents.js', () => ({ publishDeckRevisionWritten: () => undefined }), { virtual: true });

import {
  GetCommand, PutCommand, QueryCommand, TransactWriteCommand, UpdateCommand, type DynamoDBDocumentClient,
} from '@aws-sdk/lib-dynamodb';
import { InMemoryImageBlobStore } from '../../pipeline/image/ImageBlobStore';
import { DdbPresentationDocumentStore, DOCUMENTS_TABLE, type PresentationDocumentRow, type StoredRevision } from '../presentationDocuments';
import { canonicalJson, revKey } from '../presentationRevisionItems';
import { migrateRevisionRow } from '../revisionItemsMigration';

const REVS = 'test-presentation-revisions';

function conditionalFailure(): Error {
  return Object.assign(new Error('The conditional request failed'), { name: 'ConditionalCheckFailedException' });
}

/** Two tables; evaluates exactly the conditions the store and the migration write. */
function fakeTables() {
  const rows = new Map<string, Record<string, any>>();
  const items = new Map<string, Record<string, any>>();
  const calls: string[] = [];
  const itemKey = (key: Record<string, any>) => `${key.documentId}|${key.rev}`;
  const headOk = (row: Record<string, any> | undefined, input: Record<string, any>): boolean => {
    const expr: string = input.ConditionExpression ?? '';
    const v = input.ExpressionAttributeValues ?? {};
    if (expr.startsWith('attribute_not_exists(documentId)')) return !row;
    if (!row) return false;
    if (expr.startsWith('attribute_exists(documentId) AND (revisionCount')) {
      if (typeof row.revisionCount === 'number') return row.revisionCount === v[':expected'];
      return (row.revisions?.length ?? 0) === v[':expected'] && (row.revisions !== undefined || v[':expected'] === 0);
    }
    if (expr.startsWith('size(revisions) = :count')) {
      return Array.isArray(row.revisions) && row.revisions.length === v[':count'] && row.updatedAt === v[':updatedAt'];
    }
    throw new Error(`unknown condition ${expr}`);
  };
  const applyUpdate = (row: Record<string, any>, input: Record<string, any>) => {
    const v = input.ExpressionAttributeValues ?? {};
    const names = input.ExpressionAttributeNames ?? {};
    const expr: string = input.UpdateExpression;
    if (expr.startsWith('REMOVE revisions')) {
      delete row.revisions;
      row.revisionCount ??= v[':count'];
      row.headRevision ??= v[':head'];
      return;
    }
    for (const part of expr.replace(/^SET /, '').split(',')) {
      const [lhs, rhs] = part.split('=').map((s) => s.trim());
      row[names[lhs!] ?? lhs!] = structuredClone(v[rhs!]);
    }
  };
  const client = {
    async send(command: any) {
      const input = command.input;
      calls.push(`${command.constructor.name}:${input.TableName ?? 'tx'}`);
      if (command instanceof GetCommand) {
        const found = input.TableName === REVS ? items.get(itemKey(input.Key)) : rows.get(input.Key.documentId);
        return { Item: found ? structuredClone(found) : undefined };
      }
      if (command instanceof QueryCommand) {
        const documentId = input.ExpressionAttributeValues[':d'];
        let list = [...items.values()].filter((item) => item.documentId === documentId).sort((a, b) => a.rev.localeCompare(b.rev));
        if (input.ScanIndexForward === false) list.reverse();
        if (input.ExclusiveStartKey) {
          const start = input.ExclusiveStartKey.rev;
          list = list.filter((item) => (input.ScanIndexForward === false ? item.rev < start : item.rev > start));
        }
        const page = input.Limit ? list.slice(0, input.Limit) : list;
        return { Items: structuredClone(page), ...(input.Limit && list.length > input.Limit ? { LastEvaluatedKey: { documentId, rev: page[page.length - 1].rev } } : {}) };
      }
      if (command instanceof PutCommand) {
        if (input.TableName === REVS) {
          if (items.has(itemKey(input.Item))) throw conditionalFailure();
          items.set(itemKey(input.Item), structuredClone(input.Item));
        } else {
          if (!headOk(rows.get(input.Item.documentId), input)) throw conditionalFailure();
          rows.set(input.Item.documentId, structuredClone(input.Item));
        }
        return {};
      }
      if (command instanceof UpdateCommand) {
        const row = rows.get(input.Key.documentId);
        if (!headOk(row, input)) throw conditionalFailure();
        applyUpdate(row!, input);
        return {};
      }
      if (command instanceof TransactWriteCommand) {
        await new Promise((resolve) => setImmediate(resolve));
        const [put, head] = input.TransactItems;
        const itemTaken = items.has(itemKey(put.Put.Item));
        const headInput = head.Put ?? head.Update;
        const headId = head.Put ? head.Put.Item.documentId : head.Update.Key.documentId;
        const headPass = headOk(rows.get(headId), headInput);
        if (itemTaken || !headPass) {
          throw Object.assign(new Error('Transaction cancelled'), {
            name: 'TransactionCanceledException',
            CancellationReasons: [{ Code: itemTaken ? 'ConditionalCheckFailed' : 'None' }, { Code: headPass ? 'None' : 'ConditionalCheckFailed' }],
          });
        }
        items.set(itemKey(put.Put.Item), structuredClone(put.Put.Item));
        if (head.Put) rows.set(headId, structuredClone(head.Put.Item));
        else applyUpdate(rows.get(headId)!, head.Update);
        return {};
      }
      throw new Error(`unexpected ${command.constructor.name}`);
    },
  } as unknown as DynamoDBDocumentClient;
  return { client, rows, items, calls };
}

const spec = (tag: string) => ({ title: `Deck ${tag}`, slides: [{ id: 'cover', layout: 'title', title: tag, sources: [] }] }) as any;
const input = (tag: string) => ({
  documentId: 'doc-1', title: 'Sprint review', createdBy: 'dev-connor',
  slides: [{ id: 'cover', title: 'Cover', svg: `<svg>${tag}</svg>` }], spec: spec(tag),
});
function newStore(client: DynamoDBDocumentClient) {
  return new DdbPresentationDocumentStore({ docClient: client, blobStore: new InMemoryImageBlobStore(), mirror: null, revisionTable: REVS });
}

describe('revisions as items (NFR #188)', () => {
  it('20 racing appends land as v1..v20, one item each, the row never grows', async () => {
    const tables = fakeTables();
    const store = newStore(tables.client);
    await store.appendRevision(input('first'));
    const written = await Promise.all(Array.from({ length: 19 }, (_v, i) => store.appendRevision(input(`r${i}`))));
    const ids = [ 'v1', ...written.map((w) => w.revision.id) ].sort((a, b) => Number(a.slice(1)) - Number(b.slice(1)));
    expect(ids).toEqual(Array.from({ length: 20 }, (_v, i) => `v${i + 1}`));
    expect(tables.items.size).toBe(20);
    const row = tables.rows.get('doc-1')!;
    expect(row.revisions).toBeUndefined();
    expect(row.revisionCount).toBe(20);
    expect(row.headRevision.id).toBe('v20');
    const full = await store.get('doc-1');
    expect(full!.revisions!.map((r) => r.id)).toEqual(ids);
  });

  it('pages newest first with one Query per page', async () => {
    const tables = fakeTables();
    const store = newStore(tables.client);
    for (let i = 0; i < 12; i += 1) await store.appendRevision(input(`p${i}`));
    tables.calls.length = 0;
    const first = await store.listRevisions('doc-1', { limit: 5 });
    expect(tables.calls).toEqual([`GetCommand:${DOCUMENTS_TABLE}`, `QueryCommand:${REVS}`]);
    expect(first!.revisions.map((r) => r.id)).toEqual(['v12', 'v11', 'v10', 'v9', 'v8']);
    expect(first!.total).toBe(12);
    const second = await store.listRevisions('doc-1', { limit: 5, before: first!.nextCursor! });
    expect(second!.revisions.map((r) => r.id)).toEqual(['v7', 'v6', 'v5', 'v4', 'v3']);
    const last = await store.listRevisions('doc-1', { limit: 5, before: second!.nextCursor! });
    expect(last!.revisions.map((r) => r.id)).toEqual(['v2', 'v1']);
    expect(last!.nextCursor).toBeNull();
    expect((await store.listRevisions('doc-1', { limit: 5, before: 'v99' }))!.missingCursor).toBe(true);
  });

  it('a stale expectedHead is refused with the full head (spec included, for a rebase)', async () => {
    const tables = fakeTables();
    const store = newStore(tables.client);
    await store.appendRevision(input('a'));
    await store.appendRevision(input('b'));
    await expect(store.appendRevision({ ...input('c'), expectedHead: 'v1' })).rejects.toMatchObject({
      name: 'PresentationRevisionConflictError', revisionCount: 2, head: expect.objectContaining({ id: 'v2', spec: spec('b') }),
    });
  });

  it('a legacy array row: appends become items, reads merge, the migration copies, verifies and removes only the array', async () => {
    const tables = fakeTables();
    const legacy: StoredRevision[] = Array.from({ length: 3 }, (_v, i) => ({
      id: `v${i + 1}`, label: `v${i + 1}`, createdAt: '2026-09-22T00:00:00.000Z', createdBy: 'dev-connor', slideCount: 1,
      asset: { key: `deck-k${i}`, contentType: 'x', sizeBytes: 1 }, specRef: { key: `deck-spec-${i}.json`, contentType: 'application/json', sizeBytes: 2, sha256: 'ab' },
    }));
    tables.rows.set('doc-1', { documentId: 'doc-1', title: 'Old', type: 'presentation', updatedAt: 't0', revisions: structuredClone(legacy), revisionPreviews: {} });
    const store = newStore(tables.client);

    // An append on the unmigrated row: v4 as an item, the array untouched.
    const { revision } = await store.appendRevision(input('new'));
    expect(revision.id).toBe('v4');
    expect(tables.rows.get('doc-1')!.revisions).toHaveLength(3);
    expect((await store.get('doc-1'))!.revisions!.map((r) => r.id)).toEqual(['v1', 'v2', 'v3', 'v4']);
    expect((await store.listRevisions('doc-1', { limit: 2 }))!.revisions.map((r) => r.id)).toEqual(['v4', 'v3']);
    expect((await store.getRevision('doc-1', 'v2'))!.asset.key).toBe('deck-k1');

    // Dry run writes nothing.
    const row = tables.rows.get('doc-1') as PresentationDocumentRow;
    const dry = await migrateRevisionRow(tables.client, structuredClone(row), { apply: false, table: REVS });
    expect(dry).toMatchObject({ action: 'would-migrate', revisions: 3, toCopy: 3 });
    expect(tables.items.size).toBe(1);

    // A save that lands between the read and the removal wins; the next pass finishes.
    const stale = structuredClone(tables.rows.get('doc-1')) as PresentationDocumentRow;
    await store.appendRevision(input('racer'));
    expect(await migrateRevisionRow(tables.client, stale, { apply: true, table: REVS })).toMatchObject({ action: 'raced', copied: 3 });
    expect(tables.rows.get('doc-1')!.revisions).toHaveLength(3);

    const done = await migrateRevisionRow(tables.client, structuredClone(tables.rows.get('doc-1')) as PresentationDocumentRow, { apply: true, table: REVS });
    expect(done).toMatchObject({ action: 'migrated', revisions: 3, copied: 0 });
    const after = tables.rows.get('doc-1')!;
    expect(after.revisions).toBeUndefined();
    expect(after.revisionCount).toBe(5);
    expect(after.headRevision.id).toBe('v5');
    for (let i = 0; i < 3; i += 1) {
      expect(canonicalJson(tables.items.get(`doc-1|${revKey(i + 1)}`)!.revision)).toBe(canonicalJson(legacy[i]));
    }
    expect((await store.listRevisions('doc-1', { limit: 10 }))!.revisions.map((r) => r.id)).toEqual(['v5', 'v4', 'v3', 'v2', 'v1']);
    // Idempotent: nothing left to do.
    expect(await migrateRevisionRow(tables.client, structuredClone(after) as PresentationDocumentRow, { apply: true, table: REVS })).toMatchObject({ action: 'skip' });
  });

  it('keeps a row whose existing item differs from its array entry', async () => {
    const tables = fakeTables();
    const entry = { id: 'v1', label: 'v1', createdAt: 't', createdBy: 'u', slideCount: 1, asset: { key: 'deck-a', contentType: 'x', sizeBytes: 1 } };
    tables.rows.set('doc-2', { documentId: 'doc-2', updatedAt: 't0', revisions: [entry] });
    tables.items.set(`doc-2|${revKey(1)}`, { documentId: 'doc-2', rev: revKey(1), version: 1, revision: { ...entry, label: 'other' } });
    expect(await migrateRevisionRow(tables.client, structuredClone(tables.rows.get('doc-2')) as PresentationDocumentRow, { apply: true, table: REVS }))
      .toMatchObject({ action: 'keep' });
    expect(tables.rows.get('doc-2')!.revisions).toHaveLength(1);
  });
});
