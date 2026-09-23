// NFR #48: the presentation blob sweeper is reference-safe. Slide and spec
// blobs are shared by content across revisions and decks, so nothing is
// removed that any live row names, nothing younger than the grace period is
// removed at all, and a refused or raced save leaves no .pptx that outlives it.

jest.mock('../../lib/aws-clients', () => ({ docClient: { send: jest.fn() }, ddbClient: { send: jest.fn() } }));
jest.mock('../../lib/ddb-table-name', () => ({ tableName: (name: string) => `test-${name}` }));

import { mkdtemp, rm, utimes, readdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DeleteCommand, GetCommand, PutCommand, ScanCommand, UpdateCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { DdbImageBlobStore, InMemoryImageBlobStore } from '../../pipeline/image/ImageBlobStore';
import { VolumeImageBlobStore } from '../../pipeline/image/VolumeImageBlobStore';
import {
  DdbPresentationDocumentStore, PresentationRevisionConflictError, PresentationRowTooLargeError,
  contentHash, slideContentKey, type PresentationDocumentRow,
} from '../presentationDocuments';
import {
  DEFAULT_DECK_BLOB_GRACE_MS, collectBlobReferences, createDeckBlobSweepTask, sweepDeckBlobs, type LiveRowSource,
} from '../blobSweeper';
import { sampleDeck } from './fixtures';

const HOUR = 60 * 60 * 1000;
const T0 = Date.parse('2026-09-20T00:00:00.000Z');

/** An in-memory crdt-documents table that honours the store's conditional writes. */
function fakeDocuments() {
  const rows = new Map<string, Record<string, unknown>>();
  let failNextUpdate = false;
  const client = {
    async send(command: unknown) {
      const input = (command as { input: Record<string, any> }).input;
      if (command instanceof GetCommand) return { Item: structuredClone(rows.get(input.Key.documentId)) };
      if (command instanceof PutCommand) {
        if (rows.has(input.Item.documentId)) throw Object.assign(new Error('exists'), { name: 'ConditionalCheckFailedException' });
        rows.set(input.Item.documentId, structuredClone(input.Item));
        return {};
      }
      if (command instanceof UpdateCommand) {
        const row = rows.get(input.Key.documentId);
        const values = input.ExpressionAttributeValues;
        if (failNextUpdate || !row || ((row.revisions as unknown[] | undefined)?.length ?? 0) !== values[':expectedCount']) {
          failNextUpdate = false;
          throw Object.assign(new Error('The conditional request failed'), { name: 'ConditionalCheckFailedException' });
        }
        Object.assign(row, {
          revisions: values[':revisions'], revisionPreviews: values[':revisionPreviews'],
          outline: values[':outline'], pageCount: values[':pageCount'], updatedAt: values[':updatedAt'],
        });
        return {};
      }
      throw new Error(`unexpected command ${(command as object).constructor.name}`);
    },
  } as unknown as DynamoDBDocumentClient;
  const liveRows: LiveRowSource = async function* () { for (const row of rows.values()) yield row; };
  return { client, rows, liveRows, raceNextUpdate: () => { failNextUpdate = true; } };
}

function setup() {
  let now = T0;
  const clock = { now: () => now, advance: (ms: number) => { now += ms; } };
  const primary = new InMemoryImageBlobStore({ now: clock.now });
  const mirror = new InMemoryImageBlobStore({ now: clock.now });
  const docs = fakeDocuments();
  const store = new DdbPresentationDocumentStore({ docClient: docs.client, blobStore: primary, mirror, now: () => new Date(clock.now()) });
  const sweep = () => sweepDeckBlobs({ stores: [mirror, primary], rows: docs.liveRows, now: clock.now });
  return { clock, primary, mirror, docs, store, sweep };
}

async function keys(store: InMemoryImageBlobStore): Promise<string[]> {
  const out: string[] = [];
  for await (const entry of store.listAssets('deck-')) out.push(entry.key);
  return out.sort();
}

const shared = { id: 'cover', title: 'Cover', svg: '<svg>the same cover in both decks</svg>' };
const sharedKey = slideContentKey(contentHash(Buffer.from(shared.svg, 'utf8')));

describe('sweepDeckBlobs — mark and sweep over live rows', () => {
  it('a slide shared by two decks survives deleting one deck and a sweep; the deleted deck’s own blobs go', async () => {
    const { clock, primary, mirror, docs, store, sweep } = setup();
    const a = await store.appendRevision({ documentId: 'deck-a', title: 'A', createdBy: 'u', pptx: Buffer.from('PK-a'),
      slides: [shared, { id: 'only-a', title: 'Only A', svg: '<svg>only a</svg>' }] });
    const b = await store.appendRevision({ documentId: 'deck-b', title: 'B', createdBy: 'u', pptx: Buffer.from('PK-b'), slides: [shared] });
    expect(a.revision.slides[0]!.asset.key).toBe(sharedKey);
    expect(b.revision.slides[0]!.asset.key).toBe(sharedKey);

    docs.rows.delete('deck-a');
    clock.advance(DEFAULT_DECK_BLOB_GRACE_MS + HOUR);
    const result = await sweep();

    for (const blobs of [primary, mirror]) {
      expect(await blobs.getBlob(sharedKey)).not.toBeNull();
      expect(await blobs.getBlob(b.revision.asset.key)).not.toBeNull();
      expect(await blobs.getBlob(a.revision.asset.key)).toBeNull();
      expect(await blobs.getBlob(a.revision.slides[1]!.asset.key)).toBeNull();
    }
    expect(result).toMatchObject({ rowsScanned: 1, deleted: 2, refused: 0 });
    // And deck B still reads its slide through the store.
    expect((await store.slidePreview('deck-b', 'v1', 'cover'))!.body.toString('utf8')).toBe(shared.svg);
  });

  it('never sweeps a blob younger than the grace period, even when nothing names it', async () => {
    const { clock, primary, mirror, docs, store, sweep } = setup();
    const a = await store.appendRevision({ documentId: 'deck-a', title: 'A', createdBy: 'u', pptx: Buffer.from('PK-a'), slides: [shared] });
    await store.appendRevision({ documentId: 'deck-keep', title: 'K', createdBy: 'u', pptx: Buffer.from('PK-k'), slides: [] });
    docs.rows.delete('deck-a');
    clock.advance(DEFAULT_DECK_BLOB_GRACE_MS - HOUR);

    const result = await sweep();
    expect(result).toMatchObject({ candidates: 0, deleted: 0 });
    expect(await primary.getBlob(a.revision.asset.key)).not.toBeNull();
    expect(await mirror.getBlob(sharedKey)).not.toBeNull();
  });

  it('sweeps an old blob no live row names, from every store', async () => {
    const { clock, primary, mirror, store, sweep } = setup();
    await store.appendRevision({ documentId: 'deck-keep', title: 'K', createdBy: 'u', pptx: Buffer.from('PK-k'), slides: [shared] });
    await primary.putBlob('deck-svg-orphan.svg', Buffer.from('<svg/>'), { contentType: 'image/svg+xml' });
    await mirror.putBlob('deck-svg-orphan.svg', Buffer.from('<svg/>'), { contentType: 'image/svg+xml' });
    await primary.putBlob('image-not-a-deck.png', Buffer.from('png'), { contentType: 'image/png' });
    clock.advance(DEFAULT_DECK_BLOB_GRACE_MS + 1);

    const result = await sweep();
    expect(result.deleted).toBe(1);
    expect(await primary.getBlob('deck-svg-orphan.svg')).toBeNull();
    expect(await mirror.getBlob('deck-svg-orphan.svg')).toBeNull();
    // Not a deck key: never considered.
    expect(await primary.getBlob('image-not-a-deck.png')).not.toBeNull();
    expect(await keys(primary)).toContain(sharedKey);
  });

  it('keeps a blob a save rewrote after the sweep listed it (conditional delete)', async () => {
    const { clock, primary, mirror, docs, store } = setup();
    await store.appendRevision({ documentId: 'deck-keep', title: 'K', createdBy: 'u', pptx: Buffer.from('PK-k'), slides: [] });
    await mirror.putBlob('deck-svg-reused.svg', Buffer.from('<svg/>'), { contentType: 'image/svg+xml' });
    await primary.putBlob('deck-svg-reused.svg', Buffer.from('<svg/>'), { contentType: 'image/svg+xml' });
    clock.advance(DEFAULT_DECK_BLOB_GRACE_MS + 1);

    // A save for a new deck writes the same content while the sweep is marking,
    // and its row has not landed yet.
    const rows: LiveRowSource = async function* () {
      await mirror.putBlob('deck-svg-reused.svg', Buffer.from('<svg/>'), { contentType: 'image/svg+xml' });
      await primary.putBlob('deck-svg-reused.svg', Buffer.from('<svg/>'), { contentType: 'image/svg+xml' });
      yield* docs.liveRows();
    };
    const result = await sweepDeckBlobs({ stores: [mirror, primary], rows, now: clock.now });
    expect(result).toMatchObject({ deleted: 0, refused: 1 });
    expect(await primary.getBlob('deck-svg-reused.svg')).not.toBeNull();
    expect(await mirror.getBlob('deck-svg-reused.svg')).not.toBeNull();
  });

  it('deletes nothing when the row scan finds no presentation at all', async () => {
    const { clock, primary, docs, sweep } = setup();
    await primary.putBlob('deck-svg-x.svg', Buffer.from('<svg/>'), { contentType: 'image/svg+xml' });
    clock.advance(DEFAULT_DECK_BLOB_GRACE_MS * 2);
    expect(docs.rows.size).toBe(0);
    const result = await sweep();
    expect(result.skipped).toMatch(/no presentation rows/);
    expect(await primary.getBlob('deck-svg-x.svg')).not.toBeNull();
  });

  it('counts every way a row names a blob — revisions, specRef, legacy revisionSlides', () => {
    const refs = collectBlobReferences({
      revisions: [{ asset: { key: 'deck-d-v1-ab.pptx' }, specRef: { key: 'deck-spec-1.json' }, slides: [{ asset: { key: 'deck-svg-1.svg' } }] }],
      revisionSlides: { v0: [{ asset: { key: 'deck-d-v0-cover.svg' } }] },
      title: 'deck-shaped title that is not a key',
    });
    expect([...refs].sort()).toEqual(['deck-d-v0-cover.svg', 'deck-d-v1-ab.pptx', 'deck-spec-1.json', 'deck-svg-1.svg']);
  });

  it('as a cluster task, runs at most once per interval', async () => {
    const { clock, primary, docs } = setup();
    const listAssets = jest.spyOn(primary, 'listAssets');
    const task = createDeckBlobSweepTask({ stores: [primary], rows: docs.liveRows, now: clock.now, everyMs: HOUR })!;
    await task.run();
    await task.run();
    expect(listAssets).toHaveBeenCalledTimes(1);
    clock.advance(HOUR);
    await task.run();
    expect(listAssets).toHaveBeenCalledTimes(2);
  });
});

describe('refused and raced saves leave no .pptx behind', () => {
  it('a save refused by the row-size guard (NFR #4) writes no .pptx at all', async () => {
    const { primary, mirror, store } = setup();
    const spec = { ...sampleDeck(), slides: sampleDeck().slides.map((slide) => ({ ...slide, notes: 'n'.repeat(60_000) })) };
    await expect(store.appendRevision({ documentId: 'deck-a', title: 'A', createdBy: 'u', pptx: Buffer.from('PK'), slides: [shared], spec }))
      .rejects.toBeInstanceOf(PresentationRowTooLargeError);
    expect(await keys(primary)).toEqual([]);
    expect(await keys(mirror)).toEqual([]);
  });

  it('a save that loses the row race leaves a .pptx no row names, which the next sweep past grace removes', async () => {
    const { clock, primary, mirror, docs, store, sweep } = setup();
    const winner = await store.appendRevision({ documentId: 'deck-a', title: 'A', createdBy: 'u', pptx: Buffer.from('PK-1'), slides: [shared] });
    docs.raceNextUpdate();
    await expect(store.appendRevision({ documentId: 'deck-a', title: 'A', createdBy: 'u', pptx: Buffer.from('PK-2'),
      slides: [shared, { id: 'lost', title: 'Lost', svg: '<svg>lost</svg>' }], expectedHead: 'v1' }))
      .rejects.toBeInstanceOf(PresentationRevisionConflictError);
    const loser = (await keys(primary)).filter((key) => key.endsWith('.pptx') && key !== winner.revision.asset.key);
    expect(loser).toHaveLength(1);

    clock.advance(DEFAULT_DECK_BLOB_GRACE_MS + 1);
    const result = await sweep();
    expect(result.deleted).toBe(2); // the losing .pptx and its unadopted slide
    for (const blobs of [primary, mirror]) {
      expect(await keys(blobs)).toEqual([sharedKey, winner.revision.asset.key].sort());
    }
    expect((docs.rows.get('deck-a') as unknown as PresentationDocumentRow).revisions).toHaveLength(1);
  });
});

describe('store halves the sweeper stands on', () => {
  it('the DDB store puts assets with no ttl, and deletes one only if its write time is unchanged', async () => {
    const sent: Array<{ command: unknown; input: Record<string, any> }> = [];
    const client = { async send(command: any) { sent.push({ command, input: command.input }); return {}; } } as unknown as DynamoDBDocumentClient;
    const store = new DdbImageBlobStore({ docClient: client, now: () => T0 });
    await store.putBlob('deck-svg-1.svg', Buffer.from('<svg/>'), { contentType: 'image/svg+xml' });
    const put = sent.find((entry) => entry.command instanceof PutCommand)!;
    expect(put.input.Item.ttlEpoch).toBeUndefined();
    expect(put.input.Item.uploadedAt).toBe(new Date(T0).toISOString());

    await store.deleteAssetIfUntouched('deck-svg-1.svg', T0);
    const del = sent.find((entry) => entry.command instanceof DeleteCommand)!;
    expect(del.input.ConditionExpression).toContain('uploadedAt = :seen');
    expect(del.input.ExpressionAttributeValues[':seen']).toBe(new Date(T0).toISOString());
  });

  it('the DDB store reports a refused delete as false and lists assets with their write time', async () => {
    const client = {
      async send(command: any) {
        if (command instanceof DeleteCommand) throw Object.assign(new Error('no'), { name: 'ConditionalCheckFailedException' });
        if (command instanceof ScanCommand) {
          return command.input.ExclusiveStartKey
            ? { Items: [{ imageId: 'deck-b.pptx', uploadedAt: 'not a date' }] }
            : { Items: [{ imageId: 'deck-a.pptx', uploadedAt: new Date(T0).toISOString() }], LastEvaluatedKey: { imageId: 'deck-a.pptx' } };
        }
        return {};
      },
    } as unknown as DynamoDBDocumentClient;
    const store = new DdbImageBlobStore({ docClient: client });
    expect(await store.deleteAssetIfUntouched('deck-a.pptx', T0)).toBe(false);
    const listed = [];
    for await (const entry of store.listAssets('deck-')) listed.push(entry);
    expect(listed).toEqual([{ key: 'deck-a.pptx', touchedAtMs: T0 }]);
  });

  it('the volume store lists by mtime and deletes body and sidecar only when untouched', async () => {
    const dir = await mkdtemp(join(tmpdir(), 'deck-blob-sweep-'));
    try {
      const store = new VolumeImageBlobStore(dir);
      await store.putBlob('deck-svg-1.svg', Buffer.from('<svg/>'), { contentType: 'image/svg+xml' });
      const old = new Date(T0);
      await utimes(join(dir, 'deck-svg-1.svg'), old, old);
      const listed = [];
      for await (const entry of store.listAssets('deck-')) listed.push(entry);
      expect(listed).toEqual([{ key: 'deck-svg-1.svg', touchedAtMs: T0 }]);

      expect(await store.deleteAssetIfUntouched('deck-svg-1.svg', T0 - 1)).toBe(false);
      expect(await store.deleteAssetIfUntouched('deck-svg-1.svg', T0)).toBe(true);
      expect(await readdir(dir)).toEqual([]);
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });
});
