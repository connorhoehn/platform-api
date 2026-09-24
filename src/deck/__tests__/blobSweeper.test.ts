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
import { DOCUMENTS_TABLE, isSlideListKey } from '../presentationDocuments';
import { PRESENTATION_REVISIONS_TABLE } from '../presentationRevisionItems';
import {
  DEFAULT_DECK_BLOB_GRACE_MS, collectBlobReferences, createDeckBlobSweepTask, sweepDeckBlobs, type LiveRowSource,
} from '../blobSweeper';
import { sampleDeck } from './fixtures';
import { ChunkedBlobStore } from '../chunkedBlobStore';
import { scanPresentationRows, unknownReferenceShape } from '../blobSweeper';

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

  it('a dry run names what it would delete and deletes nothing', async () => {
    const { clock, primary, mirror, docs, store } = setup();
    await store.appendRevision({ documentId: 'deck-keep', title: 'K', createdBy: 'u', pptx: Buffer.from('PK-k'), slides: [shared] });
    await primary.putBlob('deck-svg-orphan.svg', Buffer.from('<svg/>'), { contentType: 'image/svg+xml' });
    clock.advance(DEFAULT_DECK_BLOB_GRACE_MS + 1);
    const before = await keys(primary);

    const result = await sweepDeckBlobs({ stores: [mirror, primary], rows: docs.liveRows, now: clock.now, dryRun: true });
    expect(result.wouldDelete).toEqual(['deck-svg-orphan.svg']);
    expect(result.deleted).toBe(0);
    expect(await keys(primary)).toEqual(before);
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

describe('a slide list moved off the row still protects its slides (NFR #36)', () => {
  async function twoRevisions() {
    const ctx = setup();
    const v1 = await ctx.store.appendRevision({ documentId: 'deck-a', title: 'A', createdBy: 'u', pptx: Buffer.from('PK-1'),
      slides: [shared, { id: 'only-v1', title: 'Only v1', svg: '<svg>only in v1</svg>' }] });
    await ctx.store.appendRevision({ documentId: 'deck-a', title: 'A', createdBy: 'u', pptx: Buffer.from('PK-2'), slides: [shared] });
    const row = ctx.docs.rows.get('deck-a') as unknown as PresentationDocumentRow;
    const manifest = row.revisions![0]!.slidesRef!.key;
    const onlyV1 = v1.revision.slides[1]!.asset.key;
    return { ...ctx, row, manifest, onlyV1 };
  }

  it('a slide named only through a manifest, and the manifest itself, survive a sweep', async () => {
    const { clock, primary, mirror, sweep, row, manifest, onlyV1 } = await twoRevisions();
    expect(isSlideListKey(manifest)).toBe(true);
    // The row no longer names v1's own slide anywhere — only the manifest does.
    expect(collectBlobReferences(row).has(onlyV1)).toBe(false);

    clock.advance(DEFAULT_DECK_BLOB_GRACE_MS + HOUR);
    const result = await sweep();
    expect(result).toMatchObject({ deleted: 0, manifestsRead: 1 });
    expect(result.skipped).toBeUndefined();
    for (const blobs of [primary, mirror]) {
      expect(await blobs.getBlob(onlyV1)).not.toBeNull();
      expect(await blobs.getBlob(manifest)).not.toBeNull();
    }
  });

  it('reads the manifest from the durable copy when the volume lost it', async () => {
    const { clock, primary, mirror, docs, sweep, manifest, onlyV1 } = await twoRevisions();
    // Only the mirror still has the manifest; an orphan proves the sweep ran.
    const lost = new InMemoryImageBlobStore({ now: clock.now });
    for await (const entry of primary.listAssets('deck-')) {
      if (entry.key !== manifest) { const blob = (await primary.getBlob(entry.key))!; await lost.putBlob(entry.key, blob.body, { contentType: blob.asset.contentType }); }
    }
    await mirror.putBlob('deck-svg-orphan.svg', Buffer.from('<svg/>'), { contentType: 'image/svg+xml' });
    clock.advance(DEFAULT_DECK_BLOB_GRACE_MS + HOUR);
    const result = await sweepDeckBlobs({ stores: [mirror, lost], rows: docs.liveRows, now: clock.now });
    expect(result).toMatchObject({ deleted: 1, manifestsRead: 1 });
    expect(await mirror.getBlob(onlyV1)).not.toBeNull();
    expect(await mirror.getBlob(manifest)).not.toBeNull();
    void primary;
  });

  it('an unreadable manifest aborts deletion: nothing is removed, not even a true orphan', async () => {
    const { clock, primary, mirror, docs, manifest, onlyV1 } = await twoRevisions();
    await primary.putBlob('deck-svg-orphan.svg', Buffer.from('<svg/>'), { contentType: 'image/svg+xml' });
    clock.advance(DEFAULT_DECK_BLOB_GRACE_MS + HOUR);
    for (const readBlob of [async () => null, async () => Buffer.from('not json'), async () => { throw new Error('volume offline'); }]) {
      const result = await sweepDeckBlobs({ stores: [mirror, primary], rows: docs.liveRows, now: clock.now, readBlob });
      expect(result.deleted).toBe(0);
      expect(result.skipped).toMatch(/could not be read/);
    }
    expect(await primary.getBlob('deck-svg-orphan.svg')).not.toBeNull();
    expect(await primary.getBlob(onlyV1)).not.toBeNull();
    expect(await primary.getBlob(manifest)).not.toBeNull();
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
    // The losing .pptx, its unadopted slide, and the manifest it wrote for v1's
    // list (NFR #36) — which the winning row still carries inline.
    expect(result.deleted).toBe(3);
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

// Incident 2026-09-23 (NFR #70): every shape a row can name a blob in has to
// survive a sweep, a shape the sweeper does not know turns every delete off,
// and a referenced DDB copy still carrying the pre-#48 ttl stops expiring.
describe('every reference shape survives a sweep; unknown shapes fail closed (NFR #70)', () => {
  const svg = (label: string) => Buffer.from(`<svg>${label}</svg>`);
  async function seedBoth(ctx: ReturnType<typeof setup>, key: string, body = svg(key)) {
    await ctx.primary.putBlob(key, body, { contentType: 'image/svg+xml' });
    await ctx.mirror.putBlob(key, body, { contentType: 'image/svg+xml' });
  }
  function rowsOf(...rows: Array<Record<string, unknown>>): LiveRowSource {
    return async function* () { for (const row of rows) yield row; };
  }
  async function sweepRows(ctx: ReturnType<typeof setup>, ...rows: Array<Record<string, unknown>>) {
    ctx.clock.advance(DEFAULT_DECK_BLOB_GRACE_MS + HOUR);
    return sweepDeckBlobs({ stores: [ctx.mirror, ctx.primary], rows: rowsOf(...rows), now: ctx.clock.now });
  }

  const shapes: Array<[string, string, Record<string, unknown>]> = [
    ['legacy per-deck slide key with an inline svg copy', 'deck-legacy-v1-cover.svg',
      { documentId: 'legacy', type: 'presentation', revisions: [{ id: 'v1', asset: { key: 'deck-legacy-v1.pptx' },
        slides: [{ id: 'cover', svg: '<svg/>', asset: { key: 'deck-legacy-v1-cover.svg', contentType: 'image/svg+xml' } }] }] }],
    ['legacy revisionSlides map', 'deck-legacy-v2-risks.svg',
      { documentId: 'legacy2', type: 'presentation', revisionSlides: { v2: [{ id: 'risks', asset: { key: 'deck-legacy-v2-risks.svg' } }] } }],
    ['seeded row of another type with no revisions', 'deck-seeded-v1.pptx',
      { documentId: 'active-now-x-sprint-review', type: 'custom', revisionPreviews: { v1: { body: '<svg/>', contentType: 'image/svg+xml' } },
        download: { asset: { key: 'deck-seeded-v1.pptx' } } }],
    ['preview pointer naming a slide blob', 'deck-svg-preview.svg',
      { documentId: 'p', type: 'presentation', revisionPreviews: { v1: { slideId: 'cover', asset: { key: 'deck-svg-preview.svg' } } } }],
    ['older spec moved to a blob', 'deck-spec-old.json',
      { documentId: 's', type: 'presentation', revisions: [{ id: 'v1', specRef: { key: 'deck-spec-old.json', contentType: 'application/json', sha256: 'x' } }] }],
    ['a key embedded in a URL', 'deck-svg-in-url.svg',
      { documentId: 'u', type: 'presentation', thumbnailUrl: '/api/blobs/deck-svg-in-url.svg?v=2' }],
  ];

  it.each(shapes)('%s', async (_label, key, row) => {
    const ctx = setup();
    await seedBoth(ctx, key);
    await seedBoth(ctx, 'deck-svg-true-orphan.svg');
    const result = await sweepRows(ctx, row);
    expect(result.skipped).toBeUndefined();
    for (const blobs of [ctx.primary, ctx.mirror]) {
      expect(await blobs.getBlob(key)).not.toBeNull();
      expect(await blobs.getBlob('deck-svg-true-orphan.svg')).toBeNull();
    }
  });

  it('a moved slide list (slidesRef manifest) protects the slides inside it', async () => {
    const ctx = setup();
    await seedBoth(ctx, 'deck-svg-in-manifest.svg');
    await seedBoth(ctx, 'deck-slides-m.json', Buffer.from(JSON.stringify([{ id: 'a', asset: { key: 'deck-svg-in-manifest.svg' } }])));
    await seedBoth(ctx, 'deck-svg-true-orphan.svg');
    const result = await sweepRows(ctx, { documentId: 'm', type: 'presentation', revisions: [{ id: 'v1', slidesRef: { key: 'deck-slides-m.json' } }] });
    expect(result).toMatchObject({ deleted: 1, manifestsRead: 1 });
    expect(await ctx.mirror.getBlob('deck-svg-in-manifest.svg')).not.toBeNull();
    expect(await ctx.mirror.getBlob('deck-slides-m.json')).not.toBeNull();
  });

  it('a large .pptx split into parts keeps its head and every part', async () => {
    const ctx = setup();
    const chunked = new ChunkedBlobStore(ctx.mirror, 8, 8);
    const body = Buffer.from('PK'.repeat(20));
    await chunked.putBlob('deck-big-v1.pptx', body, { contentType: 'application/vnd.openxmlformats-officedocument.presentationml.presentation' });
    ctx.clock.advance(DEFAULT_DECK_BLOB_GRACE_MS + HOUR);
    const result = await sweepDeckBlobs({ stores: [chunked], now: ctx.clock.now,
      rows: rowsOf({ documentId: 'big', type: 'presentation', revisions: [{ id: 'v1', asset: { key: 'deck-big-v1.pptx' } }] }) });
    expect(result.deleted).toBe(0);
    expect((await chunked.getBlob('deck-big-v1.pptx'))!.body.equals(body)).toBe(true);
  });

  it.each([
    ['a specRef that is not a key', { revisions: [{ id: 'v1', specRef: { bucket: 'b', path: 'deck-spec-x' } }] }],
    ['a slidesRef that is not a key', { revisions: [{ id: 'v1', slidesRef: { key: 'slides/v1' } }] }],
    ['an asset with no key', { revisions: [{ id: 'v1', asset: { href: 'deck-x.pptx' } }] }],
    ['an asset key in an unknown deck format', { revisions: [{ id: 'v1', asset: { key: 'deck-x.webp' } }] }],
    ['revisions that are not a list', { revisions: { v1: {} } }],
    ['revisionSlides that are not lists', { revisionSlides: { v1: 'deck-x' } }],
  ])('fails closed on %s: not even a true orphan is deleted', async (_label, shape) => {
    const ctx = setup();
    await seedBoth(ctx, 'deck-svg-true-orphan.svg');
    const result = await sweepRows(ctx, { documentId: 'odd', type: 'presentation', ...shape });
    expect(result.deleted).toBe(0);
    expect(result.skipped).toMatch(/shape this build does not know/);
    expect(await ctx.mirror.getBlob('deck-svg-true-orphan.svg')).not.toBeNull();
    expect(unknownReferenceShape({ documentId: 'odd', ...shape })).not.toBeNull();
  });

  it('an unknown shape inside a moved slide list fails closed too', async () => {
    const ctx = setup();
    await seedBoth(ctx, 'deck-slides-odd.json', Buffer.from(JSON.stringify([{ id: 'a', asset: { url: 'x' } }])));
    await seedBoth(ctx, 'deck-svg-true-orphan.svg');
    const result = await sweepRows(ctx, { documentId: 'm', revisions: [{ id: 'v1', slidesRef: { key: 'deck-slides-odd.json' } }] });
    expect(result.deleted).toBe(0);
    expect(result.skipped).toMatch(/deck-slides-odd\.json\[0\]\.asset has no key/);
  });

  it('clears the legacy expiry on every referenced copy, and only those, even when it deletes nothing', async () => {
    const ctx = setup();
    await seedBoth(ctx, 'deck-old-v1.pptx');
    await seedBoth(ctx, 'deck-svg-unnamed.svg');
    ctx.mirror.setExpiry('deck-old-v1.pptx', T0 + 30 * 24 * HOUR);
    ctx.mirror.setExpiry('deck-svg-unnamed.svg', T0 + 30 * 24 * HOUR);
    // Young: no candidates at all, but the expiry is still cleared.
    const result = await sweepDeckBlobs({ stores: [ctx.mirror, ctx.primary], now: ctx.clock.now,
      rows: rowsOf({ documentId: 'old', type: 'presentation', revisions: [{ id: 'v1', asset: { key: 'deck-old-v1.pptx' } }] }) });
    expect(result).toMatchObject({ candidates: 0, deleted: 0, expiryCleared: 1 });
    const expiries = new Map<string, number | undefined>();
    for await (const entry of ctx.mirror.listAssets('deck-')) expiries.set(entry.key, entry.expiresAtMs);
    expect(expiries.get('deck-old-v1.pptx')).toBeUndefined();
    expect(expiries.get('deck-svg-unnamed.svg')).toBe(T0 + 30 * 24 * HOUR);
  });

  it('the DDB store lists an item’s ttl and clears it with a REMOVE that touches nothing else', async () => {
    const sent: Array<{ command: unknown; input: Record<string, any> }> = [];
    const client = {
      async send(command: any) {
        sent.push({ command, input: command.input });
        if (command instanceof ScanCommand) return { Items: [{ imageId: 'deck-a.pptx', uploadedAt: new Date(T0).toISOString(), ttlEpoch: 1_800_000_000 }] };
        return {};
      },
    } as unknown as DynamoDBDocumentClient;
    const store = new DdbImageBlobStore({ docClient: client, now: () => T0 });
    const listed = [];
    for await (const entry of store.listAssets('deck-')) listed.push(entry);
    expect(listed).toEqual([{ key: 'deck-a.pptx', touchedAtMs: T0, expiresAtMs: 1_800_000_000_000 }]);
    expect(await store.clearExpiry('deck-a.pptx')).toBe(true);
    const update = sent.find((entry) => entry.command instanceof UpdateCommand)!;
    expect(update.input.UpdateExpression).toBe('REMOVE ttlEpoch');
    expect(update.input.ConditionExpression).toBe('attribute_exists(ttlEpoch)');
  });

  it('the row scan reads every document row, unfiltered', async () => {
    const sent: Array<Record<string, any>> = [];
    const client = { async send(command: any) { sent.push(command.input); return { Items: [{ documentId: 'a' }] }; } } as unknown as DynamoDBDocumentClient;
    const rows = [];
    for await (const row of scanPresentationRows(client)()) rows.push(row);
    // The document rows, then the revision items (NFR #188) — both unfiltered.
    expect(rows).toHaveLength(2);
    expect(sent.map((input) => input.TableName)).toEqual([DOCUMENTS_TABLE, PRESENTATION_REVISIONS_TABLE]);
    expect(sent.every((input) => input.FilterExpression === undefined)).toBe(true);
  });
});

describe('a superseded revision’s .pptx is a rebuildable cache (Loop 33 ppt)', () => {
  const pptx = (n: string) => `deck-pptx-${n.repeat(64).slice(0, 64)}.pptx`;
  const item = (documentId: string, version: number, key: string, spec: 'inline' | string | null) => ({
    documentId, rev: `v${String(version).padStart(6, '0')}`, version,
    revision: {
      id: `v${version}`, label: `v${version}`, createdAt: '2026-09-20T00:00:00.000Z', createdBy: 'u', slideCount: 1,
      asset: { key, contentType: 'application/pptx', sizeBytes: 4 },
      ...(spec === 'inline' ? { spec: { title: 'T', slides: [] } } : spec ? { specRef: { key: spec, contentType: 'application/json', sizeBytes: 2, sha256: 'x' } } : {}),
    },
  });
  async function run(rows: Array<Record<string, unknown>>, extra: { keepSupersededPptx?: boolean } = {}) {
    let now = T0;
    const store = new InMemoryImageBlobStore({ now: () => now });
    for (const key of [pptx('a'), pptx('b'), pptx('c'), pptx('d'), 'deck-spec-old.json']) {
      await store.putBlob(key, Buffer.from('PK'), { contentType: 'application/octet-stream' });
    }
    now += DEFAULT_DECK_BLOB_GRACE_MS + 1;
    const source: LiveRowSource = async function* () { for (const row of rows) yield row; };
    return sweepDeckBlobs({ stores: [store], rows: source, now: () => now, dryRun: true, ...extra });
  }

  it('offers an older revision’s content .pptx to the sweep; the head’s stays', async () => {
    const result = await run([
      { documentId: 'deck-1', revisionCount: 3 },
      item('deck-1', 1, pptx('a'), 'inline'),
      item('deck-1', 2, pptx('b'), 'deck-spec-old.json'),
      item('deck-1', 3, pptx('c'), 'inline'),
      { documentId: 'deck-2', revisionCount: 1 }, item('deck-2', 1, pptx('d'), 'inline'),
    ]);
    expect(result.wouldDelete?.sort()).toEqual([pptx('a'), pptx('b')].sort());
    expect(result.supersededPptxUnmarked).toBe(2);
  });

  it('keeps it when any other row names the same file (a revert, another deck)', async () => {
    const result = await run([
      { documentId: 'deck-1', revisionCount: 2 },
      item('deck-1', 1, pptx('a'), 'inline'),
      item('deck-1', 2, pptx('a'), 'inline'),
      { documentId: 'deck-2', revisionCount: 2 }, item('deck-2', 1, pptx('b'), 'inline'),
      { documentId: 'deck-3', revisionCount: 1 }, item('deck-3', 1, pptx('b'), 'inline'),
    ]);
    expect(result.wouldDelete?.sort()).toEqual([pptx('c'), pptx('d'), 'deck-spec-old.json'].sort());
  });

  it('keeps it when the spec it would be rebuilt from is gone, or the head is unknown', async () => {
    const result = await run([
      item('deck-0', 1, pptx('c'), 'inline'), // before its row: head unknown
      { documentId: 'deck-1', revisionCount: 3 },
      item('deck-1', 1, pptx('a'), 'deck-spec-lost.json'),
      item('deck-1', 2, pptx('b'), null),
      item('deck-1', 3, pptx('d'), 'inline'),
      { documentId: 'deck-0', revisionCount: 2 },
    ]);
    expect(result.wouldDelete).toEqual(['deck-spec-old.json']);
  });

  it('keepSupersededPptx restores the old marking, and a legacy per-revision key is never a cache', async () => {
    const legacy = await run([
      { documentId: 'deck-1', revisionCount: 2 },
      item('deck-1', 1, 'deck-deck-1-v1-abcd1234.pptx', 'inline'),
      item('deck-1', 2, pptx('a'), 'inline'),
    ]);
    expect(legacy.supersededPptxUnmarked).toBe(0);
    const kept = await run([
      { documentId: 'deck-1', revisionCount: 2 },
      item('deck-1', 1, pptx('b'), 'inline'),
      item('deck-1', 2, pptx('a'), 'inline'),
    ], { keepSupersededPptx: true });
    expect(kept.wouldDelete).not.toContain(pptx('b'));
  });
});
