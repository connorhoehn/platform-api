// Where a deck's bytes actually go, and what happens when the volume they went
// to is gone.
//
// Both halves of a real defect: rows survived a pod replacement while the blob
// volume did not, so every per-slide preview 404'd; and the renders were not
// landing in the directory the operator had mounted at all.

jest.mock('../../lib/ddb-table-name', () => ({ tableName: (name: string) => `test-${name}` }));
jest.mock('../../lib/aws-clients', () => ({ docClient: { send: jest.fn(async () => ({})) }, ddbClient: { send: jest.fn() } }));

import { mkdtempSync, readdirSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { GetCommand, PutCommand, UpdateCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { IMAGE_BLOB_SIZE_CAP_BYTES, InMemoryImageBlobStore } from '../../pipeline/image/ImageBlobStore';
import { PPTX_CONTENT_TYPE } from '../renderPptx';
import { VolumeImageBlobStore } from '../../pipeline/image/VolumeImageBlobStore';
import { __resetVisionServicesForTests, getAssetBlobStore } from '../../pipeline/image/visionServices';
import {
  DdbPresentationDocumentStore, MAX_ROW_BYTES, PresentationRowTooLargeError, contentHash, estimateItemBytes,
  inlineSlideBytes, resolveRevisionSpec, slideContentKey,
  type PresentationDocumentRow,
} from '../presentationDocuments';
import { sampleDeck } from './fixtures';
import { MAX_CHUNKED_BLOB_BYTES } from '../chunkedBlobStore';

const slides = [
  { id: 'cover', title: 'Cover', svg: '<svg xmlns="http://www.w3.org/2000/svg">cover</svg>' },
  { id: 'sources', title: 'Sources', svg: '<svg xmlns="http://www.w3.org/2000/svg">sources</svg>' },
];

function fakeClient(): { client: DynamoDBDocumentClient; written: () => PresentationDocumentRow | null } {
  let row: PresentationDocumentRow | null = null;
  const client = {
    async send(command: unknown) {
      if (command instanceof GetCommand) return { Item: row ?? undefined };
      if (command instanceof PutCommand) {
        row = (command as unknown as { input: { Item: PresentationDocumentRow } }).input.Item;
      }
      if (command instanceof UpdateCommand && row) {
        // The store's one update: SET each `name = :name`. Round-trip through
        // JSON the way DynamoDB would hand it back.
        const values = (command as unknown as { input: { ExpressionAttributeValues: Record<string, unknown> } }).input.ExpressionAttributeValues;
        const next: Record<string, unknown> = { ...row };
        for (const [key, value] of Object.entries(values)) if (key !== ':expectedCount') next[key.slice(1)] = value;
        row = JSON.parse(JSON.stringify(next));
      }
      return {};
    },
  } as unknown as DynamoDBDocumentClient;
  return { client, written: () => row };
}

describe('a slide’s markup lives in the blob store, by content (NFR #4)', () => {
  it('is written to the volume and the durable copy under its hash; the row keeps only the record', async () => {
    const blobStore = new InMemoryImageBlobStore();
    const mirror = new InMemoryImageBlobStore();
    const { client, written } = fakeClient();
    const store = new DdbPresentationDocumentStore({ docClient: client, blobStore, mirror, now: () => new Date('2026-09-21T10:00:00.000Z') });
    const { revision } = await store.appendRevision({ documentId: 'doc-1', title: 'Deck', createdBy: 'u', pptx: Buffer.from('PK'), slides });

    const sha = contentHash(slides[0]!.svg);
    expect(revision.slides[0]).toMatchObject({ sha256: sha, asset: { key: slideContentKey(sha), sizeBytes: Buffer.byteLength(slides[0]!.svg) } });
    expect(revision.slides[0]!.svg).toBeUndefined();
    expect(written()!.revisionSlides).toBeUndefined();
    // The cover preview names its slide instead of carrying a third copy.
    expect(written()!.revisionPreviews).toEqual({ v1: { contentType: 'image/svg+xml', slideId: 'cover' } });
    for (const place of [blobStore, mirror]) {
      expect((await place.getBlob(slideContentKey(sha)))!.body.toString('utf8')).toBe(slides[0]!.svg);
    }
    expect(await store.slidePreview('doc-1', 'v1', 'cover')).toMatchObject({ from: 'blob', sha256: sha });
  });

  it('serves a slide from the durable copy when the volume lost it', async () => {
    const blobStore = new InMemoryImageBlobStore();
    const mirror = new InMemoryImageBlobStore();
    const { client } = fakeClient();
    const writer = new DdbPresentationDocumentStore({ docClient: client, blobStore, mirror });
    await writer.appendRevision({ documentId: 'doc-1', title: 'Deck', createdBy: 'u', pptx: Buffer.from('PK'), slides });
    // A pod replacement: the row and the durable copy survive, the volume does not.
    const reader = new DdbPresentationDocumentStore({ docClient: client, blobStore: new InMemoryImageBlobStore(), mirror });
    const preview = await reader.slidePreview('doc-1', 'v1', 'sources');
    expect(preview).toMatchObject({ from: 'mirror', contentType: 'image/svg+xml' });
    expect(preview!.body.toString('utf8')).toBe(slides[1]!.svg);
  });

  it('20 revisions of a 6-slide deck: unchanged slides are one blob, old specs move off the row, the row stays small', async () => {
    const blobStore = new InMemoryImageBlobStore();
    const mirror = new InMemoryImageBlobStore();
    const puts = jest.spyOn(mirror, 'putBlob');
    const { client, written } = fakeClient();
    const store = new DdbPresentationDocumentStore({ docClient: client, blobStore, mirror });
    const deck = sampleDeck();
    const six = Array.from({ length: 6 }, (_v, index) => ({ id: `s${index}`, title: `Slide ${index}`, svg: `<svg xmlns="http://www.w3.org/2000/svg">${'p'.repeat(2400)}${index}</svg>` }));
    const sizes: number[] = [];
    for (let rev = 1; rev <= 20; rev += 1) {
      // Each save changes one slide (the one a composer edit touched).
      const edited = six.map((slide, index) => (index === rev % 6 ? { ...slide, svg: slide.svg.replace('</svg>', `r${rev}</svg>`) } : slide));
      six.splice(0, 6, ...edited);
      const spec = { ...deck, title: `Deck r${rev}` };
      await store.appendRevision({ documentId: 'audit-row-diet', title: 'Deck', createdBy: 'u', pptx: Buffer.from('PK'), slides: six, spec });
      sizes.push(estimateItemBytes(written() as never));
    }
    const row = written()!;
    expect(row.revisions).toHaveLength(20);
    expect(sizes[19]!).toBeLessThan(150 * 1024);
    // Growth per revision is the record, not the markup (~15 KB of SVG per save here).
    expect((sizes[19]! - sizes[9]!) / 10).toBeLessThan(3 * 1024);
    // 6 slides + 19 edited slides + 19 moved specs + 20 .pptx — never a slide twice.
    const svgPuts = puts.mock.calls.filter(([key]) => String(key).startsWith('deck-svg-')).length;
    expect(svgPuts).toBe(6 + 19);
    // Only the head carries its spec; every earlier one reads back from the blob.
    expect(row.revisions!.filter((revision) => revision.spec)).toHaveLength(1);
    expect(row.revisions![19]!.spec!.title).toBe('Deck r20');
    expect((await resolveRevisionSpec(store, row.revisions![0]!))!.title).toBe('Deck r1');
    // And an old revision's slides still render.
    expect((await store.slidePreview('audit-row-diet', 'v1', 's0'))!.body.toString('utf8')).toContain('p');
  });

  it('serves the slide from the row when the blob file is gone', async () => {
    // The row survived; the volume did not. Nothing is in the blob store.
    const { client } = fakeClient();
    const row: PresentationDocumentRow = {
      documentId: 'doc-1',
      revisions: [{
        id: 'v1', label: 'v1', createdAt: '2026-09-21T10:00:00.000Z', createdBy: 'u', slideCount: 1,
        asset: { key: 'gone.pptx', contentType: 'application/x', sizeBytes: 1 },
        slides: [{
          id: 'cover', index: 1, title: 'Cover', previewHandle: 'doc-1.v1.cover',
          asset: { key: 'gone.svg', contentType: 'image/svg+xml', sizeBytes: 10 },
          svg: '<svg xmlns="http://www.w3.org/2000/svg">cover</svg>',
        }],
      }],
    };
    const store = new DdbPresentationDocumentStore({ docClient: client, blobStore: new InMemoryImageBlobStore() });
    jest.spyOn(store, 'get').mockResolvedValue(row);

    const preview = await store.slidePreview('doc-1', 'v1', 'cover');
    expect(preview).toMatchObject({ from: 'inline', contentType: 'image/svg+xml' });
    expect(preview!.body.toString('utf8')).toContain('cover');
  });

  it('falls back to the blob for a revision written before inlining', async () => {
    const blobStore = new InMemoryImageBlobStore();
    await blobStore.putBlob('old.svg', Buffer.from('<svg>old</svg>', 'utf8'), { contentType: 'image/svg+xml' });
    const { client } = fakeClient();
    const store = new DdbPresentationDocumentStore({ docClient: client, blobStore });
    jest.spyOn(store, 'get').mockResolvedValue({
      documentId: 'doc-1',
      revisionSlides: {
        v1: [{ id: 'cover', index: 1, title: 'Cover', previewHandle: 'doc-1.v1.cover', asset: { key: 'old.svg', contentType: 'image/svg+xml', sizeBytes: 14 } }],
      },
    });
    const preview = await store.slidePreview('doc-1', 'v1', 'cover');
    expect(preview).toMatchObject({ from: 'blob' });
    expect(preview!.body.toString('utf8')).toBe('<svg>old</svg>');
  });

  it('refuses a row that would pass the item limit with its own message, before any slide is stored', async () => {
    const { client, written } = fakeClient();
    const blobStore = new InMemoryImageBlobStore();
    const puts = jest.spyOn(blobStore, 'putBlob');
    const store = new DdbPresentationDocumentStore({ docClient: client, blobStore, mirror: null });
    const spec = { ...sampleDeck(), slides: sampleDeck().slides.map((slide) => ({ ...slide, notes: 'n'.repeat(60_000) })) };
    const attempt = store.appendRevision({ documentId: 'doc-1', title: 'Deck', createdBy: 'u', pptx: Buffer.from('PK'), slides, spec });
    await expect(attempt).rejects.toBeInstanceOf(PresentationRowTooLargeError);
    await expect(attempt).rejects.toThrow(new RegExp(`over the ${MAX_ROW_BYTES / 1024} KB a document row may hold`));
    expect(written()).toBeNull();
    expect(puts.mock.calls.filter(([key]) => String(key).startsWith('deck-svg-'))).toHaveLength(0);
  });

  it('estimates an item the way DynamoDB counts it', () => {
    expect(estimateItemBytes({ a: 'xyz' })).toBe(4);
    expect(estimateItemBytes({ list: ['ab', 'c'] })).toBe(4 + 3 + 3 + 2);
    expect(estimateItemBytes({ m: { k: 'v' } })).toBe(1 + 3 + 1 + 1 + 1);
  });

  it('counts what a row already holds, without double-counting the two places it records slides', () => {
    const slide = { id: 'a', index: 1, title: 'A', previewHandle: 'h', asset: { key: 'k', contentType: 'image/svg+xml', sizeBytes: 5 }, svg: '12345' };
    expect(inlineSlideBytes({ documentId: 'd', revisions: [{ id: 'v1', label: 'v1', createdAt: '', createdBy: '', slideCount: 1, asset: slide.asset, slides: [slide] }], revisionSlides: { v1: [slide] } })).toBe(5);
    expect(inlineSlideBytes(null)).toBe(0);
  });
});

describe('the .pptx has a second home', () => {
  it('is written to both stores under the same key', async () => {
    const primary = new InMemoryImageBlobStore();
    const mirror = new InMemoryImageBlobStore();
    const { client } = fakeClient();
    const store = new DdbPresentationDocumentStore({ docClient: client, blobStore: primary, mirror });
    const { revision } = await store.appendRevision({ documentId: 'doc-1', title: 'Deck', createdBy: 'u', pptx: Buffer.from('PKdeck'), slides });

    expect((await primary.getBlob(revision.asset.key))!.body.toString('utf8')).toBe('PKdeck');
    expect((await mirror.getBlob(revision.asset.key))!.body.toString('utf8')).toBe('PKdeck');
  });

  it('serves the file from the durable copy when the volume no longer has it', async () => {
    const primary = new InMemoryImageBlobStore();
    const mirror = new InMemoryImageBlobStore();
    await mirror.putBlob('deck-doc-1-v1.pptx', Buffer.from('PKdeck'), { contentType: PPTX_CONTENT_TYPE });
    const { client } = fakeClient();
    const store = new DdbPresentationDocumentStore({ docClient: client, blobStore: primary, mirror });
    jest.spyOn(store, 'get').mockResolvedValue({
      documentId: 'doc-1',
      revisions: [{
        id: 'v1', label: 'v1', createdAt: '', createdBy: 'u', slideCount: 1,
        asset: { key: 'deck-doc-1-v1.pptx', contentType: PPTX_CONTENT_TYPE, sizeBytes: 6 },
        slides: [],
      }],
    });

    const file = await store.revisionFile('doc-1', 'v1');
    expect(file).toMatchObject({ from: 'mirror', contentType: PPTX_CONTENT_TYPE });
    expect(file!.body.toString('utf8')).toBe('PKdeck');
  });

  it('refuses a deck too big for the durable copy with a 413 that says why (NFR #61: over one item is chunked, not refused)', async () => {
    const { client } = fakeClient();
    const store = new DdbPresentationDocumentStore({
      docClient: client, blobStore: new InMemoryImageBlobStore(), mirror: new InMemoryImageBlobStore(),
    });
    await expect(store.appendRevision({
      documentId: 'doc-1', title: 'Deck', createdBy: 'u',
      pptx: Buffer.alloc(IMAGE_BLOB_SIZE_CAP_BYTES + 1), slides,
    })).resolves.toMatchObject({ documentId: 'doc-1' });
    await expect(store.appendRevision({
      documentId: 'doc-1', title: 'Deck', createdBy: 'u',
      pptx: Buffer.alloc(MAX_CHUNKED_BLOB_BYTES + 1), slides,
    })).rejects.toMatchObject({ status: 413, message: expect.stringMatching(/nothing was saved/) });
  });

  it('keeps one copy when the primary store is already the durable one', async () => {
    const { client } = fakeClient();
    const store = new DdbPresentationDocumentStore({ docClient: client, blobStore: new InMemoryImageBlobStore(), mirror: null });
    const { revision } = await store.appendRevision({ documentId: 'doc-1', title: 'Deck', createdBy: 'u', pptx: Buffer.from('PK'), slides });
    expect(revision.asset.key).toMatch(/^deck-doc-1-v1-[A-Za-z0-9]{8}\.pptx$/);
  });
});

describe('a deck’s blobs go to the configured directory', () => {
  const originalStore = process.env.IMAGE_BLOB_STORE;
  const originalDir = process.env.IMAGE_BLOB_DIR;
  let directory: string;

  beforeEach(() => {
    directory = mkdtempSync(join(tmpdir(), 'deck-blobs-'));
    process.env.IMAGE_BLOB_STORE = 'fs';
    process.env.IMAGE_BLOB_DIR = directory;
    __resetVisionServicesForTests();
  });

  afterEach(() => {
    rmSync(directory, { recursive: true, force: true });
    if (originalStore === undefined) delete process.env.IMAGE_BLOB_STORE; else process.env.IMAGE_BLOB_STORE = originalStore;
    if (originalDir === undefined) delete process.env.IMAGE_BLOB_DIR; else process.env.IMAGE_BLOB_DIR = originalDir;
    __resetVisionServicesForTests();
  });

  it('resolves the configured volume store, and says where it writes', () => {
    const store = getAssetBlobStore();
    expect(store).toBeInstanceOf(VolumeImageBlobStore);
    expect((store as VolumeImageBlobStore).directory).toBe(directory);
  });

  it('writes the .pptx and every slide into it', async () => {
    const { client } = fakeClient();
    // No blobStore override: the store resolves the configured one, the way it
    // does in the pod.
    const presentation = new DdbPresentationDocumentStore({ docClient: client });
    const { revision } = await presentation.appendRevision({
      documentId: 'doc-1', title: 'Deck', createdBy: 'u', pptx: Buffer.from('PKdeck'), slides,
    });

    const files = readdirSync(directory);
    expect(files).toContain(revision.asset.key);
    for (const slide of revision.slides) expect(files).toContain(slide.asset.key);
    expect(revision.asset.key).toMatch(/^deck-doc-1-v1-[A-Za-z0-9]{8}\.pptx$/);
  });
});
