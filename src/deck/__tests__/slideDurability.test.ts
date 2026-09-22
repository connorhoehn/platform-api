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
import { GetCommand, PutCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { InMemoryImageBlobStore } from '../../pipeline/image/ImageBlobStore';
import { VolumeImageBlobStore } from '../../pipeline/image/VolumeImageBlobStore';
import { __resetVisionServicesForTests, getAssetBlobStore } from '../../pipeline/image/visionServices';
import {
  DdbPresentationDocumentStore, MAX_INLINE_SLIDE_BYTES, inlineSlideBytes,
  type PresentationDocumentRow,
} from '../presentationDocuments';

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
      return {};
    },
  } as unknown as DynamoDBDocumentClient;
  return { client, written: () => row };
}

describe('a slide’s markup lives on the row', () => {
  it('is written inline beside the blob copy', async () => {
    const blobStore = new InMemoryImageBlobStore();
    const { client, written } = fakeClient();
    const store = new DdbPresentationDocumentStore({ docClient: client, blobStore, now: () => new Date('2026-09-21T10:00:00.000Z') });
    const { revision } = await store.appendRevision({ documentId: 'doc-1', title: 'Deck', createdBy: 'u', pptx: Buffer.from('PK'), slides });

    expect(revision.slides[0]!.svg).toBe(slides[0]!.svg);
    expect(written()!.revisionSlides!.v1![0]!.svg).toBe(slides[0]!.svg);
    expect((await blobStore.getBlob(revision.slides[0]!.asset.key))!.body.toString('utf8')).toBe(slides[0]!.svg);
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

  it('refuses a revision that would put more markup on the row than it can hold', async () => {
    const { client } = fakeClient();
    const store = new DdbPresentationDocumentStore({ docClient: client, blobStore: new InMemoryImageBlobStore() });
    const huge = Array.from({ length: 40 }, (_v, index) => ({
      id: `s${index}`, title: `S${index}`, svg: `<svg>${'x'.repeat(10_000)}</svg>`,
    }));
    await expect(store.appendRevision({ documentId: 'doc-1', title: 'Deck', createdBy: 'u', pptx: Buffer.from('PK'), slides: huge }))
      .rejects.toThrow(new RegExp(`over the ${MAX_INLINE_SLIDE_BYTES}B`));
  });

  it('counts what a row already holds, without double-counting the two places it records slides', () => {
    const slide = { id: 'a', index: 1, title: 'A', previewHandle: 'h', asset: { key: 'k', contentType: 'image/svg+xml', sizeBytes: 5 }, svg: '12345' };
    expect(inlineSlideBytes({ documentId: 'd', revisions: [{ id: 'v1', label: 'v1', createdAt: '', createdBy: '', slideCount: 1, asset: slide.asset, slides: [slide] }], revisionSlides: { v1: [slide] } })).toBe(5);
    expect(inlineSlideBytes(null)).toBe(0);
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
    expect(files).toContain('deck-doc-1-v1.pptx');
    expect(files).toContain('deck-doc-1-v1-cover.svg');
    expect(files).toContain('deck-doc-1-v1-sources.svg');
    expect(revision.asset.key).toBe('deck-doc-1-v1.pptx');
  });
});
