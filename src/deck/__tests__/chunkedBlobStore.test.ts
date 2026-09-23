import { GetCommand, PutCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { InMemoryImageBlobStore, IMAGE_BLOB_SIZE_CAP_BYTES, type PutBlobMeta } from '../../pipeline/image/ImageBlobStore';
import { BlobTooLargeError, CHUNK_MANIFEST_CONTENT_TYPE, ChunkedBlobStore, MAX_CHUNKED_BLOB_BYTES } from '../chunkedBlobStore';
import { DdbPresentationDocumentStore, type PresentationDocumentRow } from '../presentationDocuments';
import { PPTX_CONTENT_TYPE } from '../renderPptx';

// NFR #61: the durable DynamoDB copy of a .pptx holds 350 KB per item; a
// 40-slide deck with long notes renders ~428 KB and its save failed with a 500.

/** An in-memory store that refuses bodies over one DDB item, like DdbImageBlobStore, and records write order. */
class CappedStore extends InMemoryImageBlobStore {
  order: string[] = [];
  async putBlob(key: string, body: Buffer, meta: PutBlobMeta) {
    if (body.byteLength > IMAGE_BLOB_SIZE_CAP_BYTES) throw new Error(`over the DDB inline cap: ${key}`);
    this.order.push(key);
    return super.putBlob(key, body, meta);
  }
}

const meta = { contentType: PPTX_CONTENT_TYPE, createdBy: 'user-frank', label: 'deck v1.pptx' };
const bytes = (n: number) => Buffer.from(Array.from({ length: n }, (_, i) => (i * 31 + 7) % 251));

describe('ChunkedBlobStore', () => {
  it('passes a body within one item straight through', async () => {
    const inner = new CappedStore();
    const store = new ChunkedBlobStore(inner);
    await store.putBlob('presentations/d/v1-a.pptx', bytes(1000), meta);
    expect(inner.order).toEqual(['presentations/d/v1-a.pptx']);
    expect((await store.getBlob('presentations/d/v1-a.pptx'))!.body.equals(bytes(1000))).toBe(true);
  });

  it('stores a 428 KB body as parts written before the head, and reads it back byte for byte', async () => {
    const inner = new CappedStore();
    const store = new ChunkedBlobStore(inner);
    const body = bytes(428 * 1024);
    const asset = await store.putBlob('presentations/d/v1-a.pptx', body, meta);
    expect(asset).toEqual({ key: 'presentations/d/v1-a.pptx', contentType: PPTX_CONTENT_TYPE, sizeBytes: body.byteLength });
    expect(inner.order).toHaveLength(3);
    expect(inner.order[2]).toBe('presentations/d/v1-a.pptx'); // head last
    expect((await inner.getBlob('presentations/d/v1-a.pptx'))!.asset.contentType).toBe(CHUNK_MANIFEST_CONTENT_TYPE);
    const got = await store.getBlob('presentations/d/v1-a.pptx');
    expect(got!.body.equals(body)).toBe(true);
    expect(got!.asset).toEqual({ key: 'presentations/d/v1-a.pptx', contentType: PPTX_CONTENT_TYPE, sizeBytes: body.byteLength });
  });

  it('a missing or altered part reads as absent, never as wrong bytes', async () => {
    const inner = new CappedStore();
    const store = new ChunkedBlobStore(inner);
    await store.putBlob('presentations/d/v1-a.pptx', bytes(700 * 1024), meta);
    await inner.putBlob('presentations/d/v1-a.pptx~part-2', bytes(10), meta);
    expect(await store.getBlob('presentations/d/v1-a.pptx')).toBeNull();
  });

  it('the sweeper sees heads only, and deleting a head deletes its parts', async () => {
    const inner = new CappedStore();
    const store = new ChunkedBlobStore(inner);
    await store.putBlob('presentations/d/v1-a.pptx', bytes(700 * 1024), meta);
    await store.putBlob('presentations/d/v2-b.pptx', bytes(10), meta);
    const listed: { key: string; touchedAtMs: number }[] = [];
    for await (const entry of store.listAssets('presentations/')) listed.push(entry);
    expect(listed.map((entry) => entry.key).sort()).toEqual(['presentations/d/v1-a.pptx', 'presentations/d/v2-b.pptx']);
    const head = listed.find((entry) => entry.key.endsWith('v1-a.pptx'))!;
    expect(await store.deleteAssetIfUntouched(head.key, head.touchedAtMs)).toBe(true);
    const left: string[] = [];
    for await (const entry of inner.listAssets('presentations/')) left.push(entry.key);
    expect(left).toEqual(['presentations/d/v2-b.pptx']);
  });

  it('refuses over the chunked limit with a 413 and writes nothing', async () => {
    const inner = new CappedStore();
    const store = new ChunkedBlobStore(inner);
    await expect(store.putBlob('presentations/d/v1-a.pptx', Buffer.alloc(MAX_CHUNKED_BLOB_BYTES + 1), meta))
      .rejects.toMatchObject({ status: 413 });
    expect(inner.order).toEqual([]);
  });
});

describe('DdbPresentationDocumentStore with a chunked durable copy (NFR #61)', () => {
  function fakeClient(row: () => PresentationDocumentRow | null) {
    const puts: Record<string, unknown>[] = [];
    const client = {
      async send(command: unknown) {
        const input = (command as { input: Record<string, unknown> }).input;
        if (command instanceof GetCommand) return { Item: row() ?? undefined };
        if (command instanceof PutCommand) puts.push(input.Item as Record<string, unknown>);
        return {};
      },
    } as unknown as DynamoDBDocumentClient;
    return { client, puts };
  }
  const slides = [{ id: 'cover', title: 'Cover', svg: '<svg>cover</svg>' }, { id: 'sources', title: 'Sources', svg: '<svg>s</svg>' }];

  it('saves a 428 KB .pptx and serves it from the durable copy once the volume has lost it', async () => {
    const volume = new InMemoryImageBlobStore();
    const durable = new CappedStore();
    let saved: PresentationDocumentRow | null = null;
    const { client, puts } = fakeClient(() => saved);
    const store = new DdbPresentationDocumentStore({ docClient: client, blobStore: volume, mirror: new ChunkedBlobStore(durable) });
    const pptx = bytes(428 * 1024);
    const { revision } = await store.appendRevision({ documentId: 'audit-61', title: 'Forty slides', createdBy: 'user-frank', pptx, slides });
    saved = puts[0] as unknown as PresentationDocumentRow;
    volume.reset(); // the pod and its hostPath are gone
    const file = await store.revisionFile('audit-61', revision.id);
    expect(file?.from).toBe('mirror');
    expect(file!.body.equals(pptx)).toBe(true);
  });

  it('a .pptx over the durable limit is a 413 before any blob or row is written', async () => {
    const volume = new InMemoryImageBlobStore();
    const durable = new CappedStore();
    const { client, puts } = fakeClient(() => null);
    const store = new DdbPresentationDocumentStore({ docClient: client, blobStore: volume, mirror: new ChunkedBlobStore(durable) });
    const error = await store.appendRevision({ documentId: 'audit-61', title: 'Huge', createdBy: 'user-frank', pptx: Buffer.alloc(MAX_CHUNKED_BLOB_BYTES + 1), slides })
      .catch((err: unknown) => err);
    expect(error).toBeInstanceOf(BlobTooLargeError);
    expect((error as BlobTooLargeError).status).toBe(413);
    expect((error as Error).message).toMatch(/nothing was saved/);
    expect(durable.order).toEqual([]);
    const onVolume: string[] = [];
    for await (const entry of volume.listAssets('')) onVolume.push(entry.key);
    expect(onVolume).toEqual([]);
    expect(puts).toEqual([]);
  });
});
