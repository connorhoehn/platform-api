import { createHash } from 'crypto';
import {
  IMAGE_BLOB_SIZE_CAP_BYTES,
  type AssetBlobStore,
  type BlobAsset,
  type PutBlobMeta,
  type StoredAssetEntry,
  type SweepableAssetStore,
} from '../pipeline/image/ImageBlobStore';
import { AppError } from '../middleware/error-handler';

/**
 * NFR #61: a blob bigger than one DynamoDB item, stored as several.
 *
 * The durable copy of a deck's `.pptx` is a DynamoDB item (the volume is a
 * hostPath that goes with the node, and two platform replicas do not share
 * it). One item holds 350 KB of body; a 40-slide deck with long notes renders
 * ~428 KB, and the save failed with a generic 500. A body over the item cap is
 * now written as parts (`<key>~part-<n>`, each ≤ PART_BYTES) FIRST, then a
 * small head item under `<key>` naming them with their sha256 — so a head is
 * never visible before every part it names. Reads reassemble and verify;
 * sweeper listing hides the parts, and deleting a head deletes its parts.
 *
 * Anything over MAX_CHUNKED_BLOB_BYTES is refused with a 413 before a byte is
 * written (`assertChunkedBlobFits`).
 */

export const CHUNK_MANIFEST_CONTENT_TYPE = 'application/vnd.platform.blob-chunks+json';
/** Under the 350 KB item cap, leaving room for the item's other attributes. */
export const PART_BYTES = 320 * 1024;
/** 8 MB: 26 parts. A deck larger than this is not a save we want to accept inline. */
export const MAX_CHUNKED_BLOB_BYTES = 8 * 1024 * 1024;
const PART_MARK = '~part-';

interface ChunkManifest {
  chunked: 1;
  contentType: string;
  sizeBytes: number;
  sha256: string;
  parts: { key: string; sizeBytes: number; sha256: string }[];
}

const sha256 = (body: Buffer) => createHash('sha256').update(body).digest('hex');

export class BlobTooLargeError extends AppError {
  constructor(readonly key: string, readonly sizeBytes: number, readonly limitBytes: number, what = 'file') {
    super(413,
      `the ${what} is ${Math.ceil(sizeBytes / 1024)} KB, over the ${Math.round(limitBytes / 1024)} KB the durable store accepts — `
      + 'nothing was saved. Shorten the speaker notes, remove images or split the deck.');
    this.name = 'BlobTooLargeError';
  }
}

/** Throws the 413 before anything is written. */
export function assertChunkedBlobFits(key: string, sizeBytes: number, what?: string): void {
  if (sizeBytes > MAX_CHUNKED_BLOB_BYTES) throw new BlobTooLargeError(key, sizeBytes, MAX_CHUNKED_BLOB_BYTES, what);
}

export function isChunkPartKey(key: string): boolean {
  return key.includes(PART_MARK);
}

export class ChunkedBlobStore implements AssetBlobStore, SweepableAssetStore {
  constructor(
    readonly inner: AssetBlobStore & SweepableAssetStore,
    private readonly itemCapBytes = IMAGE_BLOB_SIZE_CAP_BYTES,
    private readonly partBytes = PART_BYTES,
  ) {}

  async putBlob(key: string, body: Buffer, meta: PutBlobMeta): Promise<BlobAsset> {
    if (body.byteLength <= this.itemCapBytes) return this.inner.putBlob(key, body, meta);
    assertChunkedBlobFits(key, body.byteLength);
    const parts: ChunkManifest['parts'] = [];
    for (let offset = 0, n = 1; offset < body.byteLength; offset += this.partBytes, n += 1) {
      const slice = body.subarray(offset, Math.min(offset + this.partBytes, body.byteLength));
      parts.push({ key: `${key}${PART_MARK}${n}`, sizeBytes: slice.byteLength, sha256: sha256(slice) });
    }
    // Parts first, then the head that names them.
    await Promise.all(parts.map((part, i) => this.inner.putBlob(
      part.key,
      body.subarray(i * this.partBytes, i * this.partBytes + part.sizeBytes),
      { contentType: 'application/octet-stream', ...(meta.createdBy ? { createdBy: meta.createdBy } : {}), label: `${meta.label ?? key} part ${i + 1}/${parts.length}` },
    )));
    const manifest: ChunkManifest = { chunked: 1, contentType: meta.contentType, sizeBytes: body.byteLength, sha256: sha256(body), parts };
    await this.inner.putBlob(key, Buffer.from(JSON.stringify(manifest)), { ...meta, contentType: CHUNK_MANIFEST_CONTENT_TYPE });
    return { key, contentType: meta.contentType, sizeBytes: body.byteLength };
  }

  async getBlob(key: string): Promise<{ body: Buffer; asset: BlobAsset } | null> {
    const head = await this.inner.getBlob(key);
    if (!head || head.asset.contentType !== CHUNK_MANIFEST_CONTENT_TYPE) return head;
    let manifest: ChunkManifest;
    try { manifest = JSON.parse(head.body.toString('utf8')); } catch { return null; }
    const bodies = await Promise.all(manifest.parts.map((part) => this.inner.getBlob(part.key)));
    // A missing or altered part means the bytes are not these: report absent,
    // so the caller falls through as it does for any other missing copy.
    if (bodies.some((got, i) => !got || sha256(got.body) !== manifest.parts[i]!.sha256)) return null;
    const body = Buffer.concat(bodies.map((got) => got!.body));
    if (body.byteLength !== manifest.sizeBytes || sha256(body) !== manifest.sha256) return null;
    return { body, asset: { key, contentType: manifest.contentType, sizeBytes: manifest.sizeBytes } };
  }

  async *listAssets(prefix: string): AsyncIterable<StoredAssetEntry> {
    for await (const entry of this.inner.listAssets(prefix)) {
      if (!isChunkPartKey(entry.key)) yield entry;
    }
  }

  async deleteAssetIfUntouched(key: string, touchedAtMs: number): Promise<boolean> {
    const deleted = await this.inner.deleteAssetIfUntouched(key, touchedAtMs);
    if (!deleted) return false;
    // The head is gone, so nothing can reach its parts any more.
    for await (const part of this.inner.listAssets(`${key}${PART_MARK}`)) {
      await this.inner.deleteAssetIfUntouched(part.key, part.touchedAtMs).catch(() => false);
    }
    return true;
  }

  /** The head's expiry; parts are written with none (they postdate NFR #48). */
  async clearExpiry(key: string): Promise<boolean> {
    return this.inner.clearExpiry ? this.inner.clearExpiry(key) : false;
  }
}
