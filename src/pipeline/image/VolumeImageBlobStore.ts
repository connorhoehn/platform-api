// platform-api/src/pipeline/image/VolumeImageBlobStore.ts
//
// Filesystem-backed ImageBlobStore implementation. Stores each image as
// two files under a base directory:
//
//   <dir>/<imageId>          — raw image bytes
//   <dir>/<imageId>.meta.json — ImageMeta sidecar
//
// Base directory is taken from the IMAGE_BLOB_DIR env var; defaults to
// /var/lib/pipeline-images. The directory is created recursively on
// construction if it does not exist.
//
// Motivation: DDB items are capped at ~400KB (350KB usable for body),
// which limits upscaled images to a max edge of ~512px after compression.
// The VolumeImageBlobStore has no inline size cap — a full-resolution
// 2048×2048 image (~8–25 MB) fits naturally. Mirrors the LVS-recordings
// hostPath precedent for local-dev + operator-managed volume storage.
//
// No cloud SDKs. Uses node:fs/promises only.

import { mkdir, writeFile, readFile, access } from 'node:fs/promises';
import { join, basename } from 'node:path';
import type { AssetBlobStore, BlobAsset, ImageBlobStore, PutBlobMeta } from './ImageBlobStore';
import type { ImageMeta } from './types';

/** Default base dir — override with IMAGE_BLOB_DIR env var. */
const DEFAULT_BLOB_DIR = '/var/lib/pipeline-images';

/** Storage capability: full 2048 edge, 25 MB byte ceiling. */
export const VOLUME_MAX_STORED_EDGE = 2048;
export const VOLUME_CAP_BYTES = 25 * 1024 * 1024;

/**
 * Sanitize an imageId for use as a filename. UUIDs are safe by
 * construction (hex + hyphens, no path separators) but we defend
 * against path traversal just in case an unexpected id leaks through.
 *
 * Strips any leading dots or path separators and rejects ids that
 * contain directory separators or null bytes.
 */
function sanitizeImageId(imageId: string): string {
  // Reject obvious path traversal patterns early — cleaner than
  // stripping and silently accepting a transformed id.
  if (
    imageId.includes('/') ||
    imageId.includes('\\') ||
    imageId.includes('\0') ||
    imageId.includes('..') ||
    imageId.trim() === ''
  ) {
    throw new Error(`VolumeImageBlobStore: invalid imageId '${imageId}'`);
  }
  // basename is a final safety net — it strips any remaining directory
  // component that somehow passed the checks above.
  const safe = basename(imageId);
  if (!safe || safe !== imageId) {
    throw new Error(`VolumeImageBlobStore: imageId '${imageId}' normalised to unexpected value`);
  }
  return safe;
}

export class VolumeImageBlobStore implements ImageBlobStore, AssetBlobStore {
  /** Largest edge that this store is willing to preserve. */
  readonly maxStoredEdge = VOLUME_MAX_STORED_EDGE;
  /** Byte ceiling for a single stored image. */
  readonly capBytes = VOLUME_CAP_BYTES;

  private readonly dir: string;
  /** Whether mkdir has been called at least once (lazy-init guard). */
  private ensured = false;

  constructor(dir?: string) {
    this.dir = dir ?? (process.env['IMAGE_BLOB_DIR'] || DEFAULT_BLOB_DIR);
  }

  /** Create the directory if it doesn't exist (idempotent). */
  private async ensureDir(): Promise<void> {
    if (this.ensured) return;
    await mkdir(this.dir, { recursive: true });
    this.ensured = true;
  }

  private bodyPath(imageId: string): string {
    return join(this.dir, sanitizeImageId(imageId));
  }

  private metaPath(imageId: string): string {
    return join(this.dir, `${sanitizeImageId(imageId)}.meta.json`);
  }

  async put(imageId: string, body: Buffer, meta: ImageMeta): Promise<string> {
    await this.ensureDir();
    await Promise.all([
      writeFile(this.bodyPath(imageId), body),
      writeFile(this.metaPath(imageId), JSON.stringify(meta), 'utf8'),
    ]);
    return imageId;
  }

  async get(imageId: string): Promise<{ body: Buffer; meta: ImageMeta } | null> {
    try {
      const [body, metaRaw] = await Promise.all([
        readFile(this.bodyPath(imageId)),
        readFile(this.metaPath(imageId), 'utf8'),
      ]);
      const meta = JSON.parse(metaRaw) as ImageMeta;
      return { body, meta };
    } catch (err) {
      // ENOENT → missing image → soft-null (same contract as DdbImageBlobStore)
      if ((err as NodeJS.ErrnoException).code === 'ENOENT') return null;
      // eslint-disable-next-line no-console
      console.warn(
        '[VolumeImageBlobStore] get failed; returning null',
        { imageId, dir: this.dir, err: err instanceof Error ? err.message : String(err) },
      );
      return null;
    }
  }

  async putBlob(key: string, body: Buffer, meta: PutBlobMeta): Promise<BlobAsset> {
    await this.ensureDir();
    const asset: BlobAsset = { key, contentType: meta.contentType, sizeBytes: body.byteLength };
    await Promise.all([
      writeFile(this.bodyPath(key), body),
      writeFile(this.metaPath(key), JSON.stringify({
        ...asset,
        createdAt: new Date().toISOString(),
        ...(meta.createdBy ? { createdBy: meta.createdBy } : {}),
        ...(meta.label ? { label: meta.label } : {}),
      }), 'utf8'),
    ]);
    return asset;
  }

  async getBlob(key: string): Promise<{ body: Buffer; asset: BlobAsset } | null> {
    try {
      const [body, metaRaw] = await Promise.all([
        readFile(this.bodyPath(key)),
        readFile(this.metaPath(key), 'utf8'),
      ]);
      const meta = JSON.parse(metaRaw) as Partial<BlobAsset>;
      return {
        body,
        asset: {
          key,
          contentType: meta.contentType ?? 'application/octet-stream',
          sizeBytes: meta.sizeBytes ?? body.byteLength,
        },
      };
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code === 'ENOENT') return null;
      // eslint-disable-next-line no-console
      console.warn(
        '[VolumeImageBlobStore] getBlob failed; returning null',
        { key, dir: this.dir, err: err instanceof Error ? err.message : String(err) },
      );
      return null;
    }
  }

  async getMetadata(imageId: string): Promise<ImageMeta | null> {
    try {
      // Only read the sidecar — avoids loading potentially-large body bytes.
      const metaRaw = await readFile(this.metaPath(imageId), 'utf8');
      return JSON.parse(metaRaw) as ImageMeta;
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code === 'ENOENT') return null;
      return null;
    }
  }
}
