// platform-api/src/pipeline/image/__tests__/VolumeImageBlobStore.test.ts
//
// Round-trip + capability tests for VolumeImageBlobStore.
// Uses an OS tmp dir so no permanent files are left behind.

import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import sharp from 'sharp';
import { VolumeImageBlobStore, VOLUME_MAX_STORED_EDGE, VOLUME_CAP_BYTES } from '../VolumeImageBlobStore';
import { IMAGE_BLOB_SIZE_CAP_BYTES } from '../ImageBlobStore';
import type { ImageMeta } from '../types';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeMeta(width: number, height: number, extra: Partial<ImageMeta> = {}): ImageMeta {
  return {
    mimeType: 'image/png',
    sizeBytes: width * height * 3,
    width,
    height,
    uploadedAt: new Date().toISOString(),
    ...extra,
  };
}

/** Build a real PNG of the given dimensions (grey fill). */
async function makePng(width: number, height: number): Promise<Buffer> {
  const pixels = Buffer.alloc(width * height * 3, 128);
  return sharp(pixels, { raw: { width, height, channels: 3 } }).png().toBuffer();
}

/** Build a noisy PNG large enough to exceed the DDB 350KB cap. */
async function makeLargePng(): Promise<Buffer> {
  const w = 800;
  const h = 800;
  const pixels = Buffer.alloc(w * h * 3);
  for (let i = 0; i < pixels.length; i++) {
    pixels[i] = (i * 131 + 37) % 256;
  }
  return sharp(pixels, { raw: { width: w, height: h, channels: 3 } })
    .png({ compressionLevel: 0 })
    .toBuffer();
}

// ---------------------------------------------------------------------------
// Suite
// ---------------------------------------------------------------------------

describe('VolumeImageBlobStore', () => {
  let dir: string;
  let store: VolumeImageBlobStore;

  beforeEach(async () => {
    dir = await mkdtemp(join(tmpdir(), 'vibs-test-'));
    store = new VolumeImageBlobStore(dir);
  });

  afterEach(async () => {
    await rm(dir, { recursive: true, force: true });
  });

  // ── Capability declarations ─────────────────────────────────────────────

  it('declares maxStoredEdge = 2048', () => {
    expect(store.maxStoredEdge).toBe(VOLUME_MAX_STORED_EDGE);
    expect(store.maxStoredEdge).toBe(2048);
  });

  it('declares capBytes = 25 MB', () => {
    expect(store.capBytes).toBe(VOLUME_CAP_BYTES);
    expect(store.capBytes).toBe(25 * 1024 * 1024);
  });

  // ── Round-trip ──────────────────────────────────────────────────────────

  it('put → get returns the original bytes and meta intact', async () => {
    const bytes = await makePng(64, 64);
    const meta = makeMeta(64, 64, { uploadedBy: 'test-user' });
    const id = 'aaaabbbb-1111-2222-3333-ccccddddeeee';

    await store.put(id, bytes, meta);
    const result = await store.get(id);

    expect(result).not.toBeNull();
    expect(result!.body).toEqual(bytes);
    expect(result!.meta.mimeType).toBe('image/png');
    expect(result!.meta.width).toBe(64);
    expect(result!.meta.height).toBe(64);
    expect(result!.meta.uploadedBy).toBe('test-user');
  });

  it('put → getMetadata returns meta without needing the body', async () => {
    const bytes = await makePng(128, 128);
    const meta = makeMeta(128, 128);
    const id = 'aaaabbbb-0000-1111-2222-333344445555';

    await store.put(id, bytes, meta);
    const fetchedMeta = await store.getMetadata(id);

    expect(fetchedMeta).not.toBeNull();
    expect(fetchedMeta!.width).toBe(128);
    expect(fetchedMeta!.height).toBe(128);
    expect(fetchedMeta!.mimeType).toBe('image/png');
  });

  // ── Missing key → null ──────────────────────────────────────────────────

  it('get returns null when imageId does not exist', async () => {
    const result = await store.get('does-not-exist-1234-abcd-efef-abcdef012345');
    expect(result).toBeNull();
  });

  it('getMetadata returns null when imageId does not exist', async () => {
    const result = await store.getMetadata('does-not-exist-5678-abcd-efef-fedcba987654');
    expect(result).toBeNull();
  });

  // ── Large image (>350KB DDB cap) stored intact ──────────────────────────

  it('stores a large image (>350KB) intact without truncation', async () => {
    const bigBytes = await makeLargePng();

    // Verify the fixture is actually over the DDB cap so the test is meaningful.
    expect(bigBytes.byteLength).toBeGreaterThan(IMAGE_BLOB_SIZE_CAP_BYTES);

    const meta = makeMeta(800, 800);
    meta.sizeBytes = bigBytes.byteLength;
    const id = 'bigimage-1111-2222-3333-aabbccddeeff';

    await store.put(id, bigBytes, meta);
    const result = await store.get(id);

    expect(result).not.toBeNull();
    expect(result!.body.byteLength).toBe(bigBytes.byteLength);
    expect(result!.body).toEqual(bigBytes);
  });

  // ── Idempotent overwrite ─────────────────────────────────────────────────

  it('overwrites a previous put with the same imageId', async () => {
    const id = 'overwrite-test-1234-5678-abcd-efgh1234';
    const v1 = await makePng(32, 32);
    const v2 = await makePng(64, 64);

    await store.put(id, v1, makeMeta(32, 32));
    await store.put(id, v2, makeMeta(64, 64));

    const result = await store.get(id);
    expect(result!.meta.width).toBe(64);
    expect(result!.body.byteLength).toBe(v2.byteLength);
  });

  // ── Path traversal defense ───────────────────────────────────────────────

  it('rejects imageIds with path separators', async () => {
    const bytes = await makePng(8, 8);
    const meta = makeMeta(8, 8);
    await expect(store.put('../etc/passwd', bytes, meta)).rejects.toThrow(/invalid imageId/);
  });

  it('rejects imageIds with null bytes', async () => {
    const bytes = await makePng(8, 8);
    const meta = makeMeta(8, 8);
    await expect(store.put('id\0evil', bytes, meta)).rejects.toThrow(/invalid imageId/);
  });
});
