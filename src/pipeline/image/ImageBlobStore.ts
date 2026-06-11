// platform-api/src/pipeline/image/ImageBlobStore.ts
//
// DDB-backed storage for image bytes + metadata. Two impls:
//
//   - DdbImageBlobStore  — production: writes to `pipeline-images` DDB
//                          table keyed by imageId. Same substrate as
//                          every other pipeline-* table; no S3, no
//                          cloud-only deps (operator standing rule).
//   - InMemoryImageBlobStore — tests + envs without DDB; mirrors the
//                              interface exactly.
//
// Bodies >~390KB still drop — they don't fit in a single DDB item.
// Client-side resize (frontend/src/lib/imagePreprocess.ts caps long
// edge at 1568px) brings typical phone photos well under this. The
// HTTP route enforces the cap on server too.
//
// Code parity with platform-api/src/pipeline/judge/RawTextStore.ts.

import {
  DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
} from '@aws-sdk/lib-dynamodb';
import { docClient as defaultDocClient } from '../../lib/aws-clients';
import { tableName } from '../../lib/ddb-table-name';
import type { ImageMeta } from './types';

const PIPELINE_IMAGES_TABLE = tableName('pipeline-images');

const DEFAULT_TTL_DAYS = 30;
const SECONDS_PER_DAY = 24 * 60 * 60;

/**
 * Hard cap on bytes-per-image stored inline. DDB item limit is 400KB
 * total, so leaving headroom for metadata + ttl epoch.
 */
export const IMAGE_BLOB_SIZE_CAP_BYTES = 350 * 1024;

export interface ImageBlobStore {
  /**
   * Optional: largest image edge (in pixels) this store is willing to
   * preserve. Step handlers use this to decide how aggressively to
   * cap-fit before calling `put()`.
   *
   * - DdbImageBlobStore:    512 (DDB inline cap ≈ 350KB)
   * - VolumeImageBlobStore: 2048 (full-res upscale)
   * - InMemoryImageBlobStore: undefined (no cap, tests decide)
   *
   * When undefined, callers fall back to their own default.
   */
  readonly maxStoredEdge?: number;

  /**
   * Optional: maximum byte size this store accepts per image. Step
   * handlers pass this as `capBytes` to `capFitImage()` so the
   * compression target matches the store's limit.
   *
   * - DdbImageBlobStore:    350 * 1024
   * - VolumeImageBlobStore: 25 * 1024 * 1024
   * - InMemoryImageBlobStore: undefined
   */
  readonly capBytes?: number;

  /**
   * Persist a body + metadata for the given imageId. Returns the same
   * imageId for symmetry with the rawtext store (caller already chose
   * the id, this is just confirmation).
   *
   * Idempotent on imageId — re-uploads overwrite.
   *
   * Throws when body exceeds IMAGE_BLOB_SIZE_CAP_BYTES so callers
   * can surface a clean 413 to the upload route.
   */
  put(imageId: string, body: Buffer, meta: ImageMeta): Promise<string>;

  /**
   * Fetch a body + metadata by imageId. Returns null when the key
   * doesn't exist or the fetch fails (caller decides whether to
   * surface UI).
   */
  get(imageId: string): Promise<{ body: Buffer; meta: ImageMeta } | null>;

  /**
   * Fetch metadata only (skips the body bytes). Cheaper for thumbnail
   * routes and admin listings that don't need the full image.
   */
  getMetadata(imageId: string): Promise<ImageMeta | null>;
}

// ---------------------------------------------------------------------------
// DDB-backed impl
// ---------------------------------------------------------------------------

export interface DdbImageBlobStoreOptions {
  /** Override for tests. Defaults to the shared docClient. */
  docClient?: DynamoDBDocumentClient;
  /** Override retention. Defaults to 30 days (shorter than judgments — images are transient). */
  ttlDays?: number;
  /** Override wall-clock for tests. Returns ms-since-epoch. */
  now?: () => number;
}

/** Persistence row shape — imageId-keyed, body + metadata + ttl. */
interface ImageRow {
  imageId: string;
  body: Buffer;
  mimeType: string;
  sizeBytes: number;
  width: number;
  height: number;
  uploadedAt: string;
  uploadedBy?: string;
  /** Unix-seconds. DDB native TTL drops rows older than this. */
  ttlEpoch: number;
}

export class DdbImageBlobStore implements ImageBlobStore {
  /**
   * Max stored edge for DDB-backed images: 1024px. This is the historical
   * upscale default (DEFAULT_STORE_MAX_EDGE) — the capBytes ceiling + the
   * JPEG quality ladder in capFitImage keep a 1024px image under the DDB
   * 350KB item cap. NOT 512: setting it to 512 silently halved DDB upscale
   * output from its prior 1024 default. Generate is unaffected because its
   * source is already 512 and capFitImage uses withoutEnlargement.
   */
  readonly maxStoredEdge = 1024;
  /** DDB inline cap — leaves headroom for metadata + ttl. */
  readonly capBytes = IMAGE_BLOB_SIZE_CAP_BYTES;

  private readonly docClient: DynamoDBDocumentClient;
  private readonly ttlSeconds: number;
  private readonly now: () => number;

  constructor(opts: DdbImageBlobStoreOptions = {}) {
    this.docClient = opts.docClient ?? defaultDocClient;
    this.ttlSeconds = (opts.ttlDays ?? DEFAULT_TTL_DAYS) * SECONDS_PER_DAY;
    this.now = opts.now ?? (() => Date.now());
  }

  async put(imageId: string, body: Buffer, meta: ImageMeta): Promise<string> {
    if (body.byteLength > IMAGE_BLOB_SIZE_CAP_BYTES) {
      throw new Error(
        `image body ${body.byteLength}B exceeds DDB inline cap ${IMAGE_BLOB_SIZE_CAP_BYTES}B — resize before upload`,
      );
    }
    const ttlEpoch = Math.floor(this.now() / 1000) + this.ttlSeconds;
    const item: ImageRow = {
      imageId,
      body,
      mimeType: meta.mimeType,
      sizeBytes: meta.sizeBytes,
      width: meta.width,
      height: meta.height,
      uploadedAt: meta.uploadedAt,
      ...(meta.uploadedBy ? { uploadedBy: meta.uploadedBy } : {}),
      ttlEpoch,
    };
    await this.docClient.send(
      new PutCommand({
        TableName: PIPELINE_IMAGES_TABLE,
        Item: item,
      }),
    );
    return imageId;
  }

  async get(imageId: string): Promise<{ body: Buffer; meta: ImageMeta } | null> {
    try {
      const result = await this.docClient.send(
        new GetCommand({
          TableName: PIPELINE_IMAGES_TABLE,
          Key: { imageId },
        }),
      );
      const item = result.Item as ImageRow | undefined;
      if (!item) return null;
      // DDB may deliver `body` as Buffer | Uint8Array depending on the
      // marshaller config; normalize to Buffer.
      const body = Buffer.isBuffer(item.body) ? item.body : Buffer.from(item.body as unknown as Uint8Array);
      return {
        body,
        meta: {
          mimeType: item.mimeType,
          sizeBytes: item.sizeBytes,
          width: item.width,
          height: item.height,
          uploadedAt: item.uploadedAt,
          ...(item.uploadedBy ? { uploadedBy: item.uploadedBy } : {}),
        },
      };
    } catch (err) {
      // Soft-fail to null so the HTTP route surfaces a clean 404 instead
      // of a 500, but log so operators can distinguish "missing row" from
      // "table doesn't exist", "permission denied", "transient timeout".
      // eslint-disable-next-line no-console
      console.warn(
        '[DdbImageBlobStore] get failed; returning null',
        { imageId, table: PIPELINE_IMAGES_TABLE, err: err instanceof Error ? err.message : String(err) },
      );
      return null;
    }
  }

  async getMetadata(imageId: string): Promise<ImageMeta | null> {
    // Use a ProjectionExpression to fetch only the metadata fields,
    // skipping the `body` attribute (which can be up to 350 KB).
    // This saves DDB read-unit cost on metadata-only calls (thumbnails,
    // listings, HEAD-equivalent checks).
    try {
      const result = await this.docClient.send(
        new GetCommand({
          TableName: PIPELINE_IMAGES_TABLE,
          Key: { imageId },
          ProjectionExpression: 'imageId, mimeType, sizeBytes, #w, #h, uploadedAt, uploadedBy',
          // `width` and `height` are not reserved words but we alias them
          // for safety with other DDB expression parsers.
          ExpressionAttributeNames: { '#w': 'width', '#h': 'height' },
        }),
      );
      const item = result.Item as Partial<ImageRow> | undefined;
      if (!item?.imageId) return null;
      return {
        mimeType: item.mimeType ?? '',
        sizeBytes: item.sizeBytes ?? 0,
        width: item.width ?? 0,
        height: item.height ?? 0,
        uploadedAt: item.uploadedAt ?? '',
        ...(item.uploadedBy ? { uploadedBy: item.uploadedBy } : {}),
      };
    } catch {
      return null;
    }
  }
}

// ---------------------------------------------------------------------------
// In-memory impl (tests + envs without DDB)
// ---------------------------------------------------------------------------

export class InMemoryImageBlobStore implements ImageBlobStore {
  private readonly rows = new Map<string, { body: Buffer; meta: ImageMeta }>();

  async put(imageId: string, body: Buffer, meta: ImageMeta): Promise<string> {
    if (body.byteLength > IMAGE_BLOB_SIZE_CAP_BYTES) {
      throw new Error(
        `image body ${body.byteLength}B exceeds inline cap ${IMAGE_BLOB_SIZE_CAP_BYTES}B`,
      );
    }
    this.rows.set(imageId, { body, meta });
    return imageId;
  }

  async get(imageId: string): Promise<{ body: Buffer; meta: ImageMeta } | null> {
    return this.rows.get(imageId) ?? null;
  }

  async getMetadata(imageId: string): Promise<ImageMeta | null> {
    return this.rows.get(imageId)?.meta ?? null;
  }

  /** Test helper — clear all stored rows. */
  reset(): void {
    this.rows.clear();
  }
}
