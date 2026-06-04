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
    // DDB doesn't have a "projection skip body" GetItem option that's
    // cheaper than fetching the full item — Get pulls the whole row.
    // For v1 we delegate to get() and drop the body. If image bodies
    // grow large (multi-MB chunked spillover in some future revision),
    // revisit with an explicit ProjectionExpression that excludes `body`.
    const row = await this.get(imageId);
    return row?.meta ?? null;
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
