// platform-api/src/pipeline/judge/RawTextStore.ts
//
// v2 — pluggable storage for judgment rawModelText payloads that exceed
// the DDB inline cap (32KB). The default impl writes to S3; an
// in-memory impl exists for tests + envs without S3 configured.
//
// Why a store interface instead of inlining S3 calls into the repo:
//   - Tests can drive the spillover path without a real bucket.
//   - Local-dev (Tilt) can use the in-memory impl when no S3 endpoint
//     is configured, falling back gracefully.
//   - A future LocalStack swap stays a one-line change in bootstrap.
//
// Key shape: `judgments/${judgmentId}.txt` — flat namespace, judgmentId
// is a UUID so collisions are not a concern. We don't shard by date
// because the per-judgment DDB row's TTL handles retention and S3
// lifecycle policies (operator-side) handle bucket-level pruning.

import {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
  type S3ClientConfig,
} from '@aws-sdk/client-s3';
import { Readable } from 'stream';

export interface RawTextStore {
  /**
   * Persist a body for the given judgmentId. Returns the storage key
   * that should be saved on the DDB row (`rawTextS3Key`).
   *
   * Idempotent on judgmentId — re-uploads overwrite.
   */
  put(judgmentId: string, body: string): Promise<string>;

  /**
   * Fetch a body by key. Returns null when the key doesn't exist or the
   * fetch fails (caller's caller decides whether to surface a UI error).
   */
  get(key: string): Promise<string | null>;
}

/**
 * Canonical key shape — exported so callers can mock or pre-compute.
 */
export function rawTextKeyFor(judgmentId: string): string {
  return `judgments/${judgmentId}.txt`;
}

// ---------------------------------------------------------------------------
// Real S3 impl
// ---------------------------------------------------------------------------

export interface S3RawTextStoreOptions {
  bucket: string;
  client?: S3Client;
  s3ClientConfig?: S3ClientConfig;
}

export class S3RawTextStore implements RawTextStore {
  private readonly bucket: string;
  private readonly client: S3Client;

  constructor(opts: S3RawTextStoreOptions) {
    this.bucket = opts.bucket;
    this.client = opts.client ?? new S3Client(opts.s3ClientConfig ?? {});
  }

  async put(judgmentId: string, body: string): Promise<string> {
    const key = rawTextKeyFor(judgmentId);
    await this.client.send(
      new PutObjectCommand({
        Bucket: this.bucket,
        Key: key,
        Body: body,
        ContentType: 'text/plain; charset=utf-8',
      }),
    );
    return key;
  }

  async get(key: string): Promise<string | null> {
    try {
      const result = await this.client.send(
        new GetObjectCommand({ Bucket: this.bucket, Key: key }),
      );
      const body = result.Body;
      if (!body) return null;
      if (typeof (body as { transformToString?: unknown }).transformToString === 'function') {
        return await (body as { transformToString: () => Promise<string> }).transformToString();
      }
      // Older SDK / Node Readable path
      const chunks: Buffer[] = [];
      for await (const chunk of body as unknown as Readable) {
        chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk as Uint8Array));
      }
      return Buffer.concat(chunks).toString('utf-8');
    } catch {
      return null;
    }
  }
}

// ---------------------------------------------------------------------------
// In-memory impl (tests + local-dev without S3)
// ---------------------------------------------------------------------------

export class InMemoryRawTextStore implements RawTextStore {
  private readonly bodies = new Map<string, string>();

  async put(judgmentId: string, body: string): Promise<string> {
    const key = rawTextKeyFor(judgmentId);
    this.bodies.set(key, body);
    return key;
  }

  async get(key: string): Promise<string | null> {
    return this.bodies.get(key) ?? null;
  }

  /** Test helper — clear all stored bodies. */
  reset(): void {
    this.bodies.clear();
  }
}
