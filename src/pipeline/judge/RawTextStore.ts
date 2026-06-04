// platform-api/src/pipeline/judge/RawTextStore.ts
//
// v2 — pluggable storage for judgment rawModelText payloads that exceed
// the inline DDB cap (32KB). Two impls:
//
//   - DdbRawTextStore  — production: writes to a second DDB table
//                        (`pipeline-judgment-rawtext`) keyed by
//                        judgmentId. Same substrate as
//                        pipeline-judgments + pipeline-judgment-
//                        aggregates; no extra services to run.
//   - InMemoryRawTextStore — tests + envs without DDB; mirrors the
//                            interface exactly.
//
// Bodies >~390KB still drop — they don't fit in a single DDB item.
// This is the practical tail for LLM responses; if you ever need
// "summarize War and Peace and store the model's verbatim 500KB
// thought-stream", that's a different feature.
//
// Why a store interface instead of inlining DDB calls into the repo:
//   - Tests can drive the spillover path without spinning a second
//     DDB-local table.
//   - Local-dev that doesn't have the table provisioned can fall back
//     to InMemoryRawTextStore without changing application code.
//   - Future move to a different backing store stays a one-line swap
//     in judgeServices.

import {
  DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
} from '@aws-sdk/lib-dynamodb';
import { docClient as defaultDocClient } from '../../lib/aws-clients';
import { tableName } from '../../lib/ddb-table-name';

const PIPELINE_JUDGMENT_RAWTEXT_TABLE = tableName('pipeline-judgment-rawtext');

const DEFAULT_TTL_DAYS = 90;
const SECONDS_PER_DAY = 24 * 60 * 60;

export interface RawTextStore {
  /**
   * Persist a body for the given judgmentId. Returns the storage key
   * that should be saved on the parent judgment row (`rawTextKey`).
   *
   * Idempotent on judgmentId — re-uploads overwrite.
   */
  put(judgmentId: string, body: string): Promise<string>;

  /**
   * Fetch a body by key. Returns null when the key doesn't exist or
   * the fetch fails (caller's caller decides whether to surface UI).
   */
  get(key: string): Promise<string | null>;
}

/**
 * Canonical key shape — exported so callers can mock or pre-compute.
 * Kept as a string namespace ("body/{judgmentId}") so the same field
 * can later hold a different scheme without callers caring.
 */
export function rawTextKeyFor(judgmentId: string): string {
  return `body/${judgmentId}`;
}

// ---------------------------------------------------------------------------
// DDB-backed impl
// ---------------------------------------------------------------------------

export interface DdbRawTextStoreOptions {
  /** Override for tests. Defaults to the shared docClient. */
  docClient?: DynamoDBDocumentClient;
  /** Override retention. Defaults to 90 days, matching pipeline-judgments. */
  ttlDays?: number;
  /** Override wall-clock for tests. Returns ms-since-epoch. */
  now?: () => number;
}

/** Persistence row shape — judgmentId-keyed, body + ttl. */
interface RawTextRow {
  judgmentId: string;
  body: string;
  /** Unix-seconds. DDB native TTL drops rows older than this. */
  ttlEpoch: number;
}

export class DdbRawTextStore implements RawTextStore {
  private readonly docClient: DynamoDBDocumentClient;
  private readonly ttlSeconds: number;
  private readonly now: () => number;

  constructor(opts: DdbRawTextStoreOptions = {}) {
    this.docClient = opts.docClient ?? defaultDocClient;
    this.ttlSeconds = (opts.ttlDays ?? DEFAULT_TTL_DAYS) * SECONDS_PER_DAY;
    this.now = opts.now ?? (() => Date.now());
  }

  async put(judgmentId: string, body: string): Promise<string> {
    const ttlEpoch = Math.floor(this.now() / 1000) + this.ttlSeconds;
    const item: RawTextRow = { judgmentId, body, ttlEpoch };
    await this.docClient.send(
      new PutCommand({
        TableName: PIPELINE_JUDGMENT_RAWTEXT_TABLE,
        Item: item,
      }),
    );
    return rawTextKeyFor(judgmentId);
  }

  async get(key: string): Promise<string | null> {
    // Key is `body/${judgmentId}` — strip the namespace prefix.
    const judgmentId = key.startsWith('body/') ? key.slice('body/'.length) : key;
    try {
      const result = await this.docClient.send(
        new GetCommand({
          TableName: PIPELINE_JUDGMENT_RAWTEXT_TABLE,
          Key: { judgmentId },
        }),
      );
      const item = result.Item as RawTextRow | undefined;
      return item?.body ?? null;
    } catch (err) {
      // Soft-fail to null so the HTTP route surfaces a clean 404 instead
      // of a 500, but log so operators can distinguish "missing row" from
      // "table doesn't exist", "permission denied", "transient timeout".
      // eslint-disable-next-line no-console
      console.warn(
        '[DdbRawTextStore] get failed; returning null',
        { judgmentId, table: PIPELINE_JUDGMENT_RAWTEXT_TABLE, err: err instanceof Error ? err.message : String(err) },
      );
      return null;
    }
  }
}

// ---------------------------------------------------------------------------
// In-memory impl (tests + envs without DDB)
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
