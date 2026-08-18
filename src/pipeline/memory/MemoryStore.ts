// platform-api/src/pipeline/memory/MemoryStore.ts
//
// Backing store for the `memory` pipeline node.
//
// Two scopes, and the difference matters for correctness, not just lifetime:
//
//   - 'session'   - keyed by runId. A run's memory is private to that run, so
//                   two concurrent runs of the same pipeline cannot read each
//                   other's scratch values. Short TTL.
//   - 'long-term' - keyed by pipelineId. Survives across runs, which is the
//                   whole point (conversation history, accumulated facts).
//
// There is no `sessionId` anywhere in the pipeline runtime - scoping is runId
// + pipelineId - so 'session' binds to the runId. That is what the editor's
// scope help text describes, and the two must not drift apart.
//
// Shaped as an interface + DDB impl + in-memory impl, matching RawTextStore
// and ImageBlobStore, so tests and local dev never need a table.

import type { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { GetCommand, PutCommand, DeleteCommand } from '@aws-sdk/lib-dynamodb';
import { docClient as defaultDocClient } from '../../lib/aws-clients';
import { tableName } from '../../lib/ddb-table-name';

const PIPELINE_MEMORY_TABLE = tableName('pipeline-memory');

const SECONDS_PER_DAY = 24 * 60 * 60;
/** Session memory is scratch space for one run; a day is generous. */
const DEFAULT_SESSION_TTL_DAYS = 1;
/** Long-term memory outlives runs but is still not permanent by default. */
const DEFAULT_LONG_TERM_TTL_DAYS = 90;

/**
 * Separator between namespace and key in the sort key. Namespaces are
 * validated to exclude it, so `a` + `b:c` and `a:b` + `c` cannot collide.
 */
const KEY_SEPARATOR = '#';

export type MemoryScope = 'session' | 'long-term';

export interface MemoryScopeRef {
  scope: MemoryScope;
  /** Required for 'session'. */
  runId?: string;
  /** Required for 'long-term'. */
  pipelineId?: string;
}

export interface MemoryStore {
  /**
   * Read one record. Returns null when absent - a miss is a normal outcome
   * for a memory read, not an error.
   */
  get(ref: MemoryScopeRef, namespace: string, key: string): Promise<unknown>;

  /** Write one record, overwriting any existing value at the same key. */
  put(
    ref: MemoryScopeRef,
    namespace: string,
    key: string,
    value: unknown,
  ): Promise<void>;

  /** Remove one record. Absent keys are not an error. */
  delete(ref: MemoryScopeRef, namespace: string, key: string): Promise<void>;
}

/**
 * Partition key. The scope owner is part of the key rather than a filter, so a
 * session read physically cannot reach another run's rows.
 *
 * Exported so tests and callers can pre-compute or assert on the key shape.
 */
export function memoryPartitionKey(ref: MemoryScopeRef): string {
  if (ref.scope === 'session') {
    if (!ref.runId) {
      throw new Error("memory: scope 'session' requires a runId");
    }
    return `run/${ref.runId}`;
  }
  if (!ref.pipelineId) {
    throw new Error("memory: scope 'long-term' requires a pipelineId");
  }
  return `pipeline/${ref.pipelineId}`;
}

/**
 * Sort key. The namespace is part of the key so two namespaces cannot collide
 * on the same key name.
 */
export function memorySortKey(namespace: string, key: string): string {
  if (namespace.includes(KEY_SEPARATOR)) {
    throw new Error(
      `memory: namespace must not contain '${KEY_SEPARATOR}' (got '${namespace}')`,
    );
  }
  return `${namespace}${KEY_SEPARATOR}${key}`;
}

interface MemoryRow {
  scopeKey: string;
  itemKey: string;
  value: unknown;
  ttlEpoch: number;
}

// ---------------------------------------------------------------------------
// DDB-backed impl
// ---------------------------------------------------------------------------

export interface DdbMemoryStoreOptions {
  /** Override for tests. Defaults to the shared docClient. */
  docClient?: DynamoDBDocumentClient;
  sessionTtlDays?: number;
  longTermTtlDays?: number;
  now?: () => number;
}

export class DdbMemoryStore implements MemoryStore {
  private readonly docClient: DynamoDBDocumentClient;
  private readonly sessionTtlSeconds: number;
  private readonly longTermTtlSeconds: number;
  private readonly now: () => number;

  constructor(opts: DdbMemoryStoreOptions = {}) {
    this.docClient = opts.docClient ?? defaultDocClient;
    this.sessionTtlSeconds =
      (opts.sessionTtlDays ?? DEFAULT_SESSION_TTL_DAYS) * SECONDS_PER_DAY;
    this.longTermTtlSeconds =
      (opts.longTermTtlDays ?? DEFAULT_LONG_TERM_TTL_DAYS) * SECONDS_PER_DAY;
    this.now = opts.now ?? (() => Date.now());
  }

  async get(
    ref: MemoryScopeRef,
    namespace: string,
    key: string,
  ): Promise<unknown> {
    const result = await this.docClient.send(
      new GetCommand({
        TableName: PIPELINE_MEMORY_TABLE,
        Key: {
          scopeKey: memoryPartitionKey(ref),
          itemKey: memorySortKey(namespace, key),
        },
      }),
    );
    const item = result.Item as MemoryRow | undefined;
    if (!item) return null;

    // DynamoDB TTL deletion is best-effort and can lag by up to ~48h. Honour
    // the expiry on read so a session value cannot outlive its own contract
    // just because the sweeper is behind.
    if (item.ttlEpoch && item.ttlEpoch * 1000 <= this.now()) return null;

    return item.value ?? null;
  }

  async put(
    ref: MemoryScopeRef,
    namespace: string,
    key: string,
    value: unknown,
  ): Promise<void> {
    const ttlSeconds =
      ref.scope === 'session' ? this.sessionTtlSeconds : this.longTermTtlSeconds;
    const item: MemoryRow = {
      scopeKey: memoryPartitionKey(ref),
      itemKey: memorySortKey(namespace, key),
      value,
      ttlEpoch: Math.floor(this.now() / 1000) + ttlSeconds,
    };
    await this.docClient.send(
      new PutCommand({ TableName: PIPELINE_MEMORY_TABLE, Item: item }),
    );
  }

  async delete(
    ref: MemoryScopeRef,
    namespace: string,
    key: string,
  ): Promise<void> {
    await this.docClient.send(
      new DeleteCommand({
        TableName: PIPELINE_MEMORY_TABLE,
        Key: {
          scopeKey: memoryPartitionKey(ref),
          itemKey: memorySortKey(namespace, key),
        },
      }),
    );
  }
}

// ---------------------------------------------------------------------------
// In-memory impl (tests, local dev)
// ---------------------------------------------------------------------------

export class InMemoryMemoryStore implements MemoryStore {
  private readonly rows = new Map<string, unknown>();

  private rowKey(ref: MemoryScopeRef, namespace: string, key: string): string {
    return `${memoryPartitionKey(ref)}${KEY_SEPARATOR}${memorySortKey(namespace, key)}`;
  }

  async get(ref: MemoryScopeRef, namespace: string, key: string) {
    const k = this.rowKey(ref, namespace, key);
    return this.rows.has(k) ? this.rows.get(k) : null;
  }

  async put(ref: MemoryScopeRef, namespace: string, key: string, value: unknown) {
    this.rows.set(this.rowKey(ref, namespace, key), value);
  }

  async delete(ref: MemoryScopeRef, namespace: string, key: string) {
    this.rows.delete(this.rowKey(ref, namespace, key));
  }

  /** Test helper: number of stored records. */
  get size(): number {
    return this.rows.size;
  }
}
