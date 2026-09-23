// One composer request → one run, across retries and across replicas.
//
// `POST /api/deck/revise-run` claims `(user, document, requestId)` before it
// starts a run: a DynamoDB conditional put of 'pending' (only when no live
// claim exists), then the run id once the engine answers. A retried POST —
// the browser's, a proxy's, or the same click landing on the other platform
// replica — reads the run id back instead of starting a second edit. A retry
// that arrives while the first is still starting waits a few seconds for the id.
//
// Why DynamoDB and not Redis: a Redis claim carried a TTL, so the maxmemory
// policy that evicts TTL'd keys (volatile-*) could drop it, and every Redis
// restart (no persistence locally, NFR #88) dropped all of them — either way
// a retry would start a second run. The claim is the idempotency guarantee,
// so it lives in the durable store: one conditional put + one update per run.
// `expiresAt` is the table's TTL attribute; DynamoDB deletes lazily, so an
// expired row is treated as absent by the condition and by reads.

import {
  CreateTableCommand,
  DescribeTableCommand,
  DescribeTimeToLiveCommand,
  UpdateTimeToLiveCommand,
  waitUntilTableExists,
  type DynamoDBClient,
} from '@aws-sdk/client-dynamodb';
import { DeleteCommand, GetCommand, PutCommand, UpdateCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { ddbClient, docClient } from '../lib/aws-clients';
import { tableName } from '../lib/ddb-table-name';

export const REQUEST_CLAIM_TTL_SECONDS = 24 * 60 * 60;
export const DECK_REVISE_CLAIMS_TABLE = tableName('deck-revise-claims');
const PENDING = 'pending';

/** Where claims live. Semantic operations, so a store cannot get the conditions wrong. */
export interface ClaimStore {
  /** Create the claim only when no live one exists; true when this caller now holds it. */
  create(key: string, value: string, ttlSeconds: number): Promise<boolean>;
  /** The live claim's value, or null. */
  get(key: string): Promise<string | null>;
  /** Replace 'pending' with the run id (only while still pending). */
  commit(key: string, runId: string, ttlSeconds: number): Promise<void>;
  /** Drop the claim only while it is still pending. */
  release(key: string): Promise<void>;
}

/** Per-process claims: tests, and nothing else — one replica's worth of idempotency. */
export class MemoryClaimStore implements ClaimStore {
  private readonly values = new Map<string, { value: string; until: number }>();
  private live(key: string): string | null {
    const entry = this.values.get(key);
    return entry && entry.until > Date.now() ? entry.value : null;
  }
  async create(key: string, value: string, ttlSeconds: number): Promise<boolean> {
    if (this.live(key) !== null) return false;
    this.values.set(key, { value, until: Date.now() + ttlSeconds * 1000 });
    return true;
  }
  async get(key: string): Promise<string | null> { return this.live(key); }
  async commit(key: string, runId: string, ttlSeconds: number): Promise<void> {
    if (this.live(key) === PENDING) this.values.set(key, { value: runId, until: Date.now() + ttlSeconds * 1000 });
  }
  async release(key: string): Promise<void> {
    if (this.live(key) === PENDING) this.values.delete(key);
  }
}

function conditionFailed(error: unknown): boolean {
  return (error as { name?: string })?.name === 'ConditionalCheckFailedException';
}

const tablesReady = new Map<string, Promise<void>>();

/** Idempotent: create a claims table (pk `claimKey`) and its TTL on first use. Default: the deck's. */
export async function ensureDeckReviseClaimsTable(
  client: DynamoDBClient = ddbClient,
  table: string = DECK_REVISE_CLAIMS_TABLE,
): Promise<void> {
  try {
    await client.send(new DescribeTableCommand({ TableName: table }));
  } catch (error) {
    if ((error as { name?: string })?.name !== 'ResourceNotFoundException') throw error;
    try {
      await client.send(new CreateTableCommand({
        TableName: table,
        BillingMode: 'PAY_PER_REQUEST',
        AttributeDefinitions: [{ AttributeName: 'claimKey', AttributeType: 'S' }],
        KeySchema: [{ AttributeName: 'claimKey', KeyType: 'HASH' }],
      }));
    } catch (createError) {
      if ((createError as { name?: string })?.name !== 'ResourceInUseException') throw createError;
    }
    await waitUntilTableExists({ client, maxWaitTime: 60 }, { TableName: table });
  }
  const ttl = await client.send(new DescribeTimeToLiveCommand({ TableName: table }));
  const status = ttl.TimeToLiveDescription?.TimeToLiveStatus;
  if (status !== 'ENABLED' && status !== 'ENABLING') {
    await client.send(new UpdateTimeToLiveCommand({
      TableName: table,
      TimeToLiveSpecification: { Enabled: true, AttributeName: 'expiresAt' },
    }));
  }
}

/** One table-ready promise per table; a failed attempt is retried on the next call. */
function tableReadyOnce(table: string): Promise<void> {
  let ready = tablesReady.get(table);
  if (!ready) {
    ready = ensureDeckReviseClaimsTable(ddbClient, table).catch((error) => { tablesReady.delete(table); throw error; });
    tablesReady.set(table, ready);
  }
  return ready;
}

/**
 * Conditional writes against a claims table (the deck's by default; the
 * idempotency middleware runs one on `idempotency-claims`, NFR #97); reads
 * are strongly consistent.
 */
export class DynamoClaimStore implements ClaimStore {
  private readonly ready: () => Promise<void>;
  constructor(
    private readonly client: DynamoDBDocumentClient = docClient,
    ready?: () => Promise<void>,
    private readonly table: string = DECK_REVISE_CLAIMS_TABLE,
  ) {
    this.ready = ready ?? (() => tableReadyOnce(this.table));
  }

  private now(): number { return Math.floor(Date.now() / 1000); }

  async create(key: string, value: string, ttlSeconds: number): Promise<boolean> {
    await this.ready();
    const now = this.now();
    try {
      await this.client.send(new PutCommand({
        TableName: this.table,
        Item: { claimKey: key, value, expiresAt: now + ttlSeconds },
        ConditionExpression: 'attribute_not_exists(claimKey) OR expiresAt < :now',
        ExpressionAttributeValues: { ':now': now },
      }));
      return true;
    } catch (error) {
      if (conditionFailed(error)) return false;
      throw error;
    }
  }

  async get(key: string): Promise<string | null> {
    await this.ready();
    const out = await this.client.send(new GetCommand({ TableName: this.table, Key: { claimKey: key }, ConsistentRead: true }));
    const item = out.Item as { value?: unknown; expiresAt?: unknown } | undefined;
    if (!item || typeof item.value !== 'string') return null;
    if (typeof item.expiresAt === 'number' && item.expiresAt < this.now()) return null;
    return item.value;
  }

  async commit(key: string, runId: string, ttlSeconds: number): Promise<void> {
    await this.ready();
    try {
      await this.client.send(new UpdateCommand({
        TableName: this.table,
        Key: { claimKey: key },
        UpdateExpression: 'SET #v = :run, expiresAt = :exp',
        ConditionExpression: '#v = :pending',
        ExpressionAttributeNames: { '#v': 'value' },
        ExpressionAttributeValues: { ':run': runId, ':exp': this.now() + ttlSeconds, ':pending': PENDING },
      }));
    } catch (error) {
      // Someone else's run id is already there; theirs stands.
      if (!conditionFailed(error)) throw error;
    }
  }

  async release(key: string): Promise<void> {
    await this.ready();
    try {
      await this.client.send(new DeleteCommand({
        TableName: this.table,
        Key: { claimKey: key },
        ConditionExpression: '#v = :pending',
        ExpressionAttributeNames: { '#v': 'value' },
        ExpressionAttributeValues: { ':pending': PENDING },
      }));
    } catch (error) {
      if (!conditionFailed(error)) throw error;
    }
  }
}

const durable = new DynamoClaimStore();
let override: ClaimStore | null = null;
/** Tests: a store to claim in (null = back to DynamoDB). */
export function setClaimStore(next: ClaimStore | null): void { override = next; }

function store(): ClaimStore { return override ?? durable; }

export function requestClaimKey(userId: string, documentId: string, requestId: string): string {
  return `deck-revise:request:${userId}:${documentId}:${requestId}`;
}

export type RequestClaim =
  | { status: 'claimed'; commit(runId: string): Promise<void>; release(): Promise<void> }
  | { status: 'existing'; runId: string }
  | { status: 'in-flight' };

export async function claimDeckReviseRequest(
  args: { userId: string; documentId: string; requestId: string },
  opts: { waitMs?: number; pollMs?: number } = {},
): Promise<RequestClaim> {
  const key = requestClaimKey(args.userId, args.documentId, args.requestId);
  const claims = store();
  if (await claims.create(key, PENDING, REQUEST_CLAIM_TTL_SECONDS)) {
    return {
      status: 'claimed',
      commit: (runId) => claims.commit(key, runId, REQUEST_CLAIM_TTL_SECONDS),
      // The run never started: let a retry start it.
      release: () => claims.release(key),
    };
  }
  const waitMs = opts.waitMs ?? 5000;
  const pollMs = opts.pollMs ?? 200;
  const until = Date.now() + waitMs;
  for (;;) {
    const value = await claims.get(key);
    if (value && value !== PENDING) return { status: 'existing', runId: value };
    if (value === null) {
      // The first attempt released its claim (its trigger failed): this one may start.
      return claimDeckReviseRequest(args, { waitMs: Math.max(0, until - Date.now()), pollMs });
    }
    if (Date.now() >= until) return { status: 'in-flight' };
    await new Promise((resolve) => setTimeout(resolve, pollMs));
  }
}
