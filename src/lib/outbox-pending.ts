/**
 * The social-outbox's "pending" sparse index — how the drainer finds rows
 * that still need publishing without reading the ones it already published.
 *
 * Every UNPROCESSED row is written with `pendingShard` = one of
 * PENDING_SHARDS fixed values; the drainer's terminal write (PROCESSED or
 * FAILED) REMOVEs it in the same conditional update. `pending-index` is keyed
 * on that attribute (sort key: the ulid `outboxId`, i.e. receipt order), so
 * the index holds only undrained rows and a drainer poll is PENDING_SHARDS
 * Queries whose cost follows the backlog, not the table (processed rows stay
 * about a day under the ttl for debugging).
 *
 * Same shape as the work-graph outbox (`OUTBOX#0..3` partitions, NFR #27):
 * a fixed handful of shards queried every tick, so no one partition key takes
 * every write, and adding the index to an existing table is an UpdateTable,
 * never a recreate.
 */
import {
  DescribeTableCommand,
  UpdateTableCommand,
  type DynamoDBClient,
} from '@aws-sdk/client-dynamodb';

export const PENDING_INDEX = 'pending-index';
export const PENDING_ATTR = 'pendingShard';
export const PENDING_SHARDS = ['P0', 'P1', 'P2', 'P3'] as const;

/** A stable shard for a row: the last character of its ulid (random bits). */
export function pendingShardFor(outboxId: string): string {
  const code = outboxId.length > 0 ? outboxId.charCodeAt(outboxId.length - 1) : 0;
  return PENDING_SHARDS[code % PENDING_SHARDS.length];
}

/** The attribute every UNPROCESSED outbox row carries; spread into the Item. */
export function pendingFields(outboxId: string): Record<string, string> {
  return { [PENDING_ATTR]: pendingShardFor(outboxId) };
}

/** True for the errors DynamoDB gives while the index is missing or still backfilling. */
export function isPendingIndexUnavailable(err: unknown): boolean {
  const e = err as { name?: string; message?: string };
  if (e?.name === 'ResourceNotFoundException') return true;
  return e?.name === 'ValidationException' && /index/i.test(e?.message ?? '');
}

/** Whether `pending-index` exists and is ACTIVE (backfill finished). */
export async function pendingIndexActive(client: DynamoDBClient, table: string): Promise<boolean> {
  try {
    const desc = await client.send(new DescribeTableCommand({ TableName: table }));
    const index = (desc.Table?.GlobalSecondaryIndexes ?? []).find((g) => g.IndexName === PENDING_INDEX);
    return index?.IndexStatus === 'ACTIVE';
  } catch {
    return false;
  }
}

/**
 * Add `pending-index` to an existing outbox table if it is not there. One
 * UpdateTable (additive; DynamoDB backfills it online). Never drops or
 * recreates anything; an existing index — ACTIVE or still CREATING — is left
 * alone, and until it is ACTIVE the drainer keeps using its table walk.
 */
export async function ensureOutboxPendingIndex(client: DynamoDBClient, table: string): Promise<'present' | 'created'> {
  const desc = await client.send(new DescribeTableCommand({ TableName: table }));
  const has = (desc.Table?.GlobalSecondaryIndexes ?? []).some((g) => g.IndexName === PENDING_INDEX);
  if (has) return 'present';
  await client.send(new UpdateTableCommand({
    TableName: table,
    AttributeDefinitions: [
      { AttributeName: PENDING_ATTR, AttributeType: 'S' },
      { AttributeName: 'outboxId', AttributeType: 'S' },
    ],
    GlobalSecondaryIndexUpdates: [{
      Create: {
        IndexName: PENDING_INDEX,
        KeySchema: [
          { AttributeName: PENDING_ATTR, KeyType: 'HASH' },
          { AttributeName: 'outboxId', KeyType: 'RANGE' },
        ],
        Projection: { ProjectionType: 'ALL' },
      },
    }],
  }));
  return 'created';
}
