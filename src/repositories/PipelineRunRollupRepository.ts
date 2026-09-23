// platform-api/src/repositories/PipelineRunRollupRepository.ts
//
// One row per pipeline in `pipeline-run-rollups` (PK `pipelineId`, on-demand),
// the derived read model behind the pipelines page's status pill. It is its
// own table on purpose:
//   - the definition row carries an optimistic lock and belongs to one user,
//     while runs of a system pipeline are triggered by everyone — a run
//     finishing must not bump a definition's version or fight an editor's save;
//   - no GSI is added to the populated `pipeline-runs` table (DynamoDB Local
//     cannot backfill one — see the NFR doc's GSI trap).
//
// Writes are compare-and-set on `updatedAt`: the caller merges the event into
// the row it READ and puts the result only if the row is still that version.
// Two replicas absorbing events for one pipeline at the same moment cannot
// lose each other's merge — one wins, the other re-reads and merges again.
// Reads are strongly consistent for the same reason.

import {
  BatchGetCommand,
  GetCommand,
  PutCommand,
  type DynamoDBDocumentClient,
} from '@aws-sdk/lib-dynamodb';
import {
  CreateTableCommand,
  DescribeTableCommand,
  waitUntilTableExists,
  type DynamoDBClient,
} from '@aws-sdk/client-dynamodb';
import { ddbClient, docClient } from '../lib/aws-clients';
import { tableName } from '../lib/ddb-table-name';
import type { PipelineRunRollup } from '../pipeline/run-rollup';

export const PIPELINE_RUN_ROLLUPS_TABLE = tableName('pipeline-run-rollups');

/** BatchGetItem's cap on keys per call. */
const BATCH_GET_LIMIT = 100;

export class RollupVersionConflict extends Error {
  constructor(public readonly pipelineId: string) {
    super(`rollup for ${pipelineId} changed since it was read`);
    this.name = 'RollupVersionConflict';
  }
}

function conditionFailed(err: unknown): boolean {
  return (err as { name?: string } | null)?.name === 'ConditionalCheckFailedException';
}

/** Create the table when it is missing (local dev, a fresh account); a no-op otherwise. */
export async function ensurePipelineRunRollupsTable(
  client: DynamoDBClient = ddbClient,
  table: string = PIPELINE_RUN_ROLLUPS_TABLE,
): Promise<void> {
  try {
    await client.send(new DescribeTableCommand({ TableName: table }));
    return;
  } catch (err) {
    if ((err as { name?: string })?.name !== 'ResourceNotFoundException') throw err;
  }
  try {
    await client.send(new CreateTableCommand({
      TableName: table,
      BillingMode: 'PAY_PER_REQUEST',
      AttributeDefinitions: [{ AttributeName: 'pipelineId', AttributeType: 'S' }],
      KeySchema: [{ AttributeName: 'pipelineId', KeyType: 'HASH' }],
    }));
  } catch (err) {
    if ((err as { name?: string })?.name !== 'ResourceInUseException') throw err;
  }
  await waitUntilTableExists({ client, maxWaitTime: 60 }, { TableName: table });
}

export class PipelineRunRollupRepository {
  private ready: Promise<void> | null = null;

  constructor(
    private readonly client: DynamoDBDocumentClient = docClient,
    private readonly table: string = PIPELINE_RUN_ROLLUPS_TABLE,
    private readonly ensure: () => Promise<void> = () => ensurePipelineRunRollupsTable(ddbClient, table),
  ) {}

  /** One table-ready promise; a failed attempt is retried on the next call. */
  private tableReady(): Promise<void> {
    if (!this.ready) {
      this.ready = this.ensure().catch((err) => { this.ready = null; throw err; });
    }
    return this.ready;
  }

  async get(pipelineId: string): Promise<PipelineRunRollup | null> {
    await this.tableReady();
    const out = await this.client.send(new GetCommand({
      TableName: this.table,
      Key: { pipelineId },
      ConsistentRead: true,
    }));
    return (out.Item as PipelineRunRollup | undefined) ?? null;
  }

  /** Every asked id → its row or null. Chunked at BatchGetItem's limit; unprocessed keys are retried once. */
  async getMany(pipelineIds: readonly string[]): Promise<Map<string, PipelineRunRollup | null>> {
    const out = new Map<string, PipelineRunRollup | null>();
    const wanted = [...new Set(pipelineIds)].filter(Boolean);
    for (const id of wanted) out.set(id, null);
    if (wanted.length === 0) return out;
    await this.tableReady();
    for (let i = 0; i < wanted.length; i += BATCH_GET_LIMIT) {
      let keys = wanted.slice(i, i + BATCH_GET_LIMIT).map((pipelineId) => ({ pipelineId }));
      for (let attempt = 0; attempt < 2 && keys.length > 0; attempt++) {
        const res = await this.client.send(new BatchGetCommand({
          RequestItems: { [this.table]: { Keys: keys } },
        }));
        for (const row of (res.Responses?.[this.table] ?? []) as PipelineRunRollup[]) {
          if (typeof row.pipelineId === 'string') out.set(row.pipelineId, row);
        }
        keys = (res.UnprocessedKeys?.[this.table]?.Keys ?? []) as Array<{ pipelineId: string }>;
      }
    }
    return out;
  }

  /**
   * Write `next` only if the row is still the version `seen` (null = no row
   * yet). Throws RollupVersionConflict when it is not, so the caller can
   * re-read and merge again.
   */
  async putIfUnchanged(next: PipelineRunRollup, seen: PipelineRunRollup | null): Promise<void> {
    await this.tableReady();
    try {
      await this.client.send(new PutCommand({
        TableName: this.table,
        Item: next,
        ...(seen
          ? {
              ConditionExpression: 'updatedAt = :seen',
              ExpressionAttributeValues: { ':seen': seen.updatedAt },
            }
          : { ConditionExpression: 'attribute_not_exists(pipelineId)' }),
      }));
    } catch (err) {
      if (conditionFailed(err)) throw new RollupVersionConflict(next.pipelineId);
      throw err;
    }
  }

  /**
   * Write `next` unless a row with a newer `updatedAt` is already there — the
   * backfill's put: a row the subscriber has since written wins. Returns
   * whether the write landed.
   */
  async putIfNotNewer(next: PipelineRunRollup): Promise<boolean> {
    await this.tableReady();
    try {
      await this.client.send(new PutCommand({
        TableName: this.table,
        Item: next,
        ConditionExpression: 'attribute_not_exists(pipelineId) OR updatedAt < :at',
        ExpressionAttributeValues: { ':at': next.updatedAt },
      }));
      return true;
    } catch (err) {
      if (conditionFailed(err)) return false;
      throw err;
    }
  }
}

/** Singleton on the shared document client. */
export const pipelineRunRollupRepo = new PipelineRunRollupRepository();
