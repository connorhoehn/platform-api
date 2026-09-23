// platform-api/src/repositories/DocumentWorkRepository.ts
//
// Table `document-work` (PK `documentId`, on-demand; GSI `scope-index` on
// `scopeId`): one row per document that someone has planned — status, points,
// owner, priority, rank, outcome, acceptance criteria and links. The meaning
// of a PATCH lives in documents/workBackfill.ts; this file only stores rows.
//
// Its own table, not the lifecycle row: lifecycle transitions are CAS on
// `status` with their own history, and work edits must neither bump that
// version nor contend with it (PLAN §2.1). The table and its GSI are created
// together when missing — a GSI is never added to a populated table (the
// DynamoDB Local GSI trap, NFR #57).
//
// Writes are conditional on `revision` (attribute_not_exists at 0), reads are
// strongly consistent: two platform replicas need no leader and no cache.

import {
  BatchGetCommand,
  GetCommand,
  PutCommand,
  QueryCommand,
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
import type { DocumentWork } from '../documents/workBackfill';

export const DOCUMENT_WORK_TABLE = tableName('document-work');
export const DOCUMENT_WORK_SCOPE_INDEX = 'scope-index';
const BATCH_GET_LIMIT = 100;

export class WorkRevisionConflict extends Error {
  constructor(public readonly documentId: string) {
    super(`work row for ${documentId} changed since it was read`);
    this.name = 'WorkRevisionConflict';
  }
}

function conditionFailed(err: unknown): boolean {
  return (err as { name?: string } | null)?.name === 'ConditionalCheckFailedException';
}

/** Create the table (with its GSI) when it is missing; a no-op otherwise. */
export async function ensureDocumentWorkTable(
  client: DynamoDBClient = ddbClient,
  table: string = DOCUMENT_WORK_TABLE,
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
      AttributeDefinitions: [
        { AttributeName: 'documentId', AttributeType: 'S' },
        { AttributeName: 'scopeId', AttributeType: 'S' },
      ],
      KeySchema: [{ AttributeName: 'documentId', KeyType: 'HASH' }],
      GlobalSecondaryIndexes: [{
        IndexName: DOCUMENT_WORK_SCOPE_INDEX,
        KeySchema: [{ AttributeName: 'scopeId', KeyType: 'HASH' }],
        Projection: { ProjectionType: 'ALL' },
      }],
    }));
  } catch (err) {
    if ((err as { name?: string })?.name !== 'ResourceInUseException') throw err;
  }
  await waitUntilTableExists({ client, maxWaitTime: 60 }, { TableName: table });
}

export class DocumentWorkRepository {
  private ready: Promise<void> | null = null;

  constructor(
    private readonly client: DynamoDBDocumentClient = docClient,
    private readonly table: string = DOCUMENT_WORK_TABLE,
    private readonly ensure: () => Promise<void> = () => ensureDocumentWorkTable(ddbClient, table),
  ) {}

  private tableReady(): Promise<void> {
    if (!this.ready) this.ready = this.ensure().catch((err) => { this.ready = null; throw err; });
    return this.ready;
  }

  async get(documentId: string): Promise<DocumentWork | null> {
    await this.tableReady();
    const out = await this.client.send(new GetCommand({ TableName: this.table, Key: { documentId }, ConsistentRead: true }));
    return (out.Item as DocumentWork | undefined) ?? null;
  }

  /** Every asked id → its row or null. Chunked at BatchGetItem's limit; unprocessed keys retried once. */
  async getMany(documentIds: readonly string[]): Promise<Map<string, DocumentWork | null>> {
    const out = new Map<string, DocumentWork | null>();
    const wanted = [...new Set(documentIds)].filter(Boolean);
    for (const id of wanted) out.set(id, null);
    if (wanted.length === 0) return out;
    await this.tableReady();
    for (let i = 0; i < wanted.length; i += BATCH_GET_LIMIT) {
      let keys = wanted.slice(i, i + BATCH_GET_LIMIT).map((documentId) => ({ documentId }));
      for (let attempt = 0; attempt < 2 && keys.length > 0; attempt++) {
        const res = await this.client.send(new BatchGetCommand({ RequestItems: { [this.table]: { Keys: keys, ConsistentRead: true } } }));
        for (const row of (res.Responses?.[this.table] ?? []) as DocumentWork[]) {
          if (typeof row.documentId === 'string') out.set(row.documentId, row);
        }
        keys = (res.UnprocessedKeys?.[this.table]?.Keys ?? []) as Array<{ documentId: string }>;
      }
    }
    return out;
  }

  /** Rows listed under a scope (the GSI is eventually consistent; the Work column refetches on a signal). */
  async listByScope(scopeId: string, limit = 500): Promise<DocumentWork[]> {
    await this.tableReady();
    const rows: DocumentWork[] = [];
    let start: Record<string, unknown> | undefined;
    do {
      const res = await this.client.send(new QueryCommand({
        TableName: this.table,
        IndexName: DOCUMENT_WORK_SCOPE_INDEX,
        KeyConditionExpression: 'scopeId = :s',
        ExpressionAttributeValues: { ':s': scopeId },
        ...(start ? { ExclusiveStartKey: start } : {}),
      }));
      rows.push(...((res.Items ?? []) as DocumentWork[]));
      start = res.LastEvaluatedKey as Record<string, unknown> | undefined;
    } while (start && rows.length < limit);
    return rows.slice(0, limit);
  }

  /**
   * Write `next` only if the stored row is still at `seenRevision` (0 = no row).
   * Throws WorkRevisionConflict otherwise, so the caller re-reads and re-applies.
   */
  async putIfRevision(next: DocumentWork, seenRevision: number): Promise<void> {
    await this.tableReady();
    try {
      await this.client.send(new PutCommand({
        TableName: this.table,
        Item: next,
        ...(seenRevision === 0
          ? { ConditionExpression: 'attribute_not_exists(documentId)' }
          : { ConditionExpression: 'revision = :seen', ExpressionAttributeValues: { ':seen': seenRevision } }),
      }));
    } catch (err) {
      if (conditionFailed(err)) throw new WorkRevisionConflict(next.documentId);
      throw err;
    }
  }
}

export const documentWorkRepo = new DocumentWorkRepository();
