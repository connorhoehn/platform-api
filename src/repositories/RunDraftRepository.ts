// platform-api/src/repositories/RunDraftRepository.ts
//
// Table `document-run-drafts` (PK `documentId`, SK `draftId`, on-demand; GSI
// `runId-index` on `runId`): a run someone prepared on a document before
// starting it. `draftId` is the client's requestId, so every write is keyed by
// something the client can replay.
//
// Status machine: draft → dispatching → dispatched (or draft → cancelled).
// Every transition is a conditional write on the current status (and on
// `revision` for content edits); two platform replicas that race on one draft
// cannot both move it. The run itself is started under the fixed
// Idempotency-Key `run-draft:<draftId>` (routes/runDrafts.ts), so a replica
// that finds a draft stuck at `dispatching` — its first dispatcher crashed —
// can repeat the trigger and get the same runId back.
//
// Created with its GSI when missing; a GSI is never added to a populated table
// (NFR #57).

import {
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

export const RUN_DRAFTS_TABLE = tableName('document-run-drafts');
export const RUN_DRAFTS_RUN_INDEX = 'runId-index';

export const RUN_DRAFT_STATUSES = ['draft', 'dispatching', 'dispatched', 'cancelled'] as const;
export type RunDraftStatus = (typeof RUN_DRAFT_STATUSES)[number];
export const RUN_DRAFT_MODELS = ['haiku', 'sonnet', 'opus'] as const;
export type RunDraftModel = (typeof RUN_DRAFT_MODELS)[number];

export interface RunDraftSource { kind: 'document' | 'transcript' | 'recording'; id: string }

export interface RunDraft {
  documentId: string;
  /** The client's requestId (≤ 64, [A-Za-z0-9_-]). */
  draftId: string;
  organizationId?: string;
  createdBy: string;
  pipelineId: string;
  instruction: string;
  model?: RunDraftModel;
  contextBudgetTokens?: number;
  hints?: string[];
  sources?: RunDraftSource[];
  status: RunDraftStatus;
  runId?: string;
  /** Who took draft → dispatching; the run is started as this person, on every retry. */
  dispatchedBy?: string;
  /** When draft → dispatching was taken (the "Dispatch not confirmed" clock). */
  dispatchStartedAt?: string;
  dispatchedAt?: string;
  /** The last dispatch refusal (trigger 4xx/5xx), kept on the draft it returned to. */
  dispatchError?: string;
  revision: number;
  createdAt: string;
  updatedAt: string;
  updatedBy?: string;
}

export class RunDraftConflict extends Error {
  constructor(public readonly documentId: string, public readonly draftId: string) {
    super(`run draft ${documentId}/${draftId} changed since it was read`);
    this.name = 'RunDraftConflict';
  }
}

function conditionFailed(err: unknown): boolean {
  return (err as { name?: string } | null)?.name === 'ConditionalCheckFailedException';
}

export async function ensureRunDraftsTable(
  client: DynamoDBClient = ddbClient,
  table: string = RUN_DRAFTS_TABLE,
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
        { AttributeName: 'draftId', AttributeType: 'S' },
        { AttributeName: 'runId', AttributeType: 'S' },
      ],
      KeySchema: [
        { AttributeName: 'documentId', KeyType: 'HASH' },
        { AttributeName: 'draftId', KeyType: 'RANGE' },
      ],
      GlobalSecondaryIndexes: [{
        IndexName: RUN_DRAFTS_RUN_INDEX,
        KeySchema: [{ AttributeName: 'runId', KeyType: 'HASH' }],
        Projection: { ProjectionType: 'ALL' },
      }],
    }));
  } catch (err) {
    if ((err as { name?: string })?.name !== 'ResourceInUseException') throw err;
  }
  await waitUntilTableExists({ client, maxWaitTime: 60 }, { TableName: table });
}

export class RunDraftRepository {
  private ready: Promise<void> | null = null;

  constructor(
    private readonly client: DynamoDBDocumentClient = docClient,
    private readonly table: string = RUN_DRAFTS_TABLE,
    private readonly ensure: () => Promise<void> = () => ensureRunDraftsTable(ddbClient, table),
  ) {}

  private tableReady(): Promise<void> {
    if (!this.ready) this.ready = this.ensure().catch((err) => { this.ready = null; throw err; });
    return this.ready;
  }

  async get(documentId: string, draftId: string): Promise<RunDraft | null> {
    await this.tableReady();
    const out = await this.client.send(new GetCommand({ TableName: this.table, Key: { documentId, draftId }, ConsistentRead: true }));
    return (out.Item as RunDraft | undefined) ?? null;
  }

  /** Every draft on one document (strongly consistent; a document holds few). */
  async listForDocument(documentId: string, limit = 100): Promise<RunDraft[]> {
    await this.tableReady();
    const res = await this.client.send(new QueryCommand({
      TableName: this.table,
      KeyConditionExpression: 'documentId = :d',
      ExpressionAttributeValues: { ':d': documentId },
      ConsistentRead: true,
      Limit: limit,
    }));
    return (res.Items ?? []) as RunDraft[];
  }

  /** The draft that started `runId`, if any (GSI; eventually consistent). */
  async getByRunId(runId: string): Promise<RunDraft | null> {
    await this.tableReady();
    const res = await this.client.send(new QueryCommand({
      TableName: this.table,
      IndexName: RUN_DRAFTS_RUN_INDEX,
      KeyConditionExpression: 'runId = :r',
      ExpressionAttributeValues: { ':r': runId },
      Limit: 1,
    }));
    return ((res.Items ?? [])[0] as RunDraft | undefined) ?? null;
  }

  /** Create only when absent. False when a draft with this id already exists. */
  async create(draft: RunDraft): Promise<boolean> {
    await this.tableReady();
    try {
      await this.client.send(new PutCommand({ TableName: this.table, Item: draft, ConditionExpression: 'attribute_not_exists(draftId)' }));
      return true;
    } catch (err) {
      if (conditionFailed(err)) return false;
      throw err;
    }
  }

  /**
   * Replace the draft only if it is still at `seen.revision` AND in one of
   * `fromStatuses`. Throws RunDraftConflict when it is not.
   */
  async replaceIf(next: RunDraft, seen: { revision: number; fromStatuses: readonly RunDraftStatus[] }): Promise<void> {
    await this.tableReady();
    const statusValues = Object.fromEntries(seen.fromStatuses.map((s, i) => [`:s${i}`, s]));
    try {
      await this.client.send(new PutCommand({
        TableName: this.table,
        Item: next,
        ConditionExpression: `revision = :rev AND #status IN (${Object.keys(statusValues).join(', ')})`,
        ExpressionAttributeNames: { '#status': 'status' },
        ExpressionAttributeValues: { ':rev': seen.revision, ...statusValues },
      }));
    } catch (err) {
      if (conditionFailed(err)) throw new RunDraftConflict(next.documentId, next.draftId);
      throw err;
    }
  }
}

export const runDraftRepo = new RunDraftRepository();
