import {
  DeleteCommand,
  DynamoDBDocumentClient,
  ScanCommand,
  UpdateCommand,
} from '@aws-sdk/lib-dynamodb';
import { randomUUID } from 'crypto';
import { hostname } from 'os';
import { docClient } from '../lib/aws-clients';
import { catalog } from '../lib/event-catalog';
import { WORK_GRAPH_TABLE } from '../lib/work-graph-dynamo';
import log from '../lib/logger';

const OWNER = `${process.pid}@${hostname()}-${randomUUID()}`;
const DEFAULT_LEASE_MS = 30_000;

export interface WorkGraphProjectionChanged {
  organizationId: string;
  actorId: string;
  day: string;
  revision: number;
  sourceEventId: string;
}

interface OutboxRow extends WorkGraphProjectionChanged {
  PK: string;
  SK: string;
  itemKind: 'outbox';
  eventType: 'work-graph.projection.changed.v1';
  createdAt: string;
}

interface DrainerClient {
  send(command: ScanCommand | UpdateCommand | DeleteCommand): Promise<{ Items?: Record<string, unknown>[]; LastEvaluatedKey?: Record<string, unknown> }>;
}

function isConditionalFailure(error: unknown): boolean {
  return (error as { name?: string })?.name === 'ConditionalCheckFailedException';
}

function parseRow(value: unknown): OutboxRow | null {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return null;
  const row = value as Record<string, unknown>;
  if (typeof row.PK !== 'string' || typeof row.SK !== 'string'
    || !row.SK.startsWith('OUTBOX#') || row.itemKind !== 'outbox'
    || row.eventType !== 'work-graph.projection.changed.v1'
    || typeof row.organizationId !== 'string' || typeof row.actorId !== 'string'
    || typeof row.day !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(row.day)
    || !Number.isSafeInteger(row.revision) || Number(row.revision) < 1
    || typeof row.sourceEventId !== 'string' || typeof row.createdAt !== 'string') return null;
  return row as unknown as OutboxRow;
}

export function createWorkGraphOutboxDrainer(options: {
  client?: DynamoDBDocumentClient;
  publish?: (eventName: string, payload: WorkGraphProjectionChanged) => Promise<void>;
  owner?: string;
  leaseMs?: number;
  batchSize?: number;
  now?: () => number;
} = {}) {
  const client = (options.client ?? docClient) as unknown as DrainerClient;
  const publish = options.publish ?? ((eventName, payload) => catalog.publish(eventName, payload));
  const owner = options.owner ?? OWNER;
  const leaseMs = options.leaseMs ?? DEFAULT_LEASE_MS;
  const batchSize = options.batchSize ?? 25;
  const now = options.now ?? Date.now;

  async function claim(row: OutboxRow): Promise<boolean> {
    try {
      await client.send(new UpdateCommand({
        TableName: WORK_GRAPH_TABLE,
        Key: { PK: row.PK, SK: row.SK },
        UpdateExpression: 'SET outboxLeaseOwner = :owner, outboxLeaseUntil = :until',
        ConditionExpression: 'itemKind = :outbox AND (attribute_not_exists(outboxLeaseUntil) OR outboxLeaseUntil < :now)',
        ExpressionAttributeValues: {
          ':owner': owner,
          ':until': now() + leaseMs,
          ':now': now(),
          ':outbox': 'outbox',
        },
      }));
      return true;
    } catch (error) {
      if (isConditionalFailure(error)) return false;
      throw error;
    }
  }

  async function drainOnce(): Promise<number> {
    const result = await client.send(new ScanCommand({
      TableName: WORK_GRAPH_TABLE,
      FilterExpression: 'itemKind = :outbox',
      ExpressionAttributeValues: { ':outbox': 'outbox' },
      Limit: batchSize,
    }));
    let published = 0;
    for (const value of result.Items ?? []) {
      const row = parseRow(value);
      if (!row || !(await claim(row))) continue;
      const payload: WorkGraphProjectionChanged = {
        organizationId: row.organizationId,
        actorId: row.actorId,
        day: row.day,
        revision: row.revision,
        sourceEventId: row.sourceEventId,
      };
      try {
        await publish(row.eventType, payload);
        await client.send(new DeleteCommand({
          TableName: WORK_GRAPH_TABLE,
          Key: { PK: row.PK, SK: row.SK },
          ConditionExpression: 'itemKind = :outbox AND outboxLeaseOwner = :owner',
          ExpressionAttributeValues: { ':outbox': 'outbox', ':owner': owner },
        }));
        published += 1;
      } catch (error) {
        log.error(
          { errMessage: error instanceof Error ? error.message : String(error), sourceEventId: row.sourceEventId },
          'work-graph outbox publish failed; lease will expire for retry',
        );
      }
    }
    return published;
  }

  return { drainOnce };
}

export function startWorkGraphOutboxDrainer(options: Parameters<typeof createWorkGraphOutboxDrainer>[0] & {
  pollIntervalMs?: number;
} = {}): { stop(): void; drainOnce(): Promise<number> } {
  const drainer = createWorkGraphOutboxDrainer(options);
  let running = false;
  const timer = setInterval(() => {
    if (running) return;
    running = true;
    void drainer.drainOnce()
      .catch((error) => log.error(
        { errMessage: error instanceof Error ? error.message : String(error) },
        'work-graph outbox scan failed',
      ))
      .finally(() => { running = false; });
  }, options.pollIntervalMs ?? 2_000);
  timer.unref?.();
  return { stop: () => clearInterval(timer), drainOnce: drainer.drainOnce };
}
