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
import { getRedisClient } from '../lib/redis-client';
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

/**
 * distributed-core `RedisPubSubManager` topic the gateway listens on. The
 * durable event above goes to the message queue, which hands each event to ONE
 * consumer; a gateway replica holding a viewer's stream may not be it. This
 * signal fans out to every replica so each can replay the streams it holds
 * when — and only when — their person's projection moved, instead of polling.
 */
export const PROJECTION_CHANGED_SIGNAL_TOPIC = 'work-graph.projection.changed';
/**
 * The gateway runs its RedisPubSubManager with `channelPrefix: ''` (so its
 * `websocket:*` channels keep their names), so the Redis channel IS the topic.
 */
const PUBSUB_CHANNEL_PREFIX = '';

/**
 * Best-effort. The payload names the person and the org, nothing about the
 * work; a lost signal costs latency (the gateway's safety poll picks it up),
 * never correctness, so a failure is logged and dropped.
 */
export async function publishProjectionChangedSignal(change: Pick<WorkGraphProjectionChanged, 'organizationId' | 'actorId' | 'day' | 'revision'>): Promise<void> {
  const client = await getRedisClient();
  if (!client) return;
  // The wire shape RedisPubSubManager.publish produces, so its subscribers
  // accept it (they drop anything without metadata.messageId).
  const wire = {
    topic: PROJECTION_CHANGED_SIGNAL_TOPIC,
    payload: { organizationId: change.organizationId, actorId: change.actorId, day: change.day, revision: change.revision },
    metadata: {
      messageId: `pub-platform-api-${Date.now()}-${randomUUID()}`,
      publisherNodeId: `platform-api@${hostname()}`,
      timestamp: Date.now(),
      topic: PROJECTION_CHANGED_SIGNAL_TOPIC,
    },
  };
  await client.publish(PUBSUB_CHANNEL_PREFIX + PROJECTION_CHANGED_SIGNAL_TOPIC, JSON.stringify(wire));
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
  /** The fan-out signal to gateway replicas; `null` turns it off. */
  signal?: ((payload: WorkGraphProjectionChanged) => Promise<void>) | null;
  owner?: string;
  leaseMs?: number;
  batchSize?: number;
  now?: () => number;
} = {}) {
  const client = (options.client ?? docClient) as unknown as DrainerClient;
  const publish = options.publish ?? ((eventName, payload) => catalog.publish(eventName, payload));
  const signal = options.signal === undefined ? publishProjectionChangedSignal : options.signal;
  const owner = options.owner ?? OWNER;
  const leaseMs = options.leaseMs ?? DEFAULT_LEASE_MS;
  const batchSize = options.batchSize ?? 25;
  const now = options.now ?? Date.now;
  // Scan's Limit applies before its filter. Even a page with no outbox rows
  // may have a continuation key; starting over would starve later events.
  let scanCursor: Record<string, unknown> | undefined;
  let inFlight: Promise<number> | undefined;

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

  async function drainPage(): Promise<number> {
    const result = await client.send(new ScanCommand({
      TableName: WORK_GRAPH_TABLE,
      FilterExpression: 'itemKind = :outbox',
      ExpressionAttributeValues: { ':outbox': 'outbox' },
      Limit: batchSize,
      ...(scanCursor ? { ExclusiveStartKey: scanCursor } : {}),
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
        if (signal) {
          await signal(payload).catch((error: unknown) => log.warn(
            { errMessage: error instanceof Error ? error.message : String(error), sourceEventId: row.sourceEventId },
            'work-graph projection signal not sent; gateways will catch up on their safety poll',
          ));
        }
      } catch (error) {
        log.error(
          { errMessage: error instanceof Error ? error.message : String(error), sourceEventId: row.sourceEventId },
          'work-graph outbox publish failed; lease will expire for retry',
        );
      }
    }
    // Advance only after processing this bounded page. A scan/claim outage
    // retries the page; failed publications stay leased for the next sweep.
    scanCursor = result.LastEvaluatedKey;
    return published;
  }

  function drainOnce(): Promise<number> {
    // Manual drains and the timer share one cursor and one page of work.
    if (!inFlight) inFlight = drainPage().finally(() => { inFlight = undefined; });
    return inFlight;
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
