import { ScanCommand, UpdateCommand, PutCommand, DeleteCommand } from '@aws-sdk/lib-dynamodb';
import { randomUUID } from 'crypto';
import { hostname } from 'os';
import { docClient } from '../lib/aws-clients';
import { publishDurable } from '../lib/event-catalog';
import { tableName } from '../lib/ddb-table-name';
import log from '../lib/logger';

const OUTBOX_TABLE = tableName('social-outbox');
const LOCK_KEY = { outboxId: 'OUTBOX_LOCK#drainer' };
const LEASE_TTL_MS = 30_000;
const HEARTBEAT_INTERVAL_MS = 10_000;

/** 24 hours in seconds — shortened TTL for completed/failed rows (kept briefly for debugging). */
const TERMINAL_TTL_SECONDS = 24 * 60 * 60;

const OWNER_ID = `${process.pid}@${hostname()}-${randomUUID()}`;

export interface OutboxDrainerConfig {
  pollIntervalMs?: number;
  batchSize?: number;
  maxRetries?: number;
}

interface OutboxRow {
  outboxId: string;
  status: string;
  eventType: string;
  queueName: string;
  payload: string;
  createdAt: string;
  retryCount?: number;
}

async function acquireLease(): Promise<boolean> {
  const now = Date.now();
  const expiresAt = now + LEASE_TTL_MS;

  try {
    await docClient.send(
      new PutCommand({
        TableName: OUTBOX_TABLE,
        Item: {
          ...LOCK_KEY,
          owner: OWNER_ID,
          expiresAt,
        },
        ConditionExpression: 'attribute_not_exists(outboxId) OR expiresAt < :now',
        ExpressionAttributeValues: { ':now': now },
      }),
    );
    return true;
  } catch (err: unknown) {
    if ((err as { name?: string }).name === 'ConditionalCheckFailedException') {
      return false;
    }
    throw err;
  }
}

async function renewLease(): Promise<void> {
  const expiresAt = Date.now() + LEASE_TTL_MS;
  await docClient.send(
    new UpdateCommand({
      TableName: OUTBOX_TABLE,
      Key: LOCK_KEY,
      UpdateExpression: 'SET expiresAt = :expiresAt',
      ConditionExpression: '#owner = :owner',
      ExpressionAttributeNames: { '#owner': 'owner' },
      ExpressionAttributeValues: {
        ':expiresAt': expiresAt,
        ':owner': OWNER_ID,
      },
    }),
  );
}

async function releaseLease(): Promise<void> {
  try {
    await docClient.send(
      new DeleteCommand({
        TableName: OUTBOX_TABLE,
        Key: LOCK_KEY,
        ConditionExpression: '#owner = :owner',
        ExpressionAttributeNames: { '#owner': 'owner' },
        ExpressionAttributeValues: { ':owner': OWNER_ID },
      }),
    );
  } catch {
    // Best-effort release; lease will expire naturally if this fails
  }
}

export function startOutboxDrainer(config?: OutboxDrainerConfig): { stop: () => void } {
  const pollIntervalMs = config?.pollIntervalMs ?? 2000;
  const batchSize = config?.batchSize ?? 25;
  const maxRetries = config?.maxRetries ?? 3;

  let timer: ReturnType<typeof setInterval> | null = null;
  let heartbeatTimer: ReturnType<typeof setInterval> | null = null;
  let running = false;
  let stopping = false;

  async function poll(): Promise<void> {
    if (running || stopping) return;
    running = true;

    try {
      const acquired = await acquireLease();
      if (!acquired) {
        return;
      }

      heartbeatTimer = setInterval(() => {
        renewLease().catch((err: unknown) => {
          const e = err as { message?: string };
          log.warn({ errMessage: e?.message }, 'outbox-drainer lease renewal failed');
        });
      }, HEARTBEAT_INTERVAL_MS);

      try {
        const result = await docClient.send(
          new ScanCommand({
            TableName: OUTBOX_TABLE,
            FilterExpression: '#status = :unprocessed',
            ExpressionAttributeNames: { '#status': 'status' },
            ExpressionAttributeValues: { ':unprocessed': 'UNPROCESSED' },
            Limit: batchSize,
          }),
        );

        const rows = (result.Items ?? []) as OutboxRow[];

        for (const row of rows) {
          await processRow(row, maxRetries);
        }
      } finally {
        if (heartbeatTimer) {
          clearInterval(heartbeatTimer);
          heartbeatTimer = null;
        }
        await releaseLease();
      }
    } catch (err: unknown) {
      const e = err as { message?: string };
      log.error({ errMessage: e?.message }, 'outbox-drainer poll failed');
    } finally {
      running = false;
    }
  }

  async function processRow(row: OutboxRow, maxRetriesCount: number): Promise<void> {
    try {
      const parsedPayload = JSON.parse(row.payload) as Record<string, unknown>;

      await publishDurable(row.eventType, parsedPayload);

      const completedAt = new Date().toISOString();
      const ttl = Math.floor(Date.now() / 1000) + TERMINAL_TTL_SECONDS;

      await docClient.send(
        new UpdateCommand({
          TableName: OUTBOX_TABLE,
          Key: { outboxId: row.outboxId },
          UpdateExpression: 'SET #status = :processed, processedAt = :now, completedAt = :completedAt, #ttl = :ttl',
          ExpressionAttributeNames: { '#status': 'status', '#ttl': 'ttl' },
          ExpressionAttributeValues: {
            ':processed': 'PROCESSED',
            ':now': completedAt,
            ':completedAt': completedAt,
            ':ttl': ttl,
          },
        }),
      );

      log.info({ eventType: row.eventType, outboxId: row.outboxId }, 'outbox-drainer published');
    } catch (err: unknown) {
      const e = err as { message?: string };
      log.error(
        { eventType: row.eventType, outboxId: row.outboxId, errMessage: e?.message },
        'outbox-drainer publish failed',
      );

      const currentRetryCount = row.retryCount ?? 0;
      const newRetryCount = currentRetryCount + 1;

      if (newRetryCount >= maxRetriesCount) {
        const failedAt = new Date().toISOString();
        const ttl = Math.floor(Date.now() / 1000) + TERMINAL_TTL_SECONDS;

        await docClient.send(
          new UpdateCommand({
            TableName: OUTBOX_TABLE,
            Key: { outboxId: row.outboxId },
            UpdateExpression: 'SET #status = :failed, retryCount = :retryCount, failedAt = :failedAt, #ttl = :ttl',
            ExpressionAttributeNames: { '#status': 'status', '#ttl': 'ttl' },
            ExpressionAttributeValues: {
              ':failed': 'FAILED',
              ':retryCount': newRetryCount,
              ':failedAt': failedAt,
              ':ttl': ttl,
            },
          }),
        );
        log.warn(
          { eventType: row.eventType, outboxId: row.outboxId, retryCount: newRetryCount },
          'outbox-drainer row marked FAILED after max retries',
        );
      } else {
        await docClient.send(
          new UpdateCommand({
            TableName: OUTBOX_TABLE,
            Key: { outboxId: row.outboxId },
            UpdateExpression: 'SET retryCount = :retryCount',
            ExpressionAttributeValues: {
              ':retryCount': newRetryCount,
            },
          }),
        );
      }
    }
  }

  timer = setInterval(() => {
    void poll();
  }, pollIntervalMs);

  void poll();

  return {
    stop: () => {
      stopping = true;
      if (timer) {
        clearInterval(timer);
        timer = null;
      }
      if (heartbeatTimer) {
        clearInterval(heartbeatTimer);
        heartbeatTimer = null;
      }
      void releaseLease();
    },
  };
}
