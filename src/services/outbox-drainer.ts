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

// In-memory mirror of the lease deadline written to DDB. Updated on every
// successful acquire/renew, cleared on renewal failure. processRow gates on
// `Date.now() < leaseExpiresAt` to refuse publishing once the lease has
// lapsed — preventing a stale drainer from clobbering rows another replica
// is now processing.
let leaseExpiresAt = 0;

function leaseValid(): boolean {
  return Date.now() < leaseExpiresAt;
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
    leaseExpiresAt = expiresAt;
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
  leaseExpiresAt = expiresAt;
}

async function releaseLease(): Promise<void> {
  leaseExpiresAt = 0;
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
          // Renewal failed — drop in-memory lease so subsequent processRow
          // calls abort cleanly rather than racing against whoever steals
          // the lease next. The current batch may already have rows in flight
          // (see leaseValid() gate below), so this is best-effort fencing.
          leaseExpiresAt = 0;
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
          if (!leaseValid()) {
            log.warn(
              { remainingRows: rows.length - rows.indexOf(row) },
              'outbox-drainer aborting batch — lease lapsed mid-scan',
            );
            break;
          }
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
    if (!leaseValid()) {
      log.warn(
        { outboxId: row.outboxId },
        'outbox-drainer skipping row — lease no longer valid',
      );
      return;
    }
    try {
      const parsedPayload = JSON.parse(row.payload) as Record<string, unknown>;

      await publishDurable(row.eventType, parsedPayload);

      // Re-check ownership after the network call: publishDurable could have
      // taken seconds, and the lease may have lapsed while we waited.
      // Refusing the status write here is safe — UNPROCESSED rows stay
      // claimable; the worst case is a duplicate publish next round, which
      // downstream consumers must handle as part of at-least-once semantics.
      if (!leaseValid()) {
        log.warn(
          { outboxId: row.outboxId },
          'outbox-drainer skipping status update — lease lapsed during publish',
        );
        return;
      }

      const completedAt = new Date().toISOString();
      const ttl = Math.floor(Date.now() / 1000) + TERMINAL_TTL_SECONDS;

      await docClient.send(
        new UpdateCommand({
          TableName: OUTBOX_TABLE,
          Key: { outboxId: row.outboxId },
          UpdateExpression: 'SET #status = :processed, processedAt = :now, completedAt = :completedAt, #ttl = :ttl',
          ConditionExpression: 'attribute_exists(outboxId) AND #status = :unprocessed',
          ExpressionAttributeNames: { '#status': 'status', '#ttl': 'ttl' },
          ExpressionAttributeValues: {
            ':processed': 'PROCESSED',
            ':unprocessed': 'UNPROCESSED',
            ':now': completedAt,
            ':completedAt': completedAt,
            ':ttl': ttl,
          },
        }),
      );

      log.info({ eventType: row.eventType, outboxId: row.outboxId }, 'outbox-drainer published');
    } catch (err: unknown) {
      const e = err as { message?: string; name?: string };
      // If another drainer already marked this row PROCESSED/FAILED, the
      // ConditionExpression on our status write will reject — swallow that
      // specific case quietly; it's the desired outcome under contention.
      if (e?.name === 'ConditionalCheckFailedException') {
        log.info(
          { outboxId: row.outboxId },
          'outbox-drainer status write rejected — row already terminal',
        );
        return;
      }
      log.error(
        { eventType: row.eventType, outboxId: row.outboxId, errMessage: e?.message },
        'outbox-drainer publish failed',
      );

      if (!leaseValid()) {
        log.warn(
          { outboxId: row.outboxId },
          'outbox-drainer skipping retry-count update — lease lapsed',
        );
        return;
      }

      const currentRetryCount = row.retryCount ?? 0;
      const newRetryCount = currentRetryCount + 1;

      if (newRetryCount >= maxRetriesCount) {
        const failedAt = new Date().toISOString();
        const ttl = Math.floor(Date.now() / 1000) + TERMINAL_TTL_SECONDS;

        try {
          await docClient.send(
            new UpdateCommand({
              TableName: OUTBOX_TABLE,
              Key: { outboxId: row.outboxId },
              UpdateExpression: 'SET #status = :failed, retryCount = :retryCount, failedAt = :failedAt, #ttl = :ttl',
              ConditionExpression: 'attribute_exists(outboxId) AND #status = :unprocessed',
              ExpressionAttributeNames: { '#status': 'status', '#ttl': 'ttl' },
              ExpressionAttributeValues: {
                ':failed': 'FAILED',
                ':unprocessed': 'UNPROCESSED',
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
        } catch (writeErr: unknown) {
          const w = writeErr as { name?: string };
          if (w?.name !== 'ConditionalCheckFailedException') throw writeErr;
        }
      } else {
        try {
          await docClient.send(
            new UpdateCommand({
              TableName: OUTBOX_TABLE,
              Key: { outboxId: row.outboxId },
              UpdateExpression: 'SET retryCount = :retryCount',
              ConditionExpression: 'attribute_exists(outboxId) AND #status = :unprocessed',
              ExpressionAttributeNames: { '#status': 'status' },
              ExpressionAttributeValues: {
                ':retryCount': newRetryCount,
                ':unprocessed': 'UNPROCESSED',
              },
            }),
          );
        } catch (writeErr: unknown) {
          const w = writeErr as { name?: string };
          if (w?.name !== 'ConditionalCheckFailedException') throw writeErr;
        }
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
