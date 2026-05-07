/**
 * OutboxDrainer — polls the `social-outbox` DynamoDB table for UNPROCESSED rows
 * and publishes them through event-catalog's durable transport.
 *
 * Designed as a single-node poller (no distributed locking). At low volume the
 * full-table Scan with a FilterExpression on `status` is acceptable. If volume
 * grows, add a GSI on status+createdAt and switch to Query.
 */
import { ScanCommand, UpdateCommand } from '@aws-sdk/lib-dynamodb';
import { docClient } from '../lib/aws-clients';
import { publishDurable } from '../lib/event-catalog';
import { tableName } from '../lib/ddb-table-name';
import log from '../lib/logger';

const OUTBOX_TABLE = tableName('social-outbox');

export interface OutboxDrainerConfig {
  pollIntervalMs?: number; // default 2000
  batchSize?: number; // default 25
  maxRetries?: number; // default 3
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

export function startOutboxDrainer(config?: OutboxDrainerConfig): { stop: () => void } {
  const pollIntervalMs = config?.pollIntervalMs ?? 2000;
  const batchSize = config?.batchSize ?? 25;
  const maxRetries = config?.maxRetries ?? 3;

  let timer: ReturnType<typeof setInterval> | null = null;
  let running = false;

  async function poll(): Promise<void> {
    if (running) return; // skip if previous poll is still in-flight
    running = true;

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

      // Process sequentially to avoid overwhelming the message-queue
      for (const row of rows) {
        await processRow(row);
      }
    } catch (err: unknown) {
      const e = err as { message?: string };
      log.error({ errMessage: e?.message }, 'outbox-drainer poll failed');
    } finally {
      running = false;
    }
  }

  async function processRow(row: OutboxRow): Promise<void> {
    try {
      const parsedPayload = JSON.parse(row.payload) as Record<string, unknown>;

      await publishDurable(row.eventType, parsedPayload);

      // Mark as PROCESSED
      await docClient.send(
        new UpdateCommand({
          TableName: OUTBOX_TABLE,
          Key: { outboxId: row.outboxId },
          UpdateExpression: 'SET #status = :processed, processedAt = :now',
          ExpressionAttributeNames: { '#status': 'status' },
          ExpressionAttributeValues: {
            ':processed': 'PROCESSED',
            ':now': new Date().toISOString(),
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

      if (newRetryCount >= maxRetries) {
        // Mark as FAILED — stop retrying
        await docClient.send(
          new UpdateCommand({
            TableName: OUTBOX_TABLE,
            Key: { outboxId: row.outboxId },
            UpdateExpression: 'SET #status = :failed, retryCount = :retryCount',
            ExpressionAttributeNames: { '#status': 'status' },
            ExpressionAttributeValues: {
              ':failed': 'FAILED',
              ':retryCount': newRetryCount,
            },
          }),
        );
        log.warn(
          { eventType: row.eventType, outboxId: row.outboxId, retryCount: newRetryCount },
          'outbox-drainer row marked FAILED after max retries',
        );
      } else {
        // Increment retry count
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

  // Start polling
  timer = setInterval(() => {
    void poll();
  }, pollIntervalMs);

  // Run immediately on start
  void poll();

  return {
    stop: () => {
      if (timer) {
        clearInterval(timer);
        timer = null;
      }
    },
  };
}
