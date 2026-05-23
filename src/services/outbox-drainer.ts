import { ScanCommand, UpdateCommand } from '@aws-sdk/lib-dynamodb';
import { randomUUID } from 'crypto';
import { hostname } from 'os';
import { LeasedKVLock } from 'distributed-core';
import { docClient } from '../lib/aws-clients';
import { catalog } from '../lib/event-catalog';
import { tableName } from '../lib/ddb-table-name';
import { createDDBLeaseStore } from '../lib/ddb-lease-store';
import log from '../lib/logger';

const OUTBOX_TABLE = tableName('social-outbox');
const LOCK_KEY = 'OUTBOX_LOCK#drainer';
const LEASE_TTL_MS = 30_000;

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

// M4.3a-followup completion: the inline acquire/renew/release lease loop +
// in-memory `leaseExpiresAt` mirror were replaced with DC's LeasedKVLock,
// backed by the shared createDDBLeaseStore adapter (same DDB row format,
// same conditional-write semantics — the row keyed at OUTBOX_LOCK#drainer
// is byte-compatible with the previous Phase 54.6.5 layout).
//
// The Phase 54.6.5 hardening guarantees are preserved at TWO independent
// fences:
//   - Lock-level fence: `lock.isLeader()` gates publish + status writes;
//     mirrors the prior `leaseValid()` helper. LeasedKVLock's heartbeat
//     handler clears its own in-memory `leaseExpiresAt` on renewal
//     failure, so `isLeader()` flips false the same way the inline impl
//     did, just without us owning the timer.
//   - Row-level fence: per-row UpdateCommands keep their
//     `attribute_exists(outboxId) AND #status = :unprocessed` condition
//     so a stale drainer that beats the lock fence still cannot clobber
//     a row another replica is now terminal-writing.
//
// Per-poll-cycle pattern is preserved (acquire → drain batch → release
// per cycle, NOT continuous leadership). Each `poll()` constructs and
// drives a fresh LeasedKVLock instance via start()/stop() so leadership
// handoff is bounded by pollIntervalMs.

function createDrainerLock(): LeasedKVLock {
  return new LeasedKVLock({
    store: createDDBLeaseStore({ docClient, tableName: OUTBOX_TABLE }),
    lockKey: LOCK_KEY,
    ttlMs: LEASE_TTL_MS,
    ownerId: OWNER_ID,
    onLeaseLost: () => {
      // Renewal failed mid-batch — surface for ops visibility. The lock
      // already cleared its in-memory state, so isLeader() returns false
      // and any in-flight processRow() will refuse the status write on
      // its own (lock-level fence below + row-level fence on the write).
      log.warn({}, 'outbox-drainer lease renewal failed');
    },
  });
}

export function startOutboxDrainer(config?: OutboxDrainerConfig): { stop: () => void } {
  const pollIntervalMs = config?.pollIntervalMs ?? 2000;
  const batchSize = config?.batchSize ?? 25;
  const maxRetries = config?.maxRetries ?? 3;

  let timer: ReturnType<typeof setInterval> | null = null;
  let activeLock: LeasedKVLock | null = null;
  let running = false;
  let stopping = false;

  async function poll(): Promise<void> {
    if (running || stopping) return;
    running = true;

    const lock = createDrainerLock();
    activeLock = lock;

    try {
      let acquired = false;
      try {
        acquired = await lock.start();
      } catch (err: unknown) {
        const e = err as { message?: string };
        log.error({ errMessage: e?.message }, 'outbox-drainer lease acquire failed');
        return;
      }
      if (!acquired) {
        return;
      }

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
          if (!lock.isLeader()) {
            log.warn(
              { remainingRows: rows.length - rows.indexOf(row) },
              'outbox-drainer aborting batch — lease lapsed mid-scan',
            );
            break;
          }
          await processRow(lock, row, maxRetries);
        }
      } finally {
        await lock.stop();
      }
    } catch (err: unknown) {
      const e = err as { message?: string };
      log.error({ errMessage: e?.message }, 'outbox-drainer poll failed');
    } finally {
      activeLock = null;
      running = false;
    }
  }

  async function processRow(
    lock: LeasedKVLock,
    row: OutboxRow,
    maxRetriesCount: number,
  ): Promise<void> {
    if (!lock.isLeader()) {
      log.warn(
        { outboxId: row.outboxId },
        'outbox-drainer skipping row — lease no longer valid',
      );
      return;
    }
    try {
      const parsedPayload = JSON.parse(row.payload) as Record<string, unknown>;

      // catalog.publish routes to the durable MQ transport for declared
      // durable events (e.g. pipeline.run.*). The local `publishDurable`
      // helper only writes to the outbox table — calling it here would
      // re-queue the row instead of delivering it.
      await catalog.publish(row.eventType, parsedPayload);

      // Re-check ownership after the network call: publishDurable could have
      // taken seconds, and the lease may have lapsed while we waited.
      // Refusing the status write here is safe — UNPROCESSED rows stay
      // claimable; the worst case is a duplicate publish next round, which
      // downstream consumers must handle as part of at-least-once semantics.
      if (!lock.isLeader()) {
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

      if (!lock.isLeader()) {
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
      // Best-effort release of any in-flight lock; if the poll cycle is
      // still running it will hit its own finally + lock.stop(). The lock
      // itself swallows release errors and the lease will TTL-expire.
      const inflight = activeLock;
      if (inflight) {
        void inflight.stop();
      }
    },
  };
}
