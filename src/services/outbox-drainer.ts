import { QueryCommand, ScanCommand, UpdateCommand } from '@aws-sdk/lib-dynamodb';
import { randomUUID } from 'crypto';
import { hostname } from 'os';
import { LeasedKVLock } from 'distributed-core';
import { ddbClient, docClient } from '../lib/aws-clients';
import { catalog } from '../lib/event-catalog';
import { tableName } from '../lib/ddb-table-name';
import { createDDBLeaseStore } from '../lib/ddb-lease-store';
import {
  PENDING_ATTR,
  PENDING_INDEX,
  PENDING_SHARDS,
  isPendingIndexUnavailable,
  pendingIndexActive,
  pendingShardFor,
} from '../lib/outbox-pending';
import log from '../lib/logger';
import { registerOutboxWake } from '../lib/outboxWake';
import { localTtlSweepEnabled } from '../lib/localTtlSweep';
import {
  recordOutboxLeaseAcquired,
  recordOutboxOldestPendingSeconds,
  recordOutboxPublishFailure,
  recordOutboxPublished,
} from '../observability/metrics';

const OUTBOX_TABLE = tableName('social-outbox');
const LOCK_KEY = 'OUTBOX_LOCK#drainer';
const LEASE_TTL_MS = 30_000;

/**
 * TTL for completed/failed rows (kept briefly for debugging; nothing reads
 * them). 24 h in AWS. NFR #223: in local dev the outbox took ~100k rows/day
 * (95% `work-graph.pipeline.lifecycle.v1` under run load), so 24 h of
 * retention alone was 110 MB of a DynamoDB Local that ran out of heap — dev
 * keeps 1 h. `OUTBOX_TERMINAL_TTL_SECONDS` overrides either.
 */
export function outboxTerminalTtlSeconds(env: NodeJS.ProcessEnv = process.env): number {
  const explicit = Number(env.OUTBOX_TERMINAL_TTL_SECONDS);
  if (Number.isFinite(explicit) && explicit > 0) return Math.floor(explicit);
  return localTtlSweepEnabled(env) ? 60 * 60 : 24 * 60 * 60;
}
const TERMINAL_TTL_SECONDS = outboxTerminalTtlSeconds();
/** Rows examined per round trip while looking for pending ones. */
const SCAN_PAGE_SIZE = 200;
/** A cap so one poll cycle cannot walk an arbitrarily large table. */
const MAX_SCAN_PAGES = 20;
/** How often, while pending-index is not ACTIVE, to ask whether it is. */
const INDEX_RECHECK_MS = 15_000;

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
  /** Where the next fallback table walk resumes (undefined = the top). NFR 34b. */
  let scanCursor: Record<string, unknown> | undefined;
  /** The stray-adoption walk: its cursor, strays seen this pass, and whether a clean pass ended it. */
  let adoptCursor: Record<string, unknown> | undefined;
  let strayCount = 0;
  let adoptionDone = false;
  let indexActive = false;
  let indexCheckedAt = 0;
  let indexWaitLogged = false;

  // NFR #133: a writer in this process wakes the drainer instead of waiting
  // for the next tick. A wake during a poll is served right after it; wakes
  // within a few ms share one poll (one lease round trip, not one per row).
  let wakeAgain = false;
  let wakeTimer: ReturnType<typeof setTimeout> | null = null;
  function wake(): void {
    if (stopping) return;
    if (running) { wakeAgain = true; return; }
    if (wakeTimer) return;
    wakeTimer = setTimeout(() => { wakeTimer = null; void poll(); }, 10);
    wakeTimer.unref?.();
  }
  const unregisterWake = registerOutboxWake('platform-events', wake);

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

      // PromQL-visible signal that *this* replica won the lease — paired
      // with `outbox_oldest_pending_seconds` to detect "lease keeps moving
      // but rows aren't draining" vs "no one's holding the lease."
      recordOutboxLeaseAcquired();

      try {
        const rows = await findPending(lock);

        // "Oldest pending": from the index the batch is each shard's oldest
        // rows, so this is exact; on the fallback table walk it is a lower
        // bound when there are more than batchSize pending rows. Good enough as
        // a "is the drainer keeping up" signal; an emergency-paging alert
        // would also watch run-queue depth.
        let oldestPendingSeconds = 0;
        if (rows.length > 0) {
          const nowMs = Date.now();
          let oldestMs = nowMs;
          for (const r of rows) {
            const t = new Date(r.createdAt).getTime();
            if (Number.isFinite(t) && t < oldestMs) oldestMs = t;
          }
          oldestPendingSeconds = Math.max(0, (nowMs - oldestMs) / 1000);
        }
        recordOutboxOldestPendingSeconds(oldestPendingSeconds);

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
      if (wakeAgain) { wakeAgain = false; wake(); }
    }
  }

  /**
   * The rows to publish this poll, oldest first.
   *
   * Normally `pending-index` (see lib/outbox-pending): PENDING_SHARDS Queries
   * that read only undrained rows, so a poll costs the backlog, not the
   * table — the table keeps ~a day of PROCESSED rows under the ttl, and
   * walking them was O(table) per poll (NFR #34c). The table walk below is
   * kept for the one case the index cannot serve: it does not exist yet or is
   * still backfilling.
   */
  async function findPending(lock: LeasedKVLock): Promise<OutboxRow[]> {
    // Until the index is ACTIVE (created at boot, then backfilled) a Query
    // fails — and DynamoDB Local answers it with a bare InternalFailure —
    // so ask the table, at most every INDEX_RECHECK_MS, and walk meanwhile.
    if (!indexActive && Date.now() - indexCheckedAt >= INDEX_RECHECK_MS) {
      indexCheckedAt = Date.now();
      indexActive = await pendingIndexActive(ddbClient, OUTBOX_TABLE);
      if (indexActive) log.info({}, 'outbox-drainer: pending-index ACTIVE — table walk off');
      else if (!indexWaitLogged) {
        indexWaitLogged = true;
        log.warn({}, 'outbox-drainer: pending-index not ACTIVE yet — walking the table');
      }
    }
    if (!indexActive) return scanPending(lock);
    let rows: OutboxRow[];
    try {
      rows = await queryPendingIndex();
    } catch (err: unknown) {
      if (!isPendingIndexUnavailable(err)) throw err;
      indexActive = false;
      return scanPending(lock);
    }
    if (rows.length < batchSize && !adoptionDone) await adoptStrays(lock);
    return rows;
  }

  /** Each shard's oldest rows, merged into receipt (ulid) order, one batch. */
  async function queryPendingIndex(): Promise<OutboxRow[]> {
    const pages = await Promise.all(PENDING_SHARDS.map((shard) => docClient.send(
      new QueryCommand({
        TableName: OUTBOX_TABLE,
        IndexName: PENDING_INDEX,
        KeyConditionExpression: '#p = :shard',
        ExpressionAttributeNames: { '#p': PENDING_ATTR },
        ExpressionAttributeValues: { ':shard': shard },
        Limit: batchSize,
      }),
    )));
    const listed = pages.flatMap((page) => (page.Items ?? []) as OutboxRow[]);
    // A listed row that is no longer UNPROCESSED is either index lag (it was
    // processed a moment ago) or a row that went terminal without dropping its
    // shard (e.g. processed by an older build). Skip it, and drop the shard so
    // it cannot sit at the head of its shard, ahead of real pending rows.
    const stale = listed.filter((row) => row.status !== 'UNPROCESSED');
    await Promise.all(stale.map((row) => docClient.send(new UpdateCommand({
      TableName: OUTBOX_TABLE,
      Key: { outboxId: row.outboxId },
      UpdateExpression: 'REMOVE #p',
      ConditionExpression: 'attribute_exists(outboxId) AND #status <> :unprocessed',
      ExpressionAttributeNames: { '#status': 'status', '#p': PENDING_ATTR },
      ExpressionAttributeValues: { ':unprocessed': 'UNPROCESSED' },
    })).catch((err: unknown) => {
      if ((err as { name?: string })?.name !== 'ConditionalCheckFailedException') throw err;
    })));
    const rows = listed.filter((row) => row.status === 'UNPROCESSED');
    rows.sort((a, b) => (a.outboxId < b.outboxId ? -1 : a.outboxId > b.outboxId ? 1 : 0));
    return rows.slice(0, batchSize);
  }

  /**
   * Rows written without `pendingShard` — by a build from before the index,
   * e.g. the other pod mid-rollout — are invisible to it. Until one full walk
   * of the table finds none, each poll that has spare batch room walks up to
   * MAX_SCAN_PAGES pages and gives such rows their shard (conditional: still
   * UNPROCESSED, still unsharded), so they drain through the index next poll.
   * One delivery path; the walk switches itself off for the life of the
   * process once the table is clean.
   */
  async function adoptStrays(lock: LeasedKVLock): Promise<void> {
    let pages = 0;
    while (pages < MAX_SCAN_PAGES && lock.isLeader()) {
      const result = await docClient.send(
        new ScanCommand({
          TableName: OUTBOX_TABLE,
          FilterExpression: '#status = :unprocessed AND attribute_not_exists(#p)',
          ExpressionAttributeNames: { '#status': 'status', '#p': PENDING_ATTR },
          ExpressionAttributeValues: { ':unprocessed': 'UNPROCESSED' },
          Limit: SCAN_PAGE_SIZE,
          ...(adoptCursor ? { ExclusiveStartKey: adoptCursor } : {}),
        }),
      );
      pages += 1;
      for (const row of (result.Items ?? []) as OutboxRow[]) {
        strayCount += 1;
        try {
          await docClient.send(new UpdateCommand({
            TableName: OUTBOX_TABLE,
            Key: { outboxId: row.outboxId },
            UpdateExpression: 'SET #p = :shard',
            ConditionExpression: '#status = :unprocessed AND attribute_not_exists(#p)',
            ExpressionAttributeNames: { '#status': 'status', '#p': PENDING_ATTR },
            ExpressionAttributeValues: { ':unprocessed': 'UNPROCESSED', ':shard': pendingShardFor(row.outboxId) },
          }));
        } catch (err: unknown) {
          if ((err as { name?: string })?.name !== 'ConditionalCheckFailedException') throw err;
        }
      }
      adoptCursor = result.LastEvaluatedKey as Record<string, unknown> | undefined;
      if (!adoptCursor) {
        // End of a full walk from the top.
        if (strayCount === 0) {
          adoptionDone = true;
          log.info({}, 'outbox-drainer: no unsharded rows left — stray walk off');
        } else {
          log.info({ adopted: strayCount }, 'outbox-drainer: adopted unsharded rows into pending-index');
        }
        strayCount = 0;
        return;
      }
    }
  }

  /**
   * Fallback while `pending-index` is unavailable: walk the table for
   * UNPROCESSED rows. A Scan's `Limit` counts rows EXAMINED, not matched, so
   * page until the batch is full; and resume where the last poll stopped,
   * wrapping at the end (NFR 34b — restarting from the top every poll
   * stranded any row past the first MAX_SCAN_PAGES × SCAN_PAGE_SIZE).
   */
  async function scanPending(lock: LeasedKVLock): Promise<OutboxRow[]> {
    const rows: OutboxRow[] = [];
    let cursor = scanCursor;
    let wrapped = cursor === undefined;
    let pages = 0;
    for (;;) {
      const pageStart = cursor;
      const result = await docClient.send(
        new ScanCommand({
          TableName: OUTBOX_TABLE,
          FilterExpression: '#status = :unprocessed',
          ExpressionAttributeNames: { '#status': 'status' },
          ExpressionAttributeValues: { ':unprocessed': 'UNPROCESSED' },
          Limit: SCAN_PAGE_SIZE,
          ...(pageStart ? { ExclusiveStartKey: pageStart } : {}),
        }),
      );
      rows.push(...((result.Items ?? []) as OutboxRow[]));
      pages += 1;
      if (rows.length > batchSize) {
        // This page is only partly taken: read it again next poll (the
        // rows published from it will be PROCESSED by then).
        cursor = pageStart;
        break;
      }
      cursor = result.LastEvaluatedKey as Record<string, unknown> | undefined;
      if (rows.length === batchSize) break;
      if (!cursor) {
        // The end of the table: go round once more from the top.
        if (wrapped) break;
        wrapped = true;
      }
      // The lease is the budget for a whole poll cycle, and each page is
      // a round trip; a scan that outlives it would publish as a
      // non-leader and be refused row by row.
      if (pages >= MAX_SCAN_PAGES || !lock.isLeader()) break;
    }
    scanCursor = cursor;
    rows.length = Math.min(rows.length, batchSize);
    return rows;
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
          UpdateExpression: 'SET #status = :processed, processedAt = :now, completedAt = :completedAt, #ttl = :ttl REMOVE #p',
          ConditionExpression: 'attribute_exists(outboxId) AND #status = :unprocessed',
          ExpressionAttributeNames: { '#status': 'status', '#ttl': 'ttl', '#p': PENDING_ATTR },
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
      recordOutboxPublished(row.eventType);
    } catch (err: unknown) {
      const e = err as { message?: string; name?: string };
      // If another drainer already marked this row PROCESSED/FAILED, the
      // ConditionExpression on our status write will reject — swallow that
      // specific case quietly; it's the desired outcome under contention.
      // Don't count this as a failure — the row was published successfully
      // by someone (us or another replica). The conditional-check rejection
      // is a contention outcome, not a publish error.
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
      recordOutboxPublishFailure(row.eventType);

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
              UpdateExpression: 'SET #status = :failed, retryCount = :retryCount, failedAt = :failedAt, #ttl = :ttl REMOVE #p',
              ConditionExpression: 'attribute_exists(outboxId) AND #status = :unprocessed',
              ExpressionAttributeNames: { '#status': 'status', '#ttl': 'ttl', '#p': PENDING_ATTR },
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

  // The dev-only TTL stand-in moved to the sweep leader (NFR #223,
  // `local-ttl-sweep` in pipeline/bootstrap.ts): it used to run here on every
  // replica, unleased, scanning the whole outbox every 10 minutes.

  void poll();

  return {
    stop: () => {
      stopping = true;
      unregisterWake();
      if (wakeTimer) { clearTimeout(wakeTimer); wakeTimer = null; }
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
