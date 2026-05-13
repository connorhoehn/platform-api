// platform-api/src/services/scheduler-leader-lock.ts
//
// Phase 54.6.1 — leader-lock primitive for the schedule evaluator.
//
// The schedule evaluator's tick() fires cron-scheduled pipeline triggers. In
// a multi-replica deployment, two replicas tick'ing within the same minute
// would each fire every matching pipeline — duplicate runs, billing noise,
// surprise side effects.
//
// This lock gates each tick so only one replica holds the leader role for
// the tick window. The other replicas attempt to acquire, see the lock is
// held, and skip the tick. After the TTL the lock auto-expires; on the next
// tick whichever replica wins the race becomes leader for that tick.
//
// Pattern mirrors `services/outbox-drainer.ts` for operator consistency:
// DDB-backed conditional PutItem, same outbox table, same shape.

import { PutCommand } from '@aws-sdk/lib-dynamodb';
import { docClient } from '../lib/aws-clients';
import { tableName } from '../lib/ddb-table-name';
import log from '../lib/logger';

const OUTBOX_TABLE = tableName(process.env.DYNAMODB_OUTBOX_TABLE || 'social-outbox');

/** Single-row lock identifier in the outbox table. */
const LOCK_KEY = { outboxId: 'SCHEDULE_LOCK#scheduleEvaluator' };

/**
 * Leader-lock contract. The schedule evaluator calls `acquire()` at the
 * start of each tick, runs work if true, then calls `release()`. Failures
 * to release are non-fatal — the lease will expire on its own.
 */
export interface SchedulerLeaderLock {
  /**
   * Attempts to claim leadership for the next `ttlMs`. Returns true if
   * acquired, false if another replica currently holds it.
   */
  acquire(ttlMs: number): Promise<boolean>;
  /** Best-effort release. Idempotent. */
  release(): Promise<void>;
}

/**
 * DDB-conditional lock (no distributed-core dependency). Stores the lock
 * row in the outbox table, conditional on either non-existence or
 * past-expiry. Same pattern as outbox-drainer's lease.
 */
export function createDDBSchedulerLeaderLock(ownerId: string): SchedulerLeaderLock {
  let held = false;

  return {
    async acquire(ttlMs: number): Promise<boolean> {
      const now = Date.now();
      const expiresAt = now + ttlMs;
      try {
        await docClient.send(
          new PutCommand({
            TableName: OUTBOX_TABLE,
            Item: {
              ...LOCK_KEY,
              owner: ownerId,
              expiresAt,
              acquiredAt: now,
            },
            ConditionExpression: 'attribute_not_exists(outboxId) OR expiresAt < :now',
            ExpressionAttributeValues: { ':now': now },
          }),
        );
        held = true;
        return true;
      } catch (err: unknown) {
        if ((err as { name?: string }).name === 'ConditionalCheckFailedException') {
          return false;
        }
        // Treat any other error as non-acquired so we don't fire duplicates
        // on a transient DDB hiccup. Log so it's visible to operators.
        log.warn(
          { err: err instanceof Error ? err.message : String(err) },
          '[scheduler-leader-lock] acquire failed (treating as non-leader)',
        );
        return false;
      }
    },

    async release(): Promise<void> {
      if (!held) return;
      held = false;
      // We rely on TTL-based expiry rather than a hard delete to keep this
      // implementation simple. A future tick by ANY replica will see the
      // lock and either find it expired (and overwrite) or held (and skip).
      // Net effect: ttl-bounded leadership handoff with zero coordination
      // cost on release. Aligns with the outbox-drainer fallback model.
    },
  };
}
