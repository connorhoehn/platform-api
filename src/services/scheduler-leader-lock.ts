// platform-api/src/services/scheduler-leader-lock.ts
//
// Phase 54.6.1 — leader-lock for the schedule evaluator's per-tick window.
//
// Acts as a thin per-batch wrapper over distributed-core's LeaseStore
// (DDB-backed via createDDBLeaseStore). The acquire-per-tick / release
// pattern is intentionally NOT LeasedKVLock — that primitive runs
// continuous heartbeats for long-lived leadership, which is overkill
// for sub-second tick windows. This file just delegates to LeaseStore's
// tryClaim + releaseIfOwner.
//
// Other lock sites in this repo (ingest engine schedule timer) DO use
// the long-lived LeasedKVLock pattern; the underlying DDB row format
// is identical thanks to the shared LeaseStore adapter.

import { docClient } from '../lib/aws-clients';
import { tableName } from '../lib/ddb-table-name';
import { createDDBLeaseStore } from '../lib/ddb-lease-store';
import log from '../lib/logger';

const OUTBOX_TABLE = tableName(process.env.DYNAMODB_OUTBOX_TABLE || 'social-outbox');
const LOCK_KEY = 'SCHEDULE_LOCK#scheduleEvaluator';

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
 * DDB-conditional lock built on the shared LeaseStore adapter. Implements
 * the per-batch SchedulerLeaderLock contract scheduleEvaluator expects.
 */
export function createDDBSchedulerLeaderLock(ownerId: string): SchedulerLeaderLock {
  const store = createDDBLeaseStore({ docClient, tableName: OUTBOX_TABLE });
  let held = false;

  return {
    async acquire(ttlMs: number): Promise<boolean> {
      try {
        const claimed = await store.tryClaim(LOCK_KEY, {
          owner: ownerId,
          expiresAt: Date.now() + ttlMs,
        });
        if (claimed) held = true;
        return claimed;
      } catch (err: unknown) {
        // Treat any unexpected error as non-acquired so we don't fire
        // duplicates on a transient DDB hiccup. Log so it's visible.
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
      // Best-effort delete; lease will TTL-expire naturally if the
      // delete fails. The next replica's tick sees the expired row and
      // overwrites it, so leadership handoff is bounded by ttlMs.
      await store.releaseIfOwner(LOCK_KEY, ownerId);
    },
  };
}
