// src/services/__tests__/scheduler-leader-lock.test.ts
//
// Phase 54.6.1 — verifies the DDB-conditional leader-lock for the schedule
// evaluator. Pattern: acquire succeeds when no row exists or current row
// is expired; fails on ConditionalCheckFailedException (another replica
// holds it). Any other DDB error is treated as non-leader (safe default —
// don't risk duplicate fires on a transient hiccup).

const mockSend = jest.fn<Promise<unknown>, [unknown]>();
jest.mock('../../lib/aws-clients', () => ({
  docClient: { send: mockSend },
}));
jest.mock('../../lib/ddb-table-name', () => ({
  tableName: (n: string) => n,
}));

import { createDDBSchedulerLeaderLock } from '../scheduler-leader-lock';
import { getRegistry } from '../../observability/metrics';
import type { MetricSample } from 'distributed-core';

function counterValue(samples: MetricSample[], name: string): number {
  const m = samples.find((s) => s.name === name);
  if (!m) throw new Error(`metric "${name}" not found`);
  return m.value as number;
}

beforeEach(() => {
  jest.clearAllMocks();
  getRegistry().reset();
});

describe('createDDBSchedulerLeaderLock', () => {
  it('returns true when DDB accepts the conditional PutItem', async () => {
    mockSend.mockResolvedValue({});
    const lock = createDDBSchedulerLeaderLock('replica-1');
    expect(await lock.acquire(60_000)).toBe(true);
    expect(mockSend).toHaveBeenCalledTimes(1);
  });

  it('returns false when DDB rejects with ConditionalCheckFailedException', async () => {
    const err = Object.assign(new Error('conflict'), { name: 'ConditionalCheckFailedException' });
    mockSend.mockRejectedValue(err);
    const lock = createDDBSchedulerLeaderLock('replica-2');
    expect(await lock.acquire(60_000)).toBe(false);
  });

  it('returns false on any other DDB error (safe default — no duplicate fires)', async () => {
    mockSend.mockRejectedValue(new Error('ThrottlingException'));
    const lock = createDDBSchedulerLeaderLock('replica-3');
    expect(await lock.acquire(60_000)).toBe(false);
  });

  it('writes the correct conditional PutItem shape', async () => {
    mockSend.mockResolvedValue({});
    const lock = createDDBSchedulerLeaderLock('replica-4');
    const before = Date.now();
    await lock.acquire(120_000);
    const after = Date.now();
    const cmd = mockSend.mock.calls[0][0] as { input: Record<string, unknown> };
    expect(cmd.input).toMatchObject({
      ConditionExpression: 'attribute_not_exists(outboxId) OR expiresAt < :now',
    });
    const item = cmd.input.Item as Record<string, unknown>;
    expect(item.outboxId).toBe('SCHEDULE_LOCK#scheduleEvaluator');
    expect(item.owner).toBe('replica-4');
    expect(typeof item.expiresAt).toBe('number');
    // TTL ~= 120s from now (sanity bound; the LeaseStore adapter no longer
    // writes acquiredAt — the row format is owner + expiresAt only)
    expect((item.expiresAt as number)).toBeGreaterThanOrEqual(before + 120_000);
    expect((item.expiresAt as number)).toBeLessThanOrEqual(after + 120_000);
  });

  it('release() is a no-op on a lock that was never acquired', async () => {
    const lock = createDDBSchedulerLeaderLock('replica-5');
    await expect(lock.release()).resolves.toBeUndefined();
    expect(mockSend).not.toHaveBeenCalled();
  });

  it('release() issues a conditional DeleteItem when the lock was held', async () => {
    // Post-LeasedKVLock migration: release() now performs an
    // owner-conditional delete via the shared LeaseStore adapter for
    // immediate leadership handoff. Old behavior (TTL-only, no delete)
    // is gone — we hand off in milliseconds instead of waiting out the lease.
    mockSend.mockResolvedValue({});
    const lock = createDDBSchedulerLeaderLock('replica-6');
    await lock.acquire(60_000);
    await lock.release();
    expect(mockSend).toHaveBeenCalledTimes(2);
    const releaseCmd = mockSend.mock.calls[1][0] as { input: Record<string, unknown> };
    expect(releaseCmd.input).toMatchObject({
      ConditionExpression: '#owner = :owner',
    });
  });

  // hub#1133 — observability counters
  describe('observability counters', () => {
    it('increments scheduler_leader_acquired_total on successful claim', async () => {
      mockSend.mockResolvedValue({});
      const lock = createDDBSchedulerLeaderLock('replica-m1');
      await lock.acquire(60_000);
      const { metrics } = getRegistry().getSnapshot();
      expect(counterValue(metrics, 'scheduler_leader_acquired_total')).toBe(1);
      expect(counterValue(metrics, 'scheduler_leader_skipped_total')).toBe(0);
      expect(counterValue(metrics, 'scheduler_leader_errors_total')).toBe(0);
    });

    it('increments scheduler_leader_skipped_total when another replica holds the lease', async () => {
      const err = Object.assign(new Error('conflict'), { name: 'ConditionalCheckFailedException' });
      mockSend.mockRejectedValue(err);
      const lock = createDDBSchedulerLeaderLock('replica-m2');
      await lock.acquire(60_000);
      const { metrics } = getRegistry().getSnapshot();
      expect(counterValue(metrics, 'scheduler_leader_skipped_total')).toBe(1);
      expect(counterValue(metrics, 'scheduler_leader_acquired_total')).toBe(0);
      expect(counterValue(metrics, 'scheduler_leader_errors_total')).toBe(0);
    });

    it('increments scheduler_leader_errors_total when the lease store throws unexpectedly', async () => {
      mockSend.mockRejectedValue(new Error('ThrottlingException'));
      const lock = createDDBSchedulerLeaderLock('replica-m3');
      await lock.acquire(60_000);
      const { metrics } = getRegistry().getSnapshot();
      expect(counterValue(metrics, 'scheduler_leader_errors_total')).toBe(1);
      expect(counterValue(metrics, 'scheduler_leader_acquired_total')).toBe(0);
      expect(counterValue(metrics, 'scheduler_leader_skipped_total')).toBe(0);
    });
  });
});
