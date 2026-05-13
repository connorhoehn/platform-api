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

beforeEach(() => {
  jest.clearAllMocks();
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
    await lock.acquire(120_000);
    const cmd = mockSend.mock.calls[0][0] as { input: Record<string, unknown> };
    expect(cmd.input).toMatchObject({
      ConditionExpression: 'attribute_not_exists(outboxId) OR expiresAt < :now',
    });
    const item = cmd.input.Item as Record<string, unknown>;
    expect(item.outboxId).toBe('SCHEDULE_LOCK#scheduleEvaluator');
    expect(item.owner).toBe('replica-4');
    expect(typeof item.expiresAt).toBe('number');
    expect(typeof item.acquiredAt).toBe('number');
    // TTL ~= 120s from acquiredAt
    expect((item.expiresAt as number) - (item.acquiredAt as number)).toBe(120_000);
  });

  it('release() is a no-op on a lock that was never acquired', async () => {
    const lock = createDDBSchedulerLeaderLock('replica-5');
    await expect(lock.release()).resolves.toBeUndefined();
    expect(mockSend).not.toHaveBeenCalled();
  });

  it('release() is a no-op even after acquire (TTL-based expiry, not delete)', async () => {
    mockSend.mockResolvedValue({});
    const lock = createDDBSchedulerLeaderLock('replica-6');
    await lock.acquire(60_000);
    await lock.release();
    // Only the acquire call hit DDB; release is in-memory only.
    expect(mockSend).toHaveBeenCalledTimes(1);
  });
});
