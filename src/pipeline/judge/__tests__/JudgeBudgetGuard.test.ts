// platform-api/src/pipeline/judge/__tests__/JudgeBudgetGuard.test.ts
//
// Unit tests for JudgeBudgetGuard.
//
// Exercises: cap-from-env, per-pipeline override, isExhausted boundary,
// Redis-down soft-fail (allow), record() increment + TTL set, day-bucket
// rollover on midnight UTC.

const mockGetRedisClient = jest.fn();
jest.mock('../../../lib/redis-client', () => ({
  getRedisClient: () => mockGetRedisClient(),
}));
jest.mock('../../../lib/logger', () => ({
  __esModule: true,
  default: { warn: jest.fn(), info: jest.fn(), error: jest.fn(), debug: jest.fn() },
}));

import { JudgeBudgetGuard } from '../JudgeBudgetGuard';

interface FakeRedis {
  get: jest.Mock;
  incrByFloat: jest.Mock;
  expire: jest.Mock;
}

function fakeRedis(): FakeRedis {
  return {
    get: jest.fn().mockResolvedValue(null),
    incrByFloat: jest.fn().mockResolvedValue('0'),
    expire: jest.fn().mockResolvedValue(1),
  };
}

const ENV_BACKUP = { ...process.env };

beforeEach(() => {
  mockGetRedisClient.mockReset();
  process.env = { ...ENV_BACKUP };
});

// ---------------------------------------------------------------------------
// Cap resolution
// ---------------------------------------------------------------------------

describe('JudgeBudgetGuard — cap resolution', () => {
  it('uses the default $10/day when env + options unset', async () => {
    const r = fakeRedis();
    r.get.mockResolvedValue('9.5'); // 9.5 + 1.0 = 10.5 > 10 → exhausted
    mockGetRedisClient.mockResolvedValue(r);
    const guard = new JudgeBudgetGuard();
    expect(await guard.isExhausted('pl_1', 1)).toBe(true);
  });

  it('honors JUDGE_DAILY_BUDGET_USD_DEFAULT env override', async () => {
    process.env.JUDGE_DAILY_BUDGET_USD_DEFAULT = '50';
    const r = fakeRedis();
    r.get.mockResolvedValue('9.5'); // 9.5 + 1.0 = 10.5 < 50 → not exhausted
    mockGetRedisClient.mockResolvedValue(r);
    const guard = new JudgeBudgetGuard();
    expect(await guard.isExhausted('pl_1', 1)).toBe(false);
  });

  it('honors perPipelineBudgetUsd override over default', async () => {
    const r = fakeRedis();
    r.get.mockResolvedValue('9.5'); // pipeline_lean cap is 5 → exhausted
    mockGetRedisClient.mockResolvedValue(r);
    const guard = new JudgeBudgetGuard({
      perPipelineBudgetUsd: { pipeline_lean: 5 },
    });
    expect(await guard.isExhausted('pipeline_lean', 0.1)).toBe(true);
  });

  it('falls back to options.defaultDailyBudgetUsd when env is empty', async () => {
    const r = fakeRedis();
    r.get.mockResolvedValue('14');
    mockGetRedisClient.mockResolvedValue(r);
    const guard = new JudgeBudgetGuard({ defaultDailyBudgetUsd: 15 });
    expect(await guard.isExhausted('pl_1', 0.5)).toBe(false);
    expect(await guard.isExhausted('pl_1', 2)).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// isExhausted soft-fail
// ---------------------------------------------------------------------------

describe('JudgeBudgetGuard — isExhausted', () => {
  it('returns false (allow) when Redis is null', async () => {
    mockGetRedisClient.mockResolvedValue(null);
    const guard = new JudgeBudgetGuard();
    expect(await guard.isExhausted('pl_1', 100)).toBe(false);
  });

  it('returns false (allow) when Redis throws', async () => {
    const r = fakeRedis();
    r.get.mockRejectedValue(new Error('CONN_RESET'));
    mockGetRedisClient.mockResolvedValue(r);
    const guard = new JudgeBudgetGuard();
    expect(await guard.isExhausted('pl_1', 100)).toBe(false);
  });

  it('returns false when current spend is 0 (missing key)', async () => {
    const r = fakeRedis();
    mockGetRedisClient.mockResolvedValue(r);
    const guard = new JudgeBudgetGuard();
    expect(await guard.isExhausted('pl_1', 5)).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// record
// ---------------------------------------------------------------------------

describe('JudgeBudgetGuard — record', () => {
  it('calls INCRBYFLOAT + EXPIRE with the daily key', async () => {
    const r = fakeRedis();
    mockGetRedisClient.mockResolvedValue(r);
    const fixed = new Date('2026-06-02T15:00:00Z').getTime();
    const guard = new JudgeBudgetGuard({ now: () => fixed });
    await guard.record('pl_1', 0.123);

    expect(r.incrByFloat).toHaveBeenCalledWith('judge:budget:pl_1:2026-06-02', 0.123);
    expect(r.expire).toHaveBeenCalledWith('judge:budget:pl_1:2026-06-02', 48 * 60 * 60);
  });

  it('is a no-op when cost is 0', async () => {
    const r = fakeRedis();
    mockGetRedisClient.mockResolvedValue(r);
    const guard = new JudgeBudgetGuard();
    await guard.record('pl_1', 0);

    expect(r.incrByFloat).not.toHaveBeenCalled();
  });

  it('is a no-op when Redis is unavailable', async () => {
    mockGetRedisClient.mockResolvedValue(null);
    const guard = new JudgeBudgetGuard();
    await guard.record('pl_1', 0.5); // should not throw
  });

  it('swallows Redis errors silently', async () => {
    const r = fakeRedis();
    r.incrByFloat.mockRejectedValue(new Error('boom'));
    mockGetRedisClient.mockResolvedValue(r);
    const guard = new JudgeBudgetGuard();
    await expect(guard.record('pl_1', 0.5)).resolves.toBeUndefined();
  });

  it('uses UTC day boundary, not local', async () => {
    // 2026-06-02 23:30Z → still 2026-06-02 in UTC even if local TZ would
    // have rolled over to 2026-06-03.
    const r = fakeRedis();
    mockGetRedisClient.mockResolvedValue(r);
    const lateUtc = new Date('2026-06-02T23:30:00Z').getTime();
    const guard = new JudgeBudgetGuard({ now: () => lateUtc });
    await guard.record('pl_1', 0.1);

    expect(r.incrByFloat).toHaveBeenCalledWith(
      expect.stringContaining(':2026-06-02'),
      expect.any(Number),
    );
  });
});
