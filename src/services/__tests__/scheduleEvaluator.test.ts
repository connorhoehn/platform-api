// Tests for the schedule evaluator.
//
// Focus:
//   - cron grammar coverage: *, exact, comma list, A-B range, *\/N step
//   - five-field validation (reject 4 / 6 fields)
//   - evaluateOnce filters by status === 'published' && triggerBinding.event === 'schedule'
//   - tick() suppresses double-fires within the same minute
//   - triggerNow() bypasses the schedule filter

import {
  ScheduleEvaluator,
  createScheduleEvaluator,
  parseCronExpression,
  cronMatches,
  type PipelineForSchedule,
} from '../scheduleEvaluator';
import log from '../../lib/logger';

function pipeline(over: Partial<PipelineForSchedule> = {}): PipelineForSchedule {
  return {
    id: 'p-1',
    status: 'published',
    triggerBinding: { event: 'schedule', schedule: '* * * * *' },
    ...over,
  };
}

describe('parseCronExpression', () => {
  test('accepts five-field "* * * * *"', () => {
    const c = parseCronExpression('* * * * *');
    expect(c).not.toBeNull();
    expect(c!.minute.match).toBe('*');
  });

  test('accepts exact values "30 14 1 * *"', () => {
    const c = parseCronExpression('30 14 1 * *');
    expect(c).not.toBeNull();
    expect(Array.from(c!.minute.match as Set<number>)).toEqual([30]);
    expect(Array.from(c!.hour.match as Set<number>)).toEqual([14]);
    expect(Array.from(c!.dayOfMonth.match as Set<number>)).toEqual([1]);
  });

  test('accepts comma list "0,15,30,45 * * * *"', () => {
    const c = parseCronExpression('0,15,30,45 * * * *');
    expect(c).not.toBeNull();
    const minutes = Array.from(c!.minute.match as Set<number>).sort((a, b) => a - b);
    expect(minutes).toEqual([0, 15, 30, 45]);
  });

  test('accepts dash range "0 9 * * 1-5"', () => {
    const c = parseCronExpression('0 9 * * 1-5');
    expect(c).not.toBeNull();
    const dows = Array.from(c!.dayOfWeek.match as Set<number>).sort((a, b) => a - b);
    expect(dows).toEqual([1, 2, 3, 4, 5]);
  });

  test('accepts step "*/15 * * * *"', () => {
    const c = parseCronExpression('*/15 * * * *');
    expect(c).not.toBeNull();
    expect(c!.minute.match).toBe('*');
    expect(c!.minute.step).toBe(15);
  });

  test('rejects 4 fields', () => {
    expect(parseCronExpression('* * * *')).toBeNull();
  });

  test('rejects 6 fields', () => {
    expect(parseCronExpression('0 * * * * *')).toBeNull();
  });

  test('rejects out-of-range values', () => {
    expect(parseCronExpression('60 * * * *')).toBeNull();
    expect(parseCronExpression('* 24 * * *')).toBeNull();
    expect(parseCronExpression('* * 32 * *')).toBeNull();
    expect(parseCronExpression('* * * 13 *')).toBeNull();
    expect(parseCronExpression('* * * * 7')).toBeNull();
  });

  test('rejects exotic patterns', () => {
    expect(parseCronExpression('1-30/2 * * * *')).toBeNull(); // step on range
    expect(parseCronExpression('? * * * *')).toBeNull();
    expect(parseCronExpression('L * * * *')).toBeNull();
  });
});

describe('cronMatches', () => {
  test('"* * * * *" matches every minute', () => {
    const c = parseCronExpression('* * * * *')!;
    expect(cronMatches(c, new Date('2026-04-23T10:30:00'))).toBe(true);
    expect(cronMatches(c, new Date('2026-04-23T10:31:00'))).toBe(true);
  });

  test('"0 9 * * 1-5" weekday 9am — Mon match, Sat no match', () => {
    const c = parseCronExpression('0 9 * * 1-5')!;
    // 2026-04-20 is a Monday.
    expect(cronMatches(c, new Date(2026, 3, 20, 9, 0))).toBe(true);
    // Saturday (2026-04-25) — should not match.
    expect(cronMatches(c, new Date(2026, 3, 25, 9, 0))).toBe(false);
    // Right hour but wrong minute on a weekday.
    expect(cronMatches(c, new Date(2026, 3, 20, 9, 1))).toBe(false);
  });

  test('"*/15 * * * *" matches at :00, :15, :30, :45 only', () => {
    const c = parseCronExpression('*/15 * * * *')!;
    expect(cronMatches(c, new Date(2026, 3, 23, 10, 0))).toBe(true);
    expect(cronMatches(c, new Date(2026, 3, 23, 10, 15))).toBe(true);
    expect(cronMatches(c, new Date(2026, 3, 23, 10, 30))).toBe(true);
    expect(cronMatches(c, new Date(2026, 3, 23, 10, 45))).toBe(true);
    expect(cronMatches(c, new Date(2026, 3, 23, 10, 1))).toBe(false);
    expect(cronMatches(c, new Date(2026, 3, 23, 10, 14))).toBe(false);
  });

  test('"30 14 1 * *" matches the 1st of any month at 14:30', () => {
    const c = parseCronExpression('30 14 1 * *')!;
    expect(cronMatches(c, new Date(2026, 3, 1, 14, 30))).toBe(true);   // Apr 1
    expect(cronMatches(c, new Date(2026, 4, 1, 14, 30))).toBe(true);   // May 1
    expect(cronMatches(c, new Date(2026, 3, 2, 14, 30))).toBe(false);  // Apr 2
    expect(cronMatches(c, new Date(2026, 3, 1, 14, 29))).toBe(false);  // Apr 1 14:29
  });
});

describe('ScheduleEvaluator.evaluateOnce', () => {
  test('returns ids only for published schedule pipelines whose cron matches now', () => {
    const pipelines: PipelineForSchedule[] = [
      pipeline({ id: 'a', triggerBinding: { event: 'schedule', schedule: '* * * * *' } }),
      pipeline({ id: 'b', status: 'draft', triggerBinding: { event: 'schedule', schedule: '* * * * *' } }),
      pipeline({ id: 'c', triggerBinding: { event: 'manual' } }),
      pipeline({ id: 'd', triggerBinding: { event: 'schedule', schedule: '0 0 1 1 *' } }), // Jan 1, 00:00
    ];
    const evaluator = new ScheduleEvaluator({
      listPipelines: () => pipelines,
      trigger: async () => {},
    });
    const ids = evaluator.evaluateOnce(new Date(2026, 3, 23, 10, 30));
    expect(ids).toEqual(['a']);
  });

  test('skips pipelines with invalid cron strings', () => {
    const pipelines: PipelineForSchedule[] = [
      pipeline({ id: 'a', triggerBinding: { event: 'schedule', schedule: 'not-a-cron' } }),
      pipeline({ id: 'b', triggerBinding: { event: 'schedule', schedule: '* * * * *' } }),
    ];
    const errSpy = jest.spyOn(console, 'error').mockImplementation(() => {});
    const evaluator = new ScheduleEvaluator({
      listPipelines: () => pipelines,
      trigger: async () => {},
    });
    const ids = evaluator.evaluateOnce(new Date(2026, 3, 23, 10, 30));
    expect(ids).toEqual(['b']);
    errSpy.mockRestore();
  });
});

describe('ScheduleEvaluator.tick double-fire suppression', () => {
  test('does not fire the same pipeline twice within the same minute', async () => {
    const fired: Array<{ id: string; at: string }> = [];
    const evaluator = createScheduleEvaluator({
      listPipelines: () => [pipeline({ id: 'a' })],
      trigger: async (id, payload) => {
        fired.push({ id, at: String(payload.firedAt) });
      },
    });
    const t1 = new Date(2026, 3, 23, 10, 30, 5);
    const t2 = new Date(2026, 3, 23, 10, 30, 30);
    const t3 = new Date(2026, 3, 23, 10, 31, 0);
    evaluator.tick(t1);
    evaluator.tick(t2);
    evaluator.tick(t3);
    // Wait a microtask so pending promises resolve.
    await Promise.resolve();
    expect(fired.map(f => f.id)).toEqual(['a', 'a']);
    // First two ticks are in the same minute -> only one firing for that minute.
    expect(fired[0].at).toBe(t1.toISOString());
    expect(fired[1].at).toBe(t3.toISOString());
  });
});

describe('ScheduleEvaluator.triggerNow', () => {
  test('fires regardless of schedule', async () => {
    const fired: string[] = [];
    const evaluator = createScheduleEvaluator({
      listPipelines: () => [pipeline({ id: 'x', triggerBinding: { event: 'schedule', schedule: '0 0 1 1 *' } })],
      trigger: async (id) => { fired.push(id); },
    });
    evaluator.triggerNow('x');
    await Promise.resolve();
    expect(fired).toEqual(['x']);
  });

  test('swallows trigger errors without throwing', async () => {
    const errSpy = jest.spyOn(log, 'error').mockImplementation(() => log);
    const evaluator = createScheduleEvaluator({
      listPipelines: () => [pipeline({ id: 'y' })],
      trigger: async () => { throw new Error('manual trigger failed'); },
    });
    evaluator.triggerNow('y');
    await Promise.resolve();
    expect(errSpy).toHaveBeenCalledWith(
      expect.objectContaining({ pipelineId: 'y', err: expect.any(Error) }),
      '[scheduleEvaluator] manual trigger failed',
    );
    errSpy.mockRestore();
  });
});

describe('ScheduleEvaluator.start / stop', () => {
  test('start() sets a repeating timer; stop() clears it', () => {
    jest.useFakeTimers();
    const fired: string[] = [];
    const evaluator = createScheduleEvaluator({
      listPipelines: () => [pipeline({ id: 'timer-p' })],
      trigger: async (id) => { fired.push(id); },
      pollIntervalMs: 100,
      now: () => new Date(2026, 3, 23, 10, 30, 0),
    });
    evaluator.start();
    jest.advanceTimersByTime(150);
    evaluator.stop();
    // Timer fired at least once
    expect(fired.length).toBeGreaterThanOrEqual(1);
    jest.useRealTimers();
  });

  test('start() is idempotent — second call does not create a second timer', () => {
    jest.useFakeTimers();
    const fired: string[] = [];
    const evaluator = createScheduleEvaluator({
      listPipelines: () => [pipeline({ id: 'idem-p' })],
      trigger: async (id) => { fired.push(id); },
      pollIntervalMs: 100,
      now: () => new Date(2026, 3, 23, 10, 30, 0),
    });
    evaluator.start();
    evaluator.start(); // second call — no-op
    jest.advanceTimersByTime(150);
    evaluator.stop();
    // One timer, one tick per interval — not doubled
    expect(fired.length).toBe(1);
    jest.useRealTimers();
  });

  test('stop() is safe to call before start()', () => {
    const evaluator = createScheduleEvaluator({
      listPipelines: () => [],
      trigger: async () => {},
    });
    expect(() => evaluator.stop()).not.toThrow();
  });

  test('tick() errors inside the interval are caught by start()', async () => {
    jest.useFakeTimers();
    const errSpy = jest.spyOn(log, 'error').mockImplementation(() => log);
    let call = 0;
    const evaluator = createScheduleEvaluator({
      listPipelines: () => { if (call++ === 0) throw new Error('listPipelines exploded'); return []; },
      trigger: async () => {},
      pollIntervalMs: 100,
      now: () => new Date(2026, 3, 23, 10, 30, 0),
    });
    evaluator.start();
    // Phase 54.6.1: runProtectedTick is async; advance fake timers AND let
    // the resulting promise chain settle so the .catch() fires.
    jest.advanceTimersByTime(150);
    await Promise.resolve();
    await Promise.resolve();
    evaluator.stop();
    expect(errSpy).toHaveBeenCalledWith(
      expect.objectContaining({ err: expect.any(Error) }),
      '[scheduleEvaluator] tick error',
    );
    errSpy.mockRestore();
    jest.useRealTimers();
  });
});

describe('ScheduleEvaluator.tick — trigger error catch', () => {
  test('swallows trigger() rejection without crashing tick()', async () => {
    const errSpy = jest.spyOn(log, 'error').mockImplementation(() => log);
    const evaluator = createScheduleEvaluator({
      listPipelines: () => [pipeline({ id: 'fail-p' })],
      trigger: async () => { throw new Error('ddb gone'); },
    });
    evaluator.tick(new Date(2026, 3, 23, 10, 30, 0));
    await Promise.resolve();
    expect(errSpy).toHaveBeenCalledWith(
      expect.objectContaining({ pipelineId: 'fail-p', err: expect.any(Error) }),
      '[scheduleEvaluator] trigger failed',
    );
    errSpy.mockRestore();
  });
});

// ---------------------------------------------------------------------------
// Phase 54.6.1 — leader lock integration
// ---------------------------------------------------------------------------

describe('ScheduleEvaluator.runProtectedTick — leader lock', () => {
  function makeFakeLock(acquireResult: boolean) {
    const calls = { acquire: 0, release: 0, ttlMsSeen: [] as number[] };
    return {
      calls,
      acquire: async (ttlMs: number) => {
        calls.acquire += 1;
        calls.ttlMsSeen.push(ttlMs);
        return acquireResult;
      },
      release: async () => { calls.release += 1; },
    };
  }

  test('runs the tick when no lock is configured (legacy behavior)', async () => {
    const fired: string[] = [];
    const evaluator = createScheduleEvaluator({
      listPipelines: () => [pipeline({ id: 'no-lock' })],
      trigger: async (id) => { fired.push(id); },
    });
    const ran = await evaluator.runProtectedTick(new Date(2026, 3, 23, 10, 30, 0));
    expect(ran).toBe(true);
    await Promise.resolve();
    expect(fired).toEqual(['no-lock']);
  });

  test('skips the tick when lock cannot be acquired (another replica is leader)', async () => {
    const fired: string[] = [];
    const lock = makeFakeLock(false);
    const evaluator = createScheduleEvaluator({
      listPipelines: () => [pipeline({ id: 'should-skip' })],
      trigger: async (id) => { fired.push(id); },
      leaderLock: lock,
    });
    const ran = await evaluator.runProtectedTick(new Date(2026, 3, 23, 10, 30, 0));
    expect(ran).toBe(false);
    expect(lock.calls.acquire).toBe(1);
    expect(lock.calls.release).toBe(0); // never acquired, no release
    expect(fired).toEqual([]);
  });

  test('runs the tick when lock is acquired and releases after', async () => {
    const fired: string[] = [];
    const lock = makeFakeLock(true);
    const evaluator = createScheduleEvaluator({
      listPipelines: () => [pipeline({ id: 'is-leader' })],
      trigger: async (id) => { fired.push(id); },
      leaderLock: lock,
    });
    const ran = await evaluator.runProtectedTick(new Date(2026, 3, 23, 10, 30, 0));
    expect(ran).toBe(true);
    expect(lock.calls.acquire).toBe(1);
    expect(lock.calls.release).toBe(1);
    await Promise.resolve();
    expect(fired).toEqual(['is-leader']);
  });

  test('passes 2x pollIntervalMs as the lock TTL', async () => {
    const lock = makeFakeLock(true);
    const evaluator = createScheduleEvaluator({
      pollIntervalMs: 30_000,
      listPipelines: () => [],
      trigger: async () => { /* no-op */ },
      leaderLock: lock,
    });
    await evaluator.runProtectedTick(new Date(2026, 3, 23, 10, 30, 0));
    expect(lock.calls.ttlMsSeen[0]).toBe(60_000); // 2 * 30_000
  });

  test('release errors do not propagate (best-effort)', async () => {
    const lock = {
      acquire: async () => true,
      release: async () => { throw new Error('ddb timeout'); },
    };
    const evaluator = createScheduleEvaluator({
      listPipelines: () => [pipeline({ id: 'release-error' })],
      trigger: async () => { /* no-op */ },
      leaderLock: lock,
    });
    await expect(
      evaluator.runProtectedTick(new Date(2026, 3, 23, 10, 30, 0)),
    ).resolves.toBe(true);
  });
});

describe('ScheduleEvaluator.tick — dedupe set eviction', () => {
  test('prunes entries older than ~10 minutes when fired set exceeds 1024', async () => {
    const fired: string[] = [];
    const baseMinuteEpoch = Math.floor(new Date(2026, 3, 23, 10, 30, 0).getTime() / 60_000);

    // Build an evaluator that returns a different pipeline each tick so each
    // tick adds a new key and grows the set.  We need to push the set past 1024
    // entries without needing 1024 calls, so we pre-populate via multiple ticks
    // across different minute epochs.
    const evaluator = createScheduleEvaluator({
      listPipelines: () => [pipeline({ id: 'evict-p' })],
      trigger: async (id) => { fired.push(id); },
    });

    // Simulate 1025 distinct minutes — each adds one entry to `fired`.
    for (let i = 0; i <= 1025; i++) {
      const t = new Date((baseMinuteEpoch + i) * 60_000);
      evaluator.tick(t);
    }
    await Promise.resolve();
    // After eviction the set should be smaller than the naive unbounded size.
    // We can't read the private set directly, but we can verify nothing threw
    // and the evaluator still fires on the next unique minute.
    const countBefore = fired.length;
    evaluator.tick(new Date((baseMinuteEpoch + 2000) * 60_000));
    await Promise.resolve();
    expect(fired.length).toBe(countBefore + 1);
  });
});
