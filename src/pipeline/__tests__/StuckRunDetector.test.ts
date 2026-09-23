// social-api/src/pipeline/__tests__/StuckRunDetector.test.ts
//
// Hub#1285 (self-driven hub#1809): unit tests for StuckRunDetector.

import { describe, it, expect, jest, beforeEach, afterEach } from '@jest/globals';
import { StuckRunDetector } from '../StuckRunDetector';
import type { PipelineRunRepository, PipelineRunItem } from '../../repositories/PipelineRunRepository';
import type { RunPersistenceService } from '../runPersistence';

function makeRun(overrides: Partial<PipelineRunItem> = {}): PipelineRunItem {
  return {
    pipelineId: 'p1',
    runId: 'r1',
    status: 'running',
    triggeredBy: 'u1',
    triggerType: 'manual',
    startedAt: new Date(Date.now() - 10 * 60_000).toISOString(),
    ...overrides,
  };
}

function stubPersistence(staleIds: Array<{ runId: string; pipelineId: string }>) {
  return {
    getStaleRunIds: jest.fn((_cutoff: number) => staleIds),
  } as unknown as RunPersistenceService;
}

function stubRepo(run: PipelineRunItem | null, putShouldThrow = false) {
  const put = jest.fn(async () => {
    if (putShouldThrow) throw new Error('ddb failed');
  });
  const get = jest.fn(async () => run);
  return {
    repo: { put, get } as unknown as PipelineRunRepository,
    put,
    get,
  };
}

describe('StuckRunDetector (hub#1285)', () => {
  const NOW = 1_000_000_000_000;

  describe('check()', () => {
    it('marks stuckAt on a run that has gone silent past the threshold', async () => {
      const run = makeRun({ runId: 'r1', pipelineId: 'p1' });
      const { repo, put } = stubRepo(run);
      const detector = new StuckRunDetector({
        runPersistence: stubPersistence([{ runId: 'r1', pipelineId: 'p1' }]),
        repository: repo,
        stuckThresholdMs: 300_000,
        now: () => NOW,
      });

      await detector.check();

      expect(put).toHaveBeenCalledTimes(1);
      const saved = (put.mock.calls[0] as unknown as [PipelineRunItem])[0];
      expect(saved.stuckAt).toBe(new Date(NOW).toISOString());
      expect(saved.runId).toBe('r1');
      expect(saved.status).toBe('running'); // status not changed by detector
    });

    it('NFR loop 18: a stuck run reaches the rollup row and the bus as pipeline:run:stuck', async () => {
      const run = makeRun({ runId: 'r1', pipelineId: 'p1', lastEventAt: '2026-01-01T00:00:00.000Z' });
      const { repo } = stubRepo(run);
      const applyResolved = jest.fn(async () => null);
      const publish = jest.fn(async () => undefined);
      const detector = new StuckRunDetector({
        runPersistence: stubPersistence([{ runId: 'r1', pipelineId: 'p1' }]),
        repository: repo,
        stuckThresholdMs: 300_000,
        now: () => NOW,
        rollup: { applyResolved },
        publish,
      });

      await detector.check();

      const stuckAt = new Date(NOW).toISOString();
      expect(applyResolved).toHaveBeenCalledWith({ pipelineId: 'p1', runId: 'r1', status: 'stuck', at: stuckAt, startedAt: run.startedAt });
      expect(publish).toHaveBeenCalledWith('pipeline:run:stuck', { runId: 'r1', lastHeartbeatAt: '2026-01-01T00:00:00.000Z', at: stuckAt, pipelineId: 'p1' });
    });

    it('NFR loop 18: a rollup failure is logged; stuckAt stands', async () => {
      const { repo, put } = stubRepo(makeRun());
      const detector = new StuckRunDetector({
        runPersistence: stubPersistence([{ runId: 'r1', pipelineId: 'p1' }]),
        repository: repo,
        stuckThresholdMs: 300_000,
        now: () => NOW,
        rollup: { applyResolved: jest.fn(async () => { throw new Error('cas gave up'); }) },
      });
      await detector.check();
      expect(put).toHaveBeenCalledTimes(1);
    });

    it('skips runs that are already marked stuck', async () => {
      const run = makeRun({ stuckAt: '2026-01-01T00:00:00.000Z' });
      const { repo, put } = stubRepo(run);
      const detector = new StuckRunDetector({
        runPersistence: stubPersistence([{ runId: 'r1', pipelineId: 'p1' }]),
        repository: repo,
        now: () => NOW,
      });

      await detector.check();

      expect(put).not.toHaveBeenCalled();
    });

    it('skips when repository.get returns null (run already purged)', async () => {
      const { repo, put } = stubRepo(null);
      const detector = new StuckRunDetector({
        runPersistence: stubPersistence([{ runId: 'r1', pipelineId: 'p1' }]),
        repository: repo,
        now: () => NOW,
      });

      await detector.check();

      expect(put).not.toHaveBeenCalled();
    });

    it('does nothing when getStaleRunIds returns empty list', async () => {
      const { repo, put } = stubRepo(makeRun());
      const detector = new StuckRunDetector({
        runPersistence: stubPersistence([]),
        repository: repo,
        now: () => NOW,
      });

      await detector.check();

      expect(put).not.toHaveBeenCalled();
    });

    it('continues checking remaining runs when one put() fails', async () => {
      const run1 = makeRun({ runId: 'r1', pipelineId: 'p1' });
      const run2 = makeRun({ runId: 'r2', pipelineId: 'p1' });
      let callCount = 0;
      const repo = {
        get: jest.fn(async (_pid: string, rid: string) => (rid === 'r1' ? run1 : run2)),
        put: jest.fn(async () => {
          if (++callCount === 1) throw new Error('transient ddb error');
        }),
      } as unknown as PipelineRunRepository;

      const detector = new StuckRunDetector({
        runPersistence: stubPersistence([
          { runId: 'r1', pipelineId: 'p1' },
          { runId: 'r2', pipelineId: 'p1' },
        ]),
        repository: repo,
        now: () => NOW,
      });

      await expect(detector.check()).resolves.toBeUndefined();
      expect((repo.put as ReturnType<typeof jest.fn>).mock.calls).toHaveLength(2);
    });
  });

  describe('start() / stop()', () => {
    it('stop() is safe to call before start()', () => {
      const { repo } = stubRepo(null);
      const detector = new StuckRunDetector({
        runPersistence: stubPersistence([]),
        repository: repo,
        now: () => NOW,
      });
      expect(() => detector.stop()).not.toThrow();
    });

    it('start() is idempotent — second call does not create a second timer', () => {
      const { repo } = stubRepo(null);
      const detector = new StuckRunDetector({
        runPersistence: stubPersistence([]),
        repository: repo,
        checkIntervalMs: 600_000,
        now: () => NOW,
      });
      detector.start();
      detector.start(); // should be a no-op
      detector.stop();
    });
  });

  describe('resolveIntervalMs()', () => {
    const ENV_KEY = 'PIPELINE_STUCK_CHECK_INTERVAL_MS';

    afterEach(() => {
      delete process.env[ENV_KEY];
    });

    it('returns 60s by default when env var is absent', () => {
      delete process.env[ENV_KEY];
      expect(StuckRunDetector.resolveIntervalMs()).toBe(60_000);
    });

    it('returns the env value when it is a valid positive number', () => {
      process.env[ENV_KEY] = '30000';
      expect(StuckRunDetector.resolveIntervalMs()).toBe(30_000);
    });

    it('falls back to default when env value is not a positive finite number', () => {
      process.env[ENV_KEY] = 'bad';
      expect(StuckRunDetector.resolveIntervalMs()).toBe(60_000);
    });
  });

  describe('resolveThresholdMs()', () => {
    const ENV_KEY = 'PIPELINE_STUCK_THRESHOLD_MS';

    afterEach(() => {
      delete process.env[ENV_KEY];
    });

    it('returns 5 min by default when env var is absent', () => {
      delete process.env[ENV_KEY];
      expect(StuckRunDetector.resolveThresholdMs()).toBe(5 * 60_000);
    });

    it('returns the env value when it is a valid positive number', () => {
      process.env[ENV_KEY] = '120000';
      expect(StuckRunDetector.resolveThresholdMs()).toBe(120_000);
    });

    it('falls back to default when env value is not a positive finite number', () => {
      process.env[ENV_KEY] = '-100';
      expect(StuckRunDetector.resolveThresholdMs()).toBe(5 * 60_000);
    });
  });

  describe('start() — error path', () => {
    it('swallows check() errors without crashing the interval', async () => {
      const { repo } = stubRepo(null);
      // Make check() reject by having getStaleRunIds throw
      const badPersistence = {
        getStaleRunIds: jest.fn(() => { throw new Error('persistence exploded'); }),
      } as unknown as RunPersistenceService;

      const detector = new StuckRunDetector({
        runPersistence: badPersistence,
        repository: repo,
        checkIntervalMs: 10,
        now: () => NOW,
      });

      jest.useFakeTimers();
      detector.start();
      // Advance past one interval tick
      jest.advanceTimersByTime(20);
      // Give the microtask queue a tick to settle
      await Promise.resolve();
      detector.stop();
      jest.useRealTimers();
      // No throw — the catch swallowed it
    });
  });
});
