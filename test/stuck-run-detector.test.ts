// test/stuck-run-detector.test.ts
//
// Tests for StuckRunDetector: env-var config resolution, start/stop lifecycle,
// and check() logic (marks stuck, skips already-stuck, handles repo errors).

jest.mock('../src/lib/logger', () => ({
    withContext: jest.fn(() => ({ info: jest.fn(), warn: jest.fn(), error: jest.fn() })),
}));

import { StuckRunDetector } from '../src/pipeline/StuckRunDetector';

// ─── helpers ──────────────────────────────────────────────────────────────

const ORIG_CHECK = process.env.PIPELINE_STUCK_CHECK_INTERVAL_MS;
const ORIG_THRESH = process.env.PIPELINE_STUCK_THRESHOLD_MS;

afterEach(() => {
    if (ORIG_CHECK === undefined) delete process.env.PIPELINE_STUCK_CHECK_INTERVAL_MS;
    else process.env.PIPELINE_STUCK_CHECK_INTERVAL_MS = ORIG_CHECK;
    if (ORIG_THRESH === undefined) delete process.env.PIPELINE_STUCK_THRESHOLD_MS;
    else process.env.PIPELINE_STUCK_THRESHOLD_MS = ORIG_THRESH;
});

interface FakeRun {
    runId: string;
    pipelineId: string;
    stuckAt?: string;
}

function makeRunPersistence(staleIds: { runId: string; pipelineId: string }[] = []) {
    return { getStaleRunIds: jest.fn(() => staleIds) };
}

function makeRepo(existing: FakeRun | null = null) {
    return {
        get: jest.fn(async () => existing),
        put: jest.fn(async () => {}),
    };
}

function makeDetector(opts: {
    stale?: { runId: string; pipelineId: string }[];
    existing?: FakeRun | null;
    threshold?: number;
    now?: () => number;
} = {}) {
    return new StuckRunDetector({
        runPersistence: makeRunPersistence(opts.stale ?? []) as never,
        repository: makeRepo(opts.existing) as never,
        stuckThresholdMs: opts.threshold ?? 5_000,
        now: opts.now,
    });
}

// ─── resolveIntervalMs ────────────────────────────────────────────────────

describe('StuckRunDetector.resolveIntervalMs', () => {
    test('defaults to 60000 when env not set', () => {
        delete process.env.PIPELINE_STUCK_CHECK_INTERVAL_MS;
        expect(StuckRunDetector.resolveIntervalMs()).toBe(60_000);
    });

    test('uses valid env value', () => {
        process.env.PIPELINE_STUCK_CHECK_INTERVAL_MS = '30000';
        expect(StuckRunDetector.resolveIntervalMs()).toBe(30_000);
    });

    test('falls back to default for non-numeric env value', () => {
        process.env.PIPELINE_STUCK_CHECK_INTERVAL_MS = 'invalid';
        expect(StuckRunDetector.resolveIntervalMs()).toBe(60_000);
    });

    test('falls back to default for zero', () => {
        process.env.PIPELINE_STUCK_CHECK_INTERVAL_MS = '0';
        expect(StuckRunDetector.resolveIntervalMs()).toBe(60_000);
    });

    test('falls back to default for negative value', () => {
        process.env.PIPELINE_STUCK_CHECK_INTERVAL_MS = '-1000';
        expect(StuckRunDetector.resolveIntervalMs()).toBe(60_000);
    });
});

// ─── resolveThresholdMs ───────────────────────────────────────────────────

describe('StuckRunDetector.resolveThresholdMs', () => {
    test('defaults to 5 minutes when env not set', () => {
        delete process.env.PIPELINE_STUCK_THRESHOLD_MS;
        expect(StuckRunDetector.resolveThresholdMs()).toBe(5 * 60_000);
    });

    test('uses valid env value', () => {
        process.env.PIPELINE_STUCK_THRESHOLD_MS = '120000';
        expect(StuckRunDetector.resolveThresholdMs()).toBe(120_000);
    });

    test('falls back to default for invalid env value', () => {
        process.env.PIPELINE_STUCK_THRESHOLD_MS = 'bad';
        expect(StuckRunDetector.resolveThresholdMs()).toBe(5 * 60_000);
    });
});

// ─── start / stop ─────────────────────────────────────────────────────────

describe('StuckRunDetector start/stop', () => {
    test('start() sets up an interval that does not hold the process open', () => {
        const detector = makeDetector();
        detector.start();
        detector.stop();
    });

    test('start() is a no-op when already started', () => {
        const detector = makeDetector();
        detector.start();
        detector.start(); // should not throw or create a second timer
        detector.stop();
    });

    test('stop() is safe to call when not started', () => {
        const detector = makeDetector();
        expect(() => detector.stop()).not.toThrow();
    });

    test('stop() is idempotent', () => {
        const detector = makeDetector();
        detector.start();
        detector.stop();
        expect(() => detector.stop()).not.toThrow();
    });
});

// ─── check() ──────────────────────────────────────────────────────────────

describe('StuckRunDetector check()', () => {
    test('does nothing when no stale runs', async () => {
        const repo = makeRepo(null);
        const detector = new StuckRunDetector({
            runPersistence: makeRunPersistence([]) as never,
            repository: repo as never,
            stuckThresholdMs: 5_000,
        });
        await detector.check();
        expect(repo.get).not.toHaveBeenCalled();
        expect(repo.put).not.toHaveBeenCalled();
    });

    test('marks a stale run stuck', async () => {
        const existing: FakeRun = { runId: 'run-1', pipelineId: 'pipe-1' };
        const repo = makeRepo(existing);
        const now = 1_700_000_000_000;
        const detector = new StuckRunDetector({
            runPersistence: makeRunPersistence([{ runId: 'run-1', pipelineId: 'pipe-1' }]) as never,
            repository: repo as never,
            stuckThresholdMs: 5_000,
            now: () => now,
        });
        await detector.check();
        expect(repo.put).toHaveBeenCalledTimes(1);
        const saved = (repo.put.mock as unknown as { calls: unknown[][] }).calls[0][0] as FakeRun;
        expect(saved.stuckAt).toBe(new Date(now).toISOString());
    });

    test('skips run that is already marked stuck', async () => {
        const existing: FakeRun = { runId: 'run-2', pipelineId: 'pipe-1', stuckAt: '2026-01-01T00:00:00Z' };
        const repo = makeRepo(existing);
        const detector = new StuckRunDetector({
            runPersistence: makeRunPersistence([{ runId: 'run-2', pipelineId: 'pipe-1' }]) as never,
            repository: repo as never,
            stuckThresholdMs: 5_000,
        });
        await detector.check();
        expect(repo.put).not.toHaveBeenCalled();
    });

    test('skips run when repo.get returns null (run gone)', async () => {
        const repo = makeRepo(null);
        const detector = new StuckRunDetector({
            runPersistence: makeRunPersistence([{ runId: 'run-3', pipelineId: 'pipe-1' }]) as never,
            repository: repo as never,
            stuckThresholdMs: 5_000,
        });
        await detector.check();
        expect(repo.put).not.toHaveBeenCalled();
    });

    test('repo error does not propagate — continues to next run', async () => {
        const run1: FakeRun = { runId: 'run-1', pipelineId: 'pipe-1' };
        const run2: FakeRun = { runId: 'run-2', pipelineId: 'pipe-2' };
        const getCount = { n: 0 };
        const repo = {
            get: jest.fn(async (_pid: string, rid: string) => {
                getCount.n++;
                if (rid === 'run-1') throw new Error('DDB error');
                return run2;
            }),
            put: jest.fn(async () => {}),
        };
        const detector = new StuckRunDetector({
            runPersistence: makeRunPersistence([
                { runId: 'run-1', pipelineId: 'pipe-1' },
                { runId: 'run-2', pipelineId: 'pipe-2' },
            ]) as never,
            repository: repo as never,
            stuckThresholdMs: 5_000,
        });
        await expect(detector.check()).resolves.toBeUndefined();
        // run-2 should still have been processed
        expect(repo.put).toHaveBeenCalledTimes(1);
        const saved = (repo.put.mock as unknown as { calls: unknown[][] }).calls[0][0] as FakeRun;
        expect(saved.runId).toBe('run-2');
    });

    test('uses injected now() for the cutoff', async () => {
        const existing: FakeRun = { runId: 'run-1', pipelineId: 'pipe-1' };
        const runPersistence = makeRunPersistence([{ runId: 'run-1', pipelineId: 'pipe-1' }]);
        const repo = makeRepo(existing);
        const fixedNow = 9_999_999_999_999;
        const detector = new StuckRunDetector({
            runPersistence: runPersistence as never,
            repository: repo as never,
            stuckThresholdMs: 1_000,
            now: () => fixedNow,
        });
        await detector.check();
        const cutoffArg = (runPersistence.getStaleRunIds.mock as unknown as { calls: unknown[][] }).calls[0][0] as number;
        expect(cutoffArg).toBe(fixedNow - 1_000);
    });
});
