// test/scan-orphaned-runs.test.ts
//
// Tests for scanOrphanedRuns: window filtering, transition logic,
// per-row error isolation, and result counters.

jest.mock('../src/lib/logger', () => ({
    withContext: jest.fn(() => ({ info: jest.fn(), warn: jest.fn(), error: jest.fn() })),
}));

import { scanOrphanedRuns } from '../src/pipeline/scanOrphanedRuns';
import type { PipelineRunItem } from '../src/repositories/PipelineRunRepository';

// ─── helpers ──────────────────────────────────────────────────────────────

const NOW = 1_700_000_000_000; // fixed clock
const WINDOW_MS = 60_000; // 1-minute window for tests

function makeRun(startedAt: string, overrides: Partial<PipelineRunItem> = {}): PipelineRunItem {
    return {
        runId: `run-${Math.random().toString(36).slice(2)}`,
        pipelineId: 'pipe-1',
        triggeredBy: 'user-1',
        status: 'running',
        startedAt,
        ...overrides,
    } as unknown as PipelineRunItem;
}

function recentStart(): string {
    return new Date(NOW - WINDOW_MS / 2).toISOString(); // within window
}

function oldStart(): string {
    return new Date(NOW - WINDOW_MS * 2).toISOString(); // outside window
}

function makeRepo(runs: PipelineRunItem[] = []) {
    return {
        listByStatus: jest.fn(async () => runs),
        put: jest.fn(async () => {}),
    };
}

// ─── no running runs ──────────────────────────────────────────────────────

describe('scanOrphanedRuns — empty result', () => {
    test('returns zeroes when no running runs found', async () => {
        const repo = makeRepo([]);
        const result = await scanOrphanedRuns({ repository: repo as never, resumeWindowMs: WINDOW_MS, now: () => NOW });
        expect(result).toEqual({ scanned: 0, transitioned: 0, skippedOutsideWindow: 0 });
        expect(repo.put).not.toHaveBeenCalled();
    });
});

// ─── window filtering ─────────────────────────────────────────────────────

describe('scanOrphanedRuns — window filtering', () => {
    test('transitions run within the resume window', async () => {
        const run = makeRun(recentStart());
        const repo = makeRepo([run]);
        const result = await scanOrphanedRuns({ repository: repo as never, resumeWindowMs: WINDOW_MS, now: () => NOW });
        expect(result.transitioned).toBe(1);
        expect(result.skippedOutsideWindow).toBe(0);
        expect(repo.put).toHaveBeenCalledTimes(1);
    });

    test('skips run older than resume window', async () => {
        const run = makeRun(oldStart());
        const repo = makeRepo([run]);
        const result = await scanOrphanedRuns({ repository: repo as never, resumeWindowMs: WINDOW_MS, now: () => NOW });
        expect(result.transitioned).toBe(0);
        expect(result.skippedOutsideWindow).toBe(1);
        expect(repo.put).not.toHaveBeenCalled();
    });

    test('mixed runs: recent transitioned, old skipped', async () => {
        const recent = makeRun(recentStart());
        const old = makeRun(oldStart());
        const repo = makeRepo([recent, old]);
        const result = await scanOrphanedRuns({ repository: repo as never, resumeWindowMs: WINDOW_MS, now: () => NOW });
        expect(result.scanned).toBe(2);
        expect(result.transitioned).toBe(1);
        expect(result.skippedOutsideWindow).toBe(1);
    });

    test('run with startedAt exactly at the cutoff is skipped (< not <=)', async () => {
        const cutoffMs = NOW - WINDOW_MS;
        const run = makeRun(new Date(cutoffMs - 1).toISOString());
        const repo = makeRepo([run]);
        const result = await scanOrphanedRuns({ repository: repo as never, resumeWindowMs: WINDOW_MS, now: () => NOW });
        expect(result.skippedOutsideWindow).toBe(1);
    });
});

// ─── transition logic ─────────────────────────────────────────────────────

describe('scanOrphanedRuns — transition logic', () => {
    test('puts updated run with status=interrupted and interruptedAt', async () => {
        const run = makeRun(recentStart());
        const repo = makeRepo([run]);
        await scanOrphanedRuns({ repository: repo as never, resumeWindowMs: WINDOW_MS, now: () => NOW });
        const saved = (repo.put.mock as unknown as { calls: unknown[][] }).calls[0][0] as PipelineRunItem;
        expect(saved.status).toBe('interrupted');
        expect(saved.interruptedAt).toBe(new Date(NOW).toISOString());
        expect(saved.runId).toBe(run.runId);
    });

    test('preserves other fields on the run', async () => {
        const run = makeRun(recentStart(), { pipelineId: 'pipe-x', triggeredBy: 'user-x' });
        const repo = makeRepo([run]);
        await scanOrphanedRuns({ repository: repo as never, resumeWindowMs: WINDOW_MS, now: () => NOW });
        const saved = (repo.put.mock as unknown as { calls: unknown[][] }).calls[0][0] as PipelineRunItem;
        expect(saved.pipelineId).toBe('pipe-x');
        expect(saved.triggeredBy).toBe('user-x');
    });
});

// ─── error handling ───────────────────────────────────────────────────────

describe('scanOrphanedRuns — error handling', () => {
    test('listByStatus error returns early with zeroes', async () => {
        const repo = {
            listByStatus: jest.fn(async () => { throw new Error('DDB unavailable'); }),
            put: jest.fn(async () => {}),
        };
        const result = await scanOrphanedRuns({ repository: repo as never, resumeWindowMs: WINDOW_MS, now: () => NOW });
        expect(result).toEqual({ scanned: 0, transitioned: 0, skippedOutsideWindow: 0 });
    });

    test('put error for one row does not abort remaining rows', async () => {
        const run1 = makeRun(recentStart());
        const run2 = makeRun(recentStart());
        let putCount = 0;
        const repo = {
            listByStatus: jest.fn(async () => [run1, run2]),
            put: jest.fn(async (_item: PipelineRunItem) => {
                putCount++;
                if (putCount === 1) throw new Error('first row fails');
            }),
        };
        const result = await scanOrphanedRuns({ repository: repo as never, resumeWindowMs: WINDOW_MS, now: () => NOW });
        // Only run2 succeeded
        expect(result.transitioned).toBe(1);
        expect(result.scanned).toBe(2);
    });

    test('run with invalid startedAt is skipped without throwing', async () => {
        const run = makeRun('not-a-date');
        const repo = makeRepo([run]);
        const result = await scanOrphanedRuns({ repository: repo as never, resumeWindowMs: WINDOW_MS, now: () => NOW });
        expect(result.transitioned).toBe(0);
        expect(repo.put).not.toHaveBeenCalled();
    });
});

// ─── defaults ─────────────────────────────────────────────────────────────

describe('scanOrphanedRuns — defaults', () => {
    test('uses 4-hour default window when resumeWindowMs not provided', async () => {
        const fourHoursAgo = new Date(Date.now() - 3 * 60 * 60 * 1000).toISOString(); // 3h ago → within 4h window
        const run = makeRun(fourHoursAgo);
        const repo = makeRepo([run]);
        const result = await scanOrphanedRuns({ repository: repo as never });
        expect(result.transitioned).toBe(1);
    });
});
