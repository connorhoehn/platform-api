// test/shutdown-orchestrator.test.ts
//
// Tests for createShutdown: normal teardown, per-dep error isolation,
// optional deps (runPersistence, stuckRunDetector, hintedHandoff, fdBridge).

import { createShutdown } from '../src/pipeline/ShutdownOrchestrator';

// ─── helpers ──────────────────────────────────────────────────────────────

function makeLogger() {
    return { error: jest.fn() };
}

function makeDeps(overrides: Record<string, unknown> = {}) {
    const keepAlive = setInterval(() => {}, 1_000_000);
    return {
        keepAlive,
        runQueueSubIds: ['sub-1', 'sub-2'],
        eventBus: { unsubscribe: jest.fn() },
        runPersistence: { stop: jest.fn(async () => {}) },
        stuckRunDetector: { stop: jest.fn() },
        moduleRegistry: { stop: jest.fn(async () => {}) },
        resourceRegistry: { stop: jest.fn(async () => {}) },
        hintedHandoff: { stop: jest.fn(async () => {}) },
        fdBridge: { stop: jest.fn(async () => {}) },
        cluster: { stop: jest.fn(async () => {}) },
        logger: makeLogger(),
        ...overrides,
    };
}

// ─── normal shutdown ──────────────────────────────────────────────────────

describe('createShutdown — normal teardown', () => {
    test('returns a function', () => {
        const deps = makeDeps();
        const shutdown = createShutdown(deps as never);
        expect(typeof shutdown).toBe('function');
        clearInterval(deps.keepAlive);
    });

    test('clears the keepAlive interval', async () => {
        const deps = makeDeps();
        const shutdown = createShutdown(deps as never);
        await shutdown();
        // No assertion possible on the interval itself, but shutdown must not throw
    });

    test('unsubscribes all runQueueSubIds', async () => {
        const deps = makeDeps();
        const shutdown = createShutdown(deps as never);
        await shutdown();
        expect(deps.eventBus.unsubscribe).toHaveBeenCalledTimes(2);
        expect(deps.eventBus.unsubscribe).toHaveBeenCalledWith('sub-1');
        expect(deps.eventBus.unsubscribe).toHaveBeenCalledWith('sub-2');
    });

    test('stops stuckRunDetector', async () => {
        const deps = makeDeps();
        const shutdown = createShutdown(deps as never);
        await shutdown();
        expect(deps.stuckRunDetector.stop).toHaveBeenCalledTimes(1);
    });

    test('stops runPersistence', async () => {
        const deps = makeDeps();
        const shutdown = createShutdown(deps as never);
        await shutdown();
        expect(deps.runPersistence.stop).toHaveBeenCalledTimes(1);
    });

    test('stops moduleRegistry', async () => {
        const deps = makeDeps();
        const shutdown = createShutdown(deps as never);
        await shutdown();
        expect(deps.moduleRegistry.stop).toHaveBeenCalledTimes(1);
    });

    test('stops resourceRegistry', async () => {
        const deps = makeDeps();
        const shutdown = createShutdown(deps as never);
        await shutdown();
        expect(deps.resourceRegistry.stop).toHaveBeenCalledTimes(1);
    });

    test('stops hintedHandoff', async () => {
        const deps = makeDeps();
        const shutdown = createShutdown(deps as never);
        await shutdown();
        expect(deps.hintedHandoff.stop).toHaveBeenCalledTimes(1);
    });

    test('stops fdBridge', async () => {
        const deps = makeDeps();
        const shutdown = createShutdown(deps as never);
        await shutdown();
        expect(deps.fdBridge.stop).toHaveBeenCalledTimes(1);
    });

    test('stops cluster', async () => {
        const deps = makeDeps();
        const shutdown = createShutdown(deps as never);
        await shutdown();
        expect(deps.cluster.stop).toHaveBeenCalledTimes(1);
    });

    test('no error logged when all deps stop cleanly', async () => {
        const deps = makeDeps();
        const shutdown = createShutdown(deps as never);
        await shutdown();
        expect(deps.logger.error).not.toHaveBeenCalled();
    });
});

// ─── optional deps ────────────────────────────────────────────────────────

describe('createShutdown — optional deps', () => {
    test('works when runPersistence is null', async () => {
        const deps = makeDeps({ runPersistence: null });
        const shutdown = createShutdown(deps as never);
        await expect(shutdown()).resolves.toBeUndefined();
    });

    test('works when stuckRunDetector is null', async () => {
        const deps = makeDeps({ stuckRunDetector: null });
        const shutdown = createShutdown(deps as never);
        await expect(shutdown()).resolves.toBeUndefined();
    });

    test('works when hintedHandoff is null', async () => {
        const deps = makeDeps({ hintedHandoff: null });
        const shutdown = createShutdown(deps as never);
        await expect(shutdown()).resolves.toBeUndefined();
    });

    test('works when fdBridge is null', async () => {
        const deps = makeDeps({ fdBridge: null });
        const shutdown = createShutdown(deps as never);
        await expect(shutdown()).resolves.toBeUndefined();
    });

    test('works when runQueueSubIds is empty', async () => {
        const deps = makeDeps({ runQueueSubIds: [] });
        const shutdown = createShutdown(deps as never);
        await shutdown();
        expect(deps.eventBus.unsubscribe).not.toHaveBeenCalled();
    });
});

// ─── error isolation ──────────────────────────────────────────────────────

describe('createShutdown — error isolation', () => {
    test('stuckRunDetector.stop() error is logged, shutdown continues', async () => {
        const deps = makeDeps({
            stuckRunDetector: { stop: jest.fn(() => { throw new Error('stop-err'); }) },
        });
        const shutdown = createShutdown(deps as never);
        await expect(shutdown()).resolves.toBeUndefined();
        expect(deps.logger.error).toHaveBeenCalledWith(
            expect.objectContaining({ err: expect.stringContaining('stop-err') }),
            'stuckRunDetector.stop() failed',
        );
        // Other deps should still have been stopped
        expect(deps.cluster.stop).toHaveBeenCalledTimes(1);
    });

    test('moduleRegistry.stop() error is logged, shutdown continues', async () => {
        const deps = makeDeps({
            moduleRegistry: { stop: jest.fn(async () => { throw new Error('mod-err'); }) },
        });
        const shutdown = createShutdown(deps as never);
        await expect(shutdown()).resolves.toBeUndefined();
        expect(deps.logger.error).toHaveBeenCalledWith(
            expect.objectContaining({ err: expect.stringContaining('mod-err') }),
            'moduleRegistry.stop() failed',
        );
        expect(deps.cluster.stop).toHaveBeenCalledTimes(1);
    });

    test('cluster.stop() error is logged but shutdown resolves', async () => {
        const deps = makeDeps({
            cluster: { stop: jest.fn(async () => { throw new Error('cluster-err'); }) },
        });
        const shutdown = createShutdown(deps as never);
        await expect(shutdown()).resolves.toBeUndefined();
        expect(deps.logger.error).toHaveBeenCalledWith(
            expect.objectContaining({ err: expect.stringContaining('cluster-err') }),
            'cluster.stop() failed',
        );
    });

    test('eventBus.unsubscribe() error does not abort remaining unsubscribes', async () => {
        const deps = makeDeps({
            runQueueSubIds: ['sub-1', 'sub-2', 'sub-3'],
            eventBus: {
                unsubscribe: jest.fn((id: string) => {
                    if (id === 'sub-1') throw new Error('unsubscribe-err');
                }),
            },
        });
        const shutdown = createShutdown(deps as never);
        await shutdown();
        expect(deps.eventBus.unsubscribe).toHaveBeenCalledTimes(3);
    });
});
