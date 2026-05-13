// platform-api/src/ingest/__tests__/engine-schedule-leader.test.ts
//
// Phase 54.6.3b — leader-elect the IngestEngine 60s schedule timer.
// Verifies: leader path runs evaluateSchedules, non-leader path skips it,
// and engine.stop() releases the lease (and is idempotent).

import { IngestEngine, type IngestEngineDeps, type IngestEventBus, type IngestLeaderLock } from '../engine';
import type { IngestSource } from '../types';
import type { IngestSourceRepository } from '../repositories/IngestSourceRepository';
import type { IngestItemRepository } from '../repositories/IngestItemRepository';
import type { SyncCursorRepository } from '../repositories/SyncCursorRepository';
import type { TypedDocumentRepository } from '../../repositories/TypedDocumentRepository';

jest.setTimeout(15_000);

jest.mock('../adapters/adapter', () => ({ createAdapter: jest.fn() }));
jest.mock('../../pipeline/bridge-types', () => ({
    getPipelineBridge: jest.fn(() => null),
    setPipelineBridge: jest.fn(),
}));
jest.mock('../../observability/metrics', () => ({
    recordIngestItem: jest.fn(),
    recordIngestSyncDuration: jest.fn(),
    recordIngestQueueDepthDelta: jest.fn(),
    recordIngestMappingConfidence: jest.fn(),
    recordIngestDlqDepthDelta: jest.fn(),
}));

function makeSource(overrides: Partial<IngestSource> = {}): IngestSource {
    return {
        sourceId: 'src-1',
        name: 'S',
        type: 'local',
        status: 'active',
        config: { path: '/data' },
        syncSchedule: '*/5 * * * *',
        mappingRules: [],
        defaultPipelineId: null,
        lastSyncAt: null,
        lastSyncStats: null,
        createdBy: 'u',
        createdAt: '2026-01-01T00:00:00Z',
        updatedAt: '2026-01-01T00:00:00Z',
        ...overrides,
    } as IngestSource;
}

function makeSourceRepo(sources: IngestSource[]): IngestSourceRepository {
    return {
        getById: jest.fn(async (id: string) => sources.find((s) => s.sourceId === id) ?? null),
        listByStatus: jest.fn(async () => sources),
        updateSource: jest.fn(async () => {}),
    } as unknown as IngestSourceRepository;
}

function makeEventBus(): IngestEventBus {
    return { emit: jest.fn(), on: jest.fn(), off: jest.fn() };
}

function makeFakeLock(acquireReturns: boolean): IngestLeaderLock & {
    acquireMock: jest.Mock;
    renewMock: jest.Mock;
    releaseMock: jest.Mock;
} {
    const acquireMock = jest.fn(async () => acquireReturns);
    const renewMock = jest.fn(async () => {});
    const releaseMock = jest.fn(async () => {});
    return {
        acquire: acquireMock as unknown as IngestLeaderLock['acquire'],
        renew: renewMock as unknown as IngestLeaderLock['renew'],
        release: releaseMock as unknown as IngestLeaderLock['release'],
        acquireMock,
        renewMock,
        releaseMock,
    };
}

function makeDeps(over: Partial<IngestEngineDeps> = {}): IngestEngineDeps {
    return {
        sourceRepository: makeSourceRepo([]),
        itemRepository: {} as IngestItemRepository,
        cursorRepository: {} as SyncCursorRepository,
        documentRepository: {} as TypedDocumentRepository,
        eventBus: makeEventBus(),
        enqueueItem: jest.fn(async () => {}),
        ...over,
    };
}

/**
 * The engine's `evaluateSchedules` is private and only invoked from the 60s
 * timer. We exercise it via the internal `tickSchedule` path by reaching the
 * private method through a typed-any cast — keeping the test focused on the
 * gating logic rather than wall-clock timing.
 */
function tickOnce(engine: IngestEngine): Promise<void> {
    return (engine as unknown as { tickSchedule: () => Promise<void> }).tickSchedule();
}

describe('IngestEngine — leader-elect schedule timer (Phase 54.6.3b)', () => {
    beforeEach(() => {
        jest.clearAllMocks();
    });

    test('leader path: lock acquired → evaluateSchedules runs (listByStatus called)', async () => {
        const source = makeSource({ syncSchedule: null }); // null so triggerSync is not invoked
        const sourceRepo = makeSourceRepo([source]);
        const lock = makeFakeLock(true);
        const engine = new IngestEngine(makeDeps({ sourceRepository: sourceRepo, leaderLock: lock }));

        await engine.start();
        await tickOnce(engine);

        expect(lock.acquireMock).toHaveBeenCalledTimes(1);
        expect(sourceRepo.listByStatus).toHaveBeenCalledWith('active');

        await engine.stop();
    });

    test('contention path: lock NOT acquired → evaluateSchedules does NOT run', async () => {
        const sourceRepo = makeSourceRepo([makeSource()]);
        const lock = makeFakeLock(false);
        const engine = new IngestEngine(makeDeps({ sourceRepository: sourceRepo, leaderLock: lock }));

        await engine.start();
        await tickOnce(engine);

        expect(lock.acquireMock).toHaveBeenCalledTimes(1);
        expect(sourceRepo.listByStatus).not.toHaveBeenCalled();

        await engine.stop();
        // No lease ever held, so release should NOT be called on stop.
        expect(lock.releaseMock).not.toHaveBeenCalled();
    });

    test('stop() releases the lease when this replica was the leader', async () => {
        const lock = makeFakeLock(true);
        const engine = new IngestEngine(makeDeps({
            sourceRepository: makeSourceRepo([]),
            leaderLock: lock,
        }));

        await engine.start();
        await tickOnce(engine); // become leader
        await engine.stop();

        expect(lock.releaseMock).toHaveBeenCalledTimes(1);
    });

    test('stop() is idempotent — does not double-release the lease', async () => {
        const lock = makeFakeLock(true);
        const engine = new IngestEngine(makeDeps({
            sourceRepository: makeSourceRepo([]),
            leaderLock: lock,
        }));

        await engine.start();
        await tickOnce(engine);
        await engine.stop();
        await engine.stop(); // second stop must be a no-op

        expect(lock.releaseMock).toHaveBeenCalledTimes(1);
    });
});
