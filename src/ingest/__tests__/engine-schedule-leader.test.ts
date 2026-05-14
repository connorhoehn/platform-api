// platform-api/src/ingest/__tests__/engine-schedule-leader.test.ts
//
// Phase 54.6.3b → M4.3a-followup: leader-elect the IngestEngine 60s
// schedule timer. After the LeasedKVLock migration, leader gating is
// expressed via DC's primitive backed by a tiny in-memory LeaseStore
// for tests. The engine still calls tickSchedule() on its 60s timer;
// the lock decides whether evaluateSchedules() runs.

import { LeasedKVLock, type LeaseStore } from 'distributed-core';
import { IngestEngine, type IngestEngineDeps, type IngestEventBus } from '../engine';
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

/**
 * Minimal in-memory LeaseStore. `firstClaimSucceeds` controls whether the
 * initial tryClaim returns true; subsequent calls (after release) reset
 * to claimable. Mirrors DC's contract closely enough for the engine's
 * leader-vs-contention test paths.
 */
function makeInMemoryLeaseStore(opts: { firstClaimSucceeds: boolean }): LeaseStore & {
    spy: { tryClaim: jest.Mock; releaseIfOwner: jest.Mock };
} {
    let claimed: { owner: string; expiresAt: number } | null = null;
    let allowFirstClaim = opts.firstClaimSucceeds;

    const tryClaim = jest.fn(async (key: string, value: { owner: string; expiresAt: number }) => {
        if (claimed && claimed.expiresAt > Date.now()) return false;
        if (!allowFirstClaim) {
            // Simulate someone else holding the lease for this contention test.
            claimed = { owner: 'other-replica', expiresAt: Date.now() + 30_000 };
            return false;
        }
        claimed = { ...value };
        return true;
    });
    const renewIfOwner = jest.fn(async (_key: string, owner: string, newExpiresAt: number) => {
        if (!claimed || claimed.owner !== owner) throw new Error('not owner');
        claimed.expiresAt = newExpiresAt;
    });
    const releaseIfOwner = jest.fn(async (_key: string, owner: string) => {
        if (claimed && claimed.owner === owner) claimed = null;
    });

    return {
        tryClaim,
        renewIfOwner,
        releaseIfOwner,
        spy: { tryClaim, releaseIfOwner },
    };
}

function makeLockWithStore(opts: { firstClaimSucceeds: boolean }): {
    lock: LeasedKVLock;
    store: ReturnType<typeof makeInMemoryLeaseStore>;
} {
    const store = makeInMemoryLeaseStore(opts);
    const lock = new LeasedKVLock({
        store,
        lockKey: 'INGEST_SCHEDULE_LOCK#evaluator-test',
        ttlMs: 30_000,
        ownerId: 'test-owner',
    });
    return { lock, store };
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
 * `tickSchedule` is private; tests reach into it to drive a single tick
 * deterministically rather than waiting for the 60s timer.
 */
function tickOnce(engine: IngestEngine): Promise<void> {
    return (engine as unknown as { tickSchedule: () => Promise<void> }).tickSchedule();
}

describe('IngestEngine — leader-elect schedule timer (Phase 54.6.3b / M4.3a-followup)', () => {
    beforeEach(() => {
        jest.clearAllMocks();
    });

    test('leader path: lock acquired → evaluateSchedules runs (listByStatus called)', async () => {
        const source = makeSource({ syncSchedule: null });
        const sourceRepo = makeSourceRepo([source]);
        const { lock, store } = makeLockWithStore({ firstClaimSucceeds: true });
        const engine = new IngestEngine(makeDeps({ sourceRepository: sourceRepo, leaderLock: lock }));

        await engine.start();
        await tickOnce(engine);

        expect(store.spy.tryClaim).toHaveBeenCalledTimes(1);
        expect(sourceRepo.listByStatus).toHaveBeenCalledWith('active');

        await engine.stop();
    });

    test('contention path: lock NOT acquired → evaluateSchedules does NOT run', async () => {
        const sourceRepo = makeSourceRepo([makeSource()]);
        const { lock, store } = makeLockWithStore({ firstClaimSucceeds: false });
        const engine = new IngestEngine(makeDeps({ sourceRepository: sourceRepo, leaderLock: lock }));

        await engine.start();
        await tickOnce(engine);

        expect(store.spy.tryClaim).toHaveBeenCalledTimes(1);
        expect(sourceRepo.listByStatus).not.toHaveBeenCalled();

        await engine.stop();
        // No lease ever held, so release should NOT be called on stop.
        expect(store.spy.releaseIfOwner).not.toHaveBeenCalled();
    });

    test('stop() releases the lease when this replica was the leader', async () => {
        const { lock, store } = makeLockWithStore({ firstClaimSucceeds: true });
        const engine = new IngestEngine(makeDeps({
            sourceRepository: makeSourceRepo([]),
            leaderLock: lock,
        }));

        await engine.start();
        await tickOnce(engine);
        await engine.stop();

        expect(store.spy.releaseIfOwner).toHaveBeenCalledTimes(1);
    });

    test('stop() is idempotent — does not double-release the lease', async () => {
        const { lock, store } = makeLockWithStore({ firstClaimSucceeds: true });
        const engine = new IngestEngine(makeDeps({
            sourceRepository: makeSourceRepo([]),
            leaderLock: lock,
        }));

        await engine.start();
        await tickOnce(engine);
        await engine.stop();
        await engine.stop(); // second stop must be a no-op

        expect(store.spy.releaseIfOwner).toHaveBeenCalledTimes(1);
    });
});
