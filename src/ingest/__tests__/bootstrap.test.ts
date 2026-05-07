// social-api/src/ingest/__tests__/bootstrap.test.ts
//
// Integration tests for bootstrapIngest: returned handle shape, idempotent
// producer deduplication, DLQ wiring, createSession per-source identity,
// and shutdown teardown.

import { wrap } from 'distributed-core';
import { bootstrapIngest, type IngestBootstrapDeps } from '../bootstrap';
import type { IngestSourceRepository } from '../repositories/IngestSourceRepository';
import type { IngestItemRepository } from '../repositories/IngestItemRepository';
import type { SyncCursorRepository } from '../repositories/SyncCursorRepository';
import type { TypedDocumentRepository } from '../../repositories/TypedDocumentRepository';

// ─── Minimal stub deps (not exercised by bootstrap itself) ────────────────

function makeDeps(): IngestBootstrapDeps {
    return {
        sourceRepository: {} as IngestSourceRepository,
        itemRepository: {} as IngestItemRepository,
        cursorRepository: {} as SyncCursorRepository,
        documentRepository: {} as TypedDocumentRepository,
    };
}

// ─── handle shape ─────────────────────────────────────────────────────────

describe('bootstrapIngest — returned handle', () => {
    test('returns all required handle properties', async () => {
        const handle = await bootstrapIngest(makeDeps());
        expect(typeof handle.syncQueue).toBe('object');
        expect(typeof handle.ingestQueue).toBe('object');
        expect(typeof handle.syncProducer).toBe('object');
        expect(typeof handle.ingestProducer).toBe('object');
        expect(typeof handle.eventBus).toBe('object');
        expect(typeof handle.dlq).toBe('object');
        expect(typeof handle.createSession).toBe('function');
        expect(typeof handle.shutdown).toBe('function');
        await handle.shutdown();
    });

    test('syncProducer has send method', async () => {
        const handle = await bootstrapIngest(makeDeps());
        expect(typeof handle.syncProducer.send).toBe('function');
        await handle.shutdown();
    });

    test('dlq has put and list methods', async () => {
        const handle = await bootstrapIngest(makeDeps());
        expect(typeof handle.dlq.put).toBe('function');
        expect(typeof handle.dlq.list).toBe('function');
        await handle.shutdown();
    });
});

// ─── syncProducer deduplication ───────────────────────────────────────────

describe('bootstrapIngest — syncProducer deduplication', () => {
    test('sending same dedupKey twice deduplicates within window', async () => {
        const handle = await bootstrapIngest(makeDeps(), { syncDedupWindowMs: 60_000 });
        const task = { sourceId: 'src-1', triggeredBy: 'manual' as const, requestedAt: new Date().toISOString() };
        const env1 = wrap(task, { dedupKey: task.sourceId });
        const env2 = wrap(task, { dedupKey: task.sourceId });
        const r1 = await handle.syncProducer.send(env1);
        const r2 = await handle.syncProducer.send(env2);
        expect(r1.accepted).toBe(true);
        expect(r2.accepted).toBe(false);
        await handle.shutdown();
    });

    test('different sourceIds are not deduplicated', async () => {
        const handle = await bootstrapIngest(makeDeps());
        const t1 = { sourceId: 'src-1', triggeredBy: 'manual' as const, requestedAt: new Date().toISOString() };
        const t2 = { sourceId: 'src-2', triggeredBy: 'manual' as const, requestedAt: new Date().toISOString() };
        const r1 = await handle.syncProducer.send(wrap(t1, { dedupKey: t1.sourceId }));
        const r2 = await handle.syncProducer.send(wrap(t2, { dedupKey: t2.sourceId }));
        expect(r1.accepted).toBe(true);
        expect(r2.accepted).toBe(true);
        await handle.shutdown();
    });
});

// ─── createSession ────────────────────────────────────────────────────────

describe('bootstrapIngest — createSession', () => {
    test('returns a lock object with acquire/release methods', async () => {
        const handle = await bootstrapIngest(makeDeps());
        const lock = handle.createSession('src-1');
        expect(typeof lock.acquire).toBe('function');
        expect(typeof lock.release).toBe('function');
        await handle.shutdown();
    });

    test('acquire returns true when uncontested', async () => {
        const handle = await bootstrapIngest(makeDeps());
        const lock = handle.createSession('src-1');
        const acquired = await lock.acquire();
        expect(acquired).toBe(true);
        await lock.release();
        await handle.shutdown();
    });

    test('acquire returns false when already held', async () => {
        const handle = await bootstrapIngest(makeDeps());
        const lock = handle.createSession('src-1');
        await lock.acquire();
        const second = await lock.acquire();
        expect(second).toBe(false);
        await lock.release();
        await handle.shutdown();
    });

    test('same sourceId returns separate lock instances sharing state', async () => {
        const handle = await bootstrapIngest(makeDeps());
        const l1 = handle.createSession('src-1');
        await l1.acquire();
        // A second call for same sourceId sees the lock as held
        const l2 = handle.createSession('src-1');
        const second = await l2.acquire();
        expect(second).toBe(false);
        await l1.release();
        await handle.shutdown();
    });

    test('different sourceIds have independent locks', async () => {
        const handle = await bootstrapIngest(makeDeps());
        const l1 = handle.createSession('src-1');
        const l2 = handle.createSession('src-2');
        await l1.acquire();
        const r2 = await l2.acquire();
        expect(r2).toBe(true);
        await l1.release();
        await l2.release();
        await handle.shutdown();
    });
});

// ─── shutdown ─────────────────────────────────────────────────────────────

describe('bootstrapIngest — shutdown', () => {
    test('resolves without throwing', async () => {
        const handle = await bootstrapIngest(makeDeps());
        await expect(handle.shutdown()).resolves.toBeUndefined();
    });

    test('second shutdown call does not throw', async () => {
        const handle = await bootstrapIngest(makeDeps());
        await handle.shutdown();
        await expect(handle.shutdown()).resolves.toBeUndefined();
    });
});

// ─── options ──────────────────────────────────────────────────────────────

describe('bootstrapIngest — options', () => {
    test('accepts custom window and dlq options without throwing', async () => {
        const handle = await bootstrapIngest(makeDeps(), {
            syncDedupWindowMs: 1000,
            ingestDedupWindowMs: 5000,
            dlqCapacity: 5,
        });
        expect(handle.syncQueue).toBeDefined();
        await handle.shutdown();
    });
});
