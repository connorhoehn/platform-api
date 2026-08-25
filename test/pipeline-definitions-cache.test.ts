// test/pipeline-definitions-cache.test.ts
//
// Tests for PipelineDefinitionsCache: snapshot management, periodic refresh,
// coalescing, webhook index, isPrimed, and __setSnapshotForTests helper.

jest.mock('../src/lib/logger', () => ({
    withContext: jest.fn(() => ({ info: jest.fn(), warn: jest.fn(), error: jest.fn() })),
}));

jest.mock('../src/observability/metrics', () => ({
    recordCacheAccess: jest.fn(),
    recordPipelineDefinitionsRefresh: jest.fn(),
}));

jest.mock('../src/pipeline/definitions-repository', () => ({
    definitionsRepo: { listAll: jest.fn(async () => []) },
}));

import { PipelineDefinitionsCache } from '../src/pipeline/definitions-cache';
import type { PipelineDefinition } from '../src/pipeline/definitions-repository';
import { recordCacheAccess } from '../src/observability/metrics';

// ─── helpers ──────────────────────────────────────────────────────────────

type PartialDef = Partial<PipelineDefinition> & { id: string };

function makeDef(overrides: PartialDef): PipelineDefinition {
    return { ...overrides } as unknown as PipelineDefinition;
}

function makeWebhookDef(id: string, webhookPath: string, secret?: string): PipelineDefinition {
    return {
        id,
        triggerBinding: {
            event: 'webhook',
            webhookPath,
            ...(secret ? { webhookSecret: secret } : {}),
        },
    } as unknown as PipelineDefinition;
}

function makeLoader(items: PipelineDefinition[] = []) {
    return jest.fn(async () => items);
}

// ─── start / stop ─────────────────────────────────────────────────────────

describe('PipelineDefinitionsCache — start/stop', () => {
    test('all() returns empty before start', () => {
        const cache = new PipelineDefinitionsCache({ loader: makeLoader([]) });
        expect(cache.all()).toEqual([]);
    });

    test('start() primes the cache with loader results', async () => {
        const def = makeDef({ id: 'p-1' });
        const cache = new PipelineDefinitionsCache({ loader: makeLoader([def]) });
        await cache.start();
        expect(cache.all()).toHaveLength(1);
        expect(cache.all()[0]).toEqual(def);
        cache.stop();
    });

    test('isPrimed() is false before start and true after', async () => {
        const cache = new PipelineDefinitionsCache({ loader: makeLoader([]) });
        expect(cache.isPrimed()).toBe(false);
        await cache.start();
        expect(cache.isPrimed()).toBe(true);
        cache.stop();
    });

    test('start() is a no-op when already started', async () => {
        const loader = makeLoader([]);
        const cache = new PipelineDefinitionsCache({ loader });
        await cache.start();
        await cache.start();
        expect(loader).toHaveBeenCalledTimes(2); // first start + second triggers refresh
        cache.stop();
    });

    test('stop() is safe to call when not started', () => {
        const cache = new PipelineDefinitionsCache({ loader: makeLoader([]) });
        expect(() => cache.stop()).not.toThrow();
    });

    test('stop() is idempotent', async () => {
        const cache = new PipelineDefinitionsCache({ loader: makeLoader([]) });
        await cache.start();
        cache.stop();
        expect(() => cache.stop()).not.toThrow();
    });
});

// ─── refresh() coalescing ─────────────────────────────────────────────────

describe('PipelineDefinitionsCache — refresh() coalescing', () => {
    test('concurrent refresh() calls share the same in-flight promise, then run ONE follow-up', async () => {
        const loader = makeLoader([]);
        const cache = new PipelineDefinitionsCache({ loader });
        // Fire three refreshes without awaiting between them.
        const p1 = cache.refresh();
        const p2 = cache.refresh();
        const p3 = cache.refresh();
        await Promise.all([p1, p2, p3]);
        // Wave A contract change: callers coalesce onto one in-flight Scan
        // (so a burst can't storm DDB), but a call that lands mid-flight
        // marks the cache dirty and exactly ONE follow-up load runs after —
        // the in-flight Scan's snapshot predates whatever write prompted
        // the late caller. Silently swallowing it left a freshly published
        // pipeline invisible to the event dispatcher until the 60s timer.
        await new Promise((r) => setTimeout(r, 10));
        expect(loader).toHaveBeenCalledTimes(2);
        cache.stop();
    });

    test('a write during an in-flight refresh becomes visible via the follow-up', async () => {
        const oldDef = makeDef({ id: 'p-old' });
        const newDef = makeDef({ id: 'p-new' });
        let store: PipelineDefinition[] = [oldDef];
        let releaseFirst!: () => void;
        const firstGate = new Promise<void>((r) => { releaseFirst = r; });
        let calls = 0;
        const loader = jest.fn(async () => {
            calls++;
            // First Scan snapshots BEFORE the write and hangs until released.
            const snapshot = [...store];
            if (calls === 1) await firstGate;
            return snapshot;
        });
        const cache = new PipelineDefinitionsCache({ loader });

        const first = cache.refresh();          // Scan starts, snapshots [p-old]
        store = [oldDef, newDef];               // the "publish" lands mid-Scan
        const second = cache.refresh();         // poked by the publish route
        releaseFirst();
        await Promise.all([first, second]);
        // Let the chained follow-up refresh settle.
        await new Promise((r) => setTimeout(r, 10));

        expect(loader).toHaveBeenCalledTimes(2);
        expect(cache.all().map((d) => (d as { id?: string }).id)).toContain('p-new');
        cache.stop();
    });

    test('sequential refresh() calls each invoke the loader', async () => {
        const loader = makeLoader([]);
        const cache = new PipelineDefinitionsCache({ loader });
        await cache.refresh();
        await cache.refresh();
        expect(loader).toHaveBeenCalledTimes(2);
        cache.stop();
    });

    test('loader error keeps previous snapshot and still sets primed', async () => {
        const def = makeDef({ id: 'p-1' });
        const loader = jest.fn()
            .mockResolvedValueOnce([def])
            .mockRejectedValueOnce(new Error('DDB down'));
        const cache = new PipelineDefinitionsCache({ loader });
        await cache.refresh();
        expect(cache.all()).toHaveLength(1);
        // second refresh fails but should not throw for outer caller
        await expect(cache.refresh()).rejects.toThrow('DDB down');
        // snapshot preserved
        expect(cache.all()).toHaveLength(1);
        expect(cache.isPrimed()).toBe(true);
        cache.stop();
    });
});

// ─── all() ────────────────────────────────────────────────────────────────

describe('PipelineDefinitionsCache — all()', () => {
    test('returns snapshot after refresh', async () => {
        const defs = [makeDef({ id: 'a' }), makeDef({ id: 'b' })];
        const cache = new PipelineDefinitionsCache({ loader: makeLoader(defs) });
        await cache.refresh();
        expect(cache.all()).toHaveLength(2);
        cache.stop();
    });

    test('returns updated snapshot after second refresh', async () => {
        const first = [makeDef({ id: 'a' })];
        const second = [makeDef({ id: 'a' }), makeDef({ id: 'b' })];
        const loader = jest.fn().mockResolvedValueOnce(first).mockResolvedValueOnce(second);
        const cache = new PipelineDefinitionsCache({ loader });
        await cache.refresh();
        expect(cache.all()).toHaveLength(1);
        await cache.refresh();
        expect(cache.all()).toHaveLength(2);
        cache.stop();
    });
});

// ─── getWebhookEntry() ────────────────────────────────────────────────────

describe('PipelineDefinitionsCache — getWebhookEntry()', () => {
    test('returns undefined for unknown path', async () => {
        const cache = new PipelineDefinitionsCache({ loader: makeLoader([]) });
        await cache.refresh();
        expect(cache.getWebhookEntry('/unknown')).toBeUndefined();
        cache.stop();
    });

    test('returns entry for known webhook path', async () => {
        const def = makeWebhookDef('p-wh', '/hooks/my-webhook');
        const cache = new PipelineDefinitionsCache({ loader: makeLoader([def]) });
        await cache.refresh();
        const entry = cache.getWebhookEntry('/hooks/my-webhook');
        expect(entry).toBeDefined();
        expect(entry?.pipelineId).toBe('p-wh');
        cache.stop();
    });

    test('includes secret when present', async () => {
        const def = makeWebhookDef('p-sec', '/hooks/secure', 'my-secret');
        const cache = new PipelineDefinitionsCache({ loader: makeLoader([def]) });
        await cache.refresh();
        const entry = cache.getWebhookEntry('/hooks/secure');
        expect(entry?.secret).toBe('my-secret');
        cache.stop();
    });

    test('omits secret field when secret is empty string', async () => {
        const def = {
            id: 'p-nosec',
            triggerBinding: { event: 'webhook', webhookPath: '/hooks/nosec', webhookSecret: '' },
        } as unknown as PipelineDefinition;
        const cache = new PipelineDefinitionsCache({ loader: makeLoader([def]) });
        await cache.refresh();
        const entry = cache.getWebhookEntry('/hooks/nosec');
        expect(entry?.secret).toBeUndefined();
        cache.stop();
    });

    test('non-webhook trigger pipelines are not indexed', async () => {
        const def = {
            id: 'p-cron',
            triggerBinding: { event: 'cron', webhookPath: '/hooks/cron' },
        } as unknown as PipelineDefinition;
        const cache = new PipelineDefinitionsCache({ loader: makeLoader([def]) });
        await cache.refresh();
        expect(cache.getWebhookEntry('/hooks/cron')).toBeUndefined();
        cache.stop();
    });

    test('calls recordCacheAccess with hit=true on match', async () => {
        const def = makeWebhookDef('p-hit', '/hooks/hit');
        const cache = new PipelineDefinitionsCache({ loader: makeLoader([def]) });
        await cache.refresh();
        (recordCacheAccess as jest.Mock).mockClear();
        cache.getWebhookEntry('/hooks/hit');
        expect(recordCacheAccess).toHaveBeenCalledWith('definitions', true);
        cache.stop();
    });

    test('calls recordCacheAccess with hit=false on miss', async () => {
        const cache = new PipelineDefinitionsCache({ loader: makeLoader([]) });
        await cache.refresh();
        (recordCacheAccess as jest.Mock).mockClear();
        cache.getWebhookEntry('/hooks/miss');
        expect(recordCacheAccess).toHaveBeenCalledWith('definitions', false);
        cache.stop();
    });
});

// ─── __setSnapshotForTests ────────────────────────────────────────────────

describe('PipelineDefinitionsCache — __setSnapshotForTests', () => {
    test('sets snapshot and marks cache primed without calling loader', () => {
        const loader = makeLoader([]);
        const cache = new PipelineDefinitionsCache({ loader });
        const defs = [makeDef({ id: 'injected' })];
        cache.__setSnapshotForTests(defs);
        expect(cache.all()).toHaveLength(1);
        expect(cache.isPrimed()).toBe(true);
        expect(loader).not.toHaveBeenCalled();
    });

    test('rebuilds webhook index when snapshot is set', () => {
        const cache = new PipelineDefinitionsCache({ loader: makeLoader([]) });
        cache.__setSnapshotForTests([makeWebhookDef('p-test', '/hooks/test')]);
        const entry = cache.getWebhookEntry('/hooks/test');
        expect(entry?.pipelineId).toBe('p-test');
    });
});
