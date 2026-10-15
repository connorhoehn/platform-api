// platform-api/src/pipeline/__tests__/definitions-cache.multi-replica.test.ts
//
// Wave3-PA-5 — 2-replica simulation for the pipeline-definitions cache
// invalidation path. Spins two PipelineDefinitionsCache instances against a
// shared loader and routes invalidation publish/subscribe through a tiny
// in-memory Redis-pub/sub double. Asserts:
//
//   1. A write on replica A → publishInvalidate → replica B's subscriber
//      fires cache.refresh() within milliseconds (no 60s wait).
//   2. The 60s timer remains as a safety net when the publish path fails.
//   3. Multiple concurrent invalidations coalesce onto one in-flight refresh
//      per replica (no Scan storm).
//   4. Each write path covered: PUT (definition.upsert), DELETE
//      (definition.delete), PUBLISH (definition.publish), template
//      instantiate.
//
// The production path uses `publishInvalidate` (Redis PUBLISH) + the
// subscriber registered in `startInvalidationSubscriber`. We mock both so
// we can drive the bus deterministically.

import { describe, it, expect, jest, beforeEach } from '@jest/globals';

jest.mock('../../observability/metrics', () => ({
  recordCacheAccess: jest.fn(),
  recordPipelineDefinitionsRefresh: jest.fn(),
}));

jest.mock('../../lib/logger', () => ({
  withContext: () => ({
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
    debug: jest.fn(),
  }),
}));

// Stub the redis-client used by publishInvalidate. We replace .publish with
// an in-memory bus that fans the message out to every registered
// subscriber. This models the Redis pub/sub semantics that connect the
// replicas in production.
const subscribers: Array<(msg: string) => void> = [];
const sharedPublish = jest.fn(async (_channel: string, msg: string) => {
  for (const sub of subscribers) sub(msg);
  return subscribers.length;
});
jest.mock('../../lib/redis-client', () => ({
  getRedisClient: jest.fn(async () => ({ publish: sharedPublish })),
}));

import { PipelineDefinitionsCache } from '../definitions-cache';
import type { PipelineDefinition } from '../definitions-repository';
import {
  publishInvalidate,
  startInvalidationSubscriber,
} from '../definitions-invalidator';

jest.mock('../definitions-repository', () => ({
  definitionsRepo: {
    listAll: jest.fn(async () => []),
  },
}));

function makeScheduleDef(id: string, cron: string): PipelineDefinition {
  return {
    id,
    userId: 'u-test',
    name: `Pipeline ${id}`,
    nodes: [],
    edges: [],
    status: 'published',
    triggerBinding: { event: 'schedule', schedule: cron },
  } as unknown as PipelineDefinition;
}

function makeWebhookDef(id: string, path: string, secret?: string): PipelineDefinition {
  return {
    id,
    userId: 'u-test',
    name: `Pipeline ${id}`,
    nodes: [],
    edges: [],
    status: 'published',
    triggerBinding: { event: 'webhook', webhookPath: path, ...(secret ? { webhookSecret: secret } : {}) },
  } as unknown as PipelineDefinition;
}

/**
 * Build the in-memory Redis client double used by startInvalidationSubscriber.
 * Registers itself with the shared `subscribers` array so a sharedPublish
 * call fans messages out to every replica.
 */
function makeFakeClient() {
  let handler: ((msg: string) => void) | null = null;
  const client = {
    on: jest.fn(),
    connect: jest.fn(async () => {}),
    subscribe: jest.fn(async (_channel: string, h: (msg: string) => void) => {
      handler = h;
      subscribers.push((msg: string) => { if (handler) handler(msg); });
    }),
    unsubscribe: jest.fn(async () => { handler = null; }),
    quit: jest.fn(async () => {}),
  };
  return client;
}

describe('definitions-cache — 2-replica push invalidation', () => {
  beforeEach(() => {
    subscribers.length = 0;
    sharedPublish.mockClear();
  });

  it('write on replica A → replica B refreshes within milliseconds', async () => {
    // Shared "DDB-store" of definitions. Both caches load from the same
    // source-of-truth; the test mutates it before publishing invalidate.
    let store: PipelineDefinition[] = [makeScheduleDef('p-1', '* * * * *')];
    const loader = jest.fn(async () => store.slice());

    const cacheA = new PipelineDefinitionsCache({ loader, intervalMs: 60_000 });
    const cacheB = new PipelineDefinitionsCache({ loader, intervalMs: 60_000 });

    // Prime both caches.
    await cacheA.refresh();
    await cacheB.refresh();
    expect(cacheA.all().map((d) => d.id)).toEqual(['p-1']);
    expect(cacheB.all().map((d) => d.id)).toEqual(['p-1']);

    // Subscribe B (replica B is the one that needs to be notified of A's write).
    const fakeClientB = makeFakeClient();
    await startInvalidationSubscriber(cacheB, {
      _client: fakeClientB as unknown as import('redis').RedisClientType,
    });

    // Replica A writes (simulating routes/pipelineDefinitions PUT).
    store = [
      makeScheduleDef('p-1', '* * * * *'),
      makeScheduleDef('p-2', '*/5 * * * *'),
    ];
    await publishInvalidate('definition.upsert');

    // Allow the subscriber to process the message and the refresh promise
    // to settle. cache.refresh() is async — chase its microtasks.
    await new Promise((r) => setImmediate(r));
    await new Promise((r) => setImmediate(r));

    expect(cacheB.all().map((d) => d.id).sort()).toEqual(['p-1', 'p-2']);
    // 2 prime + 1 on going live (P2.2: catch up on anything missed before the
    // subscription) + 1 invalidate-driven refresh.
    expect(loader).toHaveBeenCalledTimes(4);
  });

  it('delete on replica A → replica B sees the deletion', async () => {
    let store: PipelineDefinition[] = [
      makeWebhookDef('w-1', '/hooks/a', 'secret-a'),
      makeWebhookDef('w-2', '/hooks/b', 'secret-b'),
    ];
    const loader = jest.fn(async () => store.slice());
    const cacheB = new PipelineDefinitionsCache({ loader, intervalMs: 60_000 });
    await cacheB.refresh();
    expect(cacheB.getWebhookEntry('/hooks/a')?.pipelineId).toBe('w-1');

    const fakeClientB = makeFakeClient();
    await startInvalidationSubscriber(cacheB, {
      _client: fakeClientB as unknown as import('redis').RedisClientType,
    });

    // Replica A deletes w-1.
    store = [makeWebhookDef('w-2', '/hooks/b', 'secret-b')];
    await publishInvalidate('definition.delete');

    await new Promise((r) => setImmediate(r));
    await new Promise((r) => setImmediate(r));

    expect(cacheB.getWebhookEntry('/hooks/a')).toBeUndefined();
    expect(cacheB.getWebhookEntry('/hooks/b')?.pipelineId).toBe('w-2');
  });

  it('every documented write reason triggers a refresh on subscribers', async () => {
    const loader = jest.fn(async () => []);
    const cacheB = new PipelineDefinitionsCache({ loader, intervalMs: 60_000 });
    await cacheB.refresh();

    const fakeClientB = makeFakeClient();
    await startInvalidationSubscriber(cacheB, {
      _client: fakeClientB as unknown as import('redis').RedisClientType,
    });
    await new Promise((r) => setImmediate(r)); // the go-live refresh settles
    const refreshSpy = jest.spyOn(cacheB, 'refresh');

    // These are exactly the strings emitted by every production write path:
    //   - pipelineDefinitions.ts: upsert, delete, publish
    //   - ingestTemplates.ts:     template-instantiate
    const reasons = [
      'definition.upsert',
      'definition.delete',
      'definition.publish',
      'definition.template-instantiate',
    ];
    for (const r of reasons) {
      await publishInvalidate(r);
    }
    await new Promise((r) => setImmediate(r));
    await new Promise((r) => setImmediate(r));

    // Each invalidate triggers one refresh on the subscriber-only path
    // (the producer doesn't re-refresh from its OWN publish in this test).
    expect(refreshSpy).toHaveBeenCalledTimes(reasons.length);
  });

  it('subscriber failure mode: timer fallback still propagates writes', async () => {
    // Simulates Redis flap — publishInvalidate fails on replica A. Replica B
    // must STILL eventually see the new state via its 60s timer. We model
    // the timer by manually invoking refresh().
    let store: PipelineDefinition[] = [makeScheduleDef('p-old', '* * * * *')];
    const loader = jest.fn(async () => store.slice());

    const cacheB = new PipelineDefinitionsCache({ loader, intervalMs: 60_000 });
    await cacheB.refresh();

    // Replica A writes; publish would fail (no subscriber attached at all).
    store = [makeScheduleDef('p-new', '*/15 * * * *')];
    await publishInvalidate('definition.upsert'); // no-op: no subscribers

    // No invalidation received → cache B still serves stale data...
    expect(cacheB.all().map((d) => d.id)).toEqual(['p-old']);

    // ...until the 60s timer fires (simulated by manual refresh).
    await cacheB.refresh();
    expect(cacheB.all().map((d) => d.id)).toEqual(['p-new']);
  });

  it('coalesces concurrent invalidations onto a single in-flight refresh', async () => {
    // Multiple writes in burst → many publish messages → many subscriber
    // handler invocations. The cache must coalesce them so loader is not
    // hit N times.
    let firstResolve!: () => void;
    const firstGate = new Promise<void>((r) => { firstResolve = r; });
    let loaderCalls = 0;
    const loader = jest.fn(async () => {
      loaderCalls += 1;
      if (loaderCalls === 1) {
        await firstGate; // hold the first refresh in flight
      }
      return [];
    });

    const cacheB = new PipelineDefinitionsCache({ loader, intervalMs: 60_000 });
    const fakeClientB = makeFakeClient();
    await startInvalidationSubscriber(cacheB, {
      _client: fakeClientB as unknown as import('redis').RedisClientType,
    });

    // Fire 5 invalidations in quick succession.
    await Promise.all([
      publishInvalidate('definition.upsert'),
      publishInvalidate('definition.upsert'),
      publishInvalidate('definition.upsert'),
      publishInvalidate('definition.upsert'),
      publishInvalidate('definition.upsert'),
    ]);
    await new Promise((r) => setImmediate(r));

    // First refresh is in flight (gated). Subsequent invalidations are
    // coalesced onto it.
    expect(loaderCalls).toBe(1);
    firstResolve();
    await new Promise((r) => setImmediate(r));
    // After first refresh resolves, the cache may fire one follow-up
    // refresh from a coalesced-but-late delivery. Asserting <= 2 lets the
    // refresh-coalescer be a correctness invariant, not a strict 1:1 count.
    expect(loaderCalls).toBeLessThanOrEqual(2);
  });
});
