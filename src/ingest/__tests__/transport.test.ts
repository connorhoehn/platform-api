// platform-api/src/ingest/__tests__/transport.test.ts
//
// Wave2-PA-2 — covers both transport modes (`memory` and `mq`) of the
// ingest item handoff. The transport selector replaces the previously
// hardcoded publishDurable+catalog.subscribe wiring with a swappable
// abstraction; the safe-rollout default is `memory`.

import {
  createIngestTransport,
  createMemoryIngestTransport,
  createMqIngestTransport,
  resolveIngestTransportMode,
} from '../transport';
import type { IngestEngine } from '../engine';
import type {
  EventEnvelope,
  EventHandler,
  Subscription,
} from '@connorhoehn/event-catalog';

// Silence logger
jest.mock('../../lib/logger', () => ({
  __esModule: true,
  default: {
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
    debug: jest.fn(),
  },
}));

// Stub the catalog import — we never want a real Redis/DDB connection here.
// The MQ-mode tests inject their own fake via the `_catalog` + `_publishDurable`
// seams, so the module-level mock only needs `subscribe` + `publishDurable` to
// exist as no-ops.
jest.mock('../../lib/event-catalog', () => ({
  catalog: {
    subscribe: jest.fn(),
  },
  publishDurable: jest.fn(async () => {}),
}));

// ─── Helpers ───────────────────────────────────────────────────────────────

function makeFakeEngine(): IngestEngine & {
  processed: Array<{ ingestId: string; tags?: string[] }>;
} {
  const processed: Array<{ ingestId: string; tags?: string[] }> = [];
  return {
    processItem: jest.fn(async (ingestId: string, tags?: string[]) => {
      processed.push({ ingestId, tags });
    }),
    processed,
  } as unknown as IngestEngine & {
    processed: Array<{ ingestId: string; tags?: string[] }>;
  };
}

// Minimal fake catalog mirroring the shape used by engine-partitioned.test.ts.
function makeFakeCatalog() {
  let handler: EventHandler<unknown> | null = null;
  const subscribed: string[] = [];
  return {
    subscribed,
    isHandlerRegistered: () => handler !== null,
    subscribe: jest.fn(
      <T>(name: string, h: EventHandler<T>): Subscription => {
        subscribed.push(name);
        handler = h as EventHandler<unknown>;
        return {
          unsubscribe: async () => { handler = null; },
          pause: () => {},
          resume: () => {},
          isPaused: () => false,
        };
      },
    ),
    async deliver(payload: Record<string, unknown>): Promise<void> {
      if (!handler) throw new Error('no handler registered');
      const envelope = {
        payload,
        metadata: { id: `env-${Math.random()}`, timestamp: new Date().toISOString() },
      } as unknown as EventEnvelope<unknown>;
      await handler(envelope);
    },
  };
}

// ─── Mode resolution ──────────────────────────────────────────────────────

describe('resolveIngestTransportMode', () => {
  test('defaults to "memory" when unset', () => {
    expect(resolveIngestTransportMode(undefined)).toBe('memory');
  });

  test('explicit "memory" → memory', () => {
    expect(resolveIngestTransportMode('memory')).toBe('memory');
  });

  test('explicit "mq" → mq', () => {
    expect(resolveIngestTransportMode('mq')).toBe('mq');
  });

  test('case-insensitive', () => {
    expect(resolveIngestTransportMode('MQ')).toBe('mq');
    expect(resolveIngestTransportMode('Memory')).toBe('memory');
  });

  test('empty string falls back to memory', () => {
    expect(resolveIngestTransportMode('')).toBe('memory');
  });

  test('unknown value falls back to memory', () => {
    expect(resolveIngestTransportMode('redis')).toBe('memory');
  });
});

// ─── Memory transport ─────────────────────────────────────────────────────

describe('createMemoryIngestTransport', () => {
  test('reports mode = memory', () => {
    const t = createMemoryIngestTransport();
    expect(t.mode).toBe('memory');
  });

  test('enqueueItem dispatches to engine.processItem', async () => {
    const engine = makeFakeEngine();
    const t = createMemoryIngestTransport();
    const consumer = t.startConsumer(engine);
    await t.enqueueItem({ ingestId: 'i-1', sourceId: 's-1', tags: ['a'] });
    await consumer.stop();
    expect(engine.processed).toEqual([{ ingestId: 'i-1', tags: ['a'] }]);
  });

  test('preserves FIFO per sourceId — single chain serializes', async () => {
    const engine = makeFakeEngine();
    // Slow the first processItem so the second has to queue behind it.
    const order: string[] = [];
    let firstResolve!: () => void;
    const firstStarted = new Promise<void>((res) => { firstResolve = res; });
    let firstGate!: () => void;
    const firstWait = new Promise<void>((res) => { firstGate = res; });
    (engine.processItem as jest.Mock).mockImplementation(async (id: string) => {
      order.push(`start:${id}`);
      if (id === 'i-1') {
        firstResolve();
        await firstWait;
      }
      order.push(`end:${id}`);
    });

    const t = createMemoryIngestTransport();
    const consumer = t.startConsumer(engine);

    await t.enqueueItem({ ingestId: 'i-1', sourceId: 's-1' });
    await t.enqueueItem({ ingestId: 'i-2', sourceId: 's-1' });
    await firstStarted;
    // i-2 must not have started yet
    expect(order).toEqual(['start:i-1']);
    firstGate();
    await consumer.stop();
    expect(order).toEqual(['start:i-1', 'end:i-1', 'start:i-2', 'end:i-2']);
  });

  test('different sourceIds process concurrently', async () => {
    const engine = makeFakeEngine();
    const inflight: Set<string> = new Set();
    let maxInflight = 0;
    let gateA!: () => void;
    let gateB!: () => void;
    const waitA = new Promise<void>((res) => { gateA = res; });
    const waitB = new Promise<void>((res) => { gateB = res; });
    (engine.processItem as jest.Mock).mockImplementation(async (id: string) => {
      inflight.add(id);
      maxInflight = Math.max(maxInflight, inflight.size);
      if (id === 'i-a') await waitA;
      if (id === 'i-b') await waitB;
      inflight.delete(id);
    });

    const t = createMemoryIngestTransport();
    const consumer = t.startConsumer(engine);
    await t.enqueueItem({ ingestId: 'i-a', sourceId: 's-A' });
    await t.enqueueItem({ ingestId: 'i-b', sourceId: 's-B' });
    // Let microtasks flush so both dispatches have begun.
    await new Promise((r) => setImmediate(r));
    expect(inflight.size).toBe(2);
    gateA();
    gateB();
    await consumer.stop();
    expect(maxInflight).toBe(2);
  });

  test('processItem errors do not break the chain', async () => {
    const engine = makeFakeEngine();
    (engine.processItem as jest.Mock).mockImplementationOnce(async () => {
      throw new Error('boom');
    });
    const t = createMemoryIngestTransport();
    const consumer = t.startConsumer(engine);
    await t.enqueueItem({ ingestId: 'i-1', sourceId: 's-1' });
    await t.enqueueItem({ ingestId: 'i-2', sourceId: 's-1' });
    await consumer.stop();
    expect(engine.processItem).toHaveBeenCalledTimes(2);
  });

  test('items enqueued before startConsumer are drained on start', async () => {
    const engine = makeFakeEngine();
    const t = createMemoryIngestTransport();
    // Producer fires before consumer registers — must not drop.
    await t.enqueueItem({ ingestId: 'i-1', sourceId: 's-1' });
    await t.enqueueItem({ ingestId: 'i-2', sourceId: 's-1' });
    const consumer = t.startConsumer(engine);
    await consumer.stop();
    expect(engine.processed.map((p) => p.ingestId)).toEqual(['i-1', 'i-2']);
  });

  test('stop() drains in-flight chain before returning', async () => {
    const engine = makeFakeEngine();
    let resolved = false;
    let gate!: () => void;
    const wait = new Promise<void>((res) => { gate = res; });
    (engine.processItem as jest.Mock).mockImplementation(async () => {
      await wait;
      resolved = true;
    });
    const t = createMemoryIngestTransport();
    const consumer = t.startConsumer(engine);
    await t.enqueueItem({ ingestId: 'i-1', sourceId: 's-1' });
    const stopPromise = consumer.stop();
    // stop should still be pending while processItem is gated
    let stopResolved = false;
    void stopPromise.then(() => { stopResolved = true; });
    await new Promise((r) => setImmediate(r));
    expect(stopResolved).toBe(false);
    gate();
    await stopPromise;
    expect(resolved).toBe(true);
  });
});

// ─── MQ transport ─────────────────────────────────────────────────────────

describe('createMqIngestTransport', () => {
  test('reports mode = mq', () => {
    const t = createMqIngestTransport({
      _catalog: makeFakeCatalog(),
      _publishDurable: jest.fn(async () => {}),
    });
    expect(t.mode).toBe('mq');
  });

  test('enqueueItem calls publishDurable with the right event + payload', async () => {
    const fakeCatalog = makeFakeCatalog();
    const publish = jest.fn(async () => {});
    const t = createMqIngestTransport({
      _catalog: fakeCatalog,
      _publishDurable: publish,
    });
    await t.enqueueItem({ ingestId: 'i-1', sourceId: 's-1', tags: ['a'] });
    expect(publish).toHaveBeenCalledWith('ingest:item.queued', {
      ingestId: 'i-1',
      sourceId: 's-1',
      tags: ['a'],
    });
  });

  test('startConsumer subscribes to ingest:item.queued and routes to processItem', async () => {
    const fakeCatalog = makeFakeCatalog();
    const t = createMqIngestTransport({
      _catalog: fakeCatalog,
      _publishDurable: jest.fn(async () => {}),
    });
    const engine = makeFakeEngine();
    const consumer = t.startConsumer(engine);
    expect(fakeCatalog.subscribed).toEqual(['ingest:item.queued']);
    await fakeCatalog.deliver({ ingestId: 'i-1', sourceId: 's-1', tags: ['x'] });
    expect(engine.processed).toEqual([{ ingestId: 'i-1', tags: ['x'] }]);
    await consumer.stop();
    expect(fakeCatalog.isHandlerRegistered()).toBe(false);
  });

  test('handler re-throws on processItem failure so the transport redelivers', async () => {
    const fakeCatalog = makeFakeCatalog();
    const t = createMqIngestTransport({
      _catalog: fakeCatalog,
      _publishDurable: jest.fn(async () => {}),
    });
    const engine = makeFakeEngine();
    (engine.processItem as jest.Mock).mockRejectedValueOnce(new Error('transient'));
    t.startConsumer(engine);
    await expect(
      fakeCatalog.deliver({ ingestId: 'i-x', sourceId: 's-1' }),
    ).rejects.toThrow('transient');
  });

  test('survives restart: messages enqueued before stop are redelivered to a fresh consumer', async () => {
    // Simulate MQ Phase 1 durability — a captured payload is replayed to a
    // brand-new transport instance after the producer "process" has died.
    const fakeCatalog = makeFakeCatalog();
    const publishedPayloads: Array<{ ingestId: string; sourceId: string; tags?: string[] }> = [];
    const publish = jest.fn(async (_name: string, payload: unknown) => {
      publishedPayloads.push(payload as { ingestId: string; sourceId: string; tags?: string[] });
    });

    // --- Replica A: producer publishes, then dies before consuming ---
    const producer = createMqIngestTransport({
      _catalog: fakeCatalog,
      _publishDurable: publish,
    });
    const consumerA = producer.startConsumer(makeFakeEngine());
    await producer.enqueueItem({ ingestId: 'i-survives', sourceId: 's-1', tags: ['kept'] });
    await consumerA.stop(); // replica A crashes

    expect(publishedPayloads).toHaveLength(1);

    // --- Replica B: fresh transport, picks up the payload from MQ ---
    const replicaBCatalog = makeFakeCatalog();
    const replicaB = createMqIngestTransport({
      _catalog: replicaBCatalog,
      _publishDurable: jest.fn(),
    });
    const engineB = makeFakeEngine();
    replicaB.startConsumer(engineB);

    // The MQ would re-deliver the still-unacked payload to the new
    // subscriber. We simulate that by feeding the captured payload through.
    await replicaBCatalog.deliver(publishedPayloads[0]);
    expect(engineB.processed).toEqual([{ ingestId: 'i-survives', tags: ['kept'] }]);
  });

  test('unsubscribe is tolerant of internal errors', async () => {
    const errCatalog = {
      subscribe: jest.fn(<T>(_n: string, _h: EventHandler<T>): Subscription => ({
        unsubscribe: async () => { throw new Error('redis blip'); },
        pause: () => {},
        resume: () => {},
        isPaused: () => false,
      })),
    };
    const t = createMqIngestTransport({
      _catalog: errCatalog as unknown as { subscribe: typeof import('../../lib/event-catalog').catalog.subscribe },
      _publishDurable: jest.fn(),
    });
    const consumer = t.startConsumer(makeFakeEngine());
    await expect(consumer.stop()).resolves.toBeUndefined();
  });
});

// ─── createIngestTransport factory ────────────────────────────────────────

describe('createIngestTransport (factory)', () => {
  test('mode override beats env var', () => {
    process.env.PLATFORM_API_INGEST_TRANSPORT = 'mq';
    const t = createIngestTransport({ mode: 'memory' });
    expect(t.mode).toBe('memory');
    delete process.env.PLATFORM_API_INGEST_TRANSPORT;
  });

  test('env var "mq" → mq transport', () => {
    const prev = process.env.PLATFORM_API_INGEST_TRANSPORT;
    process.env.PLATFORM_API_INGEST_TRANSPORT = 'mq';
    try {
      const t = createIngestTransport({
        _catalog: makeFakeCatalog(),
        _publishDurable: jest.fn(),
      });
      expect(t.mode).toBe('mq');
    } finally {
      if (prev === undefined) delete process.env.PLATFORM_API_INGEST_TRANSPORT;
      else process.env.PLATFORM_API_INGEST_TRANSPORT = prev;
    }
  });

  test('unset env var → memory transport', () => {
    const prev = process.env.PLATFORM_API_INGEST_TRANSPORT;
    delete process.env.PLATFORM_API_INGEST_TRANSPORT;
    try {
      const t = createIngestTransport();
      expect(t.mode).toBe('memory');
    } finally {
      if (prev !== undefined) process.env.PLATFORM_API_INGEST_TRANSPORT = prev;
    }
  });
});
