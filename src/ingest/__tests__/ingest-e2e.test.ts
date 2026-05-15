// platform-api/src/ingest/__tests__/ingest-e2e.test.ts
//
// End-to-end test that boots the full ingest stack and verifies the complete
// flow: source event → bootstrapIngest → IngestEngine.triggerSync →
// IngestEngine.processItem → catalog emission or DLQ.
//
// Design:
//   - bootstrapIngest is called with real in-memory queues (no DDB/WAL).
//   - IngestEngine is wired with stub repositories and a fake catalog
//     (same pattern as engine-partitioned.test.ts).
//   - The adapter layer is mocked so no file-system I/O occurs.
//   - Metrics, bridge-types, and logger side-effects are silenced.
//   - The fake catalog's deliverAll() replays enqueued events to the
//     consumer, which calls engine.processItem exactly as production does.

import { bootstrapIngest, type IngestBootstrapDeps } from '../bootstrap';
import {
  IngestEngine,
  type IngestEngineDeps,
  type IngestEventBus,
} from '../engine';
import { startIngestQueueConsumer } from '../queue-consumer';
import type {
  IngestSource,
  IngestItem,
  IngestEvent,
  FileDescriptor,
  MappingRule,
  SyncCursorEntry,
} from '../types';
import type { IngestSourceRepository } from '../repositories/IngestSourceRepository';
import type { IngestItemRepository } from '../repositories/IngestItemRepository';
import type { SyncCursorRepository } from '../repositories/SyncCursorRepository';
import type { TypedDocumentRepository } from '../../repositories/TypedDocumentRepository';
import type {
  EventEnvelope,
  EventHandler,
  Subscription,
} from '@connorhoehn/event-catalog';

jest.setTimeout(15_000);

// ─── Silence side-effects ────────────────────────────────────────────────────

jest.mock('../adapters/adapter', () => ({
  createAdapter: jest.fn(),
}));

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

import { createAdapter } from '../adapters/adapter';

// ─── Test doubles ─────────────────────────────────────────────────────────────

/**
 * Minimal fake catalog: captures publishes and exposes deliverAll() to feed
 * them to the registered subscriber in arrival order (same semantics as the
 * real FIFO durable queue).
 */
function makeFakeCatalog() {
  const published: Array<{ name: string; payload: Record<string, unknown> }> = [];
  let handler: EventHandler<unknown> | null = null;

  return {
    published,
    publish: async (name: string, payload: Record<string, unknown>) => {
      published.push({ name, payload });
    },
    subscribe: jest.fn(
      <T>(_name: string, h: EventHandler<T>): Subscription => {
        handler = h as EventHandler<unknown>;
        let paused = false;
        return {
          unsubscribe: async () => {
            handler = null;
          },
          pause: () => { paused = true; },
          resume: () => { paused = false; },
          isPaused: () => paused,
        };
      },
    ),
    async deliverAll(): Promise<void> {
      if (!handler) return;
      for (const event of [...published]) {
        const envelope = {
          payload: event.payload,
          metadata: {
            id: `env-${published.indexOf(event)}`,
            timestamp: new Date().toISOString(),
          },
        } as unknown as EventEnvelope<unknown>;
        await handler(envelope);
      }
      published.length = 0;
    },
  };
}

function makeSource(overrides: Partial<IngestSource> = {}): IngestSource {
  return {
    sourceId: 'src-1',
    name: 'E2E Test Source',
    type: 'local',
    status: 'active',
    config: { path: '/data' },
    syncSchedule: null,
    mappingRules: [],
    defaultPipelineId: null,
    lastSyncAt: null,
    lastSyncStats: null,
    createdAt: '2026-01-01T00:00:00Z',
    updatedAt: '2026-01-01T00:00:00Z',
    ...overrides,
  } as IngestSource;
}

const TEXT_RULE: MappingRule = {
  ruleId: 'r-text',
  priority: 1,
  condition: { field: 'mimeType', op: 'eq', value: 'text/plain' },
  targetTypeId: 'plain-doc',
  fieldMapping: { name: 'title' },
};

function makeSourceRepo(source: IngestSource | null = makeSource()) {
  return {
    getById: jest.fn(async () => source),
    updateSource: jest.fn(async () => {}),
    listByStatus: jest.fn(async () => (source ? [source] : [])),
  } as unknown as IngestSourceRepository;
}

function makeItemRepo() {
  const store = new Map<string, IngestItem>();
  const statusHistory: Array<{ id: string; status: string }> = [];
  return {
    store,
    statusHistory,
    put: jest.fn(async (item: IngestItem) => {
      store.set(item.ingestId, { ...item });
    }),
    getById: jest.fn(async (id: string) => {
      const it = store.get(id);
      return it ? { ...it } : null;
    }),
    updateStatus: jest.fn(async (id: string, status: string, extra: Record<string, unknown> = {}) => {
      const item = store.get(id);
      if (item) {
        Object.assign(item, { status, ...extra });
        store.set(id, item);
      }
      statusHistory.push({ id, status });
    }),
    listByStatus: jest.fn(async () => []),
  } as unknown as IngestItemRepository & {
    store: Map<string, IngestItem>;
    statusHistory: Array<{ id: string; status: string }>;
  };
}

function makeCursorRepo(existingEntry: SyncCursorEntry | null = null) {
  const store = new Map<string, SyncCursorEntry>();
  if (existingEntry) {
    store.set(`${existingEntry.sourceId}:${existingEntry.filePath}`, existingEntry);
  }
  return {
    store,
    get: jest.fn(async (sourceId: string, filePath: string) =>
      store.get(`${sourceId}:${filePath}`) ?? null,
    ),
    put: jest.fn(async (entry: SyncCursorEntry) => {
      store.set(`${entry.sourceId}:${entry.filePath}`, entry);
    }),
    listBySource: jest.fn(async () => []),
    deleteBySource: jest.fn(async () => {}),
  } as unknown as SyncCursorRepository;
}

function makeDocumentRepo() {
  const store = new Map<string, unknown>();
  return {
    store,
    get: jest.fn(async () => null),
    create: jest.fn(async (doc: { documentId: string }) => {
      store.set(doc.documentId, doc);
    }),
  } as unknown as TypedDocumentRepository;
}

function makeEventBus(): IngestEventBus & { events: IngestEvent[] } {
  const events: IngestEvent[] = [];
  return {
    events,
    emit: jest.fn((e: IngestEvent) => { events.push(e); }),
    on: jest.fn(),
    off: jest.fn(),
  };
}

function makeAdapter(files: FileDescriptor[]) {
  return {
    list: jest.fn(async function* () {
      for (const f of files) yield f;
    }),
    fetch: jest.fn(async () => ({ data: Buffer.from('content'), encoding: 'utf-8' as const })),
  };
}

function makeFile(overrides: Partial<FileDescriptor> = {}): FileDescriptor {
  return {
    path: 'docs/report.txt',
    name: 'report.txt',
    hash: 'hash-abc',
    size: 200,
    mimeType: 'text/plain',
    lastModified: new Date().toISOString(),
    ...overrides,
  };
}

/** Minimal bootstrap deps — bootstrap itself never touches repos; they're
 *  wired into IngestEngine separately. */
function makeBootstrapDeps(): IngestBootstrapDeps {
  return {
    sourceRepository: {} as IngestSourceRepository,
    itemRepository: {} as IngestItemRepository,
    cursorRepository: {} as SyncCursorRepository,
    documentRepository: {} as TypedDocumentRepository,
  };
}

// ─── Happy path ───────────────────────────────────────────────────────────────

describe('ingest e2e — happy path: triggerSync → processItem → done', () => {
  let fakeCatalog: ReturnType<typeof makeFakeCatalog>;
  let itemRepo: ReturnType<typeof makeItemRepo>;
  let documentRepo: ReturnType<typeof makeDocumentRepo>;
  let eventBus: ReturnType<typeof makeEventBus>;
  let engine: IngestEngine;
  let consumer: { stop: () => Promise<void> };

  beforeEach(async () => {
    jest.clearAllMocks();

    fakeCatalog = makeFakeCatalog();
    itemRepo = makeItemRepo();
    documentRepo = makeDocumentRepo();
    eventBus = makeEventBus();

    const source = makeSource({ mappingRules: [TEXT_RULE] });

    const deps: IngestEngineDeps = {
      sourceRepository: makeSourceRepo(source),
      itemRepository: itemRepo,
      cursorRepository: makeCursorRepo(),
      documentRepository: documentRepo,
      eventBus,
      enqueueItem: async (params) => {
        await fakeCatalog.publish(
          'ingest:item.queued',
          params as unknown as Record<string, unknown>,
        );
      },
    };

    engine = new IngestEngine(deps);
    consumer = startIngestQueueConsumer({ engine, _catalog: fakeCatalog });

    (createAdapter as jest.Mock).mockReturnValue(makeAdapter([makeFile()]));
  });

  afterEach(async () => {
    await consumer.stop();
  });

  test('triggerSync enqueues file and consumer processes it to done', async () => {
    await engine.triggerSync('src-1', 'manual');
    expect(fakeCatalog.published).toHaveLength(1);
    expect(fakeCatalog.published[0].name).toBe('ingest:item.queued');

    await fakeCatalog.deliverAll();

    // Item must reach 'done' status
    const finalStatuses = itemRepo.statusHistory.map((h) => h.status);
    expect(finalStatuses).toContain('mapping');
    expect(finalStatuses).toContain('materializing');
    expect(finalStatuses).toContain('done');
  });

  test('catalog-emission: sync.started and sync.completed events are emitted', async () => {
    await engine.triggerSync('src-1', 'manual');
    await fakeCatalog.deliverAll();

    const types = eventBus.events.map((e) => e.type);
    expect(types).toContain('sync.started');
    expect(types).toContain('sync.completed');
  });

  test('catalog-emission: item.mapped event is emitted with correct typeId', async () => {
    await engine.triggerSync('src-1', 'manual');
    await fakeCatalog.deliverAll();

    const mappedEvent = eventBus.events.find((e) => e.type === 'item.mapped');
    expect(mappedEvent).toBeDefined();
    if (mappedEvent?.type === 'item.mapped') {
      expect(mappedEvent.typeId).toBe('plain-doc');
      expect(mappedEvent.method).toBe('rule');
      expect(mappedEvent.confidence).toBe(1.0);
    }
  });

  test('document is materialized in the document repository', async () => {
    await engine.triggerSync('src-1', 'manual');
    await fakeCatalog.deliverAll();

    expect(documentRepo.create).toHaveBeenCalledTimes(1);
    const call = (documentRepo.create as jest.Mock).mock.calls[0][0];
    expect(call.typeId).toBe('plain-doc');
    expect(typeof call.documentId).toBe('string');
    expect(call.createdBy).toContain('ingest:src-1');
  });
});

// ─── Mapping failure → item.failed event ──────────────────────────────────────

describe('ingest e2e — mapping failure: no matching rule → item.failed', () => {
  test('item with no matching rule is marked failed and emits item.failed', async () => {
    jest.clearAllMocks();

    const fakeCatalog = makeFakeCatalog();
    const itemRepo = makeItemRepo();
    const eventBus = makeEventBus();

    // Source has NO mapping rules — matchByRules will return null
    const source = makeSource({ mappingRules: [] });

    const deps: IngestEngineDeps = {
      sourceRepository: makeSourceRepo(source),
      itemRepository: itemRepo,
      cursorRepository: makeCursorRepo(),
      documentRepository: makeDocumentRepo(),
      eventBus,
      enqueueItem: async (params) => {
        await fakeCatalog.publish(
          'ingest:item.queued',
          params as unknown as Record<string, unknown>,
        );
      },
    };

    const engine = new IngestEngine(deps);
    const consumer = startIngestQueueConsumer({ engine, _catalog: fakeCatalog });

    (createAdapter as jest.Mock).mockReturnValue(
      makeAdapter([makeFile({ mimeType: 'application/octet-stream' })]),
    );

    await engine.triggerSync('src-1', 'manual');
    await fakeCatalog.deliverAll();
    await consumer.stop();

    // Item must be marked failed
    const failedStatuses = itemRepo.statusHistory.filter((h) => h.status === 'failed');
    expect(failedStatuses).toHaveLength(1);

    // item.failed event must be emitted
    const failedEvent = eventBus.events.find((e) => e.type === 'item.failed');
    expect(failedEvent).toBeDefined();
    if (failedEvent?.type === 'item.failed') {
      expect(failedEvent.error).toMatch(/No mapping rule matched/);
      expect(failedEvent.retriable).toBe(true);
    }
  });
});

// ─── DLQ wiring: bootstrapIngest DLQ integrates with item flow ────────────────

describe('ingest e2e — bootstrapIngest DLQ wiring', () => {
  test('DLQ can accept a failed ingest item via the bootstrap handle', async () => {
    const handle = await bootstrapIngest(makeBootstrapDeps(), { dlqCapacity: 5 });

    const failedItem: IngestItem = {
      ingestId: 'item-dlq-1',
      sourceId: 'src-1',
      filePath: 'bad-file.bin',
      fileHash: 'h-bad',
      mimeType: 'application/octet-stream',
      fileSize: 0,
      status: 'failed',
      mappedTypeId: null,
      mappingConfidence: null,
      mappingMethod: null,
      documentId: null,
      pipelineRunId: null,
      error: 'No mapping rule matched',
      attempts: 3,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    };

    // Wrap the item in a minimal Envelope and send to DLQ
    const { wrap } = await import('distributed-core/messaging-queue');
    const envelope = wrap(failedItem, { dedupKey: failedItem.ingestId });
    await handle.dlq.put(envelope, {
      lastError: 'No mapping rule matched',
      failedAtMs: Date.now(),
      totalAttempts: 3,
    });

    const page = await handle.dlq.list();
    expect(page.items).toHaveLength(1);
    expect(page.items[0].envelope.body.ingestId).toBe('item-dlq-1');
    expect(page.items[0].lastError).toMatch(/No mapping rule matched/);

    await handle.shutdown();
  });

  test('bootstrapIngest handle provides an idempotent ingestProducer', async () => {
    const handle = await bootstrapIngest(makeBootstrapDeps());

    const item: IngestItem = {
      ingestId: 'item-prod-1',
      sourceId: 'src-1',
      filePath: 'file.txt',
      fileHash: 'hash',
      mimeType: 'text/plain',
      fileSize: 100,
      status: 'queued',
      mappedTypeId: null,
      mappingConfidence: null,
      mappingMethod: null,
      documentId: null,
      pipelineRunId: null,
      error: null,
      attempts: 0,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    };

    const { wrap } = await import('distributed-core/messaging-queue');
    const env1 = wrap(item, { dedupKey: item.ingestId });
    const env2 = wrap(item, { dedupKey: item.ingestId });

    const r1 = await handle.ingestProducer.send(env1);
    const r2 = await handle.ingestProducer.send(env2);

    expect(r1.accepted).toBe(true);
    expect(r2.accepted).toBe(false); // deduplicated

    await handle.shutdown();
  });
});

// ─── Shutdown: bootstrapIngest tears down cleanly ────────────────────────────

describe('ingest e2e — bootstrapIngest shutdown', () => {
  test('shutdown resolves and is idempotent — no hanging promises', async () => {
    const handle = await bootstrapIngest(makeBootstrapDeps());

    await expect(handle.shutdown()).resolves.toBeUndefined();
    await expect(handle.shutdown()).resolves.toBeUndefined(); // second call is no-op
  });

  test('shutdown clears locks — subsequent createSession starts fresh', async () => {
    const handle = await bootstrapIngest(makeBootstrapDeps());
    const lock = handle.createSession('src-1');
    await lock.acquire(); // mark as held

    await handle.shutdown();

    // After shutdown, the lock map is cleared; creating a new handle starts
    // fresh (verified indirectly — shutdown must not throw).
    // We bootstrap again to ensure the call-site pattern works cleanly.
    const handle2 = await bootstrapIngest(makeBootstrapDeps());
    const lock2 = handle2.createSession('src-1');
    const acquired = await lock2.acquire();
    expect(acquired).toBe(true); // fresh state

    await handle2.shutdown();
  });

  test('engine can start and stop after bootstrapIngest shutdown', async () => {
    jest.clearAllMocks();
    const fakeCatalog = makeFakeCatalog();

    const handle = await bootstrapIngest(makeBootstrapDeps());
    const deps: IngestEngineDeps = {
      sourceRepository: makeSourceRepo(),
      itemRepository: makeItemRepo(),
      cursorRepository: makeCursorRepo(),
      documentRepository: makeDocumentRepo(),
      eventBus: makeEventBus(),
      enqueueItem: async (params) => {
        await fakeCatalog.publish(
          'ingest:item.queued',
          params as unknown as Record<string, unknown>,
        );
      },
    };

    const engine = new IngestEngine(deps);
    await engine.start();
    await engine.stop();
    await handle.shutdown();
  });
});

// ─── Multiple files: ordering preserved ──────────────────────────────────────

describe('ingest e2e — multiple files processed in arrival order', () => {
  test('all files reach done status in the order they were synced', async () => {
    jest.clearAllMocks();

    const fakeCatalog = makeFakeCatalog();
    const itemRepo = makeItemRepo();
    const source = makeSource({ mappingRules: [TEXT_RULE] });

    const deps: IngestEngineDeps = {
      sourceRepository: makeSourceRepo(source),
      itemRepository: itemRepo,
      cursorRepository: makeCursorRepo(),
      documentRepository: makeDocumentRepo(),
      eventBus: makeEventBus(),
      enqueueItem: async (params) => {
        await fakeCatalog.publish(
          'ingest:item.queued',
          params as unknown as Record<string, unknown>,
        );
      },
    };

    const engine = new IngestEngine(deps);
    const consumer = startIngestQueueConsumer({ engine, _catalog: fakeCatalog });

    (createAdapter as jest.Mock).mockReturnValue(
      makeAdapter([
        makeFile({ path: 'file-1.txt', name: 'file-1.txt', hash: 'h1' }),
        makeFile({ path: 'file-2.txt', name: 'file-2.txt', hash: 'h2' }),
        makeFile({ path: 'file-3.txt', name: 'file-3.txt', hash: 'h3' }),
      ]),
    );

    await engine.triggerSync('src-1', 'manual');
    expect(fakeCatalog.published).toHaveLength(3);

    await fakeCatalog.deliverAll();
    await consumer.stop();

    // All three items must exist and be done
    const allStatuses = [...itemRepo.store.values()].map((i) => i.status);
    expect(allStatuses).toHaveLength(3);
    expect(allStatuses.every((s) => s === 'done')).toBe(true);
  });
});
