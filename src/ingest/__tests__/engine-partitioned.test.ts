// platform-api/src/ingest/__tests__/engine-partitioned.test.ts
//
// Phase 54.6.3 — flipped from in-memory partition queues to durable
// `ingest:item.queued`. Per-source FIFO ordering now lives in the
// message-queue (FIFO + partitionKey:sourceId, see src/lib/event-catalog.ts).
// What this suite still owns:
//   1. triggerSync publishes one durable event per new file
//   2. The IngestQueueConsumer routes each delivery to engine.processItem
//   3. tags travel through the durable payload (no in-process cache)
//   4. processItem is invoked even when triggerSync ran on a different
//      "replica" (simulated by routing events through the fake catalog
//      without sharing engine state)

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

import { createAdapter } from '../adapters/adapter';

// ─── Test doubles ──────────────────────────────────────────────────────────

function makeSource(sourceId: string, mappingRules: IngestSource['mappingRules'] = []): IngestSource {
  return {
    sourceId,
    name: `Source ${sourceId}`,
    type: 'local',
    status: 'active',
    config: {},
    syncSchedule: null,
    mappingRules,
    defaultPipelineId: null,
    lastSyncAt: null,
    lastSyncStats: null,
    createdBy: 'test',
    createdAt: '2026-01-01T00:00:00Z',
    updatedAt: '2026-01-01T00:00:00Z',
  } as unknown as IngestSource;
}

function makeSourceRepo(sources: IngestSource[]) {
  const map = new Map(sources.map((s) => [s.sourceId, s]));
  return {
    getById: jest.fn(async (id: string) => map.get(id) ?? null),
    updateSource: jest.fn(async () => {}),
    listByStatus: jest.fn(async () => [...map.values()]),
  } as unknown as IngestSourceRepository;
}

function makeItemRepo() {
  const store = new Map<string, IngestItem>();
  const doneOrder: string[] = [];
  const doneBySource = new Map<string, string[]>();
  return {
    store,
    doneOrder,
    doneBySource,
    put: jest.fn(async (item: IngestItem) => {
      store.set(item.ingestId, { ...item });
    }),
    getById: jest.fn(async (id: string) => {
      const it = store.get(id);
      return it ? { ...it } : null;
    }),
    updateStatus: jest.fn(async (id: string, status: string, extra: Record<string, unknown> = {}) => {
      const item = store.get(id);
      if (!item) return;
      Object.assign(item, { status, ...extra });
      if (status === 'done') {
        doneOrder.push(id);
        const bucket = doneBySource.get(item.sourceId) ?? [];
        bucket.push(id);
        doneBySource.set(item.sourceId, bucket);
      }
    }),
    listByStatus: jest.fn(async () => []),
  } as unknown as IngestItemRepository & {
    store: Map<string, IngestItem>;
    doneOrder: string[];
    doneBySource: Map<string, string[]>;
  };
}

function makeCursorRepo() {
  return {
    get: jest.fn(async () => null),
    put: jest.fn(async () => {}),
  } as unknown as SyncCursorRepository;
}

function makeDocumentRepo() {
  return { get: jest.fn(async () => null), create: jest.fn(async () => {}) } as unknown as TypedDocumentRepository;
}

function makeEventBus(): IngestEventBus & { events: IngestEvent[] } {
  const events: IngestEvent[] = [];
  return { events, emit: jest.fn((e) => events.push(e)), on: jest.fn(), off: jest.fn() };
}

function makeAdapter(files: Array<{ path: string; hash?: string; tags?: string[] }>) {
  return {
    list: jest.fn(async function* (): AsyncGenerator<FileDescriptor> {
      for (const f of files) {
        yield {
          path: f.path,
          name: f.path,
          hash: f.hash ?? 'h',
          size: 10,
          mimeType: 'text/plain',
          lastModified: new Date().toISOString(),
          tags: f.tags,
        };
      }
    }),
    fetch: jest.fn(async () => ({ data: Buffer.from('x'), encoding: 'utf-8' as const })),
  };
}

const PASSTHROUGH_RULE: MappingRule = {
  ruleId: 'r1',
  priority: 1,
  condition: { field: 'mimeType', op: 'eq', value: 'text/plain' },
  targetTypeId: 't',
  fieldMapping: {},
};

/**
 * Minimal fake catalog: captures publishes per event-name in arrival order
 * and exposes `deliverAll()` to feed them to the registered subscriber. The
 * subscriber is awaited per delivery to preserve "single consumer, one at a
 * time, in arrival order" semantics — which is what the durable queue's
 * FIFO + partitionKey:sourceId contract guarantees in production.
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
      for (const event of published) {
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

// ─── Tests ────────────────────────────────────────────────────────────────

describe('IngestEngine — durable handoff (Phase 54.6.3)', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  test('triggerSync publishes one ingest:item.queued event per new file', async () => {
    const sourceRepo = makeSourceRepo([makeSource('src-1', [PASSTHROUGH_RULE])]);
    const fakeCatalog = makeFakeCatalog();

    const adapter = makeAdapter([
      { path: 'file-a.txt', hash: 'ha' },
      { path: 'file-b.txt', hash: 'hb' },
      { path: 'file-c.txt', hash: 'hc' },
    ]);
    (createAdapter as jest.Mock).mockReturnValue(adapter);

    const deps: IngestEngineDeps = {
      sourceRepository: sourceRepo,
      itemRepository: makeItemRepo(),
      cursorRepository: makeCursorRepo(),
      documentRepository: makeDocumentRepo(),
      eventBus: makeEventBus(),
      enqueueItem: async (params) => {
        await fakeCatalog.publish('ingest:item.queued', params as unknown as Record<string, unknown>);
      },
    };
    const engine = new IngestEngine(deps);

    await engine.triggerSync('src-1', 'manual');

    expect(fakeCatalog.published).toHaveLength(3);
    expect(fakeCatalog.published.every((p) => p.name === 'ingest:item.queued')).toBe(true);
    const sourceIds = fakeCatalog.published.map((p) => p.payload.sourceId);
    expect(new Set(sourceIds)).toEqual(new Set(['src-1']));
  });

  test('consumer routes each delivery to processItem, preserving arrival order', async () => {
    const sourceRepo = makeSourceRepo([makeSource('src-1', [PASSTHROUGH_RULE])]);
    const itemRepo = makeItemRepo();
    const fakeCatalog = makeFakeCatalog();

    (createAdapter as jest.Mock).mockReturnValue(
      makeAdapter([
        { path: 'a.txt', hash: 'ha' },
        { path: 'b.txt', hash: 'hb' },
        { path: 'c.txt', hash: 'hc' },
      ]),
    );

    const engine = new IngestEngine({
      sourceRepository: sourceRepo,
      itemRepository: itemRepo,
      cursorRepository: makeCursorRepo(),
      documentRepository: makeDocumentRepo(),
      eventBus: makeEventBus(),
      enqueueItem: async (params) => {
        await fakeCatalog.publish('ingest:item.queued', params as unknown as Record<string, unknown>);
      },
    });

    const consumer = startIngestQueueConsumer({ engine, _catalog: fakeCatalog });

    await engine.triggerSync('src-1', 'manual');
    await fakeCatalog.deliverAll();
    await consumer.stop();

    expect(itemRepo.doneOrder).toHaveLength(3);
    const queued = [...itemRepo.store.keys()];
    expect(itemRepo.doneOrder).toEqual(queued);
  });

  test('tags travel through the durable payload, not an in-process cache', async () => {
    const sourceRepo = makeSourceRepo([makeSource('src-1', [PASSTHROUGH_RULE])]);
    const itemRepo = makeItemRepo();
    const fakeCatalog = makeFakeCatalog();

    (createAdapter as jest.Mock).mockReturnValue(
      makeAdapter([{ path: 'tagged.txt', hash: 'h-tagged', tags: ['important', 'reviewed'] }]),
    );

    // Two distinct IngestEngine instances simulate two replicas: replica A
    // syncs (and publishes), replica B's consumer processes. The processItem
    // call lands on replica B, which never saw the tag list — it must come
    // through the durable envelope.
    const replicaA = new IngestEngine({
      sourceRepository: sourceRepo,
      itemRepository: itemRepo,
      cursorRepository: makeCursorRepo(),
      documentRepository: makeDocumentRepo(),
      eventBus: makeEventBus(),
      enqueueItem: async (params) => {
        await fakeCatalog.publish('ingest:item.queued', params as unknown as Record<string, unknown>);
      },
    });
    const replicaB = new IngestEngine({
      sourceRepository: sourceRepo,
      itemRepository: itemRepo,
      cursorRepository: makeCursorRepo(),
      documentRepository: makeDocumentRepo(),
      eventBus: makeEventBus(),
      enqueueItem: async () => {
        throw new Error('replica B should not publish — it only processes');
      },
    });
    const processSpy = jest.spyOn(replicaB, 'processItem');
    const consumer = startIngestQueueConsumer({ engine: replicaB, _catalog: fakeCatalog });

    await replicaA.triggerSync('src-1', 'manual');
    await fakeCatalog.deliverAll();
    await consumer.stop();

    expect(processSpy).toHaveBeenCalledTimes(1);
    const [, deliveredTags] = processSpy.mock.calls[0];
    expect(deliveredTags).toEqual(['important', 'reviewed']);
  });
});
