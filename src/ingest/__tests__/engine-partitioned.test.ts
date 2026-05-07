// social-api/src/ingest/__tests__/engine-partitioned.test.ts
//
// Tests for the partitioned queue behavior introduced in hub#2135:
//   - items within a sourceId are processed in order (FIFO per partition)
//   - items across sourceIds run in parallel (one worker per active source)
//   - throughput benchmark demonstrates parallel speedup

import { IngestEngine, type IngestEngineDeps, type IngestEventBus } from '../engine';
import type { IngestSource, IngestItem, IngestEvent, FileDescriptor, MappingRule } from '../types';
import type { IngestSourceRepository } from '../repositories/IngestSourceRepository';
import type { IngestItemRepository } from '../repositories/IngestItemRepository';
import type { SyncCursorRepository } from '../repositories/SyncCursorRepository';
import type { TypedDocumentRepository } from '../../repositories/TypedDocumentRepository';

jest.setTimeout(30_000);

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

// ─── Helpers ───────────────────────────────────────────────────────────────

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

/**
 * Item repo that tracks items written via put() and exposes them via getById,
 * mirroring the real DDB-backed repo's behavior. processingDelay simulates
 * per-item work and runs on every status update; doneOrder records the
 * order of items reaching 'done' so tests can assert on FIFO/parallelism.
 */
function makeItemRepo(opts: { processingDelay?: number } = {}) {
  const processingDelay = opts.processingDelay ?? 0;
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
        if (processingDelay > 0) await new Promise((r) => setTimeout(r, processingDelay));
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

function makeAdapter(files: Array<{ path: string; hash?: string }>) {
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
        };
      }
    }),
    fetch: jest.fn(async () => ({ data: Buffer.from('x'), encoding: 'utf-8' as const })),
  };
}

async function waitFor(predicate: () => boolean, timeoutMs = 10_000, pollMs = 5): Promise<void> {
  const start = Date.now();
  while (!predicate()) {
    if (Date.now() - start > timeoutMs) {
      throw new Error(`waitFor timed out after ${timeoutMs}ms`);
    }
    await new Promise((r) => setTimeout(r, pollMs));
  }
}

const PASSTHROUGH_RULE: MappingRule = {
  ruleId: 'r1',
  priority: 1,
  condition: { field: 'mimeType', op: 'eq', value: 'text/plain' },
  targetTypeId: 't',
  fieldMapping: {},
};

// ─── Ordering tests ────────────────────────────────────────────────────────

describe('IngestEngine — partitioned queue ordering', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  test('items from the same source are processed in FIFO order', async () => {
    const sourceRepo = makeSourceRepo([makeSource('src-1', [PASSTHROUGH_RULE])]);
    const itemRepo = makeItemRepo();
    const cursorRepo = makeCursorRepo();
    const documentRepo = makeDocumentRepo();
    const eventBus = makeEventBus();

    const adapter = makeAdapter([
      { path: 'file-a.txt', hash: 'ha' },
      { path: 'file-b.txt', hash: 'hb' },
      { path: 'file-c.txt', hash: 'hc' },
    ]);
    (createAdapter as jest.Mock).mockReturnValue(adapter);

    const engine = new IngestEngine({
      sourceRepository: sourceRepo,
      itemRepository: itemRepo,
      cursorRepository: cursorRepo,
      documentRepository: documentRepo,
      eventBus,
    });
    await engine.start();
    await engine.triggerSync('src-1', 'manual');

    await waitFor(() => itemRepo.doneOrder.length >= 3, 10_000);
    await engine.stop();

    expect(itemRepo.doneOrder).toHaveLength(3);

    // Verify FIFO within partition: each done id was put() before the next.
    // store insertion order matches put() order, which matches adapter yield order.
    const expectedOrder = [...itemRepo.store.keys()];
    expect(itemRepo.doneOrder).toEqual(expectedOrder);
  });

  test('items from different sources proceed independently', async () => {
    const sourceA = makeSource('src-a', [PASSTHROUGH_RULE]);
    const sourceB = makeSource('src-b', [PASSTHROUGH_RULE]);

    const sourceRepo = makeSourceRepo([sourceA, sourceB]);
    const itemRepo = makeItemRepo();
    const cursorRepo = makeCursorRepo();
    const documentRepo = makeDocumentRepo();
    const eventBus = makeEventBus();

    (createAdapter as jest.Mock).mockImplementation((source: IngestSource) => {
      const prefix = source.sourceId === 'src-a' ? 'a-file' : 'b-file';
      return makeAdapter([
        { path: `${prefix}-1.txt`, hash: `${prefix}-1` },
        { path: `${prefix}-2.txt`, hash: `${prefix}-2` },
      ]);
    });

    const engine = new IngestEngine({
      sourceRepository: sourceRepo,
      itemRepository: itemRepo,
      cursorRepository: cursorRepo,
      documentRepository: documentRepo,
      eventBus,
    });
    await engine.start();

    await Promise.all([
      engine.triggerSync('src-a', 'manual'),
      engine.triggerSync('src-b', 'manual'),
    ]);

    await waitFor(() => itemRepo.doneOrder.length >= 4, 10_000);
    await engine.stop();

    expect(itemRepo.doneBySource.get('src-a')).toHaveLength(2);
    expect(itemRepo.doneBySource.get('src-b')).toHaveLength(2);
  });
});

// ─── Throughput benchmark ──────────────────────────────────────────────────

describe('IngestEngine — throughput benchmark', () => {
  test('parallel partitions complete faster than sequential processing', async () => {
    const SOURCES = 4;
    const FILES_PER_SOURCE = 3;
    const ITEM_DELAY_MS = 25; // simulated per-item work

    const sources = Array.from({ length: SOURCES }, (_, i) =>
      makeSource(`src-${i}`, [PASSTHROUGH_RULE]),
    );

    const sourceRepo = makeSourceRepo(sources);
    const itemRepo = makeItemRepo({ processingDelay: ITEM_DELAY_MS });
    const cursorRepo = makeCursorRepo();
    const documentRepo = makeDocumentRepo();
    const eventBus = makeEventBus();

    (createAdapter as jest.Mock).mockImplementation((source: IngestSource) => {
      const idx = source.sourceId.split('-')[1];
      return makeAdapter(
        Array.from({ length: FILES_PER_SOURCE }, (_, j) => ({
          path: `${idx}-${j}-file.txt`,
          hash: `${idx}-${j}`,
        })),
      );
    });

    const engine = new IngestEngine({
      sourceRepository: sourceRepo,
      itemRepository: itemRepo,
      cursorRepository: cursorRepo,
      documentRepository: documentRepo,
      eventBus,
    });
    await engine.start();

    const t0 = Date.now();
    await Promise.all(sources.map((s) => engine.triggerSync(s.sourceId, 'manual')));

    const totalItems = SOURCES * FILES_PER_SOURCE;
    await waitFor(() => itemRepo.doneOrder.length >= totalItems, 15_000);
    const elapsed = Date.now() - t0;

    await engine.stop();

    expect(itemRepo.doneOrder).toHaveLength(totalItems);

    // Sequential worst-case: SOURCES * FILES_PER_SOURCE * ITEM_DELAY_MS = 4*3*25 = 300ms
    // Parallel best-case: FILES_PER_SOURCE * ITEM_DELAY_MS = 3*25 = 75ms
    // Assert parallel completes in well under sequential time, with headroom
    // for test runner overhead.
    const sequentialMs = SOURCES * FILES_PER_SOURCE * ITEM_DELAY_MS;
    const singleSourceMs = FILES_PER_SOURCE * ITEM_DELAY_MS;
    expect(elapsed).toBeLessThan(sequentialMs); // strictly better than serial
    expect(elapsed).toBeLessThan(singleSourceMs * 3 + 500); // close to parallel-ideal

    // eslint-disable-next-line no-console
    console.log(`Partitioned: ${elapsed}ms vs sequential worst-case: ${sequentialMs}ms`);
  });
});
