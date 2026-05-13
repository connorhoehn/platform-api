// social-api/src/ingest/__tests__/agentic-review.test.ts
//
// Integration test for the agentic review trigger flow:
//   1. A file with `agenticReviewTag` is synced.
//   2. After materialization, the review pipeline is triggered.
//   3. An `item.review.triggered` event is emitted.

import { randomUUID } from 'crypto';
import { IngestEngine, IngestEventBus, IngestEngineDeps } from '../engine';
import { MockDriveAdapter } from '../adapters/mock-drive';
import type { IngestSource, IngestItem, IngestEvent, SyncCursorEntry } from '../types';
import type { TypedDocumentItem } from '../../repositories/TypedDocumentRepository';

// --- Mock the pipeline bridge ---
const mockTrigger = jest.fn().mockResolvedValue({ runId: 'review-run-123' });

jest.mock('../../pipeline/bridge-types', () => ({
  getPipelineBridge: () => ({ trigger: mockTrigger }),
}));

// --- Mock metrics (no-ops) ---
jest.mock('../../observability/metrics', () => ({
  recordIngestItem: jest.fn(),
  recordIngestSyncDuration: jest.fn(),
  recordIngestQueueDepthDelta: jest.fn(),
  recordIngestMappingConfidence: jest.fn(),
  recordIngestDlqDepthDelta: jest.fn(),
}));

// --- Mock the adapter factory to return our shared MockDriveAdapter instance ---
const sharedAdapter = new MockDriveAdapter();

jest.mock('../adapters/adapter', () => ({
  createAdapter: () => sharedAdapter,
}));

// --- In-memory repository fakes ---

class FakeSourceRepo {
  private store = new Map<string, IngestSource>();

  async getById(id: string): Promise<IngestSource | null> {
    return this.store.get(id) ?? null;
  }

  async putSource(source: IngestSource): Promise<void> {
    this.store.set(source.sourceId, source);
  }

  async updateSource(id: string, patch: Partial<IngestSource>): Promise<void> {
    const existing = this.store.get(id);
    if (existing) {
      Object.assign(existing, patch);
    }
  }

  async listByStatus(_status: string): Promise<IngestSource[]> {
    return [];
  }
}

class FakeItemRepo {
  private store = new Map<string, IngestItem>();

  async getById(id: string): Promise<IngestItem | null> {
    return this.store.get(id) ?? null;
  }

  async put(item: IngestItem): Promise<void> {
    this.store.set(item.ingestId, item);
  }

  async updateStatus(
    id: string,
    _status: string,
    extra?: Partial<IngestItem>,
  ): Promise<void> {
    const existing = this.store.get(id);
    if (existing && extra) {
      Object.assign(existing, extra);
    }
  }

  getAll(): IngestItem[] {
    return Array.from(this.store.values());
  }
}

class FakeCursorRepo {
  private store = new Map<string, SyncCursorEntry>();

  async get(sourceId: string, filePath: string): Promise<SyncCursorEntry | null> {
    return this.store.get(`${sourceId}:${filePath}`) ?? null;
  }

  async put(entry: SyncCursorEntry): Promise<void> {
    this.store.set(`${entry.sourceId}:${entry.filePath}`, entry);
  }
}

class FakeDocumentRepo {
  private store = new Map<string, TypedDocumentItem>();

  async create(doc: TypedDocumentItem): Promise<void> {
    this.store.set(doc.documentId, doc);
  }

  getAll(): TypedDocumentItem[] {
    return Array.from(this.store.values());
  }
}

class FakeEventBus implements IngestEventBus {
  events: IngestEvent[] = [];
  private handlers: Array<(e: IngestEvent) => void> = [];

  emit(event: IngestEvent): void {
    this.events.push(event);
    for (const h of this.handlers) h(event);
  }

  on(handler: (e: IngestEvent) => void): void {
    this.handlers.push(handler);
  }

  off(handler: (e: IngestEvent) => void): void {
    this.handlers = this.handlers.filter((h) => h !== handler);
  }
}

// --- Test suite ---

describe('Agentic review trigger', () => {
  let engine: IngestEngine;
  let sourceRepo: FakeSourceRepo;
  let itemRepo: FakeItemRepo;
  let cursorRepo: FakeCursorRepo;
  let documentRepo: FakeDocumentRepo;
  let eventBus: FakeEventBus;

  const SOURCE_ID = 'src-review-test';
  const REVIEW_PIPELINE_ID = 'pipeline-review-001';

  beforeEach(() => {
    mockTrigger.mockClear();

    sourceRepo = new FakeSourceRepo();
    itemRepo = new FakeItemRepo();
    cursorRepo = new FakeCursorRepo();
    documentRepo = new FakeDocumentRepo();
    eventBus = new FakeEventBus();

    // Phase 54.6.3 — inline "enqueue → process" so triggerSync exercises
    // the same path the production IngestQueueConsumer takes. Tags flow
    // through the enqueueItem payload, matching the durable contract.
    let engineRef: IngestEngine;
    const enqueueItem = async ({ ingestId, tags }: { ingestId: string; sourceId: string; tags?: string[] }) => {
      await engineRef.processItem(ingestId, tags);
    };
    engine = new IngestEngine({
      sourceRepository: sourceRepo,
      itemRepository: itemRepo,
      cursorRepository: cursorRepo,
      documentRepository: documentRepo,
      eventBus,
      enqueueItem,
    } as unknown as IngestEngineDeps);
    engineRef = engine;

    // Seed the mock adapter with a file tagged "needs-review"
    sharedAdapter.seed([
      {
        path: 'reports/q1-financials.md',
        content: '# Q1 Financials\nRevenue: $1.2M',
        tags: ['needs-review', 'finance'],
        lastModified: new Date().toISOString(),
      },
      {
        path: 'docs/readme.md',
        content: '# README',
        tags: ['public'],
        lastModified: new Date().toISOString(),
      },
    ]);

    // Set up source with agenticReviewTag and a mapping rule that matches .md files
    const source: IngestSource = {
      sourceId: SOURCE_ID,
      name: 'Review Test Source',
      type: 'google-drive',
      config: {
        agenticReviewTag: 'needs-review',
        reviewPipelineId: REVIEW_PIPELINE_ID,
      },
      syncSchedule: null,
      mappingRules: [
        {
          ruleId: 'rule-md',
          condition: { field: 'extension', op: 'eq', value: 'md' },
          targetTypeId: 'document-type-markdown',
          fieldMapping: { name: 'title', path: 'sourcePath' },
          priority: 1,
        },
      ],
      defaultPipelineId: null,
      status: 'active',
      lastSyncAt: null,
      lastSyncStats: null,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    };

    sourceRepo.putSource(source);
  });

  it('triggers the review pipeline when a file with the review tag is ingested', async () => {
    // Trigger sync — this enqueues files
    await engine.triggerSync(SOURCE_ID, 'manual');

    // Process all queued items
    const items = itemRepo.getAll();
    expect(items.length).toBe(2); // both .md files were enqueued

    for (const item of items) {
      await engine.processItem(item.ingestId);
    }

    // The review pipeline should have been triggered for the "needs-review" file
    expect(mockTrigger).toHaveBeenCalledTimes(1);
    expect(mockTrigger).toHaveBeenCalledWith(
      expect.objectContaining({
        pipelineId: REVIEW_PIPELINE_ID,
        triggerPayload: expect.objectContaining({
          triggerType: 'agentic-review',
        }),
      }),
    );

    // Check the emitted events include item.review.triggered
    const reviewEvents = eventBus.events.filter(
      (e) => e.type === 'item.review.triggered',
    );
    expect(reviewEvents).toHaveLength(1);

    const reviewEvent = reviewEvents[0] as Extract<IngestEvent, { type: 'item.review.triggered' }>;
    expect(reviewEvent.pipelineRunId).toBe('review-run-123');
    expect(reviewEvent.documentId).toBeDefined();
    expect(reviewEvent.ingestId).toBeDefined();
  });

  it('does NOT trigger review for files without the review tag', async () => {
    // Remove the tagged file, keep only the untagged one
    sharedAdapter.removeFile('reports/q1-financials.md');

    await engine.triggerSync(SOURCE_ID, 'manual');

    const items = itemRepo.getAll();
    for (const item of items) {
      await engine.processItem(item.ingestId);
    }

    // No review pipeline trigger
    expect(mockTrigger).not.toHaveBeenCalled();

    const reviewEvents = eventBus.events.filter(
      (e) => e.type === 'item.review.triggered',
    );
    expect(reviewEvents).toHaveLength(0);
  });

  it('does NOT trigger review when source has no agenticReviewTag configured', async () => {
    // Update source to remove the review config
    await sourceRepo.updateSource(SOURCE_ID, {
      config: {},
    } as Partial<IngestSource>);

    await engine.triggerSync(SOURCE_ID, 'manual');

    const items = itemRepo.getAll();
    for (const item of items) {
      await engine.processItem(item.ingestId);
    }

    expect(mockTrigger).not.toHaveBeenCalled();
  });
});

describe('checkTagTriggers', () => {
  let engine: IngestEngine;
  let sourceRepo: FakeSourceRepo;
  let itemRepo: FakeItemRepo;
  let cursorRepo: FakeCursorRepo;
  let documentRepo: FakeDocumentRepo;
  let eventBus: FakeEventBus;

  const SOURCE_ID = 'src-tag-trigger';

  beforeEach(() => {
    sourceRepo = new FakeSourceRepo();
    itemRepo = new FakeItemRepo();
    cursorRepo = new FakeCursorRepo();
    documentRepo = new FakeDocumentRepo();
    eventBus = new FakeEventBus();

    let engineRef: IngestEngine;
    const enqueueItem = async ({ ingestId, tags }: { ingestId: string; sourceId: string; tags?: string[] }) => {
      await engineRef.processItem(ingestId, tags);
    };
    engine = new IngestEngine({
      sourceRepository: sourceRepo,
      itemRepository: itemRepo,
      cursorRepository: cursorRepo,
      documentRepository: documentRepo,
      eventBus,
      enqueueItem,
    } as unknown as IngestEngineDeps);
    engineRef = engine;

    sharedAdapter.seed([
      {
        path: 'tagged/important.md',
        content: '# Important doc',
        tags: ['urgent'],
        lastModified: new Date().toISOString(),
      },
      {
        path: 'tagged/normal.md',
        content: '# Normal doc',
        tags: [],
        lastModified: new Date().toISOString(),
      },
    ]);
  });

  it('enqueues only files with the configured trigger tag', async () => {
    const source: IngestSource = {
      sourceId: SOURCE_ID,
      name: 'Tag Trigger Source',
      type: 'google-drive',
      config: { triggerOnTag: 'urgent' },
      syncSchedule: null,
      mappingRules: [],
      defaultPipelineId: null,
      status: 'active',
      lastSyncAt: null,
      lastSyncStats: null,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    };
    await sourceRepo.putSource(source);

    await engine.checkTagTriggers(SOURCE_ID);

    const items = itemRepo.getAll();
    expect(items).toHaveLength(1);
    expect(items[0].filePath).toBe('tagged/important.md');
  });

  it('skips files that have not changed since last sync', async () => {
    const source: IngestSource = {
      sourceId: SOURCE_ID,
      name: 'Tag Trigger Source',
      type: 'google-drive',
      config: { triggerOnTag: 'urgent' },
      syncSchedule: null,
      mappingRules: [],
      defaultPipelineId: null,
      status: 'active',
      lastSyncAt: null,
      lastSyncStats: null,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    };
    await sourceRepo.putSource(source);

    // First call enqueues the file
    await engine.checkTagTriggers(SOURCE_ID);
    expect(itemRepo.getAll()).toHaveLength(1);

    // Second call should skip (hash unchanged)
    await engine.checkTagTriggers(SOURCE_ID);
    expect(itemRepo.getAll()).toHaveLength(1); // still just 1
  });

  it('does nothing when triggerOnTag is not configured', async () => {
    const source: IngestSource = {
      sourceId: SOURCE_ID,
      name: 'No Tag Source',
      type: 'google-drive',
      config: {},
      syncSchedule: null,
      mappingRules: [],
      defaultPipelineId: null,
      status: 'active',
      lastSyncAt: null,
      lastSyncStats: null,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    };
    await sourceRepo.putSource(source);

    await engine.checkTagTriggers(SOURCE_ID);

    expect(itemRepo.getAll()).toHaveLength(0);
  });
});
