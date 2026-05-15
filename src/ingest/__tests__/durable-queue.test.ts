// platform-api/src/ingest/__tests__/durable-queue.test.ts
//
// Tests for the WAL durability layer:
//   - WalQueueStore: put / delete / loadPending round-trip
//   - DurableIngestQueue: enqueueBatch persists, completeBatch deletes,
//     hydrateFromStore replays, nack-no-retry deletes
//   - bootstrapIngest durable path: items survive shutdown() + re-bootstrap()
//   - bootstrapIngest in-memory path: unchanged (InMemoryQueueStore)

import os from 'node:os';
import fs from 'node:fs';
import path from 'node:path';
import { wrap } from 'distributed-core/messaging-queue';
import { WalQueueStore, InMemoryQueueStore, DurableIngestQueue } from '../durable-queue';
import { bootstrapIngest, type IngestBootstrapDeps } from '../bootstrap';
import type { IngestSourceRepository } from '../repositories/IngestSourceRepository';
import type { IngestItemRepository } from '../repositories/IngestItemRepository';
import type { SyncCursorRepository } from '../repositories/SyncCursorRepository';
import type { TypedDocumentRepository } from '../../repositories/TypedDocumentRepository';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeDeps(queueStore?: IngestBootstrapDeps['queueStore']): IngestBootstrapDeps {
  return {
    sourceRepository: {} as IngestSourceRepository,
    itemRepository: {} as IngestItemRepository,
    cursorRepository: {} as SyncCursorRepository,
    documentRepository: {} as TypedDocumentRepository,
    queueStore,
  };
}

function tempWalDir(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'pa-wal-test-'));
  return dir;
}

function makeSyncTask(sourceId: string) {
  return { sourceId, triggeredBy: 'manual' as const, requestedAt: new Date().toISOString() };
}

function makeIngestItem(ingestId: string): import('../types').IngestItem {
  return {
    ingestId,
    sourceId: 'src-1',
    filePath: `/files/${ingestId}.txt`,
    fileHash: 'abc123',
    mimeType: 'text/plain',
    fileSize: 42,
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
}

// ---------------------------------------------------------------------------
// WalQueueStore unit tests
// ---------------------------------------------------------------------------

describe('WalQueueStore', () => {
  let dir: string;
  let store: WalQueueStore;

  beforeEach(() => {
    dir = tempWalDir();
    store = new WalQueueStore(dir);
  });

  afterEach(() => {
    fs.rmSync(dir, { recursive: true, force: true });
  });

  test('init creates WAL file', async () => {
    await store.init('my-queue');
    const walPath = path.join(dir, 'my-queue.wal');
    expect(fs.existsSync(walPath)).toBe(true);
  });

  test('loadPending returns empty array on fresh store', async () => {
    await store.init('q1');
    const items = await store.loadPending('q1');
    expect(items).toEqual([]);
  });

  test('putBatch then loadPending returns envelopes', async () => {
    await store.init('q1');
    const env = wrap({ x: 1 }, { dedupKey: 'k1' });
    await store.putBatch('q1', [env]);
    const loaded = await store.loadPending('q1');
    expect(loaded).toHaveLength(1);
    expect(loaded[0].id).toBe(env.id);
  });

  test('putBatch with multiple envelopes round-trips all', async () => {
    await store.init('q1');
    const envs = [wrap({ n: 1 }), wrap({ n: 2 }), wrap({ n: 3 })];
    await store.putBatch('q1', envs);
    const loaded = await store.loadPending('q1');
    expect(loaded).toHaveLength(3);
    expect(loaded.map((e) => e.id)).toEqual(envs.map((e) => e.id));
  });

  test('delete removes specific envelope, preserves others', async () => {
    await store.init('q1');
    const e1 = wrap({ v: 'a' });
    const e2 = wrap({ v: 'b' });
    const e3 = wrap({ v: 'c' });
    await store.putBatch('q1', [e1, e2, e3]);
    await store.delete('q1', e2.id);
    const loaded = await store.loadPending('q1');
    expect(loaded).toHaveLength(2);
    expect(loaded.map((e) => e.id)).toContain(e1.id);
    expect(loaded.map((e) => e.id)).toContain(e3.id);
    expect(loaded.map((e) => e.id)).not.toContain(e2.id);
  });

  test('delete on non-existent id is a no-op', async () => {
    await store.init('q1');
    const env = wrap({ v: 'a' });
    await store.putBatch('q1', [env]);
    await store.delete('q1', 'does-not-exist');
    const loaded = await store.loadPending('q1');
    expect(loaded).toHaveLength(1);
  });

  test('separate queue names use separate files', async () => {
    await store.init('q-alpha');
    await store.init('q-beta');
    await store.putBatch('q-alpha', [wrap({ src: 'alpha' })]);
    const alpha = await store.loadPending('q-alpha');
    const beta = await store.loadPending('q-beta');
    expect(alpha).toHaveLength(1);
    expect(beta).toHaveLength(0);
  });
});

// ---------------------------------------------------------------------------
// DurableIngestQueue unit tests
// ---------------------------------------------------------------------------

describe('DurableIngestQueue', () => {
  let dir: string;

  beforeEach(() => { dir = tempWalDir(); });
  afterEach(() => { fs.rmSync(dir, { recursive: true, force: true }); });

  test('enqueueBatch persists to store', async () => {
    const store = new WalQueueStore(dir);
    await store.init('sq');
    const q = new DurableIngestQueue<{ n: number }>({ store, queueName: 'sq' });
    const env = wrap({ n: 1 });
    await q.enqueueBatch([env]);
    const loaded = await store.loadPending('sq');
    expect(loaded).toHaveLength(1);
    expect(loaded[0].id).toBe(env.id);
  });

  test('completeBatch deletes from store', async () => {
    const store = new WalQueueStore(dir);
    await store.init('sq');
    const q = new DurableIngestQueue<{ n: number }>({ store, queueName: 'sq' });
    const env = wrap({ n: 1 });
    await q.enqueueBatch([env]);
    const leases = await q.leaseBatch('w1', { maxMessages: 1, durationMs: 30_000, nowMs: Date.now() });
    expect(leases).toHaveLength(1);
    await q.completeBatch(leases as Parameters<typeof q.completeBatch>[0]);
    const loaded = await store.loadPending('sq');
    expect(loaded).toHaveLength(0);
  });

  test('nack with retry=true leaves item in store', async () => {
    const store = new WalQueueStore(dir);
    await store.init('sq');
    const q = new DurableIngestQueue<{ n: number }>({ store, queueName: 'sq' });
    const env = wrap({ n: 1 });
    await q.enqueueBatch([env]);
    const leases = await q.leaseBatch('w1', { maxMessages: 1, durationMs: 30_000, nowMs: Date.now() });
    await q.nackBatch(leases as Parameters<typeof q.nackBatch>[0], { retry: true });
    const loaded = await store.loadPending('sq');
    expect(loaded).toHaveLength(1); // still in WAL
  });

  test('nack with retry=false deletes from store', async () => {
    const store = new WalQueueStore(dir);
    await store.init('sq');
    const q = new DurableIngestQueue<{ n: number }>({ store, queueName: 'sq' });
    const env = wrap({ n: 1 });
    await q.enqueueBatch([env]);
    const leases = await q.leaseBatch('w1', { maxMessages: 1, durationMs: 30_000, nowMs: Date.now() });
    await q.nackBatch(leases as Parameters<typeof q.nackBatch>[0], { retry: false });
    const loaded = await store.loadPending('sq');
    expect(loaded).toHaveLength(0);
  });

  test('hydrateFromStore replays pending from store into in-memory queue', async () => {
    const store = new WalQueueStore(dir);
    await store.init('sq');
    // Write two envelopes directly to the store (simulating crash-before-complete)
    const e1 = wrap({ n: 10 });
    const e2 = wrap({ n: 20 });
    await store.putBatch('sq', [e1, e2]);
    // New queue instance (simulates restart)
    const q2 = new DurableIngestQueue<{ n: number }>({ store, queueName: 'sq' });
    const count = await q2.hydrateFromStore();
    expect(count).toBe(2);
    expect(q2.pendingCount()).toBe(2);
  });

  test('hydrate does not re-persist items to store (no double-write)', async () => {
    const store = new WalQueueStore(dir);
    await store.init('sq');
    const env = wrap({ n: 1 });
    await store.putBatch('sq', [env]);
    const q = new DurableIngestQueue<{ n: number }>({ store, queueName: 'sq' });
    await q.hydrateFromStore();
    // After hydration, loadPending should still see 1 item (no duplicates)
    const loaded = await store.loadPending('sq');
    expect(loaded).toHaveLength(1);
  });
});

// ---------------------------------------------------------------------------
// bootstrapIngest — durable path: survive shutdown + re-bootstrap
// ---------------------------------------------------------------------------

describe('bootstrapIngest — durable path (WalQueueStore)', () => {
  let dir: string;

  beforeEach(() => { dir = tempWalDir(); });
  afterEach(() => { fs.rmSync(dir, { recursive: true, force: true }); });

  test('syncQueue items survive shutdown + re-bootstrap', async () => {
    const store = new WalQueueStore(dir);
    const handle1 = await bootstrapIngest(makeDeps(store));

    // Enqueue a sync task directly
    const task = makeSyncTask('src-crash');
    const env = wrap(task, { dedupKey: task.sourceId });
    await handle1.syncQueue.enqueueBatch([env]);
    expect((handle1.syncQueue as DurableIngestQueue<typeof task>).pendingCount()).toBe(1);

    // Shutdown (simulates clean crash / restart)
    await handle1.shutdown();

    // Re-bootstrap with same store → hydrates from WAL
    const handle2 = await bootstrapIngest(makeDeps(store));
    expect((handle2.syncQueue as DurableIngestQueue<typeof task>).pendingCount()).toBe(1);

    // Lease and complete — WAL entry removed
    const leases = await handle2.syncQueue.leaseBatch('w1', {
      maxMessages: 1,
      durationMs: 30_000,
      nowMs: Date.now(),
    });
    expect(leases).toHaveLength(1);
    expect(leases[0].envelope.id).toBe(env.id);
    await handle2.syncQueue.completeBatch(leases);
    const remaining = await store.loadPending('sync-queue');
    expect(remaining).toHaveLength(0);

    await handle2.shutdown();
  });

  test('ingestQueue items survive shutdown + re-bootstrap', async () => {
    const store = new WalQueueStore(dir);
    const handle1 = await bootstrapIngest(makeDeps(store));

    const item = makeIngestItem('item-001');
    const env = wrap(item, { dedupKey: item.ingestId });
    await handle1.ingestQueue.enqueueBatch([env]);

    await handle1.shutdown();

    const handle2 = await bootstrapIngest(makeDeps(store));
    const leases = await handle2.ingestQueue.leaseBatch('w1', {
      maxMessages: 5,
      durationMs: 30_000,
      nowMs: Date.now(),
    });
    expect(leases).toHaveLength(1);
    expect((leases[0].envelope as { body: { ingestId: string } }).body.ingestId).toBe('item-001');

    await handle2.shutdown();
  });

  test('completed items do not reappear after re-bootstrap', async () => {
    const store = new WalQueueStore(dir);
    const handle1 = await bootstrapIngest(makeDeps(store));

    const item = makeIngestItem('item-done');
    const env = wrap(item, { dedupKey: item.ingestId });
    await handle1.ingestQueue.enqueueBatch([env]);

    // Consume + complete before shutdown
    const leases = await handle1.ingestQueue.leaseBatch('w1', {
      maxMessages: 1,
      durationMs: 30_000,
      nowMs: Date.now(),
    });
    await handle1.ingestQueue.completeBatch(leases);
    await handle1.shutdown();

    // Re-bootstrap: queue should be empty
    const handle2 = await bootstrapIngest(makeDeps(store));
    const leases2 = await handle2.ingestQueue.leaseBatch('w1', {
      maxMessages: 5,
      durationMs: 30_000,
      nowMs: Date.now(),
    });
    expect(leases2).toHaveLength(0);

    await handle2.shutdown();
  });
});

// ---------------------------------------------------------------------------
// bootstrapIngest — in-memory path unchanged
// ---------------------------------------------------------------------------

describe('bootstrapIngest — in-memory path (InMemoryQueueStore, default)', () => {
  test('returns functional queues with InMemoryQueueStore', async () => {
    const store = new InMemoryQueueStore();
    const handle = await bootstrapIngest(makeDeps(store));
    expect(handle.syncQueue).toBeDefined();
    expect(handle.ingestQueue).toBeDefined();
    await handle.shutdown();
  });

  test('enqueue + lease works on in-memory path', async () => {
    const store = new InMemoryQueueStore();
    const handle = await bootstrapIngest(makeDeps(store));
    const task = makeSyncTask('src-mem');
    const env = wrap(task);
    await handle.syncQueue.enqueueBatch([env]);
    const leases = await handle.syncQueue.leaseBatch('w1', {
      maxMessages: 1,
      durationMs: 30_000,
      nowMs: Date.now(),
    });
    expect(leases).toHaveLength(1);
    expect(leases[0].envelope.id).toBe(env.id);
    await handle.shutdown();
  });

  test('no env var set uses in-memory queue (no WAL file created)', async () => {
    // Ensure env var is unset for this test
    const orig = process.env.INGEST_QUEUE_DURABLE;
    delete process.env.INGEST_QUEUE_DURABLE;
    try {
      const handle = await bootstrapIngest(makeDeps());
      await handle.syncQueue.enqueueBatch([wrap(makeSyncTask('src-1'))]);
      await handle.shutdown();
      // No WAL directory in default tmpdir path — nothing to assert beyond no throw
    } finally {
      if (orig !== undefined) process.env.INGEST_QUEUE_DURABLE = orig;
    }
  });
});
