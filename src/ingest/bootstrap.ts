// social-api/src/ingest/bootstrap.ts
//
// Bootstrap for the ingest engine. Wires up distributed-core primitives:
// - InMemoryBatchedQueue (sync tasks) + InMemoryBatchedQueue (ingest items)
// - InMemoryIdempotentProducer for deduplicating re-syncs (with InMemoryDedupStore)
// - SourceSyncLock for per-source sync locking (simple in-memory mutex)
// - EventBus for progress events
// - InMemoryDeadLetterQueue for failed items
//
// Follows the composition-layer pattern from pipeline/bootstrap.ts but
// scoped to ingest concerns. Single-node posture (in-memory transport)
// consistent with CONSTITUTION path-(a).

import {
  InMemoryBatchedQueue,
  InMemoryIdempotentProducer,
  InMemoryDedupStore,
  EventBusFactory,
  InMemoryDeadLetterQueue,
} from 'distributed-core';
import type { EventBus, ProducerSink, Envelope } from 'distributed-core';

import type {
  BatchedQueue,
  IdempotentProducer,
  DeadLetterQueue,
} from 'distributed-core';

import type { IngestSourceRepository } from './repositories/IngestSourceRepository';
import type { IngestItemRepository } from './repositories/IngestItemRepository';
import type { SyncCursorRepository } from './repositories/SyncCursorRepository';
import type { TypedDocumentRepository } from '../repositories/TypedDocumentRepository';
import type { SyncTask, IngestItem } from './types';

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/** Simple per-source sync lock for single-node posture. */
export interface SourceSyncLock {
  /** Acquire the lock. Resolves true if acquired, false if already held. */
  acquire(): Promise<boolean>;
  /** Release the lock. */
  release(): Promise<void>;
}

export interface IngestBootstrapDeps {
  sourceRepository: IngestSourceRepository;
  itemRepository: IngestItemRepository;
  cursorRepository: SyncCursorRepository;
  documentRepository: TypedDocumentRepository;
}

export interface IngestBootstrapHandle {
  /** Sync task queue — manual trigger enqueues here. */
  syncQueue: BatchedQueue<SyncTask>;
  /** Ingest item queue — sync worker enqueues discovered files. */
  ingestQueue: BatchedQueue<IngestItem>;
  /** Idempotent producer for sync tasks. */
  syncProducer: IdempotentProducer<SyncTask>;
  /** Idempotent producer for ingest items. */
  ingestProducer: IdempotentProducer<IngestItem>;
  /** EventBus for progress events — bridge subscribes to this. */
  eventBus: EventBus;
  /** Dead-letter queue for failed ingestion items. */
  dlq: DeadLetterQueue<IngestItem>;
  /** Creates a SourceSyncLock for a source (one sync per source). */
  createSession: (sourceId: string) => SourceSyncLock;
  /** Graceful shutdown — stops workers, closes queues. */
  shutdown: () => Promise<void>;
}

export interface IngestBootstrapOptions {
  /** Deduplication window for sync tasks (ms, default: 60000). */
  syncDedupWindowMs?: number;
  /** Deduplication window for ingest items (ms, default: 300000). */
  ingestDedupWindowMs?: number;
  /** DLQ attempt history cap (default: 10). */
  dlqCapacity?: number;
}

// ---------------------------------------------------------------------------
// Bootstrap
// ---------------------------------------------------------------------------

/**
 * Wire up distributed-core primitives for the ingest engine. Returns handles
 * for the queues, producers, event bus, and shutdown. Does NOT start worker
 * loops — caller is responsible for subscribing handlers to the queues.
 *
 * Idempotent only at the call site — callers must not bootstrap twice.
 */
export async function bootstrapIngest(
  deps: IngestBootstrapDeps,
  opts: IngestBootstrapOptions = {},
): Promise<IngestBootstrapHandle> {
  const {
    syncDedupWindowMs = 60_000,
    ingestDedupWindowMs = 300_000,
    dlqCapacity = 10,
  } = opts;

  // Sync task queue (manual/scheduled sync triggers)
  const syncQueue = new InMemoryBatchedQueue<SyncTask>();

  // Ingest item queue (files discovered during sync)
  const ingestQueue = new InMemoryBatchedQueue<IngestItem>();

  // Dead-letter queue for failed ingest items
  const dlq = new InMemoryDeadLetterQueue<IngestItem>({
    redriveSink: {
      enqueue: async (env: Envelope<IngestItem>) => {
        await ingestQueue.enqueueBatch([env]);
      },
    },
    attemptHistoryCap: dlqCapacity,
  });

  // ProducerSink adapters — bridge single-item enqueue to enqueueBatch
  const syncSink: ProducerSink<SyncTask> = {
    enqueue: async (env: Envelope<SyncTask>) => {
      await syncQueue.enqueueBatch([env]);
    },
  };
  const ingestSink: ProducerSink<IngestItem> = {
    enqueue: async (env: Envelope<IngestItem>) => {
      await ingestQueue.enqueueBatch([env]);
    },
  };

  // Idempotent producers (dedup by source or file identity)
  const syncProducer = new InMemoryIdempotentProducer<SyncTask>({
    sink: syncSink,
    dedupStore: new InMemoryDedupStore(),
    windowMs: syncDedupWindowMs,
  });

  const ingestProducer = new InMemoryIdempotentProducer<IngestItem>({
    sink: ingestSink,
    dedupStore: new InMemoryDedupStore(),
    windowMs: ingestDedupWindowMs,
  });

  // EventBus for progress events (bridged to WebSocket). Uses
  // EventBusFactory.createLocal() for the in-process single-node posture.
  const eventBus: EventBus = EventBusFactory.createLocal('ingest', {
    topic: 'ingest',
    autoCompactOptions: { keepLastNPerType: 100 },
  });

  // Per-source sync locks (single-node in-memory mutex)
  const locks = new Map<string, { held: boolean }>();
  const createSession = (sourceId: string): SourceSyncLock => {
    if (!locks.has(sourceId)) {
      locks.set(sourceId, { held: false });
    }
    const lock = locks.get(sourceId)!;
    return {
      acquire: async () => {
        if (lock.held) return false;
        lock.held = true;
        return true;
      },
      release: async () => {
        lock.held = false;
      },
    };
  };

  // Shutdown orchestration
  let shutdownCalled = false;
  const shutdown = async (): Promise<void> => {
    if (shutdownCalled) return;
    shutdownCalled = true;
    locks.clear();
  };

  return {
    syncQueue,
    ingestQueue,
    syncProducer,
    ingestProducer,
    eventBus,
    dlq,
    createSession,
    shutdown,
  };
}
