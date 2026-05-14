// platform-api/src/ingest/transport.ts
//
// Wave2-PA-2 — transport selector for the ingest item handoff.
//
// Two modes, gated by `PLATFORM_API_INGEST_TRANSPORT`:
//
//   `memory` (default): producer enqueues into an in-process queue keyed by
//     sourceId. The consumer dispatches to engine.processItem() within the
//     same Node process. Per-source FIFO is preserved by a per-source promise
//     chain. This is the safe-rollout default — zero new infra dependencies,
//     identical semantics to the pre-Phase 54.6.3 partition worker code.
//     Trade-off: items in flight at SIGTERM are lost (in-process).
//
//   `mq`: producer writes to the durable `ingest:item.queued` queue via the
//     event-catalog outbox + message-queue transport. Consumer is a long-poll
//     `catalog.subscribe()`. Survives platform-api restarts (MQ Phase 1
//     durability via MESSAGE_QUEUE_STORE=ddb). Multi-replica safe — any
//     replica can pick up any item. Per-source FIFO preserved by the queue's
//     `fifo: true` + `partitionKey: sourceId` declaration (see
//     src/lib/event-catalog.ts).
//
// The selector is read once at bootstrap. Switching modes requires a restart;
// in-flight items in `memory` are not migrated to `mq`.

import type { IngestEngine, EnqueueItem } from './engine';
import { publishDurable, catalog } from '../lib/event-catalog';
import type {
  EventEnvelope,
  EventHandler,
  Subscription,
} from '@connorhoehn/event-catalog';
import log from '../lib/logger';

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

export type IngestTransportMode = 'memory' | 'mq';

export interface IngestTransport {
  /** Producer side — engine injects this as `enqueueItem`. */
  enqueueItem: EnqueueItem;
  /** Consumer side — bootstrap calls this once the engine is constructed. */
  startConsumer(engine: IngestEngine): { stop: () => Promise<void> };
  /** Which mode is active. Reported for log/observability. */
  mode: IngestTransportMode;
}

export interface CreateIngestTransportOptions {
  /** Override the env-var read (tests). */
  mode?: IngestTransportMode;
  /**
   * Test injection seam for the mq-mode consumer side. Defaults to the
   * shared `catalog` singleton; tests pass a fake with a `.subscribe` shape
   * matching `EventCatalog.subscribe`.
   */
  _catalog?: { subscribe: typeof catalog.subscribe };
  /**
   * Test injection seam for the mq-mode producer side. Defaults to
   * `publishDurable('ingest:item.queued', ...)`; tests pass a capture.
   */
  _publishDurable?: typeof publishDurable;
}

// ---------------------------------------------------------------------------
// Mode resolution
// ---------------------------------------------------------------------------

/**
 * Resolve the configured transport mode. Defaults to `memory` for safe
 * rollout — operators flip to `mq` once message-queue durability is proven
 * in their environment.
 */
export function resolveIngestTransportMode(
  raw: string | undefined = process.env.PLATFORM_API_INGEST_TRANSPORT,
): IngestTransportMode {
  const v = (raw ?? '').trim().toLowerCase();
  if (v === 'mq') return 'mq';
  if (v === 'memory' || v === '') return 'memory';
  log.warn(
    { value: raw },
    `[ingest-transport] unrecognized PLATFORM_API_INGEST_TRANSPORT="${raw}"; falling back to "memory"`,
  );
  return 'memory';
}

// ---------------------------------------------------------------------------
// In-memory transport
// ---------------------------------------------------------------------------

interface QueuedItem {
  ingestId: string;
  sourceId: string;
  tags?: string[];
}

/**
 * Per-source FIFO in-process transport. Each enqueue chains onto the
 * source's existing promise so items for the same sourceId are processed
 * serially, even when the producer fires them in parallel. Across distinct
 * sourceIds, processing is concurrent.
 *
 * Failure policy: if the engine's processItem throws, the error is logged
 * and the chain continues (mirrors the pre-Phase 54.6.3 partition worker
 * which caught + logged per-item errors). The engine already records
 * `failed` status on the item itself.
 */
export function createMemoryIngestTransport(): IngestTransport {
  // Per-source promise chains preserve FIFO for items sharing a sourceId.
  const chains = new Map<string, Promise<void>>();
  // Single registered engine — populated by startConsumer. Until then,
  // enqueues queue up against `pendingBeforeStart`.
  let engine: IngestEngine | null = null;
  const pendingBeforeStart: QueuedItem[] = [];
  let stopped = false;

  const dispatch = (item: QueuedItem): Promise<void> => {
    if (!engine) {
      // Should not happen in practice — startConsumer runs before triggerSync
      // can fire. Defensive: log and drop.
      log.warn(
        { ingestId: item.ingestId },
        '[ingest-transport:memory] no engine registered; dropping item',
      );
      return Promise.resolve();
    }
    return engine.processItem(item.ingestId, item.tags).catch((err) => {
      const msg = err instanceof Error ? err.message : String(err);
      log.error(
        { err: msg, ingestId: item.ingestId, sourceId: item.sourceId },
        '[ingest-transport:memory] processItem failed',
      );
      // Swallow: engine already recorded `failed` on the item row.
    });
  };

  const enqueueItem: EnqueueItem = async (params) => {
    if (stopped) return;
    const item: QueuedItem = {
      ingestId: params.ingestId,
      sourceId: params.sourceId,
      tags: params.tags,
    };
    if (!engine) {
      pendingBeforeStart.push(item);
      return;
    }
    const prev = chains.get(item.sourceId) ?? Promise.resolve();
    const next = prev.then(() => dispatch(item));
    chains.set(item.sourceId, next);
    // Schedule cleanup: when this is the tail of its source's chain, drop the
    // map entry to avoid unbounded growth across long-lived processes.
    void next.finally(() => {
      if (chains.get(item.sourceId) === next) {
        chains.delete(item.sourceId);
      }
    });
  };

  const startConsumer: IngestTransport['startConsumer'] = (e) => {
    engine = e;
    // Drain anything that arrived before the consumer was registered.
    const drained = pendingBeforeStart.splice(0);
    for (const item of drained) {
      const prev = chains.get(item.sourceId) ?? Promise.resolve();
      const next = prev.then(() => dispatch(item));
      chains.set(item.sourceId, next);
      void next.finally(() => {
        if (chains.get(item.sourceId) === next) {
          chains.delete(item.sourceId);
        }
      });
    }
    return {
      stop: async () => {
        stopped = true;
        // Wait for any in-flight chains to drain so a SIGTERM doesn't yank
        // half-processed items mid-flight.
        await Promise.allSettled([...chains.values()]);
        chains.clear();
        engine = null;
      },
    };
  };

  return { enqueueItem, startConsumer, mode: 'memory' };
}

// ---------------------------------------------------------------------------
// Message-queue transport
// ---------------------------------------------------------------------------

/**
 * Durable message-queue transport. Producer side writes to the event-catalog
 * outbox (`publishDurable`); the outbox-drainer moves rows to message-queue.
 * Consumer side long-polls `ingest:item.queued` via `catalog.subscribe()`,
 * which the catalog library routes to its durable transport when the event
 * is declared `transport: 'durable'`.
 *
 * Restart-safety: items written to the outbox or already in the MQ survive
 * platform-api restarts (the outbox row is `UNPROCESSED` until drained; MQ
 * messages stay queued until a consumer acknowledges).
 */
export function createMqIngestTransport(
  opts: Pick<CreateIngestTransportOptions, '_catalog' | '_publishDurable'> = {},
): IngestTransport {
  const cat = opts._catalog ?? catalog;
  const publish = opts._publishDurable ?? publishDurable;

  const enqueueItem: EnqueueItem = async ({ ingestId, sourceId, tags }) => {
    await publish('ingest:item.queued', { ingestId, sourceId, tags });
  };

  const startConsumer: IngestTransport['startConsumer'] = (engine) => {
    interface IngestQueuedPayload {
      ingestId: string;
      sourceId: string;
      tags?: string[];
    }
    const handler: EventHandler<IngestQueuedPayload> = async (
      envelope: EventEnvelope<IngestQueuedPayload>,
    ) => {
      const { ingestId, tags } = envelope.payload;
      try {
        await engine.processItem(ingestId, tags);
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        log.error(
          { err: msg, ingestId },
          '[ingest-transport:mq] processItem failed; will redeliver',
        );
        // Re-throw so the durable transport retries via its DLQ-after-N policy.
        throw err;
      }
    };
    const subscription: Subscription = cat.subscribe<IngestQueuedPayload>(
      'ingest:item.queued',
      handler,
    );
    return {
      stop: async () => {
        try {
          await subscription.unsubscribe();
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          log.warn({ err: msg }, '[ingest-transport:mq] unsubscribe failed');
        }
      },
    };
  };

  return { enqueueItem, startConsumer, mode: 'mq' };
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

export function createIngestTransport(
  opts: CreateIngestTransportOptions = {},
): IngestTransport {
  const mode = opts.mode ?? resolveIngestTransportMode();
  log.info({ mode }, '[ingest-transport] selected');
  if (mode === 'mq') {
    return createMqIngestTransport(opts);
  }
  return createMemoryIngestTransport();
}
