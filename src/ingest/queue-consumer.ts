// platform-api/src/ingest/queue-consumer.ts
//
// Phase 54.6.3 — subscribes to the durable `ingest:item.queued` queue and
// routes each delivery to `IngestEngine.processItem`. Replaces the in-process
// partition queue + per-source worker coroutines that lived inside the engine
// before this phase. Per-source FIFO ordering is preserved through the queue
// (FIFO + partitionKey:sourceId, declared in src/lib/event-catalog.ts).
//
// Failure policy: re-throws so the durable transport sees the failure and
// re-delivers. The engine's processItem already marks rows 'failed' on its
// own (and increments attempts) — the consumer-level throw is the at-least-
// once fallback for transport-level failures (DDB outage, network blip).

import type { EventEnvelope, Subscription } from '@connorhoehn/event-catalog';
import { catalog } from '../lib/event-catalog';
import type { IngestEngine } from './engine';
import log from '../lib/logger';

interface IngestQueuedPayload {
  ingestId: string;
  sourceId: string;
  tags?: string[];
}

export interface StartIngestQueueConsumerOptions {
  engine: IngestEngine;
  _catalog?: { subscribe: typeof catalog.subscribe };
}

export function startIngestQueueConsumer(
  opts: StartIngestQueueConsumerOptions,
): { stop: () => Promise<void> } {
  const cat = opts._catalog ?? catalog;
  const engine = opts.engine;

  const subscription: Subscription = cat.subscribe<IngestQueuedPayload>(
    'ingest:item.queued',
    async (envelope: EventEnvelope<IngestQueuedPayload>) => {
      const { ingestId, tags } = envelope.payload;
      try {
        await engine.processItem(ingestId, tags);
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        log.error(
          { err: message, ingestId },
          '[ingest-queue-consumer] processItem failed; will redeliver',
        );
        throw err;
      }
    },
  );

  return {
    async stop() {
      try {
        await subscription.unsubscribe();
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        log.warn({ err: message }, '[ingest-queue-consumer] unsubscribe failed');
      }
    },
  };
}
