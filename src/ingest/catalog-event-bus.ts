// Catalog-backed IngestEventBus implementation. Publishes ingest progress
// events via the event-catalog's SignalTransport (Redis pub/sub under the
// hood) so the gateway can subscribe and fan out to WebSocket clients.
//
// Channel: `ingest:events` (backward-compatible with the legacy
// RedisIngestEventBus channel; gateway-side relay handles fan-out to
// per-source channels based on event.sourceId).

import { ulid } from 'ulid';
import type { IngestEventBus } from './engine';
import type { IngestEvent } from './types';
import { catalog } from '../lib/event-catalog';
import type { EventEnvelope } from '@connorhoehn/event-catalog';

const CHANNEL = 'ingest:events';

export class CatalogIngestEventBus implements IngestEventBus {
  private readonly handlers: Array<(event: IngestEvent) => void> = [];

  emit(event: IngestEvent): void {
    // Local handlers (for in-process consumers like the platform-api bridge)
    for (const handler of this.handlers) {
      try {
        handler(event);
      } catch (err) {
        console.warn('[catalog-ingest-event-bus] local handler threw', err);
      }
    }

    // Publish to Redis via the catalog's signal transport (fire-and-forget)
    const envelope: EventEnvelope = {
      eventName: 'ingest:progress',
      payload: event,
      metadata: {
        id: ulid(),
        producer: 'platform-api',
        timestamp: Date.now(),
      },
    };

    const signal = catalog.getSignalTransport();
    signal.publish(CHANNEL, envelope).catch((err: unknown) => {
      const msg = err instanceof Error ? err.message : String(err);
      console.warn('[catalog-ingest-event-bus] publish failed:', msg);
    });
  }

  on(handler: (event: IngestEvent) => void): void {
    this.handlers.push(handler);
  }

  off(handler: (event: IngestEvent) => void): void {
    const idx = this.handlers.indexOf(handler);
    if (idx !== -1) this.handlers.splice(idx, 1);
  }
}

export { CHANNEL as INGEST_REDIS_CHANNEL };
