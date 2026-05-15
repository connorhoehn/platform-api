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
import log from '../lib/logger';

const CHANNEL = 'ingest:events';

export class CatalogIngestEventBus implements IngestEventBus {
  private readonly handlers: Array<(event: IngestEvent) => void> = [];

  emit(event: IngestEvent): void {
    // Local handlers (for in-process consumers like the platform-api bridge)
    for (const handler of this.handlers) {
      try {
        handler(event);
      } catch (err) {
        log.warn({ err }, '[catalog-ingest-event-bus] local handler threw');
      }
    }

    // Phase P3.2 — defensive validation against the registered schema
    // (which now carries the discriminator's enum constraint). Surfaces
    // sub-type drift at publish time; logged-only so a bad event never
    // blocks the ingest loop.
    try {
      const result = catalog.getRegistry().validate('ingest:progress', event);
      if (!result.valid) {
        const errors = result.errors?.map((e) => e.message).join('; ') ?? 'unknown';
        log.warn({ eventType: event.type, errors }, '[catalog-ingest-event-bus] ingest:progress validation failed');
      }
    } catch {
      // Registry lookup failures (e.g. event not yet declared) are non-fatal.
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
      log.warn({ err: msg }, '[catalog-ingest-event-bus] publish failed');
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
