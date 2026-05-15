// platform-api/src/ingest/bridge/ingest-bridge.ts
//
// Bridge between ingest EventBus and WebSocket clients. Subscribes to ingest
// progress events and forwards them to WebSocket channel `ingest:progress`.
//
// Follows the pattern of pipeline-bridge: EventBus subscriber → WebSocket fan-out.
// Clients subscribe via WebSocket channel registration in gateway message-router.

import type { IngestEventBus } from '../engine';
import type { IngestEvent } from '../types';
import log from '../../lib/logger';

/**
 * WebSocket broadcast function signature. Implementations send messages to all
 * clients subscribed to a given channel.
 */
export type WebSocketBroadcast = (channel: string, message: unknown) => void;

export interface IngestBridgeConfig {
  /** Event bus to subscribe to for ingest progress events. */
  eventBus: IngestEventBus;
  /** WebSocket broadcast function (injected from gateway layer). */
  broadcast: WebSocketBroadcast;
}

/**
 * Bridge handle for lifecycle management. Call stop() during shutdown to
 * unsubscribe from the event bus.
 */
export interface IngestBridge {
  /** Unsubscribe from event bus and stop forwarding messages. */
  stop: () => void;
}

const INGEST_CHANNEL = 'ingest:progress';

/**
 * Wire up ingest EventBus → WebSocket bridge. All IngestEvent messages are
 * forwarded to WebSocket clients subscribed to the `ingest:progress` channel.
 *
 * Start immediately upon creation. Call `stop()` during graceful shutdown.
 */
export function createIngestBridge(config: IngestBridgeConfig): IngestBridge {
  const { eventBus, broadcast } = config;

  // Handler: forward ingest events to WebSocket clients
  const handler = (event: IngestEvent): void => {
    try {
      broadcast(INGEST_CHANNEL, event);
    } catch (err) {
      log.error({ err }, '[ingest-bridge] Failed to broadcast event');
    }
  };

  // Subscribe to event bus
  eventBus.on(handler);

  log.info({ channel: INGEST_CHANNEL }, '[ingest-bridge] Started, forwarding to channel');

  // Shutdown
  const stop = (): void => {
    eventBus.off(handler);
    log.info('[ingest-bridge] Stopped');
  };

  return { stop };
}
