// social-api/src/ingest/bridge/ingest-bridge.ts
//
// Bridge between ingest EventBus and WebSocket clients. Subscribes to ingest
// progress events and forwards them to WebSocket channel `ingest:progress`.
//
// Follows the pattern of pipeline-bridge: EventBus subscriber → WebSocket fan-out.
// Clients subscribe via WebSocket channel registration in gateway message-router.

import type { IngestEventBus } from '../engine';
import type { IngestEvent } from '../types';

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
      console.error('[ingest-bridge] Failed to broadcast event:', err);
    }
  };

  // Subscribe to event bus
  eventBus.on(handler);

  console.log(`[ingest-bridge] Started, forwarding to channel: ${INGEST_CHANNEL}`);

  // Shutdown
  const stop = (): void => {
    eventBus.off(handler);
    console.log('[ingest-bridge] Stopped');
  };

  return { stop };
}
