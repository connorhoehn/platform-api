// src/services/activity-consumer.ts
//
// Phase 57b — drains `activity.recorded` from message-queue and persists
// to the user-activity DDB table. The producer side is the
// websocket-gateway's ActivityService dual-publish (Phase 57a).
//
// Idempotency:
//   - The sort key includes the catalog-generated envelope id (ulid), so
//     re-delivered messages overwrite the same row with the same payload.
//     PutItem is safe to retry; no double-insert.
//
// Failure mode:
//   - Re-throws DDB write errors so the durable transport sees the
//     failure and re-delivers (at-least-once). One bad message does NOT
//     halt the consumer — it just stays in the queue until the underlying
//     issue is fixed.
//
// Phase 57c will remove the parallel inline DDB write in the gateway
// once this consumer is proven stable.

import { PutCommand } from '@aws-sdk/lib-dynamodb';
import type { EventEnvelope, Subscription } from '@connorhoehn/event-catalog';
import { catalog } from '../lib/event-catalog';
import { docClient } from '../lib/aws-clients';
import { tableName } from '../lib/ddb-table-name';
import log from '../lib/logger';

const TABLE = tableName(process.env.DYNAMODB_ACTIVITY_TABLE || 'user-activity');

interface ActivityPayload {
  eventType: string;
  detail?: Record<string, unknown>;
  timestamp: string;
  userId?: string | null;
  displayName?: string;
}

/**
 * Start the activity consumer. Returns a handle whose `stop()` unsubscribes.
 *
 * Safe to call in tests with an injected catalog stub via the `_catalog`
 * parameter (default: the module-singleton `catalog`).
 */
export function startActivityConsumer(
  opts: { _catalog?: { subscribe: typeof catalog.subscribe } } = {},
): { stop: () => Promise<void> } {
  const cat = opts._catalog ?? catalog;

  const subscription: Subscription = cat.subscribe<ActivityPayload>(
    'activity.recorded',
    async (envelope: EventEnvelope<ActivityPayload>) => {
      const payload = envelope.payload;

      // Activity rows are keyed on userId; anonymous events have nowhere
      // to land. Drop them — they're already captured in Redis history
      // and broadcast live to subscribers.
      if (!payload.userId) {
        return;
      }

      // Composite sort key: ISO timestamp + envelope id. Re-deliveries of
      // the same envelope hash to the same row → PutItem is idempotent.
      const sortKey = `${payload.timestamp}#${envelope.metadata.id}`;

      try {
        await docClient.send(new PutCommand({
          TableName: TABLE,
          Item: {
            userId: payload.userId,
            timestamp: sortKey,
            eventType: payload.eventType,
            detail: JSON.stringify(payload.detail ?? {}),
            displayName: payload.displayName ?? 'anonymous',
          },
        }));
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        log.error({ err: message, eventType: payload.eventType }, '[activity-consumer] DDB write failed; will redeliver');
        throw err; // propagate so durable transport redelivers
      }
    },
  );

  return {
    async stop() {
      try {
        await subscription.unsubscribe();
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        log.warn({ err: message }, '[activity-consumer] unsubscribe failed');
      }
    },
  };
}
