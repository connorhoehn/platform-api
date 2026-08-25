// src/services/call-reaction-consumer.ts
//
// Drains the durable `call:reaction.recorded` event (producer:
// websocket-gateway) into the call-reactions DDB table. Reactions are
// keyed by lobbyName so recording playback + transcript injection can
// query the exact [startedAt, endedAt] window of a recording.
//
// Idempotency:
//   - The sort key is `${at}#${reactionId}` — the gateway mints
//     reactionId once per reaction, so re-delivered messages overwrite
//     the same row. PutItem is safe to retry; no double-insert.
//
// Failure mode:
//   - Re-throws DDB write errors so the durable transport sees the
//     failure and re-delivers (at-least-once). One bad message does NOT
//     halt the consumer — it stays in the queue until the underlying
//     issue is fixed (DLQ after 3 attempts per the declaration).

import type { EventEnvelope, Subscription } from '@connorhoehn/event-catalog';
import { catalog } from '../lib/event-catalog';
import { callReactionRepo } from '../repositories';
import type { CallReactionInput } from '../repositories';
import log from '../lib/logger';

interface CallReactionPayload {
  reactionId: string;
  channel: string;
  lobbyName: string;
  userId?: string | null;
  displayName?: string;
  emoji: string;
  effect?: string;
  at: string; // ISO
}

/**
 * Start the call-reaction consumer. Returns a handle whose `stop()`
 * unsubscribes.
 *
 * Safe to call in tests with an injected catalog stub via the `_catalog`
 * parameter (default: the module-singleton `catalog`).
 */
export function startCallReactionConsumer(
  opts: {
    _catalog?: { subscribe: typeof catalog.subscribe };
  } = {},
): { stop: () => Promise<void> } {
  const cat = opts._catalog ?? catalog;

  const subscription: Subscription = cat.subscribe<CallReactionPayload>(
    'call:reaction.recorded',
    async (envelope: EventEnvelope<CallReactionPayload>) => {
      const payload = envelope.payload;

      const row: CallReactionInput = {
        reactionId: payload.reactionId,
        channel: payload.channel,
        lobbyName: payload.lobbyName,
        userId: payload.userId ?? null,
        displayName: payload.displayName,
        emoji: payload.emoji,
        effect: payload.effect,
        at: payload.at,
      };

      try {
        await callReactionRepo.putReaction(row);
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        log.error(
          { err: message, reactionId: payload.reactionId, lobbyName: payload.lobbyName },
          '[call-reaction-consumer] DDB write failed; will redeliver',
        );
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
        log.warn({ err: message }, '[call-reaction-consumer] unsubscribe failed');
      }
    },
  };
}
