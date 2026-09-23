/**
 * PipelineEventRedisRelay — EventBus → Redis pub/sub egress for live pipeline
 * signals.
 *
 * WIRE CONTRACT (consumer: `websocket-gateway/src/pipeline-relay.ts`)
 *
 * - The gateway pSUBSCRIBEs `dcore:topic:pipeline.events.*` whenever no
 *   MESSAGE_QUEUE_URL / PIPELINE_EVENT_BUS_URL is configured, and re-emits
 *   whatever it receives on its `pipelineEventSource` EventEmitter.
 * - Channel: `dcore:topic:pipeline.events.<type-with-dots>` — the BusEvent
 *   `type` with its `pipeline:` prefix stripped and remaining colons turned
 *   into dots (`pipeline:step:started` → `…pipeline.events.step.started`).
 *   The prefix matches distributed-core's RedisPubSubManager default
 *   (`channelPrefix: 'dcore:topic:'`), so a real pub/sub manager can take
 *   this over later without touching the consumer.
 * - Body: the raw BusEvent as JSON. The gateway also accepts a wrapped
 *   `{ topic, payload }` envelope; it requires `.type` to be a string on
 *   whichever shape arrives, so the unwrapped BusEvent is the simpler
 *   choice.
 *
 * This is the LIVE path and is distinct from the `publishDurable(...)`
 * subscriptions in bootstrap.ts, which write a DynamoDB outbox row for
 * external consumers. Only run-level events go durable; the step-level ones
 * (`pipeline:step:started` / `:completed` / `:failed` / `:skipped`) have no
 * other egress, which is why the canvas showed every node as
 * `data-state="idle"` for a whole run before this existed.
 *
 * Failure policy: a relay problem must never take pipeline bootstrap down or
 * fail a run, so every path here swallows its own errors and logs. Publishing
 * re-resolves the shared client each time, which means a Redis outage
 * degrades to "no live canvas updates" and self-heals on reconnect.
 */
import type { BusEvent } from 'distributed-core';
import { getRedisClient } from '../lib/redis-client';
import log from '../lib/logger';

const CHANNEL_PREFIX = 'dcore:topic:pipeline.events.';
const TYPE_PREFIX = 'pipeline:';

const relayLog = log.child({ component: 'pipeline-event-redis-relay' });

export interface PipelineEventRedisRelay {
  /** Channel suffix count published so far — test/observability affordance. */
  readonly published: number;
  stop(): Promise<void>;
}

interface StartOpts {
  /** distributed-core EventBus from `module.getEventBus()`. */
  eventBus: {
    subscribeAll(handler: (event: BusEvent) => Promise<void>): string;
    unsubscribe(subscriptionId: string): void;
  };
  /**
   * The EventBus's own transport already publishes every BusEvent on a
   * channel the gateway's pattern matches: Redis pubsub, topic
   * `pipeline.events.<nodeId>` → `dcore:topic:pipeline.events.<nodeId>`, on
   * the same Redis. Relaying as well delivered every event to the gateway
   * twice (NFR #38), so the relay stands down. `PIPELINE_EVENT_RELAY=1`
   * forces it on anyway.
   */
  busReachesGateway?: boolean;
}

/** Same Redis server? Compares host and port only (auth/db do not change which PUBLISH the gateway hears). */
export function sameRedisTarget(a: string | undefined, b: string | undefined): boolean {
  if (!a || !b) return false;
  try {
    const left = new URL(a);
    const right = new URL(b);
    return left.hostname === right.hostname && (left.port || '6379') === (right.port || '6379');
  } catch {
    return false;
  }
}

/**
 * `pipeline:step:started` → `dcore:topic:pipeline.events.step.started`
 */
export function channelForType(type: string): string {
  const suffix = type.startsWith(TYPE_PREFIX) ? type.slice(TYPE_PREFIX.length) : type;
  return CHANNEL_PREFIX + suffix.replace(/:/g, '.');
}

/**
 * Subscribe to every `pipeline:*` BusEvent and forward it to Redis.
 *
 * Returns null (rather than throwing) when the relay is disabled or cannot
 * be wired at all, so the caller can treat "no relay" as a normal state.
 */
export async function startPipelineEventRedisRelay(
  { eventBus, busReachesGateway = false }: StartOpts,
): Promise<PipelineEventRedisRelay | null> {
  if (process.env.PIPELINE_EVENT_RELAY === '0') {
    relayLog.info('pipeline event Redis relay DISABLED (PIPELINE_EVENT_RELAY=0)');
    return null;
  }
  if (busReachesGateway && process.env.PIPELINE_EVENT_RELAY !== '1') {
    relayLog.info(
      `pipeline event Redis relay not started: the EventBus already publishes to ${CHANNEL_PREFIX}<nodeId> on the gateway's Redis, `
      + 'and relaying too delivered every event twice (NFR #38). PIPELINE_EVENT_RELAY=1 forces it on.',
    );
    return null;
  }

  let published = 0;
  let stopped = false;

  const subId = eventBus.subscribeAll(async (event: BusEvent) => {
    if (stopped) return;
    if (typeof event?.type !== 'string' || !event.type.startsWith(TYPE_PREFIX)) return;

    const channel = channelForType(event.type);
    try {
      // Lazy per-publish resolve: the shared client nulls itself out on error,
      // so this is also the reconnect path. null = Redis down or in backoff.
      const client = await getRedisClient();
      if (!client) return;
      await client.publish(channel, JSON.stringify(event));
      published++;
    } catch (err) {
      relayLog.warn(
        { err: err instanceof Error ? err.message : String(err), channel, type: event.type },
        'failed to publish pipeline event to Redis',
      );
    }
  });

  // Warm the connection so a Redis misconfiguration shows up at boot rather
  // than as silently missing canvas updates on the first run. Best-effort:
  // getRedisClient() already logs and returns null instead of throwing.
  await getRedisClient();

  relayLog.info(`pipeline event Redis relay started (pattern=${CHANNEL_PREFIX}*)`);

  return {
    get published() { return published; },
    async stop(): Promise<void> {
      if (stopped) return;
      stopped = true;
      try {
        eventBus.unsubscribe(subId);
      } catch (err) {
        relayLog.warn(
          { err: err instanceof Error ? err.message : String(err) },
          'eventBus.unsubscribe() failed during relay stop',
        );
      }
      // The Redis connection is the shared client — owned by redis-client.ts
      // and used by the cache layer too, so it is deliberately NOT closed here.
    },
  };
}
