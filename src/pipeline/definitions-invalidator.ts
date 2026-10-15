// platform-api/src/pipeline/definitions-invalidator.ts
//
// Phase 54.6.2 — push-based cache invalidation for pipelineDefinitionsCache.
//
// The cache is refreshed locally by the writing replica via
// pokeCacheAfterWrite(), but OTHER replicas only see the change at their
// next 60s tick. Webhook routes on a stale replica route to the wrong
// pipeline; the schedule evaluator on a stale replica misses or
// double-fires depending on which leader-lock holder it is.
//
// Fix: every mutation publishes a Redis pub/sub message; every replica
// subscribes and triggers cache.refresh() on receipt.
//
// ROADMAP P2.2 (lane P1): while the subscription is live the cache drops its
// 60 s DynamoDB poll to a slow safety poll (`setEventDriven(true)`); a
// dropped connection restores the 60 s poll until the subscriber is ready
// again, and the reconnect refreshes once to cover the gap.

import { createClient, type RedisClientType } from 'redis';
import { getRedisClient } from '../lib/redis-client';
import { withContext } from '../lib/logger';
import type { PipelineDefinitionsCache } from './definitions-cache';

const log = withContext({ module: 'pipelineDefinitionsInvalidator' });

const REDIS_URL = `redis://${process.env.REDIS_ENDPOINT ?? 'redis'}:${process.env.REDIS_PORT ?? '6379'}`;
const CHANNEL = 'platform-api:pipeline-definitions:invalidate';

interface InvalidateMessage {
  reason: string;
  at: number;
  /** Optional originating replica id for debug-log only. */
  source?: string;
}

/**
 * Publish an invalidation notice. Fire-and-forget — failures are logged
 * and the 60s timer fallback in PipelineDefinitionsCache will eventually
 * surface the write.
 */
export async function publishInvalidate(reason: string): Promise<void> {
  try {
    const client = await getRedisClient();
    if (!client) {
      log.debug({ reason }, 'redis client unavailable; skipping invalidate publish');
      return;
    }
    const msg: InvalidateMessage = {
      reason,
      at: Date.now(),
      source: `${process.env.HOSTNAME ?? 'localhost'}-${process.pid}`,
    };
    await client.publish(CHANNEL, JSON.stringify(msg));
  } catch (err) {
    log.warn(
      { err: err instanceof Error ? err.message : String(err), reason },
      'definitions-invalidate publish failed (fallback to 60s timer)',
    );
  }
}

/**
 * Start a dedicated subscriber connection that calls cache.refresh() on
 * each invalidation message. node-redis requires a separate connection
 * for SUBSCRIBE (it can't share with the publishing client), so this
 * owns its own client.
 *
 * Returns a stop() handle for graceful shutdown.
 */
export async function startInvalidationSubscriber(
  cache: PipelineDefinitionsCache,
  opts: { _client?: RedisClientType } = {},
): Promise<{ stop: () => Promise<void> }> {
  let client: RedisClientType | null = null;
  try {
    client = opts._client ?? createClient({
      url: REDIS_URL,
      socket: {
        connectTimeout: 3_000,
        reconnectStrategy: (retries) => Math.min(1000 * (retries + 1), 30_000),
      },
    }) as RedisClientType;
    client.on('error', (err: Error) => {
      cache.setEventDriven(false);
      log.warn({ err: err.message }, 'definitions-invalidate subscriber error');
    });
    let subscribed = false;
    // node-redis resubscribes on reconnect; `ready` marks the channel live again.
    client.on('ready', () => { if (subscribed) cache.setEventDriven(true); });
    client.on('reconnecting', () => cache.setEventDriven(false));
    client.on('end', () => cache.setEventDriven(false));
    await client.connect();
    await client.subscribe(CHANNEL, (raw) => {
      let parsed: InvalidateMessage | null = null;
      try {
        parsed = JSON.parse(raw) as InvalidateMessage;
      } catch {
        log.warn({ raw }, 'malformed invalidate message; ignoring');
        return;
      }
      log.debug({ reason: parsed.reason, source: parsed.source }, 'invalidate received');
      // Fire-and-forget. cache.refresh() coalesces concurrent calls.
      void cache.refresh().catch((err: unknown) => {
        log.warn(
          { err: err instanceof Error ? err.message : String(err) },
          'cache refresh on invalidate failed',
        );
      });
    });
    subscribed = true;
    cache.setEventDriven(true);
    log.info('definitions-invalidate subscriber started');
    return {
      async stop() {
        cache.setEventDriven(false);
        if (!client) return;
        try {
          await client.unsubscribe(CHANNEL);
        } catch { /* best-effort */ }
        try {
          await client.quit();
        } catch { /* best-effort */ }
      },
    };
  } catch (err) {
    cache.setEventDriven(false);
    // A client that never connected must not keep reconnecting in the background.
    try { if (client && !opts._client) client.destroy(); } catch { /* best-effort */ }
    log.warn(
      { err: err instanceof Error ? err.message : String(err) },
      'definitions-invalidate subscriber failed to start (60s timer will still refresh)',
    );
    return { stop: async () => { /* never started */ } };
  }
}
