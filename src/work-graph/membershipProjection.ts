/**
 * A person's conversations, kept in the cache Redis (NFR #16, realtime-examples).
 *
 * `chat-members` is keyed `(channel, userId)`, so "which conversations is this
 * person in" was a filtered Scan of the whole table on every work-graph
 * request (viewer resolve, policy revision, conversation audiences). This is
 * the maintained answer: `wg:conv:<userId>` in the cache Redis (allkeys-lru —
 * a shortcut, never the truth), filled on a miss from the same Scan.
 *
 * Invalidation is event-driven. Every committed membership write in the
 * gateway (`withMembershipAccessSignal`, NFR #105) publishes
 * `work-graph.access.changed {viewerId, reason: 'membership'}` over dc
 * pub/sub on the main Redis. Every platform replica subscribes and bumps
 * `wg:conv-gen:<userId>` (and drops the value) in the shared cache, so a
 * change committed through either gateway reaches every platform replica's
 * next read. A value is only believed when it was filled under the current
 * generation, which closes the fill-vs-invalidate race (a Scan that started
 * before the write finishing after the signal).
 *
 * The gateway re-asks `/authorize` the moment it sends that signal, racing
 * this subscriber; that path runs inside `withFreshMembership` and always
 * reads DynamoDB (then refreshes the cache), so a revocation is never judged
 * against a value the signal has not reached yet. Writes that bypass the
 * gateway (seed scripts) are bounded by the TTL.
 */
import { AsyncLocalStorage } from 'node:async_hooks';
import { createClient, type RedisClientType } from 'redis';

import { getRedisCacheClient, sharedRedisUrl } from '../lib/redis-client';
import log from '../lib/logger';

export const MEMBERSHIP_CACHE_TTL_S = 120;
const GEN_TTL_S = 24 * 3600;
const ACCESS_CHANGED_TOPIC = 'work-graph.access.changed';

const valueKey = (userId: string): string => `wg:conv:${userId}`;
const genKey = (userId: string): string => `wg:conv-gen:${userId}`;

type CacheClient = Pick<RedisClientType, 'mGet' | 'get' | 'set' | 'incr' | 'expire' | 'del'>;

const fresh = new AsyncLocalStorage<true>();

/** Runs `work` with membership read from DynamoDB, not the cache (the access re-check path). */
export function withFreshMembership<T>(work: () => Promise<T>): Promise<T> {
  return fresh.run(true, work);
}

export interface MembershipCounters { hits: number; misses: number; fresh: number; invalidations: number; unavailable: number }
export const membershipCounters: MembershipCounters = { hits: 0, misses: 0, fresh: 0, invalidations: 0, unavailable: 0 };

export class MembershipProjection {
  constructor(private readonly cache: () => Promise<CacheClient | null> = getRedisCacheClient) {}

  /** The person's conversation ids — from the cache when its generation still holds, else `load()`. */
  async conversationIds(userId: string, load: () => Promise<string[]>): Promise<string[]> {
    const cache = await this.cache().catch(() => null);
    if (!cache) {
      membershipCounters.unavailable += 1;
      return load();
    }
    let generation = '0';
    try {
      if (fresh.getStore()) {
        membershipCounters.fresh += 1;
        generation = (await cache.get(genKey(userId))) ?? '0';
      } else {
        const [raw, gen] = await cache.mGet([valueKey(userId), genKey(userId)]);
        generation = gen ?? '0';
        const cached = raw ? JSON.parse(raw) as { gen?: string; ids?: unknown } : null;
        if (cached && cached.gen === generation && Array.isArray(cached.ids)) {
          membershipCounters.hits += 1;
          return cached.ids.filter((id): id is string => typeof id === 'string');
        }
        membershipCounters.misses += 1;
      }
    } catch (err) {
      log.warn({ err: (err as Error).message }, 'membership projection read failed; reading DynamoDB');
      return load();
    }
    // The generation was read BEFORE the load: a signal landing during it
    // bumps the generation, and this fill is then never believed.
    const ids = await load();
    await cache.set(valueKey(userId), JSON.stringify({ gen: generation, ids }), { EX: MEMBERSHIP_CACHE_TTL_S })
      .catch(() => undefined);
    return ids;
  }

  async invalidate(userId: string): Promise<void> {
    const cache = await this.cache().catch(() => null);
    if (!cache) return;
    membershipCounters.invalidations += 1;
    await cache.incr(genKey(userId));
    await Promise.all([cache.expire(genKey(userId), GEN_TTL_S), cache.del(valueKey(userId))]);
  }
}

export const membershipProjection = new MembershipProjection();

/** The viewer a `work-graph.access.changed` wire message names for a membership change, if any. */
export function membershipChangeViewer(raw: string): string | null {
  try {
    const wire = JSON.parse(raw) as { payload?: { viewerId?: unknown; reason?: unknown } };
    const payload = wire?.payload;
    return payload?.reason === 'membership' && typeof payload.viewerId === 'string' && payload.viewerId
      ? payload.viewerId
      : null;
  } catch {
    return null;
  }
}

/**
 * Subscribes to the membership signal on the main Redis (its own connection:
 * node-redis cannot SUBSCRIBE on a client it also commands).
 */
export async function startMembershipInvalidationSubscriber(
  projection: MembershipProjection = membershipProjection,
): Promise<{ stop: () => Promise<void> }> {
  const client = createClient({
    url: sharedRedisUrl(),
    socket: { connectTimeout: 3_000, reconnectStrategy: (retries) => Math.min(1000 * (retries + 1), 30_000) },
  }) as RedisClientType;
  client.on('error', (err: Error) => log.warn({ err: err.message }, 'membership-invalidate subscriber error'));
  await client.connect();
  await client.subscribe(ACCESS_CHANGED_TOPIC, (raw) => {
    const viewerId = membershipChangeViewer(raw);
    if (!viewerId) return;
    void projection.invalidate(viewerId).catch((err: unknown) => {
      log.warn({ err: (err as Error).message, viewerId }, 'membership projection invalidate failed');
    });
  });
  log.info('membership-invalidate subscriber started');
  return {
    async stop() {
      try { await client.unsubscribe(ACCESS_CHANGED_TOPIC); } catch { /* best-effort */ }
      try { await client.quit(); } catch { /* best-effort */ }
    },
  };
}
