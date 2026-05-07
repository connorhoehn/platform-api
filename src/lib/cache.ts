/**
 * Read-through cache utilities backed by Redis.
 *
 * Pattern: check Redis first → fall back to DynamoDB → populate cache on miss.
 * If Redis is unavailable, every call silently falls through to DynamoDB.
 *
 * Cache keys:
 *   social:profile:{userId}   — 5 min TTL
 *   social:room:{roomId}      — 2 min TTL
 *   social:group:{groupId}    — 2 min TTL
 */
import { getRedisClient } from './redis-client';
import { withContext } from './logger';

const log = withContext({ module: 'cache' });

const TTL_PROFILE = 300;  // 5 minutes
const TTL_ROOM = 120;     // 2 minutes
const TTL_GROUP = 120;    // 2 minutes

// ---------------------------------------------------------------------------
// Generic helpers
// ---------------------------------------------------------------------------

async function cacheGet<T>(key: string): Promise<T | null> {
  try {
    const redis = await getRedisClient();
    if (!redis) return null;
    const raw = await redis.get(key);
    if (!raw) return null;
    return JSON.parse(raw) as T;
  } catch (err) {
    log.warn({ err, key }, 'cache read failed');
    return null;
  }
}

async function cacheSet(key: string, value: unknown, ttlSeconds: number): Promise<void> {
  try {
    const redis = await getRedisClient();
    if (!redis) return;
    await redis.set(key, JSON.stringify(value), { EX: ttlSeconds });
  } catch (err) {
    log.warn({ err, key }, 'cache write failed');
  }
}

async function cacheDelete(key: string): Promise<void> {
  try {
    const redis = await getRedisClient();
    if (!redis) return;
    await redis.del(key);
  } catch (err) {
    log.warn({ err, key }, 'cache delete failed');
  }
}

// ---------------------------------------------------------------------------
// Profile cache
// ---------------------------------------------------------------------------

export function profileCacheKey(userId: string): string {
  return `social:profile:${userId}`;
}

export async function getCachedProfile<T>(userId: string): Promise<T | null> {
  return cacheGet<T>(profileCacheKey(userId));
}

export async function setCachedProfile(userId: string, profile: unknown): Promise<void> {
  return cacheSet(profileCacheKey(userId), profile, TTL_PROFILE);
}

export async function invalidateProfileCache(userId: string): Promise<void> {
  return cacheDelete(profileCacheKey(userId));
}

// ---------------------------------------------------------------------------
// Room cache
// ---------------------------------------------------------------------------

export function roomCacheKey(roomId: string): string {
  return `social:room:${roomId}`;
}

export async function getCachedRoom<T>(roomId: string): Promise<T | null> {
  return cacheGet<T>(roomCacheKey(roomId));
}

export async function setCachedRoom(roomId: string, room: unknown): Promise<void> {
  return cacheSet(roomCacheKey(roomId), room, TTL_ROOM);
}

export async function invalidateRoomCache(roomId: string): Promise<void> {
  return cacheDelete(roomCacheKey(roomId));
}

// ---------------------------------------------------------------------------
// Group cache
// ---------------------------------------------------------------------------

export function groupCacheKey(groupId: string): string {
  return `social:group:${groupId}`;
}

export async function getCachedGroup<T>(groupId: string): Promise<T | null> {
  return cacheGet<T>(groupCacheKey(groupId));
}

export async function setCachedGroup(groupId: string, group: unknown): Promise<void> {
  return cacheSet(groupCacheKey(groupId), group, TTL_GROUP);
}

export async function invalidateGroupCache(groupId: string): Promise<void> {
  return cacheDelete(groupCacheKey(groupId));
}
