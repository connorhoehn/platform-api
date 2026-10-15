/**
 * Shared Redis client for the platform-api.
 *
 * Used by:
 * - Cache layer (profile, room, group lookups)
 * - Health check endpoint
 *
 * Connection is lazy: getRedisClient() connects on first call and
 * reuses the connection thereafter. If Redis is down, callers get null
 * and should fall back to DynamoDB directly.
 */
import { createClient, RedisClientType } from 'redis';
import log from './logger';

const REDIS_URL = `redis://${process.env.REDIS_ENDPOINT ?? 'redis'}:${process.env.REDIS_PORT ?? '6379'}`;

/** The Redis the shared client talks to — what the gateway's relays listen on. */
export function sharedRedisUrl(): string {
  return REDIS_URL;
}
const RETRY_INTERVAL_MS = 5_000;

let client: RedisClientType | null = null;
let connecting = false;
let lastAttempt = 0;
/**
 * The client whose connect() is still pending. It retries on its own
 * (reconnectStrategy) for a few seconds, so shutdown must be able to reach it:
 * before lane P1 (2026-10-14) a caller that gave up, or a test file that ended,
 * left it dialling `redis` once a second with nothing holding it.
 */
let pendingClient: RedisClientType | null = null;
let pendingCacheClient: RedisClientType | null = null;

/** Close a client that never became the shared one; never throws. */
function discard(c: RedisClientType): void {
  try { if (c.isOpen) c.destroy(); } catch { /* already closed */ }
}

export async function getRedisClient(): Promise<RedisClientType | null> {
  if (client?.isReady) return client;
  if (connecting) return null;

  const now = Date.now();
  if (now - lastAttempt < RETRY_INTERVAL_MS) return null;

  connecting = true;
  lastAttempt = now;
  let c: RedisClientType | null = null;
  try {
    c = createClient({
      url: REDIS_URL,
      socket: {
        connectTimeout: 3000,
        reconnectStrategy: (retries) => retries < 3 ? 1000 : false,
      },
    }) as RedisClientType;
    c.on('error', (err: Error) => {
      log.warn({ err: err.message }, '[redis-client] Redis error');
      client = null;
    });
    pendingClient = c;
    try {
      await c.connect();
    } finally {
      if (pendingClient === c) pendingClient = null;
    }
    // Shutdown destroyed it mid-connect (node-redis then resolves connect()).
    if (c.isOpen === false) throw new Error('Redis client closed during connect');
    client = c;
    connecting = false;
    return c;
  } catch (err) {
    // node-redis can reject connect() on the first socket error while the
    // client keeps reconnecting in the background; nothing would own it.
    // Close it before anything else here can throw.
    if (c) discard(c);
    client = null;
    connecting = false;
    log.warn({ err: (err as Error).message }, '[redis-client] Redis connect failed');
    return null;
  }
}

/**
 * The cache Redis (NFR #88): a separate instance (`REDIS_CACHE_URL`, the
 * chart's `redis-cache` container) with `allkeys-lru`, for values that are
 * only ever a shortcut — work-graph view bases today. The main Redis runs
 * `noeviction` because it holds coordination state that carries a TTL and is
 * NOT re-created on a miss (deck-revise request claims, idempotency records,
 * judge budget counters, sequence counters); an eviction policy there could
 * drop one of those under memory pressure. Without `REDIS_CACHE_URL` (shared
 * services, tests) this is the main client, exactly as before.
 */
let cacheClient: RedisClientType | null = null;
let cacheConnecting = false;
let cacheLastAttempt = 0;

export async function getRedisCacheClient(): Promise<RedisClientType | null> {
  const url = process.env.REDIS_CACHE_URL?.trim();
  if (!url) return getRedisClient();
  if (cacheClient?.isReady) return cacheClient;
  if (cacheConnecting) return null;
  const now = Date.now();
  if (now - cacheLastAttempt < RETRY_INTERVAL_MS) return null;
  cacheConnecting = true;
  cacheLastAttempt = now;
  let c: RedisClientType | null = null;
  try {
    c = createClient({
      url,
      socket: { connectTimeout: 3000, reconnectStrategy: (retries) => retries < 3 ? 1000 : false },
    }) as RedisClientType;
    c.on('error', (err: Error) => {
      log.warn({ err: err.message }, '[redis-client] cache Redis error');
      cacheClient = null;
    });
    pendingCacheClient = c;
    try {
      await c.connect();
    } finally {
      if (pendingCacheClient === c) pendingCacheClient = null;
    }
    if (c.isOpen === false) throw new Error('Redis cache client closed during connect');
    cacheClient = c;
    return c;
  } catch (err) {
    if (c) discard(c);
    cacheClient = null;
    log.warn({ err: (err as Error).message }, '[redis-client] cache Redis connect failed');
    return null;
  } finally {
    cacheConnecting = false;
  }
}

/** Close only existing connections; shutdown must never dial a new server. */
export async function closeRedisClients(): Promise<void> {
  // A connect still retrying is cancelled too (its connect() then rejects and
  // its caller sees null), so nothing is left dialling after shutdown.
  for (const pending of [pendingClient, pendingCacheClient]) if (pending) discard(pending);
  pendingClient = null; pendingCacheClient = null;
  const clients = new Set([client, cacheClient]);
  client = null; cacheClient = null;
  // Used after owned work has settled/cancelled; do not hang shutdown waiting
  // for an unavailable Redis server to acknowledge QUIT.
  const results = await Promise.allSettled([...clients].filter(c => c?.isOpen).map(async c => c!.destroy()));
  const failures = results.filter((result): result is PromiseRejectedResult => result.status === 'rejected');
  if (failures.length) throw new AggregateError(failures.map(result => result.reason), 'Redis shutdown failed');
}
