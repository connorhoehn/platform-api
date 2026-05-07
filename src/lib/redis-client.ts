/**
 * Shared Redis client for the platform-api.
 *
 * Used by:
 * - Cache layer (profile, room, group lookups)
 * - Health check endpoint
 * - BroadcastService (via its own private client — not shared here)
 *
 * Connection is lazy: getRedisClient() connects on first call and
 * reuses the connection thereafter. If Redis is down, callers get null
 * and should fall back to DynamoDB directly.
 */
import { createClient, RedisClientType } from 'redis';

const REDIS_URL = `redis://${process.env.REDIS_ENDPOINT ?? 'redis'}:${process.env.REDIS_PORT ?? '6379'}`;
const RETRY_INTERVAL_MS = 5_000;

let client: RedisClientType | null = null;
let connecting = false;
let lastAttempt = 0;

export async function getRedisClient(): Promise<RedisClientType | null> {
  if (client?.isReady) return client;
  if (connecting) return null;

  const now = Date.now();
  if (now - lastAttempt < RETRY_INTERVAL_MS) return null;

  connecting = true;
  lastAttempt = now;
  try {
    const c = createClient({
      url: REDIS_URL,
      socket: {
        connectTimeout: 3000,
        reconnectStrategy: (retries) => retries < 3 ? 1000 : false,
      },
    }) as RedisClientType;
    c.on('error', (err: Error) => {
      console.warn('[redis-client] Redis error:', err.message);
      client = null;
    });
    await c.connect();
    client = c;
    connecting = false;
    return c;
  } catch (err) {
    console.warn('[redis-client] Redis connect failed:', (err as Error).message);
    client = null;
    connecting = false;
    return null;
  }
}
