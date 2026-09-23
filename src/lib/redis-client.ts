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
      log.warn({ err: err.message }, '[redis-client] Redis error');
      client = null;
    });
    await c.connect();
    client = c;
    connecting = false;
    return c;
  } catch (err) {
    log.warn({ err: (err as Error).message }, '[redis-client] Redis connect failed');
    client = null;
    connecting = false;
    return null;
  }
}
