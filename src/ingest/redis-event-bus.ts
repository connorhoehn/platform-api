// Redis-backed IngestEventBus implementation. Publishes ingest progress events
// to Redis pub/sub so the gateway (running in a separate process/pod) can
// subscribe via ingest-relay and fan out to WebSocket clients.
//
// Channel: `ingest:events` (single channel; gateway-side relay handles fan-out
// to per-source channels based on event.sourceId).

import type { IngestEventBus } from './engine';
import type { IngestEvent } from './types';

const CHANNEL = 'ingest:events';

export interface RedisIngestEventBusConfig {
  redisUrl?: string;
  logger?: { info?: (...args: unknown[]) => void; warn?: (...args: unknown[]) => void; error?: (...args: unknown[]) => void };
}

export class RedisIngestEventBus implements IngestEventBus {
  private client: ReturnType<typeof import('redis').createClient> | null = null;
  private readonly redisUrl: string;
  private readonly logger: NonNullable<RedisIngestEventBusConfig['logger']>;
  private readonly handlers: Array<(event: IngestEvent) => void> = [];
  private connected = false;

  constructor(config: RedisIngestEventBusConfig = {}) {
    this.redisUrl = config.redisUrl || this.resolveRedisUrl();
    this.logger = config.logger || console;
  }

  async connect(): Promise<void> {
    if (this.connected) return;

    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const redis = require('redis');
    this.client = redis.createClient({ url: this.redisUrl });

    this.client!.on('error', (err: Error) => {
      this.logger.warn?.('[redis-ingest-event-bus] redis error', err?.message);
    });

    await this.client!.connect();
    this.connected = true;
    this.logger.info?.(`[redis-ingest-event-bus] connected to ${this.redisUrl}`);
  }

  emit(event: IngestEvent): void {
    // Local handlers (for in-process consumers like the social-api bridge)
    for (const handler of this.handlers) {
      try {
        handler(event);
      } catch (err) {
        this.logger.error?.('[redis-ingest-event-bus] local handler threw', err);
      }
    }

    // Publish to Redis (fire-and-forget)
    if (this.client && this.connected) {
      this.client.publish(CHANNEL, JSON.stringify(event)).catch((err: Error) => {
        this.logger.warn?.('[redis-ingest-event-bus] publish failed', err?.message);
      });
    }
  }

  on(handler: (event: IngestEvent) => void): void {
    this.handlers.push(handler);
  }

  off(handler: (event: IngestEvent) => void): void {
    const idx = this.handlers.indexOf(handler);
    if (idx !== -1) this.handlers.splice(idx, 1);
  }

  async disconnect(): Promise<void> {
    if (!this.connected || !this.client) return;
    try {
      await this.client.quit();
    } catch {
      // ignore
    }
    this.connected = false;
  }

  private resolveRedisUrl(): string {
    if (process.env.REDIS_URL) return process.env.REDIS_URL;
    const host = process.env.REDIS_ENDPOINT || 'redis';
    const port = process.env.REDIS_PORT || '6379';
    return `redis://${host}:${port}`;
  }
}

export { CHANNEL as INGEST_REDIS_CHANNEL };
