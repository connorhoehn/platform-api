// src/pipeline/__tests__/definitions-invalidator.test.ts
//
// Phase 54.6.2 — verifies push-invalidation publishes to Redis on write
// and triggers cache.refresh() on subscribed replicas.

const mockSharedPublish = jest.fn<Promise<number>, [string, string]>();
jest.mock('../../lib/redis-client', () => ({
  getRedisClient: jest.fn(),
}));

import * as redisClient from '../../lib/redis-client';
import { publishInvalidate, startInvalidationSubscriber } from '../definitions-invalidator';
import type { PipelineDefinitionsCache } from '../definitions-cache';

beforeEach(() => {
  jest.clearAllMocks();
});

describe('publishInvalidate', () => {
  it('publishes to the pipeline-definitions:invalidate channel when redis is available', async () => {
    mockSharedPublish.mockResolvedValue(1);
    (redisClient.getRedisClient as jest.Mock).mockResolvedValue({
      publish: mockSharedPublish,
    });
    await publishInvalidate('test.reason');
    expect(mockSharedPublish).toHaveBeenCalledTimes(1);
    const [channel, payload] = mockSharedPublish.mock.calls[0];
    expect(channel).toBe('platform-api:pipeline-definitions:invalidate');
    const parsed = JSON.parse(payload);
    expect(parsed.reason).toBe('test.reason');
    expect(typeof parsed.at).toBe('number');
    expect(typeof parsed.source).toBe('string');
  });

  it('is a silent no-op when redis client is unavailable', async () => {
    (redisClient.getRedisClient as jest.Mock).mockResolvedValue(null);
    await expect(publishInvalidate('test.no-redis')).resolves.toBeUndefined();
    expect(mockSharedPublish).not.toHaveBeenCalled();
  });

  it('swallows publish errors and continues', async () => {
    (redisClient.getRedisClient as jest.Mock).mockResolvedValue({
      publish: jest.fn().mockRejectedValue(new Error('Redis flap')),
    });
    await expect(publishInvalidate('test.error')).resolves.toBeUndefined();
  });
});

describe('startInvalidationSubscriber', () => {
  function makeFakeClient() {
    let handler: ((msg: string) => void) | null = null;
    const calls = { connect: 0, subscribe: 0, unsubscribe: 0, quit: 0 };
    const client = {
      on: jest.fn(),
      connect: jest.fn(async () => { calls.connect += 1; }),
      subscribe: jest.fn(async (_channel: string, h: (msg: string) => void) => {
        calls.subscribe += 1;
        handler = h;
      }),
      unsubscribe: jest.fn(async () => { calls.unsubscribe += 1; }),
      quit: jest.fn(async () => { calls.quit += 1; }),
      deliver: (msg: string) => { if (handler) handler(msg); },
    };
    return { client, calls };
  }

  function makeFakeCache(): PipelineDefinitionsCache & { refreshCalls: number } {
    const stub = {
      refreshCalls: 0,
      refresh: jest.fn(function (this: { refreshCalls: number }) {
        this.refreshCalls += 1;
        return Promise.resolve();
      }),
    };
    return stub as unknown as PipelineDefinitionsCache & { refreshCalls: number };
  }

  it('connects and subscribes on start', async () => {
    const { client, calls } = makeFakeClient();
    const cache = makeFakeCache();
    const handle = await startInvalidationSubscriber(cache, {
      _client: client as unknown as import('redis').RedisClientType,
    });
    expect(calls.connect).toBe(1);
    expect(calls.subscribe).toBe(1);
    await handle.stop();
    expect(calls.unsubscribe).toBe(1);
    expect(calls.quit).toBe(1);
  });

  it('triggers cache.refresh() on incoming valid message', async () => {
    const { client } = makeFakeClient();
    const cache = makeFakeCache();
    await startInvalidationSubscriber(cache, {
      _client: client as unknown as import('redis').RedisClientType,
    });
    client.deliver(JSON.stringify({ reason: 'test', at: Date.now(), source: 'replica-x' }));
    await new Promise((r) => setImmediate(r));
    expect((cache as unknown as { refreshCalls: number }).refreshCalls).toBe(1);
  });

  it('ignores malformed messages without throwing', async () => {
    const { client } = makeFakeClient();
    const cache = makeFakeCache();
    await startInvalidationSubscriber(cache, {
      _client: client as unknown as import('redis').RedisClientType,
    });
    expect(() => client.deliver('{not-json')).not.toThrow();
    await new Promise((r) => setImmediate(r));
    expect((cache as unknown as { refreshCalls: number }).refreshCalls).toBe(0);
  });

  it('returns a no-op stop() when subscriber init throws', async () => {
    const failing = {
      on: jest.fn(),
      connect: jest.fn().mockRejectedValue(new Error('connect timeout')),
      subscribe: jest.fn(),
      unsubscribe: jest.fn(),
      quit: jest.fn(),
    };
    const cache = makeFakeCache();
    const handle = await startInvalidationSubscriber(cache, {
      _client: failing as unknown as import('redis').RedisClientType,
    });
    await expect(handle.stop()).resolves.toBeUndefined();
  });
});
