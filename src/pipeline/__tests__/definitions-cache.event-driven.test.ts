// ROADMAP P2.2 contracts (lane P1): the definitions cache is driven by the
// Redis invalidation channel, not by a 60 s DynamoDB Scan.
//
//   1. With no live subscription it polls every 60 s (unchanged fallback).
//   2. Once the subscriber is live, the periodic Scan drops to the slow
//      safety poll (15 min) and invalidations refresh at once.
//   3. A dropped subscription restores the 60 s poll; the reconnect refreshes
//      once (messages published during the gap were lost) and goes slow again.
//   4. A subscriber that never connects leaves the 60 s poll in place.

jest.mock('../../lib/redis-client', () => ({ getRedisClient: jest.fn(async () => null) }));
jest.mock('../definitions-repository', () => ({ definitionsRepo: { listAll: jest.fn(async () => []) } }));

import { PipelineDefinitionsCache } from '../definitions-cache';
import { startInvalidationSubscriber } from '../definitions-invalidator';

type Handler = (...args: unknown[]) => void;

function fakeSubscriberClient(opts: { connectFails?: boolean } = {}) {
  const handlers = new Map<string, Handler[]>();
  let onMessage: ((raw: string) => void) | null = null;
  const client = {
    on: jest.fn((event: string, h: Handler): void => { handlers.set(event, [...(handlers.get(event) ?? []), h]); }),
    connect: jest.fn(async () => { if (opts.connectFails) throw new Error('ECONNREFUSED'); }),
    subscribe: jest.fn(async (_c: string, h: (raw: string) => void) => { onMessage = h; }),
    unsubscribe: jest.fn(async () => undefined),
    quit: jest.fn(async () => undefined),
    destroy: jest.fn(),
  };
  return {
    client,
    emit: (event: string, ...args: unknown[]) => (handlers.get(event) ?? []).forEach((h) => h(...args)),
    deliver: (reason: string) => onMessage?.(JSON.stringify({ reason, at: Date.now() })),
  };
}

const MIN = 60_000;

beforeEach(() => { jest.useFakeTimers(); });
afterEach(() => { jest.useRealTimers(); });

describe('definitions cache — event-invalidated with a slow safety poll', () => {
  it('polls DynamoDB every 60 s while no invalidation subscription is live', async () => {
    const loader = jest.fn(async () => []);
    const cache = new PipelineDefinitionsCache({ loader });
    await cache.start();
    expect(loader).toHaveBeenCalledTimes(1);
    expect(cache.pollIntervalMs()).toBe(MIN);
    await jest.advanceTimersByTimeAsync(3 * MIN);
    expect(loader).toHaveBeenCalledTimes(4);
    cache.stop();
  });

  it('drops to the 15-minute safety poll once the subscriber is live, and refreshes on each invalidation', async () => {
    const loader = jest.fn(async () => []);
    const cache = new PipelineDefinitionsCache({ loader });
    await cache.start();
    const sub = fakeSubscriberClient();
    const handle = await startInvalidationSubscriber(cache, { _client: sub.client as never });
    await jest.advanceTimersByTimeAsync(0);
    expect(cache.isEventDriven()).toBe(true);
    expect(cache.pollIntervalMs()).toBe(15 * MIN);
    const afterLive = loader.mock.calls.length; // start + go-live catch-up
    expect(afterLive).toBe(2);

    await jest.advanceTimersByTimeAsync(14 * MIN);
    expect(loader).toHaveBeenCalledTimes(afterLive); // no 60 s Scans

    sub.deliver('definition.upsert');
    await jest.advanceTimersByTimeAsync(0);
    expect(loader).toHaveBeenCalledTimes(afterLive + 1); // push, not poll

    await jest.advanceTimersByTimeAsync(1 * MIN);
    expect(loader).toHaveBeenCalledTimes(afterLive + 2); // the safety poll at 15 min
    await handle.stop();
    expect(cache.isEventDriven()).toBe(false);
    cache.stop();
  });

  it('restores the 60 s poll while the subscription is down and catches up when it returns', async () => {
    const loader = jest.fn(async () => []);
    const cache = new PipelineDefinitionsCache({ loader });
    await cache.start();
    const sub = fakeSubscriberClient();
    const handle = await startInvalidationSubscriber(cache, { _client: sub.client as never });
    await jest.advanceTimersByTimeAsync(0);
    const base = loader.mock.calls.length;

    sub.emit('reconnecting');
    expect(cache.isEventDriven()).toBe(false);
    expect(cache.pollIntervalMs()).toBe(MIN);
    await jest.advanceTimersByTimeAsync(2 * MIN);
    expect(loader).toHaveBeenCalledTimes(base + 2);

    sub.emit('ready');
    await jest.advanceTimersByTimeAsync(0);
    expect(cache.isEventDriven()).toBe(true);
    expect(loader).toHaveBeenCalledTimes(base + 3); // one catch-up refresh
    await jest.advanceTimersByTimeAsync(10 * MIN);
    expect(loader).toHaveBeenCalledTimes(base + 3);
    await handle.stop();
    cache.stop();
  });

  it('keeps the 60 s poll when the subscriber cannot start', async () => {
    const loader = jest.fn(async () => []);
    const cache = new PipelineDefinitionsCache({ loader });
    await cache.start();
    const sub = fakeSubscriberClient({ connectFails: true });
    const handle = await startInvalidationSubscriber(cache, { _client: sub.client as never });
    expect(cache.isEventDriven()).toBe(false);
    expect(cache.pollIntervalMs()).toBe(MIN);
    await handle.stop();
    cache.stop();
  });
});
