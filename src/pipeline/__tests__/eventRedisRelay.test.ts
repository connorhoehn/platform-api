// NFR #38: every pipeline event reached the gateway twice — once on the
// EventBus's own Redis channel (`dcore:topic:pipeline.events.<nodeId>`) and
// once more from this relay (`dcore:topic:pipeline.events.<type>`), both
// matched by the gateway's `dcore:topic:pipeline.events.*` pattern.
const publish = jest.fn(async () => 1);
jest.mock('../../lib/redis-client', () => ({ getRedisClient: async () => ({ publish }) }));

import { channelForType, sameRedisTarget, startPipelineEventRedisRelay } from '../event-redis-relay';

function fakeBus() {
  const handlers: Array<(event: unknown) => Promise<void>> = [];
  return {
    handlers,
    subscribeAll: jest.fn((handler: (event: unknown) => Promise<void>) => { handlers.push(handler); return `sub-${handlers.length}`; }),
    unsubscribe: jest.fn(),
  };
}

const event = { id: 'e1', type: 'pipeline:run:started', payload: { runId: 'r1' }, timestamp: 1, sourceNodeId: 'n1', version: 1 };

describe('pipeline event Redis relay', () => {
  const original = process.env.PIPELINE_EVENT_RELAY;
  afterEach(() => {
    publish.mockClear();
    if (original === undefined) delete process.env.PIPELINE_EVENT_RELAY; else process.env.PIPELINE_EVENT_RELAY = original;
  });

  it('stands down when the bus already publishes where the gateway listens: one copy per event', async () => {
    const bus = fakeBus();
    const relay = await startPipelineEventRedisRelay({ eventBus: bus, busReachesGateway: true });
    expect(relay).toBeNull();
    expect(bus.subscribeAll).not.toHaveBeenCalled();
  });

  it('relays when the bus does not reach the gateway (in-memory pubsub, or another Redis)', async () => {
    const bus = fakeBus();
    const relay = await startPipelineEventRedisRelay({ eventBus: bus, busReachesGateway: false });
    await bus.handlers[0]!(event);
    expect(publish).toHaveBeenCalledWith(channelForType('pipeline:run:started'), JSON.stringify(event));
    expect(relay!.published).toBe(1);
    await relay!.stop();
  });

  it('PIPELINE_EVENT_RELAY=1 forces it on; =0 forces it off', async () => {
    process.env.PIPELINE_EVENT_RELAY = '1';
    const forced = await startPipelineEventRedisRelay({ eventBus: fakeBus(), busReachesGateway: true });
    expect(forced).not.toBeNull();
    await forced!.stop();
    process.env.PIPELINE_EVENT_RELAY = '0';
    expect(await startPipelineEventRedisRelay({ eventBus: fakeBus(), busReachesGateway: false })).toBeNull();
  });

  it('compares Redis targets by host and port', () => {
    expect(sameRedisTarget('redis://wsg-redis:6379', 'redis://wsg-redis:')).toBe(true);
    expect(sameRedisTarget('redis://wsg-redis:6379', 'redis://wsg-redis:6380')).toBe(false);
    expect(sameRedisTarget('redis://a:6379', 'redis://b:6379')).toBe(false);
    expect(sameRedisTarget(undefined, 'redis://a:6379')).toBe(false);
  });
});
