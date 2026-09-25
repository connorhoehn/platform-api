// NFR #38: every pipeline event reached the gateway twice — once on the
// EventBus's own Redis channel (`dcore:topic:pipeline.events.<nodeId>`) and
// once more from this relay (`dcore:topic:pipeline.events.<type>`), both
// matched by the gateway's `dcore:topic:pipeline.events.*` pattern.
const publish = jest.fn(async (_channel: string, _body: string) => 1);
let redisUp = true;
jest.mock('../../lib/redis-client', () => ({ getRedisClient: async () => (redisUp ? { publish } : null) }));

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
    publish.mockReset();
    publish.mockImplementation(async () => 1);
    redisUp = true;
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

  // Ported from the pre-2fba534 draft suite (3970c7d): behaviours of the
  // current module that the NFR #38 tests above do not pin down.

  it('maps the colon-form bus type onto the gateway pattern, and accepts dot-form', () => {
    expect(channelForType('pipeline:step:started')).toBe('dcore:topic:pipeline.events.step.started');
    expect(channelForType('pipeline:approval:requested')).toBe('dcore:topic:pipeline.events.approval.requested');
    expect(channelForType('step.completed')).toBe('dcore:topic:pipeline.events.step.completed');
  });

  it('publishes the BusEvent verbatim, keeping the colon-form type the gateway bridge requires', async () => {
    const bus = fakeBus();
    const relay = await startPipelineEventRedisRelay({ eventBus: bus });
    const step = { ...event, type: 'pipeline:step:completed', payload: { runId: 'r1', stepId: 'n1' }, version: 7 };
    await bus.handlers[0]!(step);
    const [channel, body] = publish.mock.calls[0]!;
    expect(channel).toBe('dcore:topic:pipeline.events.step.completed');
    expect(JSON.parse(body)).toEqual(step);
    await relay!.stop();
  });

  it('ignores the registry/cluster chatter that shares the bus', async () => {
    const bus = fakeBus();
    const relay = await startPipelineEventRedisRelay({ eventBus: bus });
    await bus.handlers[0]!({ ...event, type: 'registry:module-registered' });
    await bus.handlers[0]!({ ...event, type: 'cluster:node:joined' });
    await bus.handlers[0]!({ id: 'x' });
    expect(publish).not.toHaveBeenCalled();
    expect(relay!.published).toBe(0);
    await relay!.stop();
  });

  it('swallows a publish failure instead of throwing into the bus, and does not count it', async () => {
    const bus = fakeBus();
    const relay = await startPipelineEventRedisRelay({ eventBus: bus });
    publish.mockImplementationOnce(async () => { throw new Error('redis down'); });
    await expect(bus.handlers[0]!(event)).resolves.toBeUndefined();
    expect(relay!.published).toBe(0);
    await bus.handlers[0]!(event);
    expect(relay!.published).toBe(1);
    await relay!.stop();
  });

  it('degrades to no publish while Redis is down, and resumes when the client comes back', async () => {
    const bus = fakeBus();
    const relay = await startPipelineEventRedisRelay({ eventBus: bus });
    redisUp = false;
    await expect(bus.handlers[0]!(event)).resolves.toBeUndefined();
    expect(publish).not.toHaveBeenCalled();
    redisUp = true;
    await bus.handlers[0]!(event);
    expect(relay!.published).toBe(1);
    await relay!.stop();
  });

  it('stop() unsubscribes once, is idempotent, and drops events that race it', async () => {
    const bus = fakeBus();
    const relay = await startPipelineEventRedisRelay({ eventBus: bus });
    await relay!.stop();
    await relay!.stop();
    expect(bus.unsubscribe).toHaveBeenCalledTimes(1);
    expect(bus.unsubscribe).toHaveBeenCalledWith('sub-1');
    await bus.handlers[0]!(event);
    expect(publish).not.toHaveBeenCalled();
  });

  it('stop() survives an unsubscribe that throws', async () => {
    const bus = fakeBus();
    bus.unsubscribe.mockImplementation(() => { throw new Error('gone'); });
    const relay = await startPipelineEventRedisRelay({ eventBus: bus });
    await expect(relay!.stop()).resolves.toBeUndefined();
  });
});
