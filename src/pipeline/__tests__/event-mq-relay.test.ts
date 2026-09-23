import { isPipelineMqLaneEvent, isPipelineHotLaneEvent } from '@connorhoehn/event-catalog';
import { isLiveMqEventType, resolveLiveMqConfig, startPipelineEventMqRelay } from '../event-mq-relay';

// NFR #35: live pipeline progress onto the MQ queue the gateway consumes.

function fakeMq(status: (method: string, n: number) => number = () => 200) {
  const calls: Array<{ method: string; url: string; body?: any }> = [];
  const fetchImpl = jest.fn(async (url: string, init: RequestInit) => {
    const method = String(init.method);
    calls.push({ method, url, body: init.body ? JSON.parse(String(init.body)) : undefined });
    const code = status(method, calls.filter((c) => c.method === method).length);
    return { ok: code >= 200 && code < 300, status: code } as Response;
  }) as unknown as typeof fetch;
  return { calls, fetchImpl };
}

const ev = (type: string, id = `${type}-1`) => ({ id, type, payload: { runId: 'r1' }, version: 1, timestamp: 1, sourceNodeId: 'n' });

describe('pipeline live events → MQ (NFR #35)', () => {
  it('sends run/step/approval/deck-revise progress, not tokens, heartbeats, checkpoints or context patches', () => {
    for (const t of ['pipeline:run:started', 'pipeline:run:completed', 'pipeline:step:started', 'pipeline:step:failed', 'pipeline:approval:requested', 'pipeline:deck:revise:phase']) {
      expect(isLiveMqEventType(t)).toBe(true);
    }
    for (const t of ['pipeline:llm:token', 'pipeline:run:checkpoint', 'pipeline:step:heartbeat', 'pipeline:step:context.patched', 'social:thing', undefined]) {
      expect(isLiveMqEventType(t)).toBe(false);
    }
  });

  it('is the event-catalog lane predicate, so the gateway hot lane carries exactly the rest (NFR #130)', () => {
    expect(isLiveMqEventType).toBe(isPipelineMqLaneEvent);
    for (const t of ['pipeline:run:started', 'pipeline:llm:token', 'pipeline:run:checkpoint', 'pipeline:step:context.patched']) {
      expect(isLiveMqEventType(t)).toBe(!isPipelineHotLaneEvent(t));
    }
  });

  it('is on when a queue is configured, off by flag or without one', () => {
    expect(resolveLiveMqConfig({ MESSAGE_QUEUE_URL: 'http://mq:3000/' })).toMatchObject({ enabled: true, url: 'http://mq:3000' });
    expect(resolveLiveMqConfig({ PIPELINE_EVENT_BUS_URL: 'http://bus', MESSAGE_QUEUE_URL: 'http://mq' })).toMatchObject({ url: 'http://bus' });
    expect(resolveLiveMqConfig({ MESSAGE_QUEUE_URL: 'http://mq', PIPELINE_LIVE_EVENTS_MQ: 'off' }).enabled).toBe(false);
    expect(resolveLiveMqConfig({}).enabled).toBe(false);
  });

  it('ensures the queue once, then sends each event once, in order, as the BusEvent body', async () => {
    const mq = fakeMq((method) => (method === 'PUT' ? 409 : 200));
    const handlers: Array<(e: any) => Promise<void>> = [];
    const relay = startPipelineEventMqRelay({
      url: 'http://mq:3000',
      fetchImpl: mq.fetchImpl,
      eventBus: { subscribeAll: (h) => { handlers.push(h); return 'sub'; }, unsubscribe: jest.fn() },
    });
    await handlers[0]!(ev('pipeline:run:started', 'a') as any);
    await handlers[0]!(ev('pipeline:llm:token', 'tok') as any);
    await handlers[0]!(ev('pipeline:step:started', 'b') as any);
    relay.publish(ev('pipeline:deck:revise:phase', 'c'));
    await relay.flush();
    expect(mq.calls.filter((c) => c.method === 'PUT')).toHaveLength(1);
    expect(mq.calls[0]!.url).toBe('http://mq:3000/api/v1/queues/pipeline-events');
    const sent = mq.calls.filter((c) => c.method === 'POST').map((c) => JSON.parse(c.body.message_body).id);
    expect(sent).toEqual(['a', 'b', 'c']);
    expect(relay.stats).toMatchObject({ sent: 3, failed: 0 });
    await relay.stop();
  });

  it('never throws into the run: failures are counted, and a missing queue is re-ensured', async () => {
    const mq = fakeMq((method, n) => (method === 'POST' && n === 1 ? 404 : 200));
    const relay = startPipelineEventMqRelay({ url: 'http://mq', fetchImpl: mq.fetchImpl });
    relay.publish(ev('pipeline:run:started', 'a'));
    relay.publish(ev('pipeline:run:completed', 'b'));
    await relay.flush();
    expect(relay.stats).toMatchObject({ sent: 1, failed: 1 });
    expect(mq.calls.filter((c) => c.method === 'PUT')).toHaveLength(2);
  });

  it('bounds the backlog while MQ is slow (oldest dropped and counted)', async () => {
    let release!: () => void;
    const gate = new Promise<void>((r) => { release = r; });
    const fetchImpl = jest.fn(async () => { await gate; return { ok: true, status: 200 } as Response; }) as unknown as typeof fetch;
    const relay = startPipelineEventMqRelay({ url: 'http://mq', fetchImpl });
    for (let i = 0; i < 1_005; i += 1) relay.publish(ev('pipeline:step:started', `e${i}`));
    release();
    await relay.flush();
    // The first event was already in flight; 1,004 waited against a cap of 1,000.
    expect(relay.stats.dropped).toBe(4);
    expect(relay.stats.sent).toBe(1_001);
  });
});
