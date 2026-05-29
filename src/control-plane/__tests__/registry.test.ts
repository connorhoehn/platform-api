// platform-api/src/control-plane/__tests__/registry.test.ts
//
// Smoke tests for ControlPlaneSubscriberRegistry — the shared owner of
// the ControlPlaneChannel + Redis connection that future CRD subscribers
// register against.

import { ControlPlaneSubscriberRegistry } from '../registry';
import type { RegisteredSubscriber } from '../registry';
import type { ControlPlanePayload, ReconcileAck } from 'distributed-core';

type Handler = (payload: ControlPlanePayload<unknown>) => Promise<ReconcileAck>;

interface FakeSub {
  channel: string;
  handler: Handler | null;
  unsubscribeCalled: boolean;
}

function makeFakeControlPlane() {
  const subs: FakeSub[] = [];
  return {
    subs,
    started: false,
    stopped: false,
    async start() { this.started = true; },
    async stop() { this.stopped = true; },
    async subscribe<T>(
      channel: string,
      handler: (payload: ControlPlanePayload<T>) => Promise<ReconcileAck>,
    ): Promise<{ unsubscribe: () => Promise<void> }> {
      const sub: FakeSub = { channel, handler: handler as Handler, unsubscribeCalled: false };
      subs.push(sub);
      return {
        unsubscribe: async () => { sub.unsubscribeCalled = true; },
      };
    },
  };
}

function makeRegistration(name: string, channel: string): RegisteredSubscriber<unknown> & { invocations: ControlPlanePayload<unknown>[] } {
  const invocations: ControlPlanePayload<unknown>[] = [];
  return {
    name,
    channel,
    invocations,
    handler: async (payload) => {
      invocations.push(payload);
      return {
        channel: payload.channel,
        appliedVersion: payload.version,
        appliedAt: new Date().toISOString(),
        subscriberNodeId: 'test-node',
        status: 'applied',
      };
    },
  };
}

function makePayload(channel: string, version = 1): ControlPlanePayload<unknown> {
  return {
    version,
    channel,
    config: { hello: 'world' },
    publishedAt: new Date().toISOString(),
    publisherNodeId: 'operator',
  };
}

describe('ControlPlaneSubscriberRegistry', () => {
  it('subscribes every registered handler on start()', async () => {
    const cp = makeFakeControlPlane();
    const reg = new ControlPlaneSubscriberRegistry({
      nodeId: 'test-node',
      _controlPlane: cp as never,
    });

    const a = makeRegistration('a', 'feature.kind-a.definitions');
    const b = makeRegistration('b', 'feature.kind-b.*');
    reg.register(a);
    reg.register(b);

    await reg.start();

    expect(cp.started).toBe(true);
    expect(cp.subs).toHaveLength(2);
    expect(cp.subs.map(s => s.channel).sort()).toEqual([
      'feature.kind-a.definitions',
      'feature.kind-b.*',
    ]);
  });

  it('routes payloads to the correct handler', async () => {
    const cp = makeFakeControlPlane();
    const reg = new ControlPlaneSubscriberRegistry({ _controlPlane: cp as never });

    const a = makeRegistration('a', 'feature.kind-a.definitions');
    const b = makeRegistration('b', 'feature.kind-b.*');
    reg.register(a);
    reg.register(b);
    await reg.start();

    const subA = cp.subs.find(s => s.channel === 'feature.kind-a.definitions')!;
    const subB = cp.subs.find(s => s.channel === 'feature.kind-b.*')!;
    await subA.handler!(makePayload('feature.kind-a.definitions'));
    await subB.handler!(makePayload('feature.kind-b.foo'));

    expect(a.invocations).toHaveLength(1);
    expect(b.invocations).toHaveLength(1);
    expect(a.invocations[0].channel).toBe('feature.kind-a.definitions');
    expect(b.invocations[0].channel).toBe('feature.kind-b.foo');
  });

  it('unsubscribes every handler on stop()', async () => {
    const cp = makeFakeControlPlane();
    const reg = new ControlPlaneSubscriberRegistry({ _controlPlane: cp as never });

    reg.register(makeRegistration('a', 'feature.kind-a.definitions'));
    reg.register(makeRegistration('b', 'feature.kind-b.*'));
    await reg.start();
    await reg.stop();

    expect(cp.subs.every(s => s.unsubscribeCalled)).toBe(true);
    expect(cp.stopped).toBe(true);
  });

  it('register() after start() is a no-op (warns)', async () => {
    const cp = makeFakeControlPlane();
    const reg = new ControlPlaneSubscriberRegistry({ _controlPlane: cp as never });

    reg.register(makeRegistration('a', 'feature.kind-a.definitions'));
    await reg.start();

    reg.register(makeRegistration('late', 'feature.late.definitions'));

    expect(cp.subs).toHaveLength(1);
    expect(cp.subs[0].channel).toBe('feature.kind-a.definitions');
  });

  it('start() with no subscribers is a benign no-op', async () => {
    const cp = makeFakeControlPlane();
    const reg = new ControlPlaneSubscriberRegistry({ _controlPlane: cp as never });

    await reg.start();
    expect(cp.started).toBe(false); // no subs → no bootstrap

    await reg.stop();
    expect(cp.stopped).toBe(false);
  });
});
