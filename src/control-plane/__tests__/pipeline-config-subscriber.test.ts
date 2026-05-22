// platform-api/src/control-plane/__tests__/pipeline-config-subscriber.test.ts

import {
  PipelineConfigSubscriber,
  type AgenticPipelineConfig,
} from '../pipeline-config-subscriber';
import type { ControlPlanePayload, ReconcileAck } from 'distributed-core';

// ---------------------------------------------------------------------------
// Helpers / fakes
// ---------------------------------------------------------------------------

function makeFakePayload(
  overrides: Partial<ControlPlanePayload<AgenticPipelineConfig>> = {},
): ControlPlanePayload<AgenticPipelineConfig> {
  return {
    version: 1,
    channel: 'feature.agentic-pipeline.definitions',
    config: {
      name: 'my-pipeline',
      definition: { id: 'my-pipeline', steps: [] },
      triggers: [{ type: 'webhook', config: {} }],
    },
    publishedAt: new Date().toISOString(),
    publisherNodeId: 'operator-node',
    ...overrides,
  };
}

type Handler = (payload: ControlPlanePayload<AgenticPipelineConfig>) => Promise<ReconcileAck>;

interface FakeSubscription {
  handler: Handler | null;
  unsubscribeCalled: boolean;
}

function makeFakeControlPlane(sub: FakeSubscription) {
  return {
    started: false,
    stopped: false,
    async start() {
      this.started = true;
    },
    async stop() {
      this.stopped = true;
    },
    async subscribe(
      _channel: string,
      handler: Handler,
    ): Promise<{ unsubscribe: () => Promise<void> }> {
      sub.handler = handler;
      return {
        unsubscribe: async () => {
          sub.unsubscribeCalled = true;
        },
      };
    },
  };
}

function makeFakeRepo() {
  const calls: Array<{ userId: string; def: Record<string, unknown> }> = [];
  return {
    calls,
    async put(userId: string, def: Record<string, unknown>): Promise<void> {
      calls.push({ userId, def });
    },
  };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('PipelineConfigSubscriber', () => {
  it('subscribes on start() with the correct channel', async () => {
    const sub: FakeSubscription = { handler: null, unsubscribeCalled: false };
    const cp = makeFakeControlPlane(sub);
    const repo = makeFakeRepo();

    const subscriber = new PipelineConfigSubscriber({
      channel: 'feature.agentic-pipeline.definitions',
      nodeId: 'test-node',
      definitionsRepository: repo as never,
      _controlPlane: cp as never,
    });

    await subscriber.start();

    expect(cp.started).toBe(true);
    expect(sub.handler).not.toBeNull();
  });

  it('writes definition to repo and acks applied on valid config', async () => {
    const sub: FakeSubscription = { handler: null, unsubscribeCalled: false };
    const cp = makeFakeControlPlane(sub);
    const repo = makeFakeRepo();

    const subscriber = new PipelineConfigSubscriber({
      channel: 'feature.agentic-pipeline.definitions',
      nodeId: 'test-node-2',
      definitionsRepository: repo as never,
      _controlPlane: cp as never,
    });

    await subscriber.start();

    const payload = makeFakePayload({ version: 42 });
    const ack = await sub.handler!(payload);

    // Repo should have received the write.
    expect(repo.calls).toHaveLength(1);
    expect(repo.calls[0].def).toMatchObject({ id: 'my-pipeline' });

    // Ack should report applied.
    expect(ack.status).toBe('applied');
    expect(ack.appliedVersion).toBe(42);
    expect(ack.subscriberNodeId).toBe('test-node-2');
    expect(ack.channel).toBe('feature.agentic-pipeline.definitions');
  });

  it('propagates error (rejected) when repo.put throws', async () => {
    const sub: FakeSubscription = { handler: null, unsubscribeCalled: false };
    const cp = makeFakeControlPlane(sub);

    const brokenRepo = {
      async put(): Promise<void> {
        throw new Error('DDB write failed');
      },
    };

    const subscriber = new PipelineConfigSubscriber({
      channel: 'feature.agentic-pipeline.definitions',
      nodeId: 'test-node-3',
      definitionsRepository: brokenRepo as never,
      _controlPlane: cp as never,
    });

    await subscriber.start();

    const payload = makeFakePayload();
    // Handler re-throws on repo failure; ControlPlaneChannel converts to
    // 'rejected' ack internally. At this seam we just verify the throw.
    await expect(sub.handler!(payload)).rejects.toThrow('DDB write failed');
  });

  it('stop() calls unsubscribe and stops the channel', async () => {
    const sub: FakeSubscription = { handler: null, unsubscribeCalled: false };
    const cp = makeFakeControlPlane(sub);
    const repo = makeFakeRepo();

    const subscriber = new PipelineConfigSubscriber({
      channel: 'feature.agentic-pipeline.definitions',
      nodeId: 'test-node-4',
      definitionsRepository: repo as never,
      _controlPlane: cp as never,
    });

    await subscriber.start();
    await subscriber.stop();

    expect(sub.unsubscribeCalled).toBe(true);
    expect(cp.stopped).toBe(true);
  });

  it('start() is non-fatal when ControlPlaneChannel.start() throws', async () => {
    const brokenCp = {
      async start() {
        throw new Error('Redis unavailable');
      },
      async stop() { /* no-op */ },
      async subscribe() {
        return { unsubscribe: async () => { /* no-op */ } };
      },
    };
    const repo = makeFakeRepo();

    const subscriber = new PipelineConfigSubscriber({
      channel: 'feature.agentic-pipeline.definitions',
      nodeId: 'test-node-5',
      definitionsRepository: repo as never,
      _controlPlane: brokenCp as never,
    });

    // Must not throw — failure is logged as warn and silently swallowed.
    await expect(subscriber.start()).resolves.toBeUndefined();
    // stop() must also be safe with no active channel.
    await expect(subscriber.stop()).resolves.toBeUndefined();
  });
});
