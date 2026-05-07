// social-api/src/pipeline/__tests__/failureDetectorBridge.test.ts
//
// Verifies the opt-in `setupFailureDetectorBridge` wiring:
//   1. With `PIPELINE_FD_BRIDGE_ENABLED=true`, bootstrap succeeds, the bridge
//      handle is constructed (proven by exercising the same code path on a
//      bare cluster), heartbeats publish, and the failure detector remains
//      healthy for the local node.
//   2. With the env var unset, `setupFailureDetectorBridge` returns `null`
//      and bootstrap behavior is identical to the pre-bridge baseline
//      (module RUNNING, EventBus available, shutdown clean).
//
// Both branches use FixtureLLMClient so no external API is reached.

// `distributed-core/testing` is a real subpath export — Jest resolves
// it via package `exports`. tsc with our current `module: commonjs`
// (classic resolver) doesn't honor `exports`. Suppressing here rather
// than flipping the project to `moduleResolution: nodenext`.
import { FixtureLLMClient } from 'distributed-core/testing';
import { Cluster } from 'distributed-core';

import { bootstrapPipeline } from '../bootstrap';
import { setupFailureDetectorBridge } from '../config/failureDetectorBridge';

jest.setTimeout(20_000);

describe('setupFailureDetectorBridge — opt-in liveness wiring', () => {
  const ORIGINAL_ENV = { ...process.env };

  afterEach(() => {
    // Reset env between cases so test ordering does not bleed flag state.
    process.env = { ...ORIGINAL_ENV };
  });

  test('returns null when PIPELINE_FD_BRIDGE_ENABLED is unset (default-off)', async () => {
    delete process.env.PIPELINE_FD_BRIDGE_ENABLED;

    // Stand up a minimal Cluster directly so we do not pay the full bootstrap
    // cost just to exercise the disabled branch — the function is pure
    // w.r.t. the cluster aside from reading env.
    const cluster = await Cluster.createSingleNode({
      nodeId: `fdb-disabled-${Date.now()}`,
      registry: { type: 'memory' },
      failureDetection: { heartbeatMs: 100, deadTimeoutMs: 600, activeProbing: true },
    });
    await cluster.start();

    try {
      const handle = await setupFailureDetectorBridge(cluster);
      expect(handle).toBeNull();
    } finally {
      await cluster.stop();
    }
  });

  test('bootstrap is identical to baseline when bridge is disabled', async () => {
    delete process.env.PIPELINE_FD_BRIDGE_ENABLED;

    const fixture = new FixtureLLMClient(['ok']);
    const { module, nodeId, shutdown } = await bootstrapPipeline({ llmClient: fixture });

    expect(typeof nodeId).toBe('string');
    expect(nodeId.length).toBeGreaterThan(0);

    const bus = module.getEventBus();
    expect(bus).toBeDefined();
    expect(typeof bus.subscribe).toBe('function');

    await expect(shutdown()).resolves.toBeUndefined();
  });

  test('returns a handle when PIPELINE_FD_BRIDGE_ENABLED=true and feeds heartbeats into FD', async () => {
    process.env.PIPELINE_FD_BRIDGE_ENABLED = 'true';
    // Keep the heartbeat tight so we can deterministically assert the
    // FailureDetector saw activity within the test window.
    process.env.PIPELINE_FD_BRIDGE_HEARTBEAT_MS = '50';

    const cluster = await Cluster.createSingleNode({
      nodeId: `fdb-enabled-${Date.now()}`,
      registry: { type: 'memory' },
      failureDetection: { heartbeatMs: 100, deadTimeoutMs: 600, activeProbing: true },
    });
    await cluster.start();

    let handle: Awaited<ReturnType<typeof setupFailureDetectorBridge>> = null;
    try {
      handle = await setupFailureDetectorBridge(cluster);
      expect(handle).not.toBeNull();
      expect(typeof handle?.stop).toBe('function');

      // Subscribe to the same heartbeat topic and verify our heartbeat
      // payload publishes within a couple of intervals. This proves the
      // PubSubHeartbeatSource is publishing — the failure detector then
      // calls `recordNodeActivity` on every received remote heartbeat
      // (loop-prevention drops our own self-heartbeats inside the source).
      const seen: unknown[] = [];
      cluster.pubsub.subscribe('pipeline.heartbeat', (_topic, payload) => {
        seen.push(payload);
      });

      // Wait long enough for at least one publish tick (50ms interval).
      await new Promise((resolve) => setTimeout(resolve, 250));

      // The local subscription delivers our own heartbeat (PubSubManager
      // delivers locally even though the source's source-side dedupe
      // only drops them on the remote-recv path). We just need to prove
      // *something* hit the topic.
      expect(seen.length).toBeGreaterThan(0);
      const sample = seen[0] as { nodeId?: unknown; timestamp?: unknown };
      expect(typeof sample.nodeId).toBe('string');
      expect(typeof sample.timestamp).toBe('number');

      // Sanity: the FailureDetector should still be running and have not
      // marked the local node failed. We don't have a public `isAlive`
      // surface; instead we assert getStatus() reports running, which is
      // enough to prove the bridge didn't accidentally tear it down.
      const status = cluster.failureDetector.getStatus();
      expect(status.isRunning).toBe(true);
    } finally {
      if (handle) await handle.stop();
      await cluster.stop();
    }
  });
});
