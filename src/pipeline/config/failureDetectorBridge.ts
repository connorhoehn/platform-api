// social-api/src/pipeline/config/failureDetectorBridge.ts
//
// Wires distributed-core's `FailureDetectorBridge` + `PubSubHeartbeatSource`
// onto the bootstrap's already-constructed `Cluster` facade, providing an
// out-of-band liveness signal that survives gossip-transport partitions.
//
// Today, `Cluster.create()` auto-wires a `FailureDetector` that derives
// liveness from the gossip transport. When gossip is partitioned, that
// detector goes silent. The bridge below adds a parallel heartbeat path:
//
//   1. `PubSubHeartbeatSource` publishes a periodic heartbeat on the
//      `pipeline.heartbeat` PubSub topic (a separate fabric from gossip)
//      and feeds every received heartbeat into the same `FailureDetector`
//      via `recordNodeActivity()`. So even if gossip is wedged, the FD
//      still sees evidence that peers are up.
//
//   2. `FailureDetectorBridge` listens for `node-failed` (and optionally
//      `node-suspected`) events from the FD and translates them into
//      cleanup actions on attached targets — `router.handleNodeLeft()`,
//      `connectionRegistry.handleRemoteNodeFailure()`, and
//      `lock.handleRemoteNodeFailure()`. The pipeline supplies the
//      router and lock targets from `cluster.router` / `cluster.lock`;
//      there's no `connectionRegistry` at this layer.
//
// Opt-in: `PIPELINE_FD_BRIDGE_ENABLED=true` enables the bridge. Default
// is disabled — when disabled this module returns `null` and the caller
// skips wiring entirely.
//
// Tunables (env, all read at call-time):
//   PIPELINE_FD_BRIDGE_ENABLED        — 'true' to enable. Default 'false'.
//   PIPELINE_FD_BRIDGE_TOPIC          — PubSub topic. Default 'pipeline.heartbeat'.
//   PIPELINE_FD_BRIDGE_HEARTBEAT_MS   — Publish interval. Default 1000.

import {
  FailureDetectorBridge,
  PubSubHeartbeatSource,
} from 'distributed-core';
import type {
  Cluster,
  FailureDetectorBridgeTargets,
} from 'distributed-core';

const DEFAULT_TOPIC = 'pipeline.heartbeat';
const DEFAULT_HEARTBEAT_INTERVAL_MS = 1000;

export interface FailureDetectorBridgeOptions {
  /**
   * Override the PubSub topic used for heartbeat fan-out. Default is
   * `PIPELINE_FD_BRIDGE_TOPIC` env or `'pipeline.heartbeat'`.
   */
  topic?: string;
  /**
   * Override the heartbeat publish interval (ms). Default is
   * `PIPELINE_FD_BRIDGE_HEARTBEAT_MS` env or `1000`.
   */
  heartbeatIntervalMs?: number;
  /**
   * Force-enable / force-disable irrespective of the env var. When omitted,
   * the env var is consulted.
   */
  enabled?: boolean;
}

export interface FailureDetectorBridgeHandle {
  /** Stop the bridge and the heartbeat source, in reverse construction order. */
  stop: () => Promise<void>;
}

function isEnvEnabled(): boolean {
  const raw = process.env.PIPELINE_FD_BRIDGE_ENABLED;
  if (raw == null) return false;
  return raw.trim().toLowerCase() === 'true';
}

function resolveTopic(opt: string | undefined): string {
  if (opt != null && opt !== '') return opt;
  const raw = process.env.PIPELINE_FD_BRIDGE_TOPIC;
  if (raw != null && raw !== '') return raw;
  return DEFAULT_TOPIC;
}

function resolveHeartbeatMs(opt: number | undefined): number {
  if (opt != null && Number.isFinite(opt) && opt > 0) return opt;
  const raw = process.env.PIPELINE_FD_BRIDGE_HEARTBEAT_MS;
  if (raw != null && raw !== '') {
    const parsed = Number(raw);
    if (Number.isFinite(parsed) && parsed > 0) return parsed;
  }
  return DEFAULT_HEARTBEAT_INTERVAL_MS;
}

/**
 * Construct + start the FailureDetectorBridge and PubSubHeartbeatSource on the
 * supplied `cluster`. Returns `null` when the bridge is disabled (default).
 *
 * Lifecycle: when the returned handle's `stop()` resolves, both the bridge and
 * the heartbeat source have detached their listeners and timers. Safe to call
 * even if the cluster is in the middle of stopping.
 */
export async function setupFailureDetectorBridge(
  cluster: Cluster,
  opts: FailureDetectorBridgeOptions = {},
): Promise<FailureDetectorBridgeHandle | null> {
  const enabled = opts.enabled ?? isEnvEnabled();
  if (!enabled) {
    return null;
  }

  const topic = resolveTopic(opts.topic);
  const heartbeatIntervalMs = resolveHeartbeatMs(opts.heartbeatIntervalMs);

  // The heartbeat source publishes our own heartbeat onto `topic` and
  // subscribes to the same topic so every remote heartbeat is fed into
  // the failure detector via `recordNodeActivity(publisherNodeId)`.
  const heartbeatSource = new PubSubHeartbeatSource(
    cluster.pubsub,
    cluster.failureDetector,
    cluster.clusterManager.localNodeId,
    {
      topic,
      heartbeatIntervalMs,
    },
  );

  // FailureDetectorBridgeTargets shape is { router?, connectionRegistry?, lock? }.
  // We supply the cluster facade's router + lock; the gateway has no
  // ConnectionRegistry at this layer (that's a connections-fabric concern,
  // not a pipeline concern), so we leave it undefined. All fields are
  // optional — the bridge no-ops on missing collaborators.
  const targets: FailureDetectorBridgeTargets = {
    router: cluster.router,
    lock: cluster.lock,
  };

  const bridge = new FailureDetectorBridge(
    cluster.failureDetector,
    targets,
    { handleSuspected: true },
  );

  await heartbeatSource.start();
  await bridge.start();

  return {
    stop: async () => {
      // Reverse construction order — stop the bridge first so its event
      // listeners are detached before the source stops publishing.
      await bridge.stop();
      await heartbeatSource.stop();
    },
  };
}
