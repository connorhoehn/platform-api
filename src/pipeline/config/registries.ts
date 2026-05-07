// social-api/src/pipeline/config/registries.ts
//
// Construct the application-layer registries that ApplicationRegistry passes
// into PipelineModule's context. These are sibling concepts to the Cluster
// facade — the facade owns ClusterManager / PubSubManager / EntityRegistry /
// ResourceRouter / DistributedLock; ResourceRegistry / ResourceTypeRegistry /
// ResourceTopologyManager / ApplicationRegistry sit ABOVE the cluster
// substrate.
//
// Mode mirrors the cluster's registry config so the durability decision is
// consistent across substrate and resource layers:
//   - 'memory' → entityRegistryType: 'memory'
//   - 'wal'    → entityRegistryType: 'wal' (requires registryWalFilePath)
//   - 'raft'   → entityRegistry: cluster.registry (the cluster's
//                RaftEntityRegistry, shared via the v0.10.0 injection slot)
//
// The pre-v0.10.0 downgrade-with-warning shim that fell back to wal-or-
// memory in raft mode has been removed; ResourceRegistry now directly
// adopts the cluster's RaftEntityRegistry per techdebt 5.2.

import {
  ApplicationRegistry,
  MetricsTracker,
  ResourceRegistry,
  ResourceTopologyManager,
  ResourceTypeRegistry,
  StateAggregator,
} from 'distributed-core';
import type { ClusterManager, EntityRegistry } from 'distributed-core';
import type { PipelineRegistryMode } from './cluster';

export interface BuildAppLayerRegistriesArgs {
  nodeId: string;
  clusterMgr: ClusterManager;
  /**
   * Resolved registry mode (same value passed into `buildClusterConfig`).
   * Drives the `entityRegistry*` slot used by the resource-side
   * `ResourceRegistry`.
   */
  mode: PipelineRegistryMode;
  /**
   * WAL file path. Required when `mode === 'wal'`. Ignored for 'memory'
   * and 'raft' modes.
   */
  registryWalFilePath: string | undefined;
  /**
   * Cluster's `EntityRegistry` instance — `cluster.registry` from
   * `Cluster.create()`. Required when `mode === 'raft'` (v0.10.0+);
   * `ResourceRegistry` adopts the same `RaftEntityRegistry` via the
   * `entityRegistry?` injection slot so resource-typed records share
   * Raft durability with the cluster's substrate state.
   */
  clusterEntityRegistry?: EntityRegistry;
}

export interface AppLayerRegistries {
  resourceRegistry: ResourceRegistry;
  resourceTypeRegistry: ResourceTypeRegistry;
  topologyManager: ResourceTopologyManager;
  moduleRegistry: ApplicationRegistry;
  /**
   * Non-fatal warnings produced while resolving the config. Empty in the
   * v0.10.0+ codepaths; retained so bootstrap doesn't have to special-case
   * the absence of warnings.
   */
  warnings: string[];
}

/**
 * Construct + start the application-layer registries. Caller is responsible
 * for stopping them in the correct order during shutdown:
 *   1. moduleRegistry.stop()
 *   2. resourceRegistry.stop()
 *
 * (The cluster's substrate registries — `cluster.registry`, etc. — are torn
 * down by `cluster.stop()`. When `mode === 'raft'` the same `cluster.registry`
 * is injected into ResourceRegistry, but ResourceRegistry's `stop()` does NOT
 * tear down the underlying entity registry — that stays with the cluster.)
 *
 * Note: `resourceTypeRegistry`, `stateAggregator`, and `metricsTracker` are
 * constructor-only collaborators here. We deliberately do NOT call
 * `stateAggregator.start()` — single-node mode has nothing to aggregate, and
 * StateAggregator's setupMessageHandling() in the constructor only installs
 * a transport listener which is harmless.
 */
export async function buildAppLayerRegistries(
  args: BuildAppLayerRegistriesArgs,
): Promise<AppLayerRegistries> {
  const { nodeId, clusterMgr, mode, registryWalFilePath, clusterEntityRegistry } = args;

  const warnings: string[] = [];

  let resourceRegistry: ResourceRegistry;

  switch (mode) {
    case 'memory':
      resourceRegistry = new ResourceRegistry({
        nodeId,
        entityRegistryType: 'memory',
      });
      break;
    case 'wal':
      if (!registryWalFilePath) {
        throw new Error(
          `[pipeline:config] buildAppLayerRegistries: 'wal' mode requires registryWalFilePath. `
          + `Either set PIPELINE_REGISTRY_WAL_PATH or switch PIPELINE_REGISTRY_MODE to 'memory'.`,
        );
      }
      resourceRegistry = new ResourceRegistry({
        nodeId,
        entityRegistryType: 'wal',
        entityRegistryConfig: { walConfig: { filePath: registryWalFilePath } },
      });
      break;
    case 'raft': {
      if (!clusterEntityRegistry) {
        throw new Error(
          `[pipeline:config] buildAppLayerRegistries: 'raft' mode requires clusterEntityRegistry `
          + `(the Cluster's RaftEntityRegistry from cluster.registry). `
          + `Bootstrap should pass it after cluster.start() returns.`,
        );
      }
      // v0.10.0 (techdebt 5.2): ResourceRegistry adopts the cluster's
      // RaftEntityRegistry directly via the entityRegistry injection slot.
      // Resource-typed records share Raft durability with the rest of the
      // cluster's entity state — no more downgrade.
      resourceRegistry = new ResourceRegistry({
        nodeId,
        entityRegistry: clusterEntityRegistry,
      });
      break;
    }
    default: {
      const _exhaustive: never = mode;
      throw new Error(`[pipeline:config] buildAppLayerRegistries: unhandled mode: ${String(_exhaustive)}`);
    }
  }

  await resourceRegistry.start();

  const resourceTypeRegistry = new ResourceTypeRegistry();

  const stateAggregator = new StateAggregator(clusterMgr);
  const metricsTracker = new MetricsTracker({});

  const topologyManager = new ResourceTopologyManager(
    clusterMgr,
    resourceRegistry,
    resourceTypeRegistry,
    stateAggregator,
    metricsTracker,
  );

  const moduleRegistry = new ApplicationRegistry(
    clusterMgr,
    resourceRegistry,
    topologyManager,
  );
  await moduleRegistry.start();

  return {
    resourceRegistry,
    resourceTypeRegistry,
    topologyManager,
    moduleRegistry,
    warnings,
  };
}
