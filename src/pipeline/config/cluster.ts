// platform-api/src/pipeline/config/cluster.ts
//
// Pure builder for the distributed-core `ClusterConfig` literal that is fed
// into `Cluster.create()`. Splits each concern (transport, pubsub, registry,
// failureDetection) into its own helper so individual streams can extend them
// without touching bootstrap.ts.
//
// The builder takes already-resolved values (no env reads) so it stays unit-
// testable. Env resolution stays in bootstrap.ts's `resolveOptions()`.
//
// One env-reading helper does live here — `resolveRegistryMode()` — because
// it produces a value that several places in `BuildClusterConfigArgs` and
// `BuildAppLayerRegistriesArgs` need to read consistently. Keeping it next
// to `buildRegistryConfig` co-locates the contract.

import type {
  ClusterConfig,
  ClusterPubSubConfig,
  ClusterTransportConfig,
  MetricsRegistry,
  RegistryConfig,
  ClusterFailureDetectionConfig,
  RaftConfig,
  RaftRpcSigner,
} from 'distributed-core';
import { DEFAULT_RAFT_CONFIG } from 'distributed-core';

export type PipelineTransportKind = 'in-memory' | 'websocket' | 'tcp' | 'udp' | 'http';

/**
 * Modes the pipeline registry can run in. Mirrors the env var
 * `PIPELINE_REGISTRY_MODE` ('memory' | 'wal' | 'raft').
 *
 *  - 'memory' — no durability, hermetic per-process. Default in tests.
 *  - 'wal'    — on-disk WAL-backed entity registry. Default when a wal
 *               path is configured.
 *  - 'raft'   — cluster's entity registry is `RaftEntityRegistry`; writes
 *               are linearizable through consensus. Requires `raftDataDir`.
 *               As of distributed-core v0.10.0 the resource-side registry
 *               can also adopt the cluster's RaftEntityRegistry directly
 *               via the new `ResourceRegistryConfig.entityRegistry?` slot
 *               (techdebt 5.2). See config/registries.ts.
 */
export type PipelineRegistryMode = 'memory' | 'wal' | 'raft';

/**
 * Modes the pipeline pubsub can run in. Mirrors the env var
 * `PIPELINE_PUBSUB_MODE` ('memory' | 'redis').
 *
 *  - 'memory' — in-memory pubsub fabric, hermetic per-process. Default
 *               for single-node deploys.
 *  - 'redis'  — Redis-backed pubsub for multi-node communication. Requires
 *               REDIS_URL to be set.
 */
export type PipelinePubSubMode = 'memory' | 'redis';

export interface BuildClusterConfigArgs {
  /** Stable node id resolved up-front by `resolveStableNodeId`. */
  nodeId: string;
  /** Cluster gossip transport. Single-process deploys use 'in-memory'. */
  transport: PipelineTransportKind;
  /** Local bind port for non-in-memory transports. Ignored for in-memory. */
  basePort: number;
  /**
   * Seed nodes for TCP transport discovery. Comma-separated host:port list
   * (e.g. "platform-api-0:7100,platform-api-1:7100,platform-api-2:7100").
   * Only used when `transport === 'tcp'`; ignored for in-memory.
   */
  seedNodes: string[];
  /**
   * Pubsub mode. Controls whether the cluster uses in-memory or Redis-backed
   * pubsub. Resolved from env / opts via `resolvePubSubMode()` in bootstrap.
   */
  pubsubMode: PipelinePubSubMode;
  /**
   * Redis connection URL. Required when `pubsubMode === 'redis'`; ignored
   * otherwise.
   */
  redisUrl: string | undefined;
  /**
   * Resolved registry mode. Drives the `'memory' | 'wal' | 'raft'` branch
   * of `buildRegistryConfig`. Bootstrap resolves this from env / opts via
   * `resolveRegistryMode()` and passes the result through.
   */
  registryMode: PipelineRegistryMode;
  /**
   * WAL file path. Required when `registryMode === 'wal'`; ignored otherwise.
   */
  registryWalFilePath: string | undefined;
  /**
   * On-disk dataDir for the Raft log + persistent state + snapshots.
   * Required when `registryMode === 'raft'`; ignored otherwise.
   */
  raftDataDir: string | undefined;
  /**
   * Optional Raft RPC signer (produced by `config/signer.ts:buildRaftSigner`).
   * When set, RaftRpcRouter signs every outbound RPC and verifies every
   * inbound RPC. When `undefined`, RPCs are unsigned (back-compat).
   * Only consulted when `registryMode === 'raft'`.
   */
  raftSigner: RaftRpcSigner | undefined;
  /**
   * Raft config overrides (Phase 4.2). Env-resolved knobs for snapshot
   * threshold, election/heartbeat timers, max batch size, pre-vote, clock
   * drift, proposal timeout, plus the Phase-4-specific fixed overrides
   * (staleReadsAllowed=false, leaseReadEnabled=true). Only consulted when
   * `registryMode === 'raft'`.
   */
  raftConfigOverrides: Partial<Omit<RaftConfig, 'dataDir' | 'metrics' | 'signer'>> | undefined;
  /**
   * Whether to apply jest-fast failure-detection timers (100ms / 600ms vs
   * the prod 1s / 6s). Detected from NODE_ENV / JEST_WORKER_ID at the
   * bootstrap entry; passed through here so the builder stays env-agnostic.
   */
  fastTimers: boolean;
  /** platform-api's MetricsRegistry singleton. Threaded into ResourceRouter / DistributedLock. */
  metricsRegistry: MetricsRegistry;
}

// ---------------------------------------------------------------------------
// Sub-builders — each owns ONE concern in `ClusterConfig`. Streams that
// modify a single concern only edit one of these.
// ---------------------------------------------------------------------------

export function buildTransportConfig(
  transport: PipelineTransportKind,
  basePort: number,
  seedNodes: string[],
): ClusterTransportConfig {
  if (transport === 'in-memory') {
    return { type: 'in-memory' };
  }
  if (transport === 'tcp' && seedNodes.length > 0) {
    return { type: 'tcp', port: basePort, seedNodes };
  }
  return { type: transport, port: basePort };
}

export interface BuildPubSubConfigArgs {
  mode: PipelinePubSubMode;
  redisUrl?: string;
}

/**
 * Build the cluster-side `ClusterPubSubConfig` for `Cluster.create()`. Each
 * mode branch validates its own preconditions (redisUrl when 'redis') so
 * misconfiguration surfaces with a clear, actionable error before the cluster
 * comes up.
 */
export function buildPubSubConfig(args: BuildPubSubConfigArgs): ClusterPubSubConfig {
  switch (args.mode) {
    case 'memory':
      return { type: 'memory' };
    case 'redis':
      if (!args.redisUrl || args.redisUrl.trim() === '') {
        throw new Error(
          `[pipeline:config] buildPubSubConfig: 'redis' mode requires a redisUrl. `
          + `Set REDIS_URL to a valid Redis connection string (e.g. redis://localhost:6379).`,
        );
      }
      return { type: 'redis', url: args.redisUrl };
    default: {
      const _exhaustive: never = args.mode;
      throw new Error(`[pipeline:config] buildPubSubConfig: unhandled mode: ${String(_exhaustive)}`);
    }
  }
}

export interface BuildRegistryConfigArgs {
  mode: PipelineRegistryMode;
  walFilePath?: string;
  raftDataDir?: string;
  metrics?: MetricsRegistry;
  signer?: RaftRpcSigner;
  /**
   * Raft configuration overrides. When provided, these override the defaults
   * from distributed-core's DEFAULT_RAFT_CONFIG. Only consulted when
   * mode === 'raft'.
   */
  raftConfigOverrides?: Partial<Omit<RaftConfig, 'dataDir' | 'metrics' | 'signer'>>;
}

/**
 * Build the cluster-side `RegistryConfig` for `Cluster.create()`. Each mode
 * branch validates its own preconditions (walFilePath when 'wal', raftDataDir
 * when 'raft') so misconfiguration surfaces with a clear, actionable error
 * before the cluster comes up.
 *
 * 'raft' threads `metrics` and `signer` into `RaftConfig` so RaftNode emits
 * proposal / election / replication / snapshot counters into the same registry
 * the rest of platform-api uses, and signs every RPC when a per-node KeyManager
 * is provisioned.
 */
export function buildRegistryConfig(args: BuildRegistryConfigArgs): RegistryConfig {
  switch (args.mode) {
    case 'memory':
      return { type: 'memory' };
    case 'wal':
      if (!args.walFilePath) {
        throw new Error(
          `[pipeline:config] buildRegistryConfig: 'wal' mode requires a walFilePath. `
          + `Either provide PIPELINE_REGISTRY_WAL_PATH or switch PIPELINE_REGISTRY_MODE to 'memory'.`,
        );
      }
      return { type: 'wal', walPath: args.walFilePath };
    case 'raft': {
      if (!args.raftDataDir || args.raftDataDir.trim() === '') {
        throw new Error(
          `[pipeline:config] buildRegistryConfig: 'raft' mode requires a raftDataDir. `
          + `Set PIPELINE_RAFT_DATA_DIR to a writable path (e.g. /var/lib/platform-api/raft).`,
        );
      }
      const raftConfig: RaftConfig = {
        // Spread distributed-core defaults first so future timer/batch/preVote
        // knobs land for free as the upstream defaults evolve.
        // DEFAULT_RAFT_CONFIG is typed as Omit<RaftConfig, 'dataDir'> by design,
        // so we add `dataDir` explicitly below.
        ...DEFAULT_RAFT_CONFIG,
        // Env-overridable config knobs from raftConfigOverrides (Phase 4.2).
        ...(args.raftConfigOverrides ?? {}),
        // Fixed overrides last — dataDir, metrics, signer always come from args.
        dataDir: args.raftDataDir,
        ...(args.metrics ? { metrics: args.metrics } : {}),
        ...(args.signer ? { signer: args.signer } : {}),
      };
      return { type: 'raft', raftConfig };
    }
    default: {
      const _exhaustive: never = args.mode;
      throw new Error(`[pipeline:config] buildRegistryConfig: unhandled mode: ${String(_exhaustive)}`);
    }
  }
}

/**
 * Resolve the effective registry mode from env + a fallback hint. Single
 * source of truth so bootstrap and downstream consumers (e.g. the resource-
 * side registry builder) all see the same mode.
 *
 * Priority:
 *   1. Explicit `PIPELINE_REGISTRY_MODE` env var ('memory' | 'wal' | 'raft').
 *   2. `walFilePath` provided → 'wal'.
 *   3. Fallback → 'memory'.
 */
export function resolveRegistryMode(
  walFilePath: string | undefined,
): PipelineRegistryMode {
  const raw = process.env.PIPELINE_REGISTRY_MODE;
  if (raw === 'memory' || raw === 'wal' || raw === 'raft') {
    return raw;
  }
  if (raw !== undefined && raw !== '') {
    throw new Error(
      `[pipeline:config] PIPELINE_REGISTRY_MODE must be one of 'memory' | 'wal' | 'raft', got: ${raw}`,
    );
  }
  return walFilePath ? 'wal' : 'memory';
}

/**
 * Resolve the effective pubsub mode from env. Single source of truth so
 * bootstrap and downstream consumers all see the same mode.
 *
 * Priority:
 *   1. Explicit `PIPELINE_PUBSUB_MODE` env var ('memory' | 'redis').
 *   2. Fallback → 'memory'.
 */
export function resolvePubSubMode(): PipelinePubSubMode {
  const raw = process.env.PIPELINE_PUBSUB_MODE;
  if (raw === 'memory' || raw === 'redis') {
    return raw;
  }
  if (raw !== undefined && raw !== '') {
    throw new Error(
      `[pipeline:config] PIPELINE_PUBSUB_MODE must be one of 'memory' | 'redis', got: ${raw}`,
    );
  }
  return 'memory';
}

/**
 * Resolve Raft config overrides from env vars. Returns an object with only
 * the fields that are explicitly set via env; unset vars are omitted so
 * DEFAULT_RAFT_CONFIG defaults take precedence.
 *
 * Supported env vars (Phase 4.2):
 *   RAFT_SNAPSHOT_THRESHOLD        — default 10000
 *   RAFT_ELECTION_TIMEOUT_MIN      — default 150
 *   RAFT_ELECTION_TIMEOUT_MAX      — default 300
 *   RAFT_HEARTBEAT_INTERVAL        — default 50
 *   RAFT_MAX_BATCH_SIZE            — default 100
 *   RAFT_PRE_VOTE_ENABLED          — default true
 *   RAFT_MAX_CLOCK_DRIFT_MS        — default 50
 *   RAFT_PROPOSAL_TIMEOUT_MS       — default 5000
 *   RAFT_STALE_READS_ALLOWED       — Phase 4.2 fixed: false (approvals must be linearizable)
 *   RAFT_LEASE_READ_ENABLED        — Phase 4.2 fixed: true (~50ms-stale reads without quorum)
 */
export function resolveRaftConfigOverrides(): Partial<Omit<RaftConfig, 'dataDir' | 'metrics' | 'signer'>> {
  const overrides: Partial<Omit<RaftConfig, 'dataDir' | 'metrics' | 'signer'>> = {};

  // Snapshot threshold
  const snapshotThreshold = process.env.RAFT_SNAPSHOT_THRESHOLD;
  if (snapshotThreshold !== undefined && snapshotThreshold !== '') {
    const parsed = Number(snapshotThreshold);
    if (!Number.isFinite(parsed) || parsed <= 0 || !Number.isInteger(parsed)) {
      throw new Error(
        `[pipeline:config] RAFT_SNAPSHOT_THRESHOLD must be a positive integer, got: ${snapshotThreshold}`,
      );
    }
    overrides.snapshotThreshold = parsed;
  }

  // Timer config knobs
  const electionTimeoutMin = process.env.RAFT_ELECTION_TIMEOUT_MIN;
  const electionTimeoutMax = process.env.RAFT_ELECTION_TIMEOUT_MAX;
  const heartbeatInterval = process.env.RAFT_HEARTBEAT_INTERVAL;
  if (electionTimeoutMin !== undefined || electionTimeoutMax !== undefined || heartbeatInterval !== undefined) {
    overrides.timerConfig = { ...DEFAULT_RAFT_CONFIG.timerConfig };
    if (electionTimeoutMin !== undefined && electionTimeoutMin !== '') {
      const parsed = Number(electionTimeoutMin);
      if (!Number.isFinite(parsed) || parsed <= 0 || !Number.isInteger(parsed)) {
        throw new Error(
          `[pipeline:config] RAFT_ELECTION_TIMEOUT_MIN must be a positive integer, got: ${electionTimeoutMin}`,
        );
      }
      overrides.timerConfig.electionTimeoutMinMs = parsed;
    }
    if (electionTimeoutMax !== undefined && electionTimeoutMax !== '') {
      const parsed = Number(electionTimeoutMax);
      if (!Number.isFinite(parsed) || parsed <= 0 || !Number.isInteger(parsed)) {
        throw new Error(
          `[pipeline:config] RAFT_ELECTION_TIMEOUT_MAX must be a positive integer, got: ${electionTimeoutMax}`,
        );
      }
      overrides.timerConfig.electionTimeoutMaxMs = parsed;
    }
    if (heartbeatInterval !== undefined && heartbeatInterval !== '') {
      const parsed = Number(heartbeatInterval);
      if (!Number.isFinite(parsed) || parsed <= 0 || !Number.isInteger(parsed)) {
        throw new Error(
          `[pipeline:config] RAFT_HEARTBEAT_INTERVAL must be a positive integer, got: ${heartbeatInterval}`,
        );
      }
      overrides.timerConfig.heartbeatIntervalMs = parsed;
    }
  }

  // Max batch size
  const maxBatchSize = process.env.RAFT_MAX_BATCH_SIZE;
  if (maxBatchSize !== undefined && maxBatchSize !== '') {
    const parsed = Number(maxBatchSize);
    if (!Number.isFinite(parsed) || parsed <= 0 || !Number.isInteger(parsed)) {
      throw new Error(
        `[pipeline:config] RAFT_MAX_BATCH_SIZE must be a positive integer, got: ${maxBatchSize}`,
      );
    }
    overrides.maxBatchSize = parsed;
  }

  // Pre-vote enabled
  const preVoteEnabled = process.env.RAFT_PRE_VOTE_ENABLED;
  if (preVoteEnabled !== undefined && preVoteEnabled !== '') {
    const lower = preVoteEnabled.toLowerCase();
    if (lower === 'true' || lower === '1') {
      overrides.preVoteEnabled = true;
    } else if (lower === 'false' || lower === '0') {
      overrides.preVoteEnabled = false;
    } else {
      throw new Error(
        `[pipeline:config] RAFT_PRE_VOTE_ENABLED must be 'true' or 'false', got: ${preVoteEnabled}`,
      );
    }
  }

  // Max clock drift
  const maxClockDrift = process.env.RAFT_MAX_CLOCK_DRIFT_MS;
  if (maxClockDrift !== undefined && maxClockDrift !== '') {
    const parsed = Number(maxClockDrift);
    if (!Number.isFinite(parsed) || parsed < 0 || !Number.isInteger(parsed)) {
      throw new Error(
        `[pipeline:config] RAFT_MAX_CLOCK_DRIFT_MS must be a non-negative integer, got: ${maxClockDrift}`,
      );
    }
    overrides.maxClockDriftMs = parsed;
  }

  // Proposal timeout
  const proposalTimeout = process.env.RAFT_PROPOSAL_TIMEOUT_MS;
  if (proposalTimeout !== undefined && proposalTimeout !== '') {
    const parsed = Number(proposalTimeout);
    if (!Number.isFinite(parsed) || parsed <= 0 || !Number.isInteger(parsed)) {
      throw new Error(
        `[pipeline:config] RAFT_PROPOSAL_TIMEOUT_MS must be a positive integer, got: ${proposalTimeout}`,
      );
    }
    overrides.proposalTimeoutMs = parsed;
  }

  // Phase 4.2 fixed overrides: staleReadsAllowed=false, leaseReadEnabled=true
  // (approvals must be linearizable; lease reads give ~50ms-stale without quorum).
  overrides.staleReadsAllowed = false;
  overrides.leaseReadEnabled = true;

  return overrides;
}

/**
 * Build the failure-detection config. Returns `undefined` outside test mode
 * so distributed-core falls back to its production defaults (1s / 6s).
 *
 * `Cluster.create()` accepts `failureDetection: undefined` — it does NOT need
 * a literal `{}` for the defaulted path, so dropping the key entirely is
 * preferable to passing an empty object.
 */
export function buildFailureDetectionConfig(
  fastTimers: boolean,
): ClusterFailureDetectionConfig | undefined {
  if (!fastTimers) return undefined;
  return {
    heartbeatMs: 100,
    deadTimeoutMs: 600,
    activeProbing: true,
  };
}

// ---------------------------------------------------------------------------
// Top-level builder
// ---------------------------------------------------------------------------

/**
 * Compose a full `ClusterConfig` from already-resolved bootstrap arguments.
 * Pure function — no env reads, no fs I/O. Intentionally side-effect-free
 * so future tests can pin the literal shape.
 *
 * Multi-node mode (Phase 4): when `pubsubMode === 'redis'`, the topic becomes
 * shared (`'pipeline'`) across all replicas instead of per-node. This enables
 * the cluster to coordinate state across multiple pods.
 */
export function buildClusterConfig(args: BuildClusterConfigArgs): ClusterConfig {
  const fdConfig = buildFailureDetectionConfig(args.fastTimers);

  // Topic: shared across replicas when using Redis pubsub, per-node otherwise.
  const topic = args.pubsubMode === 'redis' ? 'pipeline' : `pipeline-${args.nodeId}`;

  const config: ClusterConfig = {
    nodeId: args.nodeId,
    topic,
    pubsub: buildPubSubConfig({
      mode: args.pubsubMode,
      redisUrl: args.redisUrl,
    }),
    transport: buildTransportConfig(args.transport, args.basePort, args.seedNodes),
    registry: buildRegistryConfig({
      mode: args.registryMode,
      walFilePath: args.registryWalFilePath,
      raftDataDir: args.raftDataDir,
      metrics: args.metricsRegistry,
      signer: args.raftSigner,
      raftConfigOverrides: args.raftConfigOverrides,
    }),
    metrics: args.metricsRegistry,
    // No `logger` — the facade defaults to a NOOP logger, which is exactly
    // what we want. Test-mode noise reduction is handled by the per-bootstrap
    // logger constructed in bootstrap.ts.
  };

  if (fdConfig !== undefined) {
    config.failureDetection = fdConfig;
  }

  return config;
}
