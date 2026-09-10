// platform-api/src/pipeline/bootstrap.ts
//
// Bootstrap that stands up an in-process distributed-core Cluster +
// PipelineModule for the platform-api service. This file is the *composition
// layer* — it resolves env-derived options, calls each `config/*` builder,
// then stitches the cluster lifecycle (Cluster.create / start, registry
// start, module register/initialize, shutdown) together.
//
// Per-concern config builders live in `./config/`:
//
//   - config/cluster.ts          — buildClusterConfig() (transport / pubsub /
//                                  registry / failureDetection)
//   - config/pipelineModule.ts   — buildPipelineModuleConfig()
//   - config/registries.ts       — buildAppLayerRegistries()
//   - config/identity.ts         — resolveStableNodeId()
//   - config/walPreflight.ts     — verifyWalParentWritable()
//
// In-flight streams that extend these builders (without touching this file):
//   - Stream 7 (RaftFactory)  → config/cluster.ts buildRegistryConfig()
//   - Stream 8 (RpcSigning)   → config/cluster.ts (RaftConfig.signer slot)
//   - Stream 9 (FDBridge)     → new file config/failureDetectorBridge.ts
//                                wired AFTER cluster.start() in bootstrap.
//   - Stream 10 (HHQueue)     → new file config/hintedHandoffQueue.ts
//                                wired AFTER cluster.start() in bootstrap.
//
// What the Cluster facade gives us "for free":
//   - cluster.scope('pipeline')   namespacing for locks/elections (NOT YET USED — see field notes).
//   - cluster.snapshot()          postmortem aggregator.
//   - cluster.lock                replaces hand-wired DistributedLock.
//   - metrics: config field       single-config metrics threading (FR-6).
//
// Tunables (env at call time, safe defaults):
//   PIPELINE_CLUSTER_TRANSPORT  'in-memory' | 'tcp' | 'websocket' | 'http' |
//                                'udp'. Default 'in-memory'. Single-process
//                                deploys must use 'in-memory'; the other
//                                transports are reserved for Phase 4 multi-node.
//   PIPELINE_TRANSPORT_PORT     Local bind port for non-in-memory transports.
//                                Default 7100 (matches Phase 4 planning).
//   SEED_NODES                  Comma-separated host:port list for TCP
//                                transport discovery (e.g.
//                                "platform-api-0:7100,platform-api-1:7100").
//                                Only used when PIPELINE_CLUSTER_TRANSPORT=tcp.
//   PIPELINE_PUBSUB_MODE        'memory' | 'redis'. Default 'memory'. Multi-node
//                                deploys use 'redis' for cross-replica pubsub.
//   REDIS_URL                   Redis connection string. Required when
//                                PIPELINE_PUBSUB_MODE=redis (e.g.
//                                "redis://localhost:6379").
//   PIPELINE_WAL_PATH           EventBus WAL path. When set, pipeline run
//                                state survives restart. Default:
//                                  '/var/lib/platform-api/pipeline-wal.log' (prod)
//                                  '/tmp/pipeline-wal.log'                 (dev)
//                                Set =disabled to opt out and run in-memory.
//   PIPELINE_REGISTRY_WAL_PATH  ResourceRegistry entity WAL path. When set,
//                                the entity registry uses 'wal' mode. Defaults
//                                to undefined in test mode (per-test isolation),
//                                and to durable on-disk paths otherwise.
//   PIPELINE_IDENTITY_FILE      Stable node identity file. Loaded via
//                                distributed-core's loadOrCreateNodeId(). Set
//                                =disabled to mint a fresh ephemeral id every
//                                boot (warning logged).
//   PIPELINE_TEST_FAST_MODE     'false' to disable test fast-timers; default
//                                enabled when NODE_ENV=test or JEST_WORKER_ID
//                                is set.
//
// The bridge in src/pipeline-bridge subscribes to module.getEventBus() and
// routes the six bridge surfaces (getRun, getHistory, listActiveRuns,
// getMetrics, getPendingApprovals, pipeline.run.reassigned) into WebSockets.

import { sign as jwtSign } from 'jsonwebtoken';
import { Cluster, IS_TEST_ENV } from 'distributed-core';
import { PipelineModule } from 'distributed-core/applications/pipeline';
import { InMemoryDeadLetterQueue, wrap } from 'distributed-core/messaging-queue';
import type {
  BusEvent,
  EventBusCompactionOptions,
} from 'distributed-core';
import type { LLMClient } from 'distributed-core/applications/pipeline';
import type { DeadLetterQueue, Envelope } from 'distributed-core/messaging-queue';

import { createLLMClient } from './factories/LLMClientFactory';
import { ExecutorAuditSubscriber } from './executor-audit-subscriber';
import { auditRepo } from './audit-repository';
import { setupFailureDetectorBridge } from './config/failureDetectorBridge';
import {
  getRegistry as getMetricsRegistry,
  getQueueMetrics,
  incrementBusDeadLetter,
  recordPipelineExecutorBusy,
} from '../observability/metrics';
import {
  buildClusterConfig,
  resolveRegistryMode,
  resolvePubSubMode,
  resolveRaftConfigOverrides,
  type PipelineRegistryMode,
  type PipelineTransportKind,
  type PipelinePubSubMode,
} from './config/cluster';
import { buildPipelineModuleConfig } from './config/pipelineModule';
import { buildJudgeStepHandlers } from './judge/registerJudgeHandlers';
import { buildVisionStepHandlers } from './image/registerVisionHandlers';
import { buildMemoryStepHandlers } from './memory/registerMemoryHandlers';
import { buildAgentStepHandlers } from './agent/registerAgentHandlers';
import { buildDocumentAgentHandlers } from '../documents/documentAgentPipeline';
import { buildChatCommandHandlers } from '../documents/chatCommandHandlers';
import { getCustomStepHandlers, registerCustomStepHandler } from './customStepHandlers';
import { buildRecordingPipelineHandlers } from '../recordings/recordingPipelines';
import { buildLiveCaptionsHandlers } from '../recordings/liveCaptions';
import { getJudgmentAggregateRepo } from './judge/judgeServices';
import { JudgeAggregatorSubscriber } from './judge/judge-aggregator-subscriber';
import { buildAppLayerRegistries } from './config/registries';
import { resolveStableNodeId } from './config/identity';
import { setupHintedHandoffQueue } from './config/hintedHandoffQueue';
import { buildRaftSigner } from './config/signer';
import { RunPersistenceService, setRunPersistenceService } from './runPersistence';
import { StuckRunDetector } from './StuckRunDetector';
import { scanOrphanedRuns } from './scanOrphanedRuns';
import { resumeInterruptedRuns } from './resumeInterruptedRuns';
import { definitionsRepo } from './definitions-repository';
import { attachFailedRunDeadLetters, isRunRetrigger, retriggerFromDeadLetter, type RunRetriggerPayload } from './failedRunDeadLetters';
import { forgetDeadLetters, rehydrateDeadLetters, setDeadLetterStore } from './deadLetterStore';
import { getSystemPipeline } from './systemPipelines';
import { getPipelineBridge } from '../routes/pipelineTriggers';
import { runWalPreflight } from './WalPreflight';
import { createShutdown } from './ShutdownOrchestrator';
import {
  startPipelineEventRedisRelay,
  type PipelineEventRedisRelay,
} from './event-redis-relay';
import log, { withContext } from '../lib/logger';
import { publishDurable, catalog } from '../lib/event-catalog';
import { createValidatingPublisher } from '@connorhoehn/event-catalog';

// ---------------------------------------------------------------------------
// Test-mode helpers
// ---------------------------------------------------------------------------

function isPipelineFastTestModeEnabled(): boolean {
  if (!IS_TEST_ENV) return false;
  const raw = process.env.PIPELINE_TEST_FAST_MODE;
  if (raw == null) return true;
  return raw.trim().toLowerCase() !== 'false';
}

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

export interface PipelineBootstrap {
  /** The live PipelineModule — bridge subscribes to its EventBus. */
  module: PipelineModule;
  /** Stable cluster node id (matches gossip membership). */
  nodeId: string;
  /**
   * EventBus dead-letter queue (T9 / lib-expansion-3). Populated by the
   * bus's deadLetterHandler when subscribers throw or publishes fail.
   * `null` when DLQ recording is disabled (PIPELINE_EVENT_BUS_DLQ_ENABLED=false).
   */
  dlq: DeadLetterQueue<BusEvent> | null;
  /** Coordinated tear-down: stops the module, then the cluster. */
  shutdown: () => Promise<void>;
}

function busEventAsEnvelope(event: BusEvent): Envelope<BusEvent> {
  // Stable envelope id — bus events already carry a string `id`. Fall back
  // to a synthetic id if a malformed event reaches the DLQ handler.
  const id = typeof event.id === 'string' && event.id.length > 0
    ? event.id
    : `${event.type}-${event.timestamp ?? Date.now()}`;
  const enqueuedAtMs = typeof event.timestamp === 'number' && Number.isFinite(event.timestamp)
    ? event.timestamp
    : Date.now();
  return wrap(event, {
    id,
    now: () => enqueuedAtMs,
    attemptCount: 0,
  });
}

export interface BootstrapOptions {
  walFilePath?: string;
  transport?: PipelineTransportKind;
  basePort?: number;
  llmClient?: LLMClient;
  identityFile?: string;
  registryWalFilePath?: string;
  /**
   * Pipeline run persistence repository. When provided, bootstrap subscribes
   * to pipeline:run:* events and persists run state to DDB. Optional —
   * defaults to undefined (run persistence disabled).
   */
  pipelineRunRepository?: import('../repositories/PipelineRunRepository').PipelineRunRepository;
  /**
   * DLQ persistence repository. When provided, the deadLetterHandler also
   * writes entries to DDB so they survive restarts. Optional — defaults to
   * undefined (DLQ persistence disabled, in-memory only).
   */
  dlqRepository?: import('../repositories/DLQRepository').DLQRepository;
  /**
   * Registry mode override. When set, takes precedence over the env-var
   * resolver in `resolveRegistryMode()`. Tests use this to opt into the
   * 'raft' branch without monkey-patching `process.env`.
   *
   *  - 'memory' → in-process EntityRegistry, no durability.
   *  - 'wal'    → on-disk WAL-backed EntityRegistry. Requires
   *               `registryWalFilePath` (or PIPELINE_REGISTRY_WAL_PATH).
   *  - 'raft'   → cluster's EntityRegistry is RaftEntityRegistry — writes
   *               are linearizable through consensus. Requires
   *               `raftDataDir` (or PIPELINE_RAFT_DATA_DIR). The
   *               ResourceRegistry's *resource-side* entity registry is
   *               downgraded to wal-or-memory because distributed-core
   *               HEAD's ResourceRegistry constructor can't host a Raft
   *               entity registry (see `config/registries.ts` for the
   *               full gap analysis).
   */
  registryMode?: PipelineRegistryMode;
  /**
   * On-disk dataDir for the Raft log + persistent state + snapshots. Only
   * consulted when `registryMode === 'raft'`. Overridden by
   * PIPELINE_RAFT_DATA_DIR when not provided here.
   */
  raftDataDir?: string;
  /**
   * Directory holding per-node signer keys. When set, each Raft RPC is
   * signed with the node's private key + verified on receipt. Overridden
   * by PIPELINE_RAFT_SIGNER_DIR when not provided here. When unset on
   * both, RPCs are unsigned (back-compat).
   */
  raftSignerDir?: string;
  /**
   * Stream 4 (BusCompact): EventBus auto-compaction interval (ms). When
   * set AND `walFilePath` is configured, the bus runs `compact()` on this
   * interval. Overridden by PIPELINE_EVENT_BUS_COMPACT_INTERVAL_MS when
   * not provided here. Default: undefined (no auto-compaction).
   */
  eventBusAutoCompactIntervalMs?: number;
  /**
   * Stream 4 (BusCompact): compaction options used at each tick.
   * Overridden by PIPELINE_EVENT_BUS_COMPACT_KEEP_LAST_N when not
   * provided here. Default: { keepLastNPerType: 100 } when an interval
   * is set.
   */
  eventBusAutoCompactOptions?: EventBusCompactionOptions;
}

interface ResolvedBootstrapOptions {
  transport: PipelineTransportKind;
  basePort: number;
  seedNodes: string[];
  pubsubMode: PipelinePubSubMode;
  redisUrl: string | undefined;
  walFilePath: string | undefined;
  walExplicitlyDisabled: boolean;
  identityFilePath: string | undefined;
  identityExplicitlyDisabled: boolean;
  registryWalFilePath: string | undefined;
  registryMode: PipelineRegistryMode;
  raftDataDir: string | undefined;
  raftSignerDir: string | undefined;
  // Stream 4 (BusCompact): EventBus auto-compaction config. Both undefined
  // means the bus runs in manual-compact-only mode.
  eventBusAutoCompactIntervalMs: number | undefined;
  eventBusAutoCompactOptions: EventBusCompactionOptions | undefined;
}

// ---------------------------------------------------------------------------
// Env / option resolution
// ---------------------------------------------------------------------------

function resolveOptions(opts: BootstrapOptions): ResolvedBootstrapOptions {
  const transport = (opts.transport
    ?? (process.env.PIPELINE_CLUSTER_TRANSPORT as BootstrapOptions['transport'])
    ?? 'in-memory') as PipelineTransportKind;
  const basePort = opts.basePort ?? Number(process.env.PIPELINE_TRANSPORT_PORT ?? '7100');
  if (!Number.isFinite(basePort) || basePort < 0) {
    throw new Error(`[pipeline] PIPELINE_TRANSPORT_PORT must be >= 0, got: ${basePort}`);
  }

  // Seed nodes for TCP transport discovery. Parse SEED_NODES env var as a
  // comma-separated list of host:port entries.
  const seedNodes: string[] = [];
  const rawSeedNodes = process.env.SEED_NODES;
  if (rawSeedNodes && rawSeedNodes.trim() !== '') {
    seedNodes.push(...rawSeedNodes.split(',').map(s => s.trim()).filter(s => s !== ''));
  }

  // Pubsub mode: env var → fallback to 'memory'.
  const pubsubMode: PipelinePubSubMode = resolvePubSubMode();
  const redisUrl = process.env.REDIS_URL;

  // WAL path resolution: opts > env > default. The magic value 'disabled'
  // (env only) opts out → in-memory mode.
  let walFilePath: string | undefined;
  let walExplicitlyDisabled = false;
  const rawEnv = process.env.PIPELINE_WAL_PATH;
  if (opts.walFilePath !== undefined) {
    walFilePath = opts.walFilePath;
  } else if (rawEnv !== undefined && rawEnv !== '') {
    if (rawEnv === 'disabled') {
      walFilePath = undefined;
      walExplicitlyDisabled = true;
    } else {
      walFilePath = rawEnv;
    }
  } else {
    walFilePath = process.env.NODE_ENV === 'production'
      ? '/var/lib/platform-api/pipeline-wal.log'
      : '/tmp/pipeline-wal.log';
  }

  // Identity path resolution mirrors WAL — both opts and env honor 'disabled'.
  // Tests get ephemeral identity (sharing IDs across sequential bootstrap
  // calls in one jest run breaks the existing "distinct nodeIds" suite).
  let identityFilePath: string | undefined;
  let identityExplicitlyDisabled = false;
  const rawIdentityEnv = process.env.PIPELINE_IDENTITY_FILE;
  if (opts.identityFile !== undefined) {
    if (opts.identityFile === 'disabled') {
      identityFilePath = undefined;
      identityExplicitlyDisabled = true;
    } else {
      identityFilePath = opts.identityFile;
    }
  } else if (rawIdentityEnv !== undefined && rawIdentityEnv !== '') {
    if (rawIdentityEnv === 'disabled') {
      identityFilePath = undefined;
      identityExplicitlyDisabled = true;
    } else {
      identityFilePath = rawIdentityEnv;
    }
  } else if (IS_TEST_ENV) {
    identityFilePath = undefined;
  } else {
    identityFilePath = process.env.NODE_ENV === 'production'
      ? '/var/lib/platform-api/node-identity'
      : '/tmp/platform-api-node-identity';
  }

  // Registry WAL: tests fall through to undefined → 'memory' for hermetic
  // isolation. No 'disabled' magic value here — callers wanting in-memory
  // behavior should run under test env or pass an empty value.
  const registryWalFilePath = resolveRegistryWalPath(opts.registryWalFilePath);

  // Registry mode: opts wins over env. resolveRegistryMode() reads
  // PIPELINE_REGISTRY_MODE; when absent, falls back to 'wal' if walPath is
  // set, else 'memory'.
  const registryMode: PipelineRegistryMode =
    opts.registryMode ?? resolveRegistryMode(registryWalFilePath);

  // Raft dataDir: explicit opt > env > sensible default for raft mode.
  const raftDataDir = opts.raftDataDir
    ?? process.env.PIPELINE_RAFT_DATA_DIR
    ?? (registryMode === 'raft'
      ? (process.env.NODE_ENV === 'production'
          ? '/var/lib/platform-api/raft'
          : `/tmp/platform-api-raft-${process.pid}`)
      : undefined);

  // Raft signer dir: per-node KeyManager secrets directory. Off by default
  // (unsigned RPCs); set to enable signing.
  const raftSignerDir = opts.raftSignerDir ?? process.env.PIPELINE_RAFT_SIGNER_DIR;

  // --- Stream 4 (BusCompact) — EventBus auto-compaction tunables ---
  //
  // PIPELINE_EVENT_BUS_COMPACT_INTERVAL_MS controls the cadence; when unset,
  // the bus runs no auto-compaction (manual `compact()` only).
  //
  // PIPELINE_EVENT_BUS_COMPACT_KEEP_LAST_N controls keepLastNPerType for
  // the compaction options. Defaults to 100 (matches upstream EventBus
  // default) when an interval is configured but no keep-N override is given.
  let eventBusAutoCompactIntervalMs: number | undefined;
  if (opts.eventBusAutoCompactIntervalMs !== undefined) {
    eventBusAutoCompactIntervalMs = opts.eventBusAutoCompactIntervalMs;
  } else {
    const envInterval = process.env.PIPELINE_EVENT_BUS_COMPACT_INTERVAL_MS;
    if (envInterval !== undefined && envInterval !== '') {
      const parsed = Number(envInterval);
      if (!Number.isFinite(parsed) || parsed < 0) {
        throw new Error(
          `[pipeline] PIPELINE_EVENT_BUS_COMPACT_INTERVAL_MS must be a non-negative number, got: ${envInterval}`,
        );
      }
      eventBusAutoCompactIntervalMs = parsed;
    }
  }

  let eventBusAutoCompactOptions: EventBusCompactionOptions | undefined;
  if (opts.eventBusAutoCompactOptions !== undefined) {
    eventBusAutoCompactOptions = opts.eventBusAutoCompactOptions;
  } else if (eventBusAutoCompactIntervalMs !== undefined) {
    // An interval was set but no explicit options object — read keepLastN
    // from env (default 100) and synthesize the options.
    const envKeepN = process.env.PIPELINE_EVENT_BUS_COMPACT_KEEP_LAST_N;
    let keepLastN = 100;
    if (envKeepN !== undefined && envKeepN !== '') {
      const parsed = Number(envKeepN);
      if (!Number.isFinite(parsed) || parsed < 0 || !Number.isInteger(parsed)) {
        throw new Error(
          `[pipeline] PIPELINE_EVENT_BUS_COMPACT_KEEP_LAST_N must be a non-negative integer, got: ${envKeepN}`,
        );
      }
      keepLastN = parsed;
    }
    eventBusAutoCompactOptions = { keepLastNPerType: keepLastN };
  }

  return {
    transport,
    basePort,
    seedNodes,
    pubsubMode,
    redisUrl,
    walFilePath,
    walExplicitlyDisabled,
    identityFilePath,
    identityExplicitlyDisabled,
    registryWalFilePath,
    registryMode,
    raftDataDir,
    raftSignerDir,
    eventBusAutoCompactIntervalMs,
    eventBusAutoCompactOptions,
  };
}

function resolveRegistryWalPath(explicit: string | undefined): string | undefined {
  if (explicit !== undefined && explicit !== '') return explicit;
  const envValue = process.env.PIPELINE_REGISTRY_WAL_PATH;
  if (envValue !== undefined && envValue !== '') return envValue;
  if (IS_TEST_ENV) return undefined;
  return process.env.NODE_ENV === 'production'
    ? '/var/lib/platform-api/pipeline-registry-wal.log'
    : '/tmp/pipeline-registry-wal.log';
}

// ---------------------------------------------------------------------------
// Bootstrap
// ---------------------------------------------------------------------------

/**
 * Stand up an in-process single-node cluster (via `Cluster.create()`) with a
 * PipelineModule registered. Returns the module + a shutdown function.
 * Idempotent only at the call site — callers are responsible for not
 * bootstrapping twice in the same process.
 */
export async function bootstrapPipeline(opts: BootstrapOptions = {}): Promise<PipelineBootstrap> {
  const {
    transport,
    basePort,
    seedNodes,
    pubsubMode,
    redisUrl,
    walFilePath,
    walExplicitlyDisabled,
    identityFilePath,
    identityExplicitlyDisabled,
    registryWalFilePath,
    registryMode,
    raftDataDir,
    raftSignerDir,
    eventBusAutoCompactIntervalMs,
    eventBusAutoCompactOptions,
  } = resolveOptions(opts);

  // --- Pre-flight: writability + identity ---
  await runWalPreflight({ walFilePath, registryWalFilePath, registryMode, raftDataDir });
  const { nodeId, persistentNodeId } = await resolveStableNodeId(identityFilePath);

  // Hold the Node event loop open during cluster setup. Every internal cluster
  // timer is `.unref()`'d, so without this hold a one-shot script (no HTTP
  // server) would see Node drain before Cluster.create()/start() resolves.
  const keepAlive = setInterval(() => { /* hold the event loop */ }, 1 << 30);

  const fastTimers = isPipelineFastTestModeEnabled();
  const metricsRegistry = getMetricsRegistry();

  // Build the per-node Raft signer (returns undefined unless
  // PIPELINE_RAFT_SIGNER_DIR / opts.raftSignerDir is set). When set, every
  // outbound Raft RPC is signed and every inbound RPC is verified using the
  // node's KeyManager-backed key pair.
  const raftSigner = buildRaftSigner({
    nodeId,
    keyManagerSecretsDir: raftSignerDir,
  });

  // Resolve Raft config overrides from env vars (Phase 4.2). Only consulted
  // when registryMode === 'raft'.
  const raftConfigOverrides = registryMode === 'raft' ? resolveRaftConfigOverrides() : undefined;

  // --- Cluster: build config → create → start ---
  const clusterConfig = buildClusterConfig({
    nodeId,
    transport,
    basePort,
    seedNodes,
    pubsubMode,
    redisUrl,
    registryMode,
    registryWalFilePath,
    raftDataDir,
    raftSigner,
    raftConfigOverrides,
    fastTimers,
    metricsRegistry,
  });
  const cluster = await Cluster.create(clusterConfig);
  await cluster.start();

  // Optional cluster sidecars wired AFTER cluster.start(). Each setup helper
  // is feature-flagged via env (see config/*.ts) and returns null when its
  // feature is disabled — making both opt-in and zero-cost when off.
  const fdBridge = await setupFailureDetectorBridge(cluster);
  const hintedHandoff = await setupHintedHandoffQueue(cluster);

  const clusterMgr = cluster.clusterManager;
  const pubsub = cluster.pubsub;

  // --- App-layer registries (sit ABOVE the cluster substrate) ---
  // When registryMode === 'raft' (v0.10.0+), ResourceRegistry adopts the
  // cluster's RaftEntityRegistry directly via the entityRegistry injection
  // slot — resource-typed records share Raft durability with the rest of
  // the cluster's entity state.
  const {
    resourceRegistry,
    topologyManager,
    moduleRegistry,
    warnings: registryWarnings,
  } = await buildAppLayerRegistries({
    nodeId,
    clusterMgr,
    mode: registryMode,
    registryWalFilePath,
    clusterEntityRegistry: registryMode === 'raft' ? cluster.registry : undefined,
  });

  // --- Bootstrap-level logger (separate from the per-module logger below) ---
  const bootstrapLog = withContext({ service: 'pipeline-bootstrap', nodeId });
  const quietInfo = fastTimers;
  const logger = {
    info:  quietInfo
      ? (_msg: string, _meta?: unknown) => { /* suppressed in test mode */ }
      : (msg: string, meta?: unknown) => bootstrapLog.info(meta ?? {}, msg),
    warn:  (msg: string, meta?: unknown) => bootstrapLog.warn(meta ?? {}, msg),
    error: (msg: string, meta?: unknown) => bootstrapLog.error(meta ?? {}, msg),
  };

  // Identity decision logging.
  if (persistentNodeId) {
    logger.info(`Stable identity loaded from ${identityFilePath} — nodeId=${nodeId}`);
  } else if (identityExplicitlyDisabled) {
    logger.warn(
      'Stable identity explicitly disabled via PIPELINE_IDENTITY_FILE=disabled — nodeId is ephemeral and will change on every restart',
    );
  }

  // WAL decision logging.
  if (walFilePath) {
    logger.info(`WAL enabled at ${walFilePath} — pipeline state will survive restart`);
  } else if (walExplicitlyDisabled) {
    logger.warn(
      'WAL explicitly disabled via PIPELINE_WAL_PATH=disabled — pipeline state is in-memory only and will be lost on restart',
    );
  } else {
    logger.warn('WAL disabled — pipeline state is in-memory only');
  }
  // Registry-mode decision logging + downgrade warnings produced by
  // buildAppLayerRegistries (notably the ResourceRegistry-can't-host-Raft
  // downgrade documented in config/registries.ts).
  if (registryMode === 'raft') {
    if (raftSigner) {
      logger.info(
        `Raft mode active (dataDir=${raftDataDir}); RPC payload signing ENABLED via per-node KeyManager`,
      );
    } else {
      logger.warn(
        `Raft mode active (dataDir=${raftDataDir}); RPC payload signing DISABLED — `
        + `set PIPELINE_RAFT_SIGNER_DIR to a writable directory to enable signed RPCs`,
      );
    }
  } else {
    logger.info(`Registry mode: ${registryMode}`);
  }
  for (const w of registryWarnings) {
    logger.warn(w);
  }
  if (registryWalFilePath) {
    logger.info(
      `ResourceRegistry WAL enabled at ${registryWalFilePath} — registry state will survive restart`,
    );
  } else {
    logger.info('ResourceRegistry running in-memory (test mode or no PIPELINE_REGISTRY_WAL_PATH)');
  }

  // Stream 4 (BusCompact): EventBus auto-compaction decision logging.
  // Auto-compaction is a no-op upstream when walFilePath is undefined
  // (compact() throws WalNotConfiguredError) — surface that to the operator
  // so a misconfigured `INTERVAL_MS without WAL` doesn't silently degrade.
  let effectiveAutoCompactIntervalMs: number | undefined = eventBusAutoCompactIntervalMs;
  let effectiveAutoCompactOptions: EventBusCompactionOptions | undefined = eventBusAutoCompactOptions;
  if (eventBusAutoCompactIntervalMs !== undefined && !walFilePath) {
    logger.info(
      'EventBus auto-compaction skipped: no WAL configured '
      + '(PIPELINE_EVENT_BUS_COMPACT_INTERVAL_MS requires PIPELINE_WAL_PATH)',
    );
    // Don't thread through — keep the EventBus config minimal so tests that
    // look at the underlying config don't see a vestigial interval setting.
    effectiveAutoCompactIntervalMs = undefined;
    effectiveAutoCompactOptions = undefined;
  } else if (eventBusAutoCompactIntervalMs !== undefined) {
    const keepN = eventBusAutoCompactOptions?.keepLastNPerType;
    logger.info(
      `EventBus auto-compaction every ${eventBusAutoCompactIntervalMs}ms `
      + `(keepLastN=${keepN ?? 'default(100)'})`,
    );
  }

  // --- PipelineModule construction ---
  const llmClient = opts.llmClient ?? createLLMClient();

  // EventBus dead-letter handler (Stream 3 — BusDLQ). On by default — the
  // reasonable failure mode in production is to ALWAYS record dead letters
  // so subscriber bugs and publish failures don't disappear silently. Set
  // PIPELINE_EVENT_BUS_DLQ_ENABLED=false to opt out.
  //
  // T9 (lib-expansion-3): when enabled, the handler ALSO writes a structured
  // entry into an InMemoryDeadLetterQueue<BusEvent>. The DLQ is constructed
  // below (post-module so its redriveSink can re-publish via the bus) and
  // assigned into `dlqRef.current` — the handler reads through the ref so
  // construction order doesn't matter.
  const dlqRaw = process.env.PIPELINE_EVENT_BUS_DLQ_ENABLED ?? 'true';
  const dlqEnabled = dlqRaw.trim().toLowerCase() !== 'false';
  const dlqRef: { current: DeadLetterQueue<BusEvent> | null } = { current: null };
  const dlqRepo = opts.dlqRepository;
  const eventBusDeadLetterHandler = dlqEnabled
    ? (event: BusEvent, error: Error): void => {
        incrementBusDeadLetter(error.name);
        const dlq = dlqRef.current;
        const lastError = `${error.name || 'Error'}: ${error.message}`;
        const failedAtMs = Date.now();
        if (dlq) {
          // Fire-and-forget — DLQ.put is async but the bus's handler signature
          // is sync void; rejection is logged so a broken DLQ doesn't silently
          // swallow dead letters.
          void dlq
            .put(busEventAsEnvelope(event), {
              // Standard `${name}: ${message}` shape so the DLQ filter
              // (failureKindMatches RegExp at the library level) and the
              // route-side `?errorKind=` query param can match the error
              // class prefix. Falls back to `Error:` if the throwable has
              // no `name` (defensive — Error subclasses always set one).
              lastError,
              failedAtMs,
              totalAttempts: 1,
            })
            .catch((err: unknown) => {
              bootstrapLog.error({ err: String(err) }, 'DLQ.put failed');
            });
        }
        // Also persist to DDB when dlqRepository is provided.
        if (dlqRepo) {
          const ttl = Math.floor(failedAtMs / 1000) + 30 * 24 * 60 * 60; // 30 days
          void dlqRepo.create({
            messageId: typeof event.id === 'string' ? event.id : `${event.type}-${failedAtMs}`,
            eventType: event.type,
            eventPayload: event.payload,
            lastError,
            failedAt: new Date(failedAtMs).toISOString(),
            totalAttempts: 1,
            ttl,
          }).catch((err: unknown) => {
            bootstrapLog.error({ err: String(err) }, 'DLQ persistence to DDB failed');
          });
        }
        // BusEvent (v0.7.2) does not carry a `topic` field — that lives on
        // the EventBus instance, not the envelope. Surface `type` (the
        // user-facing event name) and `sourceNodeId` (which node emitted it)
        // so operators can correlate dead letters with publishers.
        bootstrapLog.error({
          type: event.type,
          sourceNodeId: event.sourceNodeId,
          error: String(error),
        }, 'event bus dead letter');
      }
    : undefined;

  // Stream 5 (BusMetrics): thread the gateway's MetricsRegistry singleton into
  // the pipeline EventBus so bus-level activity (publish/subscribe/replay)
  // surfaces on /internal/metrics. Default ON — the EventBus counters are
  // cheap and useful by default. Set PIPELINE_EVENT_BUS_METRICS_ENABLED=false
  // to opt out.
  const eventBusMetricsEnabled =
    (process.env.PIPELINE_EVENT_BUS_METRICS_ENABLED ?? 'true').trim().toLowerCase() !== 'false';
  logger.info(
    eventBusMetricsEnabled ? 'EventBus metrics ENABLED' : 'EventBus metrics DISABLED',
  );
  // DC-21: register custom step handlers (llm:judge, image:vision-analyze, …).
  // Chicken-and-egg: handlers go INTO the module config, so the module ref
  // gets patched in AFTER construction via the `moduleRef.current` cell.
  // Each handler is constructed lazily here so the module map is empty when
  // no custom step is configured (e.g. unit tests). Always non-empty in
  // production — the judge handler is always registered, even when
  // ANTHROPIC_API_KEY is missing, so misconfiguration surfaces as a clean
  // dispatch-time failure rather than "unreachable pipeline node type".
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const judgeModuleRef: { current: any | null } = { current: null };
  const customStepHandlers = buildJudgeStepHandlers({
    moduleRef: judgeModuleRef,
    logger: bootstrapLog,
  });
  // Merge image:vision-analyze handler into the same map. Same moduleRef
  // — both handlers resolve runId→pipelineId through the same module
  // reference (patched below after `new PipelineModule(...)`).
  const visionHandlers = buildVisionStepHandlers({
    moduleRef: judgeModuleRef,
    logger: bootstrapLog,
  });
  for (const [key, handler] of visionHandlers) {
    customStepHandlers.set(key, handler);
  }
  // Memory node ('memory'). Same moduleRef again — 'long-term' scope resolves
  // runId→pipelineId through it, and a run whose pipelineId cannot be resolved
  // fails the step rather than silently degrading to session scope.
  const memoryHandlers = buildMemoryStepHandlers({ moduleRef: judgeModuleRef });
  for (const [key, handler] of memoryHandlers) {
    customStepHandlers.set(key, handler);
  }
  // Agentic node ('agent'): a real `claude -p` agent loop with tool use.
  // No moduleRef needed — the trace goes out as step output, not to a
  // side repository keyed by pipelineId. Registered via the custom-handler
  // seam rather than a DC NodeType so it ships without a distributed-core
  // dist/ rebuild + pipeline-runtime SHA repin; if DC later lands a
  // first-class `agent` NodeType, built-in dispatch wins and this line is
  // simply deleted.
  const agentHandlers = buildAgentStepHandlers({ logger: bootstrapLog });
  for (const [key, handler] of agentHandlers) {
    customStepHandlers.set(key, handler);
  }
  // The chat's /doc command: read → plan (light model) → apply as a collaborator.
  for (const [key, handler] of buildDocumentAgentHandlers()) {
    customStepHandlers.set(key, handler);
  }
  // The chat's /summarize and /diagram commands (read the conversation, draw the diagram, post the card).
  for (const [key, handler] of buildChatCommandHandlers()) {
    customStepHandlers.set(key, handler);
  }
  // After a call: finalize the recording and transcribe it — as runs, not inline code.
  // Retries are the executor's: every kind honours its node's `retryPolicy`
  // (distributed-core ≥ 0.45.8), so nothing is wrapped here.
  for (const [key, handler] of buildRecordingPipelineHandlers()) {
    customStepHandlers.set(key, handler);
  }
  // During a call: the live-captions tap as a run that lasts the call (arm → stream → disarm → publish).
  for (const [key, handler] of buildLiveCaptionsHandlers()) {
    customStepHandlers.set(key, handler);
  }
  // Seed the LIVE registry (src/pipeline/customStepHandlers.ts) with the
  // built-ins and hand THAT map to the module. DC keeps the map by reference
  // and reads it at every dispatch, so chat-command plugins (src/chat/
  // chatCommandPlugin.ts) that register step kinds before OR after this
  // point are dispatchable without the bootstrap knowing their names.
  for (const [key, handler] of customStepHandlers) registerCustomStepHandler(handler, key);
  const liveStepHandlers = getCustomStepHandlers();

  // Wave A (2026-08-25): action-API wiring. When set, action nodes
  // (post-comment / update-document / create-section-item / notify) make
  // REAL HTTP calls back into platform-api's own routes instead of
  // returning `{ok, simulated:true}` silently. Resolution:
  //   PIPELINE_ACTION_API_BASE_URL set        → use it ('' disables)
  //   unset, NODE_ENV=test                    → disabled (unit tests expect
  //                                             simulated actions, and no
  //                                             HTTP server is listening)
  //   unset otherwise                         → self (http://127.0.0.1:$PORT)
  const rawActionBase = process.env.PIPELINE_ACTION_API_BASE_URL;
  const actionApiBaseUrl = rawActionBase !== undefined
    ? (rawActionBase.trim() || undefined)
    : (process.env.NODE_ENV === 'test'
        ? undefined
        : `http://127.0.0.1:${process.env.PORT ?? '3001'}`);
  // The routes the executor targets sit behind requireAuth. In dev
  // (SKIP_AUTH) an unsigned-but-decodable JWT is enough to attribute the
  // write to the 'pipeline-bot' actor; in prod, operators supply a real
  // token via PIPELINE_ACTION_API_TOKEN.
  const actionApiToken = process.env.PIPELINE_ACTION_API_TOKEN
    ?? (actionApiBaseUrl
      ? jwtSign({ sub: 'pipeline-bot', email: 'pipeline-bot@platform' }, 'dev-local')
      : undefined);

  const module = new PipelineModule(
    buildPipelineModuleConfig({
      nodeId,
      walFilePath,
      llmClient,
      metricsRegistry,
      ...(eventBusDeadLetterHandler ? { eventBusDeadLetterHandler } : {}),
      eventBusAutoCompactIntervalMs: effectiveAutoCompactIntervalMs,
      eventBusAutoCompactOptions: effectiveAutoCompactOptions,
      ...(eventBusMetricsEnabled ? { eventBusMetrics: metricsRegistry } : {}),
      customStepHandlers: liveStepHandlers,
      ...(actionApiBaseUrl ? { actionApiBaseUrl } : {}),
      ...(actionApiBaseUrl && actionApiToken
        ? { actionApiHeaders: { Authorization: `Bearer ${actionApiToken}` } }
        : {}),
    }),
  );
  // Wire the module ref now that construction is complete — judge handler
  // can resolve `runId → pipelineId` for budget routing + persistence.
  judgeModuleRef.current = module;

  // Register the module with ApplicationRegistry. The registry auto-wires the
  // 6-field ApplicationModuleContext from its own internal state
  // (clusterManager, resourceRegistry, topologyManager, moduleRegistry,
  // configuration, logger), tracks module state in its dependency graph, and
  // emits `registry:module-registered` for any observers.
  //
  // The previous code carried a `fastTimers` bypass that called
  // `module.initialize() + module.start()` directly with a quiet context,
  // purely to suppress hardcoded '[INFO] [moduleId] …' chatter from
  // ApplicationRegistry.createModuleContext. distributed-core v0.7.2 fixed
  // that upstream (createModuleContext now uses a noop logger when
  // IS_TEST_ENV, which matches our test conditions exactly), so the bypass
  // and its ~40 LOC of context-building have been removed.
  await moduleRegistry.register(module, {
    configuration: {
      // CRITICAL: PipelineModule.onInitialize() reads
      // context.configuration.pubsub to construct its internal EventBus.
      pubsub,
      // Optional MetricsRegistry — `configuration` is `Record<string, any>`
      // so this is always shape-compatible.
      metrics: metricsRegistry,
    },
  });

  // T12 (lib-expansion-3): emit dc_queue_throughput_{enqueued,completed,failed}_total
  // for the run-queue, derived from the canonical pipeline:run:* events on the
  // module's EventBus. Cardinality discipline: queue label is the bounded enum
  // 'run-queue' (see observability/metrics.ts ALLOWED_QUEUE_NAMES) — never run
  // ids, tenant ids, or pipeline ids.
  const runQueueMetrics = getQueueMetrics('run-queue');
  const eventBus = module.getEventBus();

  // --- Live pipeline signal transport (EventBus → Redis → gateway) ---
  //
  // The three `publishDurable(...)` subscriptions further down are the DURABLE
  // path: a DynamoDB outbox row for external consumers, drained through the
  // message-queue transport. They are not, and never were, how the pipeline
  // canvas learns that a node started or finished.
  //
  // The gateway's `pipeline-relay.ts` pSUBSCRIBEs `dcore:topic:pipeline.events.*`
  // whenever no MESSAGE_QUEUE_URL / PIPELINE_EVENT_BUS_URL is configured. Until
  // this relay existed, nothing on the system published to that pattern, so a
  // run would trigger, execute and complete with every canvas node stuck at
  // `data-state="idle"` and not one `pipeline:event` frame on the wire.
  //
  // This forwards EVERY `pipeline:*` BusEvent — crucially the step-level ones
  // (`pipeline:step:started` / `:completed` / `:failed` / `:skipped`), which had
  // no egress of any kind. See event-redis-relay.ts for the wire contract.
  let pipelineEventRelay: PipelineEventRedisRelay | null = null;
  try {
    pipelineEventRelay = await startPipelineEventRedisRelay({ eventBus });
  } catch (err) {
    // startPipelineEventRedisRelay swallows its own failures; this is a belt-
    // and-braces guard so a relay problem can never abort pipeline bootstrap.
    bootstrapLog.error(
      { err: err instanceof Error ? err.message : String(err) },
      'pipeline event Redis relay failed to start',
    );
  }

  const runQueueSubIds: string[] = [];
  runQueueSubIds.push(eventBus.subscribe('pipeline:run:started', async () => {
    runQueueMetrics.recordEnqueued();
  }));
  runQueueSubIds.push(eventBus.subscribe('pipeline:run:completed', async () => {
    runQueueMetrics.recordCompleted();
  }));
  runQueueSubIds.push(eventBus.subscribe('pipeline:run:failed', async () => {
    runQueueMetrics.recordFailed();
  }));

  // Two separate subscriptions to `pipeline:run:completed` below are
  // INTENTIONAL and not a race — they serve completely different purposes:
  //
  //   1. (above, line ~725) Queue-metrics subscriber: records a counter tick
  //      for the 'run-queue' throughput metric. No external side-effects.
  //
  //   2. (below, line ~745) Durable-publish subscriber: fans the event out
  //      to event-catalog (`publishDurable('pipeline.run.completed', …)`) so
  //      external consumers can subscribe to the durable event stream.
  //
  // There is no dual-fire risk: `pipeline:run:completed` is emitted exactly
  // once per run by DC's PipelineExecutor (src in distributed-core:
  // PipelineExecutor.ts). The PipelineRaftStateMachine (defined locally in
  // src/pipeline/PipelineRaftStateMachine.ts) emits only `pipeline:raft:run:*`
  // namespaced events on its OWN internal emitter — it is NOT wired to this
  // EventBus and is not instantiated in the production bootstrap. No second
  // source for `pipeline:run:completed` exists.
  //
  // DECISION (2026-05-21): PipelineRaftStateMachine is intentionally not wired here.
  // It is a complete but dormant stub for future multi-node pipeline consensus.
  // Retained for its test coverage. Activate via MultiRaftCoordinator when needed.
  //
  // Publish pipeline lifecycle events through event-catalog for external consumers.
  // Wrapped with createValidatingPublisher (warn mode) — schema validation failures
  // log a warning but do NOT block the publish. Event names use dot-notation to
  // match the durable declarations in src/lib/event-catalog.ts (pipeline.run.*).
  const publishRunStarted = createValidatingPublisher(
    catalog.getRegistry(),
    'pipeline.run.started',
    (payload: Record<string, unknown>) => publishDurable('pipeline.run.started', payload),
    { mode: 'warn' },
  );
  const publishRunCompleted = createValidatingPublisher(
    catalog.getRegistry(),
    'pipeline.run.completed',
    (payload: Record<string, unknown>) => publishDurable('pipeline.run.completed', payload),
    { mode: 'warn' },
  );
  const publishRunFailed = createValidatingPublisher(
    catalog.getRegistry(),
    'pipeline.run.failed',
    (payload: Record<string, unknown>) => publishDurable('pipeline.run.failed', payload),
    { mode: 'warn' },
  );

  // EventBus.subscribe delivers a BusEvent envelope: { id, type, payload, ... }
  // The actual emitted object lives at `evt.payload`. Reading top-level fields
  // (`data.runId`) gives undefined and trips schema validation downstream.
  eventBus.subscribe('pipeline:run:started', async (evt: unknown) => {
    const data = ((evt as { payload?: Record<string, unknown> })?.payload ?? {}) as Record<string, unknown>;
    publishRunStarted({
      runId: data.runId as string,
      pipelineId: data.pipelineId as string,
      // triggeredBy is a PipelineRunTrigger object from DC, not a string.
      triggeredBy: data.triggeredBy as Record<string, unknown> | undefined,
      startedAt: data.at as string || new Date().toISOString(),
    }).catch((err: unknown) => {
      log.error({ event: 'pipeline.run.started', err: err instanceof Error ? err.message : String(err) }, 'durable publish failed');
    });
  });

  eventBus.subscribe('pipeline:run:completed', async (evt: unknown) => {
    const data = ((evt as { payload?: Record<string, unknown> })?.payload ?? {}) as Record<string, unknown>;
    // M4 SLO trigger gate clause-2 wiring. See .agent/SLO-M4-TRIGGER.md.
    const durationMs = data.durationMs;
    if (typeof durationMs === 'number' && Number.isFinite(durationMs) && durationMs > 0) {
      recordPipelineExecutorBusy(durationMs / 1000);
    }
    publishRunCompleted({
      runId: data.runId as string,
      durationMs: data.durationMs as number | undefined,
      completedAt: data.at as string || new Date().toISOString(),
      // A run whose approval gate was rejected arrives on this event too —
      // carry the terminal status and the decision so subscribers don't have
      // to treat every `run.completed` as a success.
      ...(data.status !== undefined ? { status: data.status } : {}),
      ...(data.rejection !== undefined ? { rejection: data.rejection } : {}),
    }).catch((err: unknown) => {
      log.error({ event: 'pipeline.run.completed', err: err instanceof Error ? err.message : String(err) }, 'durable publish failed');
    });
  });

  eventBus.subscribe('pipeline:run:failed', async (evt: unknown) => {
    const data = ((evt as { payload?: Record<string, unknown> })?.payload ?? {}) as Record<string, unknown>;
    publishRunFailed({
      runId: data.runId as string,
      // error is a PipelineRunError struct from DC, not a string.
      error: data.error as Record<string, unknown> | undefined,
      failedAt: data.at as string || new Date().toISOString(),
    }).catch((err: unknown) => {
      log.error({ event: 'pipeline.run.failed', err: err instanceof Error ? err.message : String(err) }, 'durable publish failed');
    });
  });

  // Pipeline run persistence: when pipelineRunRepository is provided, use
  // RunPersistenceService to subscribe to pipeline events and persist full
  // run state (including per-step input/output) to DDB. Fire-and-forget —
  // persistence failures are logged but do not block execution.
  let runPersistence: RunPersistenceService | null = null;
  let stuckRunDetector: StuckRunDetector | null = null;
  if (opts.pipelineRunRepository) {
    runPersistence = new RunPersistenceService({
      eventBus,
      repository: opts.pipelineRunRepository,
      ttlSeconds: 90 * 24 * 60 * 60, // 90 days
      maxStepOutputBytes: 100 * 1024, // 100KB per step
    });
    runPersistence.start();
    // Expose to non-bootstrap modules (e.g. trigger route) so they can
    // pre-populate the run cache before the framework fires events.
    setRunPersistenceService(runPersistence);

    // Hub#1285: detect runs that have gone silent past the stuck threshold.
    stuckRunDetector = new StuckRunDetector({
      runPersistence,
      repository: opts.pipelineRunRepository,
    });
    stuckRunDetector.start();

    // Hub#1282 (ADR D2): single-node restart recovery. Find runs left in
    // 'running' status by a previous process crash and transition them to
    // 'interrupted' so the operator UI can surface a "Resume" button. The
    // scan is bounded by PIPELINE_RESUME_WINDOW_MS (default 4h) so old
    // already-known-broken rows aren't silently stomped on every restart.
    const resumeWindowEnv = process.env.PIPELINE_RESUME_WINDOW_MS;
    const resumeWindowMs = resumeWindowEnv ? Number(resumeWindowEnv) : undefined;
    if (resumeWindowMs !== undefined && (!Number.isFinite(resumeWindowMs) || resumeWindowMs < 0)) {
      logger.warn(
        `Invalid PIPELINE_RESUME_WINDOW_MS="${resumeWindowEnv}" — falling back to default 4h window`,
      );
    }
    // DC-25: after the orphan-scan marks dead-pod runs `interrupted`,
    // resume the recoverable ones (checkpoint + definition present) on THIS
    // process, under their original runIds. Behind PIPELINE_RESUME_ON_BOOT
    // (default ON) so operators can fall back to scan-only marking.
    const resumeOnBoot =
      (process.env.PIPELINE_RESUME_ON_BOOT ?? 'true').trim().toLowerCase() !== 'false';
    const pipelineRunRepository = opts.pipelineRunRepository;
    const runPersistenceRef = runPersistence;
    void scanOrphanedRuns({
      repository: pipelineRunRepository,
      ...(Number.isFinite(resumeWindowMs) && (resumeWindowMs ?? 0) >= 0
        ? { resumeWindowMs: resumeWindowMs as number }
        : {}),
    })
      .then(async (scan) => {
        if (!resumeOnBoot) {
          if (scan.transitioned > 0) {
            logger.info(
              `resume-on-boot DISABLED (PIPELINE_RESUME_ON_BOOT=false) — ${scan.transitioned} interrupted run(s) left for manual resume`,
            );
          }
          return;
        }
        const resumeResult = await resumeInterruptedRuns({
          repository: pipelineRunRepository,
          module,
          runPersistence: runPersistenceRef,
          definitionsRepo,
          nodeId,
          ...(Number.isFinite(resumeWindowMs) && (resumeWindowMs ?? 0) >= 0
            ? { resumeWindowMs: resumeWindowMs as number }
            : {}),
        });
        if (resumeResult.scanned > 0) {
          logger.info(
            `boot resume: ${resumeResult.resumed} resumed / ${resumeResult.scanned} interrupted ` +
              `(noCheckpoint=${resumeResult.skippedNoCheckpoint}, noDefinition=${resumeResult.skippedNoDefinition}, ` +
              `outsideWindow=${resumeResult.skippedOutsideWindow}, claimLost=${resumeResult.claimLost}, failed=${resumeResult.failed})`,
          );
        }
      })
      .catch((err) => {
        bootstrapLog.warn({ err: String(err) }, 'orphan-scan/boot-resume failed at bootstrap');
      });
  }

  // T9 (lib-expansion-3): construct the bus-event DLQ and assign into the
  // dlqRef captured by the handler above. RedriveSink re-publishes via the
  // EventBus — this is the documented "operator fixed the bug, replay the
  // failed event" semantics. When DLQ recording is disabled the ref stays
  // null and the handler short-circuits.
  let dlq: DeadLetterQueue<BusEvent> | null = null;
  if (dlqEnabled) {
    dlq = new InMemoryDeadLetterQueue<BusEvent>({
      redriveSink: {
        enqueue: async (env) => {
          const evt = env.body;
          // A failed RUN is redriven by running it again, not by re-publishing
          // its failure. See failedRunDeadLetters.ts.
          if (isRunRetrigger(evt)) {
            await retriggerFromDeadLetter(evt.payload as unknown as RunRetriggerPayload, {
              resolveDefinition: async (pipelineId, userId) =>
                getSystemPipeline(pipelineId)
                ?? ((userId ? await definitionsRepo.get(userId, pipelineId) : null) as Record<string, unknown> | null),
              trigger: async (input) => {
                const bridge = getPipelineBridge();
                if (!bridge?.trigger) throw new Error('pipeline engine is not available');
                const out = await Promise.resolve(bridge.trigger(input as never));
                return { runId: (out as { runId: string }).runId };
              },
              // The other direction of the link: the new run carries
              // `redriveOf`; the original run gets `redrivenBy`.
              ...(opts.pipelineRunRepository
                ? {
                    linkRedrive: async ({ pipelineId, failedRunId, newRunId }: { pipelineId: string; failedRunId: string; newRunId: string }) => {
                      await opts.pipelineRunRepository!.appendRedrivenBy(pipelineId, failedRunId, newRunId);
                    },
                  }
                : {}),
            });
          } else {
            await eventBus.publish(evt.type as never, evt.payload as never);
          }
          // Redriven: the in-memory queue drops it next; drop the durable copy too.
          await forgetDeadLetters(dlqRepo ?? null, [env.id]);
        },
      },
    });
    dlqRef.current = dlq;
    // Every run that ends `failed` becomes a dead letter of its own — the
    // operator's one list of things to look at and redrive.
    attachFailedRunDeadLetters({
      eventBus: eventBus as never,
      dlq,
      ...(dlqRepo ? { persist: (entry) => dlqRepo.create(entry) } : {}),
    });
    // The store is the restart-survival copy of the queue: read it back now,
    // and let the DLQ route forget what an operator purges.
    setDeadLetterStore(dlqRepo ?? null);
    if (dlqRepo) {
      const inMemoryDlq = dlq;
      void rehydrateDeadLetters(inMemoryDlq, dlqRepo).catch((err: unknown) => {
        bootstrapLog.error({ err: err instanceof Error ? err.message : String(err) }, 'could not rehydrate dead letters from the store');
      });
    }
  }

  // --- hub#2224 Wave 2: executor audit subscriber + sweeper ---
  //
  // Subscribes to PipelineModule's EventBus and persists step/run lifecycle
  // events into the pipeline-audit table so operators can answer "what
  // happened to run X 6h ago" without trawling Pino logs. The route handlers
  // already write audit rows for operator-driven mutations (trigger, cancel,
  // approve); this fills in the executor-driven complement.
  //
  // Opt-out via PIPELINE_AUDIT_LIFECYCLE_ENABLED=false.
  const auditLifecycleEnabled =
    (process.env.PIPELINE_AUDIT_LIFECYCLE_ENABLED ?? 'true').trim().toLowerCase() !== 'false';
  let executorAuditSubscriber: ExecutorAuditSubscriber | null = null;
  if (auditLifecycleEnabled) {
    const checkpointSampleRate = (() => {
      const raw = process.env.PIPELINE_AUDIT_CHECKPOINT_SAMPLE_RATE;
      if (!raw) return undefined;
      const n = Number(raw);
      return Number.isFinite(n) && n > 0 ? n : undefined;
    })();
    executorAuditSubscriber = new ExecutorAuditSubscriber({
      eventBus,
      auditRepo,
      ...(checkpointSampleRate !== undefined ? { checkpointSampleRate } : {}),
    });
    executorAuditSubscriber.start();
    logger.info('executor audit subscriber ENABLED');
  } else {
    logger.info('executor audit subscriber DISABLED via PIPELINE_AUDIT_LIFECYCLE_ENABLED=false');
  }

  // ── Judge aggregator subscriber ───────────────────────────────────────
  //
  // v2 — folds `pipeline:judge:scored` into per-pipeline/per-rubric/per-day
  // rollups (pipeline-judgment-aggregates table) for trend rendering. In-
  // process subscriber on the same EventBus the audit subscriber listens
  // to. Opt-out via JUDGE_AGGREGATOR_ENABLED=false (for envs where the
  // aggregates table isn't provisioned yet).
  const judgeAggregatorEnabled =
    (process.env.JUDGE_AGGREGATOR_ENABLED ?? 'true').trim().toLowerCase() !== 'false';
  let judgeAggregatorSubscriber: JudgeAggregatorSubscriber | null = null;
  if (judgeAggregatorEnabled) {
    judgeAggregatorSubscriber = new JudgeAggregatorSubscriber({
      eventBus,
      aggregateRepo: getJudgmentAggregateRepo(),
    });
    judgeAggregatorSubscriber.start();
    logger.info('judge aggregator subscriber ENABLED');
  } else {
    logger.info('judge aggregator subscriber DISABLED via JUDGE_AGGREGATOR_ENABLED=false');
  }

  // Belt-and-suspenders sweeper for envs where DDB native TTL doesn't fire
  // (DDB-local in particular). Production AWS DDB drops rows via the
  // `ttlEpoch` attribute set at write time, so the sweeper is a no-op there
  // — but local Tilt-driven dev clusters need it. Default off in test, on
  // elsewhere; tunable via env.
  const auditSweeperEnabled = (() => {
    const raw = process.env.PIPELINE_AUDIT_SWEEPER_ENABLED;
    if (raw !== undefined) return raw.trim().toLowerCase() !== 'false';
    return !IS_TEST_ENV;
  })();
  let auditSweeperStop: (() => void) | null = null;
  if (auditSweeperEnabled) {
    const intervalMs = (() => {
      const raw = process.env.PIPELINE_AUDIT_SWEEPER_INTERVAL_MS;
      if (!raw) return 60 * 60 * 1000; // 1h default
      const n = Number(raw);
      return Number.isFinite(n) && n > 0 ? n : 60 * 60 * 1000;
    })();
    const maxItemsPerRun = (() => {
      const raw = process.env.PIPELINE_AUDIT_SWEEPER_MAX_ITEMS;
      if (!raw) return 1000;
      const n = Number(raw);
      return Number.isFinite(n) && n > 0 ? n : 1000;
    })();
    const handle: NodeJS.Timeout = setInterval(() => {
      auditRepo
        .sweepExpired(maxItemsPerRun)
        .then((deleted) => {
          if (deleted > 0) {
            bootstrapLog.info({ deleted }, 'pipeline-audit sweep completed');
          }
        })
        .catch((err: unknown) => {
          bootstrapLog.warn(
            { err: err instanceof Error ? err.message : String(err) },
            'pipeline-audit sweep failed',
          );
        });
    }, intervalMs);
    handle.unref?.();
    auditSweeperStop = () => clearInterval(handle);
    logger.info(`pipeline-audit sweeper ENABLED (interval=${intervalMs}ms, maxItems=${maxItemsPerRun})`);
  }

  // --- Coordinated shutdown ---
  const shutdown = createShutdown({
    keepAlive,
    runQueueSubIds,
    eventBus,
    runPersistence,
    stuckRunDetector,
    executorAuditSubscriber,
    judgeAggregatorSubscriber,
    auditSweeperStop,
    pipelineEventRelay,
    moduleRegistry,
    resourceRegistry,
    hintedHandoff,
    fdBridge,
    cluster,
    logger: bootstrapLog,
  });

  return { module, nodeId, dlq, shutdown };
}
