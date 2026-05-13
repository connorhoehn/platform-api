// platform-api/src/pipeline/config/pipelineModule.ts
//
// Pure builder for `PipelineModuleConfig`. distributed-core v0.7.2's
// `PipelineModule.onInitialize()` threads the EventBus passthrough fields
// (deadLetterHandler, metrics, walSyncIntervalMs, autoCompactIntervalMs,
// autoCompactOptions) from `PipelineModuleConfig` into the constructed
// EventBus, so the gateway can wire bus-level behavior end-to-end without
// reaching into private internals.
//
// This file is the seam where each Wave-2 stream extends the constructor
// config:
//   - Stream 3 (BusDLQ)     → eventBusDeadLetterHandler
//   - Stream 4 (BusCompact) → eventBusAutoCompactIntervalMs / Options
//   - Stream 5 (BusMetrics) → eventBusMetrics
//
// The run-level metrics surface (`PipelineModuleConfig.metrics`) continues
// to produce pipeline.run.{started,completed,failed,cancelled} counters and
// is independent of the bus-level metrics field.

import type {
  LLMClient,
  MetricsRegistry,
  BusEvent,
  EventBusCompactionOptions,
} from 'distributed-core';
import { InMemoryDedupStore } from 'distributed-core';

/**
 * Inlined replacement for distributed-core's previously-exported
 * `OnBeforeActionHook` type — the type was dropped from the public
 * surface in v0.15-v0.17 narrowing. The shape matches what
 * PipelineModuleConstructorConfig.onBeforeAction still expects
 * structurally; if upstream changes the signature, this declaration
 * needs to follow.
 */
type OnBeforeActionHook = (ctx: { idempotencyKey: string }) => Promise<'proceed' | 'skip'>;

// PipelineModule's config interface lives in the applications/pipeline
// barrel; the gateway reaches it through the top-level `distributed-core`
// re-export. We intentionally re-derive the relevant subset here rather than
// importing the type so this file fails-fast if the upstream surface drifts.
export interface PipelineModuleConstructorConfig {
  moduleId: string;
  moduleName: string;
  version: string;
  resourceTypes: string[];
  configuration: Record<string, unknown>;
  llmClient: LLMClient;
  /** When set, enables WAL-backed pipeline EventBus durability. */
  walFilePath?: string;
  /** Run-level metrics: pipeline.run.{started,completed,failed,cancelled}. */
  metrics?: MetricsRegistry;
  /**
   * Stream 3 (BusDLQ): EventBus dead-letter handler — invoked when a
   * subscriber throws or a publish path fails. Threaded into
   * `EventBusConfig.deadLetterHandler` by `PipelineModule.onInitialize()`
   * (v0.7.2+). Default: errors are swallowed silently inside the bus.
   */
  eventBusDeadLetterHandler?: (event: BusEvent, error: Error) => void;
  /**
   * Stream 4 (BusCompact): when set AND `walFilePath` is configured, the
   * EventBus runs `compact()` on this interval to bound WAL growth. When
   * unset (default), no auto-compaction runs and the WAL is bounded only
   * by manual `compact()` calls. Auto-compaction is a no-op when
   * `walFilePath` is undefined.
   */
  eventBusAutoCompactIntervalMs?: number;
  /**
   * Stream 4 (BusCompact): options applied at each auto-compaction tick.
   * Default upstream: `{ keepLastNPerType: 100 }`.
   */
  eventBusAutoCompactOptions?: EventBusCompactionOptions;
  /**
   * Stream 5 (BusMetrics): bus-level metrics threaded into the underlying
   * EventBus's `EventBusConfig.metrics`. When set, EventBus emits
   * publish/subscribe/replay/compaction counters into the supplied
   * registry. Distinct from the run-level `metrics` field above; both can
   * be set independently. (distributed-core v0.7.2+.)
   */
  eventBusMetrics?: MetricsRegistry;
  /** T2 idempotency hook — see distributed-core `PipelineModuleConfig.onBeforeAction`. */
  onBeforeAction?: OnBeforeActionHook;
}

export interface BuildPipelineModuleConfigArgs {
  nodeId: string;
  walFilePath: string | undefined;
  llmClient: LLMClient;
  metricsRegistry: MetricsRegistry;
  /**
   * Stream 3 (BusDLQ): optional dead-letter handler. When defined,
   * threaded through to the underlying EventBus so subscriber-throws and
   * publish-failures are recorded instead of swallowed. Wired by
   * `bootstrap.ts` from the `PIPELINE_EVENT_BUS_DLQ_ENABLED` env var.
   */
  eventBusDeadLetterHandler?: (event: BusEvent, error: Error) => void;
  /** Stream 4 (BusCompact): see `PipelineModuleConstructorConfig`. */
  eventBusAutoCompactIntervalMs?: number;
  /** Stream 4 (BusCompact): see `PipelineModuleConstructorConfig`. */
  eventBusAutoCompactOptions?: EventBusCompactionOptions;
  /**
   * Stream 5 (BusMetrics): optional MetricsRegistry forwarded into the
   * pipeline EventBus for bus-level counters. When omitted (env-disabled),
   * no bus-level metrics are emitted. The same registry singleton can be
   * passed as `metricsRegistry` for single-pane-of-glass observability.
   */
  eventBusMetrics?: MetricsRegistry;
}

/**
 * Build the constructor argument for `new PipelineModule(...)`. Pure; no env
 * reads. Tests pass a `FixtureLLMClient` via `llmClient`.
 *
 * The `moduleId` is suffixed with `nodeId` so two bootstraps in the same
 * process get distinct identities (matches the same convention used for the
 * cluster topic in `config/cluster.ts`).
 */
// 24-hour dedup window — long enough to cover all retry backoffs for any run.
// Per-step keys live in the window even across process restarts once Redis is
// wired; the in-memory store resets on restart, which is acceptable for now
// (worst case: a retry fires a second side-effect after a crash-restart).
const ACTION_DEDUP_WINDOW_MS = 24 * 60 * 60 * 1000;

// Module-level singleton so the same store survives across multiple bootstrap
// calls in the same process (test helpers can reconstruct the whole bootstrap
// multiple times). In production there is exactly one bootstrap per process.
let actionDedupStore: InMemoryDedupStore | null = null;

function getActionDedupStore(): InMemoryDedupStore {
  if (!actionDedupStore) {
    actionDedupStore = new InMemoryDedupStore();
  }
  return actionDedupStore;
}

export const onBeforeActionHook: OnBeforeActionHook = async (ctx) => {
  const store = getActionDedupStore();
  const result = store.putIfAbsent(ctx.idempotencyKey, ctx.idempotencyKey, ACTION_DEDUP_WINDOW_MS);
  return result.reserved ? 'proceed' : 'skip';
};

export function buildPipelineModuleConfig(
  args: BuildPipelineModuleConfigArgs,
): PipelineModuleConstructorConfig {
  const config: PipelineModuleConstructorConfig = {
    moduleId:      `pipeline-${args.nodeId}`,
    moduleName:    'Pipeline',
    version:       '1.0.0',
    resourceTypes: ['pipeline-run'],
    configuration: {},
    llmClient:     args.llmClient,
    metrics:       args.metricsRegistry,
    onBeforeAction: onBeforeActionHook,
  };
  if (args.walFilePath) {
    config.walFilePath = args.walFilePath;
  }
  // Each optional eventBus* field is spread only when defined so we don't
  // override upstream defaults with explicit `undefined` values.
  if (args.eventBusDeadLetterHandler) {
    config.eventBusDeadLetterHandler = args.eventBusDeadLetterHandler;
  }
  if (args.eventBusAutoCompactIntervalMs !== undefined) {
    config.eventBusAutoCompactIntervalMs = args.eventBusAutoCompactIntervalMs;
  }
  if (args.eventBusAutoCompactOptions !== undefined) {
    config.eventBusAutoCompactOptions = args.eventBusAutoCompactOptions;
  }
  if (args.eventBusMetrics !== undefined) {
    config.eventBusMetrics = args.eventBusMetrics;
  }
  return config;
}
