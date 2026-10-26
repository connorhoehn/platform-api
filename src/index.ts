// IMPORTANT: tracing.start() must run before any other imports so that
// OpenTelemetry auto-instrumentation can monkey-patch http/express/etc.
// before they are require()'d by the rest of the application.
import { start as startTracing } from './tracing';
const tracingShutdown = startTracing();

import { createApp } from './app';
import { createScheduleEvaluator } from './services/scheduleEvaluator';
import { createFencedSchedulerLeadership, createLeaseRedisGetter } from './services/scheduler-fenced-leadership';
import { sharedRedisUrl } from './lib/redis-client';
import { pipelineDefinitionsCache } from './pipeline/definitions-cache';
import { startInvalidationSubscriber } from './pipeline/definitions-invalidator';
import { startMembershipInvalidationSubscriber } from './work-graph/membershipProjection';
import { wirePipelineBridge } from '@connorhoehn/pipeline-runtime';
import { setPipelineBridge, getPipelineBridge } from './pipeline/bridge-types';
import { bootstrapPipeline } from './pipeline/bootstrap';
import { chatCommandPluginSpecsFromEnv, createChatCommandPluginContext, loadChatCommandPlugins } from './chat/chatCommandPlugin';
import { createBridge } from './pipeline/createBridge';
import { PipelineRunRepository } from './repositories/PipelineRunRepository';
import { DLQRepository } from './repositories/DLQRepository';
import { ddbClient, docClient } from './lib/aws-clients';
import { ensureWorkGraphTable } from './lib/work-graph-dynamo';
import log from './lib/logger';
import { startEventLoopMetrics } from './observability/eventLoop';
import { assertRequiredInProd } from 'distributed-core';
import { createShutdownManager, drainHttpServer } from '@connorhoehn/service-runtime';
import { RetryManager } from 'distributed-core/transport';
import { catalog, connectCatalog, disconnectCatalog, publishDurable } from './lib/event-catalog';
import { runtimeProfile, runtimeReadiness, validateProfile } from './lib/runtime-profile';
import { ensureOutboxTTL, ensureVideoSessionsTTL, ensurePipelineRunsTTL, ensurePipelineAuditTTL, ensurePipelineDlqTTL } from './lib/dynamo-setup';
import { resolveVideoSessionStoreMode } from './repositories/videoSessionStoreFactory';
import { startOutboxDrainer } from './services/outbox-drainer';
import { whenBuildFresh } from './lib/buildFreshness';
import { startActivityConsumer } from './services/activity-consumer';
import { startSummaryProjector } from './services/summary-projector';
import { startCallReactionConsumer } from './services/call-reaction-consumer';
import { startWorkGraphConsumer } from './services/work-graph-consumer';
import { startWorkGraphOutboxDrainer } from './services/work-graph-outbox-drainer';
import { WorkSourceAccountRepository } from './repositories/WorkSourceAccountRepository';
import { workGraphRepository } from './work-graph/runtime';
import { createPipelineConfigRegistration } from './control-plane/pipeline-config-subscriber';
import { createFileUploadEndpointRegistration } from './control-plane/file-upload-subscriber';
import { ControlPlaneSubscriberRegistry } from './control-plane/registry';
import { definitionsRepo } from './pipeline/definitions-repository';

const port = process.env.PORT ?? '3001';

// Runtime profile: validate static capability declarations before opening
// connections, timers or consumers. A durable profile that cannot honour its
// guarantees refuses to start rather than silently degrading.
{
  const report = validateProfile(runtimeProfile);
  if (!report.ok) {
    log.fatal({ profile: runtimeProfile.profile, problems: report.problems }, 'runtime profile requirements failed');
    process.exit(2);
  }
  log.info({
    profile: runtimeProfile.profile,
    datastore: runtimeProfile.datastore ?? 'undeclared',
    mandatory: [...runtimeProfile.mandatory],
  }, 'runtime profile');
}

// Cognito env-var gate. In prod (or COGNITO_REQUIRED=true) exit(1) when the
// required vars are missing; in dev COGNITO_REQUIRED=false explicitly allows
// boot to continue (e.g. when running with SKIP_AUTH=true). Matches the
// previous hard fast-fail behaviour for the default (prod) case.
void assertRequiredInProd(
  {
    name: 'cognito',
    ping: async () => {
      const missing: string[] = [];
      if (!process.env.COGNITO_REGION) missing.push('COGNITO_REGION');
      if (!process.env.COGNITO_USER_POOL_ID) missing.push('COGNITO_USER_POOL_ID');
      if (missing.length > 0) {
        throw new Error(`Cognito env vars missing: ${missing.join(', ')}`);
      }
    },
  },
  {
    requiredEnvVar: 'COGNITO_REQUIRED',
    logger: log,
  },
);

// Feature-flagged at startup: the kill switch prevents any new work-graph
// delivery while preserving private projection/history rows for later resume.
if (process.env.ACTIVE_NOW_WORK_GRAPH_ENABLED === 'true') {
  void assertRequiredInProd(
    { name: 'ddb-work-graph', ping: () => ensureWorkGraphTable(ddbClient) },
    { requiredEnvVar: 'WORK_GRAPH_DDB_REQUIRED', logger: log },
  );
}

// P0 — video/recording secret gate. Without these, the LVS webhook
// receiver silently accepts unsigned payloads (HMAC verify skipped) and
// the LVS admin route defaults to literal "admin". Hard-fail prod boot
// to make the misconfiguration loud at deploy time, not at the first
// security incident. SKIP_AUTH is also rejected in prod (C7) so dev
// shortcuts can never accidentally ship.
void assertRequiredInProd(
  {
    name: 'video-secrets',
    ping: async () => {
      const missing: string[] = [];
      if (!process.env.LVS_WEBHOOK_SECRET) missing.push('LVS_WEBHOOK_SECRET');
      if (!process.env.VIDEO_ADMIN_TOKEN) missing.push('VIDEO_ADMIN_TOKEN');
      // Skip the SKIP_AUTH=true escape hatch in prod — it makes every
      // request authenticate as decoded-but-unverified-JWT, breaking
      // every authorization invariant downstream.
      if (process.env.SKIP_AUTH === 'true') {
        throw new Error('SKIP_AUTH=true is not allowed in production');
      }
      if (missing.length > 0) {
        throw new Error(`video/recording env vars missing: ${missing.join(', ')}`);
      }
    },
  },
  {
    requiredEnvVar: 'VIDEO_SECRETS_REQUIRED',
    logger: log,
  },
);

const app = createApp();
// realtime-examples NFR #202: event-loop delay + utilization on /internal/metrics.
startEventLoopMetrics();

const server = app.listen(Number(port), () => {
  log.info({ port }, 'platform-api listening');
  // Register the LVS recording webhook subscription on boot so
  // recording.completed events flow into /hooks/lvs/recording → DDB.
  // Idempotent + non-fatal — failures log and the recording pipeline
  // simply won't persist until the next platform-api boot succeeds.
  void import('./lib/lvs-webhook-bootstrap.js').then(({ bootstrapLvsWebhook, startLvsWebhookReconciler }) => {
    // One-shot bootstrap on boot.
    void bootstrapLvsWebhook();
    // Periodic reconciler — LVS forgets subscriptions on restart since they
    // live in-memory only. Without this, a SFU bounce silently breaks the
    // recordings pipeline until platform-api is also restarted, which is
    // the most common root cause of "Past Calls is empty" reports in dev
    // (and the most-likely cause in prod if LVS pods rotate independently).
    startLvsWebhookReconciler();
  }).catch((e: any) => log.warn({ err: e?.message ?? e }, 'lvs-webhook-bootstrap failed'));
});

// ---------------------------------------------------------------------------
// Event catalog — connects to Redis + message-queue so domain events can be
// published to the catalog. The process keeps serving if the connection
// fails; readiness reports the failure (503 when the runtime profile makes
// the catalog mandatory, degraded otherwise) until a reconnect succeeds.
// The outbox drainer starts unconditionally — it drains from DynamoDB which is
// independent of the catalog connection.
// ---------------------------------------------------------------------------

// NFR #65: the drainer lease waits until this process runs the host's source
// (dev only; `PLATFORM_HOST_SRC_DIR` unset means fresh at once).
let drainer: { stop: () => void } | null = null;
let drainerStopped = false;
void whenBuildFresh(log).then(() => {
  if (drainerStopped) return;
  try {
    drainer = startOutboxDrainer();
    runtimeReadiness.ready('outbox');
    log.info('outbox-drainer started');
  } catch (err: unknown) {
    runtimeReadiness.fail('outbox', err);
    log.error({ errMessage: err instanceof Error ? err.message : String(err) }, 'outbox-drainer failed to start');
  }
});

// DDB TTL provisioning. In prod (or DDB_TTL_REQUIRED=true) we refuse to boot
// if TTL can't be configured on the outbox / video-sessions tables — silent
// degradation here means stale rows accumulate forever. Dev still gets a
// warn-and-continue so a missing table doesn't block local work.
void assertRequiredInProd(
  {
    name: 'ddb-outbox-ttl',
    ping: () => ensureOutboxTTL(),
  },
  {
    requiredEnvVar: 'DDB_TTL_REQUIRED',
    logger: log,
  },
);

// Video sessions TTL is only meaningful when we're on the DDB store; the
// in-memory store enforces TTL itself. Skipping the DDB describe call when
// the operator has explicitly flipped to memory mode avoids a noisy warn
// in environments that don't even have the table provisioned.
const videoStoreMode = resolveVideoSessionStoreMode();
log.info({ mode: videoStoreMode }, 'video-session store mode');
if (videoStoreMode === 'ddb') {
  void assertRequiredInProd(
    {
      name: 'ddb-video-sessions-ttl',
      ping: () => ensureVideoSessionsTTL(),
    },
    {
      requiredEnvVar: 'DDB_TTL_REQUIRED',
      logger: log,
    },
  );
}

// Pipeline runs. Same reasoning as the outbox above, and the one that actually
// bit: ~1500 fixture runs accumulated because neither half of expiry existed.
// Only terminal runs carry a ttl, so this never expires a run that is still
// pending, running, or interrupted and waiting to be resumed at boot.
void assertRequiredInProd(
  {
    name: 'ddb-pipeline-runs-ttl',
    ping: () => ensurePipelineRunsTTL(),
  },
  {
    requiredEnvVar: 'DDB_TTL_REQUIRED',
    logger: log,
  },
);

// Pipeline audit trail (`ttlEpoch`) and dead letters (`ttl`): both stamped
// +30 d at write, neither table had TTL on (realtime-examples NFR #226).
void assertRequiredInProd(
  {
    name: 'ddb-pipeline-audit-ttl',
    ping: () => ensurePipelineAuditTTL(),
  },
  {
    requiredEnvVar: 'DDB_TTL_REQUIRED',
    logger: log,
  },
);
void assertRequiredInProd(
  {
    name: 'ddb-pipeline-dlq-ttl',
    ping: () => ensurePipelineDlqTTL(),
  },
  {
    requiredEnvVar: 'DDB_TTL_REQUIRED',
    logger: log,
  },
);

const catalogRetry = new RetryManager({
  maxRetries: 2,
  baseDelay: 2000,
  backoffFactor: 2,
  jitter: false,
  enableLogging: false,
  name: 'catalog-connect',
});

let activityConsumer: { stop: () => Promise<void> } | null = null;
let summaryProjector: { stop: () => Promise<void> } | null = null;
let callReactionConsumer: { stop: () => Promise<void> } | null = null;
let workGraphConsumer: { stop: () => Promise<void> } | null = null;
let workGraphOutboxDrainer: { stop: () => void } | null = null;

let catalogConsumersStarted = false;
// Runs once, after the first successful catalog connection, whichever path
// (initial retries or background reconnect) achieved it.
async function startCatalogConsumers(): Promise<void> {
  if (catalogConsumersStarted) return;
  catalogConsumersStarted = true;
  runtimeReadiness.ready('event-catalog');
  const consumerFailures: string[] = [];
  if (process.env.ACTIVE_NOW_WORK_GRAPH_ENABLED === 'true') {
    try {
      // Do not consume before the projection table and all lookup indexes
      // exist. A failed source dependency leaves the feature fail-closed.
      await ensureWorkGraphTable(ddbClient);
      workGraphConsumer = startWorkGraphConsumer({
        transport: catalog.getDurableTransport(),
        repository: workGraphRepository,
        accountResolver: new WorkSourceAccountRepository(docClient),
        // The document source has two declared writers. Admitting a second
        // service says who may SPEAK, not what they may say: the platform
        // may describe a presentation its own deck store wrote, and a CRDT
        // page stays the gateway's to describe.
        ownership: {
          ownsDocument: async (serviceId, documentId) => {
            if (serviceId !== 'platform-api') return false;
            const { getPresentationDocumentStore } = await import('./deck/presentationDocuments.js');
            const row = await getPresentationDocumentStore().get(documentId).catch(() => null);
            return !!row;
          },
        },
      });
      workGraphOutboxDrainer = startWorkGraphOutboxDrainer();
      runtimeReadiness.ready('work-graph');
      log.info('work-graph consumer and outbox drainer started');
    } catch (err: unknown) {
      const e = err as { message?: string };
      runtimeReadiness.fail('work-graph', err);
      log.error({ errMessage: e?.message }, 'work-graph runtime failed to start; feature is unavailable');
    }
  }
  // Phase 57b — start the activity.recorded consumer after the catalog
  // has connected. If the subscribe throws (e.g. event not declared), the
  // process continues and readiness reports durable-consumers as failed.
  try {
    activityConsumer = startActivityConsumer();
    log.info('activity-consumer started');
  } catch (err: unknown) {
    const e = err as { message?: string };
    consumerFailures.push(`activity-consumer: ${e?.message ?? String(err)}`);
    log.warn({ errMessage: e?.message }, 'activity-consumer failed to start');
  }
  // The meeting-notes summary is a step of the call-transcription pipeline
  // (src/recordings/recordingPipelines.ts); there is no summary consumer.
  // Wave A — summary-projector: single durable consumer of
  // `transcription:summary.ready`. Projects Action Items onto the
  // call-bound document and fires summary-bound pipelines.
  try {
    summaryProjector = startSummaryProjector();
    log.info('summary-projector started');
  } catch (err: unknown) {
    const e = err as { message?: string };
    consumerFailures.push(`summary-projector: ${e?.message ?? String(err)}`);
    log.warn({ errMessage: e?.message }, 'summary-projector failed to start');
  }
  // In-call emoji reactions — durable drain of `call:reaction.recorded`
  // into the call-reactions table (playback + transcript injection).
  try {
    callReactionConsumer = startCallReactionConsumer();
    log.info('call-reaction-consumer started');
  } catch (err: unknown) {
    const e = err as { message?: string };
    consumerFailures.push(`call-reaction-consumer: ${e?.message ?? String(err)}`);
    log.warn({ errMessage: e?.message }, 'call-reaction-consumer failed to start');
  }
  if (consumerFailures.length) runtimeReadiness.set('durable-consumers', 'failed', consumerFailures.join('; '));
  else runtimeReadiness.ready('durable-consumers');
}

runtimeReadiness.probe('event-catalog', async () => {
  const health = await catalog.getDurableTransport().healthCheck();
  return { ok: health.status === 'connected', details: `message-queue ${health.status}` };
});

(async () => {
  try {
    await catalogRetry.execute(() => connectCatalog());
    log.info('event-catalog connected');
  } catch (err: unknown) {
    const e = err as { message?: string };
    runtimeReadiness.set('event-catalog', 'failed', `connect failed after retries: ${e?.message ?? String(err)}; reconnecting`);
    log.warn(
      { errMessage: e?.message },
      'event-catalog connect failed after retries — scheduling background reconnect',
    );
    backgroundCatalogReconnect();
    return;
  }
  await startCatalogConsumers();
})();

function backgroundCatalogReconnect(): void {
  const attempt = (delay: number): void => {
    setTimeout(() => {
      connectCatalog()
        .then(() => {
          log.info('event-catalog connected (background retry)');
          return startCatalogConsumers();
        })
        .catch(() => {
          const nextDelay = Math.min(delay * 2, 60_000);
          log.warn({ nextDelayMs: nextDelay }, 'event-catalog background reconnect failed, retrying');
          attempt(nextDelay);
        });
    }, delay);
  };
  attempt(5_000);
}

// ---------------------------------------------------------------------------
// Chat-command plugins named in CHAT_COMMAND_PLUGINS (a package, or a path
// relative to the platform-api root). Everything they register into is live —
// the command registry, the step-handler map the pipeline module runs on, the
// system-pipeline list, the panel providers — so this neither waits for nor is
// awaited by the pipeline bootstrap below. A bad plugin is logged and skipped.
// ---------------------------------------------------------------------------
void loadChatCommandPlugins(createChatCommandPluginContext(log), chatCommandPluginSpecsFromEnv()).then(({ loaded, failed }) => {
  if (loaded.length || failed.length) log.info({ loaded, failed }, 'chat command plugins');
});

// ---------------------------------------------------------------------------
// Phase-4: bootstrap distributed-core's PipelineModule and wire its surfaces
// into the route layer. Failure here is non-fatal — the routes' stub paths
// keep working with the in-memory stubRunStore so dev workflows that don't
// need real LLM streaming aren't gated on this succeeding.
// ---------------------------------------------------------------------------

let pipelineShutdown: (() => Promise<void>) | null = null;

// Pipeline run persistence — wire PipelineRunRepository into bootstrap so run
// state is persisted to DDB. Opt-in via PIPELINE_RUN_PERSISTENCE_ENABLED=true.
const runPersistenceEnabled =
  (process.env.PIPELINE_RUN_PERSISTENCE_ENABLED ?? 'true').trim().toLowerCase() !== 'false';
const pipelineRunRepository = runPersistenceEnabled
  ? new PipelineRunRepository(docClient)
  : undefined;

// DLQ persistence — wire DLQRepository so failed events survive restarts.
// Opt-in via PIPELINE_DLQ_PERSISTENCE_ENABLED=true.
const dlqPersistenceEnabled =
  (process.env.PIPELINE_DLQ_PERSISTENCE_ENABLED ?? 'true').trim().toLowerCase() !== 'false';
const dlqRepository = dlqPersistenceEnabled
  ? new DLQRepository(docClient)
  : undefined;

const pipelineWiring = wirePipelineBridge({
  log,
  inProcessBootstrap: async () => {
    const { module, dlq, shutdown } = await bootstrapPipeline({
      pipelineRunRepository,
      dlqRepository,
    });
    return {
      bridge: createBridge(module, dlq, pipelineRunRepository),
      shutdown,
    };
  },
});
pipelineShutdown = pipelineWiring.shutdown;
// Install the bridge into the platform-api singleton once the wiring resolves.
void pipelineWiring.ready.then((bridge) => {
  if (bridge) {
    setPipelineBridge(bridge);
    runtimeReadiness.ready('pipelines');
  } else {
    // wirePipelineBridge resolves null when bootstrap failed or nothing was wired.
    runtimeReadiness.set('pipelines', 'failed', `pipeline bridge unavailable (mode ${pipelineWiring.mode})`);
  }
}, (err: unknown) => runtimeReadiness.fail('pipelines', err));

// ---------------------------------------------------------------------------
// Pipeline definitions cache — Scan-backed, refreshed every 60s.
//
// Replaces the old write-through in-memory mirror. The schedule evaluator
// (below) and the public webhook router both consume this cache
// synchronously, so cold-start blindness after a process restart is bounded
// by the first refresh's Scan latency (~tens of ms in-region) rather than
// requiring every pipeline to be re-touched via PUT.
//
// We deliberately don't `await` the first refresh — if DynamoDB is slow or
// the table doesn't exist yet, blocking startup would be worse than the
// (at most) 60-second window of "scheduler/webhooks see no pipelines".
// ---------------------------------------------------------------------------
pipelineDefinitionsCache.start().catch((err: unknown) => {
  log.error({ err }, 'pipelineDefinitionsCache initial refresh failed (will retry on tick)');
});

// Phase 54.6.2: cross-replica push invalidation. The 60s timer above is
// the safety net; this subscriber gives sub-second freshness when a
// sibling replica writes. Failure to start is non-fatal — falls back to
// the timer.
let definitionsInvalidationSubscriber: { stop: () => Promise<void> } | null = null;
// NFR #16: the membership projection's invalidation — every committed chat
// membership write (gateway) drops that person's cached conversation list.
void startMembershipInvalidationSubscriber().catch((err: unknown) => {
  log.warn({ errMessage: err instanceof Error ? err.message : String(err) },
    'membership-invalidate subscriber failed to start (cached conversation lists age out by TTL)');
});
void startInvalidationSubscriber(pipelineDefinitionsCache).then((sub) => {
  definitionsInvalidationSubscriber = sub;
}).catch((err: unknown) => {
  log.warn(
    { errMessage: err instanceof Error ? err.message : String(err) },
    'definitions-invalidate subscriber failed to start (timer will still refresh)',
  );
});

// ---------------------------------------------------------------------------
// Control-plane: subscriber registry.
//
// Owns a single ControlPlaneChannel + Redis connection shared by every
// registered handler (today: AgenticPipeline config; future: FileUpload,
// VideoHangout, CollaborativeDocument, etc.). Without this each new CRD
// kind would burn its own Redis connection.
//
// Non-fatal: if Redis is down at start() time the registry logs a warn
// and leaves handlers idle until the next start() attempt. The operator
// re-pushes once Redis recovers.
// ---------------------------------------------------------------------------
const controlPlaneRegistry = new ControlPlaneSubscriberRegistry({});
controlPlaneRegistry.register(
  createPipelineConfigRegistration({ definitionsRepository: definitionsRepo }),
);
controlPlaneRegistry.register(createFileUploadEndpointRegistration());
void controlPlaneRegistry.start().catch((err: unknown) => {
  log.warn(
    { errMessage: err instanceof Error ? err.message : String(err) },
    'control-plane registry bootstrap failed (non-fatal)',
  );
});

// ---------------------------------------------------------------------------
// Schedule evaluator — fires pipelines whose triggerBinding.event === 'schedule'
// on their cron expression. Phase 1: logs only; Phase 4 will call the pipeline
// resource to produce a run.
// ---------------------------------------------------------------------------

interface ScheduledPipelineShape {
  id?: unknown;
  status?: unknown;
  triggerBinding?: { event?: unknown; schedule?: unknown };
}

// ROADMAP P2.1 — the schedule evaluator is single-flight across replicas on a
// FencedLease (distributed-core/cluster/leases) plus a per-fire Redis claim.
const scheduleLeaseRedis = createLeaseRedisGetter(sharedRedisUrl());
const scheduler = createScheduleEvaluator({
  listPipelines: () => {
    const out: Array<{ id: string; status: string; triggerBinding?: { event: string; schedule?: string } }> = [];
    // Reads from the in-memory cache — `pipelineDefinitionsCache` refreshes
    // every 60s from DynamoDB, so this loop never makes a network call. The
    // only correctness invariant we care about here is "the cache is at most
    // 60s stale", which is fine for cron-minute granularity.
    for (const raw of pipelineDefinitionsCache.all()) {
      const p = raw as ScheduledPipelineShape;
      if (typeof p?.id !== 'string' || typeof p.status !== 'string') continue;
      const tb = p.triggerBinding;
      const triggerBinding = tb && typeof tb.event === 'string'
        ? {
            event: tb.event,
            schedule: typeof tb.schedule === 'string' ? tb.schedule : undefined,
          }
        : undefined;
      out.push({ id: p.id, status: p.status, triggerBinding });
    }
    return out;
  },
  trigger: async (pipelineId, payload) => {
    // Wave A (2026-08-25): actually create a run. This was the "one line
    // short" stub — cron matching, leader election and the definitions
    // cache all existed, but the fire was log-only.
    const bridge = getPipelineBridge();
    if (!bridge?.trigger) {
      log.warn({ pipelineId }, 'scheduler fired but pipeline bridge unavailable — skipping');
      return;
    }
    // `definition` is REQUIRED by PipelineModule.createResource (same
    // contract as the webhook route) — resolve it from the cache entry
    // that produced this fire.
    const definition = pipelineDefinitionsCache
      .all()
      .find((d) => (d as { id?: unknown }).id === pipelineId);
    if (!definition) {
      log.warn({ pipelineId }, 'scheduler fired but definition missing from cache — skipping');
      return;
    }
    try {
      // A definition can pin what its scheduled runs start from — a document
      // automation carries the document and instruction here — the same
      // fields a manual trigger would post. Without them a run of the
      // document chain found no documentId and failed at its first step.
      const pinned = ((definition as { triggerBinding?: { payload?: unknown } }).triggerBinding?.payload ?? {}) as Record<string, unknown>;
      const out = await bridge.trigger({
        pipelineId,
        definition,
        triggerPayload: { triggerType: 'schedule', ...(pinned && typeof pinned === 'object' ? pinned : {}), ...payload },
        triggeredBy: { userId: `schedule:${pipelineId}` },
      });
      log.info({ pipelineId, runId: out.runId }, 'scheduler fired pipeline run');
    } catch (err: unknown) {
      log.error(
        { pipelineId, errMessage: err instanceof Error ? err.message : String(err) },
        'scheduler bridge.trigger failed',
      );
    }
  },
  // Only the replica holding the fenced lease evaluates, and each
  // (pipeline, minute) fire is claimed atomically against that lease, so two
  // replicas — or a failover mid-minute — never fire a cron pipeline twice.
  leadership: createFencedSchedulerLeadership({ redis: scheduleLeaseRedis.get }),
});
scheduler.start();
runtimeReadiness.ready('scheduler');

// Graceful shutdown — registered via service-runtime/lifecycle's ShutdownManager.
//
// Hooks run in REVERSE registration order (LIFO), so to preserve the original
// teardown sequence we register from the OUTSIDE in: http drain LAST, scheduler
// FIRST. The manager binds SIGTERM/SIGINT itself, applies per-hook timeouts
// (default 10s) and an overall budget (default 30s) before force-exit, and
// catches individual hook errors so a single failure no longer aborts the
// rest of the teardown.
const sd = createShutdownManager({ logger: log });

// Registered FIRST → drained LAST. Closing the HTTP server is the final step
// before process exit (matches the original `server.close(() => process.exit(0))`).
sd.register('http-server', () => drainHttpServer(server));
sd.register('tracing', () => tracingShutdown());
sd.register('event-catalog', () => disconnectCatalog());
sd.register('control-plane-registry', async () => {
  await controlPlaneRegistry.stop();
});
sd.register('activity-consumer', async () => {
  if (activityConsumer) await activityConsumer.stop();
  if (summaryProjector) await summaryProjector.stop();
  if (callReactionConsumer) await callReactionConsumer.stop();
});
sd.register('work-graph', async () => {
  workGraphOutboxDrainer?.stop();
  if (workGraphConsumer) await workGraphConsumer.stop();
});
sd.register('outbox-drainer', () => { drainerStopped = true; drainer?.stop(); });
sd.register('pipeline', async () => {
  if (pipelineShutdown) await pipelineShutdown();
});
sd.register('definitions-invalidate', async () => {
  if (definitionsInvalidationSubscriber) await definitionsInvalidationSubscriber.stop();
});
sd.register('definitions-cache', () => { pipelineDefinitionsCache.stop(); });
// Registered LAST → drained FIRST. Stopping the scheduler timer first ensures
// no new pipeline triggers fire while downstream resources are being torn down.
sd.register('scheduler', async () => {
  await scheduler.shutdown();
  await scheduleLeaseRedis.close();
});
// Registered after everything else → runs first: readiness drops before any
// consumer, drainer or listener stops, so no new traffic is routed here.
sd.register('readiness', () => { runtimeReadiness.beginDrain(); });
