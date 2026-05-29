// IMPORTANT: tracing.start() must run before any other imports so that
// OpenTelemetry auto-instrumentation can monkey-patch http/express/etc.
// before they are require()'d by the rest of the application.
import { start as startTracing } from './tracing';
const tracingShutdown = startTracing();

import { createApp } from './app';
import { createScheduleEvaluator } from './services/scheduleEvaluator';
import { createDDBSchedulerLeaderLock } from './services/scheduler-leader-lock';
import { pipelineDefinitionsCache } from './pipeline/definitions-cache';
import { startInvalidationSubscriber } from './pipeline/definitions-invalidator';
import { wirePipelineBridge } from '@connorhoehn/pipeline-runtime';
import { setPipelineBridge } from './pipeline/bridge-types';
import { bootstrapPipeline } from './pipeline/bootstrap';
import { createBridge } from './pipeline/createBridge';
import { PipelineRunRepository } from './repositories/PipelineRunRepository';
import { DLQRepository } from './repositories/DLQRepository';
import { docClient } from './lib/aws-clients';
import log from './lib/logger';
import { assertRequiredInProd } from 'distributed-core';
import { createShutdownManager, drainHttpServer } from '@connorhoehn/service-runtime';
import { CatalogIngestEventBus } from './ingest/catalog-event-bus';
import { IngestEngine } from './ingest/engine';
import { setIngestEngine } from './ingest/engine-singleton';
import { createIngestTransport } from './ingest/transport';
import { RetryManager } from 'distributed-core/transport';
import { connectCatalog, disconnectCatalog, publishDurable } from './lib/event-catalog';
import { ensureOutboxTTL, ensureVideoSessionsTTL } from './lib/dynamo-setup';
import { resolveVideoSessionStoreMode } from './repositories/videoSessionStoreFactory';
import { startOutboxDrainer } from './services/outbox-drainer';
import { startActivityConsumer } from './services/activity-consumer';
import { createPipelineConfigRegistration } from './control-plane/pipeline-config-subscriber';
import { createFileUploadEndpointRegistration } from './control-plane/file-upload-subscriber';
import { ControlPlaneSubscriberRegistry } from './control-plane/registry';
import { definitionsRepo } from './pipeline/definitions-repository';

const port = process.env.PORT ?? '3001';

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
const server = app.listen(Number(port), () => {
  log.info({ port }, 'platform-api listening');
  // Register the LVS recording webhook subscription on boot so
  // recording.completed events flow into /hooks/lvs/recording → DDB.
  // Idempotent + non-fatal — failures log and the recording pipeline
  // simply won't persist until the next platform-api boot succeeds.
  void import('./lib/lvs-webhook-bootstrap.js').then(({ bootstrapLvsWebhook }) =>
    bootstrapLvsWebhook(),
  ).catch((e: any) => log.warn({ err: e?.message ?? e }, 'lvs-webhook-bootstrap failed'));
});

// ---------------------------------------------------------------------------
// Event catalog — connects to Redis + message-queue so domain events can be
// published to the catalog. Non-fatal: if connection fails the catalog client
// degrades gracefully (events simply won't publish).
// The outbox drainer starts unconditionally — it drains from DynamoDB which is
// independent of the catalog connection.
// ---------------------------------------------------------------------------

const drainer = startOutboxDrainer();
log.info('outbox-drainer started');

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

const catalogRetry = new RetryManager({
  maxRetries: 2,
  baseDelay: 2000,
  backoffFactor: 2,
  jitter: false,
  enableLogging: false,
  name: 'catalog-connect',
});

let activityConsumer: { stop: () => Promise<void> } | null = null;

(async () => {
  try {
    await catalogRetry.execute(() => connectCatalog());
    log.info('event-catalog connected');
    // Phase 57b — start the activity.recorded consumer after the catalog
    // has connected. Non-fatal — if the subscribe throws (e.g. event not
    // declared, durable transport down), we log and continue without it.
    try {
      activityConsumer = startActivityConsumer();
      log.info('activity-consumer started');
    } catch (err: unknown) {
      const e = err as { message?: string };
      log.warn({ errMessage: e?.message }, 'activity-consumer failed to start (non-fatal)');
    }
  } catch (err: unknown) {
    const e = err as { message?: string };
    log.warn(
      { errMessage: e?.message },
      'event-catalog connect failed after retries — scheduling background reconnect',
    );
    backgroundCatalogReconnect();
  }
})();

function backgroundCatalogReconnect(): void {
  const attempt = (delay: number): void => {
    setTimeout(() => {
      connectCatalog()
        .then(() => log.info('event-catalog connected (background retry)'))
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
  if (bridge) setPipelineBridge(bridge);
});

// ---------------------------------------------------------------------------
// Ingest engine bootstrap — wires IngestEngine with catalog-backed event bus
// so progress events flow to the gateway via event-catalog's signal transport
// (Redis pub/sub under the hood). Non-fatal: routes return 503 if engine
// isn't available.
//
// Wave2-PA-2: the producer/consumer handoff is now transport-pluggable via
// `PLATFORM_API_INGEST_TRANSPORT` (default `memory`, optional `mq`). See
// src/ingest/transport.ts for the rationale + restart-safety trade-offs.
// ---------------------------------------------------------------------------

let ingestShutdown: (() => Promise<void>) | null = null;

(async () => {
  try {
    const { ingestSourceRepo, ingestItemRepo, syncCursorRepo } = await import('./ingest/repositories/index.js');
    const { typedDocumentRepo } = await import('./repositories/index.js');

    const eventBus = new CatalogIngestEventBus();
    const transport = createIngestTransport();

    const engine = new IngestEngine({
      sourceRepository: ingestSourceRepo,
      itemRepository: ingestItemRepo,
      cursorRepository: syncCursorRepo,
      documentRepository: typedDocumentRepo,
      eventBus,
      enqueueItem: transport.enqueueItem,
    });

    await engine.start();
    setIngestEngine(engine);

    const consumer = transport.startConsumer(engine);

    ingestShutdown = async () => {
      await consumer.stop();
      await engine.stop();
    };

    log.info(
      { transport: transport.mode },
      'IngestEngine bootstrapped',
    );
  } catch (err: unknown) {
    const e = err as { message?: string };
    log.error(
      { errMessage: e?.message },
      'IngestEngine bootstrap FAILED — ingest routes will return 503',
    );
  }
})();

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
    // Phase 1: log only. Phase 4 will create a pipeline run via ResourceRouter.
    log.info({ pipelineId, payload }, 'scheduler firing');
  },
  // Phase 54.6.1 — multi-replica safe leader election. Without this, every
  // replica's tick would fire every matching cron pipeline once per minute,
  // causing N-fold duplicate runs. The DDB-conditional lock here mirrors
  // outbox-drainer's lease pattern. Owner id includes hostname + pid so
  // operators can identify which replica is currently the leader from the
  // DDB row.
  leaderLock: createDDBSchedulerLeaderLock(
    `${process.env.HOSTNAME ?? 'localhost'}-${process.pid}`,
  ),
});
scheduler.start();

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
});
sd.register('outbox-drainer', () => { drainer.stop(); });
sd.register('ingest', async () => {
  if (ingestShutdown) await ingestShutdown();
});
sd.register('pipeline', async () => {
  if (pipelineShutdown) await pipelineShutdown();
});
sd.register('definitions-invalidate', async () => {
  if (definitionsInvalidationSubscriber) await definitionsInvalidationSubscriber.stop();
});
sd.register('definitions-cache', () => { pipelineDefinitionsCache.stop(); });
// Registered LAST → drained FIRST. Stopping the scheduler timer first ensures
// no new pipeline triggers fire while downstream resources are being torn down.
sd.register('scheduler', () => { scheduler.stop(); });
