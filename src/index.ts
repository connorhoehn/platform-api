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
import { CatalogIngestEventBus } from './ingest/catalog-event-bus';
import { IngestEngine } from './ingest/engine';
import { setIngestEngine } from './ingest/engine-singleton';
import { startIngestQueueConsumer } from './ingest/queue-consumer';
import { RetryManager } from 'distributed-core/dist/transport/RetryManager';
import { connectCatalog, disconnectCatalog, publishDurable } from './lib/event-catalog';
import { ensureOutboxTTL } from './lib/dynamo-setup';
import { startOutboxDrainer } from './services/outbox-drainer';
import { startActivityConsumer } from './services/activity-consumer';

const port = process.env.PORT ?? '3001';

if (!process.env.COGNITO_REGION || !process.env.COGNITO_USER_POOL_ID) {
  log.error({ env: { COGNITO_REGION: !!process.env.COGNITO_REGION, COGNITO_USER_POOL_ID: !!process.env.COGNITO_USER_POOL_ID } }, 'FATAL: COGNITO_REGION and COGNITO_USER_POOL_ID must be set');
  process.exit(1);
}

const app = createApp();
const server = app.listen(Number(port), () => {
  log.info({ port }, 'platform-api listening');
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

ensureOutboxTTL().catch((err: unknown) => {
  log.warn({ err }, 'outbox TTL auto-provision failed');
});

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
// ---------------------------------------------------------------------------

let ingestShutdown: (() => Promise<void>) | null = null;

(async () => {
  try {
    const { ingestSourceRepo, ingestItemRepo, syncCursorRepo } = await import('./ingest/repositories');
    const { typedDocumentRepo } = await import('./repositories');

    const eventBus = new CatalogIngestEventBus();

    const engine = new IngestEngine({
      sourceRepository: ingestSourceRepo,
      itemRepository: ingestItemRepo,
      cursorRepository: syncCursorRepo,
      documentRepository: typedDocumentRepo,
      eventBus,
      // Phase 54.6.3 — durable handoff to IngestQueueConsumer (below).
      enqueueItem: async ({ ingestId, sourceId, tags }) => {
        await publishDurable('ingest:item.queued', { ingestId, sourceId, tags });
      },
    });

    await engine.start();
    setIngestEngine(engine);

    const queueConsumer = startIngestQueueConsumer({ engine });

    ingestShutdown = async () => {
      await queueConsumer.stop();
      await engine.stop();
    };

    log.info('IngestEngine bootstrapped (catalog event bus + durable queue consumer)');
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

// Graceful shutdown — stop the timer so the process can exit cleanly.
async function shutdown(signal: string): Promise<void> {
  log.info({ signal }, 'received shutdown signal');
  scheduler.stop();
  pipelineDefinitionsCache.stop();
  if (definitionsInvalidationSubscriber) {
    try { await definitionsInvalidationSubscriber.stop(); } catch (err) {
      log.warn({ err }, 'definitions-invalidate subscriber stop failed');
    }
  }
  if (pipelineShutdown) {
    try { await pipelineShutdown(); } catch (err) {
      log.error({ err }, 'pipeline shutdown failed');
    }
  }
  if (ingestShutdown) {
    try { await ingestShutdown(); } catch (err) {
      log.error({ err }, 'ingest shutdown failed');
    }
  }
  drainer.stop();
  if (activityConsumer) {
    try { await activityConsumer.stop(); } catch (err) {
      log.error({ err }, 'activity-consumer stop failed');
    }
  }
  try { await disconnectCatalog(); } catch (err) {
    log.error({ err }, 'event-catalog disconnect failed');
  }
  try { await tracingShutdown(); } catch (err) {
    log.error({ err }, 'tracing shutdown failed');
  }
  server.close(() => process.exit(0));
}
process.on('SIGTERM', () => { void shutdown('SIGTERM'); });
process.on('SIGINT',  () => { void shutdown('SIGINT'); });
