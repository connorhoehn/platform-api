import express from 'express';
import { requestContextMiddleware } from '@connorhoehn/service-runtime';
import { requireAuth } from './middleware/auth';
import { requestLogger } from './middleware/request-logger';
import { errorHandler } from './middleware/error-handler';
import { pipelineReadRateLimit } from './middleware/rateLimit';
import healthRouter from './routes/health';
import apiRouter from './routes/index';
import { pipelineWebhooksRouter } from './routes/pipelineWebhooks';
import { ingestWebhooksRouter } from './ingest/routes/ingestWebhooks';
import { confluenceWebhookRouter } from './ingest/routes/ingestConfluenceWebhook';
import { renderPrometheusText } from './observability/metrics';
import { createCapabilitiesRouter } from './routes/capabilities';
import { definitionsRepo } from './pipeline/definitions-repository';

export function createApp(): express.Application {
  const app = express();

  // Request context — must be FIRST so X-Correlation-ID and AsyncLocalStorage
  // context are available to all downstream middleware and route handlers.
  app.use(requestContextMiddleware());

  // CORS — allow frontend dev server. Echoes the request Origin so the
  // response is compatible with `credentials: include` fetches (browsers
  // reject the wildcard `*` when credentials are sent).
  app.use((req, res, next) => {
    const origin = req.headers.origin;
    if (origin) res.header('Access-Control-Allow-Origin', origin);
    res.header('Vary', 'Origin');
    res.header('Access-Control-Allow-Credentials', 'true');
    res.header('Access-Control-Allow-Methods', 'GET,POST,PUT,PATCH,DELETE,OPTIONS');
    res.header('Access-Control-Allow-Headers', 'Content-Type,Authorization');
    if (req.method === 'OPTIONS') { res.sendStatus(204); return; }
    next();
  });

  // Global JSON parser — skipped for /hooks/lvs/* so the LVS webhook
  // receiver can read the raw bytes for HMAC verification. Once the
  // global parser consumes the stream, the route-level raw parser
  // can't re-read it; req.body would arrive as a parsed Object and
  // the HMAC update() would crash with ERR_INVALID_ARG_TYPE.
  app.use(express.json({
    type: (req) => {
      const url = (req as any).url || '';
      if (url.startsWith('/hooks/lvs')) return false;
      const ct = req.headers['content-type'] || '';
      return ct.includes('application/json');
    },
  }));
  app.use(requestLogger);

  // Health check — no auth required. Mounted at /health (canonical) and
  // /api/health (alias) so callers using the /api/* convention don't 404.
  // Both mounts use the same router so behaviour stays identical.
  app.use('/health', healthRouter);
  app.use('/api/health', healthRouter);

  // Prometheus scrape endpoint — distributed-core MetricsRegistry shadow.
  // Public (no auth) so a Prometheus server can scrape it; matches the
  // gateway's `GET /internal/metrics` mount. See observability/metrics.ts.
  app.get('/internal/metrics', (_req, res) => {
    res.set('Content-Type', 'text/plain; version=0.0.4; charset=utf-8');
    res.status(200).send(renderPrometheusText());
  });

  // Pipeline webhooks — public endpoints (no auth) for external systems to
  // fire pipelines. Mounted BEFORE requireAuth because webhook callers don't
  // carry a Cognito JWT. See routes/pipelineWebhooks.ts.
  app.use('/hooks/pipeline', pipelineWebhooksRouter);

  // Ingest webhooks — public endpoint for external sources to push change
  // notifications. Mounted BEFORE requireAuth so external systems (S3, Drive,
  // etc.) can call without a JWT. See ingest/routes/ingestWebhooks.ts.
  app.use('/api/ingest/webhooks', ingestWebhooksRouter);

  // Confluence webhook — Atlassian "page updated" callbacks land here and
  // are normalized into the standard IngestEvent shape. Mount path matches
  // the connector spec: /api/ingest/webhook/confluence/:sourceId.
  app.use('/api/ingest/webhook/confluence', confluenceWebhookRouter);

  // LVS recording webhook — receives `recording.started` / `recording.completed`
  // from live-video-streaming, persists CallRecording rows. HMAC-protected via
  // LVS_WEBHOOK_SECRET (skipped when env unset for dev). Mounted before
  // requireAuth because LVS uses HMAC, not Cognito.
  const { lvsWebhooksRouter } = require('./routes/lvsWebhooks');
  app.use('/hooks/lvs', lvsWebhooksRouter);

  // All routes below this line require a valid Cognito JWT
  app.use(requireAuth);

  // Pipeline READ rate limit — applied at the mount level so we don't have
  // to touch any individual pipeline route file. Method-aware: only GETs are
  // limited; POST/PUT/etc pass through to their own per-route limiters.
  const pipelineGetLimiter = pipelineReadRateLimit();
  app.use('/api/pipelines', (req, res, next) => {
    if (req.method !== 'GET') { next(); return; }
    pipelineGetLimiter(req, res, next);
  });

  // Capabilities — read-only introspection surface for active AgenticPipelines.
  // Mounted before the main apiRouter so /api/capabilities/* resolves without
  // competing with any :pipelineId param route inside apiRouter.
  app.use('/api/capabilities', createCapabilitiesRouter({ definitions: definitionsRepo }));

  // API routes (profiles, groups, rooms, etc.) mounted here in later phases
  app.use('/api', apiRouter);

  // Central error middleware — MUST be last
  app.use(errorHandler);

  return app;
}
