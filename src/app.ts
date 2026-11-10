import express from 'express';
import { requestContextMiddleware } from '@connorhoehn/service-runtime';
import { requireDocumentPolicy } from './middleware/document-policy';
import { requireAuth } from './middleware/auth';
import { corsMiddleware } from './middleware/cors';
import { securityHeaders } from './middleware/security-headers';
import { serviceOnBehalfOf } from './middleware/serviceOnBehalfOf';
import { requestLogger } from './middleware/request-logger';
import { httpMetrics } from './middleware/http-metrics';
import { errorHandler } from './middleware/error-handler';
import { pipelineReadRateLimit } from './middleware/rateLimit';
import { requireLoopbackOrServiceAuth } from './middleware/internalOpsAccess';
import healthRouter, { livezHandler } from './routes/health';
import apiRouter from './routes/index';
import { pipelineWebhooksRouter } from './routes/pipelineWebhooks';
import { renderPrometheusText } from './observability/metrics';
import { busPublishTimingSummary } from './pipeline/busPublishTiming';
import { createCapabilitiesRouter } from './routes/capabilities';
import { definitionsRepo } from './pipeline/definitions-repository';
import { internalWorkGraphRouter } from './routes/internal-work-graph';
import { internalPipelineAccessRouter } from './routes/internal-pipeline-access';
import { internalVideoSessionsRouter } from './routes/internal-video-sessions';

/**
 * Which requests the GLOBAL `express.json()` is allowed to consume.
 *
 * Every HMAC-verified webhook receiver mounts its own
 * `express.raw({ type: '*\/*' })` and needs the exact bytes the sender signed.
 * body-parser marks a request consumed (`req._body = true`) as soon as one
 * parser handles it, so a router-level raw parser silently no-ops once the
 * global JSON parser has run — the handler then sees a parsed object, an
 * empty rawBody, and a HMAC computed over zero bytes.
 *
 * Exported (rather than inlined in the `express.json({ type })` option) so
 * this rule is directly testable; the regression it guards against is
 * invisible from the outside except as "signatures never verify and payloads
 * arrive empty".
 */
export function shouldParseJsonBody(url: string, contentType: string): boolean {
  if (url.startsWith('/hooks/lvs')) return false;
  if (url.startsWith('/hooks/pipeline')) return false;
  return contentType.includes('application/json');
}

export function createApp(): express.Application {
  const app = express();
  app.disable('x-powered-by');

  // Request context — must be FIRST so X-Correlation-ID and AsyncLocalStorage
  // context are available to all downstream middleware and route handlers.
  app.use(requestContextMiddleware());

  // Security headers on every response, including 404s and errors.
  app.use(securityHeaders());

  // CORS — only origins in CORS_ALLOWED_ORIGINS (loopback dev servers in
  // insecure dev mode). See middleware/cors.ts.
  app.use(corsMiddleware());

  // Global JSON parser — skipped for /hooks/lvs/* and /hooks/pipeline/* so
  // those webhook receivers can read the raw bytes for HMAC verification.
  // Once the global parser consumes the stream, the route-level raw parser
  // can't re-read it; req.body would arrive as a parsed Object and
  // the HMAC update() would crash with ERR_INVALID_ARG_TYPE.
  //
  // /hooks/pipeline was missing from this list: pipelineWebhooks.ts mounts
  // `express.raw({ type: '*/*' })` at the router level and its header comment
  // claims Express "picks the first body parser that matches", but body-parser
  // marks the request consumed (`req._body`) so the router-level raw parser
  // no-ops. Every `Content-Type: application/json` webhook therefore reached
  // the handler with a parsed object, `Buffer.isBuffer(req.body)` false, and
  // an EMPTY rawBody — so signed webhooks could never verify (HMAC over zero
  // bytes) and unsigned ones forwarded `body: {}` into the pipeline. Silent
  // payload loss; live pod logged `bodySize: 0` for a 15-byte POST.
  // The media walker's batches: forty thumbnails is a few MB, many times the
  // global limit. Scoped to the one path that carries them; body-parser marks
  // the request parsed, so the global parser below leaves it alone. 12mb fits
  // the route's 8 MB thumbnail cap as base64 (MAX_BATCH_THUMBNAIL_BYTES).
  app.use('/api/media', express.json({ limit: '12mb' }));
  // A DeckSpec is bigger than the 100kb the global parser allows — 40 slides of
  // bullets, notes and citations. These two paths take one up to 2mb so the
  // deck routes' own cap (MAX_SPEC_BYTES) is what answers 413, rather than
  // body-parser throwing a 500 on the way in.
  app.use(['/api/deck', '/api/documents/:documentId/revisions'], express.json({ limit: '2mb' }));
  app.use(express.json({
    type: (req) =>
      shouldParseJsonBody(
        (req as any).url || '',
        String(req.headers['content-type'] || ''),
      ),
  }));
  app.use(requestLogger);
  app.use(httpMetrics);

  // Health check — no auth required. Mounted at /health (canonical) and
  // /api/health (alias) so callers using the /api/* convention don't 404.
  // Both mounts use the same router so behaviour stays identical.
  app.use('/health', healthRouter);
  app.use('/api/health', healthRouter);
  // Liveness probe target — process-only, no dependency calls (see health.ts).
  app.get('/livez', livezHandler);

  // Prometheus scrape endpoint — distributed-core MetricsRegistry shadow.
  // Loopback or a service-auth envelope only (middleware/internalOpsAccess.ts):
  // it used to be public, and its sibling below resets counters. A scraper
  // runs as a sidecar or signs its request. See observability/metrics.ts.
  const internalOps = requireLoopbackOrServiceAuth();
  app.get('/internal/metrics', internalOps, (_req, res) => {
    res.set('Content-Type', 'text/plain; version=0.0.4; charset=utf-8');
    res.status(200).send(renderPrometheusText());
  });

  // realtime-examples NFR #197: pipeline bus publish latency, by phase.
  app.get('/internal/bus-publish-timing', internalOps, (req, res) => {
    res.status(200).json(busPublishTimingSummary(req.query.reset === '1'));
  });

  // Gateway-only signed projection authorization/replay boundary. This is
  // deliberately mounted before Cognito auth because the caller is a service,
  // while every request is still bound to a verified socket identity.
  app.use('/internal/work-graph', internalWorkGraphRouter);
  // realtime-examples NFR #142: may this socket subscribe to a run channel?
  app.use('/internal/pipeline-access', internalPipelineAccessRouter);
  // Document calls: the gateway reports a person gone / a call empty after
  // the rejoin grace (tab closed without Leave).
  app.use('/internal/video-sessions', internalVideoSessionsRouter);

  // Pipeline webhooks — public endpoints (no auth) for external systems to
  // fire pipelines. Mounted BEFORE requireAuth because webhook callers don't
  // carry a Cognito JWT. See routes/pipelineWebhooks.ts.
  app.use('/hooks/pipeline', pipelineWebhooksRouter);

  // LVS recording webhook — receives `recording.started` / `recording.completed`
  // from live-video-streaming, persists CallRecording rows. HMAC-protected via
  // LVS_WEBHOOK_SECRET (skipped when env unset for dev). Mounted before
  // requireAuth because LVS uses HMAC, not Cognito.
  const { lvsWebhooksRouter } = require('./routes/lvsWebhooks');
  app.use('/hooks/lvs', lvsWebhooksRouter);

  // LVS admin / observability — operator endpoints for inspecting the
  // recording pipeline (delivery ring buffer, recent VideoSessions,
  // call-session-bindings). Mounted BEFORE requireAuth because admin
  // operators carry an ADMIN_TOKEN shared secret, not a Cognito JWT.
  // The router applies `requireAdmin` per-route so unauthenticated
  // callers are rejected.
  const { lvsAdminRouter } = require('./routes/lvsAdmin');
  app.use('/api/admin', lvsAdminRouter);
  // The /agent document backfill (routes/agentDocumentsAdmin.ts), same secret.
  const { agentDocumentsAdminRouter } = require('./routes/agentDocumentsAdmin');
  app.use('/api/admin', agentDocumentsAdminRouter);

  // Verify document delegation before the generic Cognito boundary. These
  // grants use Ed25519 and intentionally are not Cognito login tokens.
  app.use('/api/documents/:documentId', (req, res, next) => {
    if (req.params.documentId === 'lifecycle-schema') { next(); return; }
    void requireDocumentPolicy(req, res, next);
  });
  // A WebSocket-triggered run: the gateway names the person inside its signed
  // body; verified here, then requireAuth accepts that principal (NFR #72).
  app.use('/api/pipelines/:pipelineId/runs', serviceOnBehalfOf);
  // All other routes retain normal Cognito authentication. requireAuth accepts
  // a document identity only after the scoped verifier attached it above.
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

  // First-class persistent hangout rooms — mounted BEFORE apiRouter so
  // `/api/rooms` paths route to the new HangoutRoomRepository-backed
  // handlers. The legacy social-rooms router (inside apiRouter) still
  // owns sub-paths like `/api/rooms/:roomId/posts` and `/api/rooms/dm`
  // because Express falls through to the next matching middleware when
  // a router has no matching handler.
  const { hangoutRoomsRouter } = require('./routes/hangoutRooms');
  app.use('/api/rooms', hangoutRoomsRouter);

  // The media catalog: the host walker posts batches here; the catalog page reads here.
  const { mediaRouter } = require('./routes/media');
  app.use('/api/media', mediaRouter);

  // API routes (profiles, groups, rooms, etc.) mounted here in later phases
  app.use('/api', apiRouter);

  // Central error middleware — MUST be last
  app.use(errorHandler);

  return app;
}
