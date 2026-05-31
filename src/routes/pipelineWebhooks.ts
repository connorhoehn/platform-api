// platform-api/src/routes/pipelineWebhooks.ts
//
// Public webhook endpoints for pipeline triggers. Mounted at /hooks/pipeline
// in app.ts BEFORE the Cognito auth middleware so external systems (Stripe,
// GitHub, internal cron, etc.) can fire pipelines without a JWT.
//
// Phase 1: validate path, capture x-* headers + body, log + 202.
// Phase 4 (this file): also enforce HMAC-SHA256 signature verification when
// the matched pipeline definition's webhook trigger binding carries a
// `webhookSecret`. Format follows GitHub's convention:
//
//   X-Pipeline-Signature-256: sha256=<hex>
//
// where `<hex>` is HMAC-SHA256(secret, raw_request_body). When no secret is
// configured we fall back to the legacy "accept unsigned" behavior so dev
// fixtures keep working — Phase-5 will gate this on a global config flag.
//
// Raw-body gotcha: `express.json()` is mounted globally in `createApp()`
// which would consume `req.body` before this route runs. To preserve the
// exact bytes signed by the source we mount `express.raw({ type: '*/*' })`
// at the router level — Express picks the first body parser that matches,
// so the upstream `express.json()` never touches webhook traffic. After
// signature verification we parse the raw buffer ourselves into the JSON
// object that the existing log line / future Phase-4 forwarder expects.

import express, { Router, type Request, type Response } from 'express';
import {
  SIGNATURE_HEADER,
  verifySignature,
} from 'distributed-core';
import { withContext } from '../lib/logger';
import {
  checkReplayWindow,
  WEBHOOK_TIMESTAMP_HEADER,
} from '../lib/webhook-replay';
import {
  recordPipelineTrigger,
  recordPipelineError,
} from '../observability/metrics';
import { auditRepo } from '../pipeline/audit-repository';
import { pipelineDefinitionsCache } from '../pipeline/definitions-cache';
import { getPipelineBridge } from './pipelineTriggers';

// Route-scoped structured logger. Every log line emitted by this file
// inherits `route: 'pipelineWebhooks'` so observability can filter on it.
const log = withContext({ route: 'pipelineWebhooks' });

export const pipelineWebhooksRouter = Router();

// Path constraint shared with the frontend TriggerConfig "Full URL" preview:
// alphanumeric plus _ and -, max 64 chars. Matches the regex used in the UI
// validator so the displayed URL is always invokable.
const VALID_PATH_RE = /^[a-zA-Z0-9_-]{1,64}$/;

// 1 MiB upper bound — webhook payloads are typically tiny (KB), and a hard
// cap prevents an unauthenticated public endpoint from being used as a
// memory-amplification vector.
const MAX_BODY_BYTES = 1024 * 1024;

/**
 * Pure helper exposed for tests — extracts only the `x-*` headers (case-
 * insensitive) and discards everything else. Authorization, Cookie, and
 * Content-Type are intentionally excluded so they never leak into pipeline
 * context as a side-effect of the webhook bridge. The signature header is
 * also stripped here — once verified it has no business reaching pipeline
 * context.
 */
export function pickSafeHeaders(
  headers: Record<string, string | string[] | undefined>,
): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(headers)) {
    if (typeof v === 'string' && k.toLowerCase().startsWith('x-')) {
      if (k.toLowerCase() === SIGNATURE_HEADER) continue;
      out[k] = v;
    }
  }
  return out;
}

/**
 * Lookup the webhook secret (if any) configured for a given path across all
 * pipeline definitions. Returns the first match — paths are expected to be
 * unique per user / pipeline, but the route itself is global so we accept
 * the first hit. Returning `undefined` means "no secret configured" and the
 * route falls through to the Phase-1 "accept unsigned" behavior.
 *
 * Backed by `pipelineDefinitionsCache.getWebhookEntry()` — O(1) index built
 * on each cache refresh. Replaces the O(n) scan from earlier waves.
 *
 * Exported so the test suite can stub it out without coupling tests to the
 * full pipeline-definition upsert flow.
 */
export function lookupWebhookSecret(path: string): string | undefined {
  return pipelineDefinitionsCache.getWebhookEntry(path)?.secret;
}

/**
 * Resolve the `pipelineId` whose `triggerBinding.webhookPath` matches `path`.
 * Returns `undefined` when no pipeline definition is bound to that path —
 * the caller is free to fall back to the path itself (e.g. for legacy
 * unsigned dev fixtures) so the bridge.trigger call still has SOMETHING to
 * key on. First match wins (paths are expected unique across the user
 * surface).
 *
 * Backed by `pipelineDefinitionsCache.getWebhookEntry()` — O(1) index built
 * on each cache refresh. Replaces the O(n) scan from earlier waves.
 *
 * Exported for tests so they can assert resolution without seeding the full
 * upsert flow.
 */
export function lookupPipelineIdByWebhookPath(
  path: string,
): string | undefined {
  return pipelineDefinitionsCache.getWebhookEntry(path)?.pipelineId;
}

// ---------------------------------------------------------------------------
// Raw-body capture
// ---------------------------------------------------------------------------
//
// `express.raw` short-circuits the upstream `express.json()` because Express
// only invokes one body parser per request — the first one whose `type`
// predicate matches. Mounting it at the router level scopes that override
// to /hooks/pipeline only; the rest of the API still gets parsed JSON.
// `type: '*/*'` accepts whatever Content-Type the caller sends so signed
// non-JSON payloads (form-encoded, plain text, etc.) still verify.
const captureRawBody = express.raw({
  type: '*/*',
  limit: MAX_BODY_BYTES,
});

/**
 * Parse the raw body buffer as JSON. Returns `{}` for empty bodies and the
 * parsed value for valid JSON; non-JSON payloads are surfaced as a string
 * under `__raw` so downstream pipeline context still has *something* to act
 * on without lying about its shape.
 */
function parseBody(raw: Buffer): unknown {
  if (!raw || raw.length === 0) return {};
  const text = raw.toString('utf8');
  try {
    return JSON.parse(text);
  } catch {
    return { __raw: text };
  }
}

// POST /hooks/pipeline/:path
pipelineWebhooksRouter.post(
  '/:path',
  captureRawBody,
  async (req: Request, res: Response) => {
    const path = req.params.path;
    if (!VALID_PATH_RE.test(path)) {
      return res
        .status(400)
        .json({ error: 'invalid path: alphanumeric, _, -, max 64 chars' });
    }

    // `express.raw` puts a Buffer on `req.body` (or `{}` if no body was sent
    // — we tolerate both). Coerce to a Buffer for the HMAC step.
    const rawBody: Buffer = Buffer.isBuffer(req.body)
      ? req.body
      : Buffer.alloc(0);

    // Signature verification — only when a secret is configured for the
    // matched path. No secret = legacy "accept unsigned" behavior so Phase-1
    // dev fixtures keep working.
    const secret = lookupWebhookSecret(path);
    if (secret) {
      const headerRaw = req.headers[SIGNATURE_HEADER];
      const header = Array.isArray(headerRaw) ? headerRaw[0] : headerRaw;
      const ok = verifySignature(secret, rawBody, header);
      if (!ok) {
        // Structured log so observability can alert on forged or
        // misconfigured webhook sources without grepping free-form text.
        log.error(
          {
            webhookPath: path,
            hasHeader: typeof header === 'string' && header.length > 0,
            bodySize: rawBody.length,
          },
          'pipeline-webhook signature verification failed',
        );
        return res.status(401).json({
          // RFC 7807 problem-details — `type` + `title` + `status` are the
          // mandatory fields; `detail` carries the human-readable reason.
          type: 'about:blank',
          title: 'Invalid webhook signature',
          status: 401,
          detail:
            typeof header === 'string' && header.length > 0
              ? 'X-Pipeline-Signature-256 did not match the expected HMAC-SHA256 of the request body.'
              : 'X-Pipeline-Signature-256 header is required when a webhook secret is configured.',
        });
      }

      // hub#1260 — replay-window guard. Paired with HMAC: only enforced
      // when a secret is configured (signed traffic), so unsigned dev
      // fixtures continue to work. A captured signed body can otherwise
      // be replayed indefinitely.
      const replay = checkReplayWindow(req.headers[WEBHOOK_TIMESTAMP_HEADER]);
      if (!replay.ok) {
        log.error(
          { webhookPath: path, reason: replay.reason },
          'pipeline-webhook replay-window check failed',
        );
        return res.status(401).json({
          type: 'about:blank',
          title: 'Invalid webhook timestamp',
          status: 401,
          detail: replay.reason,
        });
      }
    }

    // Forward only `x-*` headers (and never the signature header) so we
    // don't leak Authorization / Cookie etc. into pipeline context.
    const safeHeaders = pickSafeHeaders(
      req.headers as Record<string, string | string[] | undefined>,
    );

    const parsedBody = parseBody(rawBody);

    const payload = {
      webhookPath: path,
      body: parsedBody,
      headers: safeHeaders,
      at: new Date().toISOString(),
    };

    log.info(
      {
        path,
        bodySize: rawBody.length,
        signed: !!secret,
      },
      'pipeline-webhook received',
    );

    // Phase 4: forward to the PipelineModule bridge. Returning 202 without
    // creating a run was the silent-drop bug from the audit — external
    // systems would think they triggered a pipeline while nothing actually
    // ran. We now resolve the bridge and either create a run, fail loudly,
    // or report the subsystem as unavailable (503).
    const bridge = getPipelineBridge();
    if (!bridge || !bridge.trigger) {
      // Surface the unavailability as a pipeline error metric so dashboards
      // can alert on a missing bridge. No audit write here — there's no
      // runId (and no pipeline state change) to attach the event to.
      recordPipelineError();
      return res.status(503).json({
        accepted: false,
        error: 'pipeline subsystem unavailable',
      });
    }

    // Prefer the pipelineId bound to this webhook path; fall back to the
    // path itself when no definition is registered (matches the "no secret
    // configured" legacy unsigned mode and lets dev fixtures keep working).
    const resolvedPipelineId =
      lookupPipelineIdByWebhookPath(path) ?? path;

    // The webhook route is unauthenticated by design — there's no Cognito
    // sub to attribute the run to. Synthesize a deterministic actor id so
    // downstream audit trails can distinguish webhook-driven runs from
    // user-driven ones (and from each other, by webhook path).
    const actorUserId = `webhook:${path}`;

    try {
      const out = await bridge.trigger({
        pipelineId: resolvedPipelineId,
        triggerPayload: payload,
        triggeredBy: { userId: actorUserId },
      });

      // Increment the trigger counter synchronously — it's an in-process
      // Prometheus counter so there's no I/O cost, and we want the metric
      // to reflect reality before any audit-side hiccup can mask it.
      recordPipelineTrigger();

      // Audit write is fire-and-forget: the run has already been created
      // upstream, the response shape is contractually fixed, and a slow or
      // failing DynamoDB write must NOT delay (or fail) the 202 we owe the
      // caller. A `.catch` keeps unhandled-rejection noise out of the test
      // runner and surfaces failures via the structured logger so ops can
      // still alert on audit-table outages.
      auditRepo
        .record({
          action: 'pipeline.webhook',
          actorUserId: 'webhook:' + path,
          pipelineId: resolvedPipelineId,
          runId: out.runId,
          details: { webhookPath: path, payloadAt: payload.at },
        })
        .catch((auditErr: Error) =>
          log.error(
            { err: auditErr.message },
            'audit write failed',
          ),
        );

      return res.status(202).json({
        accepted: true,
        runId: out.runId,
        webhookPath: path,
        at: payload.at,
      });
    } catch (err) {
      const message =
        err instanceof Error ? err.message : 'pipeline trigger failed';
      log.error(
        {
          webhookPath: path,
          pipelineId: resolvedPipelineId,
          error: message,
        },
        'pipeline-webhook bridge.trigger failed',
      );

      // Same fire-and-forget rationale as the success path: the response
      // shape is contractually fixed and we don't want a downstream audit
      // hiccup to compound the bridge failure we're already reporting.
      recordPipelineError();
      auditRepo
        .record({
          action: 'pipeline.webhook',
          actorUserId: 'webhook:' + path,
          pipelineId: resolvedPipelineId,
          decision: 'failed',
          details: { error: message },
        })
        .catch((auditErr: Error) =>
          log.error(
            { err: auditErr.message },
            'audit write failed',
          ),
        );

      return res.status(500).json({
        accepted: false,
        error: message,
      });
    }
  },
);
