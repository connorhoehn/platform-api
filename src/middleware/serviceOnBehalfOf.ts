import type { Request, Response, NextFunction } from 'express';
import { SERVICE_AUTH_HEADER, verifyEnvelope } from '@connorhoehn/service-runtime';
import { withContext } from '../lib/logger';

// A run triggered over the gateway's WebSocket reaches POST
// /api/pipelines/:pipelineId/runs as a service call: the gateway holds the
// person's verified socket identity, not a bearer token it could forward. It
// used to send only its service signature, so requireAuth credited the run to
// `dev-user` with no organization and the run never got a work-graph node
// (NFR #72). Now the gateway names the person in `onBehalfOf` INSIDE the signed
// body; this verifies the signature, the calling service and the ids, and only
// then sets `req.user` — the same principal an HTTP trigger with that person's
// token would produce, so pinRequester and the source-account join see them.
//
// Mounted before requireAuth on the trigger route only. A request without the
// field (or without a service header) passes through untouched to requireAuth;
// a request that carries `onBehalfOf` but fails verification is refused — it
// must never fall back to another identity.

const ID = /^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,127}$/;
const log = withContext({ component: 'service-on-behalf-of' });

export function allowedOnBehalfOfServices(): string[] {
  return [process.env.WORK_GRAPH_GATEWAY_SERVICE_ID?.trim() || 'realtime-examples'];
}

export function serviceOnBehalfOf(req: Request, res: Response, next: NextFunction): void {
  const body = req.body as Record<string, unknown> | undefined;
  if (req.method !== 'POST' || !body || typeof body !== 'object' || !('onBehalfOf' in body)) {
    next();
    return;
  }
  const claim = body.onBehalfOf as { userId?: unknown; orgId?: unknown } | null;
  const secret = process.env.SERVICE_AUTH_SECRET?.trim();
  const header = req.header(SERVICE_AUTH_HEADER);
  if (!secret || !header) {
    log.warn({ path: req.originalUrl }, 'onBehalfOf without a service signature — refused');
    res.status(401).json({ error: 'SERVICE_AUTH_REQUIRED' });
    return;
  }
  const result = verifyEnvelope(header, {
    secret,
    allowedServices: allowedOnBehalfOfServices(),
    payload: body,
    ttlSec: 30,
    toleranceSec: 5,
  });
  if (!result.ok) {
    log.warn({ path: req.originalUrl, reason: result.reason }, 'onBehalfOf signature invalid — refused');
    res.status(401).json({ error: 'SERVICE_AUTH_INVALID', reason: result.reason });
    return;
  }
  const userId = claim && typeof claim === 'object' ? claim.userId : undefined;
  const orgId = claim && typeof claim === 'object' ? claim.orgId : undefined;
  if (typeof userId !== 'string' || !ID.test(userId)
    || (orgId !== undefined && orgId !== null && (typeof orgId !== 'string' || !ID.test(orgId)))) {
    res.status(400).json({ error: 'ON_BEHALF_OF_INVALID' });
    return;
  }
  req.user = { sub: userId, ...(typeof orgId === 'string' ? { orgId } : {}) };
  req.serviceActor = result.serviceId;
  // The claim is transport, not part of the trigger request.
  delete body.onBehalfOf;
  next();
}
