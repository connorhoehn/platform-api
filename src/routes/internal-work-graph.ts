import { Router, type Request, type Response } from 'express';
import { SERVICE_AUTH_HEADER, verifyEnvelope } from '@connorhoehn/service-runtime';
import {
  authorizeGatewayWorkGraph,
  replayGatewayWorkGraph,
  type GatewayWorkGraphQuery,
} from '../work-graph/runtime';
import { workGraphRepository } from '../work-graph/runtime';
import { workSourceAccountRepo } from '../repositories';
import {
  ingestAuthenticatedWorkEvent,
  WorkGraphIngestionDeniedError,
} from '../services/work-graph-consumer';

const router = Router();
const ID = /^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,127}$/;

function record(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function query(value: unknown): value is GatewayWorkGraphQuery {
  if (!record(value) || Object.keys(value).some((key) => !['personId', 'day', 'timezone'].includes(key))) return false;
  if (typeof value.personId !== 'string' || !ID.test(value.personId)
    || typeof value.day !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(value.day)
    || typeof value.timezone !== 'string' || value.timezone.length === 0 || value.timezone.length > 64) return false;
  try {
    new Intl.DateTimeFormat('en-US', { timeZone: value.timezone }).format();
    return true;
  } catch {
    return false;
  }
}

function authenticated(req: Request, res: Response, allowedServices: string[]): string | null {
  const secret = process.env.SERVICE_AUTH_SECRET?.trim();
  const header = req.header(SERVICE_AUTH_HEADER);
  if (!secret || !header) {
    res.status(401).json({ error: 'SERVICE_AUTH_REQUIRED' });
    return null;
  }
  const result = verifyEnvelope(header, {
    secret,
    allowedServices,
    payload: req.body as object,
    ttlSec: 30,
    toleranceSec: 5,
  });
  if (!result.ok) {
    res.status(401).json({ error: 'SERVICE_AUTH_INVALID', reason: result.reason });
    return null;
  }
  return result.serviceId;
}

function base(value: unknown): value is {
  organizationId: string;
  viewerId: string;
  query: GatewayWorkGraphQuery;
} & Record<string, unknown> {
  return record(value)
    && typeof value.organizationId === 'string' && ID.test(value.organizationId)
    && typeof value.viewerId === 'string' && ID.test(value.viewerId)
    && query(value.query);
}

type ReplayBody = {
  organizationId: string;
  viewerId: string;
  query: GatewayWorkGraphQuery;
  afterWatermark: number;
  throughWatermark: number;
  since: string;
  limit: number;
};

function replayBody(value: unknown): value is ReplayBody {
  return base(value)
    && Object.keys(value).every((key) => [
      'organizationId', 'viewerId', 'query', 'afterWatermark', 'throughWatermark', 'since', 'limit',
    ].includes(key))
    && Number.isSafeInteger(value.afterWatermark) && Number(value.afterWatermark) >= 0
    && Number.isSafeInteger(value.throughWatermark)
    && Number(value.throughWatermark) >= Number(value.afterWatermark)
    && Number.isSafeInteger(value.limit) && Number(value.limit) >= 1 && Number(value.limit) <= 1_001
    && typeof value.since === 'string' && Number.isFinite(Date.parse(value.since));
}

router.post('/authorize', async (req, res, next) => {
  try {
    if (!authenticated(req, res, [process.env.WORK_GRAPH_GATEWAY_SERVICE_ID?.trim() || 'realtime-examples'])) return;
    if (!base(req.body) || Object.keys(req.body).some((key) => !['organizationId', 'viewerId', 'query'].includes(key))) {
      res.status(400).json({ error: 'INVALID_REQUEST' });
      return;
    }
    res.status(200).json(await authorizeGatewayWorkGraph(
      req.body.organizationId,
      req.body.viewerId,
      req.body.query,
    ));
  } catch (error) {
    next(error);
  }
});

router.post('/replay', async (req, res, next) => {
  try {
    if (!authenticated(req, res, [process.env.WORK_GRAPH_GATEWAY_SERVICE_ID?.trim() || 'realtime-examples'])) return;
    if (!replayBody(req.body)) {
      res.status(400).json({ error: 'INVALID_REQUEST' });
      return;
    }
    res.status(200).json(await replayGatewayWorkGraph(
      req.body.organizationId,
      req.body.viewerId,
      req.body.query,
      {
        afterWatermark: Number(req.body.afterWatermark),
        throughWatermark: Number(req.body.throughWatermark),
        since: req.body.since,
        limit: Number(req.body.limit),
      },
    ));
  } catch (error) {
    next(error);
  }
});

router.post('/ingest', async (req, res, next) => {
  try {
    if (process.env.ACTIVE_NOW_WORK_GRAPH_ENABLED !== 'true') {
      res.status(503).json({ error: 'WORK_GRAPH_DISABLED' });
      return;
    }
    const serviceId = authenticated(req, res, [
      process.env.WORK_GRAPH_SOURCE_GATEWAY_SERVICE_ID?.trim() || 'websocket-gateway',
      process.env.WORK_GRAPH_SOURCE_COMPUTE_SERVICE_ID?.trim() || 'aws-agentcore',
    ]);
    if (!serviceId) return;
    const event = await ingestAuthenticatedWorkEvent({
      payload: req.body,
      authenticatedServiceId: serviceId,
      repository: workGraphRepository,
      accountResolver: workSourceAccountRepo,
    });
    res.status(202).json({ accepted: true, eventId: event.eventId });
  } catch (error) {
    if (error instanceof WorkGraphIngestionDeniedError) {
      const invalid = error.reason === 'invalid-payload' || error.reason === 'payload-too-large';
      res.status(invalid ? 400 : 403).json({ error: 'WORK_GRAPH_INGESTION_DENIED', reason: error.reason });
      return;
    }
    next(error);
  }
});

export { router as internalWorkGraphRouter };
