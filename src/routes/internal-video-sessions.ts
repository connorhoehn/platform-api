// routes/internal-video-sessions.ts
//
// The gateway tells platform-api what it saw on the sockets of a document call
// (realtime-examples docs/design/document-calls). A tab closed without Leave
// never calls /sessions/:id/end, so without this the row stayed `active` until
// the stale-session reaper (~5 min+) and the document head offered
// "Join · 1" into a call with nobody in it.
//
//   POST /internal/video-sessions/leave { lobbyName, sessionId, userId }
//     after the rejoin grace, that person is gone: drop them from the roster.
//   POST /internal/video-sessions/end   { lobbyName, sessionId, reason }
//     nobody is connected any more: the call is over.
//
// Both are idempotent (the leaving socket may be on either replica; the
// replica holding the grace timer or the sweep leader sends once, a repeat is
// harmless), and apply only to `kind: 'document-review'` rows — DMs, rooms and
// the global hangout keep their own lifecycle. Service-signed like
// /internal/pipeline-access; the caller is the gateway.

import { Router, type Request, type Response } from 'express';
import { SERVICE_AUTH_HEADER, verifyEnvelope } from '@connorhoehn/service-runtime';
import { videoSessionRepo } from '../repositories';
import type { VideoSessionStore } from '../repositories/VideoSessionStore';
import log from '../lib/logger';

const ID = /^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,199}$/;

function authenticated(req: Request, res: Response): boolean {
  const secret = process.env.SERVICE_AUTH_SECRET?.trim();
  const header = req.header(SERVICE_AUTH_HEADER);
  if (!secret || !header) {
    res.status(401).json({ error: 'SERVICE_AUTH_REQUIRED' });
    return false;
  }
  const result = verifyEnvelope(header, {
    secret,
    allowedServices: [process.env.WORK_GRAPH_GATEWAY_SERVICE_ID?.trim() || 'realtime-examples'],
    payload: req.body as object,
    ttlSec: 30,
    toleranceSec: 5,
  });
  if (!result.ok) {
    res.status(401).json({ error: 'SERVICE_AUTH_INVALID', reason: result.reason });
    return false;
  }
  return true;
}

function ids(body: Record<string, unknown>, ...keys: string[]): Record<string, string> | null {
  const out: Record<string, string> = {};
  for (const k of keys) {
    const v = body[k];
    if (typeof v !== 'string' || !ID.test(v)) return null;
    out[k] = v;
  }
  return out;
}

export function createInternalVideoSessionsRouter(store: VideoSessionStore = videoSessionRepo): Router {
  const router = Router();

  router.post('/leave', async (req: Request, res: Response): Promise<void> => {
    if (!authenticated(req, res)) return;
    const p = ids((req.body ?? {}) as Record<string, unknown>, 'lobbyName', 'sessionId', 'userId');
    if (!p) { res.status(400).json({ error: 'lobbyName, sessionId and userId are required' }); return; }
    try {
      const session = await store.getSession(p.lobbyName, p.sessionId);
      if (!session) { res.status(404).json({ error: 'session not found' }); return; }
      if (session.kind !== 'document-review') { res.status(200).json({ skipped: 'not a document call' }); return; }
      if (session.status !== 'active') { res.status(200).json({ remaining: 0, ended: true }); return; }
      const out = await store.removeParticipantByUserId(p.lobbyName, p.sessionId, p.userId);
      log.info({ ...p, remaining: out?.remaining }, '[internal-video-sessions] participant left (gateway)');
      res.status(200).json({ remaining: out?.remaining ?? 0, ended: false });
    } catch (err) {
      log.error({ err, ...p }, '[internal-video-sessions] leave failed');
      res.status(500).json({ error: 'leave failed' });
    }
  });

  router.post('/end', async (req: Request, res: Response): Promise<void> => {
    if (!authenticated(req, res)) return;
    const body = (req.body ?? {}) as Record<string, unknown>;
    const p = ids(body, 'lobbyName', 'sessionId');
    if (!p) { res.status(400).json({ error: 'lobbyName and sessionId are required' }); return; }
    const reason = typeof body.reason === 'string' ? body.reason.slice(0, 64) : 'gateway';
    try {
      const session = await store.getSession(p.lobbyName, p.sessionId);
      if (!session) { res.status(404).json({ error: 'session not found' }); return; }
      if (session.kind !== 'document-review') { res.status(200).json({ skipped: 'not a document call' }); return; }
      if (session.status !== 'active') { res.status(200).json({ ended: true, already: true }); return; }
      await store.endSession(p.lobbyName, p.sessionId, new Date().toISOString());
      log.info({ ...p, reason }, '[internal-video-sessions] document call ended (gateway: nobody connected)');
      res.status(200).json({ ended: true, already: false });
    } catch (err) {
      log.error({ err, ...p }, '[internal-video-sessions] end failed');
      res.status(500).json({ error: 'end failed' });
    }
  });

  return router;
}

export const internalVideoSessionsRouter = createInternalVideoSessionsRouter();
