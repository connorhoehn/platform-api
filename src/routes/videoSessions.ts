/**
 * Video session routes — composes live-video-streaming primitive APIs
 * (channels + participant tokens) into a "sessions" concept for the frontend.
 *
 * live-video-streaming exposes:
 *   POST /api/channels → { arn, name, streamKey, ingestEndpoint, playbackUrl }
 *   POST /api/channels/:arn/participant-tokens (Bearer: streamKey) → { token, participantId, expiresAt }
 *   DELETE /api/channels/:arn → teardown (we DON'T call this anymore — the SFU's
 *     admin channel reaper handles cleanup based on actual liveness, see D3 in
 *     docs/HANGOUT-DESIGN.md). Leaving channels alive on /end means users who
 *     rejoin within the reaper grace window (~5 min) land in the same call.
 *
 * platform-api persists VideoSessionRecords in DynamoDB so the frontend can
 * find-or-create by lobby name, support multi-user joins, and ref-count
 * participants for the soft-end semantics.
 *
 * Naming: the DDB partition key is still `documentId` for back-compat, but
 * conceptually it's now a **lobbyName** — any string the consumer wants to
 * use to dedupe joiners (`doc:<id>` for doc-scoped calls, `dm:<userA>-<userB>`
 * for 1:1, `global` for the hangout overlay, etc.). The API accepts both
 * fields on input; pick one.
 *
 * Resilience (P1 #4): every outbound call to the SFU goes through a shared
 * CircuitBreaker + per-request AbortSignal timeout. Without these, an LVS
 * outage that hangs requests would queue inbound work until the process
 * OOMs — this is the last critical multi-replica risk after Phase 54.6.5.
 */

import { Router, Request, Response } from 'express';
import { randomUUID } from 'crypto';
import { requireAuth, optionalAuth } from '../middleware/auth';
import { VIDEO_API_URL } from '../lib/video-auth';
import { videoSessionRepo } from '../repositories';
import { VideoSessionRecord } from '../repositories/VideoSessionRepository';
import {
  asyncHandler,
  AppError,
  ValidationError,
  NotFoundError,
} from '../middleware/error-handler';
import log from '../lib/logger';
import { callSfu, mapSfuError } from '../lib/sfu-breaker';
import {
  recordLvsParticipantReap,
  type LvsReapOutcome,
} from '../observability/metrics';
// Re-export breaker test helpers from the shared module for back-compat
// with existing test imports — see src/lib/sfu-breaker.ts for the canonical
// definitions (extracted for hub task #1001).
export {
  __resetSfuBreakerForTests,
  __configureSfuBreakerForTests,
} from '../lib/sfu-breaker';

export const videoSessionsRouter = Router();

// --- helpers --------------------------------------------------------------

/**
 * Resolve the lobby-name / documentId from the request body, accepting either
 * field. Returns the canonical string used as the DDB partition key.
 */
function resolveLobbyName(body: Record<string, unknown>): string | undefined {
  const raw = body.lobbyName ?? body.documentId;
  return typeof raw === 'string' && raw.length > 0 ? raw : undefined;
}

/**
 * Reap one LVS participant on /end (hub#1281). LVS's
 * `DELETE /api/channels/:arn/participants/:participantId` is idempotent
 * (404 on second call) and reaps the `${pid}:screen` sibling server-side.
 * One retry on network/5xx because end-call is a high-leverage flow:
 * failing it triggers ICE-consent-timeout fallback (~30s) which has caused
 * rapid-reconnect quota lockout. Best-effort overall — the worst case is
 * we lose the fast-path and fall back to the periodic reaper sweep.
 */
export async function reapLvsParticipant(args: {
  channelArn: string;
  streamKey: string;
  participantId: string;
  sessionId: string;
}): Promise<LvsReapOutcome> {
  const url = `${VIDEO_API_URL}/api/channels/${encodeURIComponent(args.channelArn)}/participants/${encodeURIComponent(args.participantId)}`;
  const init: RequestInit = {
    method: 'DELETE',
    headers: { Authorization: `Bearer ${args.streamKey}` },
  };

  for (let attempt = 1; attempt <= 2; attempt += 1) {
    try {
      const res = await callSfu(url, init);
      if (res.ok) {
        log.info(
          { sessionId: args.sessionId, participantId: args.participantId, attempt },
          '[videoSessions] LVS participant reap ok',
        );
        return 'reaped';
      }
      if (res.status === 404) {
        log.info(
          { sessionId: args.sessionId, participantId: args.participantId },
          '[videoSessions] LVS participant reap 404 (already gone)',
        );
        return 'not_found';
      }
      // 4xx (non-404) is a client-shape failure that retry won't fix.
      if (res.status < 500) {
        log.warn(
          { sessionId: args.sessionId, participantId: args.participantId, status: res.status },
          '[videoSessions] LVS participant reap non-retriable 4xx',
        );
        return 'error';
      }
      // 5xx: retry once before giving up.
      log.warn(
        { sessionId: args.sessionId, participantId: args.participantId, status: res.status, attempt },
        '[videoSessions] LVS participant reap 5xx — retrying',
      );
    } catch (reapErr) {
      // Network / breaker / timeout. Retry once.
      log.warn(
        { err: reapErr, sessionId: args.sessionId, participantId: args.participantId, attempt },
        '[videoSessions] LVS participant reap threw — retrying',
      );
    }
  }
  log.warn(
    { sessionId: args.sessionId, participantId: args.participantId },
    '[videoSessions] LVS participant reap failed after retry — relying on SFU reaper',
  );
  return 'error';
}

/**
 * HEAD the SFU to confirm the channel still exists. Returns true if 2xx.
 * Used by findOrCreate to detect sessions whose channel was reaped under us.
 *
 * Any breaker rejection / timeout collapses to "channel is not alive", so
 * findOrCreate falls through to the create path. The create path then
 * surfaces the real error to the client, instead of leaking stale records.
 */
async function sfuChannelIsAlive(channelArn: string): Promise<boolean> {
  try {
    const res = await callSfu(
      `${VIDEO_API_URL}/api/channels/${encodeURIComponent(channelArn)}`,
      { method: 'GET' },
    );
    return res.ok;
  } catch (_e) {
    return false;
  }
}

// POST /api/video/sessions — find-or-create a session for a lobby
videoSessionsRouter.post('/sessions', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  try {
    const body = (req.body ?? {}) as Record<string, unknown>;
    const lobbyName = resolveLobbyName(body) ?? randomUUID();
    // Note: displayName is not read at create-time — participants (and their
    // display names) are added via /join. Accepting it here used to look
    // intentional but the value was dropped; removed to avoid hinting at a
    // contract we don't honor.
    const findOrCreate = body.findOrCreate === true;

    // findOrCreate path: return existing active session for this lobby if its
    // SFU channel is still alive. Self-heals against reaped channels by
    // falling through to the create path when HEAD says 404.
    if (findOrCreate) {
      const existing = await videoSessionRepo.findActiveByLobbyName(lobbyName);
      if (existing && await sfuChannelIsAlive(existing.channelArn)) {
        log.info({ sessionId: existing.sessionId, lobbyName }, '[videoSessions] findOrCreate hit');
        res.status(200).json({ sessionId: existing.sessionId, reused: true });
        return;
      }
    } else {
      // Non-findOrCreate path: explicit create. If an active session
      // already exists for this lobby, log a warning so duplicate-create
      // bugs in client code (e.g. a hangout overlay that POSTs on every
      // mount) are visible. We still proceed with the create — the
      // caller asked for it — but the warning helps correlate orphaned
      // participants[] arrays with the FE call that produced them.
      const existing = await videoSessionRepo.findActiveByLobbyName(lobbyName);
      if (existing) {
        log.warn({
          lobbyName,
          existingSessionId: existing.sessionId,
          existingChannelArn: existing.channelArn,
          participantCount: (existing.participants ?? []).length,
        }, '[videoSessions] creating new session while an active one exists for this lobby — possible duplicate-create');
      }
    }

    // The SFU gates POST /api/channels behind adminAuth (added in the
    // recent live-video-streaming reconciliation). Platform-api is the only
    // trusted in-cluster caller, so we send a Bearer ADMIN_TOKEN. The token
    // is provisioned via `hack/genadmin.sh` in live-video-streaming and
    // injected into our pod from the `admin-token` Secret. Falls back to
    // literal 'admin' which matches the SFU's `||'admin'` default.
    const adminToken = process.env.VIDEO_ADMIN_TOKEN || 'admin';
    // recordingConfiguration honors the caller's intent — hangoutRooms.ts's
    // /join route forwards `room.settings.recordingEnabled` as
    // body.recordingConfiguration; DM/global calls default to enabled
    // (preserves the legacy "always record" behavior that powers the
    // Past Calls page). Pass through only if explicit; otherwise default.
    const incomingRecordingConfig = (body.recordingConfiguration && typeof body.recordingConfiguration === 'object')
      ? (body.recordingConfiguration as { enabled?: boolean; destination?: string })
      : null;
    const recordingConfiguration = incomingRecordingConfig
      ? {
          enabled: incomingRecordingConfig.enabled !== false,
          destination: incomingRecordingConfig.destination ?? 'local',
        }
      : { enabled: true, destination: 'local' };
    let channelRes: globalThis.Response;
    try {
      channelRes = await callSfu(`${VIDEO_API_URL}/api/channels`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${adminToken}`,
        },
        body: JSON.stringify({
          name: lobbyName,
          recordingConfiguration,
        }),
      });
    } catch (sfuErr) {
      throw mapSfuError(sfuErr);
    }

    if (!channelRes.ok) {
      const errorBody = await channelRes.text().catch(() => 'Unknown error');
      log.error({ status: channelRes.status, body: errorBody }, '[videoSessions] create channel failed');
      throw new AppError(channelRes.status, `Video API error: ${channelRes.status}`);
    }

    const channelData = await channelRes.json() as Record<string, unknown>;
    const channelArn = channelData.arn as string;
    const streamKey = channelData.streamKey as string;

    const sessionId = randomUUID();
    const userId = req.user!.sub;
    const now = new Date().toISOString();

    const record: VideoSessionRecord = {
      documentId: lobbyName,
      sessionId,
      channelArn,
      streamKey,
      status: 'active',
      startedAt: now,
      startedBy: userId,
      // Don't pre-seed participants[] on create — they get added via /join.
      // Pre-seeding without a per-tab participantId would make /end (keyed
      // by participantId) unable to clean up the seed entry → orphan
      // participant blocks "last out" detection.
      participants: [],
    };

    try {
      await videoSessionRepo.createSession(record);
      log.info({ sessionId, lobbyName, channelArn }, '[videoSessions] Created session');
    } catch (dbErr) {
      // Log but don't fail — the channel was created successfully
      log.error({ err: dbErr }, '[videoSessions] Failed to persist local record');
    }

    // P0 — write arn → session reverse index to Redis so the
    // recording.completed webhook handler can resolve channelArn back
    // to (sessionId, lobbyName). LVS recording metadata doesn't carry
    // these; without this index, every recording persists under a
    // synthetic 'unknown-<arn>-<recordingId>' callId.
    //
    // We log explicitly when Redis is unavailable (vs. when the write
    // throws) — previously the "redis null" branch fell through silently
    // and the user only saw "recordings persisted under unknown-<arn>"
    // with no log breadcrumb pointing back to a missing Redis. The two
    // failure modes have different remediations (restart Redis vs. fix
    // the call site) so they need different log lines.
    try {
      const { getRedisClient } = await import('../lib/redis-client.js');
      const redis = await getRedisClient();
      if (!redis) {
        log.warn({ channelArn, sessionId, lobbyName }, '[videoSessions] arn-index write skipped — Redis unavailable; recording.completed will fall back to unknown-<arn> callIds');
      } else {
        const indexKey = `video:arn-index:${channelArn}`;
        await redis.set(
          indexKey,
          JSON.stringify({ sessionId, lobbyName, createdAt: now }),
          { EX: 7 * 24 * 60 * 60 }, // 7 days — covers webhook arrival + retries
        );
        log.debug({ channelArn, sessionId, lobbyName }, '[videoSessions] arn-index written');
      }
    } catch (idxErr) {
      log.warn({ err: idxErr, channelArn }, '[videoSessions] arn-index write failed (recording lookup may degrade)');
    }

    res.status(201).json({ sessionId, reused: false });
  } catch (err) {
    if (err instanceof AppError) throw err;
    log.error({ err }, '[videoSessions] POST /sessions error');
    throw new AppError(500, 'Failed to create video session');
  }
}));

// POST /api/video/sessions/:sessionId/join — mint a participant token + (idempotent) add to participants
videoSessionsRouter.post('/sessions/:sessionId/join', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  try {
    const { sessionId } = req.params;
    const body = (req.body ?? {}) as Record<string, unknown>;
    const lobbyName = resolveLobbyName(body);
    const displayName = typeof body.displayName === 'string' ? body.displayName : undefined;

    if (!lobbyName) {
      throw new ValidationError('lobbyName (or documentId) is required in request body');
    }

    const session = await videoSessionRepo.getSession(lobbyName, sessionId);
    if (!session) {
      throw new NotFoundError('Session not found');
    }

    let tokenRes: globalThis.Response;
    try {
      tokenRes = await callSfu(
        `${VIDEO_API_URL}/api/channels/${encodeURIComponent(session.channelArn)}/participant-tokens`,
        {
          method: 'POST',
          headers: {
            Authorization: `Bearer ${session.streamKey}`,
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({ role: 'publisher', ttlSeconds: 3600 }),
        },
      );
    } catch (sfuErr) {
      throw mapSfuError(sfuErr);
    }

    if (!tokenRes.ok) {
      const errorBody = await tokenRes.text().catch(() => 'Unknown error');
      log.error({ status: tokenRes.status, body: errorBody }, '[videoSessions] participant-token request failed');
      throw new AppError(tokenRes.status, `Video API error: ${tokenRes.status}`);
    }

    const tokenData = await tokenRes.json() as Record<string, unknown>;

    // Per-tab participant: key participants[] by the SFU-returned participantId,
    // not userId. Multiple tabs of the same user (real in prod, every tab in
    // local-dev SKIP_AUTH=true) each get their own slot so individual unloads
    // ref-count correctly. Dedup against participantId to keep /join idempotent.
    const userId = req.user!.sub;
    const participantId = (tokenData.participantId as string) || randomUUID();
    const already = (session.participants ?? []).some(p => p.participantId === participantId);
    if (!already) {
      const now = new Date().toISOString();
      try {
        await videoSessionRepo.addParticipant(lobbyName, sessionId, {
          userId,
          participantId,
          displayName: displayName || userId,
          joinedAt: now,
        });
        log.info({ userId, participantId, sessionId }, '[videoSessions] Added participant to session');
      } catch (dbErr) {
        log.error({ err: dbErr }, '[videoSessions] Failed to add participant to local record');
      }
    }

    res.status(200).json({
      token: tokenData.token as string,
      participantId,
      userId,
    });
  } catch (err) {
    if (err instanceof AppError) throw err;
    log.error({ err }, '[videoSessions] POST /sessions/:id/join error');
    throw new AppError(500, 'Failed to join video session');
  }
}));

// POST /api/video/sessions/:sessionId/end — ref-counted leave
// Uses optionalAuth so sendBeacon (no auth header) works on tab close.
//
// Behavior:
//  - Caller's userId is spliced out of participants[].
//  - If participants[] is still non-empty: just persist, return 200. The SFU
//    channel stays up so other users keep their connections.
//  - If participants[] becomes empty: mark session status='ended' in DDB but
//    DON'T call DELETE on the SFU channel — the channel reaper sweeps idle
//    channels on its own cadence (currently every 5 min for olderThanSec=300),
//    which gives "rejoin within grace window = same channel" UX.
videoSessionsRouter.post('/sessions/:sessionId/end', optionalAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  try {
    const { sessionId } = req.params;
    const body = (req.body ?? {}) as Record<string, unknown>;
    const lobbyName = resolveLobbyName(body);

    if (!lobbyName) {
      throw new ValidationError('lobbyName (or documentId) is required in request body');
    }

    const session = await videoSessionRepo.getSession(lobbyName, sessionId);
    if (!session) {
      throw new NotFoundError('Session not found');
    }

    // Identify caller's per-tab participant. participantId is the key now
    // (see /join); fall back to userId-keyed removal only if the FE hasn't
    // sent participantId yet (older clients).
    const participantId = typeof body.participantId === 'string' ? body.participantId : undefined;
    const userId =
      req.user?.sub
      ?? (typeof body.userId === 'string' ? body.userId : undefined);

    let remaining = (session.participants ?? []).length;
    if (participantId) {
      const splice = await videoSessionRepo.removeParticipant(lobbyName, sessionId, participantId);
      if (splice) remaining = splice.remaining;

      // Fast-path participant teardown — call LVS's explicit reap endpoint
      // so WHIP/WHEP transports close in milliseconds rather than waiting
      // for ICE consent timeout (~10-30s). LVS endpoint is idempotent (404
      // on second call, e.g. when the FE fires both UI-button + sendBeacon)
      // and reaps the screen-share sibling pid automatically. Best-effort
      // with one retry on transport/5xx (hub#1281); failures collapse to
      // the reaper's eventual sweep so /end still returns 200 to the FE.
      const reapOutcome = await reapLvsParticipant({
        channelArn: session.channelArn,
        streamKey: session.streamKey,
        participantId,
        sessionId,
      });
      recordLvsParticipantReap(reapOutcome);
    } else if (userId) {
      // Legacy path — remove ALL participants matching userId. Kept for
      // backwards-compat with old client builds. New clients always send
      // participantId so this branch is dead in steady-state.
      const splice = await videoSessionRepo.removeParticipantByUserId(lobbyName, sessionId, userId);
      if (splice) remaining = splice.remaining;
    }

    if (remaining > 0) {
      log.info({ userId: userId ?? '(anon)', sessionId, remaining }, '[videoSessions] participant left session');
      res.status(200).json({ remaining, ended: false });
      return;
    }

    // Last participant out — mark session ended. Channel cleanup is the
    // reaper's job (see file header).
    const now = new Date().toISOString();
    try {
      await videoSessionRepo.endSession(lobbyName, sessionId, now);
      log.info({ sessionId }, '[videoSessions] Ended session (last participant out)');
    } catch (dbErr) {
      log.error({ err: dbErr }, '[videoSessions] Failed to update local record on end');
    }

    res.status(200).json({ remaining: 0, ended: true });
  } catch (err) {
    if (err instanceof AppError) throw err;
    log.error({ err }, '[videoSessions] POST /sessions/:id/end error');
    throw new AppError(500, 'Failed to end video session');
  }
}));

// POST /api/video/sessions/:sessionId/refresh-token — rotate a participant's
// stage token mid-call. Thin proxy → LVS POST /api/sessions/:arn/refresh-token
// (hub#1249 third quarter — long-lived calls survive the 1h stage-token expiry
// without forcing a full WHIP/WHEP re-handshake).
//
// Body: { participantId, lobbyName | documentId, role? }
// Returns: { token, expiresAt, participantId, role }
//
// Forwards the caller's Cognito Bearer to LVS. LVS validates participant
// registration via its sessionStore (LOBBY_TAKEN / not-registered → 4xx).
videoSessionsRouter.post('/sessions/:sessionId/refresh-token', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  try {
    const { sessionId } = req.params;
    const body = (req.body ?? {}) as Record<string, unknown>;
    const lobbyName = resolveLobbyName(body);
    const participantId = typeof body.participantId === 'string' ? body.participantId : undefined;
    const role = typeof body.role === 'string' ? body.role : undefined;

    if (!lobbyName) {
      throw new ValidationError('lobbyName (or documentId) is required in request body');
    }
    if (!participantId) {
      throw new ValidationError('participantId is required in request body');
    }

    const session = await videoSessionRepo.getSession(lobbyName, sessionId);
    if (!session) {
      throw new NotFoundError('Session not found');
    }

    // LVS's sessionId === channelArn. Forward the caller's Cognito token so
    // LVS's cognitoAuth middleware validates the same identity that hit us.
    const authHeader = req.headers.authorization;
    if (!authHeader) {
      throw new AppError(401, 'Authorization header required');
    }

    let upstream: globalThis.Response;
    try {
      upstream = await callSfu(
        `${VIDEO_API_URL}/api/sessions/${encodeURIComponent(session.channelArn)}/refresh-token`,
        {
          method: 'POST',
          headers: {
            Authorization: authHeader,
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({ participantId, ...(role ? { role } : {}) }),
        },
      );
    } catch (sfuErr) {
      throw mapSfuError(sfuErr);
    }

    if (!upstream.ok) {
      // 404 (participant not registered / session not found) and 410
      // (session ended) propagate verbatim — they're actionable on the FE.
      if (upstream.status === 404 || upstream.status === 410) {
        const errBody = await upstream.json().catch(() => ({ error: 'unknown' }));
        res.status(upstream.status).json(errBody);
        return;
      }
      const errorBody = await upstream.text().catch(() => 'Unknown error');
      log.error({ status: upstream.status, body: errorBody, sessionId }, '[videoSessions] refresh-token upstream failed');
      throw new AppError(upstream.status, `LVS refresh-token upstream ${upstream.status}`);
    }

    const tokenData = await upstream.json() as {
      token: string; expiresAt: string; participantId: string; role: string;
    };

    // LVS sessionStore records lastRefreshAt server-side — we don't mirror
    // it into our DDB row. Past-calls UI reads its activity signal from
    // joinedAt / endedAt; mid-call token refreshes aren't part of that
    // surface today.

    log.info({ sessionId, participantId }, '[videoSessions] participant token refreshed');
    res.status(200).json(tokenData);
  } catch (err) {
    if (err instanceof AppError) throw err;
    log.error({ err }, '[videoSessions] POST /sessions/:id/refresh-token error');
    throw new AppError(500, 'Failed to refresh participant token');
  }
}));

// GET /api/video/sessions/document/:documentId — list all sessions for a lobby
videoSessionsRouter.get('/sessions/document/:documentId', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  try {
    const { documentId } = req.params;
    const sessions = await videoSessionRepo.getSessionsByDocument(documentId);
    res.status(200).json({ sessions });
  } catch (err) {
    if (err instanceof AppError) throw err;
    log.error({ err }, '[videoSessions] GET /sessions/document/:documentId error');
    throw new AppError(500, 'Failed to fetch video sessions');
  }
}));

// GET /api/video/sessions/:sessionId/recording — live recording state for the
// session's LVS channel. Proxies GET ${VIDEO_API_URL}/api/channels/:arn/recording.
//
// Returns: { isRecording, recordingId?, startedAt? } from LVS.
//
// Used by the gateway FE in-call REC pill (polled every 5s while joined).
// Public-read on the LVS side; we still require auth here because the
// proxy hop reveals which sessions exist in this tenant.
//
// Failure modes:
//  - 404 if the session record is unknown.
//  - LVS upstream failures translate via mapSfuError (504 timeout, 503
//    breaker-open, etc.). The FE treats any non-2xx as "unknown" and
//    leaves the pill hidden.
videoSessionsRouter.get('/sessions/:sessionId/recording', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  try {
    const { sessionId } = req.params;
    const lobbyName = (req.query.lobbyName ?? req.query.documentId) as string | undefined;
    if (!lobbyName) {
      throw new ValidationError('lobbyName (or documentId) query parameter is required');
    }

    const session = await videoSessionRepo.getSession(lobbyName, sessionId);
    if (!session) {
      throw new NotFoundError('Session not found');
    }

    let upstream: globalThis.Response;
    try {
      upstream = await callSfu(
        `${VIDEO_API_URL}/api/channels/${encodeURIComponent(session.channelArn)}/recording`,
        { method: 'GET' },
      );
    } catch (sfuErr) {
      throw mapSfuError(sfuErr);
    }

    if (!upstream.ok) {
      // 404 from LVS means RecordingManager has nothing for this arn —
      // treat as "not recording" rather than propagating the upstream 404.
      if (upstream.status === 404) {
        res.status(200).json({ isRecording: false });
        return;
      }
      throw new AppError(upstream.status, `LVS recording-state upstream ${upstream.status}`);
    }

    const body = await upstream.json() as { isRecording: boolean; recordingId?: string; startedAt?: string };
    res.status(200).json(body);
  } catch (err) {
    if (err instanceof AppError) throw err;
    log.error({ err }, '[videoSessions] GET /sessions/:sessionId/recording error');
    throw new AppError(500, 'Failed to fetch recording state');
  }
}));

// GET /api/video/sessions/:sessionId — get a single session record
// Requires lobbyName (or documentId) as a query param since it's the partition key.
videoSessionsRouter.get('/sessions/:sessionId', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  try {
    const { sessionId } = req.params;
    const lobbyName = (req.query.lobbyName ?? req.query.documentId) as string | undefined;

    if (!lobbyName) {
      throw new ValidationError('lobbyName (or documentId) query parameter is required');
    }

    const session = await videoSessionRepo.getSession(lobbyName, sessionId);
    if (!session) {
      throw new NotFoundError('Session not found');
    }

    res.status(200).json(session);
  } catch (err) {
    if (err instanceof AppError) throw err;
    log.error({ err }, '[videoSessions] GET /sessions/:sessionId error');
    throw new AppError(500, 'Failed to fetch video session');
  }
}));
