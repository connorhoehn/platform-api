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
import { videoSessionRepo, callReactionRepo } from '../repositories';
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
// Wave 3 / ADR-002 — recording profile validator. Reject unknown profile
// strings at the API boundary with a 400 rather than letting them fall
// through to LVS's resolveRecordingProfile silent-degrade-to-'none' path.
import { isRecordingProfile } from '@connorhoehn/event-catalog';
import { tenantOf } from '../lib/tenant';
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
 *
 * Auth: ADMIN_TOKEN. The endpoint accepts either streamKey or admin token;
 * post-Wave-7 thin-proxy migration the DDB row's `streamKey` is no longer
 * populated (LVS POST /api/sessions doesn't return it), so we use the
 * admin token unconditionally. `streamKey` arg retained for compat with
 * older callers / tests; ignored when present.
 */
export async function reapLvsParticipant(args: {
  channelArn: string;
  streamKey?: string;
  participantId: string;
  sessionId: string;
}): Promise<LvsReapOutcome> {
  const url = `${VIDEO_API_URL}/api/channels/${encodeURIComponent(args.channelArn)}/participants/${encodeURIComponent(args.participantId)}`;
  const adminToken = process.env.VIDEO_ADMIN_TOKEN || 'admin';
  const init: RequestInit = {
    method: 'DELETE',
    headers: { Authorization: `Bearer ${adminToken}` },
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

// POST /api/video/sessions — find-or-create a session for a lobby
//
// Thin proxy → LVS POST /api/sessions { lobbyName, findOrCreate, creatorId,
// recordingConfiguration }. LVS Redis SETNX is atomic, so concurrent finds
// for the same lobby converge on a single channel (previously a race
// between the platform-api findActiveByLobbyName + HEAD-probe + create).
//
// DDB mirror-write is best-effort and powers the past-calls UI; LVS
// sessionStore is the source of truth for "is this lobby live".
videoSessionsRouter.post('/sessions', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  try {
    const body = (req.body ?? {}) as Record<string, unknown>;
    const lobbyName = resolveLobbyName(body) ?? randomUUID();
    const findOrCreate = body.findOrCreate === true;
    const userId = req.user!.sub;

    // recordingConfiguration: hangoutRooms.ts's /join forwards
    // `room.settings.recordingEnabled`; DM/global calls default to enabled
    // to preserve the legacy "always record" behavior that powers Past
    // Calls. Pass through only if explicit; otherwise default.
    //
    // Wave 3 / ADR-002 — accept `recordingProfile` as the canonical
    // recording-shape identifier. Defaults to `'hangout'` when absent
    // (matches the prior implicit shape: per-peer compose + transcribe
    // for video-session calls). Legacy `transcribe` boolean is still
    // accepted on the way in for one release window; resolveRecordingProfile
    // synthesizes the right profile on the LVS side when both are
    // present.
    const incomingRecordingConfig =
      body.recordingConfiguration && typeof body.recordingConfiguration === 'object'
        ? (body.recordingConfiguration as {
            enabled?: boolean;
            destination?: string;
            recordingProfile?: string;
            transcribe?: boolean;
          })
        : null;
    const profileFromBody = (() => {
      const raw = incomingRecordingConfig?.recordingProfile;
      if (raw === undefined) return undefined;
      if (isRecordingProfile(raw)) return raw;
      throw new AppError(400, `invalid recordingProfile: '${String(raw)}'`);
    })();
    const recordingConfiguration = incomingRecordingConfig
      ? {
          enabled: incomingRecordingConfig.enabled !== false,
          destination: incomingRecordingConfig.destination ?? 'local',
          // Canonical Wave-3 identifier. Falls back to legacy transcribe
          // boolean → 'hangout' synthesis if FE hasn't been migrated yet.
          recordingProfile:
            profileFromBody ??
            (incomingRecordingConfig.transcribe === true ? 'hangout' : undefined),
          // Legacy field preserved for the one-release back-compat window.
          // LVS RecordingManager resolves the profile, ignores this if
          // recordingProfile is present.
          transcribe: incomingRecordingConfig.transcribe === true,
        }
      : // Body absent → preserve legacy default exactly. NO recordingProfile
        // field so LVS' resolveRecordingProfile falls through to legacy-flag
        // synthesis (which maps {enabled:true, transcribe:false} → broadcast
        // shape: composePerPeer=false, transcribe=false). New per-peer +
        // transcribe behavior is opt-in via recordingProfile from the FE.
        { enabled: true, destination: 'local', transcribe: false };

    // A 1:1 call must not start recording while the callee is still ringing.
    //
    // LVS starts recording on producer-create — the instant the FIRST person
    // publishes — which for a broadcast is exactly right and for a DM meant
    // every recording opened with the caller alone in dead air, with the
    // ringback bleeding back through the mic. LVS cannot tell a call from a
    // stream; we can, from the lobby.
    //
    // `dm:` only, deliberately. A DM has exactly two parties, so "wait for the
    // second" is unambiguous. A room or a broadcast can legitimately hold one
    // person who is worth recording, and LVS' default of 1 keeps them working
    // exactly as before.
    const recordingConfigurationForLobby = lobbyName.startsWith('dm:')
      ? { ...recordingConfiguration, startWhenParticipants: 2 }
      : recordingConfiguration;

    // Forward the caller's Cognito Bearer so LVS validates the same
    // identity that hit us. Without a header we can't proxy — platform-api
    // requireAuth ensures one is present.
    const authHeader = req.headers.authorization;
    if (!authHeader) {
      throw new AppError(401, 'Authorization header required');
    }

    let upstream: globalThis.Response;
    try {
      upstream = await callSfu(`${VIDEO_API_URL}/api/sessions`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: authHeader },
        body: JSON.stringify({
          lobbyName,
          findOrCreate,
          creatorId: userId,
          recordingConfiguration: recordingConfigurationForLobby,
          metadata: body.metadata,
        }),
      });
    } catch (sfuErr) {
      throw mapSfuError(sfuErr);
    }

    if (!upstream.ok) {
      // LVS 409 LOBBY_TAKEN — race lost between this caller's
      // findOrCreate-miss and another caller's claim. Surface verbatim
      // so the FE can retry with findOrCreate=true to pick up the winner.
      if (upstream.status === 409) {
        const errBody = await upstream.json().catch(() => ({ error: 'lobby taken' }));
        res.status(409).json(errBody);
        return;
      }
      const errorBody = await upstream.text().catch(() => 'Unknown error');
      log.error({ status: upstream.status, body: errorBody, lobbyName }, '[videoSessions] LVS POST /sessions failed');
      throw new AppError(upstream.status, `LVS POST /sessions ${upstream.status}`);
    }

    const lvsResult = await upstream.json() as {
      sessionId: string;
      channelArn: string;
      createdAt?: string;
      hostToken?: string;
      sessionFound?: boolean;
    };
    const channelArn = lvsResult.channelArn || lvsResult.sessionId;
    const reused = lvsResult.sessionFound === true;

    // findOrCreate hit: try to reuse the existing DDB row. If the
    // lobbyName has an active row for the same channelArn, return that
    // platform-api sessionId. Otherwise fall through and create a fresh
    // DDB row pointing at the LVS-owned channel (covers the case where
    // LVS reused the session but DDB never recorded it locally).
    if (reused) {
      const existing = await videoSessionRepo.findActiveByLobbyName(lobbyName);
      if (existing && existing.channelArn === channelArn) {
        log.info({ sessionId: existing.sessionId, lobbyName }, '[videoSessions] findOrCreate hit (LVS authoritative)');
        res.status(200).json({ sessionId: existing.sessionId, reused: true });
        return;
      }
    }

    // Either fresh create or reused-by-LVS-but-not-in-DDB. Write a fresh
    // DDB row. LVS doesn't return a streamKey on /api/sessions — past-calls
    // UI doesn't need it, and the reap path uses the user's Cognito JWT
    // (not streamKey) post-migration. Stub for back-compat with existing
    // VideoSessionRecord shape until the streamKey field can be dropped.
    const sessionId = randomUUID();
    const now = new Date().toISOString();
    const record: VideoSessionRecord = {
      documentId: lobbyName,
      sessionId,
      channelArn,
      streamKey: '',  // LVS no longer exposes; DDB field retained for shape compat
      status: 'active',
      startedAt: now,
      startedBy: userId,
      participants: [],
    };

    try {
      await videoSessionRepo.createSession(record);
      log.info({ sessionId, lobbyName, channelArn, reused }, '[videoSessions] DDB row written');
    } catch (dbErr) {
      // Log but don't fail — LVS already has the canonical session.
      log.error({ err: dbErr }, '[videoSessions] Failed to persist local record');
    }

    // arn-index for the recording.completed webhook → callId resolution.
    try {
      const { getRedisClient } = await import('../lib/redis-client.js');
      const redis = await getRedisClient();
      if (!redis) {
        log.warn({ channelArn, sessionId, lobbyName }, '[videoSessions] arn-index write skipped — Redis unavailable');
      } else {
        const indexKey = `video:arn-index:${channelArn}`;
        await redis.set(
          indexKey,
          JSON.stringify({ sessionId, lobbyName, createdAt: now }),
          { EX: 7 * 24 * 60 * 60 },
        );
      }
    } catch (idxErr) {
      log.warn({ err: idxErr, channelArn }, '[videoSessions] arn-index write failed');
    }

    res.status(reused ? 200 : 201).json({ sessionId, reused });
  } catch (err) {
    if (err instanceof AppError) throw err;
    log.error({ err }, '[videoSessions] POST /sessions error');
    throw new AppError(500, 'Failed to create video session');
  }
}));

// POST /api/video/sessions/:sessionId/join — thin proxy → LVS
// POST /api/sessions/:arn/join. LVS mints the participant token (with
// Wave-5 `sub` + `mid` claims) and registers the participant in its
// sessionStore. Platform-api mirrors the participant into DDB for
// past-calls history.
videoSessionsRouter.post('/sessions/:sessionId/join', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  try {
    const { sessionId } = req.params;
    const body = (req.body ?? {}) as Record<string, unknown>;
    const lobbyName = resolveLobbyName(body);
    const displayName = typeof body.displayName === 'string' ? body.displayName : undefined;
    const role = typeof body.role === 'string' ? body.role : 'publisher';

    if (!lobbyName) {
      throw new ValidationError('lobbyName (or documentId) is required in request body');
    }

    const session = await videoSessionRepo.getSession(lobbyName, sessionId);
    if (!session) {
      throw new NotFoundError('Session not found');
    }

    const userId = req.user!.sub;
    const authHeader = req.headers.authorization;
    if (!authHeader) {
      throw new AppError(401, 'Authorization header required');
    }

    let upstream: globalThis.Response;
    try {
      upstream = await callSfu(
        `${VIDEO_API_URL}/api/sessions/${encodeURIComponent(session.channelArn)}/join`,
        {
          method: 'POST',
          headers: { Authorization: authHeader, 'Content-Type': 'application/json' },
          body: JSON.stringify({ userId, displayName, role }),
        },
      );
    } catch (sfuErr) {
      throw mapSfuError(sfuErr);
    }

    if (!upstream.ok) {
      // 404 / 410 (session not found / ended) propagate verbatim — both
      // are actionable on the FE (clear stale local state on 410).
      if (upstream.status === 404 || upstream.status === 410) {
        const errBody = await upstream.json().catch(() => ({ error: 'unknown' }));
        res.status(upstream.status).json(errBody);
        return;
      }
      const errorBody = await upstream.text().catch(() => 'Unknown error');
      log.error({ status: upstream.status, body: errorBody, sessionId }, '[videoSessions] LVS /join upstream failed');
      throw new AppError(upstream.status, `LVS /join upstream ${upstream.status}`);
    }

    const tokenData = await upstream.json() as {
      token: string; websocketUrl?: string; expiresAt?: string;
      participantId: string; role: string; displayName?: string;
    };

    // Mirror to DDB for past-calls history. Best-effort, idempotent.
    const participantId = tokenData.participantId;
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
        log.info({ userId, participantId, sessionId }, '[videoSessions] Added participant to DDB');
      } catch (dbErr) {
        log.error({ err: dbErr }, '[videoSessions] Failed to add participant to local record');
      }
    }

    res.status(200).json({ token: tokenData.token, participantId, userId });
  } catch (err) {
    if (err instanceof AppError) throw err;
    log.error({ err }, '[videoSessions] POST /sessions/:id/join error');
    throw new AppError(500, 'Failed to join video session');
  }
}));

// POST /api/video/sessions/:sessionId/end — ref-counted leave
// Uses optionalAuth so sendBeacon (no auth header) works on tab close.
//
// Authoritative path: LVS sessionStore. Platform-api forwards to
// LVS POST /api/sessions/:arn/end { participantId } when a Cognito Bearer
// is present (the common case). LVS does the ref-count, returns
// `participantsRemaining` + ended:true/false. We mirror to DDB for the
// past-calls UI; DDB writes are best-effort.
//
// Fallback path (sendBeacon, no auth header): direct DDB splice + the
// LVS reap fast-path. LVS sessionStore drifts until the reaper sweeps —
// acceptable degraded mode for the tab-close case.
//
// The reapLvsParticipant() call (hub#1281 fast WHIP/WHEP transport close)
// fires in both paths — it's orthogonal to the session ref-count and is
// what gets the departed participant's transports down in ms instead of
// the ~10-30s ICE consent timeout.
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

    const participantId = typeof body.participantId === 'string' ? body.participantId : undefined;
    const userId =
      req.user?.sub
      ?? (typeof body.userId === 'string' ? body.userId : undefined);
    const authHeader = req.headers.authorization;

    // Fast-path reap — close the departed participant's WHIP/WHEP transports
    // in ms (hub#1281). Fires on both the authoritative and fallback paths.
    // Best-effort; failures collapse to the reaper's eventual sweep.
    if (participantId) {
      const reapOutcome = await reapLvsParticipant({
        channelArn: session.channelArn,
        participantId,
        sessionId,
      });
      recordLvsParticipantReap(reapOutcome);
    }

    // Authoritative path — forward to LVS sessionStore when we have a
    // user JWT to forward. LVS is the source of truth for participant
    // ref-count post Wave 4.
    if (participantId && authHeader) {
      let upstream: globalThis.Response;
      try {
        upstream = await callSfu(
          `${VIDEO_API_URL}/api/sessions/${encodeURIComponent(session.channelArn)}/end`,
          {
            method: 'POST',
            headers: { Authorization: authHeader, 'Content-Type': 'application/json' },
            body: JSON.stringify({ participantId }),
          },
        );
      } catch (sfuErr) {
        throw mapSfuError(sfuErr);
      }

      if (!upstream.ok && upstream.status !== 404) {
        // LVS 404 means "session already gone" — treat as hard-ended.
        // Any other non-2xx is a real upstream failure.
        const errorBody = await upstream.text().catch(() => 'Unknown error');
        log.error({ status: upstream.status, body: errorBody, sessionId }, '[videoSessions] LVS /end upstream failed');
        throw new AppError(upstream.status, `LVS /end upstream ${upstream.status}`);
      }

      const lvsResult = upstream.ok
        ? (await upstream.json() as { participantsRemaining?: number; endedAt?: string | null })
        : { participantsRemaining: 0, endedAt: new Date().toISOString() };
      const remaining = lvsResult.participantsRemaining ?? 0;
      const ended = remaining === 0;

      // Mirror to DDB for past-calls UI. Best-effort.
      try {
        if (ended) {
          await videoSessionRepo.endSession(lobbyName, sessionId, lvsResult.endedAt || new Date().toISOString());
        } else {
          await videoSessionRepo.removeParticipant(lobbyName, sessionId, participantId);
        }
      } catch (dbErr) {
        log.warn({ err: dbErr, sessionId }, '[videoSessions] DDB mirror on /end failed (non-fatal)');
      }

      log.info({ userId: userId ?? '(anon)', sessionId, remaining, ended }, '[videoSessions] /end forwarded to LVS');
      res.status(200).json({ remaining, ended });
      return;
    }

    // Fallback path: no auth (sendBeacon) or no participantId (legacy
    // body-userId only). DDB-direct ref-count. LVS sessionStore drifts
    // until the reaper sweep — acceptable for tab-close.
    let remaining = (session.participants ?? []).length;
    if (participantId) {
      const splice = await videoSessionRepo.removeParticipant(lobbyName, sessionId, participantId);
      if (splice) remaining = splice.remaining;
    } else if (userId) {
      const splice = await videoSessionRepo.removeParticipantByUserId(lobbyName, sessionId, userId);
      if (splice) remaining = splice.remaining;
    }

    if (remaining > 0) {
      log.info({ userId: userId ?? '(anon)', sessionId, remaining }, '[videoSessions] participant left session (fallback)');
      res.status(200).json({ remaining, ended: false });
      return;
    }

    const now = new Date().toISOString();
    try {
      await videoSessionRepo.endSession(lobbyName, sessionId, now);
      log.info({ sessionId }, '[videoSessions] Ended session (fallback last-out)');
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
// ---------------------------------------------------------------------------
// GET /api/video/sessions/by-lobby/:lobbyName — resolve a lobby to its live
// channel WITHOUT joining it.
// ---------------------------------------------------------------------------
//
// A viewer needs the channelArn to watch: near-realtime HLS playback is
// addressed by channel. Until now the only way to learn it was POST /sessions
// with findOrCreate, which CREATES a session and enrolls you as a
// participant — the exact thing a viewer must not do. So watching a room
// required first joining it, which defeats the point of watching: the reason
// to use HLS is that an audience costs nothing per head.
//
// Read-only and non-mutating. Returns 404 when no session is live, which is
// the honest answer to "can I watch this" before anyone has started.
//
// Deliberately NOT a proxy to LVS: the repository already holds the mapping,
// and a viewer should not need LVS reachable to be told there is nothing on.
videoSessionsRouter.get(
  '/sessions/by-lobby/:lobbyName',
  requireAuth,
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    try {
      const lobbyName = decodeURIComponent(req.params.lobbyName ?? '');
      if (!lobbyName) throw new ValidationError('lobbyName is required');

      const session = await videoSessionRepo.findActiveByLobbyName(lobbyName);
      if (!session) throw new NotFoundError('No live session for this lobby');

      // Only what a viewer needs. Participant lists and tokens stay out: this
      // route is reachable by anyone who can see the room, and watching is a
      // weaker right than joining.
      res.status(200).json({
        lobbyName,
        sessionId: session.sessionId,
        channelArn: session.channelArn,
        startedAt: session.startedAt ?? null,
        startedBy: session.startedBy ?? null,
      });
    } catch (err) {
      if (err instanceof AppError) throw err;
      log.error({ err }, '[videoSessions] GET /sessions/by-lobby error');
      throw new AppError(500, 'Failed to resolve lobby');
    }
  }),
);

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

// ---------------------------------------------------------------------------
// Live captions toggle (2026-08-25 wave)
// ---------------------------------------------------------------------------
//
// POST /api/video/captions { channelArn, enabled }
//
// enabled=true  → create ONE per-participant webhook-pcm tap on the LVS
//                 channel, pointed at the live-captions ASR sidecar. The
//                 sidecar publishes caption lines to Redis; the gateway's
//                 caption-relay fans them onto the `captions:<arn>` WS
//                 channel that clients subscribe to.
// enabled=false → delete that tap.
//
// Tap ids are tracked in-process per channelArn (single-node dev is the
// deployment reality); disable falls back to scanning the channel's taps
// for one pointing at OUR sink URL, so a restarted platform-api can still
// turn captions off. Idempotent in both directions.

// --- in-call emoji reactions (playback) ------------------------------------
//
// GET /api/video/reactions?lobbyName=<name>&from=<iso>&to=<iso>
//
// Serves the reactions drained from the durable `call:reaction.recorded`
// event by call-reaction-consumer. `from`/`to` are optional ISO bounds
// (typically a recording's startedAt/endedAt) — omitted, the full
// retention window for the lobby is returned. Ascending by `at`.
videoSessionsRouter.get('/reactions', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const lobbyName = typeof req.query.lobbyName === 'string' ? req.query.lobbyName : undefined;
  if (!lobbyName) {
    throw new ValidationError('lobbyName query parameter is required');
  }
  const from = typeof req.query.from === 'string' && req.query.from.length > 0 ? req.query.from : undefined;
  const to = typeof req.query.to === 'string' && req.query.to.length > 0 ? req.query.to : undefined;
  const reactions = await callReactionRepo.listByLobby(tenantOf(req.user?.orgId), lobbyName, from, to);
  res.json({ reactions });
}));

const CAPTIONS_SINK_URL =
  process.env.LIVE_CAPTIONS_SINK_URL || 'http://localhost:3915/pcm';
const captionTapsByArn = new Map<string, string>();

videoSessionsRouter.post('/captions', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { channelArn, enabled } = (req.body ?? {}) as { channelArn?: string; enabled?: boolean };
  if (!channelArn || typeof enabled !== 'boolean') {
    throw new AppError(400, 'channelArn and enabled are required');
  }
  const adminToken = process.env.VIDEO_ADMIN_TOKEN || 'admin';
  const tapsUrl = `${VIDEO_API_URL}/api/channels/${encodeURIComponent(channelArn)}/taps`;
  const authHeaders = { Authorization: `Bearer ${adminToken}` };

  if (enabled) {
    const existing = captionTapsByArn.get(channelArn);
    if (existing) {
      res.json({ enabled: true, tapId: existing });
      return;
    }
    const createRes = await fetch(tapsUrl, {
      method: 'POST',
      headers: { ...authHeaders, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        destination: {
          type: 'webhook-pcm',
          url: CAPTIONS_SINK_URL,
          perParticipant: true,
          chunkMs: 200,
        },
      }),
    });
    if (!createRes.ok) {
      const text = await createRes.text().catch(() => '');
      throw new AppError(502, `LVS tap create failed (${createRes.status}): ${text.slice(0, 200)}`);
    }
    const body = (await createRes.json()) as { tapId?: string; id?: string };
    const tapId = body.tapId ?? body.id ?? '';
    if (tapId) captionTapsByArn.set(channelArn, tapId);
    res.json({ enabled: true, tapId });
    return;
  }

  // Disable: tracked id first, then scan for our sink URL.
  let tapId = captionTapsByArn.get(channelArn) ?? null;
  if (!tapId) {
    const listRes = await fetch(tapsUrl, { headers: authHeaders });
    if (listRes.ok) {
      const body = (await listRes.json()) as { taps?: Array<{ tapId?: string; id?: string; destination?: { url?: string } }> };
      const mine = (body.taps ?? []).find((t) => t.destination?.url === CAPTIONS_SINK_URL);
      tapId = mine?.tapId ?? mine?.id ?? null;
    }
  }
  if (tapId) {
    await fetch(`${tapsUrl}/${encodeURIComponent(tapId)}`, { method: 'DELETE', headers: authHeaders })
      .catch(() => undefined); // 404 = already gone
  }
  captionTapsByArn.delete(channelArn);
  res.json({ enabled: false });
}));
