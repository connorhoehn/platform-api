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
import { CircuitBreaker, CircuitState } from 'distributed-core/transport';
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

export const videoSessionsRouter = Router();

// --- SFU resilience layer -------------------------------------------------

// 8s per-request abort budget. The SFU's POST /channels does an IVS channel
// create round-trip; 8s is comfortably above its p99 (~500ms) but below what
// a user perceives as "hung". Timeout → AbortError → counted as a breaker
// failure → 504 to the client.
const FETCH_TIMEOUT_MS = 8000;

// Breaker timeout is the *backstop* in case AbortSignal doesn't fire (e.g.
// a runtime that ignores the signal). Must exceed FETCH_TIMEOUT_MS so the
// abort path normally wins and we don't double-count.
const BREAKER_TIMEOUT_MS = 10_000;

// Open after 5 consecutive failures, attempt half-open probe every 30s,
// require 2 half-open successes to fully close. Tuned for SFU's typical
// outage profile (single-AZ blip → ~30s; full deploy → ~2min).
const sfuBreaker = new CircuitBreaker({
  name: 'video-api',
  failureThreshold: 5,
  successThreshold: 2,
  resetTimeout: 30_000,
  timeout: BREAKER_TIMEOUT_MS,
  halfOpenMaxCalls: 1,
});

/**
 * Internal marker for "the breaker said no" — lets the routes distinguish
 * "fast-fail because circuit is open" (→ 503) from "actual upstream error".
 */
class BreakerOpenError extends Error {
  constructor() {
    super('Video API circuit breaker is open');
    this.name = 'BreakerOpenError';
  }
}

/** Carries the underlying Response so callers can map status → AppError. */
class UpstreamServerError extends Error {
  constructor(public readonly response: globalThis.Response) {
    super(`Video API upstream error ${response.status}`);
    this.name = 'UpstreamServerError';
  }
}

/**
 * Run a fetch against the SFU through the breaker with an abort-based
 * timeout. Caller still inspects `res.ok`/`res.status` for application-level
 * errors; only network failures, timeouts, and upstream 5xx are reported to
 * the breaker as failures. 4xx is client-shaped and doesn't trip the breaker.
 */
async function callSfu(input: string, init: RequestInit = {}): Promise<globalThis.Response> {
  // Fast-fail before touching fetch when the breaker is already OPEN — this
  // is the "no timer waste" property the acceptance criteria call out.
  if (sfuBreaker.getState() === CircuitState.OPEN) {
    throw new BreakerOpenError();
  }
  // We deliberately don't post-translate errors based on a *later* OPEN
  // state: the failure that *trips* the breaker should still surface its
  // real shape (504 timeout, 502 upstream-5xx, etc.) to the caller of that
  // very request. Only *subsequent* callers see fast-fail 503.
  return sfuBreaker.execute(async () => {
    const res = await fetch(input, {
      ...init,
      signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
    });
    if (res.status >= 500) {
      throw new UpstreamServerError(res);
    }
    return res;
  });
}

/**
 * Translate an SFU call rejection into the right HTTP error for the client:
 *   - circuit open       → 503 fast-fail
 *   - AbortError/timeout → 504
 *   - upstream 5xx       → forward status
 *   - anything else      → 502 bad-gateway
 */
function mapSfuError(err: unknown): AppError {
  if (err instanceof BreakerOpenError) {
    return new AppError(503, 'Video service temporarily unavailable');
  }
  if (err instanceof UpstreamServerError) {
    return new AppError(err.response.status, `Video API error: ${err.response.status}`);
  }
  if (
    err instanceof Error
    && (err.name === 'AbortError'
      || err.name === 'TimeoutError'
      || /timeout/i.test(err.message))
  ) {
    return new AppError(504, 'Video service timeout');
  }
  return new AppError(502, 'Video service unreachable');
}

// Exposed for tests that need to reset breaker state between cases.
export function __resetSfuBreakerForTests(): void {
  sfuBreaker.reset();
}

// Exposed for tests that need to shrink the reset timeout to avoid real
// 30s waits. NOT for production use — the production timeouts are tuned in
// the constructor above.
export function __configureSfuBreakerForTests(opts: {
  failureThreshold?: number;
  successThreshold?: number;
  resetTimeout?: number;
  halfOpenMaxCalls?: number;
}): void {
  sfuBreaker.updateOptions(opts);
}

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
    const displayName = typeof body.displayName === 'string' ? body.displayName : undefined;
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
    }

    // The SFU gates POST /api/channels behind adminAuth (added in the
    // recent live-video-streaming reconciliation). Platform-api is the only
    // trusted in-cluster caller, so we send a Bearer ADMIN_TOKEN. The token
    // is provisioned via `hack/genadmin.sh` in live-video-streaming and
    // injected into our pod from the `admin-token` Secret. Falls back to
    // literal 'admin' which matches the SFU's `||'admin'` default.
    const adminToken = process.env.VIDEO_ADMIN_TOKEN || 'admin';
    let channelRes: globalThis.Response;
    try {
      channelRes = await callSfu(`${VIDEO_API_URL}/api/channels`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${adminToken}`,
        },
        body: JSON.stringify({ name: lobbyName }),
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
    try {
      const { getRedisClient } = await import('../lib/redis-client.js');
      const redis = await getRedisClient();
      if (redis) {
        const indexKey = `video:arn-index:${channelArn}`;
        await redis.set(
          indexKey,
          JSON.stringify({ sessionId, lobbyName, createdAt: now }),
          { EX: 7 * 24 * 60 * 60 }, // 7 days — covers webhook arrival + retries
        );
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
