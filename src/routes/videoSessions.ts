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

export const videoSessionsRouter = Router();

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
 */
async function sfuChannelIsAlive(channelArn: string): Promise<boolean> {
  try {
    const res = await fetch(
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
        console.log('[videoSessions] findOrCreate hit', existing.sessionId, 'lobby', lobbyName);
        res.status(200).json({ sessionId: existing.sessionId, reused: true });
        return;
      }
    }

    const channelRes = await fetch(`${VIDEO_API_URL}/api/channels`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: lobbyName }),
    });

    if (!channelRes.ok) {
      const errorBody = await channelRes.text().catch(() => 'Unknown error');
      console.error('[videoSessions] create channel failed:', channelRes.status, errorBody);
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
      participants: [
        {
          userId,
          displayName: displayName || userId,
          joinedAt: now,
        },
      ],
    };

    try {
      await videoSessionRepo.createSession(record);
      console.log('[videoSessions] Created session', sessionId, 'lobby', lobbyName, 'channel', channelArn);
    } catch (dbErr) {
      // Log but don't fail — the channel was created successfully
      console.error('[videoSessions] Failed to persist local record:', dbErr);
    }

    res.status(201).json({ sessionId, reused: false });
  } catch (err) {
    if (err instanceof AppError) throw err;
    console.error('[videoSessions] POST /sessions error:', err);
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

    const tokenRes = await fetch(
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

    if (!tokenRes.ok) {
      const errorBody = await tokenRes.text().catch(() => 'Unknown error');
      console.error('[videoSessions] participant-token request failed:', tokenRes.status, errorBody);
      throw new AppError(tokenRes.status, `Video API error: ${tokenRes.status}`);
    }

    const tokenData = await tokenRes.json() as Record<string, unknown>;

    // Idempotent participant add — don't double-insert if the same user is
    // already in this session (e.g. browser refresh on a findOrCreate'd lobby).
    const userId = req.user!.sub;
    const already = (session.participants ?? []).some(p => p.userId === userId);
    if (!already) {
      const now = new Date().toISOString();
      try {
        await videoSessionRepo.addParticipant(lobbyName, sessionId, {
          userId,
          displayName: displayName || userId,
          joinedAt: now,
        });
        console.log('[videoSessions] Added participant', userId, 'to session', sessionId);
      } catch (dbErr) {
        console.error('[videoSessions] Failed to add participant to local record:', dbErr);
      }
    }

    res.status(200).json({
      token: tokenData.token as string,
      participantId: (tokenData.participantId as string) || randomUUID(),
      userId,
    });
  } catch (err) {
    if (err instanceof AppError) throw err;
    console.error('[videoSessions] POST /sessions/:id/join error:', err);
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

    // Identify caller. requireAuth-style req.user is undefined on optionalAuth
    // beacon paths; fall back to userId in the body so the FE can hint who's
    // leaving (it knows its own clientId).
    const userId =
      req.user?.sub
      ?? (typeof body.userId === 'string' ? body.userId : undefined);

    let remaining = (session.participants ?? []).length;
    if (userId) {
      const splice = await videoSessionRepo.removeParticipant(lobbyName, sessionId, userId);
      if (splice) remaining = splice.remaining;
    }

    if (remaining > 0) {
      console.log('[videoSessions] participant', userId ?? '(anon)', 'left session', sessionId, '— still', remaining, 'remaining');
      res.status(200).json({ remaining, ended: false });
      return;
    }

    // Last participant out — mark session ended. Channel cleanup is the
    // reaper's job (see file header).
    const now = new Date().toISOString();
    try {
      await videoSessionRepo.endSession(lobbyName, sessionId, now);
      console.log('[videoSessions] Ended session', sessionId, '(last participant out)');
    } catch (dbErr) {
      console.error('[videoSessions] Failed to update local record on end:', dbErr);
    }

    res.status(200).json({ remaining: 0, ended: true });
  } catch (err) {
    if (err instanceof AppError) throw err;
    console.error('[videoSessions] POST /sessions/:id/end error:', err);
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
    console.error('[videoSessions] GET /sessions/document/:documentId error:', err);
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
    console.error('[videoSessions] GET /sessions/:sessionId error:', err);
    throw new AppError(500, 'Failed to fetch video session');
  }
}));
