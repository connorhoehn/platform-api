/**
 * Video session routes — composes live-video-streaming primitive APIs
 * (channels + participant tokens) into a "sessions" concept for the frontend.
 *
 * live-video-streaming exposes:
 *   POST /api/channels → { arn, name, streamKey, ingestEndpoint, playbackUrl }
 *   POST /api/channels/:arn/participant-tokens (Bearer: streamKey) → { token, participantId, expiresAt }
 *   DELETE /api/channels/:arn → teardown
 *
 * platform-api persists VideoSessionRecords in DynamoDB so the frontend can
 * list/query sessions by document.
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

// POST /api/video/sessions — create a channel and persist a session record
videoSessionsRouter.post('/sessions', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  try {
    const { documentId, displayName } = req.body;
    const channelName = documentId || randomUUID();

    const channelRes = await fetch(`${VIDEO_API_URL}/api/channels`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: channelName }),
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
      documentId: documentId || channelName,
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
      console.log('[videoSessions] Created session', sessionId, 'channel', channelArn);
    } catch (dbErr) {
      // Log but don't fail — the channel was created successfully
      console.error('[videoSessions] Failed to persist local record:', dbErr);
    }

    res.status(201).json({ sessionId });
  } catch (err) {
    if (err instanceof AppError) throw err;
    console.error('[videoSessions] POST /sessions error:', err);
    throw new AppError(500, 'Failed to create video session');
  }
}));

// POST /api/video/sessions/:sessionId/join — mint a participant token
videoSessionsRouter.post('/sessions/:sessionId/join', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  try {
    const { sessionId } = req.params;
    const { documentId, displayName } = req.body;

    if (!documentId) {
      throw new ValidationError('documentId is required in request body');
    }

    const session = await videoSessionRepo.getSession(documentId, sessionId);
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

    // Add participant to local record
    const userId = req.user!.sub;
    const now = new Date().toISOString();
    try {
      await videoSessionRepo.addParticipant(documentId, sessionId, {
        userId,
        displayName: displayName || userId,
        joinedAt: now,
      });
      console.log('[videoSessions] Added participant', userId, 'to session', sessionId);
    } catch (dbErr) {
      console.error('[videoSessions] Failed to add participant to local record:', dbErr);
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

// POST /api/video/sessions/:sessionId/end — tear down the channel
// Uses optionalAuth so sendBeacon (no auth header) works on tab close
videoSessionsRouter.post('/sessions/:sessionId/end', optionalAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  try {
    const { sessionId } = req.params;
    const { documentId } = req.body;

    if (!documentId) {
      throw new ValidationError('documentId is required in request body');
    }

    const session = await videoSessionRepo.getSession(documentId, sessionId);
    if (!session) {
      throw new NotFoundError('Session not found');
    }

    const deleteRes = await fetch(
      `${VIDEO_API_URL}/api/channels/${encodeURIComponent(session.channelArn)}`,
      { method: 'DELETE' },
    );

    if (!deleteRes.ok) {
      const errorBody = await deleteRes.text().catch(() => 'Unknown error');
      console.error('[videoSessions] channel delete failed:', deleteRes.status, errorBody);
      throw new AppError(deleteRes.status, `Video API error: ${deleteRes.status}`);
    }

    // Update local record
    const now = new Date().toISOString();
    try {
      await videoSessionRepo.endSession(documentId, sessionId, now);
      console.log('[videoSessions] Ended session', sessionId);
    } catch (dbErr) {
      console.error('[videoSessions] Failed to update local record on end:', dbErr);
    }

    res.status(200).json({});
  } catch (err) {
    if (err instanceof AppError) throw err;
    console.error('[videoSessions] POST /sessions/:id/end error:', err);
    throw new AppError(500, 'Failed to end video session');
  }
}));

// GET /api/video/sessions/document/:documentId — list all sessions for a document
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
// Requires documentId as query param since it's the partition key
videoSessionsRouter.get('/sessions/:sessionId', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  try {
    const { sessionId } = req.params;
    const documentId = req.query.documentId as string;

    if (!documentId) {
      throw new ValidationError('documentId query parameter is required');
    }

    const session = await videoSessionRepo.getSession(documentId, sessionId);
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
