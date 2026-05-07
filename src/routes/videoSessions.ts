/**
 * Video session proxy routes — forwards requests to the videonowandlater (VNL)
 * API for creating/joining/ending hangout video sessions.
 *
 * The social-api authenticates against VNL's Cognito using a service account,
 * so the WSG frontend doesn't need VNL Cognito credentials.
 *
 * Each proxy call also persists a local VideoSessionRecord in DynamoDB so
 * reviewers can see conversation history for a document.
 */

import { Router, Request, Response } from 'express';
import { requireAuth, optionalAuth } from '../middleware/auth';
import { getVnlAuthToken, VNL_API_URL } from '../lib/vnl-auth';
import { videoSessionRepo } from '../repositories';
import { VideoSessionRecord } from '../repositories/VideoSessionRepository';
import {
  asyncHandler,
  AppError,
  ValidationError,
  NotFoundError,
} from '../middleware/error-handler';

export const videoSessionsRouter = Router();

// POST /api/video/sessions — create a HANGOUT session on VNL
videoSessionsRouter.post('/sessions', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  try {
    const vnlToken = await getVnlAuthToken();

    const vnlRes = await fetch(`${VNL_API_URL}/sessions`, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${vnlToken}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ sessionType: 'HANGOUT' }),
    });

    if (!vnlRes.ok) {
      const errorBody = await vnlRes.text().catch(() => 'Unknown error');
      console.error('[videoSessions] VNL create session failed:', vnlRes.status, errorBody);
      throw new AppError(vnlRes.status, `VNL API error: ${vnlRes.status}`);
    }

    const data = await vnlRes.json() as Record<string, unknown>;

    // Persist local record for document conversation history
    const { documentId, displayName } = req.body;
    if (documentId && data.sessionId) {
      const userId = req.user!.sub;
      const now = new Date().toISOString();
      const record: VideoSessionRecord = {
        documentId,
        sessionId: data.sessionId as string,
        vnlSessionId: data.sessionId as string,
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
        console.log('[videoSessions] Created local record for session', data.sessionId as string, 'on document', documentId);
      } catch (dbErr) {
        // Log but don't fail the request — the VNL session was created successfully
        console.error('[videoSessions] Failed to persist local record:', dbErr);
      }
    }

    res.status(201).json(data);
  } catch (err) {
    if (err instanceof AppError) throw err;
    console.error('[videoSessions] POST /sessions error:', err);
    throw new AppError(500, 'Failed to create video session');
  }
}));

// POST /api/video/sessions/:sessionId/join — get IVS stage token
videoSessionsRouter.post('/sessions/:sessionId/join', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  try {
    const { sessionId } = req.params;
    const vnlToken = await getVnlAuthToken();

    const vnlRes = await fetch(`${VNL_API_URL}/sessions/${sessionId}/join`, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${vnlToken}`,
        'Content-Type': 'application/json',
      },
    });

    if (!vnlRes.ok) {
      const errorBody = await vnlRes.text().catch(() => 'Unknown error');
      console.error('[videoSessions] VNL join failed:', vnlRes.status, errorBody);
      throw new AppError(vnlRes.status, `VNL API error: ${vnlRes.status}`);
    }

    const data = await vnlRes.json() as Record<string, unknown>;

    // Add participant to local record
    const { documentId, displayName } = req.body;
    if (documentId) {
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
    }

    res.status(200).json(data);
  } catch (err) {
    if (err instanceof AppError) throw err;
    console.error('[videoSessions] POST /sessions/:id/join error:', err);
    throw new AppError(500, 'Failed to join video session');
  }
}));

// POST /api/video/sessions/:sessionId/end — end a video session
// Uses optionalAuth so sendBeacon (no auth header) works on tab close
videoSessionsRouter.post('/sessions/:sessionId/end', optionalAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  try {
    const { sessionId } = req.params;
    const vnlToken = await getVnlAuthToken();

    const vnlRes = await fetch(`${VNL_API_URL}/sessions/${sessionId}/end`, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${vnlToken}`,
        'Content-Type': 'application/json',
      },
    });

    if (!vnlRes.ok) {
      const errorBody = await vnlRes.text().catch(() => 'Unknown error');
      console.error('[videoSessions] VNL end failed:', vnlRes.status, errorBody);
      throw new AppError(vnlRes.status, `VNL API error: ${vnlRes.status}`);
    }

    const data = await vnlRes.json() as Record<string, unknown>;

    // Update local record with ended status
    const { documentId } = req.body;
    if (documentId && req.user) {
      const now = new Date().toISOString();
      try {
        await videoSessionRepo.endSession(documentId, sessionId, now);
        console.log('[videoSessions] Ended local record for session', sessionId);
      } catch (dbErr) {
        console.error('[videoSessions] Failed to update local record on end:', dbErr);
      }
    }

    res.status(200).json(data);
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
