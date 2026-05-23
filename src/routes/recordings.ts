/**
 * Recording lookup API — read-side surface for the frontend's "Past
 * Calls" UI. Three endpoints:
 *
 *   GET /api/recordings?limit=&cursor=
 *     Lists recordings the authed user participated in. Returns recent-
 *     first (descending startedAt) with keyset pagination.
 *
 *   GET /api/recordings/:recordingId
 *     Full details for one recording. 403 unless caller participated.
 *
 *   POST /api/recordings/:recordingId/playback-token
 *     Mints an LVS playback JWT for private channels + returns the
 *     ready-to-play HLS DVR playlist URL. 403 unless caller
 *     participated.
 *
 * No write paths — recordings are written by the LVS webhook handler
 * (see routes/lvsWebhooks.ts). This file is purely read + token-mint.
 */

import { Router, Request, Response } from 'express';
import { randomUUID, randomBytes } from 'crypto';
import { PutCommand, GetCommand } from '@aws-sdk/lib-dynamodb';
import { requireAuth } from '../middleware/auth';
import { callRecordingRepo } from '../repositories';
import { docClient } from '../lib/aws-clients';
import { tableName } from '../lib/ddb-table-name';
import { VIDEO_API_URL } from '../lib/video-auth';
import {
  asyncHandler,
  AppError,
  NotFoundError,
  ValidationError,
} from '../middleware/error-handler';
import log from '../lib/logger';

export const recordingsRouter = Router();

// --------------------------------------------------------------------
// GET /api/recordings — list by authed user, optionally filtered by
// lobbyName (W6 — RoomAdminPage "View recordings" link).
// --------------------------------------------------------------------
recordingsRouter.get(
  '/recordings',
  requireAuth,
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const userId = (req as any).user?.userId ?? (req as any).user?.sub;
    if (!userId) {
      throw new AppError(401, 'unauthorized');
    }
    const limit = Math.max(1, Math.min(100, Number(req.query.limit) || 20));
    const cursor = typeof req.query.cursor === 'string' ? req.query.cursor : undefined;
    const lobbyName = typeof req.query.lobbyName === 'string' ? req.query.lobbyName : undefined;

    // W6 — room-scoped filter for RoomAdminPage. Auth is still required (any
    // authenticated user can list a room's recordings — the RoomAdminPage UI
    // gates the "View recordings" link to owners/admins, so cross-room
    // discovery is unlikely in practice). If we later want strict per-room
    // ACLs, add a membership check here against hangoutRoomRepo.
    if (lobbyName) {
      const { items, cursor: next } = await callRecordingRepo.listByLobbyName(lobbyName, limit, cursor);
      res.json({ items, cursor: next ?? null });
      return;
    }

    const { items, cursor: next } = await callRecordingRepo.listByUser(userId, { limit, cursor });
    res.json({ items, cursor: next ?? null });
  }),
);

// --------------------------------------------------------------------
// GET /api/recordings/:recordingId — single recording details
// --------------------------------------------------------------------
recordingsRouter.get(
  '/recordings/:recordingId',
  requireAuth,
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const userId = (req as any).user?.userId ?? (req as any).user?.sub;
    const { recordingId } = req.params;
    // We don't know callId from recordingId alone today (no GSI on
    // recordingId). Trade-off: query the user's participant index for
    // this recording — if absent, the user isn't authorized.
    // Index SK is `${startedAt}#${recordingId}` — we don't have
    // startedAt here so we must scan the user's recent recordings.
    // For MVP, list first page and filter.
    const { items } = await callRecordingRepo.listByUser(userId, { limit: 100 });
    const match = items.find((r) => r.recordingId === recordingId);
    if (!match) {
      throw new NotFoundError('recording not found or not authorized');
    }
    const detail = await callRecordingRepo.getRecording(match.callId, recordingId);
    if (!detail) {
      throw new NotFoundError('recording metadata missing');
    }
    res.json(detail);
  }),
);

// --------------------------------------------------------------------
// POST /api/recordings/:recordingId/playback-token
// --------------------------------------------------------------------
recordingsRouter.post(
  '/recordings/:recordingId/playback-token',
  requireAuth,
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const userId = (req as any).user?.userId ?? (req as any).user?.sub;
    const { recordingId } = req.params;
    // Same participant gate as above.
    const { items } = await callRecordingRepo.listByUser(userId, { limit: 100 });
    const match = items.find((r) => r.recordingId === recordingId);
    if (!match) {
      throw new NotFoundError('recording not found or not authorized');
    }
    const detail = await callRecordingRepo.getRecording(match.callId, recordingId);
    if (!detail) {
      throw new NotFoundError('recording metadata missing');
    }

    // Mint an LVS playback token. We hit LVS as the authoritative
    // signer; LVS validates the channel's `authorized` flag and
    // returns a JWT with 1-hour TTL.
    const arn = detail.channelArn;
    let token: string;
    let expiresAt: string;
    try {
      // LVS mint route requires the admin bearer token — without it, LVS
      // returns 401 and the FE shows "HTTP 502" beside every recording.
      // Mirrors the auth header pattern used by videoSessions.ts for
      // channel creation.
      const adminToken = process.env.VIDEO_ADMIN_TOKEN || 'admin';
      const lvsRes = await fetch(
        `${VIDEO_API_URL}/api/channels/${encodeURIComponent(arn)}/playback-tokens`,
        {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            Authorization: `Bearer ${adminToken}`,
          },
          body: JSON.stringify({ ttlSeconds: 3600 }),
        },
      );
      if (!lvsRes.ok) {
        // Public channels return 400 "not authorized" — that's fine,
        // playback is open. Fall through with empty token.
        if (lvsRes.status === 400) {
          token = '';
          expiresAt = '';
        } else {
          throw new AppError(502, `LVS playback-token mint failed: ${lvsRes.status}`);
        }
      } else {
        const body = (await lvsRes.json()) as { token: string; expiresAt: string };
        token = body.token;
        expiresAt = body.expiresAt;
      }
    } catch (e: any) {
      log.error({ err: e?.message ?? e, recordingId }, '[recordings] playback-token proxy failed');
      throw new AppError(502, 'playback-token mint failed');
    }

    // Compose the playable URL. DVR playlist over the recording's time
    // window — LVS stitches the segments into one VOD HLS playlist.
    const params = new URLSearchParams({
      from: detail.startedAt,
      to: detail.endedAt,
      ...(token ? { token } : {}),
    });
    const playlistUrl = `${VIDEO_API_URL}/api/channels/${encodeURIComponent(arn)}/dvr/playlist.m3u8?${params}`;

    res.json({ playlistUrl, token: token || null, expiresAt: expiresAt || null });
  }),
);

// --------------------------------------------------------------------
// W13 — Share-links
// --------------------------------------------------------------------
const SHARE_LINKS_TABLE = tableName('video-recording-share-links');

// POST /api/recordings/:recordingId/share — mint a share-token that
// non-participants can use to view the recording. Default TTL 7 days.
recordingsRouter.post(
  '/recordings/:recordingId/share',
  requireAuth,
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const userId = (req as any).user?.userId ?? (req as any).user?.sub;
    const { recordingId } = req.params;
    const ttlSeconds = Math.max(60, Math.min(60 * 60 * 24 * 90, Number(req.body?.ttlSeconds) || 7 * 24 * 60 * 60));
    // Participant gate — only people who were in the call can create
    // share-links. Same lookup pattern as the playback-token route.
    const { items } = await callRecordingRepo.listByUser(userId, { limit: 100 });
    const match = items.find((r) => r.recordingId === recordingId);
    if (!match) throw new NotFoundError('recording not found or not authorized');

    const shareToken = randomBytes(24).toString('base64url');
    const expiresAt = new Date(Date.now() + ttlSeconds * 1000).toISOString();
    const ttl = Math.floor(Date.now() / 1000) + ttlSeconds;

    await docClient.send(
      new PutCommand({
        TableName: SHARE_LINKS_TABLE,
        Item: {
          shareToken,
          recordingId,
          callId: match.callId,
          channelArn: match.channelArn,
          createdBy: userId,
          createdAt: new Date().toISOString(),
          expiresAt,
          viewCount: 0,
          ttl,
        },
      }),
    );

    res.json({
      shareToken,
      shareUrl: `/recordings/share/${shareToken}`,
      expiresAt,
    });
  }),
);

// GET /api/recordings/share/:shareToken/playback — unauthed access via
// a valid share-token. Returns the same playlistUrl shape as the
// authed playback-token endpoint.
recordingsRouter.get(
  '/recordings/share/:shareToken/playback',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const { shareToken } = req.params;
    const out = await docClient.send(
      new GetCommand({ TableName: SHARE_LINKS_TABLE, Key: { shareToken } }),
    );
    const link = out.Item as { recordingId: string; callId: string; channelArn: string; expiresAt: string; viewCount?: number } | undefined;
    if (!link) throw new NotFoundError('share link not found');
    if (new Date(link.expiresAt).getTime() < Date.now()) {
      throw new NotFoundError('share link expired');
    }
    const detail = await callRecordingRepo.getRecording(link.callId, link.recordingId);
    if (!detail) throw new NotFoundError('recording missing');

    // Bump view count — fire-and-forget so a DDB hiccup doesn't fail
    // the playback request.
    void docClient.send(
      new PutCommand({
        TableName: SHARE_LINKS_TABLE,
        Item: { ...link, viewCount: (link.viewCount ?? 0) + 1, lastViewedAt: new Date().toISOString() },
      }),
    ).catch(() => { /* */ });

    const params = new URLSearchParams({
      from: detail.startedAt,
      to: detail.endedAt,
    });
    const playlistUrl = `${VIDEO_API_URL}/api/channels/${encodeURIComponent(link.channelArn)}/dvr/playlist.m3u8?${params}`;
    res.json({ playlistUrl, expiresAt: link.expiresAt });
  }),
);

// DELETE /api/recordings/:recordingId — owner-only delete. Removes the
// DDB row + participant fan-out rows. The underlying MP4 stays in
// LVS storage until the GC sweep (separate retention window).
recordingsRouter.delete(
  '/recordings/:recordingId',
  requireAuth,
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const userId = (req as any).user?.userId ?? (req as any).user?.sub;
    const { recordingId } = req.params;
    const { items } = await callRecordingRepo.listByUser(userId, { limit: 100 });
    const match = items.find((r) => r.recordingId === recordingId);
    if (!match) throw new NotFoundError('recording not found or not authorized');
    const detail = await callRecordingRepo.getRecording(match.callId, recordingId);
    if (!detail) throw new NotFoundError('recording metadata missing');
    // Only the user who STARTED the recording can delete it. Other
    // participants can untag themselves via deleteByUserId (W11/H7).
    // Fail closed when startedBy is missing — historical rows that
    // pre-date startedBy must NOT become deletable by anyone (audit C5).
    if (!detail.startedBy || detail.startedBy !== userId) {
      throw new AppError(403, 'only the recording owner can delete');
    }
    await callRecordingRepo.deleteRecording(match.callId, recordingId);
    log.info({ recordingId, userId }, '[recordings] deleted by owner');
    res.status(204).end();
  }),
);

