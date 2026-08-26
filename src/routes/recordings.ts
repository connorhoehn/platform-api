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
import { callRecordingRepo, callReactionRepo, type CallRecording } from '../repositories';
import { publicizeTracks, reconcileParticipants } from '../lib/recording-truth';
import { mergeReactionsIntoTranscript } from '../lib/transcript-reactions';
import { docClient } from '../lib/aws-clients';
import { tenantOf } from '../lib/tenant';
import { tableName } from '../lib/ddb-table-name';
import { VIDEO_API_URL, VIDEO_PUBLIC_URL } from '../lib/video-auth';
import { callSfu, mapSfuError } from '../lib/sfu-breaker';
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
    res.json(publicRecordingView(detail));
  }),
);

/**
 * Everything the detail response must scrub or re-derive before it leaves
 * the process.
 *
 * `tracks[].url` / `.file` arrive from LVS as absolute recorder-pod paths —
 * in local dev, literally
 * `file:///Users/<developer>/Projects/realtime-examples/...`. A browser
 * cannot open those, and shipping them leaks a filesystem layout (a
 * developer's home directory today, a pod's mount path in production) to
 * every authenticated caller. `publicizeTracks` rewrites each track onto
 * the same proxied `/segments/:filename` URL the composed file already
 * uses and drops the raw `file` field.
 *
 * `participants` / `participantCount` are re-derived rather than served as
 * stored, so rows written before the webhook fix still render deduped,
 * initiator-inclusive people counts. Duration is already normalized by the
 * repository read path.
 */
function publicRecordingView(detail: CallRecording): Record<string, unknown> {
  const people = reconcileParticipants({
    participants: detail.participants,
    tracks: detail.tracks,
    startedBy: detail.startedBy,
    startedAt: detail.startedAt,
    endedAt: detail.endedAt,
  });
  return {
    ...detail,
    participants: people.participants,
    participantCount: people.participantCount,
    tracks: publicizeTracks(detail.tracks, {
      channelArn: detail.channelArn,
      recordingId: detail.recordingId,
      publicBaseUrl: VIDEO_PUBLIC_URL,
    }),
  };
}

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
    //
    // Auth: post-Wave-7 thin-proxy migration LVS no longer returns a
    // streamKey on POST /api/sessions, so the DDB row's streamKey is
    // always '' and can't be used to authorize this hop (would send no
    // Authorization header → LVS 401 → 502 to the FE). Use the
    // ADMIN_TOKEN bearer instead — LVS bearerAuth treats it as a
    // role=admin wildcard that satisfies `allowJwt` here. Same pattern
    // as reapLvsParticipant (videoSessions.ts).
    const arn = detail.channelArn;
    const adminToken = process.env.VIDEO_ADMIN_TOKEN || 'admin';
    let token: string;
    let expiresAt: string;
    try {
      const headers: Record<string, string> = {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${adminToken}`,
      };
      // Routed through the shared SFU CircuitBreaker (hub#1001) — an open
      // circuit fast-fails before fetch, and any 5xx from LVS counts as a
      // breaker failure so other LVS-touching routes (videoSessions.ts) see
      // the trip too.
      const lvsRes = await callSfu(
        `${VIDEO_API_URL}/api/channels/${encodeURIComponent(arn)}/playback-tokens`,
        {
          method: 'POST',
          headers,
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
      // Capture upstream error class + message for triage (hub#1004).
      // callSfu rejections carry breaker/timeout/upstream context that the
      // generic "playback-token proxy failed" string would otherwise lose.
      const errName = e?.name ?? 'Error';
      const errMessage = e?.message ?? String(e);
      log.error(
        { errorName: errName, errorMessage: errMessage, recordingId },
        '[recordings] playback-token proxy failed',
      );
      // Preserve AppError shape if the inner branch threw one (it already
      // chose the right 502 + message); otherwise translate breaker /
      // timeout / unreachable into the right status via mapSfuError so the
      // client sees 503 fast-fail / 504 timeout instead of a uniform 502.
      if (e instanceof AppError) throw e;
      throw mapSfuError(e);
    }

    // Compose the playable URL.
    //
    // The previous implementation returned a `/dvr/playlist.m3u8` URL
    // pointing at the LVS DVR HLS subsystem — a separate code path that
    // synthesizes a VOD HLS manifest from time-windowed live segments.
    // Recordings DO NOT live in that subsystem: the SFU's
    // RecordingManager writes raw MP4 files (recording_NNNNN.mp4 per
    // segment, peer_leg-NNN.mp4 per participant, composed.mp4 for the
    // multi-producer grid) under `<recordingDir>/<arnSafe>/<recordingId>/`
    // and serves them via `GET /:arn/recordings/:id/segments/:filename`
    // with byte-range support. The DVR HLS path never had any of these
    // files, so playback consistently 404'd.
    //
    // Adopt the same flow the LVS admin UI uses: hand the FE a direct
    // MP4 URL and let <video src> handle byte-range playback + seek. No
    // HLS, no playlist, no token-query-param dance (the segments route
    // is CORS-open and admin-bypass; token mint is preserved for
    // potential future use by private-channel callers).
    //
    // File-picking order: composed grid → first segment → fallback to
    // `composed.mp4` literal (so a recording with a pending compose
    // job will start playing the moment the file lands).
    const composedKey =
      (detail as { composedSegment?: { key?: string; url?: string } }).composedSegment?.key
      ?? (detail as { composedSegment?: { key?: string; url?: string } }).composedSegment?.url;
    const firstSegmentKey =
      (detail.segments && detail.segments[0]?.key)
      ?? (detail.segments && detail.segments[0]?.url);
    const playbackKey = composedKey ?? firstSegmentKey ?? 'composed.mp4';
    // `path.basename` strips any `<arnSafe>/<recordingId>/` prefix from
    // the stored key without pulling node:path into the import surface
    // unnecessarily — simple string split is enough.
    const playbackFilename = playbackKey.split('/').pop() ?? 'composed.mp4';
    const composedUrl = `${VIDEO_PUBLIC_URL}/api/channels/${encodeURIComponent(arn)}/recordings/${encodeURIComponent(recordingId)}/segments/${encodeURIComponent(playbackFilename)}`;

    // `playlistUrl` is preserved for backward compatibility with the FE's
    // RecordingPlayer (it expects a `playlistUrl` field), but is now a
    // direct MP4 URL — works with both hls.js (which falls back to
    // native <video src> for non-m3u8 URLs) and bare <video>.
    // `composedUrl` is the new explicit field; FE checks it first.
    res.json({
      playlistUrl: composedUrl,
      composedUrl,
      token: token || null,
      expiresAt: expiresAt || null,
    });
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
    const playlistUrl = `${VIDEO_PUBLIC_URL}/api/channels/${encodeURIComponent(link.channelArn)}/dvr/playlist.m3u8?${params}`;
    res.json({ playlistUrl, expiresAt: link.expiresAt });
  }),
);

// --------------------------------------------------------------------
// GET /api/recordings/:recordingId/transcript — serve the JSON
// transcript produced by the transcription-worker. 404s when
// transcriptStatus !== 'ready'. 403 unless caller participated.
// --------------------------------------------------------------------
//
// The transcript blob is stored by the worker at
//   <arnSafe>/<recordingId>/transcript.json
// alongside the recording's per-peer mp4 files. We proxy it through
// platform-api rather than handing the FE a direct S3/file URL so that
// future migrations (S3-presigned, CloudFront, auth-gated CDN) are
// invisible to the client.
//
// For local-mode dev, the worker writes to the same hostPath LVS uses
// (RECORDING_DIR). We resolve via the LVS segments-shaped HTTP path
// because LVS already authenticates + serves files there — re-using
// its file-resolution avoids re-implementing arnSafe + path traversal
// guards on the platform-api side.
recordingsRouter.get(
  '/recordings/:recordingId/transcript',
  requireAuth,
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const userId = (req as any).user?.userId ?? (req as any).user?.sub;
    const { recordingId } = req.params;
    const { items } = await callRecordingRepo.listByUser(userId, { limit: 100 });
    const match = items.find((r) => r.recordingId === recordingId);
    if (!match) throw new NotFoundError('recording not found or not authorized');
    const detail = await callRecordingRepo.getRecording(match.callId, recordingId);
    if (!detail) throw new NotFoundError('recording metadata missing');

    const status = detail.transcriptStatus;
    if (status === undefined) {
      throw new NotFoundError('transcription not requested for this recording');
    }
    if (status !== 'ready') {
      // Surface the in-flight status so the FE can show a spinner /
      // failure banner without polling for the eventual transcript.
      res.status(202).json({
        status,
        recordingId,
        error: detail.transcriptError ?? null,
        generatedAt: detail.transcriptGeneratedAt ?? null,
      });
      return;
    }
    if (!detail.transcriptKey) {
      // Shouldn't happen — ready implies key present. Defensive 500.
      throw new AppError(500, 'transcript ready but key missing');
    }

    // Fetch from LVS using the same segments URL pattern the
    // composedUrl uses. The worker stored transcript.json relative
    // to LVS_RECORDING_DIR, so the path encoding is the same as for
    // recording mp4s. We could equivalently read the file off the
    // shared volume, but LVS already implements path-traversal guards
    // we'd otherwise duplicate.
    //
    // For S3 destination the transcriptKey is already an s3:// URI;
    // in that case we'd need a different code path (signed URL or
    // boto3 GetObject). MVP supports local mode only here — the
    // worker writes the key shape uniformly for both backends, but
    // the platform-api proxy only handles the local-served-by-LVS
    // case below. Adding S3 follows the same pattern as the
    // existing composedUrl handling in /playback-token.
    const arnSafe = (detail.channelArn || '').replace(/[^A-Za-z0-9._-]/g, '_');
    const lvsUrl = `${VIDEO_API_URL}/api/channels/${encodeURIComponent(detail.channelArn)}/recordings/${encodeURIComponent(recordingId)}/segments/transcript.json`;
    log.info({ recordingId, userId, transcriptKey: detail.transcriptKey, arnSafe }, '[recordings] transcript fetch');
    try {
      const upstream = await callSfu(lvsUrl, {
        method: 'GET',
        headers: { 'Authorization': `Bearer ${process.env.LVS_ADMIN_TOKEN ?? 'admin'}` },
      });
      if (!upstream.ok) {
        if (upstream.status === 404) {
          throw new NotFoundError('transcript file missing from blob storage');
        }
        throw new AppError(upstream.status, `lvs returned ${upstream.status}`);
      }
      const text = await upstream.text();

      // Inject in-call emoji reactions as synthetic transcript segments
      // ("Connor reacted with 👍") for the recording's time window.
      // Best-effort: a reactions outage or a non-JSON transcript body
      // must not fail transcript serving — fall through to the raw text.
      try {
        const doc = JSON.parse(text) as { segments?: Array<{ startMs: number; endMs: number; speakerId: string; speakerLabel: string; text: string }> };
        if (doc && Array.isArray(doc.segments)) {
          const reactions = await callReactionRepo.listByLobby(
            tenantOf(req.user?.orgId),
            detail.lobbyName,
            detail.startedAt,
            detail.endedAt,
          );
          const merged = mergeReactionsIntoTranscript(
            doc as { segments: Array<{ startMs: number; endMs: number; speakerId: string; speakerLabel: string; text: string }> },
            reactions,
            detail.startedAt,
          );
          res.json(merged);
          return;
        }
      } catch (mergeErr) {
        log.warn(
          { err: mergeErr instanceof Error ? mergeErr.message : String(mergeErr), recordingId },
          '[recordings] reaction merge failed — serving unmerged transcript',
        );
      }
      res.type('application/json').send(text);
    } catch (e) {
      if (e instanceof AppError) throw e;
      throw mapSfuError(e);
    }
  }),
);

// --------------------------------------------------------------------
// GET /api/recordings/:recordingId/summary — serve the LLM-generated
// markdown meeting-notes summary. 404s when the recording has no
// transcription opted in; 202 + status payload while in flight; the
// raw markdown body when ready.
// --------------------------------------------------------------------
recordingsRouter.get(
  '/recordings/:recordingId/summary',
  requireAuth,
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const userId = (req as any).user?.userId ?? (req as any).user?.sub;
    const { recordingId } = req.params;
    const { items } = await callRecordingRepo.listByUser(userId, { limit: 100 });
    const match = items.find((r) => r.recordingId === recordingId);
    if (!match) throw new NotFoundError('recording not found or not authorized');
    const detail = await callRecordingRepo.getRecording(match.callId, recordingId);
    if (!detail) throw new NotFoundError('recording metadata missing');

    const status = detail.transcriptStatus;
    if (status === undefined) {
      throw new NotFoundError('transcription not requested for this recording');
    }
    if (status !== 'ready') {
      res.status(202).json({
        status,
        recordingId,
        error: detail.transcriptError ?? null,
      });
      return;
    }
    if (!detail.summaryMarkdown) {
      // Ready but no summary — happens for the "no audible legs"
      // placeholder branch. Return 200 with the placeholder so the FE
      // can render a "no transcript" badge instead of an empty panel.
      res.type('text/markdown').send('_No summary available for this recording._');
      return;
    }
    res.type('text/markdown').send(detail.summaryMarkdown);
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

