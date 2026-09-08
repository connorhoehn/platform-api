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
import {
  callRecordingRepo,
  callReactionRepo,
  hangoutRoomRepo,
  hangoutRoomMemberRepo,
  describeMedia,
  type CallRecording,
  type ComposeStatus,
} from '../repositories';
import { publicizeTracks, reconcileParticipants } from '../lib/recording-truth';
import { mergeReactionsIntoTranscript } from '../lib/transcript-reactions';
import { docClient } from '../lib/aws-clients';
import { tenantOf } from '../lib/tenant';
import { principalOf, requireAuthorized } from '../lib/authorize';
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

    // W6 — room-scoped filter for RoomAdminPage.
    //
    // This used to be open: ANY authenticated user could list ANY lobby's
    // recordings, including a private DM between two other people, and the
    // comment here leaned on the RoomAdminPage UI hiding the link. Hiding a
    // link is not access control — the endpoint was one curl away.
    //
    // Now it goes through the policy decision point (lib/authorize.ts). DM
    // membership is decidable from the lobby name; room membership needs the
    // member list, which we look up and hand to the policy. A lookup failure
    // yields no memberIds, and the policy denies — it never falls open.
    if (lobbyName) {
      const tenant = tenantOf(req.user?.orgId);
      let memberIds: string[] | undefined;
      // Carried alongside membership: an org-visible room has no member rows,
      // because nobody has to join what is already open to them. Without this
      // the policy saw an empty member list and denied everyone, including the
      // room's creator.
      let visibility: string | undefined;
      if (lobbyName.startsWith('room:')) {
        const slug = lobbyName.slice('room:'.length);
        // The SAME org resolution the rooms routes use to create and list
        // rooms. This read `orgId ?? ''`, and in local dev (SKIP_AUTH, tokens
        // with no org claim) hangoutRooms.ts falls back to 'default-org' —
        // so every room was WRITTEN under 'default-org' and looked up here
        // under '', the lookup missed, membership could not be established,
        // and the policy denied every request for a room's recordings. The
        // two routes disagreeing about the caller's org is the bug; this
        // makes them agree.
        //
        // Deliberately scoped to the room LOOKUP. The principal's tenant
        // below still comes from the real claim: substituting the placement
        // fallback there would fold every org-less token into one tenant,
        // which lib/org-claim.ts warns about at length.
        const lookupOrgId = req.user?.orgId
          ?? (process.env.SKIP_AUTH === 'true' ? 'default-org' : '');
        const room = await hangoutRoomRepo.getRoomBySlug(lookupOrgId, slug).catch(() => null);
        if (room) {
          visibility = (room as { visibility?: string }).visibility;
          const members = await hangoutRoomMemberRepo.listMembers(room.roomId).catch(() => []);
          memberIds = members.map((m: { userId: string }) => m.userId);
        }
      }
      const attrs: Record<string, unknown> = {};
      if (memberIds) attrs.memberIds = memberIds;
      if (visibility) attrs.visibility = visibility;
      await requireAuthorized({
        principal: principalOf(req.user, tenant),
        action: 'recording:listByLobby',
        resource: {
          type: 'lobby',
          id: lobbyName,
          ...(Object.keys(attrs).length > 0 ? { attrs } : {}),
        },
      });
      const { items, cursor: next } = await callRecordingRepo.listByLobbyName(lobbyName, limit, cursor);
      res.json({ items: await settleComposeForList(items), cursor: next ?? null });
      return;
    }

    const { items, cursor: next } = await callRecordingRepo.listByUser(userId, { limit, cursor });
    res.json({ items: await settleComposeForList(items), cursor: next ?? null });
  }),
);

/**
 * Give every row in a list page a compose status that is actually TRUE.
 *
 * The stored status is not enough on its own, and the stored-status-only
 * version of this would have shipped a badge that lies. Measured against
 * live LVS on 2026-09-08: of seven recordings, four carried NO
 * `composeStatus` in DDB at all — and `deriveComposeStatus` calls all four
 * `queued`, because a row with legs and no composed file is indistinguishable
 * from one whose ffmpeg job has not started. LVS's own answer for those four
 * was two `ok` and two `failed`. So the derived value was wrong in both
 * directions: two working recordings would have shown "still assembling"
 * forever, and two dead ones would have too.
 *
 * The reason the stored value is empty is structural, and it is the same one
 * `resolveMedia` documents for the detail route: LVS fires
 * `recording.completed` BEFORE the background compose job runs and never
 * fires a second webhook. Nothing pushes the answer; somebody has to pull it.
 * Until now only the detail route pulled, so a recording's status was
 * populated only if a user had already opened it — and the list is exactly
 * where you look BEFORE opening one.
 *
 * Cost control, in three parts:
 *   - Only unsettled rows are asked about. `ok`/`degraded`/`failed`/`skipped`
 *     never change, and `resolveMedia` caches them onto the row, so a given
 *     recording is fetched once in its life and never again.
 *   - Requests are grouped by `channelArn`, because LVS's endpoint returns a
 *     whole channel's recordings in one call. A room with 40 recordings on
 *     one channel costs one request, not 40.
 *   - Capped at MAX_CHANNELS distinct channels per page. Past that the
 *     remaining rows keep their derived status, which reads as "processing" —
 *     the same honest not-yet the detail route falls back to when LVS is
 *     unreachable.
 *
 * Never throws. A list that renders without badges is a small loss; a list
 * that 500s because the SFU is down is a large one.
 */
const MAX_RECONCILE_CHANNELS = 8;

async function settleComposeForList<
  T extends { callId: string; recordingId: string; channelArn: string; composeStatus?: ComposeStatus },
>(items: T[]): Promise<T[]> {
  const unsettled = items.filter(
    (r) => !r.composeStatus || !SETTLED_COMPOSE.has(r.composeStatus),
  );
  if (unsettled.length === 0) return items;

  const byChannel = new Map<string, T[]>();
  for (const r of unsettled) {
    if (!r.channelArn) continue;
    const bucket = byChannel.get(r.channelArn);
    if (bucket) bucket.push(r);
    else byChannel.set(r.channelArn, [r]);
  }

  const resolved = new Map<string, ComposeStatus>();
  const channels = Array.from(byChannel.entries()).slice(0, MAX_RECONCILE_CHANNELS);
  await Promise.all(
    channels.map(async ([arn, rows]) => {
      try {
        const res = await callSfu(
          `${VIDEO_API_URL}/api/channels/${encodeURIComponent(arn)}/recordings`,
          { method: 'GET', headers: { Authorization: `Bearer ${process.env.VIDEO_ADMIN_TOKEN || 'admin'}` } },
        );
        if (!res.ok) return;
        const body = (await res.json()) as
          | { recordings?: Array<Record<string, unknown>> }
          | Array<Record<string, unknown>>;
        const live = Array.isArray(body) ? body : (body.recordings ?? []);
        const liveById = new Map(live.map((r) => [r?.recordingId as string, r]));
        for (const row of rows) {
          const hit = liveById.get(row.recordingId);
          if (!hit) continue;
          const described = describeMedia({
            composeStatus: hit.composeStatus as ComposeStatus | undefined,
            composedSegment: hit.composedSegment as { key?: string; url?: string; size?: number } | undefined,
            segments: hit.segments as Array<{ key?: string; url?: string; size?: number }> | undefined,
            tracks: hit.tracks as Array<{ key?: string; url?: string; size?: number }> | undefined,
          });
          resolved.set(row.recordingId, described.composeStatus);
          // Same write-back the detail route does, and the reason this whole
          // function converges to zero LVS traffic instead of asking forever.
          if (SETTLED_COMPOSE.has(described.composeStatus)) {
            const seg = hit.composedSegment as { key?: string; url?: string; size?: number } | undefined;
            void callRecordingRepo
              .updateComposeState(row.callId, row.recordingId, {
                composeStatus: described.composeStatus,
                // `key` + `size` only — LVS's `composedSegment.url` is the
                // recorder's absolute filesystem path.
                composedSegment: seg ? { key: seg.key ?? seg.url, size: seg.size } : undefined,
              })
              .catch(() => { /* best-effort cache */ });
          }
        }
      } catch (e) {
        log.warn(
          { err: e instanceof Error ? e.message : String(e), arn },
          '[recordings] list compose reconcile failed — rows keep their stored status',
        );
      }
    }),
  );

  if (resolved.size === 0) return items;
  return items.map((r) => {
    const status = resolved.get(r.recordingId);
    return status ? { ...r, composeStatus: status } : r;
  });
}

/**
 * Resolve a recording from its id alone, and prove the caller was in it.
 *
 * Every id-only route used to do this by hand:
 *
 *     const { items } = await callRecordingRepo.listByUser(userId, { limit: 100 });
 *     const match = items.find((r) => r.recordingId === recordingId);
 *     if (!match) throw new NotFoundError(...);
 *
 * Six copies, and all six were wrong in the same way. The fan-out index
 * writes one row per participant per recording, so `limit: 100` is a page,
 * not a universe — a user with 101 recordings got a hard 404 on their
 * 101st, for a recording sitting in the table. Nothing about the failure
 * looked like a paging bug from the outside; it looked like the recording
 * was gone.
 *
 * Now: one GSI point-query for the recording, one point-get for the
 * caller's fan-out row. No page to fall off, no per-route re-derivation of
 * the authorization rule, and the fan-out row stays the source of truth for
 * "were you in this call" so the GDPR untag path still means something.
 */
async function requireOwnRecording(
  req: Request,
  recordingId: string,
): Promise<CallRecording> {
  const userId = (req as any).user?.userId ?? (req as any).user?.sub;
  const resolved = await callRecordingRepo.resolveForUser(userId, recordingId);
  if (!resolved) {
    throw new NotFoundError('recording not found');
  }
  // NOTE — deliberately no `resource.tenant`.
  //
  // `CallRecording.orgId` is populated on 0 of 200 live rows: the recording
  // webhook is an HMAC'd machine call with no JWT, so the writer has no
  // tenant to stamp. Passing a tenant here would mean every org-bearing
  // user is denied on every recording, because the framework's pre-policy
  // tenant check compares against a field that is always absent. Tenancy
  // gets wired here only after the webhook can resolve it from the
  // VideoSession — orgId first, then keys, then this line.
  await requireAuthorized({
    principal: principalOf(req.user, tenantOf(req.user?.orgId)),
    action: 'recording:read',
    resource: {
      type: 'recording',
      id: recordingId,
      attrs: { isParticipant: resolved.isParticipant },
    },
  });
  return resolved.detail;
}

/**
 * Terminal compose states — the ones that will not change again, so we can
 * stop asking LVS about them.
 */
const SETTLED_COMPOSE: ReadonlySet<string> = new Set(['ok', 'degraded', 'failed', 'skipped']);

/**
 * Reconcile this recording's compose state with LVS, which is the only
 * process that knows it.
 *
 * The `recording.completed` webhook fires at finalize time — BEFORE the
 * background ffmpeg compose job runs — and carries neither `composeStatus`
 * nor a real `composedUrl` (nothing in RecordingManager ever assigns
 * `record.composedUrl`, so that webhook field is always undefined). LVS
 * then never fires a second webhook when compose lands; it only publishes
 * an in-cluster SSE event, which this service does not subscribe to. So
 * every row this service holds is frozen at "compose has not started" and
 * always will be, unless somebody asks.
 *
 * We ask, once, and cache the answer on the row. `queued`/`running` get
 * re-asked; the four terminal states never do.
 *
 * When LVS is unreachable we fall back to the row — which yields
 * 'processing' or 'unavailable' rather than a wrong claim of playability.
 * An honest "we don't know yet" is the correct answer to a compose service
 * we cannot reach.
 */
async function resolveMedia(detail: CallRecording): Promise<{
  composeStatus: ComposeStatus;
  mediaState: 'playable' | 'processing' | 'unavailable';
  playbackFilename: string | null;
}> {
  if (detail.composeStatus && SETTLED_COMPOSE.has(detail.composeStatus)) {
    return describeMedia(detail);
  }
  try {
    const listRes = await callSfu(
      `${VIDEO_API_URL}/api/channels/${encodeURIComponent(detail.channelArn)}/recordings`,
      { method: 'GET', headers: { Authorization: `Bearer ${process.env.VIDEO_ADMIN_TOKEN || 'admin'}` } },
    );
    if (listRes.ok) {
      const body = (await listRes.json()) as
        | { recordings?: Array<Record<string, unknown>> }
        | Array<Record<string, unknown>>;
      const rows = Array.isArray(body) ? body : (body.recordings ?? []);
      const live = rows.find((r) => r?.recordingId === detail.recordingId);
      if (live) {
        const merged = {
          ...detail,
          composeStatus: live.composeStatus as ComposeStatus | undefined,
          composedSegment: live.composedSegment as { key?: string; url?: string } | undefined,
          segments: (live.segments as CallRecording['segments']) ?? detail.segments,
          tracks: (live.tracks as CallRecording['tracks']) ?? detail.tracks,
        };
        const described = describeMedia(merged);
        // Cache it so this round trip stops once compose settles. Fire and
        // forget — a failed write only costs us the next lookup.
        if (SETTLED_COMPOSE.has(described.composeStatus)) {
          void callRecordingRepo
            .updateComposeState(detail.callId, detail.recordingId, {
              composeStatus: described.composeStatus,
              // `key` and `size` only. LVS's `composedSegment.url` is the
              // recorder pod's absolute path — literally
              // `file:///Users/<developer>/…` in dev — and persisting it
              // would plant a filesystem-layout leak in DDB for every
              // future reader to serve. The key is all the playback URL is
              // built from; the raw url has no consumer.
              composedSegment: (() => {
                const s = live.composedSegment as { key?: string; url?: string; size?: number } | undefined;
                if (!s) return undefined;
                return { key: s.key ?? s.url, size: s.size };
              })(),
            })
            .catch(() => { /* best-effort cache */ });
        }
        return described;
      }
    }
  } catch (e) {
    log.warn(
      { err: e instanceof Error ? e.message : String(e), recordingId: detail.recordingId },
      '[recordings] compose-status reconcile failed — reporting from the stored row',
    );
  }
  return describeMedia(detail);
}

// --------------------------------------------------------------------
// GET /api/recordings/:recordingId — single recording details
// --------------------------------------------------------------------
recordingsRouter.get(
  '/recordings/:recordingId',
  requireAuth,
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const { recordingId } = req.params;
    const detail = await requireOwnRecording(req, recordingId);
    const media = await resolveMedia(detail);
    res.json({ ...publicRecordingView(detail), ...mediaView(detail, media) });
  }),
);

/**
 * The media half of a recording response: what state its video is in, and
 * the URL — if any — that will actually play.
 *
 * `composedUrl` is `null` rather than a guess. The route this replaces
 * ended its file-picking chain with `?? 'composed.mp4'`, so a recording
 * with no composed media still got a confident URL, the browser 404'd on
 * it, and the player reported "Playback error." Three distinct situations
 * — compose still running, compose skipped with nothing to show, and an
 * actual playback failure — arrived at the user as one four-word lie.
 */
function mediaView(
  detail: CallRecording,
  media: { composeStatus: ComposeStatus; mediaState: string; playbackFilename: string | null },
): Record<string, unknown> {
  const url = media.playbackFilename
    ? `${VIDEO_PUBLIC_URL}/api/channels/${encodeURIComponent(detail.channelArn)}/recordings/${encodeURIComponent(detail.recordingId)}/segments/${encodeURIComponent(media.playbackFilename)}`
    : null;
  return {
    composeStatus: media.composeStatus,
    mediaState: media.mediaState,
    composedUrl: url,
    playlistUrl: url,
  };
}

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
    // `tracks[]` was scrubbed; `composedSegment` was not, and it carries the
    // same recorder-pod absolute path in `.url`. Measured on a live row: a
    // clean `tracks[]` next to
    // `composedSegment.url: file:///Users/<developer>/…` in the very same
    // response. Scrubbing one field and not its neighbour is how a fix
    // looks finished while the leak is still on the wire, so this drops the
    // raw url and keeps only what the client has any use for. `composedUrl`
    // on the response is the proxied, playable address.
    composedSegment: detail.composedSegment
      ? {
          key: storageKeyOf(detail.composedSegment.key ?? detail.composedSegment.url),
          size: detail.composedSegment.size,
        }
      : undefined,
  };
}

/**
 * A blob-storage key, never a filesystem path.
 *
 * Keys look like `arn_local_ivs_channel_x/<recordingId>/composed.mp4`.
 * Absolute paths and `file://` URIs describe the recorder pod's disk and
 * must not leave the process; when that is all we have, the filename alone
 * carries every bit of information a client could use.
 */
function storageKeyOf(candidate: string | undefined): string | undefined {
  if (!candidate) return undefined;
  if (candidate.includes('://') || candidate.startsWith('/')) {
    return candidate.split('/').pop();
  }
  return candidate;
}

// --------------------------------------------------------------------
// POST /api/recordings/:recordingId/playback-token
// --------------------------------------------------------------------
recordingsRouter.post(
  '/recordings/:recordingId/playback-token',
  requireAuth,
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const { recordingId } = req.params;
    const detail = await requireOwnRecording(req, recordingId);

    // Resolve what there actually is to play BEFORE minting anything. When
    // the answer is "nothing yet" or "nothing ever", say so and stop —
    // there is no point signing a token for a file that does not exist,
    // and handing the client a URL anyway is precisely how three different
    // situations all became "Playback error."
    const media = await resolveMedia(detail);
    if (!media.playbackFilename) {
      res.json({
        ...mediaView(detail, media),
        token: null,
        expiresAt: null,
      });
      return;
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
          // Carry LVS's own words. Without them a 403 here reads as an
          // authorization bug in this route, when the actual cause is
          // config: LVS's admin wildcard (bearerAuth.js) is gated on
          // ADMIN_TOKEN being set, and when it is unset the literal
          // 'admin' fallback below falls through to RS256 verification
          // and 403s with `{"error":"invalid credentials"}`. The same
          // unset var makes GET /api/channels 500 with "server
          // misconfigured: ADMIN_TOKEN unset" — the honest signal that
          // this 403 was hiding. Cost a full diagnosis pass 2026-09-08.
          const detailText = await lvsRes.text().catch(() => '');
          throw new AppError(
            502,
            `LVS playback-token mint failed: ${lvsRes.status}${detailText ? ` — ${detailText.slice(0, 200)}` : ''}`,
          );
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

    // The playable URL. Recordings are raw MP4s under
    // `<recordingDir>/<arnSafe>/<recordingId>/`, served by LVS at
    // `GET /:arn/recordings/:id/segments/:filename` with byte-range
    // support — not the DVR HLS subsystem, which never held these files
    // and so consistently 404'd. Hand the FE a direct MP4 URL and let
    // <video src> handle seek; the token mint above is preserved for
    // private-channel callers, the segments route itself is CORS-open.
    //
    // `mediaView` builds the URL from the ONE filename `resolveMedia`
    // established actually exists. `playlistUrl` is kept alongside
    // `composedUrl` because RecordingPlayer reads that name.
    res.json({
      ...mediaView(detail, media),
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
    // share-links.
    const match = await requireOwnRecording(req, recordingId);

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

    // Share links used to hand out a `/dvr/playlist.m3u8` URL. The DVR HLS
    // subsystem synthesizes a VOD manifest from time-windowed LIVE segments
    // and has never held a single recording file — the authed playback path
    // moved off it for exactly that reason and this one was left behind, so
    // every share link minted since then pointed at nothing. Same resolution
    // as the authed route, so the two cannot drift apart again.
    const media = await resolveMedia(detail);
    res.json({
      ...mediaView(detail, media),
      expiresAt: link.expiresAt,
    });
  }),
);

/**
 * How long a transcript may sit un-produced before we call it.
 *
 * Transcription runs about a second per leg on a warm worker, so anything
 * still 'pending' or 'processing' twenty minutes after the call ended is
 * not slow — the worker is not running, or the job was lost. The number is
 * deliberately generous; what matters is that a finite one exists at all,
 * because "no deadline" is what turned a 202 into an infinite poll.
 */
const TRANSCRIPT_STALL_MS = Number(process.env.TRANSCRIPT_STALL_MS ?? 20 * 60 * 1000);

/**
 * Write the in-flight transcript response, including whether the client
 * should ever ask again.
 *
 * The old body was `{ status, recordingId, error, generatedAt }` — true as
 * far as it went, and useless for deciding what to do next. A client
 * looking at `status: 'pending'` can only re-poll, and since nothing ever
 * moves a lost job off 'pending', it re-polls until the tab closes.
 *
 * Two fields fix that:
 *   `terminal`     — false means the answer may change; true means it will
 *                    not, stop asking.
 *   `retryAfterMs` — how long to wait, so the backoff schedule lives with
 *                    the service that knows the job's timing rather than
 *                    being guessed at in the browser.
 *
 * A stalled job reports `status: 'stalled'`, which is a real distinction
 * from 'failed': 'failed' means the worker ran and could not produce a
 * transcript, 'stalled' means the job has sat un-progressed past the
 * deadline. Both are terminal, and conflating them would lose the one
 * detail that tells an operator which system to go look at.
 *
 * What 'stalled' does NOT distinguish — and cannot, from this row — is a
 * job nothing ever picked up from one the message queue redrove three
 * times and dropped into the DLQ. Neither outcome writes anything back
 * here: the transcription worker is a separate container that only ever
 * reports success (via `transcription:transcript.ready`), and the DLQ this
 * service persists is the pipeline one, keyed by messageId with no
 * recordingId to join on. So the row shows 'pending' with no error and no
 * attempt count in both cases. That ambiguity is stated rather than
 * papered over with a made-up status; resolving it needs a signal that
 * does not exist yet (a worker-side 'processing' write, or a DLQ lookup
 * keyed by recording), not a new value in this enum.
 */
function writeTranscriptProgress(
  res: Response,
  recordingId: string,
  detail: CallRecording,
): void {
  const status = detail.transcriptStatus;
  // Measure from when the job last moved, not from when the CALL ended.
  //
  // Those are the same instant for a fresh recording, which is exactly why
  // `endedAt` survived here as long as it did. They are nothing like the
  // same instant for a replayed or re-triggered one: a job enqueued thirty
  // seconds ago against a month-old recording reported
  // `waitedMs: 32106080` and was declared stalled before any worker could
  // possibly have looked at it.
  //
  // `transcriptStatusAt` is stamped by every `updateTranscriptStatus`
  // write, so it means "when we last saw this job move" — which is the
  // question the stall deadline is actually asking. Rows written before
  // that field existed fall back to `endedAt` and behave as they did.
  const clockedFrom = detail.transcriptStatusAt ?? detail.endedAt;
  const clockedFromMs = new Date(clockedFrom).getTime();
  const waitedMs = Date.now() - clockedFromMs;
  const stalled =
    (status === 'pending' || status === 'processing')
    && Number.isFinite(waitedMs)
    && waitedMs > TRANSCRIPT_STALL_MS;
  const terminal = status === 'failed' || stalled;
  // Backoff, not a fixed interval: a job that just started is worth asking
  // about soon, one that has been running ten minutes is not.
  const retryAfterMs = terminal
    ? null
    : Math.min(60_000, Math.max(5_000, Math.round(waitedMs / 10) || 5_000));
  if (retryAfterMs) {
    res.setHeader('Retry-After', String(Math.ceil(retryAfterMs / 1000)));
  }
  res.status(202).json({
    status: stalled ? 'stalled' : status,
    recordingId,
    terminal,
    retryAfterMs,
    waitedMs: Number.isFinite(waitedMs) ? waitedMs : null,
    // Which clock produced `waitedMs`. Without it a client reading a
    // seven-figure wait cannot tell a genuinely old job from the bug this
    // field was added alongside.
    waitedFrom: Number.isFinite(clockedFromMs) ? new Date(clockedFromMs).toISOString() : null,
    error: detail.transcriptError ?? null,
    generatedAt: detail.transcriptGeneratedAt ?? null,
    // The summary is a separate job over the transcript. It cannot be
    // further along than the transcript is, so while this endpoint is
    // answering 202 the summary is at best pending — reported anyway so
    // one shape carries both axes wherever a client looks.
    summaryStatus: detail.summaryStatus ?? null,
    summaryError: detail.summaryError ?? null,
  });
}

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
    const detail = await requireOwnRecording(req, recordingId);

    const status = detail.transcriptStatus;
    if (status === undefined) {
      throw new NotFoundError('transcription not requested for this recording');
    }
    if (status !== 'ready') {
      // Surface the in-flight status — and, crucially, whether it is still
      // worth asking again. Without that second half the client had no way
      // to tell "nearly done" from "never coming" and polled forever;
      // six identical requests for one recording were observed today, for
      // a transcript that does not exist and will not be created.
      writeTranscriptProgress(res, recordingId, detail);
      return;
    }
    if (!detail.transcriptKey) {
      // Shouldn't happen — ready implies key present. Defensive 500.
      throw new AppError(500, 'transcript ready but key missing');
    }

    // A ready transcript can sit next to a failed summary — they are two
    // jobs now, not one status. A client that fetches only the transcript
    // learns the other half here instead of having to guess from an empty
    // summary panel.
    res.setHeader(
      'X-Summary-Status',
      detail.summaryStatus ?? (detail.summaryMarkdown ? 'ready' : 'none'),
    );

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
    const { recordingId } = req.params;
    const detail = await requireOwnRecording(req, recordingId);

    const status = detail.transcriptStatus;
    if (status === undefined) {
      throw new NotFoundError('transcription not requested for this recording');
    }
    if (status !== 'ready') {
      // Same bounded contract as /transcript — the summary can't arrive
      // before the transcript it summarizes, so if that one is never
      // coming, neither is this one, and the client is told so.
      writeTranscriptProgress(res, recordingId, detail);
      return;
    }

    // Every response past this point carries the summary's own state in a
    // header, because the body is markdown and has nowhere to put it. A
    // client that has never heard of the header ignores it and renders the
    // body, which is why the failure case below answers 200 with an
    // explanation rather than a status code the old client turns into
    // `null` and an empty panel.
    res.setHeader(
      'X-Summary-Status',
      detail.summaryStatus ?? (detail.summaryMarkdown ? 'ready' : 'none'),
    );

    if (detail.summaryStatus === 'failed') {
      // The transcript is ready — this route only gets here when it is —
      // and only the summarisation over it failed. These used to be one
      // status, so an LLM outage reported the TRANSCRIPTION as failed and
      // the readable transcript went unmentioned. Say which half broke,
      // and point at the half that works.
      const detailText = detail.summaryError ? `: ${detail.summaryError}` : '.';
      res
        .type('text/markdown')
        .send(
          `_Summary generation failed${detailText}_\n\n`
          + '_The transcript itself is complete and unaffected — open the '
          + 'Transcript tab to read it._',
        );
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
    const detail = await requireOwnRecording(req, recordingId);
    // Only the user who STARTED the recording can delete it. Other
    // participants can untag themselves via deleteByUserId (W11/H7).
    // Fail closed when startedBy is missing — historical rows that
    // pre-date startedBy must NOT become deletable by anyone (audit C5).
    if (!detail.startedBy || detail.startedBy !== userId) {
      throw new AppError(403, 'only the recording owner can delete');
    }
    await callRecordingRepo.deleteRecording(detail.callId, recordingId);
    log.info({ recordingId, userId }, '[recordings] deleted by owner');
    res.status(204).end();
  }),
);

