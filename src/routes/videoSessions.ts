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
import { videoSessionRepo, callReactionRepo, profileRepo, workSourceAccountRepo } from '../repositories';
import { decideRecording } from '../recordings/recordingDecision';
import { VideoSessionRecord } from '../repositories/VideoSessionRepository';
import {
  asyncHandler,
  AppError,
  ValidationError,
  NotFoundError,
  ForbiddenError,
} from '../middleware/error-handler';
import log from '../lib/logger';
import { startLiveCaptions, stopLiveCaptions } from '../recordings/liveCaptions';
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
import { lobbyAccess, tenantLobby } from '../lib/lobbyAccess';
import { mayUsePartition } from '../lib/partition';
import { lvsSessionAuthHeader } from '../lib/lvsServiceAuth';
import {
  parseCreateFields,
  parsePatchFields,
  isCallParticipant,
  hostOf,
  withCallFields,
} from '../lib/documentCall';
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
 * NFR #175: a body-supplied participantId must name the caller's own entry in
 * the session (or, for /end, the caller started the session and may clear a
 * stale entry). An id the mirror does not list is not refused — LVS validates
 * it against the forwarded JWT — but the participant row that IS listed under
 * someone else is 403.
 */
function assertOwnParticipant(
  session: VideoSessionRecord,
  participantId: string | undefined,
  userId: string | undefined,
  opts: { ownerMayAct?: boolean } = {},
): void {
  if (!participantId) return;
  const entry = (session.participants ?? []).find((p) => p.participantId === participantId);
  if (!entry || !entry.userId || entry.userId === userId) return; // a legacy entry with no userId cannot be attributed
  if ((opts.ownerMayAct ?? true) && userId && session.startedBy === userId) return;
  throw new ForbiddenError('That participant is not you');
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

/**
 * Release a participant that platform-api has decided is gone, in BOTH of the
 * places LVS counts it.
 *
 * There are two independent registries on the LVS side and they are decremented
 * by different routes:
 *
 *   - `sessionStore` — the participant SET that produces `participantsRemaining`
 *     on POST /api/sessions/:arn/end. This is the number our /end route reads to
 *     decide whether the session is over, and the ONLY thing that decrements it
 *     is that same /end route. Nothing reaps it.
 *   - mediasoup `RoomManager` — the live transports. DELETE
 *     /api/channels/:arn/participants/:pid evicts these (see
 *     `reapLvsParticipant`) and does NOT touch `sessionStore`
 *     (live-video-streaming src/api/channels/index.js:1083).
 *
 * So a reap alone leaves the ref-count above zero forever. Both calls are
 * needed, and both are best-effort: the worst case is the SFU's own reaper
 * sweeps the transports and this session waits for `reapStaleSessions`.
 *
 * Only safe to call AFTER the replacement participant has been registered
 * upstream — LVS hard-ends the channel when a soft-end takes the count to zero.
 */
async function releaseLvsParticipant(args: {
  channelArn: string;
  participantId: string;
  sessionId: string;
  authHeader?: string;
}): Promise<void> {
  if (args.authHeader) {
    try {
      await callSfu(
        `${VIDEO_API_URL}/api/sessions/${encodeURIComponent(args.channelArn)}/end`,
        {
          method: 'POST',
          headers: { Authorization: args.authHeader, 'Content-Type': 'application/json' },
          body: JSON.stringify({ participantId: args.participantId }),
        },
      );
    } catch (endErr) {
      log.warn(
        { err: endErr, sessionId: args.sessionId, participantId: args.participantId },
        '[videoSessions] LVS sessionStore release failed (non-fatal)',
      );
    }
  }
  const outcome = await reapLvsParticipant({
    channelArn: args.channelArn,
    participantId: args.participantId,
    sessionId: args.sessionId,
  });
  recordLvsParticipantReap(outcome);
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
    // The call's partition: a tenant-prefixed lobby (`social:dm:…`) lives in
    // its tenant when the caller may use it (lib/partition.ts), else the
    // caller's own org, as before.
    const lobbyTenant = tenantLobby(lobbyName)?.tenant;
    const organizationId = lobbyTenant && mayUsePartition(req.user, lobbyTenant) ? lobbyTenant : req.user!.orgId;
    // NFR #175: only someone who may use this lobby opens (or finds) its call.
    await lobbyAccess().assertLobbyAccess(req, lobbyName);
    // In-document calls: `kind: 'document-review'` + title / documentIds /
    // media. Null for every other caller, whose rows stay exactly as before.
    const callFields = parseCreateFields(body, lobbyName, userId);
    const callFieldsOut = (row?: VideoSessionRecord | null) => {
      const src = row?.kind ? row : callFields;
      if (!src) return {};
      return { kind: src.kind, title: src.title, documentIds: src.documentIds, media: src.media, hostUserId: src.hostUserId };
    };

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
            reason?: string;
          })
        : null;

    // The recording DECISION, made once and kept: a DM records only when every
    // party has opted in on their profile (whatever the caller asked for), a
    // room records what its setting says, anything else keeps the default.
    // Stamped on the row and returned to the caller so the call chrome can
    // show "armed" and the thread card can later say why there is none.
    // In-document calls do NOT record unless someone asks: the start drawer
    // has no record option and nothing in the call says "recording" (mockups
    // 01–06), so the legacy default-on made every review record itself and
    // post "No speech was found" transcripts. Only an explicit
    // `recordingConfiguration.enabled: true` goes through the usual decision.
    const documentCallNotRecorded = callFields && incomingRecordingConfig?.enabled !== true;
    const recordingDecision: { enabled: boolean; reason: string } = documentCallNotRecorded
      ? { enabled: false, reason: 'document-call-not-recorded' }
      : await decideRecording({
        lobbyName,
        requested: incomingRecordingConfig
          ? { enabled: incomingRecordingConfig.enabled, reason: incomingRecordingConfig.reason }
          : null,
        callerId: userId,
        getProfile: (id) => profileRepo.getProfile(id),
      });
    const profileFromBody = (() => {
      const raw = incomingRecordingConfig?.recordingProfile;
      if (raw === undefined) return undefined;
      if (isRecordingProfile(raw)) return raw;
      throw new AppError(400, `invalid recordingProfile: '${String(raw)}'`);
    })();
    const recordingConfiguration = incomingRecordingConfig
      ? {
          enabled: recordingDecision.enabled,
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
        { enabled: recordingDecision.enabled, destination: 'local', transcribe: false };

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
    // `dmg:` too — a big group chat is addressed by a HASH rather than by its
    // members, but it is the same product concept and rings exactly the same
    // way before anyone answers. Matching only `dm:` meant the large groups
    // kept recording their own ringing.
    // A tenant-prefixed lobby (`orgiq:dm:<ids>`) is the same shape under its prefix.
    const lobbyShape = tenantLobby(lobbyName)?.rest ?? lobbyName;
    const isConversationCall = lobbyShape.startsWith('dm:') || lobbyShape.startsWith('dmg:');
    const recordingConfigurationForLobby = isConversationCall
      ? { ...recordingConfiguration, startWhenParticipants: 2 }
      : recordingConfiguration;

    // LVS gates this route; platform-api calls it as a service
    // (VIDEO_SERVICE_TOKEN, lib/lvsServiceAuth.ts) and the caller's bearer
    // stays here. Legacy (unset): the caller's bearer is forwarded.
    const authHeader = lvsSessionAuthHeader(req.headers.authorization);
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
        // A document review started on a lobby whose call was begun without
        // one (the old toolbar Call button) adopts the review fields once;
        // an existing review keeps its own title, list and host.
        let row: VideoSessionRecord | null = existing;
        if (callFields && !existing.kind) {
          try {
            row = await videoSessionRepo.updateDocumentCallFields(lobbyName, existing.sessionId, {
              ...callFields,
              hostUserId: existing.startedBy,
            });
          } catch (patchErr) {
            log.warn({ err: patchErr, sessionId: existing.sessionId }, '[videoSessions] adopting document-call fields failed');
          }
        }
        // The row that made the LVS session holds the decision that governs
        // it; a later joiner's own request did not change what LVS is doing.
        res.status(200).json({ sessionId: existing.sessionId, reused: true, recording: existing.recording ?? recordingDecision, ...callFieldsOut(row) });
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
      ...(organizationId ? { orgId: organizationId } : {}),
      participants: [],
      recording: recordingDecision,
      ...(callFields
        ? Object.fromEntries(Object.entries(callFields).filter(([, v]) => v !== undefined))
        : {}),
    };

    try {
      await videoSessionRepo.createSession(record);
      if (organizationId) {
        await workSourceAccountRepo.ensureVerifiedPrincipal({
          source: 'meeting', organizationId, subject: userId,
        });
      }
      log.info({ sessionId, lobbyName, channelArn, reused, recording: recordingDecision }, '[videoSessions] DDB row written');

      // Close out this lobby's PREVIOUS active rows, which are now provably
      // stale: LVS has just named the channel that is current, so any row
      // still marked active on a different channel belongs to a call that is
      // over. This is the second way a lobby accumulates active rows — the
      // reuse branch above only returns early when the arns MATCH, so every
      // call that starts a fresh channel used to leave its predecessor behind.
      //
      // Same-arn rows are deliberately left alone: two clients racing to
      // create the same lobby converge on one channel and both rows are real.
      const previous = await videoSessionRepo.getSessionsByDocument(lobbyName);
      for (const stale of previous) {
        if (stale.status !== 'active') continue;
        if (stale.sessionId === sessionId || stale.channelArn === channelArn) continue;
        await videoSessionRepo.endSession(lobbyName, stale.sessionId, now);
        log.info(
          { sessionId: stale.sessionId, lobbyName, supersededBy: sessionId },
          '[videoSessions] ended superseded session row',
        );
      }
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

    res.status(reused ? 200 : 201).json({ sessionId, reused, recording: recordingDecision, ...callFieldsOut() });
  } catch (err) {
    if (err instanceof AppError) throw err;
    log.error({ err }, '[videoSessions] POST /sessions error');
    throw new AppError(500, 'Failed to create video session');
  }
}));

// POST /api/video/sessions/guests — bring a third person into a private call.
// Body { lobbyName, userIds }. A DM's call admits the people its name lists;
// someone already allowed on the lobby may add more — admitting a knock, or
// inviting from the call's People control — and they are allowed on the lobby
// while its ACTIVE session lasts (lib/lobbyAccess.ts, "Guests"). The caller
// must be allowed on the lobby (404 otherwise, as every route here), and the
// lobby must have an active call (404: there is nothing to be let into).
// Idempotent. Answers { sessionId, guests }.
const MAX_GUESTS_PER_REQUEST = 20;
videoSessionsRouter.post('/sessions/guests', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const body = (req.body ?? {}) as Record<string, unknown>;
  const lobbyName = resolveLobbyName(body);
  if (!lobbyName) throw new ValidationError('lobbyName is required');
  const raw = body.userIds;
  const userIds = Array.isArray(raw)
    ? [...new Set(raw.filter((u): u is string => typeof u === 'string' && u.length > 0 && u.length <= 256))]
    : [];
  if (userIds.length === 0) throw new ValidationError('userIds must be a non-empty array of user ids');
  if (userIds.length > MAX_GUESTS_PER_REQUEST) throw new ValidationError(`at most ${MAX_GUESTS_PER_REQUEST} userIds`);
  await lobbyAccess().assertLobbyAccess(req, lobbyName);
  const live = await videoSessionRepo.findActiveByLobbyName(lobbyName);
  if (!live) throw new NotFoundError('No call in progress');
  const updated = await videoSessionRepo.addGuests(lobbyName, live.sessionId, userIds);
  if (!updated) throw new NotFoundError('No call in progress');
  log.info({ lobbyName, sessionId: live.sessionId, by: req.user!.sub, guests: userIds }, '[videoSessions] guests added');
  res.status(200).json({ sessionId: live.sessionId, guests: updated.guests ?? [] });
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
    // Stable per-TAB key. Optional: when absent, joins collapse on userId
    // instead (see `participantSupersedes`). Threading one through from the
    // client is what keeps two tabs of one user ref-counting separately.
    const clientId =
      typeof body.clientId === 'string' && body.clientId.length > 0 ? body.clientId : undefined;

    if (!lobbyName) {
      throw new ValidationError('lobbyName (or documentId) is required in request body');
    }
    // NFR #175: a media token only for someone who may use this lobby.
    await lobbyAccess().assertLobbyAccess(req, lobbyName);

    const session = await videoSessionRepo.getSession(lobbyName, sessionId);
    if (!session) {
      throw new NotFoundError('Session not found');
    }

    const userId = req.user!.sub;
    const authHeader = lvsSessionAuthHeader(req.headers.authorization);
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

    // Mirror to DDB for past-calls history, IDEMPOTENTLY.
    //
    // The old check here was `participants.some(p => p.participantId === id)`,
    // which could never match: LVS mints participantId as
    // `${userId}-${Date.now().toString(36)}` on every join, so the id we are
    // testing for is one we have never seen before by construction. Every
    // repeat join therefore appended — and a room entry joins TWICE (the room
    // bridge in hangoutRooms.ts, then the call widget) while hanging up once,
    // so each call left one participant behind and the ref-count never reached
    // zero. Four rows stuck 'active' on one room is what that looks like.
    //
    // `addOrReplaceParticipant` collapses on the client key instead, and hands
    // back whatever it displaced so we can release it upstream too — LVS's own
    // participant set is the number our /end route trusts.
    const participantId = tokenData.participantId;
    const participant = {
      userId,
      participantId,
      ...(clientId ? { clientId } : {}),
      displayName: displayName || userId,
      joinedAt: new Date().toISOString(),
    };
    let displaced: Array<{ participantId: string }> = [];
    try {
      const result = await videoSessionRepo.addOrReplaceParticipant(
        lobbyName,
        sessionId,
        participant,
      );
      displaced = result?.displaced ?? [];
      log.info(
        { userId, participantId, clientId, sessionId, displaced: displaced.length, remaining: result?.remaining },
        '[videoSessions] Added participant to DDB',
      );
    } catch (dbErr) {
      log.error({ err: dbErr }, '[videoSessions] Failed to add participant to local record');
    }

    // Superseded entries are stale registrations for THIS client — a second
    // join from the same tab. Release them in LVS as well, or its
    // `participantsRemaining` stays above zero and /end never hard-ends.
    // Safe here and only here: the replacement is already registered upstream,
    // so this cannot take the count to zero and tear down a live channel.
    for (const stale of displaced) {
      if (!stale.participantId || stale.participantId === participantId) continue;
      await releaseLvsParticipant({
        channelArn: session.channelArn,
        participantId: stale.participantId,
        sessionId,
        authHeader,
      });
    }

    res.status(200).json({ token: tokenData.token, participantId, userId });
  } catch (err) {
    if (err instanceof AppError) throw err;
    log.error({ err }, '[videoSessions] POST /sessions/:id/join error');
    throw new AppError(500, 'Failed to join video session');
  }
}));

/**
 * Retry LVS `/end` for a leave that could not reach the SFU (A15). The
 * record is already settled by the caller; this only lets LVS's own
 * ref-count catch up once it answers again. Idempotent on LVS's side (an
 * unknown session is 404, taken as done); gives up after the last attempt,
 * leaving LVS's reaper to sweep. Exported for tests.
 */
export const LVS_END_RETRY_DELAYS_MS: readonly number[] = [5_000, 15_000, 30_000, 60_000, 120_000];
let lvsEndRetryDelays: readonly number[] = LVS_END_RETRY_DELAYS_MS;
/** Tests only: a retry firing into the next case would eat its fetch mocks. */
export function __setLvsEndRetryDelaysForTests(delays: readonly number[] | null): void {
  lvsEndRetryDelays = delays ?? LVS_END_RETRY_DELAYS_MS;
}
export function deferLvsEnd(
  args: { channelArn: string; participantId: string; authHeader: string; sessionId: string; lobbyName?: string },
  delays: readonly number[] = lvsEndRetryDelays,
): void {
  const attempt = (i: number): void => {
    if (i >= delays.length) {
      log.warn({ sessionId: args.sessionId }, '[videoSessions] deferred LVS /end gave up — the LVS reaper will sweep');
      return;
    }
    const timer = setTimeout(async () => {
      try {
        const res = await callSfu(
          `${VIDEO_API_URL}/api/sessions/${encodeURIComponent(args.channelArn)}/end`,
          {
            method: 'POST',
            headers: { Authorization: args.authHeader, 'Content-Type': 'application/json' },
            body: JSON.stringify({ participantId: args.participantId }),
          },
        );
        log.info({ sessionId: args.sessionId, status: res.status, attempt: i + 1 }, '[videoSessions] deferred LVS /end delivered');
        // LVS is the ref-count's source of truth: when it says nobody is
        // left (or has forgotten the session — a restart), the record
        // follows. The DDB splice done at hang-up time cannot know that
        // on its own: two leaves racing the same read-modify-write each
        // leave the other one behind.
        if (args.lobbyName) {
          const body = res.ok ? await res.json().catch(() => ({})) as { participantsRemaining?: number; endedAt?: string | null } : null;
          const nobodyLeft = res.status === 404 || (body !== null && (body.participantsRemaining ?? 0) === 0);
          if (nobodyLeft) {
            const current = await videoSessionRepo.getSession(args.lobbyName, args.sessionId).catch(() => null);
            if (current && current.status === 'active') {
              await videoSessionRepo.endSession(args.lobbyName, args.sessionId, body?.endedAt || new Date().toISOString());
              log.info({ sessionId: args.sessionId }, '[videoSessions] deferred LVS /end: session ended to match LVS');
            }
          }
        }
      } catch {
        attempt(i + 1);
      }
    }, delays[i]);
    // Never hold the process open for a best-effort retry.
    (timer as { unref?: () => void }).unref?.();
  };
  attempt(0);
}

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
    // NFR #175: the lobby's own people only.
    await lobbyAccess().assertLobbyAccess(req, lobbyName);

    const session = await videoSessionRepo.getSession(lobbyName, sessionId);
    if (!session) {
      throw new NotFoundError('Session not found');
    }

    const clientId =
      typeof body.clientId === 'string' && body.clientId.length > 0 ? body.clientId : undefined;
    // participantId is the intended key and the FE sends it on both the
    // click-to-hang-up and the sendBeacon paths. `clientId` is the second
    // chance: if the beacon lost it (an unload race where the join hadn't
    // resolved yet), the per-tab key still names exactly one entry, whereas
    // the userId fallback below wipes every tab this user has open.
    const participantId =
      (typeof body.participantId === 'string' && body.participantId.length > 0
        ? body.participantId
        : undefined)
      ?? (clientId
        ? (session.participants ?? []).find((p) => p.clientId === clientId)?.participantId
        : undefined);
    // The caller, never a body-named user (app-level requireAuth always sets one).
    const userId = req.user?.sub;
    const authHeader = lvsSessionAuthHeader(req.headers.authorization);
    // NFR #175: the participant must be the caller's own entry — or the caller
    // started the session. Before this, any user could name another person's
    // participantId and the admin-token reap would evict them.
    assertOwnParticipant(session, participantId, userId);

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

    // Authoritative path — tell LVS sessionStore, as a service when
    // VIDEO_SERVICE_TOKEN is set (else with the caller's JWT, legacy). LVS is
    // the source of truth for participant ref-count post Wave 4.
    // A hang-up is the person's decision and it stands whether or not LVS can
    // hear about it (A15): when the SFU is unreachable or answers 5xx, the
    // record is settled here (the DDB path below) and LVS is told later,
    // idempotently — its own /end treats an unknown session as gone. Before,
    // the /end answered 503 and the session stayed `active`, so the lobby
    // kept its Live badge after everyone had hung up.
    let upstream: globalThis.Response | null = null;
    let lvsDeferred = false;
    if (participantId && authHeader) {
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
        const mapped = mapSfuError(sfuErr);
        log.warn(
          { sessionId, status: mapped.status, err: mapped.message },
          '[videoSessions] LVS /end unreachable — ending the record here, LVS end deferred',
        );
        deferLvsEnd({ channelArn: session.channelArn, participantId, authHeader, sessionId, lobbyName });
        lvsDeferred = true;
      }
    }

    if (upstream && participantId) {
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

    // Fallback path: no auth (sendBeacon), no participantId (legacy
    // body-userId only), or LVS unreachable (deferred above). DDB-direct
    // ref-count. LVS sessionStore drifts until the deferred /end or the
    // reaper sweep — acceptable for tab-close and for an SFU outage.
    let remaining = (session.participants ?? []).length;
    if (participantId) {
      const splice = await videoSessionRepo.removeParticipant(lobbyName, sessionId, participantId);
      if (splice) remaining = splice.remaining;
    } else if (userId) {
      const splice = await videoSessionRepo.removeParticipantByUserId(lobbyName, sessionId, userId);
      if (splice) remaining = splice.remaining;
    }

    if (remaining > 0) {
      log.info({ userId: userId ?? '(anon)', sessionId, remaining, lvsDeferred }, '[videoSessions] participant left session (fallback)');
      res.status(200).json({ remaining, ended: false, ...(lvsDeferred ? { lvsDeferred } : {}) });
      return;
    }

    const now = new Date().toISOString();
    try {
      await videoSessionRepo.endSession(lobbyName, sessionId, now);
      log.info({ sessionId }, '[videoSessions] Ended session (fallback last-out)');
    } catch (dbErr) {
      log.error({ err: dbErr }, '[videoSessions] Failed to update local record on end');
    }

    res.status(200).json({ remaining: 0, ended: true, ...(lvsDeferred ? { lvsDeferred } : {}) });
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
    // NFR #175: lobby access, then the participant must be the caller's own.
    await lobbyAccess().assertLobbyAccess(req, lobbyName);

    const session = await videoSessionRepo.getSession(lobbyName, sessionId);
    if (!session) {
      throw new NotFoundError('Session not found');
    }
    assertOwnParticipant(session, participantId, req.user?.sub, { ownerMayAct: false });

    // LVS's sessionId === channelArn. LVS's cognitoAuth gates this route; it
    // gets platform-api's service token (lib/lvsServiceAuth.ts), or the
    // caller's own bearer when none is configured (legacy).
    const authHeader = lvsSessionAuthHeader(req.headers.authorization);
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
    // NFR #175: a lobby's sessions (participants included) to its own people only.
    await lobbyAccess().assertLobbyAccess(req, documentId);
    const own = await videoSessionRepo.getSessionsByDocument(documentId);
    // In-document calls: a review hosted on ANOTHER document that lists this
    // one shows here too (the "Migration brief" row's `Join · 3`). Only
    // calls whose host document this caller may use — joining needs it.
    let reviewing: VideoSessionRecord[] = [];
    try {
      const candidates = (await videoSessionRepo.findActiveByReviewDocument(documentId))
        .filter((s) => s.documentId !== documentId);
      const allowed = await Promise.all(
        candidates.map((s) => lobbyAccess().canUseLobby(req, s.documentId).catch(() => false)),
      );
      reviewing = candidates.filter((_, i) => allowed[i]);
    } catch (scanErr) {
      log.warn({ err: scanErr, documentId }, '[videoSessions] review-document lookup failed');
    }
    const sessions = [...own, ...reviewing].map(withCallFields);
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
    // NFR #175: the lobby's own people only.
    await lobbyAccess().assertLobbyAccess(req, lobbyName);

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
/**
 * Is anyone actually publishing to this channel right now?
 *
 * `true` / `false` are answers. `null` means LVS could not be asked, which is
 * NOT the same as "nothing is on" and must not be treated as one.
 *
 * LVS derives `state` by walking its live mediasoup participants and checking
 * each producer object is open — so it is the one view of liveness nobody has
 * to remember to decrement. Every counter in this system leaks: our own
 * `participants[]` accumulates an entry per join, and LVS's own
 * `sessionStore` participant set is only ever decremented by the /end route
 * it is supposed to outlive.
 */
async function isChannelPublishing(channelArn: string): Promise<boolean | null> {
  if (!channelArn) return null;
  const url = `${VIDEO_API_URL}/api/channels/${encodeURIComponent(channelArn)}/stream`;
  const adminToken = process.env.VIDEO_ADMIN_TOKEN || 'admin';
  try {
    const upstream = await fetch(url, {
      headers: { Authorization: `Bearer ${adminToken}` },
      signal: AbortSignal.timeout(2500),
    });
    if (!upstream.ok) return null;
    const body = (await upstream.json()) as {
      stream?: { state?: unknown };
      state?: unknown;
    };
    const state = body.stream?.state ?? body.state;
    if (state === 'LIVE') return true;
    if (state === 'OFFLINE') return false;
    return null;
  } catch {
    // Unreachable, slow, or unparseable. Refusing to answer is better than
    // inventing one: a viewer told "nothing is on" because a probe timed out
    // has been lied to just as surely as one shown a dead stream.
    return null;
  }
}

videoSessionsRouter.get(
  '/sessions/by-lobby/:lobbyName',
  requireAuth,
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    try {
      const lobbyName = decodeURIComponent(req.params.lobbyName ?? '');
      if (!lobbyName) throw new ValidationError('lobbyName is required');
      // NFR #175: channelArn (enough to watch the stream) and startedBy only
      // for someone who may use the lobby.
      await lobbyAccess().assertLobbyAccess(req, lobbyName);

      // "Nothing is live" is the ORDINARY answer here — most conversations,
      // most of the time — so it is a 200 with a plain no, not an error. As a
      // 404 it was correct on the wire and wrong in practice: the browser
      // paints every 404 red, so simply opening a conversation without a
      // stream printed console errors that read like a broken build, and the
      // one caller already had to carry a comment explaining that its own
      // error path was not an error.
      const notLive = (): void => {
        res.status(200).json({ lobbyName, live: false, channelArn: null });
      };

      const session = await videoSessionRepo.findActiveByLobbyName(lobbyName);
      if (!session) return notLive();

      // `status: 'active'` is a LIFECYCLE flag, not evidence that anything is
      // being broadcast. It flips to 'ended' only when the last participant
      // leaves through the leave route, and participants leak: a room call
      // joins TWICE (once through the room bridge, once through the call
      // widget) and hangs up ONCE, so every call nets an extra participant and
      // the ref-count never reaches zero. Measured on a real room: three
      // participants left behind by three calls that were all hung up.
      //
      // So the roster cannot answer "is this watchable" — ask the SFU, which
      // knows because it holds the producer objects. It also self-heals: a
      // client that vanishes has its transport reaped within ~20-65s and the
      // channel goes OFFLINE on its own, with nobody having to say goodbye.
      const publishing = await isChannelPublishing(session.channelArn);
      if (publishing === false) {
        // A definite "nothing is on" from the authority. Take the chance to
        // close the row out — otherwise this same dead session is re-probed
        // on every poll, forever, and stays 'active' in the table where the
        // next reader has to disbelieve it all over again. Grace-window
        // guarded so a call that is still ringing is never ended, and
        // fire-and-forget so the viewer's answer is not held up by a write.
        if (isPastGrace(session)) void endStaleSession(session);
        return notLive();
      }

      // `null` is "LVS could not be asked", not "nothing is on". Falling back
      // to the roster keeps a reachable-LVS outage from hiding a live stream —
      // the leak makes this answer too generous, which is the failure worth
      // having when the alternative is telling a viewer a running broadcast
      // does not exist.
      if (publishing === null && (session.participants ?? []).length === 0) return notLive();

      // …but say WHICH of the two answers this is. The fallback above trusts a
      // roster the comment two paragraphs up calls unreliable — "every call
      // nets an extra participant and the ref-count never reaches zero" — so
      // while LVS is unreachable, every room that has ever hosted a call
      // answers `live: true`. Observed 2026-09-07: LVS had been up five hours
      // with a closed Redis client and was 500ing every request, and a room
      // with nothing running showed a "Live now" banner whose Watch button
      // opened a black stage.
      //
      // A caller cannot tell that apart from a verified broadcast unless we
      // tell it, so `verified` says whether the SFU actually confirmed this.
      // Present and true is the ordinary case; false means "the roster says
      // so and the authority could not be reached", which a UI should offer
      // more cautiously than a broadcast it can promise.
      const verified = publishing === true;

      // Only what a viewer needs. Participant lists and tokens stay out: this
      // route is reachable by anyone who can see the room, and watching is a
      // weaker right than joining.
      res.status(200).json({
        lobbyName,
        live: true,
        verified,
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

// ---------------------------------------------------------------------------
// GET /api/video/sessions/live — every lobby that is actually publishing.
// ---------------------------------------------------------------------------
//
// The list counterpart to `by-lobby`. A conversation list wants to badge every
// row, and asking per row is a request per conversation; this answers for all
// of them at once.
//
// WHY THIS CANNOT JUST RETURN THE 'active' ROWS
//
// `status: 'active'` leaks — see `listActiveCandidates`. Returning it raw
// would mark every room that has ever hosted a call as live, permanently,
// which is worse than the missing badge this route exists to fix. So each
// candidate is probed against the SFU, exactly as `by-lobby` does, and only
// the ones publishing come back.
//
// The probes are bounded and cached: LVS is asked at most PROBE_CONCURRENCY at
// a time, and the whole answer is held for CACHE_TTL_MS so that N clients
// polling this do not become N x M requests at the SFU.
//
// Registered BEFORE `/sessions/:sessionId` — Express matches in registration
// order, and `live` is a perfectly good session id as far as that route knows.

/** How many channels to ask LVS about at once. */
const PROBE_CONCURRENCY = 6;
/** How long one computed answer serves every caller. */
const CACHE_TTL_MS = 5_000;

/**
 * NFR #175: the live list is computed once for everyone (cached 5 s), then
 * narrowed per caller to the lobbies they may use — it used to list every
 * live call in every org, DM names and channelArns included.
 */
async function visibleLobbies(req: Request, lobbies: LiveLobby[]): Promise<LiveLobby[]> {
  const access = lobbyAccess();
  const keep = await Promise.all(lobbies.map((l) => access.canUseLobby(req, l.lobbyName).catch(() => false)));
  return lobbies.filter((_, i) => keep[i]);
}

interface LiveLobby {
  lobbyName: string;
  channelArn: string;
  startedAt: string | null;
  startedBy: string | null;
}

let liveCache: { at: number; value: LiveLobby[] } | null = null;
/** In-flight de-duplication: a burst of callers shares one computation. */
let liveInFlight: Promise<LiveLobby[]> | null = null;

// ---------------------------------------------------------------------------
// Stale-session reaper — sessions must end even when nobody says goodbye.
// ---------------------------------------------------------------------------
//
// A hang-up is the happy path, and the happy path is not the only one: a tab
// crashes, a laptop lid closes, a headless test process is killed. None of
// those send /end, and `status: 'active'` has no other way to become 'ended'
// — `createSession` writes no TTL either, so the row is immortal.
//
// LVS is the authority and it self-heals: it walks its live mediasoup
// producers to answer `state`, and a vanished client's transports are reaped
// on ICE/DTLS timeout in ~20-65s, after which the channel reports OFFLINE
// with nobody having had to say anything. `isChannelPublishing` already asks
// exactly that question.
//
// LAZY-ON-READ, not a timer. Three reasons: (1) the probe, the concurrency
// limiter and the answer cache all already exist here, on the one route whose
// job is to decide which lobbies are live — a sweep would be a second copy of
// the same logic; (2) platform-api runs multiple replicas and an interval
// would have every replica scanning the table on its own clock, needing a
// lock to not stampede; (3) `/sessions/live` is polled by the conversation
// list, so in practice the whole active set is examined whenever anyone has
// the app open, which is precisely when a stale 'LIVE' badge would be seen.
// The cost is that a table nobody looks at is not tidied, which nobody minds.
//
// GRACE WINDOW. A freshly created session is legitimately OFFLINE: the DM is
// still ringing, or the publisher has not finished its WHIP handshake. Ending
// those would be a far worse bug than the leak. So only rows older than
// VIDEO_SESSION_STALE_MS (default 120s — comfortably past LVS's 20-65s reap)
// are eligible, and `null` (LVS could not be asked) never ends anything.

function staleAfterMs(): number {
  const raw = process.env.VIDEO_SESSION_STALE_MS;
  const n = raw ? Number.parseInt(raw, 10) : NaN;
  return Number.isFinite(n) && n > 0 ? n : 120_000;
}

/** Is this row old enough that "not publishing" means "over" rather than "not started yet"? */
function isPastGrace(session: { startedAt?: string }, now = Date.now()): boolean {
  const started = Date.parse(session.startedAt ?? '');
  if (Number.isNaN(started)) return true; // no usable timestamp → not new
  return now - started >= staleAfterMs();
}

/**
 * End one session whose channel LVS says is not publishing. Best-effort and
 * idempotent — `endSession` is a plain conditional-free update, so a race
 * between two replicas both deciding this is harmless.
 */
async function endStaleSession(session: VideoSessionRecord): Promise<void> {
  try {
    await videoSessionRepo.endSession(
      session.documentId,
      session.sessionId,
      new Date().toISOString(),
    );
    log.info(
      { sessionId: session.sessionId, lobbyName: session.documentId, channelArn: session.channelArn },
      '[videoSessions] reaped stale session (channel not publishing)',
    );
  } catch (err) {
    log.warn({ err, sessionId: session.sessionId }, '[videoSessions] stale-session reap failed');
  }
}

/**
 * ONE pass over every active row: probe it, end it if it is dead, keep it if
 * it is live. Both callers want the same probe, so they share it — asking LVS
 * twice about the same channel would double the SFU load for no new
 * information.
 *
 * Every row, not just the newest per lobby. Rows BEHIND the newest are
 * invisible to `by-lobby` and to the badge list, so a reaper that skipped them
 * would leave them 'active' forever with nothing able to notice. That is
 * exactly how one room accumulates four active rows for one call.
 *
 * Returns the live lobbies (newest row per lobby, since only one call is
 * current per lobby) and the sessionIds it ended.
 */
async function sweepActiveSessions(
  limit = 120,
): Promise<{ live: LiveLobby[]; ended: string[] }> {
  const rows = (await videoSessionRepo.listActiveCandidates(limit, { perLobby: false })).filter(
    (s) => !!s.channelArn && !!s.documentId,
  );

  const live: LiveLobby[] = [];
  const ended: string[] = [];
  const seenLobbies = new Set<string>();

  for (let i = 0; i < rows.length; i += PROBE_CONCURRENCY) {
    const slice = rows.slice(i, i + PROBE_CONCURRENCY);
    const verdicts = await Promise.all(slice.map((s) => isChannelPublishing(s.channelArn)));
    for (let n = 0; n < slice.length; n += 1) {
      const session = slice[n];
      const publishing = verdicts[n];

      if (publishing === true) {
        // Rows arrive newest-first, so the first one seen for a lobby is the
        // current call; anything behind it is a leftover.
        if (!seenLobbies.has(session.documentId)) {
          seenLobbies.add(session.documentId);
          live.push({
            lobbyName: session.documentId,
            channelArn: session.channelArn,
            startedAt: session.startedAt ?? null,
            startedBy: session.startedBy ?? null,
          });
        }
        continue;
      }

      // `null` is "LVS could not be asked", not "nothing is on" — it neither
      // ends a session nor earns a badge. Ending a call because a probe timed
      // out would hang up on people who are still talking; badging on a guess
      // would mark a dead room live, which is the worse lie of the two since
      // nobody asked about that room.
      if (publishing === false && isPastGrace(session)) {
        await endStaleSession(session);
        ended.push(session.sessionId);
      }
    }
  }
  return { live, ended };
}

/**
 * Reap dead sessions without caring about the badge list. Exported so the
 * behaviour is directly testable and so an operator can trigger a sweep by
 * hand; `GET /api/video/sessions/live` runs the same pass on every miss.
 */
export async function reapStaleSessions(limit = 120): Promise<string[]> {
  return (await sweepActiveSessions(limit)).ended;
}

async function computeLiveLobbies(): Promise<LiveLobby[]> {
  return (await sweepActiveSessions()).live;
}

/**
 * Clears the cached answer. Tests only — without it one case's live list
 * serves the next one, and a 5s TTL is longer than a suite takes to run.
 */
export function __resetLiveLobbyCacheForTests(): void {
  liveCache = null;
  liveInFlight = null;
}

videoSessionsRouter.get(
  '/sessions/live',
  requireAuth,
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    try {
      const now = Date.now();
      if (liveCache && now - liveCache.at < CACHE_TTL_MS) {
        res.status(200).json({ lobbies: await visibleLobbies(req, liveCache.value), cached: true });
        return;
      }
      if (!liveInFlight) {
        liveInFlight = computeLiveLobbies()
          .then((value) => {
            liveCache = { at: Date.now(), value };
            return value;
          })
          .finally(() => {
            liveInFlight = null;
          });
      }
      const lobbies = await visibleLobbies(req, await liveInFlight);
      res.status(200).json({ lobbies, cached: false });
    } catch (err) {
      log.error({ err }, '[videoSessions] GET /sessions/live error');
      // An empty list, not a 500. The caller is a conversation list drawing
      // badges; it must render whether or not this could be answered, and
      // "nothing is on" is the safe reading — the same one every other
      // liveness path here takes when it cannot ask.
      res.status(200).json({ lobbies: [], degraded: true });
    }
  }),
);

// PATCH /api/video/sessions/:sessionId — in-document calls: change the
// title and/or the review list. Body { lobbyName|documentId, title?,
// documentIds? }. The caller must be in the call (join list or host); the
// title is the host's alone. Ended calls are not edited.
videoSessionsRouter.patch('/sessions/:sessionId', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  try {
    const { sessionId } = req.params;
    const body = (req.body ?? {}) as Record<string, unknown>;
    const lobbyName = resolveLobbyName(body)
      ?? ((req.query.lobbyName ?? req.query.documentId) as string | undefined);
    if (!lobbyName) {
      throw new ValidationError('lobbyName (or documentId) is required');
    }
    await lobbyAccess().assertLobbyAccess(req, lobbyName);

    const session = await videoSessionRepo.getSession(lobbyName, sessionId);
    if (!session) throw new NotFoundError('Session not found');
    if (session.status !== 'active') {
      throw new AppError(409, 'This call has ended');
    }
    const userId = req.user!.sub;
    if (!isCallParticipant(session, userId)) {
      throw new ForbiddenError('Only people in the call can change it');
    }
    const patch = parsePatchFields(body, lobbyName);
    if (patch.title !== undefined && hostOf(session) !== userId) {
      throw new ForbiddenError('Only the host can rename the call');
    }
    const updated = await videoSessionRepo.updateDocumentCallFields(lobbyName, sessionId, {
      ...patch,
      // A PATCH is what makes a plain document call a review, if it was not one.
      ...(session.kind ? {} : { kind: 'document-review' as const, hostUserId: session.startedBy }),
    });
    if (!updated) throw new NotFoundError('Session not found');
    res.status(200).json(withCallFields(updated));
  } catch (err) {
    if (err instanceof AppError) throw err;
    log.error({ err }, '[videoSessions] PATCH /sessions/:sessionId error');
    throw new AppError(500, 'Failed to update video session');
  }
}));

videoSessionsRouter.get('/sessions/:sessionId', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  try {
    const { sessionId } = req.params;
    const lobbyName = (req.query.lobbyName ?? req.query.documentId) as string | undefined;

    if (!lobbyName) {
      throw new ValidationError('lobbyName (or documentId) query parameter is required');
    }
    // NFR #175: the lobby's own people only.
    await lobbyAccess().assertLobbyAccess(req, lobbyName);

    const session = await videoSessionRepo.getSession(lobbyName, sessionId);
    if (!session) {
      throw new NotFoundError('Session not found');
    }

    res.status(200).json(withCallFields(session));
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
// The route used to arm the tap itself and keep the tap id in a Map; the
// run owns that now — see the handler below and recordings/liveCaptions.ts.

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
  // NFR #175: the lobby's own people only (was tenant-only).
  await lobbyAccess().assertLobbyAccess(req, lobbyName);
  const from = typeof req.query.from === 'string' && req.query.from.length > 0 ? req.query.from : undefined;
  const to = typeof req.query.to === 'string' && req.query.to.length > 0 ? req.query.to : undefined;
  const reactions = await callReactionRepo.listByLobby(tenantOf(req.user?.orgId), lobbyName, from, to);
  res.json({ reactions });
}));

// The tap's lifecycle is a PIPELINE RUN now (recordings/liveCaptions.ts):
// enabled=true starts a `live-captions` run (arm → stream → disarm → publish)
// and answers with its runId; enabled=false asks that run to stop, so it
// finishes through disarm and leaves a card in the conversation. Idempotent in
// both directions: a second enable returns the run already in flight, and a
// disable with no run known removes any tap pointing at our sink directly.
videoSessionsRouter.post('/captions', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { channelArn, enabled } = (req.body ?? {}) as { channelArn?: string; enabled?: boolean };
  if (!channelArn || typeof enabled !== 'boolean') {
    throw new AppError(400, 'channelArn and enabled are required');
  }
  // NFR #175: captions tap a call's audio, so only someone who may use the
  // call's lobby toggles them. The channel is resolved to its session(s);
  // an unknown channel is 404.
  const sessionsOnChannel = await videoSessionRepo.findByChannelArn(channelArn);
  const access = lobbyAccess();
  const allowed = await Promise.all(sessionsOnChannel.map((x) => access.canUseLobby(req, x.documentId).catch(() => false)));
  if (!allowed.some(Boolean)) throw new NotFoundError('Session not found');
  if (enabled) {
    const out = await startLiveCaptions({ channelArn, userId: req.user?.sub ?? 'system' });
    res.json({ enabled: true, runId: out.runId, pipelineId: out.pipelineId, reused: out.reused, ...(out.tapIds ? { tapIds: out.tapIds } : {}) });
    return;
  }
  const out = await stopLiveCaptions({ channelArn });
  res.json({ enabled: false, runId: out.runId, stopped: out.stopped });
}));
