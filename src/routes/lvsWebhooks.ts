/**
 * LVS webhook receiver — bound at `/hooks/lvs` so it stays OUTSIDE the
 * `requireAuth` wall (LVS uses HMAC, not Cognito).
 *
 * LVS fires two relevant event types via WebhookDispatcher.emit:
 *
 *   1. `sfu.recording.started` — `{ recordingId, startedAt, destination }`
 *   2. `sfu.recording.completed` — `{ recordingId, segments[], totalSize,
 *      durationMs, destination }`
 *
 * The channel ARN is on the envelope `.detail.arn` (EventBridge format)
 * or the bare payload root depending on `WEBHOOK_FORMAT`. We support
 * both because LVS env config dictates which it sends.
 *
 * On `recording.completed` we:
 *   1. Decode arn → look up VideoSessionRecord by channel ARN
 *   2. From the VideoSession, get the lobbyName + participants
 *   3. Look up CallSessionBinding by lobbyName (via gateway DDB) → callId
 *      — note: platform-api doesn't own that table directly. We resolve
 *      indirectly: VideoSession carries the lobbyName, and the gateway
 *      writes call-session-bindings keyed by lobbyName for lookup.
 *      For MVP, look up the binding row via a direct DDB Query on the
 *      table the gateway writes; the table lives in the SAME DDB
 *      account so we can read it. (See videoSessionRepo for prior art.)
 *   4. Persist CallRecording + fan-out participant index rows.
 *
 * HMAC verification (hub#1006):
 *   - `LVS_WEBHOOK_SECRET` is REQUIRED. When set, every incoming
 *     request must have `X-LVS-Signature: sha256=<hex>` matching
 *     HMAC(secret, rawBody). Missing or invalid → 401.
 *   - When the secret is NOT set, the receiver REJECTS all requests
 *     (also 401) unless `SKIP_AUTH=true` is explicitly set in the
 *     environment. SKIP_AUTH is the documented dev-only escape hatch;
 *     it must NEVER be enabled in production. Pre-hub#1006 behavior
 *     was "secret unset → accept" — a misconfigured ingress could
 *     then forge `recording.completed` events. The explicit-escape-
 *     hatch model closes that gap.
 */

import { Router, Request, Response } from 'express';
import { createHmac, timingSafeEqual } from 'crypto';
import {
  DynamoDBClient,
  QueryCommand,
} from '@aws-sdk/client-dynamodb';
import {
  callRecordingRepo,
  videoSessionRepo,
  type CallRecordingParticipant,
} from '../repositories';
import { tableName } from '../lib/ddb-table-name';
import log from '../lib/logger';
import { ddbClient } from '../lib/aws-clients';
import {
  checkReplayWindow,
  WEBHOOK_TIMESTAMP_HEADER,
} from '../lib/webhook-replay';
import { catalog } from '../lib/event-catalog';
import {
  deriveMediaDurationMs,
  reconcileParticipants,
  type TruthTrack,
} from '../lib/recording-truth';
import { resolveRecordingProfile } from '@connorhoehn/event-catalog';

export const lvsWebhooksRouter = Router();

// --------------------------------------------------------------------
// W5 — in-memory delivery ring buffer for the admin health endpoint.
// Bounded at MAX_DELIVERIES; oldest entries are evicted on push. The
// admin router (src/routes/lvsAdmin.ts) reads this via getRecentDeliveries().
// Process-local; lost on restart. Cheap, useful for live debugging.
// --------------------------------------------------------------------
export interface LvsDeliveryRecord {
  timestamp: string;
  eventType: string;
  arn: string;
  recordingId: string;
  hmacOk: boolean;
  persisted: boolean;
  error?: string;
}

const MAX_DELIVERIES = 50;
const deliveries: LvsDeliveryRecord[] = [];

function recordDelivery(entry: LvsDeliveryRecord): void {
  deliveries.push(entry);
  if (deliveries.length > MAX_DELIVERIES) {
    deliveries.splice(0, deliveries.length - MAX_DELIVERIES);
  }
}

export function getRecentDeliveries(): LvsDeliveryRecord[] {
  return deliveries.slice();
}

/**
 * Recover a recording's true start instant from its own id.
 *
 * LVS mints `recordingId` at recording START as the ISO timestamp with
 * every `:` and `.` swapped for `-`, plus a random suffix — e.g.
 * `2026-09-07T10-30-33-757Z-4b2365cf` is `2026-09-07T10:30:33.757Z`. The
 * instant is right there, exact, and it is the SAME string on every
 * redelivery of the event, which is the property the fan-out sort key
 * needs and the wall clock does not have.
 *
 * Returns null on anything that doesn't match, so an LVS build with a
 * different id scheme falls through to the clock rather than inventing a
 * date out of a random string.
 */
export function startedAtFromRecordingId(recordingId: string): string | null {
  const m = /^(\d{4}-\d{2}-\d{2})T(\d{2})-(\d{2})-(\d{2})-(\d{3})Z/.exec(recordingId ?? '');
  if (!m) return null;
  const iso = `${m[1]}T${m[2]}:${m[3]}:${m[4]}.${m[5]}Z`;
  return Number.isNaN(Date.parse(iso)) ? null : iso;
}

// Capture raw body for HMAC. The router-scoped raw parser leaves
// req.body as a Buffer; we JSON.parse it after signature verification.
lvsWebhooksRouter.use(
  require('express').raw({ type: 'application/json', limit: '256kb' }),
);

// Read at request time (not module load) so test cases can vary the
// secret + SKIP_AUTH env vars without re-importing the module.
function currentSecret(): string {
  return process.env.LVS_WEBHOOK_SECRET ?? '';
}

function isDevSkipAuth(): boolean {
  return process.env.SKIP_AUTH === 'true';
}

interface HmacVerifyResult {
  ok: boolean;
  /** Computed expected signature length (hex chars) — null when we can't recompute (e.g., body bytes lost). */
  expectedLen: number | null;
  /** Reason when ok=false. Surfaced in the rejection log line and the delivery ring buffer. */
  reason?: 'no-secret-no-skip-auth' | 'missing-header' | 'length-mismatch' | 'parsed-body' | 'sig-mismatch';
}

function verifyHmacDetailed(req: Request): HmacVerifyResult {
  const SECRET = currentSecret();
  if (!SECRET) {
    // hub#1006: no longer accept blindly when secret is unset — require
    // explicit SKIP_AUTH=true to opt into dev-mode. Anything else (prod,
    // misconfigured dev, etc.) is rejected.
    if (isDevSkipAuth()) {
      return { ok: true, expectedLen: null };
    }
    return { ok: false, expectedLen: null, reason: 'no-secret-no-skip-auth' };
  }
  const header = req.header('X-LVS-Signature') ?? '';
  if (!header) {
    return { ok: false, expectedLen: null, reason: 'missing-header' };
  }
  // Defense in depth: if a parent middleware accidentally parsed the
  // body into an Object (express.json ran before our raw parser),
  // Hmac.update would crash with ERR_INVALID_ARG_TYPE — taking down
  // the whole pod. Coerce to a Buffer-of-bytes so the verify fails
  // loudly (returns false) rather than crashing. The app.ts json
  // parser is configured to skip /hooks/lvs but this guard catches
  // any future regression.
  const body = req.body;
  let bodyBytes: Buffer;
  if (Buffer.isBuffer(body)) {
    bodyBytes = body;
  } else if (typeof body === 'string') {
    bodyBytes = Buffer.from(body, 'utf-8');
  } else {
    // Parsed Object — body bytes are lost. Can't verify HMAC.
    // Log + reject. Returns false → 401 to LVS, which will retry.
    return { ok: false, expectedLen: null, reason: 'parsed-body' };
  }
  const expected = createHmac('sha256', SECRET)
    .update(bodyBytes)
    .digest('hex');
  const provided = header.replace(/^sha256=/, '');
  if (provided.length !== expected.length) {
    return { ok: false, expectedLen: expected.length, reason: 'length-mismatch' };
  }
  try {
    const ok = timingSafeEqual(
      Buffer.from(provided, 'hex'),
      Buffer.from(expected, 'hex'),
    );
    return { ok, expectedLen: expected.length, reason: ok ? undefined : 'sig-mismatch' };
  } catch {
    return { ok: false, expectedLen: expected.length, reason: 'sig-mismatch' };
  }
}

function verifyHmac(req: Request): boolean {
  return verifyHmacDetailed(req).ok;
}

// Resolve callId from lobbyName by querying the gateway's
// call-session-bindings table. The gateway is the writer; platform-api
// is a read-only consumer of that table. (Same DDB account.)
//
// Reuse the shared `ddbClient` from aws-clients so DYNAMODB_ENDPOINT /
// LOCALSTACK_ENDPOINT are honored. The previous `new DynamoDBClient({})`
// instantiation bypassed that config, so in local-dev (Tilt + dynamodb-local)
// every callId lookup silently failed → recordings were always persisted
// under a synthetic `unknown-<arn>-<recordingId>` callId, untraceable from
// the gateway side.
const ddbRaw = ddbClient;

async function resolveCallIdByLobby(lobbyName: string): Promise<string | null> {
  // The binding table indexes by callId (PK). To find by lobbyName we
  // scan-on-attribute via a Query on a GSI; the gateway repo writes a
  // `lobbyName-index` for exactly this lookup. (Schema in
  // CallSessionBindingRepository — add lobbyName GSI in setup script.)
  try {
    const out = await ddbRaw.send(
      new QueryCommand({
        TableName: tableName('call-session-bindings'),
        IndexName: 'lobbyName-index',
        KeyConditionExpression: 'lobbyName = :l',
        ExpressionAttributeValues: { ':l': { S: lobbyName } },
        Limit: 1,
        ScanIndexForward: false, // newest first
      }),
    );
    const item = (out.Items ?? [])[0];
    return item?.callId?.S ?? null;
  } catch (e: any) {
    log.warn({ err: e?.message ?? e, lobbyName }, '[lvs-webhook] callId lookup failed');
    return null;
  }
}

lvsWebhooksRouter.post('/recording', async (req: Request, res: Response) => {
  const hmac = verifyHmacDetailed(req);
  if (!hmac.ok) {
    const sigHeader = req.header('X-LVS-Signature') ?? '';
    const body = req.body;
    const bodyBytes = Buffer.isBuffer(body)
      ? body.length
      : typeof body === 'string'
        ? body.length
        : 0;
    log.warn(
      {
        ip: req.ip,
        hasSig: !!req.header('X-LVS-Signature'),
        sigLen: sigHeader.length,
        expectedLen: hmac.expectedLen,
        bodyType: typeof body,
        bodyBytes,
        reason: hmac.reason,
      },
      '[lvs-webhook] HMAC verification failed',
    );
    recordDelivery({
      timestamp: new Date().toISOString(),
      eventType: '',
      arn: '',
      recordingId: '',
      hmacOk: false,
      persisted: false,
      error: 'hmac verification failed',
    });
    res.status(401).json({ error: 'invalid signature' });
    return;
  }

  // hub#1260 — replay-window guard, paired with HMAC. Only enforced when
  // a secret is actually configured (i.e. we just verified an HMAC);
  // dev-mode SKIP_AUTH bypasses both. An attacker who captures a signed
  // body otherwise has indefinite replay rights.
  if (currentSecret()) {
    const replay = checkReplayWindow(req.header(WEBHOOK_TIMESTAMP_HEADER));
    if (!replay.ok) {
      log.warn(
        { ip: req.ip, reason: replay.reason },
        '[lvs-webhook] replay-window check failed',
      );
      recordDelivery({
        timestamp: new Date().toISOString(),
        eventType: '',
        arn: '',
        recordingId: '',
        hmacOk: true,
        persisted: false,
        error: `replay-window: ${replay.reason}`,
      });
      res.status(401).json({ error: replay.reason });
      return;
    }
  }

  let payload: any;
  try {
    // Same defense as verifyHmac — body should be a Buffer (raw parser)
    // but accept already-parsed Object as a fallback.
    const body = req.body;
    if (Buffer.isBuffer(body)) {
      payload = JSON.parse(body.toString('utf-8'));
    } else if (typeof body === 'string') {
      payload = JSON.parse(body);
    } else if (body && typeof body === 'object') {
      payload = body;
    } else {
      throw new Error('empty body');
    }
  } catch (e: any) {
    recordDelivery({
      timestamp: new Date().toISOString(),
      eventType: '',
      arn: '',
      recordingId: '',
      hmacOk: true,
      persisted: false,
      error: 'invalid JSON',
    });
    res.status(400).json({ error: 'invalid JSON' });
    return;
  }

  // Normalize across the two envelope shapes the dispatcher emits:
  //   EventBridge: `{ 'detail-type': 'IVS Recording State Change',
  //                   detail: { event_name: 'recording.completed', stream_id, ...data },
  //                   resources: [channelArn] }`
  //   Bare:        `{ id, type: 'recording.completed', channelArn, occurredAt,
  //                   data: { recordingId, segments, totalSize, durationMs, destination } }`
  // The fields we care about (recordingId, segments, totalSize, durationMs,
  // destination, thumbnailUrl) live under `detail` in EB and under `data`
  // in bare — flatten both into one `detail` view so downstream reads are
  // shape-agnostic.
  //
  // For event-type matching we prefer `payload.detail.event_name` over
  // `payload['detail-type']` when present — EB's detail-type is a category
  // label ('IVS Recording State Change') and doesn't include the action
  // ('completed'), so a naive `.includes('completed')` check would skip
  // every EB envelope. The bare shape exposes the action directly on
  // `payload.type`.
  const detail = payload.detail
    ?? (payload.data ? { ...payload, ...payload.data } : payload);
  const eventType: string =
    detail?.event_name
    ?? payload.type
    ?? payload['detail-type']
    ?? '';
  const arn: string =
    detail.arn
    ?? detail.channelArn
    ?? detail.stream_id
    ?? payload.channelArn
    ?? (Array.isArray(payload.resources) ? payload.resources[0] : '')
    ?? '';
  const recordingId: string = detail.recordingId ?? '';

  // W5 — successful-receive log emitted right after HMAC verifies + the
  // envelope is parsed. Helps correlate webhook arrival timing with
  // downstream persistence logs.
  log.info(
    {
      eventType,
      arn,
      recordingId,
      hasDetail: !!payload.detail,
      format: payload['detail-type'] ? 'eventbridge' : 'bare',
    },
    '[lvs-webhook] received',
  );

  if (!eventType.toLowerCase().includes('recording')) {
    recordDelivery({
      timestamp: new Date().toISOString(),
      eventType,
      arn,
      recordingId,
      hmacOk: true,
      persisted: false,
      error: 'non-recording event ignored',
    });
    res.status(204).end();
    return;
  }

  // We only persist on `completed`. `started` is informational; we'd
  // need a write path for in-progress recordings only if the FE wants
  // to render a "Recording…" badge — out of scope for MVP.
  if (!eventType.toLowerCase().includes('completed')) {
    recordDelivery({
      timestamp: new Date().toISOString(),
      eventType,
      arn,
      recordingId,
      hmacOk: true,
      persisted: false,
      error: 'non-completed event ignored',
    });
    res.status(204).end();
    return;
  }

  if (!arn || !recordingId) {
    log.warn({ eventType, arn, recordingId }, '[lvs-webhook] missing arn/recordingId');
    recordDelivery({
      timestamp: new Date().toISOString(),
      eventType,
      arn,
      recordingId,
      hmacOk: true,
      persisted: false,
      error: 'missing arn or recordingId',
    });
    res.status(400).json({ error: 'missing arn or recordingId' });
    return;
  }

  // P0 — resolve channelArn → sessionId/lobbyName via the Redis
  // arn-index that videoSessions.ts writes at session-create. From
  // there we hop to VideoSession (for participants) and the gateway
  // CallSessionBindingRepository (for callId, queried by lobbyName).
  let session: any = null;
  try {
    const { getRedisClient } = await import('../lib/redis-client.js');
    const redis = await getRedisClient();
    if (redis) {
      const raw = await redis.get(`video:arn-index:${arn}`);
      if (raw) {
        const idx = JSON.parse(raw) as { sessionId: string; lobbyName: string };
        session = await videoSessionRepo.getSession?.(idx.lobbyName, idx.sessionId).catch(() => null);
        if (session) {
          log.info({ arn, sessionId: idx.sessionId, lobbyName: idx.lobbyName }, '[lvs-webhook] resolved arn via Redis index');
        } else {
          log.warn({ arn, sessionId: idx.sessionId }, '[lvs-webhook] arn-index hit but VideoSession missing — proceeding with DDB fallback');
        }
      } else {
        log.warn({ arn }, '[lvs-webhook] no arn-index entry — proceeding with DDB fallback');
      }
    }
  } catch (lookupErr: any) {
    log.warn({ err: lookupErr?.message ?? lookupErr, arn }, '[lvs-webhook] arn lookup threw — proceeding with DDB fallback');
  }

  // DDB fallback — when Redis miss / Redis down / arn-index expired we
  // scan document-video-sessions by channelArn. This was the root cause
  // of "DM recordings never appear in /recordings": DM lobbies create a
  // fresh VideoSession + arn-index per ring, and any Redis blip between
  // session-create and recording-completed left the webhook with no
  // lobbyName → no startedBy → no fan-out row → invisible recording.
  // Group hangouts dodged this because the static `global-hangout` lobby
  // had a long-lived arn-index that almost never expired mid-call.
  if (!session) {
    try {
      const matches = await videoSessionRepo.findByChannelArn(arn);
      if (matches.length > 0) {
        session = matches[0];
        log.info({ arn, sessionId: session.sessionId, lobbyName: session.documentId, source: 'ddb-scan' }, '[lvs-webhook] resolved arn via DDB scan fallback');
      } else {
        log.warn({ arn }, '[lvs-webhook] DDB scan fallback found no matching VideoSession');
      }
    } catch (scanErr: any) {
      log.warn({ err: scanErr?.message ?? scanErr, arn }, '[lvs-webhook] DDB scan fallback threw');
    }
  }

  const lobbyName = session?.documentId ?? '';
  const callId = lobbyName ? await resolveCallIdByLobby(lobbyName) : null;

  if (!callId) {
    log.warn({ arn, lobbyName }, '[lvs-webhook] no callId binding — persisting under recordingId-only');
  }

  // Convert VideoSession participants → CallRecordingParticipant
  let participants: CallRecordingParticipant[] = (session?.participants ?? []).map((p: any) => ({
    userId: p.userId,
    displayName: p.displayName,
    participantId: p.participantId,
    joinedAt: p.joinedAt,
  }));

  // DM-pattern derivation. Final safety net for `dm:<a>:<b>` lobbies when
  // both the Redis arn-index AND the DDB session-by-arn scan came back
  // empty (e.g. session row already TTL'd by the time the webhook fires
  // — rare but happens in soak runs). The lobby name itself encodes the
  // two participants, so we can fan-out to both userIds and at least keep
  // the recording reachable from the list view. Display names are
  // unrecoverable here; FE renders the userId fragment instead.
  if (participants.length === 0 && lobbyName.startsWith('dm:')) {
    const ids: string[] = lobbyName
      .slice(3) // strip "dm:"
      .split(':')
      .filter((s: string) => s.length > 0);
    if (ids.length >= 2) {
      participants = ids.map((userId: string): CallRecordingParticipant => ({ userId }));
      log.info({ arn, lobbyName, derivedParticipants: ids.length }, '[lvs-webhook] derived DM participants from lobby name');
    }
  }

  // Leg-truth reconciliation — see lib/recording-truth.ts for the full
  // rationale and the measurements. In short, neither of the two numbers
  // LVS hands us means what the UI rendered it as:
  //
  //   `session.participants` is the CHANNEL's lifetime roster: appended to
  //   on every join, never deduped, never scoped to one recording, and
  //   snapshotted after the initiator usually already hung up. Rendering
  //   its length gave a 2-person DM "7 👥" and dropped the caller entirely.
  //
  //   `detail.durationMs` is the SFU channel-session lifetime, which
  //   includes the ring, the lobby wait and teardown. Rendering it as
  //   "Duration" overstated every single recording on disk.
  //
  // Both corrections are pure functions of the payload we already have,
  // and both run again on read so a row that predates this code (or one
  // written by a pod that hasn't rolled) still renders honestly.
  const rawTracks: TruthTrack[] = Array.isArray(detail.tracks) ? detail.tracks : [];
  const sessionDurationMs = Number(detail.durationMs ?? 0);
  // `startedAt` is not a display field — it is HALF THE SORT KEY of the
  // fan-out index (`${startedAt}#${recordingId}`), so whatever we compute
  // here decides whether a redelivered webhook overwrites its own row or
  // opens a second one.
  //
  // It used to be `new Date(Date.now() - sessionDurationMs)`, i.e. the
  // handling clock, which is a different value on every delivery. That is
  // the duplicate-row bug: recordingId 2026-09-07T10-30-33-757Z-4b2365cf
  // holds two fan-out rows differing only in 30ms of `startedAt`, with
  // identical durationMs, participants and callId. Eight rows, seven
  // recordings, and the list showed one call twice.
  //
  // Three sources, in decreasing order of truth:
  //   1. `detail.startedAt` — LVS now sends it (RecordingManager's
  //      recording.completed emit). Same event, same value, always.
  //   2. The recordingId, which LVS mints as
  //      `<ISO with : and . replaced by ->-<rand>` at recording start —
  //      the exact same instant, recoverable losslessly. Covers LVS pods
  //      that predate (1).
  //   3. The handling clock, as before. Reached only when neither of the
  //      above parses, and no longer the normal path.
  const startedAt: string =
    detail.startedAt
    ?? startedAtFromRecordingId(recordingId)
    ?? new Date(Date.now() - sessionDurationMs).toISOString();
  // Derived from `startedAt` rather than the wall clock for the same
  // reason: two deliveries of one event must agree.
  const endedAt: string =
    detail.endedAt
    ?? new Date(Date.parse(startedAt) + sessionDurationMs).toISOString();
  // VideoSessionRecord exposes the creator as `startedBy` (NOT `createdBy`
  // — that field doesn't exist on the record). Resolved BEFORE
  // reconciliation now, because the initiator has to be unioned into
  // participants[]: they were in their own call even when the live roster
  // no longer says so.
  const startedBy: string | undefined = session?.startedBy ?? participants[0]?.userId;
  const reconciled = reconcileParticipants({
    participants,
    tracks: rawTracks,
    startedBy,
    startedAt,
    endedAt,
  });
  participants = reconciled.participants;
  const participantCount = reconciled.participantCount;
  const mediaDurationMs = deriveMediaDurationMs({
    tracks: rawTracks,
    sessionDurationMs,
  });

  try {
    await callRecordingRepo.putRecording({
      callId: callId ?? `unknown-${arn}-${recordingId}`,
      recordingId,
      channelArn: arn,
      lobbyName,
      sessionId: session?.sessionId,
      startedAt,
      endedAt,
      // Media length — what a viewer means by "Duration", and what the
      // player's own timeline shows. The raw session lifetime is kept
      // alongside it rather than discarded.
      durationMs: mediaDurationMs,
      sessionDurationMs,
      totalBytes: Number(detail.totalSize ?? detail.totalBytes ?? 0),
      segments: Array.isArray(detail.segments) ? detail.segments : [],
      destination: detail.destination === 's3' ? 's3' : 'local',
      participants,
      // Distinct PEOPLE (deduped by userId, scoped to this recording,
      // floored at the recorded-leg count so participantId-only legs still
      // count) — never `participants.length` on the raw roster, which is a
      // join count.
      participantCount,
      startedBy,
      // W8 — thumbnail surfaced by LVS RecordingManager. May be a
      // file:// URL in dev (hostPath) or a backend-specific URL in
      // prod (S3 signed / CloudFront / etc.). Null when ffmpeg
      // thumbnail step failed.
      thumbnailUrl: typeof detail.thumbnailUrl === 'string' ? detail.thumbnailUrl : undefined,
      // R2 — composed grid playback URL + per-peer raw tracks. LVS
      // emits these on `recording.completed` when the call had ≥2
      // producers and `composeEnabled !== false`. Missing for
      // single-producer calls (legacy) — FE falls back to the first
      // segment[0].url as the playback target.
      composedUrl: typeof detail.composedUrl === 'string' ? detail.composedUrl : undefined,
      tracks: Array.isArray(detail.tracks) ? detail.tracks : undefined,
    });
    log.info(
      {
        callId,
        recordingId,
        arn,
        participants: participants.length,
        participantCount,
        startedBy,
        legs: rawTracks.length,
        // Both durations logged so a "the UI says 18s but the file is 8s"
        // report can be triaged from the log line alone.
        mediaDurationMs,
        sessionDurationMs,
      },
      '[lvs-webhook] recording persisted',
    );

    // Transcription pipeline trigger. Wave 3 / ADR-002 — read the
    // canonical `recordingProfile` first; resolveRecordingProfile expands
    // it to {composePerPeer, transcribe, …}. The legacy `transcribe`
    // boolean is still accepted for one release window so LVS pods that
    // haven't bumped past 14217e7 keep working.
    //
    // Best-effort: failures here log + metric but do NOT fail the webhook
    // 204 — the worst case is "recording exists, transcript missing" which
    // the user can re-trigger from the UI. We avoid wrapping the put +
    // publish in an outbox TransactWrite because the put already does a
    // multi-table fan-out (target + N participant rows) which
    // publishWithOutbox's single-target model doesn't fit.
    const webhookSaysTranscribe = detail.transcribe === true || detail.transcribe === 'true';
    const resolvedProfile = resolveRecordingProfile({
      recordingProfile: typeof detail.recordingProfile === 'string' ? detail.recordingProfile : undefined,
      transcribe: webhookSaysTranscribe,
      composePerPeer: detail.composePerPeer === true || detail.composePerPeer === 'true',
      // LVS's denormalized `transcribe` reflects ITS resolved config —
      // including per-channel overrides (e.g. dm-profile transcribe
      // opt-in via LVS_DM_TRANSCRIBE). Re-resolving from the bare
      // profile id here used to DROP that override (profile id wins
      // over legacy flags in resolveRecordingProfile), silently turning
      // transcription back off for dm recordings. Passing it as an
      // override keeps profiles whose `overridable` list excludes
      // `transcribe` unaffected.
      overrides: webhookSaysTranscribe ? { transcribe: true } : undefined,
    });
    const wantTranscribe = resolvedProfile.transcribe === true;
    if (wantTranscribe) {
      const persistedCallId = callId ?? `unknown-${arn}-${recordingId}`;
      // `pending` is written BEFORE the publish on purpose: once the job is
      // on the queue a worker may pick it up and move the row to
      // 'processing' at any moment, and a status write sequenced after the
      // publish would race that and stamp the row back to 'pending'.
      //
      // The cost of that ordering is that a failed publish leaves a row
      // claiming 'pending' for a job that is not on any queue — which the
      // transcript endpoint can only discover by waiting out
      // TRANSCRIPT_STALL_MS (~20 minutes) and then guessing 'stalled'. We
      // know better than that, here and now: the publish threw. So on the
      // failure path we correct the row to 'failed' with the reason,
      // turning a twenty-minute timeout into an immediate, specific answer
      // in the UI.
      let pendingWritten = false;
      try {
        await callRecordingRepo.updateTranscriptStatus(persistedCallId, recordingId, {
          status: 'pending',
        });
        pendingWritten = true;
        await catalog.publish('transcription:job.queued', {
          recordingId,
          callId: persistedCallId,
          channelArn: arn,
          tracks: Array.isArray(detail.tracks) ? detail.tracks : [],
        });
        log.info({ recordingId, callId: persistedCallId, trackCount: Array.isArray(detail.tracks) ? detail.tracks.length : 0 },
          '[lvs-webhook] transcription job queued');
      } catch (txErr: any) {
        // Don't fail the webhook — transcription is a non-critical
        // post-processing feature. Log with enough context that an
        // operator can manually re-trigger from the UI.
        const reason = typeof txErr?.message === 'string' ? txErr.message : String(txErr);
        log.warn({ err: reason, recordingId, callId: persistedCallId, pendingWritten },
          '[lvs-webhook] transcription enqueue failed (recording still persisted)');
        // Only worth attempting when the `pending` write itself succeeded.
        // If THAT is what threw (a missing row fails the
        // attribute_exists(callId) condition) the corrective write would
        // fail identically, and there is no row to correct anyway.
        if (pendingWritten) {
          try {
            await callRecordingRepo.updateTranscriptStatus(persistedCallId, recordingId, {
              status: 'failed',
              transcriptError: `enqueue failed: ${reason}`,
            });
          } catch (markErr: any) {
            // Now the row really is stuck on 'pending' and the stall timer
            // is the only backstop left. Say so, rather than leaving a
            // silent gap between the two writes.
            log.warn({ err: markErr?.message ?? markErr, recordingId, callId: persistedCallId },
              '[lvs-webhook] could not mark transcript failed — row left pending, stall timer is the only backstop');
          }
        }
      }
    }

    recordDelivery({
      timestamp: new Date().toISOString(),
      eventType,
      arn,
      recordingId,
      hmacOk: true,
      persisted: true,
    });
    res.status(204).end();
  } catch (e: any) {
    log.error({ err: e?.message ?? e, callId, recordingId }, '[lvs-webhook] persist failed');
    recordDelivery({
      timestamp: new Date().toISOString(),
      eventType,
      arn,
      recordingId,
      hmacOk: true,
      persisted: false,
      error: typeof e?.message === 'string' ? e.message : String(e),
    });
    res.status(500).json({ error: 'persist failed' });
  }
});
