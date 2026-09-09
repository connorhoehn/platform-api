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
import { startSystemRun, buildRecordingFinalizeDefinition, RECORDING_FINALIZE_PIPELINE_ID } from '../recordings/recordingPipelines';

export const lvsWebhooksRouter = Router();

// --------------------------------------------------------------------
// W5 — in-memory delivery ring buffer for the admin health endpoint.
// Bounded at MAX_DELIVERIES; oldest entries are evicted on push. The
// admin router (src/routes/lvsAdmin.ts) reads this via getRecentDeliveries().
// Process-local; lost on restart. Cheap, useful for live debugging.
// --------------------------------------------------------------------
export interface LvsDeliveryRecord {
  /** The recording-finalize run this delivery started. */
  runId?: string;
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
  // Everything that used to happen here — the row, the wait for the composed
  // file, the card in the conversation, transcription — is a pipeline run
  // now: persisted, retried, resumable, visible. The webhook's whole job is
  // to say "a recording finished" and answer quickly.
  try {
    const runId = await startSystemRun(RECORDING_FINALIZE_PIPELINE_ID, buildRecordingFinalizeDefinition(), {
      arn, recordingId, eventType, detail,
    }, 'system');
    recordDelivery({ timestamp: new Date().toISOString(), eventType, arn, recordingId, hmacOk: true, persisted: true, error: undefined, runId } as LvsDeliveryRecord);
    log.info({ arn, recordingId, runId }, '[lvs-webhook] recording-finalize run started');
    res.status(204).end();
  } catch (e: any) {
    log.error({ err: e?.message ?? e, arn, recordingId }, "[lvs-webhook] run start failed");
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
