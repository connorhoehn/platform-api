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
 * HMAC verification: optional `LVS_WEBHOOK_SECRET` env. When set, every
 * incoming request must have `X-LVS-Signature: sha256=<hex>` matching
 * HMAC(secret, rawBody). Missing/invalid → 401. When env unset (dev),
 * we accept all calls — Tilt-only, never deploy without the secret.
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
import { docClient } from '../lib/aws-clients';

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

// Capture raw body for HMAC. The router-scoped raw parser leaves
// req.body as a Buffer; we JSON.parse it after signature verification.
lvsWebhooksRouter.use(
  require('express').raw({ type: 'application/json', limit: '256kb' }),
);

const SECRET = process.env.LVS_WEBHOOK_SECRET ?? '';

interface HmacVerifyResult {
  ok: boolean;
  /** Computed expected signature length (hex chars) — null when we can't recompute (e.g., body bytes lost). */
  expectedLen: number | null;
}

function verifyHmacDetailed(req: Request): HmacVerifyResult {
  if (!SECRET) return { ok: true, expectedLen: null }; // dev: skip
  const header = req.header('X-LVS-Signature') ?? '';
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
    return { ok: false, expectedLen: null };
  }
  const expected = createHmac('sha256', SECRET)
    .update(bodyBytes)
    .digest('hex');
  const provided = header.replace(/^sha256=/, '');
  if (provided.length !== expected.length) {
    return { ok: false, expectedLen: expected.length };
  }
  try {
    const ok = timingSafeEqual(
      Buffer.from(provided, 'hex'),
      Buffer.from(expected, 'hex'),
    );
    return { ok, expectedLen: expected.length };
  } catch {
    return { ok: false, expectedLen: expected.length };
  }
}

function verifyHmac(req: Request): boolean {
  return verifyHmacDetailed(req).ok;
}

// Resolve callId from lobbyName by querying the gateway's
// call-session-bindings table. The gateway is the writer; platform-api
// is a read-only consumer of that table. (Same DDB account.)
const ddbRaw = new DynamoDBClient({});

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

  // Normalize: LVS sends EventBridge envelope `{detail-type, detail:{...}}`
  // when WEBHOOK_FORMAT=eventbridge, or bare payload otherwise.
  const eventType: string = payload['detail-type'] ?? payload.type ?? '';
  const detail = payload.detail ?? payload;
  const arn: string = detail.arn ?? detail.channelArn ?? '';
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
          log.warn({ arn, sessionId: idx.sessionId }, '[lvs-webhook] arn-index hit but VideoSession missing — proceeding with limited metadata');
        }
      } else {
        log.warn({ arn }, '[lvs-webhook] no arn-index entry — proceeding with limited metadata');
      }
    }
  } catch (lookupErr: any) {
    log.warn({ err: lookupErr?.message ?? lookupErr, arn }, '[lvs-webhook] arn lookup threw — proceeding with limited metadata');
  }

  const lobbyName = session?.documentId ?? '';
  const callId = lobbyName ? await resolveCallIdByLobby(lobbyName) : null;

  if (!callId) {
    log.warn({ arn, lobbyName }, '[lvs-webhook] no callId binding — persisting under recordingId-only');
  }

  // Convert VideoSession participants → CallRecordingParticipant
  const participants: CallRecordingParticipant[] = (session?.participants ?? []).map((p: any) => ({
    userId: p.userId,
    displayName: p.displayName,
    participantId: p.participantId,
    joinedAt: p.joinedAt,
  }));

  try {
    await callRecordingRepo.putRecording({
      callId: callId ?? `unknown-${arn}-${recordingId}`,
      recordingId,
      channelArn: arn,
      lobbyName,
      sessionId: session?.sessionId,
      startedAt: detail.startedAt ?? new Date(Date.now() - (detail.durationMs ?? 0)).toISOString(),
      endedAt: detail.endedAt ?? new Date().toISOString(),
      durationMs: Number(detail.durationMs ?? 0),
      totalBytes: Number(detail.totalSize ?? detail.totalBytes ?? 0),
      segments: Array.isArray(detail.segments) ? detail.segments : [],
      destination: detail.destination === 's3' ? 's3' : 'local',
      participants,
      startedBy: session?.createdBy ?? participants[0]?.userId,
      // W8 — thumbnail surfaced by LVS RecordingManager. May be a
      // file:// URL in dev (hostPath) or a backend-specific URL in
      // prod (S3 signed / CloudFront / etc.). Null when ffmpeg
      // thumbnail step failed.
      thumbnailUrl: typeof detail.thumbnailUrl === 'string' ? detail.thumbnailUrl : undefined,
    });
    log.info({ callId, recordingId, arn, participants: participants.length }, '[lvs-webhook] recording persisted');
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
