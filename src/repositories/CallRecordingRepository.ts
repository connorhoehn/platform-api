/**
 * CallRecordingRepository — persists LVS recording metadata indexed by
 * the gateway-side `callId`. Driven by the LVS `recording.completed`
 * webhook arriving at platform-api.
 *
 * Two tables to keep query patterns cheap:
 *
 *   1. `video-call-recordings` (primary)
 *      - PK: callId
 *      - SK: recordingId
 *      - Attributes: channelArn, lobbyName, sessionId, startedAt, endedAt,
 *                    durationMs, totalBytes, segments[], destination,
 *                    participants[], startedBy, ttl
 *
 *   2. `video-call-recording-participants` (reverse index — fan-out write)
 *      - PK: userId
 *      - SK: `${startedAt}#${recordingId}`
 *      - Attributes: callId, channelArn, durationMs, startedAt, endedAt
 *      - Lets the frontend cheap-query "show me my last 20 recordings"
 *        without GSI scaling pain. One row per participant per recording.
 *
 * TTL on both: default 30 days, matches the LVS RecordingGc worker so
 * we don't surface dangling metadata for already-deleted media.
 */

import {
  DynamoDBDocumentClient,
  PutCommand,
  GetCommand,
  QueryCommand,
  ScanCommand,
  DeleteCommand,
  UpdateCommand,
} from '@aws-sdk/lib-dynamodb';
import { BaseRepository } from './BaseRepository';
import { tableName } from '../lib/ddb-table-name';
import {
  deriveMediaDurationMs,
  reconcileParticipants,
  type TruthTrack,
} from '../lib/recording-truth';

export const CALL_RECORDINGS_TABLE = tableName('video-call-recordings');
export const CALL_RECORDING_PARTICIPANTS_TABLE = tableName(
  'video-call-recording-participants',
);

export interface RecordingSegment {
  key: string;
  url: string;
  size: number;
}

/**
 * One per-peer raw track recording, alongside the composed file. When the
 * call had a single participant or compose was disabled, this collapses
 * to `segments[]` and `tracks` is empty.
 */
export interface RecordingTrack {
  participantId?: string;
  userId?: string;
  key: string;
  url: string;
  size: number;
  /** 'video' | 'audio' — when ffmpeg recorded a/v together this stays unset. */
  kind?: 'video' | 'audio' | 'av';
}

export interface CallRecordingParticipant {
  userId: string;
  displayName?: string;
  participantId?: string;
  joinedAt?: string;
}

export interface CallRecording {
  callId: string;
  recordingId: string;
  channelArn: string;
  lobbyName: string;
  sessionId?: string;
  startedAt: string; // ISO
  endedAt: string; // ISO
  /**
   * Length of the RECORDED MEDIA. Derived from the track timeline by
   * `deriveMediaDurationMs` — NOT the SFU channel-session lifetime, which
   * is what LVS puts on the wire and what this field used to hold. See
   * `sessionDurationMs` for that number and `lib/recording-truth.ts` for
   * the measurement that motivated the split.
   */
  durationMs: number;
  /**
   * The SFU channel-session lifetime (ring + lobby wait + media +
   * teardown). Kept because it is genuinely useful for call analytics and
   * because throwing away the raw webhook number would make the change
   * irreversible — but it is NOT what "Duration" means to a viewer, so it
   * is never the field the UI renders. Absent on rows written before
   * 2026-08-25.
   */
  sessionDurationMs?: number;
  totalBytes: number;
  segments: RecordingSegment[];
  destination: 'local' | 's3';
  participants: CallRecordingParticipant[];
  /**
   * True number of people in the recording, derived by the webhook from
   * the recording's per-leg tracks[] ∪ the session roster. NOT always
   * `participants.length`: `participants[]` only holds userId-bearing
   * entries (roster + userId-tagged tracks), while LVS multi-leg tracks
   * carry participantId only — those legs are real tiles in the composed
   * video but can't be attributed to a userId. Absent on historical rows
   * (pre-2026-08-24); readers fall back to `participants.length`.
   */
  participantCount?: number;
  startedBy?: string;
  orgId?: string;
  ttl: number; // epoch seconds
  /** W8 — first-frame poster image generated server-side. Populated
   *  when the webhook payload carried it; null when ffmpeg thumbnail
   *  step failed (rare). Rendered by ui-components <RecordingList>. */
  thumbnailUrl?: string;
  /**
   * R2 (this commit) — composed playback file (ffmpeg xstack grid of all
   * per-peer tracks). When present, the FE uses this as the primary
   * "playback" target — one video showing all participants tiled — and
   * surfaces `tracks[]` as a per-peer raw-downloads list. Null when:
   *   - the call had a single producer (segments[] IS the recording), or
   *   - compose disabled by config / ffmpeg compose step failed.
   */
  composedUrl?: string;
  /** R2 — per-peer raw track files. Empty for single-producer calls. */
  tracks?: RecordingTrack[];
  /**
   * Transcription pipeline state. Sparse — only set when the user opted
   * into transcription for this recording (recordingConfiguration.transcribe
   * was true at start). Lifecycle:
   *   undefined / 'pending' — outbox row published, worker hasn't picked up
   *   'processing'          — worker has the message, faster-whisper running
   *   'ready'               — transcript.json (and summary.md if present)
   *                           written to blob storage; UI can fetch
   *   'failed'              — exhausted DLQ retries
   */
  transcriptStatus?: 'pending' | 'processing' | 'ready' | 'failed';
  /** Blob-storage key (S3/R2) for the merged transcript JSON. Sparse. */
  transcriptKey?: string;
  /** Blob-storage key for the LLM-generated markdown summary. Reserved for
   *  the future when summaries grow past the inline DDB limit. */
  summaryKey?: string;
  /** Inline markdown summary written by the summary consumer. Stored
   *  directly in DDB because a typical 30-min meeting summary is <20 KB,
   *  well under the 400 KB item cap. If summaries ever grow large we'd
   *  switch to summaryKey + blob storage; both fields kept so the
   *  migration is a write-shape change, not a type change. */
  summaryMarkdown?: string;
  /** ISO timestamp when the worker wrote the transcript. */
  transcriptGeneratedAt?: string;
  /** Last-error string when transcriptStatus === 'failed'. Truncated to 200 chars. */
  transcriptError?: string;
}

/**
 * Shape returned by both list endpoints. Shared so the user-scoped list
 * (fan-out index) and the lobby-scoped list (primary-table scan) can't
 * drift into rendering different fields for the same card.
 */
export interface RecordingListRow {
  callId: string;
  recordingId: string;
  startedAt: string;
  endedAt: string;
  /** Media length — see CallRecording.durationMs. */
  durationMs: number;
  sessionDurationMs?: number;
  channelArn: string;
  lobbyName: string;
  thumbnailUrl?: string;
  composedUrl?: string;
  /** Distinct people, not sessions. */
  participantCount?: number;
  /** userId + displayName, so the list can render names, not raw ids. */
  participants?: Array<{ userId: string; displayName?: string }>;
}

const RETENTION_DAYS = Number(process.env.RECORDING_RETENTION_DAYS ?? 30);

/** Identities denormalized onto each fan-out row, for list-view labels. */
const FANOUT_PARTICIPANT_CAP = 12;

/**
 * The set of userIds that must be able to find this recording:
 * `participants[] ∪ {startedBy}`. Unconditional union — see the
 * putRecording docblock for why the old "only when participants is empty"
 * fallback lost the caller on 18% of real recordings.
 *
 * Shared by putRecording and deleteRecording so the write set and the
 * delete set are the same set by construction.
 */
export function fanoutSetFor(rec: {
  participants?: Array<{ userId?: string }> | null;
  startedBy?: string;
}): Set<string> {
  const ids = new Set<string>();
  for (const p of rec.participants ?? []) {
    if (p?.userId) ids.add(p.userId);
  }
  if (rec.startedBy) ids.add(rec.startedBy);
  return ids;
}

/**
 * Read-path safety net for `durationMs`.
 *
 * Rows written before 2026-08-25 hold the SFU session lifetime in
 * `durationMs` — every one of them overstates its own media (measured:
 * 98/98 by >1s, median +10.2s). The backfill script rewrites them, but a
 * derive-on-read guard means a row that the backfill missed — or one
 * written by an LVS pod still on the old payload shape — cannot re-surface
 * the lie. Rows already carrying `sessionDurationMs` were written by the
 * corrected path and are passed through untouched.
 */
function normalizeDuration<T extends { durationMs?: number; sessionDurationMs?: number; tracks?: unknown }>(
  rec: T,
): T {
  if (rec.sessionDurationMs !== undefined) return rec;
  const tracks = Array.isArray(rec.tracks) ? (rec.tracks as TruthTrack[]) : undefined;
  if (!tracks || tracks.length === 0) return rec;
  return {
    ...rec,
    durationMs: deriveMediaDurationMs({ tracks, sessionDurationMs: rec.durationMs }),
    sessionDurationMs: rec.durationMs,
  };
}

export class CallRecordingRepository extends BaseRepository {
  private participantsTable: string;

  constructor(docClient: DynamoDBDocumentClient) {
    super(CALL_RECORDINGS_TABLE, docClient);
    this.participantsTable = CALL_RECORDING_PARTICIPANTS_TABLE;
  }

  /**
   * Persist a completed recording + fan out one row per participant
   * into the reverse-index table. Idempotent — re-running the same
   * webhook payload overwrites both tables without duplicate entries.
   *
   * Visibility guarantee: the fan-out set is ALWAYS `participants[] ∪
   * {startedBy}`. The union with `startedBy` is unconditional and that is
   * the whole point — the previous code only fell back to `startedBy` when
   * `participants[]` was *empty*, which quietly lost exactly the case that
   * matters most: `participants[]` is the SFU's LIVE roster at
   * recording-complete time, and the person who PLACED the call has
   * usually already hung up by then. So a non-empty roster containing only
   * the callee looked like a complete answer, `startedBy` was skipped, and
   * the caller never got an index row. Measured on live data 2026-08-25:
   * 37 of 200 recordings (18%) had no fan-out row for their own
   * `startedBy` — permanently invisible in `GET /api/recordings` to the
   * one person who made the call. Every one of those had a non-empty
   * `participants[]`, so the old empty-only fallback could never fire.
   */
  async putRecording(rec: Omit<CallRecording, 'ttl'>): Promise<void> {
    const ttl = Math.floor(Date.now() / 1000) + RETENTION_DAYS * 86400;
    const full: CallRecording = { ...rec, ttl };
    await this.putItem(full as unknown as Record<string, unknown>);

    const fanoutUserIds = fanoutSetFor(rec);

    // Fan out participant index rows. Promise.all is fine — DDB
    // PutItem is independent per row, and we want one round-trip-batch
    // not 5 serial calls for a 5-person call.
    if (fanoutUserIds.size > 0) {
      await Promise.all(
        Array.from(fanoutUserIds).map((userId) =>
          this.docClient.send(
            new PutCommand({
              TableName: this.participantsTable,
              Item: {
                userId,
                startedAtRecordingId: `${rec.startedAt}#${rec.recordingId}`,
                callId: rec.callId,
                recordingId: rec.recordingId,
                channelArn: rec.channelArn,
                lobbyName: rec.lobbyName,
                startedAt: rec.startedAt,
                endedAt: rec.endedAt,
                // Media length, not session lifetime. The fan-out row has
                // no tracks[] of its own, so this value cannot be re-derived
                // on read — it is the reason the backfill script has to
                // touch BOTH tables (scripts/backfill-recording-truth.ts).
                durationMs: rec.durationMs,
                sessionDurationMs: rec.sessionDurationMs,
                // W8 — denormalize so list view doesn't need a join
                // back to the primary table for thumbnail rendering.
                thumbnailUrl: rec.thumbnailUrl,
                // R2 — denormalize composedUrl onto the fan-out row so
                // the list view can pick "playback target" without a
                // join. tracks[] stays on the primary record (only the
                // detail view needs the raw per-peer URLs).
                composedUrl: rec.composedUrl,
                // Denormalize the reconciled PEOPLE count for the chip
                // render. `participants[]` has already been deduped by
                // userId and scoped to this recording by the webhook, so
                // the `.length` fallback is now a people count too — it
                // used to be a join count (one entry per rejoin), which is
                // how a 2-person DM rendered "7 👥".
                participantCount: rec.participantCount ?? rec.participants?.length ?? 0,
                // Denormalize identities so the list view can render
                // "DM · Bob Martinez & Hank Hill" instead of splitting raw
                // userIds out of the lobby string. Capped so a large room
                // can't push the index row toward the 400KB item limit.
                participants: (rec.participants ?? [])
                  .slice(0, FANOUT_PARTICIPANT_CAP)
                  .map((p) => ({ userId: p.userId, displayName: p.displayName })),
                ttl,
              },
            }),
          ),
        ),
      );
    }
  }

  async getRecording(callId: string, recordingId: string): Promise<CallRecording | null> {
    const rec = await this.getItem<CallRecording>({ callId, recordingId });
    return rec ? normalizeDuration(rec) : null;
  }

  /**
   * Owner-initiated hard delete (audit C4). Removes the primary row from
   * video-call-recordings AND every fan-out row for the recording from
   * video-call-recording-participants. The MP4 stays in LVS storage until
   * the retention sweep — same lifecycle as before.
   *
   * Best-effort participant cleanup: per-row failures are swallowed so
   * one stuck DDB key can't strand the primary deletion. Callers should
   * not surface the count to end-users.
   */
  async deleteRecording(callId: string, recordingId: string): Promise<void> {
    const rec = await this.getRecording(callId, recordingId);
    if (rec) {
      // Mirror putRecording's fan-out set EXACTLY — same helper, so the
      // two can't drift. Any row putRecording wrote, this deletes; a
      // narrower set here would strand an index row that keeps surfacing
      // in GET /api/recordings after the recording is gone.
      const fanoutUserIds = fanoutSetFor(rec);
      if (fanoutUserIds.size > 0) {
        await Promise.all(
          Array.from(fanoutUserIds).map((userId) =>
            this.docClient
              .send(
                new DeleteCommand({
                  TableName: this.participantsTable,
                  Key: { userId, startedAtRecordingId: `${rec.startedAt}#${rec.recordingId}` },
                }),
              )
              .catch(() => { /* swallow — participant index is fan-out, not source of truth */ }),
          ),
        );
      }
    }
    await this.docClient.send(
      new DeleteCommand({
        TableName: CALL_RECORDINGS_TABLE,
        Key: { callId, recordingId },
      }),
    );
  }

  /** All recordings for a given call (usually 1, sometimes 2 if media paused mid-call). */
  async listByCall(callId: string): Promise<CallRecording[]> {
    const out = await this.docClient.send(
      new QueryCommand({
        TableName: CALL_RECORDINGS_TABLE,
        KeyConditionExpression: 'callId = :c',
        ExpressionAttributeValues: { ':c': callId },
      }),
    );
    return ((out.Items ?? []) as CallRecording[]).map(normalizeDuration);
  }

  /**
   * Recent recordings for a user via the fan-out index. Descending by
   * startedAt — newest first. Bounded by `limit` (default 50) and
   * optionally `cursor` (the last `startedAtRecordingId` from a prior
   * page) for keyset pagination.
   */
  async listByUser(
    userId: string,
    opts?: { limit?: number; cursor?: string },
  ): Promise<{ items: RecordingListRow[]; cursor?: string }> {
    const out = await this.docClient.send(
      new QueryCommand({
        TableName: this.participantsTable,
        KeyConditionExpression: 'userId = :u',
        ExpressionAttributeValues: { ':u': userId },
        ScanIndexForward: false, // newest first
        Limit: opts?.limit ?? 50,
        ...(opts?.cursor
          ? {
              ExclusiveStartKey: {
                userId,
                startedAtRecordingId: opts.cursor,
              },
            }
          : {}),
      }),
    );
    const items = (out.Items ?? []).map((i) => ({
      callId: i.callId as string,
      recordingId: i.recordingId as string,
      startedAt: i.startedAt as string,
      endedAt: i.endedAt as string,
      // Media length. The fan-out row has no tracks[], so this cannot be
      // re-derived here — it is whatever the writer stored, which is why
      // scripts/backfill-recording-truth.ts had to rewrite this table.
      durationMs: i.durationMs as number,
      sessionDurationMs: i.sessionDurationMs as number | undefined,
      channelArn: i.channelArn as string,
      lobbyName: i.lobbyName as string,
      thumbnailUrl: i.thumbnailUrl as string | undefined,
      composedUrl: i.composedUrl as string | undefined,
      participantCount: i.participantCount as number | undefined,
      // Denormalized identities (userId + displayName). Absent on rows
      // written before 2026-08-25; the FE falls back to the lobby-derived
      // ids for those.
      participants: Array.isArray(i.participants)
        ? (i.participants as CallRecordingParticipant[])
        : undefined,
    }));
    const cursor = out.LastEvaluatedKey
      ? (out.LastEvaluatedKey.startedAtRecordingId as string)
      : undefined;
    return { items, cursor };
  }

  /**
   * W6 — List recordings for a given lobby (e.g. `room:lounge`). Used by the
   * RoomAdminPage "View recordings" filter so room admins can see every
   * recording produced inside their room.
   *
   * TODO: v1 implementation does a Scan filtered by `lobbyName` (capped at
   * 100 rows per page) since there's no GSI on lobbyName today. Acceptable
   * because the primary table is bounded by RETENTION_DAYS, but if volume
   * grows we should promote `lobbyName` to a GSI HASH with `startedAt` as
   * the RANGE so this becomes a Query.
   *
   * The cursor is the opaque LastEvaluatedKey from the prior page (encoded
   * to a base64 JSON string so it round-trips cleanly through the URL).
   * Results are returned in whatever order DDB Scan yields — callers that
   * need recency-ordering should sort client-side.
   */
  async listByLobbyName(
    lobbyName: string,
    limit = 50,
    cursor?: string,
  ): Promise<{ items: RecordingListRow[]; cursor?: string }> {
    const cappedLimit = Math.max(1, Math.min(100, limit));
    let exclusiveStartKey: Record<string, unknown> | undefined;
    if (cursor) {
      try {
        exclusiveStartKey = JSON.parse(Buffer.from(cursor, 'base64').toString('utf8')) as Record<string, unknown>;
      } catch {
        // Bad cursor → ignore and start from the top.
        exclusiveStartKey = undefined;
      }
    }
    const out = await this.docClient.send(
      new ScanCommand({
        TableName: CALL_RECORDINGS_TABLE,
        FilterExpression: 'lobbyName = :l',
        ExpressionAttributeValues: { ':l': lobbyName },
        Limit: cappedLimit,
        ...(exclusiveStartKey ? { ExclusiveStartKey: exclusiveStartKey } : {}),
      }),
    );
    // This one scans the PRIMARY table, so unlike listByUser it has the
    // full row — tracks[] and the raw roster — and can re-derive the truth
    // on read rather than trusting whatever the writer of the day stored.
    const items: RecordingListRow[] = (out.Items ?? []).map((raw) => {
      const i = normalizeDuration(raw as unknown as CallRecording);
      const people = reconcileParticipants({
        participants: i.participants,
        tracks: i.tracks,
        startedBy: i.startedBy,
        startedAt: i.startedAt,
        endedAt: i.endedAt,
      });
      return {
        callId: i.callId,
        recordingId: i.recordingId,
        startedAt: i.startedAt,
        endedAt: i.endedAt,
        durationMs: i.durationMs,
        sessionDurationMs: i.sessionDurationMs,
        channelArn: i.channelArn,
        lobbyName: i.lobbyName,
        thumbnailUrl: i.thumbnailUrl,
        composedUrl: i.composedUrl,
        // Distinct PEOPLE. Deliberately re-derived instead of reading the
        // stored scalar: legacy rows stored `participants.length`, which
        // counted rejoins (the "2, 3, 4, 5, 6, 7 👥 over one evening" bug).
        // `undefined` rather than 0 when we know nothing — the FE renders
        // no chip for undefined, and "0 people were in this call" would be
        // its own small lie.
        participantCount: people.participantCount || undefined,
        participants: people.participants.length
          ? people.participants.map((p) => ({
              userId: p.userId,
              displayName: p.displayName,
            }))
          : undefined,
      };
    });
    const nextCursor = out.LastEvaluatedKey
      ? Buffer.from(JSON.stringify(out.LastEvaluatedKey), 'utf8').toString('base64')
      : undefined;
    return { items, ...(nextCursor ? { cursor: nextCursor } : {}) };
  }

  /**
   * P5 (H7) — GDPR delete. Removes the user from:
   *  - the participant fan-out table (all their rows in
   *    video-call-recording-participants)
   *  - the participants[] array inside video-call-recordings rows they
   *    were part of (so list-by-call no longer shows them)
   *
   * Does NOT delete the underlying MP4 — that's LVS's responsibility
   * via the GC sweep against retention. Does NOT delete the
   * video-call-recordings row when other participants are still on it.
   *
   * Returns the count of touched rows for audit logging. Best-effort:
   * partial failures log and continue (DDB retries handle transient).
   */
  async deleteByUserId(userId: string): Promise<{ indexRowsDeleted: number; recordingsRedacted: number }> {
    let indexRowsDeleted = 0;
    let recordingsRedacted = 0;

    // Drain the reverse-index page-by-page so a user with thousands of
    // recordings doesn't OOM the process.
    let cursor: string | undefined;
    const touchedRecordingIds = new Set<string>();
    const recordingCallIds = new Map<string, string>(); // recordingId -> callId

    do {
      const page = await this.listByUser(userId, { limit: 100, cursor });
      for (const item of page.items) {
        // Delete the participant-index row.
        try {
          await this.docClient.send(
            new DeleteCommand({
              TableName: this.participantsTable,
              Key: { userId, startedAtRecordingId: `${item.startedAt}#${item.recordingId}` },
            }),
          );
          indexRowsDeleted += 1;
          touchedRecordingIds.add(item.recordingId);
          recordingCallIds.set(item.recordingId, item.callId);
        } catch (_) { /* swallow — best-effort */ }
      }
      cursor = page.cursor;
    } while (cursor);

    // Redact this user from each recording's participants[] array.
    for (const recordingId of touchedRecordingIds) {
      const callId = recordingCallIds.get(recordingId);
      if (!callId) continue;
      const rec = await this.getRecording(callId, recordingId);
      if (!rec) continue;
      const filtered = (rec.participants ?? []).filter((p) => p.userId !== userId);
      try {
        await this.docClient.send(
          new UpdateCommand({
            TableName: CALL_RECORDINGS_TABLE,
            Key: { callId, recordingId },
            UpdateExpression: 'SET participants = :p',
            ExpressionAttributeValues: { ':p': filtered },
          }),
        );
        recordingsRedacted += 1;
      } catch (_) { /* swallow */ }
    }

    return { indexRowsDeleted, recordingsRedacted };
  }

  /**
   * Flip transcriptStatus on a recording row. Used at three transition points:
   *   1. lvsWebhooks  → 'pending'    (right after publishWithOutbox)
   *   2. worker start → 'processing' (after MQ receive, before whisper run)
   *   3. worker done  → 'ready'      (with transcriptKey, transcriptGeneratedAt)
   *   4. DLQ          → 'failed'     (with transcriptError)
   *
   * Conditional on the row existing (attribute_exists(callId)) so a transcript
   * update for a webhook-not-yet-persisted recording fails fast rather than
   * silently creating a transcript-only ghost row.
   */
  async updateTranscriptStatus(
    callId: string,
    recordingId: string,
    patch: {
      status: 'pending' | 'processing' | 'ready' | 'failed';
      transcriptKey?: string;
      summaryKey?: string;
      summaryMarkdown?: string;
      transcriptGeneratedAt?: string;
      transcriptError?: string;
    },
  ): Promise<void> {
    const sets: string[] = ['transcriptStatus = :s'];
    const names: Record<string, string> = {};
    const values: Record<string, unknown> = { ':s': patch.status };
    if (patch.transcriptKey !== undefined) {
      sets.push('transcriptKey = :k');
      values[':k'] = patch.transcriptKey;
    }
    if (patch.summaryKey !== undefined) {
      sets.push('summaryKey = :sk');
      values[':sk'] = patch.summaryKey;
    }
    if (patch.summaryMarkdown !== undefined) {
      sets.push('summaryMarkdown = :sm');
      values[':sm'] = patch.summaryMarkdown;
    }
    if (patch.transcriptGeneratedAt !== undefined) {
      sets.push('transcriptGeneratedAt = :ga');
      values[':ga'] = patch.transcriptGeneratedAt;
    }
    if (patch.transcriptError !== undefined) {
      sets.push('transcriptError = :err');
      // Truncate to 200 chars so a multi-MB stack trace can't bloat the row.
      values[':err'] = String(patch.transcriptError).slice(0, 200);
    }
    await this.docClient.send(
      new UpdateCommand({
        TableName: CALL_RECORDINGS_TABLE,
        Key: { callId, recordingId },
        UpdateExpression: `SET ${sets.join(', ')}`,
        ExpressionAttributeNames: Object.keys(names).length ? names : undefined,
        ExpressionAttributeValues: values,
        ConditionExpression: 'attribute_exists(callId)',
      }),
    );
  }
}

/**
 * Schema for scripts/setup-dynamodb.sh:
 *
 *   aws dynamodb create-table \
 *     --table-name ${DDB_TABLE_PREFIX}video-call-recordings \
 *     --attribute-definitions \
 *       AttributeName=callId,AttributeType=S \
 *       AttributeName=recordingId,AttributeType=S \
 *     --key-schema \
 *       AttributeName=callId,KeyType=HASH \
 *       AttributeName=recordingId,KeyType=RANGE \
 *     --billing-mode PAY_PER_REQUEST
 *
 *   aws dynamodb create-table \
 *     --table-name ${DDB_TABLE_PREFIX}video-call-recording-participants \
 *     --attribute-definitions \
 *       AttributeName=userId,AttributeType=S \
 *       AttributeName=startedAtRecordingId,AttributeType=S \
 *     --key-schema \
 *       AttributeName=userId,KeyType=HASH \
 *       AttributeName=startedAtRecordingId,KeyType=RANGE \
 *     --billing-mode PAY_PER_REQUEST
 *
 *   # TTL on both:
 *   aws dynamodb update-time-to-live --table-name ... \
 *     --time-to-live-specification "Enabled=true, AttributeName=ttl"
 */
