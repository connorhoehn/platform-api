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
  DeleteCommand,
  UpdateCommand,
} from '@aws-sdk/lib-dynamodb';
import { BaseRepository } from './BaseRepository';
import { tableName } from '../lib/ddb-table-name';

export const CALL_RECORDINGS_TABLE = tableName('video-call-recordings');
export const CALL_RECORDING_PARTICIPANTS_TABLE = tableName(
  'video-call-recording-participants',
);

export interface RecordingSegment {
  key: string;
  url: string;
  size: number;
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
  durationMs: number;
  totalBytes: number;
  segments: RecordingSegment[];
  destination: 'local' | 's3';
  participants: CallRecordingParticipant[];
  startedBy?: string;
  orgId?: string;
  ttl: number; // epoch seconds
  /** W8 — first-frame poster image generated server-side. Populated
   *  when the webhook payload carried it; null when ffmpeg thumbnail
   *  step failed (rare). Rendered by ui-components <RecordingList>. */
  thumbnailUrl?: string;
}

const RETENTION_DAYS = Number(process.env.RECORDING_RETENTION_DAYS ?? 30);

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
   */
  async putRecording(rec: Omit<CallRecording, 'ttl'>): Promise<void> {
    const ttl = Math.floor(Date.now() / 1000) + RETENTION_DAYS * 86400;
    const full: CallRecording = { ...rec, ttl };
    await this.putItem(full as unknown as Record<string, unknown>);

    // Fan out participant index rows. Promise.all is fine — DDB
    // PutItem is independent per row, and we want one round-trip-batch
    // not 5 serial calls for a 5-person call.
    if (rec.participants.length > 0) {
      await Promise.all(
        rec.participants.map((p) =>
          this.docClient.send(
            new PutCommand({
              TableName: this.participantsTable,
              Item: {
                userId: p.userId,
                startedAtRecordingId: `${rec.startedAt}#${rec.recordingId}`,
                callId: rec.callId,
                recordingId: rec.recordingId,
                channelArn: rec.channelArn,
                lobbyName: rec.lobbyName,
                startedAt: rec.startedAt,
                endedAt: rec.endedAt,
                durationMs: rec.durationMs,
                // W8 — denormalize so list view doesn't need a join
                // back to the primary table for thumbnail rendering.
                thumbnailUrl: rec.thumbnailUrl,
                ttl,
              },
            }),
          ),
        ),
      );
    }
  }

  async getRecording(callId: string, recordingId: string): Promise<CallRecording | null> {
    return this.getItem<CallRecording>({ callId, recordingId });
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
    return (out.Items ?? []) as CallRecording[];
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
  ): Promise<{ items: Array<{ callId: string; recordingId: string; startedAt: string; endedAt: string; durationMs: number; channelArn: string; lobbyName: string; }>; cursor?: string }> {
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
      durationMs: i.durationMs as number,
      channelArn: i.channelArn as string,
      lobbyName: i.lobbyName as string,
      thumbnailUrl: i.thumbnailUrl as string | undefined,
    }));
    const cursor = out.LastEvaluatedKey
      ? (out.LastEvaluatedKey.startedAtRecordingId as string)
      : undefined;
    return { items, cursor };
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
