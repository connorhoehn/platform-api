// DDB-backed implementation of `VideoSessionStore`. See
// `VideoSessionStore.ts` for the interface contract + design notes.
//
// Schema:
//   PK = documentId  (lobbyName — see VideoSessionStore docstring)
//   SK = sessionId
//   TTL attribute = `ttl` (epoch seconds). Set automatically by endSession
//   so ended sessions get GC'd after the configured grace window.
//   `ensureVideoSessionsTTL()` in src/lib/dynamo-setup.ts enables it on
//   the table at startup (idempotent).

import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { BaseRepository } from './BaseRepository';
import { tableName } from '../lib/ddb-table-name';
import {
  VideoSessionRecord,
  VideoSessionParticipant,
  VideoSessionStore,
  computeEndedSessionTtl,
} from './VideoSessionStore';

export type {
  VideoSessionRecord,
  VideoSessionParticipant,
  VideoSessionStore,
} from './VideoSessionStore';

export const VIDEO_SESSIONS_TABLE = tableName('document-video-sessions');

export class VideoSessionRepository extends BaseRepository implements VideoSessionStore {
  constructor(docClient: DynamoDBDocumentClient) {
    super(VIDEO_SESSIONS_TABLE, docClient);
  }

  async createSession(record: VideoSessionRecord): Promise<VideoSessionRecord> {
    await this.putItem(record as unknown as Record<string, unknown>);
    return record;
  }

  async endSession(
    documentId: string,
    sessionId: string,
    endedAt: string,
  ): Promise<void> {
    // Stamp TTL alongside the status change so the row eventually GCs.
    // Per-deployment override via VIDEO_SESSION_TTL_DAYS (see computeEndedSessionTtl).
    const ttl = computeEndedSessionTtl(endedAt);
    await this.updateItem({
      Key: { documentId, sessionId },
      UpdateExpression: 'SET #status = :status, endedAt = :endedAt, #ttl = :ttl',
      ExpressionAttributeNames: { '#status': 'status', '#ttl': 'ttl' },
      ExpressionAttributeValues: {
        ':status': 'ended',
        ':endedAt': endedAt,
        ':ttl': ttl,
      },
    });
  }

  async addParticipant(
    documentId: string,
    sessionId: string,
    participant: VideoSessionParticipant,
  ): Promise<void> {
    await this.updateItem({
      Key: { documentId, sessionId },
      UpdateExpression: 'SET participants = list_append(if_not_exists(participants, :empty), :participant)',
      ExpressionAttributeValues: {
        ':participant': [participant],
        ':empty': [],
      },
    });
  }

  async getSession(
    documentId: string,
    sessionId: string,
  ): Promise<VideoSessionRecord | null> {
    return this.getItem<VideoSessionRecord>({ documentId, sessionId });
  }

  async getSessionsByDocument(documentId: string): Promise<VideoSessionRecord[]> {
    const results = await this.query<VideoSessionRecord>({
      KeyConditionExpression: 'documentId = :docId',
      ExpressionAttributeValues: { ':docId': documentId },
    });
    // Sort by startedAt descending (newest first) since sessionId (UUID) is not temporal
    return results.sort((a, b) => (b.startedAt ?? '').localeCompare(a.startedAt ?? ''));
  }

  /**
   * Find the newest active session for a given lobby name (a/k/a documentId —
   * lobbyName is just a more honest field name now that this powers global /
   * 1:1 / room lobbies in addition to document-scoped calls). Returns null
   * if no active session exists.
   *
   * Used by `findOrCreate` on POST /api/video/sessions to dedupe concurrent
   * joiners onto the same SFU channel.
   */
  async findActiveByLobbyName(lobbyName: string): Promise<VideoSessionRecord | null> {
    const all = await this.getSessionsByDocument(lobbyName);
    return all.find(s => s.status === 'active') ?? null;
  }

  /**
   * Remove a participant from a session by per-tab participantId. The dedup
   * key on participants[] (replacing the older userId-keyed approach) so that
   * multiple tabs of the same user each get their own ref-count slot.
   * Returns the new participants array length so callers can ref-count.
   */
  async removeParticipant(
    documentId: string,
    sessionId: string,
    participantId: string,
  ): Promise<{ remaining: number } | null> {
    const session = await this.getSession(documentId, sessionId);
    if (!session) return null;
    const next = (session.participants ?? []).filter(p => p.participantId !== participantId);
    await this.updateItem({
      Key: { documentId, sessionId },
      UpdateExpression: 'SET participants = :participants',
      ExpressionAttributeValues: { ':participants': next },
    });
    return { remaining: next.length };
  }

  /**
   * Legacy: remove ALL participants matching a userId. Used only on the
   * /end fallback when an older FE client hasn't sent participantId in the
   * sendBeacon payload. New clients always send participantId.
   */
  async removeParticipantByUserId(
    documentId: string,
    sessionId: string,
    userId: string,
  ): Promise<{ remaining: number } | null> {
    const session = await this.getSession(documentId, sessionId);
    if (!session) return null;
    const next = (session.participants ?? []).filter(p => p.userId !== userId);
    await this.updateItem({
      Key: { documentId, sessionId },
      UpdateExpression: 'SET participants = :participants',
      ExpressionAttributeValues: { ':participants': next },
    });
    return { remaining: next.length };
  }
}
