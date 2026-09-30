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
  DocumentCallFieldsPatch,
  computeEndedSessionTtl,
  participantSupersedes,
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

  /**
   * See VideoSessionStore.addOrReplaceParticipant.
   *
   * Read-modify-write rather than `list_append`, because appending is
   * exactly the behaviour that leaks: a client that joins twice (a room
   * does) appends twice, and the second entry's participantId is a fresh
   * upstream-minted string that no /end will ever name.
   */
  async addOrReplaceParticipant(
    documentId: string,
    sessionId: string,
    participant: VideoSessionParticipant,
  ): Promise<{ displaced: VideoSessionParticipant[]; remaining: number } | null> {
    const session = await this.getSession(documentId, sessionId);
    if (!session) return null;
    const current = session.participants ?? [];
    const displaced = current.filter((p) => participantSupersedes(p, participant));
    const next = [...current.filter((p) => !participantSupersedes(p, participant)), participant];
    await this.updateItem({
      Key: { documentId, sessionId },
      UpdateExpression: 'SET participants = :participants',
      ExpressionAttributeValues: { ':participants': next },
    });
    return { displaced, remaining: next.length };
  }

  async getSession(
    documentId: string,
    sessionId: string,
  ): Promise<VideoSessionRecord | null> {
    return this.getItem<VideoSessionRecord>({ documentId, sessionId });
  }

  async addGuests(
    documentId: string,
    sessionId: string,
    userIds: string[],
  ): Promise<VideoSessionRecord | null> {
    const existing = await this.getSession(documentId, sessionId);
    if (!existing) return null;
    const guests = [...new Set([...(existing.guests ?? []), ...userIds])];
    if (guests.length === (existing.guests ?? []).length) return existing;
    await this.updateItem({
      Key: { documentId, sessionId },
      UpdateExpression: 'SET guests = :guests',
      ExpressionAttributeValues: { ':guests': guests },
    });
    return { ...existing, guests };
  }

  async updateDocumentCallFields(
    documentId: string,
    sessionId: string,
    patch: DocumentCallFieldsPatch,
  ): Promise<VideoSessionRecord | null> {
    const entries = Object.entries(patch).filter(([, v]) => v !== undefined);
    const existing = await this.getSession(documentId, sessionId);
    if (!existing) return null;
    if (entries.length === 0) return existing;
    const names: Record<string, string> = {};
    const values: Record<string, unknown> = {};
    const sets = entries.map(([k, v], i) => {
      names[`#f${i}`] = k;
      values[`:v${i}`] = v;
      return `#f${i} = :v${i}`;
    });
    await this.updateItem({
      Key: { documentId, sessionId },
      UpdateExpression: `SET ${sets.join(', ')}`,
      ExpressionAttributeNames: names,
      ExpressionAttributeValues: values,
    });
    return { ...existing, ...Object.fromEntries(entries) } as VideoSessionRecord;
  }

  async findActiveByReviewDocument(documentId: string): Promise<VideoSessionRecord[]> {
    const { ScanCommand } = await import('@aws-sdk/lib-dynamodb');
    const items: VideoSessionRecord[] = [];
    let startKey: Record<string, unknown> | undefined;
    do {
      const out = await this.docClient.send(
        new ScanCommand({
          TableName: VIDEO_SESSIONS_TABLE,
          FilterExpression: '#s = :active AND #k = :kind AND contains(documentIds, :doc)',
          ExpressionAttributeNames: { '#s': 'status', '#k': 'kind' },
          ExpressionAttributeValues: { ':active': 'active', ':kind': 'document-review', ':doc': documentId },
          ExclusiveStartKey: startKey,
        }),
      );
      items.push(...((out.Items ?? []) as VideoSessionRecord[]));
      startKey = out.LastEvaluatedKey as Record<string, unknown> | undefined;
    } while (startKey);
    return items.sort((a, b) => (b.startedAt ?? '').localeCompare(a.startedAt ?? ''));
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
   * DDB-only fallback used by the LVS recording-completed webhook when the
   * Redis arn-index has expired or Redis is down. Scans the sessions table
   * with a FilterExpression on `channelArn`. Costs ~O(table) but the table
   * is TTL-bounded (VIDEO_SESSION_TTL_DAYS, default 7d) so the scan stays
   * cheap in practice — and this only fires when Redis missed.
   *
   * Returned newest-first. Caller usually wants `[0]` (most recent session
   * for the arn). Returns [] when no match.
   *
   * Without this fallback, DM recordings disappeared whenever Redis dropped
   * the arn-index between session-create and call-end: the webhook
   * `lobbyName` would resolve to ''  → `startedBy` undefined → fan-out
   * skipped → row stranded in primary table, invisible to listByUser.
   */
  async findByChannelArn(channelArn: string): Promise<VideoSessionRecord[]> {
    const { ScanCommand } = await import('@aws-sdk/lib-dynamodb');
    const out = await this.docClient.send(
      new ScanCommand({
        TableName: VIDEO_SESSIONS_TABLE,
        FilterExpression: 'channelArn = :arn',
        ExpressionAttributeValues: { ':arn': channelArn },
      }),
    );
    const items = (out.Items ?? []) as VideoSessionRecord[];
    return items.sort((a, b) => (b.startedAt ?? '').localeCompare(a.startedAt ?? ''));
  }

  /**
   * Every session still marked active, newest-first per lobby.
   *
   * Answers "which conversations might have something on right now" in ONE
   * ask, so a conversation list can badge every row instead of resolving each
   * lobby separately.
   *
   * `status: 'active'` is a LIFECYCLE flag and it leaks — a room call joins
   * twice and hangs up once, so the ref-count rarely reaches zero and rows
   * stay 'active' long after everyone has gone. This method therefore returns
   * CANDIDATES, never an answer: the caller must ask the SFU whether each
   * channel is actually publishing. Treating this list as truth is how every
   * room that ever had a call ends up advertising a live stream.
   *
   * A Scan, like `findByChannelArn` — the table is TTL-bounded
   * (VIDEO_SESSION_TTL_DAYS, default 7d) so it stays cheap, and `limit` caps
   * the work regardless.
   */
  async listActiveCandidates(
    limit = 60,
    opts: { perLobby?: boolean } = {},
  ): Promise<VideoSessionRecord[]> {
    const perLobby = opts.perLobby !== false;
    const { ScanCommand } = await import('@aws-sdk/lib-dynamodb');
    const out = await this.docClient.send(
      new ScanCommand({
        TableName: VIDEO_SESSIONS_TABLE,
        FilterExpression: '#s = :active',
        ExpressionAttributeNames: { '#s': 'status' },
        ExpressionAttributeValues: { ':active': 'active' },
      }),
    );
    const items = (out.Items ?? []) as VideoSessionRecord[];
    items.sort((a, b) => (b.startedAt ?? '').localeCompare(a.startedAt ?? ''));
    if (!perLobby) return items.filter((i) => !!i.documentId).slice(0, limit);
    // One per lobby: a lobby with several 'active' rows has been through the
    // leak more than once, and only the newest could possibly be publishing.
    const newestPerLobby = new Map<string, VideoSessionRecord>();
    for (const item of items) {
      const lobby = item.documentId;
      if (!lobby || newestPerLobby.has(lobby)) continue;
      newestPerLobby.set(lobby, item);
      if (newestPerLobby.size >= limit) break;
    }
    return [...newestPerLobby.values()];
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
