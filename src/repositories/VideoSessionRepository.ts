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

import { DynamoDBDocumentClient, GetCommand, TransactWriteCommand } from '@aws-sdk/lib-dynamodb';
import { HANGOUT_ROOMS_TABLE, hangoutActivityPartition } from './HangoutRoomRepository';
import { BaseRepository } from './BaseRepository';
import { tableName } from '../lib/ddb-table-name';
import {
  VideoSessionRecord,
  VideoSessionParticipant,
  VideoSessionAttendance,
  VideoSessionStore,
  DocumentCallFieldsPatch,
  computeEndedSessionTtl,
  participantSupersedes,
  sameVideoCallBinding,
  VideoCallBindingConflict,
  type VideoParticipantCallBinding,
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

  async bindCallParticipant(documentId: string, sessionId: string, participantId: string, channelArn: string,
    binding: VideoParticipantCallBinding): Promise<VideoParticipantCallBinding | null> {
    if (binding.sessionId !== sessionId || binding.channelArn !== channelArn || binding.lobbyName !== documentId) return null;
    for (let attempt = 0; attempt < 4; attempt += 1) {
      const session = await this.getSession(documentId, sessionId);
      const attendance = session?.participantHistory?.[participantId];
      if (!session || session.status !== 'active' || session.channelArn !== channelArn || session.orgId !== binding.tenant
        || (session.ttl !== undefined && (!Number.isFinite(session.ttl) || session.ttl <= Math.floor(Date.now() / 1000)))
        || !session.participants?.some(p => p.participantId === participantId && p.userId === binding.userId && p.clientId === binding.clientId)
        || !attendance || attendance.leftAt || attendance.userId !== binding.userId || attendance.clientId !== binding.clientId) return null;
      if (attendance.callBinding && !sameVideoCallBinding(attendance.callBinding, binding)) throw new VideoCallBindingConflict();
      const value = attendance.callBinding ?? binding;
      try {
        await this.updateItem({ Key: { documentId, sessionId },
          UpdateExpression: 'SET #history.#pid.#binding = :binding',
          ConditionExpression: '#status = :active AND channelArn = :arn AND orgId = :tenant AND participants = :live'
            + ' AND (attribute_not_exists(#ttl) OR #ttl > :now)'
            + ' AND #history.#pid.#uid = :uid AND #history.#pid.#cid = :cid AND attribute_not_exists(#history.#pid.#left)'
            + (attendance.callBinding ? ' AND #history.#pid.#binding = :previous' : ' AND attribute_not_exists(#history.#pid.#binding)'),
          ExpressionAttributeNames: { '#history': 'participantHistory', '#pid': participantId, '#binding': 'callBinding', '#status': 'status', '#uid': 'userId', '#cid': 'clientId', '#left': 'leftAt', '#ttl': 'ttl' },
          ExpressionAttributeValues: { ':binding': value, ':active': 'active', ':arn': channelArn, ':tenant': binding.tenant,
            ':live': session.participants, ':uid': binding.userId, ':cid': binding.clientId, ':now': Math.floor(Date.now() / 1000), ...(attendance.callBinding ? { ':previous': attendance.callBinding } : {}) },
        });
        return value;
      } catch (error) {
        if ((error as { name?: string }).name !== 'ConditionalCheckFailedException') throw error;
      }
    }
    throw new VideoCallBindingConflict();
  }

  async createSession(record: VideoSessionRecord): Promise<VideoSessionRecord> {
    if (record.hangout) {
      // Session + discoverability are atomic. No Scan of all media sessions,
      // and temporary breakouts remain discoverable after the live meeting ends.
      await this.docClient.send(new TransactWriteCommand({ TransactItems: [
        { Put: { TableName: VIDEO_SESSIONS_TABLE, Item: record } },
        { Put: { TableName: HANGOUT_ROOMS_TABLE, Item: {
          orgId: hangoutActivityPartition(record.hangout.orgId, record.hangout.roomId),
          roomId: `${record.startedAt}#${record.sessionId}`, documentId: record.documentId,
          sessionId: record.sessionId, startedAt: record.startedAt,
          // The referenced session remains the attendance authority; this is only an index.
          ttl: computeEndedSessionTtl(new Date(Date.parse(record.startedAt) + 86_400_000).toISOString()),
        } } },
      ] }));
    } else await this.putItem(record as unknown as Record<string, unknown>);
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
    const session = await this.getSession(documentId, sessionId);
    if (!session) return;
    await this.rememberParticipants(documentId, sessionId, [...(session.participants ?? []), participant]);
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
   * Conditional read-modify-write rather than `list_append`, because appending is
   * exactly the behaviour that leaks: a client that joins twice (a room
   * does) appends twice, and the second entry's participantId is a fresh
   * upstream-minted string that no /end will ever name.
   */
  async addOrReplaceParticipant(
    documentId: string,
    sessionId: string,
    participant: VideoSessionParticipant,
  ): Promise<{ displaced: VideoSessionParticipant[]; remaining: number } | null> {
    return this.mutateParticipants(documentId, sessionId, current => {
      const displaced = current.filter(p => participantSupersedes(p, participant));
      const next = [...current.filter(p => !participantSupersedes(p, participant)), participant];
      return { next, result: { displaced, remaining: next.length } };
    });
  }

  /** Retry conflicting joins/leaves instead of resurrecting a stale live roster. */
  private async mutateParticipants<T>(
    documentId: string, sessionId: string,
    change: (current: VideoSessionParticipant[]) => { next: VideoSessionParticipant[]; result: T },
  ): Promise<T | null> {
    for (let attempt = 0; attempt < 8; attempt++) {
      const session = await this.getSession(documentId, sessionId);
      if (!session) return null;
      const current = session.participants ?? [];
      const { next, result } = change(current);
      await this.rememberParticipants(documentId, sessionId, [...current, ...next]);
      await this.closeAttendance(documentId, sessionId,
        current.filter(p => !next.some(incoming => incoming.participantId === p.participantId)));
      try {
        await this.updateItem({
          Key: { documentId, sessionId },
          UpdateExpression: 'SET participants = :participants',
          ConditionExpression: session.participants === undefined ? 'attribute_not_exists(participants)' : 'participants = :current',
          ExpressionAttributeValues: { ':participants': next, ...(session.participants === undefined ? {} : { ':current': current }) },
        });
        return result;
      } catch (error) {
        if ((error as { name?: string }).name !== 'ConditionalCheckFailedException') throw error;
      }
    }
    throw new Error('Concurrent video participant update did not settle');
  }

  /**
   * History writes target individual map entries, rather than replacing a
   * snapshot. Two joins (or a join and leave) cannot erase each other's
   * authenticated attendance. if_not_exists keeps the first join/leave time
   * when a request is retried. Seed live entries for pre-history rows before
   * /end removes them; already-ended corrupt recordings are not backfilled.
   */
  private async rememberParticipants(
    documentId: string, sessionId: string, participants: VideoSessionParticipant[],
  ): Promise<void> {
    const trusted = participants.filter(p => p.userId && p.participantId);
    if (!trusted.length) return;
    const initial: Record<string, VideoSessionAttendance> = Object.fromEntries(
      trusted.map(p => [p.participantId, { ...p }]),
    );
    await this.updateItem({
      Key: { documentId, sessionId },
      UpdateExpression: 'SET #history = if_not_exists(#history, :initial)',
      ConditionExpression: 'attribute_exists(documentId)',
      ExpressionAttributeNames: { '#history': 'participantHistory' },
      ExpressionAttributeValues: { ':initial': initial },
    });
    for (const participant of trusted) {
      await this.updateItem({
        Key: { documentId, sessionId },
        UpdateExpression: 'SET #history.#pid = if_not_exists(#history.#pid, :attendance)',
        ConditionExpression: 'attribute_exists(documentId)',
        ExpressionAttributeNames: { '#history': 'participantHistory', '#pid': participant.participantId },
        ExpressionAttributeValues: { ':attendance': participant },
      });
    }
  }

  private async closeAttendance(
    documentId: string, sessionId: string, participants: VideoSessionParticipant[],
  ): Promise<void> {
    const leftAt = new Date().toISOString();
    for (const participant of participants) {
      if (!participant.userId || !participant.participantId) continue;
      await this.updateItem({
        Key: { documentId, sessionId },
        UpdateExpression: 'SET #history.#pid.#left = if_not_exists(#history.#pid.#left, :leftAt)',
        ConditionExpression: 'attribute_exists(#history.#pid)',
        ExpressionAttributeNames: { '#history': 'participantHistory', '#pid': participant.participantId, '#left': 'leftAt' },
        ExpressionAttributeValues: { ':leftAt': leftAt },
      });
    }
  }

  async getSession(
    documentId: string,
    sessionId: string,
  ): Promise<VideoSessionRecord | null> {
    const result = await this.docClient.send(new GetCommand({
      TableName: VIDEO_SESSIONS_TABLE, Key: { documentId, sessionId }, ConsistentRead: true,
    }));
    return (result.Item as VideoSessionRecord) ?? null;
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
    const items: VideoSessionRecord[] = [];
    let cursor: Record<string, unknown> | undefined;
    const seen = new Set<string>();
    do {
      const out = await this.docClient.send(new ScanCommand({
        TableName: VIDEO_SESSIONS_TABLE, ConsistentRead: true,
        FilterExpression: 'channelArn = :arn', ExpressionAttributeValues: { ':arn': channelArn },
        ...(cursor ? { ExclusiveStartKey: cursor } : {}),
      }));
      items.push(...(out.Items ?? []) as VideoSessionRecord[]);
      cursor = out.LastEvaluatedKey;
      if (cursor) {
        const key = JSON.stringify(cursor);
        if (seen.has(key)) throw new Error('Video session pagination did not advance');
        seen.add(key);
      }
    } while (cursor);
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
    return this.mutateParticipants(documentId, sessionId, current => {
      const next = current.filter(p => p.participantId !== participantId);
      return { next, result: { remaining: next.length } };
    });
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
    return this.mutateParticipants(documentId, sessionId, current => {
      const next = current.filter(p => p.userId !== userId);
      return { next, result: { remaining: next.length } };
    });
  }
}
