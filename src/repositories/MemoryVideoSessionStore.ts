// In-memory implementation of `VideoSessionStore`. State lives in a
// process-local Map; lost on restart.
//
// Use cases:
//   - Tests that want to exercise route logic against a real store without
//     mocking DynamoDB.
//   - Emergency rollback via `VIDEO_SESSIONS_STORE=memory` if the DDB
//     code path ever becomes the failure mode. Operators understand that
//     restart = state loss in this mode (this is the same behaviour
//     platform-api had before the DDB-backed store landed).
//
// Semantics mirror the DDB store one-to-one, including TTL behaviour:
// reads filter out records whose `ttl` is in the past, so "endSession +
// wait past grace window" yields a null get just like DDB would.

import {
  VideoSessionRecord,
  VideoSessionParticipant,
  VideoSessionStore,
  DocumentCallFieldsPatch,
  computeEndedSessionTtl,
  participantSupersedes,
} from './VideoSessionStore';

/**
 * Composite key helper. Mirrors the DDB partition+sort key shape so the
 * Map index aligns with the DDB index for cross-store consistency.
 */
function makeKey(documentId: string, sessionId: string): string {
  return `${documentId}#${sessionId}`;
}

export class MemoryVideoSessionStore implements VideoSessionStore {
  private readonly sessions = new Map<string, VideoSessionRecord>();

  /**
   * Returns the record only if its TTL hasn't passed. Used by every read
   * path so callers see DDB-equivalent expiry behaviour.
   */
  private readOrExpire(key: string): VideoSessionRecord | undefined {
    const rec = this.sessions.get(key);
    if (!rec) return undefined;
    if (rec.ttl !== undefined && rec.ttl * 1000 <= Date.now()) {
      this.sessions.delete(key);
      return undefined;
    }
    return rec;
  }

  async createSession(record: VideoSessionRecord): Promise<VideoSessionRecord> {
    // Defensive clone so callers can mutate their copy without poisoning ours.
    const cloned = JSON.parse(JSON.stringify(record)) as VideoSessionRecord;
    this.sessions.set(makeKey(record.documentId, record.sessionId), cloned);
    return record;
  }

  async endSession(
    documentId: string,
    sessionId: string,
    endedAt: string,
  ): Promise<void> {
    const key = makeKey(documentId, sessionId);
    const rec = this.sessions.get(key);
    if (!rec) return;
    rec.status = 'ended';
    rec.endedAt = endedAt;
    rec.ttl = computeEndedSessionTtl(endedAt);
  }

  async addParticipant(
    documentId: string,
    sessionId: string,
    participant: VideoSessionParticipant,
  ): Promise<void> {
    const key = makeKey(documentId, sessionId);
    const rec = this.sessions.get(key);
    if (!rec) return;
    // DDB's list_append doesn't dedupe; we match that behaviour. Callers
    // are responsible for idempotency (the route layer does this today).
    rec.participants = [...(rec.participants ?? []), { ...participant }];
  }

  /** See VideoSessionStore.addOrReplaceParticipant. */
  async addOrReplaceParticipant(
    documentId: string,
    sessionId: string,
    participant: VideoSessionParticipant,
  ): Promise<{ displaced: VideoSessionParticipant[]; remaining: number } | null> {
    const key = makeKey(documentId, sessionId);
    const rec = this.sessions.get(key);
    if (!rec) return null;
    const current = rec.participants ?? [];
    const displaced = current.filter((p) => participantSupersedes(p, participant));
    const kept = current.filter((p) => !participantSupersedes(p, participant));
    rec.participants = [...kept, { ...participant }];
    return {
      displaced: displaced.map((p) => ({ ...p })),
      remaining: rec.participants.length,
    };
  }

  async removeParticipant(
    documentId: string,
    sessionId: string,
    participantId: string,
  ): Promise<{ remaining: number } | null> {
    const key = makeKey(documentId, sessionId);
    const rec = this.sessions.get(key);
    if (!rec) return null;
    rec.participants = (rec.participants ?? []).filter(p => p.participantId !== participantId);
    return { remaining: rec.participants.length };
  }

  async removeParticipantByUserId(
    documentId: string,
    sessionId: string,
    userId: string,
  ): Promise<{ remaining: number } | null> {
    const key = makeKey(documentId, sessionId);
    const rec = this.sessions.get(key);
    if (!rec) return null;
    rec.participants = (rec.participants ?? []).filter(p => p.userId !== userId);
    return { remaining: rec.participants.length };
  }

  async getSession(
    documentId: string,
    sessionId: string,
  ): Promise<VideoSessionRecord | null> {
    const rec = this.readOrExpire(makeKey(documentId, sessionId));
    return rec ? (JSON.parse(JSON.stringify(rec)) as VideoSessionRecord) : null;
  }

  async updateDocumentCallFields(
    documentId: string,
    sessionId: string,
    patch: DocumentCallFieldsPatch,
  ): Promise<VideoSessionRecord | null> {
    const key = makeKey(documentId, sessionId);
    const rec = this.readOrExpire(key);
    if (!rec) return null;
    for (const [k, v] of Object.entries(patch)) {
      if (v === undefined) continue;
      (rec as unknown as Record<string, unknown>)[k] = Array.isArray(v) ? [...v] : v;
    }
    return JSON.parse(JSON.stringify(rec)) as VideoSessionRecord;
  }

  async findActiveByReviewDocument(documentId: string): Promise<VideoSessionRecord[]> {
    const out: VideoSessionRecord[] = [];
    for (const key of [...this.sessions.keys()]) {
      const live = this.readOrExpire(key);
      if (!live || live.status !== 'active' || live.kind !== 'document-review') continue;
      if (!(live.documentIds ?? []).includes(documentId)) continue;
      out.push(JSON.parse(JSON.stringify(live)) as VideoSessionRecord);
    }
    return out.sort((a, b) => (b.startedAt ?? '').localeCompare(a.startedAt ?? ''));
  }

  async getSessionsByDocument(documentId: string): Promise<VideoSessionRecord[]> {
    const out: VideoSessionRecord[] = [];
    for (const [key, rec] of this.sessions.entries()) {
      if (!key.startsWith(`${documentId}#`)) continue;
      // Use readOrExpire so expired records get pruned during the scan.
      const live = this.readOrExpire(key);
      if (live) out.push(JSON.parse(JSON.stringify(live)) as VideoSessionRecord);
    }
    return out.sort((a, b) => (b.startedAt ?? '').localeCompare(a.startedAt ?? ''));
  }

  async findActiveByLobbyName(lobbyName: string): Promise<VideoSessionRecord | null> {
    const all = await this.getSessionsByDocument(lobbyName);
    return all.find(s => s.status === 'active') ?? null;
  }

  /** Parity with the DDB impl — see VideoSessionStore.findByChannelArn. */
  async findByChannelArn(channelArn: string): Promise<VideoSessionRecord[]> {
    const out: VideoSessionRecord[] = [];
    for (const key of this.sessions.keys()) {
      const live = this.readOrExpire(key);
      if (live && live.channelArn === channelArn) {
        out.push(JSON.parse(JSON.stringify(live)) as VideoSessionRecord);
      }
    }
    return out.sort((a, b) => (b.startedAt ?? '').localeCompare(a.startedAt ?? ''));
  }

  /** Parity with the DDB impl — see VideoSessionStore.listActiveCandidates. */
  async listActiveCandidates(
    limit = 60,
    opts: { perLobby?: boolean } = {},
  ): Promise<VideoSessionRecord[]> {
    const active = [...this.sessions.values()]
      .filter((s) => s.status === 'active' && !!s.documentId)
      .sort((a, b) => (b.startedAt ?? '').localeCompare(a.startedAt ?? ''));
    if (opts.perLobby === false) return active.slice(0, limit);
    const newestPerLobby = new Map<string, VideoSessionRecord>();
    for (const item of active) {
      if (newestPerLobby.has(item.documentId)) continue;
      newestPerLobby.set(item.documentId, item);
      if (newestPerLobby.size >= limit) break;
    }
    return [...newestPerLobby.values()];
  }

  /** Test helper — clear all state. Not part of the public interface. */
  __clearForTests(): void {
    this.sessions.clear();
  }

  /** Test helper — peek size without TTL filtering. Not part of the interface. */
  __size(): number {
    return this.sessions.size;
  }
}
