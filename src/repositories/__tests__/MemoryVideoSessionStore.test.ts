// Unit tests for MemoryVideoSessionStore. Mirrors the DDB-backed
// VideoSessionRepository's surface; we additionally assert TTL-based
// expiry happens at read time so the in-memory mode behaves like DDB's
// native TTL.

import { MemoryVideoSessionStore } from '../MemoryVideoSessionStore';
import type {
  VideoSessionRecord,
  VideoSessionParticipant,
} from '../VideoSessionStore';
import { computeEndedSessionTtl } from '../VideoSessionStore';

const PARTICIPANT_A: VideoSessionParticipant = {
  userId: 'user-1',
  participantId: 'tab-a',
  displayName: 'Alice',
  joinedAt: '2026-01-01T10:00:00Z',
};

const PARTICIPANT_B: VideoSessionParticipant = {
  userId: 'user-2',
  participantId: 'tab-b',
  displayName: 'Bob',
  joinedAt: '2026-01-01T10:05:00Z',
};

function makeSession(overrides: Partial<VideoSessionRecord> = {}): VideoSessionRecord {
  return {
    documentId: 'lobby-1',
    sessionId: 'sess-1',
    channelArn: 'arn:aws:ivs:us-east-1:123:channel/abc',
    streamKey: 'sk-abc',
    status: 'active',
    startedAt: '2026-01-01T10:00:00Z',
    startedBy: 'user-1',
    participants: [PARTICIPANT_A],
    ...overrides,
  };
}

describe('MemoryVideoSessionStore.createSession + getSession', () => {
  it('round-trips a record by composite key', async () => {
    const store = new MemoryVideoSessionStore();
    await store.createSession(makeSession());
    const got = await store.getSession('lobby-1', 'sess-1');
    expect(got).toEqual(makeSession());
  });

  it('returns null for unknown keys', async () => {
    const store = new MemoryVideoSessionStore();
    expect(await store.getSession('nope', 'nope')).toBeNull();
  });

  it('isolates the stored record from caller mutation (defensive copy)', async () => {
    const store = new MemoryVideoSessionStore();
    const rec = makeSession();
    await store.createSession(rec);
    rec.participants.push(PARTICIPANT_B); // mutate caller's copy after write
    const got = await store.getSession('lobby-1', 'sess-1');
    expect(got!.participants).toHaveLength(1);
  });
});

describe('MemoryVideoSessionStore.addParticipant / removeParticipant', () => {
  it('appends without dedupe (matches DDB list_append)', async () => {
    const store = new MemoryVideoSessionStore();
    await store.createSession(makeSession({ participants: [] }));
    await store.addParticipant('lobby-1', 'sess-1', PARTICIPANT_A);
    await store.addParticipant('lobby-1', 'sess-1', PARTICIPANT_A);
    const got = await store.getSession('lobby-1', 'sess-1');
    expect(got!.participants).toHaveLength(2);
  });

  it('removeParticipant returns remaining count after filter', async () => {
    const store = new MemoryVideoSessionStore();
    await store.createSession(
      makeSession({ participants: [PARTICIPANT_A, PARTICIPANT_B] }),
    );
    const r = await store.removeParticipant('lobby-1', 'sess-1', 'tab-a');
    expect(r).toEqual({ remaining: 1 });
    const got = await store.getSession('lobby-1', 'sess-1');
    expect(got!.participants.map(p => p.userId)).toEqual(['user-2']);
  });

  it('removeParticipant returns null for unknown session', async () => {
    const store = new MemoryVideoSessionStore();
    const r = await store.removeParticipant('lobby-1', 'sess-1', 'user-1');
    expect(r).toBeNull();
  });

  it('addParticipant to an unknown session is a no-op (matches DDB if_not_exists)', async () => {
    const store = new MemoryVideoSessionStore();
    await store.addParticipant('lobby-1', 'missing', PARTICIPANT_A);
    expect(await store.getSession('lobby-1', 'missing')).toBeNull();
  });
});

describe('MemoryVideoSessionStore.endSession', () => {
  it('marks status=ended + endedAt + sets TTL', async () => {
    const store = new MemoryVideoSessionStore();
    await store.createSession(makeSession());
    // Use "now" so the stamped TTL is in the future relative to Date.now()
    // and the readOrExpire filter doesn't immediately evict the row.
    const endedAt = new Date().toISOString();
    await store.endSession('lobby-1', 'sess-1', endedAt);
    const got = await store.getSession('lobby-1', 'sess-1');
    expect(got!.status).toBe('ended');
    expect(got!.endedAt).toBe(endedAt);
    expect(got!.ttl).toBe(computeEndedSessionTtl(endedAt));
  });

  it('is a no-op on an unknown session', async () => {
    const store = new MemoryVideoSessionStore();
    await expect(
      store.endSession('lobby-1', 'sess-1', new Date().toISOString()),
    ).resolves.toBeUndefined();
  });
});

describe('MemoryVideoSessionStore.getSessionsByDocument', () => {
  it('returns sessions filtered by documentId, newest startedAt first', async () => {
    const store = new MemoryVideoSessionStore();
    await store.createSession(makeSession({ sessionId: 'a', startedAt: '2026-01-01T08:00:00Z' }));
    await store.createSession(makeSession({ sessionId: 'b', startedAt: '2026-01-01T10:00:00Z' }));
    await store.createSession(makeSession({ sessionId: 'c', startedAt: '2026-01-01T09:00:00Z' }));
    await store.createSession(makeSession({ documentId: 'other', sessionId: 'd' }));
    const rows = await store.getSessionsByDocument('lobby-1');
    expect(rows.map(r => r.sessionId)).toEqual(['b', 'c', 'a']);
  });

  it('returns an empty array when no sessions match', async () => {
    const store = new MemoryVideoSessionStore();
    expect(await store.getSessionsByDocument('nobody')).toEqual([]);
  });
});

describe('MemoryVideoSessionStore.findActiveByLobbyName', () => {
  it('returns the active session, skipping ended ones', async () => {
    const store = new MemoryVideoSessionStore();
    await store.createSession(
      makeSession({ sessionId: 'a', status: 'ended', startedAt: '2026-01-01T08:00:00Z' }),
    );
    await store.createSession(
      makeSession({ sessionId: 'b', status: 'active', startedAt: '2026-01-01T09:00:00Z' }),
    );
    const found = await store.findActiveByLobbyName('lobby-1');
    expect(found!.sessionId).toBe('b');
  });

  it('returns null when no active session exists', async () => {
    const store = new MemoryVideoSessionStore();
    await store.createSession(makeSession({ status: 'ended' }));
    expect(await store.findActiveByLobbyName('lobby-1')).toBeNull();
  });
});

describe('MemoryVideoSessionStore — TTL expiry on read', () => {
  it('returns null for a record whose TTL is in the past', async () => {
    const store = new MemoryVideoSessionStore();
    const expired = makeSession({ ttl: Math.floor(Date.now() / 1000) - 60 });
    await store.createSession(expired);
    expect(await store.getSession('lobby-1', 'sess-1')).toBeNull();
  });

  it('keeps records whose TTL is in the future', async () => {
    const store = new MemoryVideoSessionStore();
    const live = makeSession({ ttl: Math.floor(Date.now() / 1000) + 3600 });
    await store.createSession(live);
    const got = await store.getSession('lobby-1', 'sess-1');
    expect(got).not.toBeNull();
    expect(got!.ttl).toBe(live.ttl);
  });

  it('filters expired records from getSessionsByDocument', async () => {
    const store = new MemoryVideoSessionStore();
    await store.createSession(
      makeSession({ sessionId: 'expired', ttl: Math.floor(Date.now() / 1000) - 60 }),
    );
    await store.createSession(makeSession({ sessionId: 'live' }));
    const rows = await store.getSessionsByDocument('lobby-1');
    expect(rows.map(r => r.sessionId)).toEqual(['live']);
  });

  it('expired records do not match findActiveByLobbyName', async () => {
    const store = new MemoryVideoSessionStore();
    await store.createSession(
      makeSession({ ttl: Math.floor(Date.now() / 1000) - 60 }),
    );
    expect(await store.findActiveByLobbyName('lobby-1')).toBeNull();
  });
});
