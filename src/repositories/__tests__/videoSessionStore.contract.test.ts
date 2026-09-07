// Contract tests — proves both the in-memory and DDB-backed
// VideoSessionStore implementations satisfy the same behavioural
// guarantees. Covers the wave2-pa-4 acceptance criteria:
//
//   (a) session create → reload from store
//   (b) session expiry via TTL
//   (c) participant join/leave persists across "restart"
//
// For the DDB store we simulate the underlying table with a Map keyed
// off (documentId, sessionId) and a hand-rolled command dispatcher.
// This is enough fidelity to assert read-after-write + persistence-across-
// store-instances semantics without needing real DynamoDB-local in unit
// tests.

import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { MemoryVideoSessionStore } from '../MemoryVideoSessionStore';
import { VideoSessionRepository } from '../VideoSessionRepository';
import type {
  VideoSessionStore,
  VideoSessionRecord,
} from '../VideoSessionStore';

jest.mock('../../lib/ddb-table-name', () => ({
  tableName: (name: string) => `test-${name}`,
}));

// ---------------------------------------------------------------------------
// Fake DDB doc client — enough surface to back VideoSessionRepository
// ---------------------------------------------------------------------------

interface FakeRow extends VideoSessionRecord {}

function makeFakeDocClient(): {
  client: DynamoDBDocumentClient;
  table: Map<string, FakeRow>;
} {
  const table = new Map<string, FakeRow>();
  const key = (documentId: string, sessionId: string) => `${documentId}#${sessionId}`;

  // Re-use the JSON-driven structure that the AWS SDK passes — each
  // command exposes an `input` property the repo reads via TableName/Key.
  // We pattern-match on the constructor name to dispatch.
  const send = jest.fn(async (cmd: { constructor: { name: string }; input: Record<string, unknown> }) => {
    const name = cmd.constructor.name;
    const input = cmd.input as Record<string, unknown>;
    if (name === 'PutCommand') {
      const item = input.Item as FakeRow;
      table.set(key(item.documentId, item.sessionId), { ...item });
      return {};
    }
    if (name === 'GetCommand') {
      const k = input.Key as { documentId: string; sessionId: string };
      // Apply DDB-style TTL: rows with ttl in the past read as missing.
      const row = table.get(key(k.documentId, k.sessionId));
      if (row && row.ttl !== undefined && row.ttl * 1000 <= Date.now()) {
        return {};
      }
      return row ? { Item: { ...row } } : {};
    }
    if (name === 'UpdateCommand') {
      const k = input.Key as { documentId: string; sessionId: string };
      const row = table.get(key(k.documentId, k.sessionId));
      if (!row) return {};
      const vals = (input.ExpressionAttributeValues ?? {}) as Record<string, unknown>;
      // VideoSessionRepository emits a small fixed set of UpdateExpressions —
      // we apply each one explicitly. Keeping the matching string-based so
      // a future expression change surfaces here as a test miss, not a
      // silent data-shape bug.
      const expr = input.UpdateExpression as string;
      if (expr.includes('#status = :status') && expr.includes('endedAt = :endedAt')) {
        row.status = vals[':status'] as 'active' | 'ended';
        row.endedAt = vals[':endedAt'] as string;
        if (expr.includes('#ttl = :ttl')) {
          row.ttl = vals[':ttl'] as number;
        }
      } else if (expr.includes('list_append')) {
        row.participants = [
          ...(row.participants ?? []),
          ...((vals[':participant'] as VideoSessionRecord['participants']) ?? []),
        ];
      } else if (expr.includes('participants = :participants')) {
        row.participants = (vals[':participants'] as VideoSessionRecord['participants']) ?? [];
      }
      return {};
    }
    if (name === 'ScanCommand') {
      // listActiveCandidates + findByChannelArn both Scan with a
      // FilterExpression. Enough fidelity to exercise the filter the repo
      // actually emits.
      const vals = (input.ExpressionAttributeValues ?? {}) as Record<string, unknown>;
      const filter = (input.FilterExpression ?? '') as string;
      const items: FakeRow[] = [];
      for (const row of table.values()) {
        if (row.ttl !== undefined && row.ttl * 1000 <= Date.now()) continue;
        if (filter.includes('#s = :active') && row.status !== vals[':active']) continue;
        if (filter.includes('channelArn = :arn') && row.channelArn !== vals[':arn']) continue;
        items.push({ ...row });
      }
      return { Items: items };
    }
    if (name === 'QueryCommand') {
      const vals = (input.ExpressionAttributeValues ?? {}) as Record<string, unknown>;
      const docId = vals[':docId'] as string;
      const items: FakeRow[] = [];
      for (const [k2, row] of table.entries()) {
        if (!k2.startsWith(`${docId}#`)) continue;
        // Same TTL filter as GetCommand.
        if (row.ttl !== undefined && row.ttl * 1000 <= Date.now()) continue;
        items.push({ ...row });
      }
      return { Items: items };
    }
    return {};
  });
  return { client: { send } as unknown as DynamoDBDocumentClient, table };
}

// ---------------------------------------------------------------------------
// Test matrix — both stores satisfy the same contract
// ---------------------------------------------------------------------------

interface StoreFactory {
  name: string;
  makeStore: () => VideoSessionStore;
  /** Returns a fresh store sharing the same backing state — "simulated restart". */
  makeRestart: (existing: VideoSessionStore) => VideoSessionStore;
}

function withMemoryStore(): StoreFactory {
  let mem: MemoryVideoSessionStore | null = null;
  return {
    name: 'MemoryVideoSessionStore',
    makeStore: () => {
      mem = new MemoryVideoSessionStore();
      return mem;
    },
    // For the in-memory store, "restart" means new instance — state is lost
    // by design. We assert THAT in a dedicated test below; for the other
    // contract assertions we re-use the same instance to express "persistence
    // semantics within a single process lifetime".
    makeRestart: existing => existing,
  };
}

function withDdbStore(): StoreFactory {
  let backing: ReturnType<typeof makeFakeDocClient> | null = null;
  return {
    name: 'VideoSessionRepository (DDB)',
    makeStore: () => {
      backing = makeFakeDocClient();
      return new VideoSessionRepository(backing.client);
    },
    // The DDB-backed store survives "restart" — i.e. we can spin up a NEW
    // repository instance pointed at the same underlying table and read
    // everything back.
    makeRestart: () => new VideoSessionRepository(backing!.client),
  };
}

const SAMPLE: VideoSessionRecord = {
  documentId: 'lobby-1',
  sessionId: 'sess-1',
  channelArn: 'arn:aws:ivs:us-east-1:123:channel/abc',
  streamKey: 'sk-1',
  status: 'active',
  startedAt: '2026-01-01T10:00:00Z',
  startedBy: 'user-1',
  participants: [
    { userId: 'user-1', participantId: 'user-1-a', displayName: 'Alice', joinedAt: '2026-01-01T10:00:00Z' },
  ],
};

describe.each([withMemoryStore(), withDdbStore()])('VideoSessionStore contract — %s', factory => {
  describe(factory.name, () => {
    // (a) session create → reload
    it('persists a session that can be reloaded by composite key', async () => {
      const store = factory.makeStore();
      await store.createSession(SAMPLE);
      const got = await store.getSession(SAMPLE.documentId, SAMPLE.sessionId);
      expect(got).toMatchObject({
        documentId: SAMPLE.documentId,
        sessionId: SAMPLE.sessionId,
        status: 'active',
      });
    });

    // (b) TTL — endSession stamps ttl, expired records read as null
    it('expires records past their TTL window on read', async () => {
      const store = factory.makeStore();
      await store.createSession({
        ...SAMPLE,
        ttl: Math.floor(Date.now() / 1000) - 60, // already expired
      });
      expect(await store.getSession(SAMPLE.documentId, SAMPLE.sessionId)).toBeNull();
    });

    // (c) participant join/leave persists across "restart"
    it('participant adds + removes persist across a simulated restart', async () => {
      const store = factory.makeStore();
      await store.createSession({ ...SAMPLE, participants: [] });
      // participantId is the removal key since the per-tab dedup change
      // (ddf3554) — removeParticipant() filters on it, not userId.
      await store.addParticipant(SAMPLE.documentId, SAMPLE.sessionId, {
        userId: 'u1', participantId: 'p1', displayName: 'A', joinedAt: '2026-01-01T10:00Z',
      });
      await store.addParticipant(SAMPLE.documentId, SAMPLE.sessionId, {
        userId: 'u2', participantId: 'p2', displayName: 'B', joinedAt: '2026-01-01T10:01Z',
      });

      // Simulate process restart — new store instance pointing at the same
      // backing state. For the memory store this returns the same instance
      // (the test that proves restart=loss for memory lives separately).
      const restarted = factory.makeRestart(store);
      const after = await restarted.getSession(SAMPLE.documentId, SAMPLE.sessionId);
      expect(after!.participants.map(p => p.userId)).toEqual(['u1', 'u2']);

      const splice = await restarted.removeParticipant(
        SAMPLE.documentId, SAMPLE.sessionId, 'p1',
      );
      expect(splice).toEqual({ remaining: 1 });

      const restarted2 = factory.makeRestart(restarted);
      const final = await restarted2.getSession(SAMPLE.documentId, SAMPLE.sessionId);
      expect(final!.participants.map(p => p.userId)).toEqual(['u2']);
    });

    it('endSession marks ended + stamps a future TTL', async () => {
      const store = factory.makeStore();
      await store.createSession(SAMPLE);
      // Use "now" as the basis so the stamped TTL is in the future relative
      // to Date.now() — otherwise the row reads back as null thanks to
      // the TTL filter in both stores.
      const endedAt = new Date().toISOString();
      await store.endSession(SAMPLE.documentId, SAMPLE.sessionId, endedAt);
      const got = await store.getSession(SAMPLE.documentId, SAMPLE.sessionId);
      expect(got!.status).toBe('ended');
      expect(got!.endedAt).toBe(endedAt);
      expect(got!.ttl).toBeGreaterThan(Math.floor(Date.parse(endedAt) / 1000));
    });

    // ---- join/leave symmetry + double-join idempotence -------------------
    //
    // The leak these guard: LVS mints participantId as
    // `${userId}-${Date.now().toString(36)}`, fresh on EVERY join, so a
    // dedupe keyed on it can never fire and a client that joins twice
    // appends twice. Nothing removes the extra, so the ref-count never
    // reaches zero and the row stays 'active' forever.

    it('join then leave nets to zero — the row its join created is the row it removes', async () => {
      const store = factory.makeStore();
      await store.createSession({ ...SAMPLE, participants: [] });

      const added = await store.addOrReplaceParticipant(SAMPLE.documentId, SAMPLE.sessionId, {
        userId: 'u1', participantId: 'u1-aaa', displayName: 'A', joinedAt: '2026-01-01T10:00Z',
      });
      expect(added).toEqual({ displaced: [], remaining: 1 });

      const left = await store.removeParticipant(SAMPLE.documentId, SAMPLE.sessionId, 'u1-aaa');
      expect(left).toEqual({ remaining: 0 });
    });

    it('a second join by the same client REPLACES the first instead of stacking', async () => {
      const store = factory.makeStore();
      await store.createSession({ ...SAMPLE, participants: [] });

      // Entering a room does exactly this: the room bridge joins, then the
      // call widget joins the same lobby as the same user. Two upstream
      // participantIds, one human, one hang-up.
      await store.addOrReplaceParticipant(SAMPLE.documentId, SAMPLE.sessionId, {
        userId: 'u1', participantId: 'u1-bridge', displayName: 'A', joinedAt: '2026-01-01T10:00Z',
      });
      const second = await store.addOrReplaceParticipant(SAMPLE.documentId, SAMPLE.sessionId, {
        userId: 'u1', participantId: 'u1-widget', displayName: 'A', joinedAt: '2026-01-01T10:00Z',
      });

      expect(second!.remaining).toBe(1);
      // The displaced entry is handed back so the caller can release it in
      // LVS too — its own participant set is what /end counts.
      expect(second!.displaced.map((p) => p.participantId)).toEqual(['u1-bridge']);

      const after = await store.getSession(SAMPLE.documentId, SAMPLE.sessionId);
      expect(after!.participants.map((p) => p.participantId)).toEqual(['u1-widget']);

      // And the single hang-up the client actually sends empties the roster.
      const left = await store.removeParticipant(SAMPLE.documentId, SAMPLE.sessionId, 'u1-widget');
      expect(left).toEqual({ remaining: 0 });
    });

    it('two clients that identify themselves keep separate ref-count slots', async () => {
      const store = factory.makeStore();
      await store.createSession({ ...SAMPLE, participants: [] });

      await store.addOrReplaceParticipant(SAMPLE.documentId, SAMPLE.sessionId, {
        userId: 'u1', participantId: 'u1-t1', clientId: 'tab-1', displayName: 'A', joinedAt: '2026-01-01T10:00Z',
      });
      const t2 = await store.addOrReplaceParticipant(SAMPLE.documentId, SAMPLE.sessionId, {
        userId: 'u1', participantId: 'u1-t2', clientId: 'tab-2', displayName: 'A', joinedAt: '2026-01-01T10:01Z',
      });

      // Same user, two tabs — nothing displaced, two slots.
      expect(t2!.displaced).toEqual([]);
      expect(t2!.remaining).toBe(2);

      // Tab 1 rejoining (a reconnect) replaces only its own slot.
      const t1again = await store.addOrReplaceParticipant(SAMPLE.documentId, SAMPLE.sessionId, {
        userId: 'u1', participantId: 'u1-t1b', clientId: 'tab-1', displayName: 'A', joinedAt: '2026-01-01T10:02Z',
      });
      expect(t1again!.displaced.map((p) => p.participantId)).toEqual(['u1-t1']);
      expect(t1again!.remaining).toBe(2);

      // Tab 2 leaving does NOT end the session out from under tab 1.
      const left = await store.removeParticipant(SAMPLE.documentId, SAMPLE.sessionId, 'u1-t2');
      expect(left).toEqual({ remaining: 1 });
    });

    it('different users are never collapsed into one another', async () => {
      const store = factory.makeStore();
      await store.createSession({ ...SAMPLE, participants: [] });
      await store.addOrReplaceParticipant(SAMPLE.documentId, SAMPLE.sessionId, {
        userId: 'u1', participantId: 'u1-a', displayName: 'A', joinedAt: '2026-01-01T10:00Z',
      });
      const second = await store.addOrReplaceParticipant(SAMPLE.documentId, SAMPLE.sessionId, {
        userId: 'u2', participantId: 'u2-a', displayName: 'B', joinedAt: '2026-01-01T10:01Z',
      });
      expect(second).toEqual({ displaced: [], remaining: 2 });
    });

    it('returns null for a session that does not exist', async () => {
      const store = factory.makeStore();
      expect(
        await store.addOrReplaceParticipant('nope', 'nope', {
          userId: 'u1', participantId: 'p', displayName: 'A', joinedAt: '2026-01-01T10:00Z',
        }),
      ).toBeNull();
    });

    // The reaper needs EVERY active row, not the newest per lobby: rows
    // behind the newest are invisible to every read path, so collapsing them
    // away is how one room ends up holding four 'active' rows forever.
    it('listActiveCandidates collapses per lobby by default and does not with perLobby:false', async () => {
      const store = factory.makeStore();
      await store.createSession({ ...SAMPLE, sessionId: 'old-1', startedAt: '2026-01-01T09:00:00Z' });
      await store.createSession({ ...SAMPLE, sessionId: 'new-1', startedAt: '2026-01-01T11:00:00Z' });

      const collapsed = await store.listActiveCandidates();
      expect(collapsed.map((s) => s.sessionId)).toEqual(['new-1']);

      const all = await store.listActiveCandidates(60, { perLobby: false });
      expect(all.map((s) => s.sessionId)).toEqual(['new-1', 'old-1']);
    });

    it('findActiveByLobbyName skips ended sessions', async () => {
      const store = factory.makeStore();
      await store.createSession({ ...SAMPLE, sessionId: 'ended-1', status: 'ended' });
      await store.createSession({ ...SAMPLE, sessionId: 'live-1', status: 'active' });
      const found = await store.findActiveByLobbyName(SAMPLE.documentId);
      expect(found!.sessionId).toBe('live-1');
    });
  });
});

// The memory store's distinguishing property: a TRUE restart (new instance,
// no shared backing state) loses everything. This is the documented
// trade-off; we encode it as a test so any future change to the contract
// is caught explicitly.
describe('MemoryVideoSessionStore — true-restart semantics', () => {
  it('a fresh instance has no shared state with a previous instance', async () => {
    const first = new MemoryVideoSessionStore();
    await first.createSession(SAMPLE);
    const second = new MemoryVideoSessionStore();
    expect(await second.getSession(SAMPLE.documentId, SAMPLE.sessionId)).toBeNull();
  });
});
