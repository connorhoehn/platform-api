// platform-api/src/repositories/__tests__/CallRecordingRepository.test.ts
//
// Owner fan-out regression: when a recording arrives with NO participants[]
// (e.g. caller dropped before any /join minted a participant token), the
// fan-out must still write ONE row keyed by `startedBy` so the recording
// owner sees it in their "Past Calls" list. Without this, the row is
// stranded in the primary table and invisible to user-scoped queries.
//
// Also covers W6: listByLobbyName — room-scoped recording filter used by
// the RoomAdminPage "View recordings" link. v1 is a Scan + FilterExpression
// since lobbyName has no GSI yet; we cap at 100 rows per page.

import {
  DynamoDBDocumentClient,
  PutCommand,
  ScanCommand,
  DeleteCommand,
} from '@aws-sdk/lib-dynamodb';
import { CallRecordingRepository } from '../CallRecordingRepository';

jest.mock('../../lib/ddb-table-name', () => ({
  tableName: (name: string) => `test-${name}`,
}));

const mockSend = jest.fn();
const docClient = { send: mockSend } as unknown as DynamoDBDocumentClient;

beforeEach(() => {
  mockSend.mockReset();
  mockSend.mockResolvedValue({});
});

describe('CallRecordingRepository.putRecording — owner fan-out', () => {
  test('empty participants[] but startedBy=u-hank writes one fan-out row for u-hank', async () => {
    const repo = new CallRecordingRepository(docClient);

    await repo.putRecording({
      callId: 'c1',
      recordingId: 'r1',
      channelArn: 'arn:x',
      lobbyName: 'lobby-x',
      sessionId: 'sess-1',
      startedAt: '2026-05-23T00:00:00.000Z',
      endedAt: '2026-05-23T00:01:00.000Z',
      durationMs: 60_000,
      totalBytes: 1024,
      segments: [],
      destination: 'local',
      participants: [],            // ← empty on purpose
      startedBy: 'u-hank',
    });

    // Find the PutCommand sent at the participants table.
    const participantPuts = mockSend.mock.calls
      .map((c) => c[0])
      .filter(
        (cmd) =>
          cmd instanceof PutCommand &&
          cmd.input.TableName === 'test-video-call-recording-participants',
      );

    expect(participantPuts).toHaveLength(1);
    const item = (participantPuts[0] as PutCommand).input.Item!;
    expect(item.userId).toBe('u-hank');
    expect(item.callId).toBe('c1');
    expect(item.recordingId).toBe('r1');
    expect(item.channelArn).toBe('arn:x');
    expect(item.startedAtRecordingId).toBe('2026-05-23T00:00:00.000Z#r1');
  });

  // ---------------------------------------------------------------------------
  // DM-vs-room parity (Problem 1). The fan-out path must treat a `dm:…` lobby
  // the same as `room:…` — historically the route layer dropped DM rows when
  // session lookup failed (no startedBy → no fan-out → invisible row). Today
  // we expect the repo to behave identically for both lobby shapes when the
  // input is identical.
  // ---------------------------------------------------------------------------
  test('DM lobby fan-out parity: dm:<a>:<b> writes the same shape as room:<slug>', async () => {
    const repo = new CallRecordingRepository(docClient);

    // Snapshot the shape under a room: lobby first…
    await repo.putRecording({
      callId: 'c-room',
      recordingId: 'r-room',
      channelArn: 'arn:room',
      lobbyName: 'room:lounge',
      sessionId: 's-room',
      startedAt: '2026-05-23T10:00:00.000Z',
      endedAt: '2026-05-23T10:05:00.000Z',
      durationMs: 300_000,
      totalBytes: 4096,
      segments: [],
      destination: 'local',
      participants: [
        { userId: 'u-hank' },
        { userId: 'u-carol' },
      ],
      startedBy: 'u-hank',
    });
    const roomFanouts = mockSend.mock.calls
      .map((c) => c[0])
      .filter(
        (cmd) =>
          cmd instanceof PutCommand &&
          cmd.input.TableName === 'test-video-call-recording-participants',
      ) as PutCommand[];
    expect(roomFanouts).toHaveLength(2);

    mockSend.mockReset();
    mockSend.mockResolvedValue({});

    // …now under a dm: lobby with the same participants. The fan-out
    // MUST land the same number of rows (one per userId) regardless of
    // lobby type. If this regresses, every DM recording becomes orphaned
    // again (the original bug).
    await repo.putRecording({
      callId: 'c-dm',
      recordingId: 'r-dm',
      channelArn: 'arn:dm',
      lobbyName: 'dm:u-hank:u-carol',
      sessionId: 's-dm',
      startedAt: '2026-05-23T11:00:00.000Z',
      endedAt: '2026-05-23T11:05:00.000Z',
      durationMs: 300_000,
      totalBytes: 4096,
      segments: [],
      destination: 'local',
      participants: [
        { userId: 'u-hank' },
        { userId: 'u-carol' },
      ],
      startedBy: 'u-hank',
    });
    const dmFanouts = mockSend.mock.calls
      .map((c) => c[0])
      .filter(
        (cmd) =>
          cmd instanceof PutCommand &&
          cmd.input.TableName === 'test-video-call-recording-participants',
      ) as PutCommand[];
    expect(dmFanouts).toHaveLength(2);

    // Each DM fan-out row carries the lobbyName as-is (FE uses it to
    // render the DM badge).
    const lobbyNames = dmFanouts.map((p) => p.input.Item!.lobbyName);
    expect(lobbyNames).toEqual(['dm:u-hank:u-carol', 'dm:u-hank:u-carol']);

    const userIds = dmFanouts.map((p) => p.input.Item!.userId).sort();
    expect(userIds).toEqual(['u-carol', 'u-hank']);
  });

  // R2 — composedUrl is denormalized onto the fan-out row so the list view
  // can pick its playback target without joining back to the primary table.
  test('composedUrl is denormalized onto the fan-out row', async () => {
    const repo = new CallRecordingRepository(docClient);
    await repo.putRecording({
      callId: 'c3',
      recordingId: 'r3',
      channelArn: 'arn:z',
      lobbyName: 'room:devs',
      startedAt: '2026-05-23T02:00:00.000Z',
      endedAt: '2026-05-23T02:05:00.000Z',
      durationMs: 300_000,
      totalBytes: 8192,
      segments: [],
      destination: 'local',
      participants: [{ userId: 'u-a' }, { userId: 'u-b' }],
      startedBy: 'u-a',
      composedUrl: 'https://storage.test/composed/r3.mp4',
      tracks: [
        { participantId: 'p1', userId: 'u-a', key: 'k1', url: 'https://t/1.mp4', size: 100 },
        { participantId: 'p2', userId: 'u-b', key: 'k2', url: 'https://t/2.mp4', size: 100 },
      ],
    });

    const fanouts = mockSend.mock.calls
      .map((c) => c[0])
      .filter(
        (cmd) =>
          cmd instanceof PutCommand &&
          cmd.input.TableName === 'test-video-call-recording-participants',
      ) as PutCommand[];
    expect(fanouts).toHaveLength(2);
    for (const f of fanouts) {
      expect(f.input.Item!.composedUrl).toBe('https://storage.test/composed/r3.mp4');
      expect(f.input.Item!.participantCount).toBe(2);
    }
  });

  // Screenshot bug (2026-08): the fan-out denorm counted participants[] —
  // the live roster at webhook time — so a 3-leg composed recording whose
  // members had already hung up rendered "1 participant". The webhook now
  // derives an explicit leg-truth participantCount; the denorm must prefer
  // it over participants.length.
  test('explicit participantCount (leg truth) wins over participants.length on the fan-out row', async () => {
    const repo = new CallRecordingRepository(docClient);
    await repo.putRecording({
      callId: 'c4',
      recordingId: 'r4',
      channelArn: 'arn:gh',
      lobbyName: 'global-hangout',
      startedAt: '2026-08-24T02:00:00.000Z',
      endedAt: '2026-08-24T02:05:00.000Z',
      durationMs: 300_000,
      totalBytes: 8192,
      segments: [],
      destination: 'local',
      // Only one roster survivor, but three recorded legs.
      participants: [{ userId: 'u-alice' }],
      participantCount: 3,
      startedBy: 'u-alice',
    });

    const fanouts = mockSend.mock.calls
      .map((c) => c[0])
      .filter(
        (cmd) =>
          cmd instanceof PutCommand &&
          cmd.input.TableName === 'test-video-call-recording-participants' &&
          cmd.input.Item!.recordingId === 'r4',
      ) as PutCommand[];
    expect(fanouts).toHaveLength(1);
    expect(fanouts[0]!.input.Item!.participantCount).toBe(3);
  });

  test('empty participants[] AND no startedBy → no fan-out rows written', async () => {
    const repo = new CallRecordingRepository(docClient);

    await repo.putRecording({
      callId: 'c2',
      recordingId: 'r2',
      channelArn: 'arn:y',
      lobbyName: 'lobby-y',
      startedAt: '2026-05-23T01:00:00.000Z',
      endedAt: '2026-05-23T01:01:00.000Z',
      durationMs: 60_000,
      totalBytes: 0,
      segments: [],
      destination: 'local',
      participants: [],
      // no startedBy
    });

    const participantPuts = mockSend.mock.calls
      .map((c) => c[0])
      .filter(
        (cmd) =>
          cmd instanceof PutCommand &&
          cmd.input.TableName === 'test-video-call-recording-participants',
      );

    expect(participantPuts).toHaveLength(0);

    // Primary table write still happened.
    const primaryPuts = mockSend.mock.calls
      .map((c) => c[0])
      .filter(
        (cmd) =>
          cmd instanceof PutCommand &&
          cmd.input.TableName === 'test-video-call-recordings',
      );
    expect(primaryPuts).toHaveLength(1);
  });
});

// ---------------------------------------------------------------------------
// W6 — listByLobbyName: room-scoped recording filter (RoomAdminPage)
// ---------------------------------------------------------------------------

describe('CallRecordingRepository.listByLobbyName', () => {
  test('issues a Scan filtered by lobbyName with the configured cap', async () => {
    const repo = new CallRecordingRepository(docClient);
    mockSend.mockResolvedValueOnce({
      Items: [
        {
          callId: 'c1',
          recordingId: 'r1',
          startedAt: '2026-05-23T00:00:00.000Z',
          endedAt: '2026-05-23T00:05:00.000Z',
          durationMs: 300_000,
          channelArn: 'arn:x',
          lobbyName: 'room:lounge',
          thumbnailUrl: 'http://thumb/1.png',
        },
        {
          callId: 'c2',
          recordingId: 'r2',
          startedAt: '2026-05-23T01:00:00.000Z',
          endedAt: '2026-05-23T01:10:00.000Z',
          durationMs: 600_000,
          channelArn: 'arn:y',
          lobbyName: 'room:lounge',
        },
      ],
      LastEvaluatedKey: { callId: 'c2', recordingId: 'r2' },
    });

    const out = await repo.listByLobbyName('room:lounge', 50);

    const scans = mockSend.mock.calls
      .map((c) => c[0])
      .filter((cmd) => cmd instanceof ScanCommand) as ScanCommand[];
    expect(scans).toHaveLength(1);
    expect(scans[0].input.TableName).toBe('test-video-call-recordings');
    expect(scans[0].input.FilterExpression).toBe('lobbyName = :l');
    expect(scans[0].input.ExpressionAttributeValues).toEqual({ ':l': 'room:lounge' });
    expect(scans[0].input.Limit).toBe(50);

    expect(out.items).toHaveLength(2);
    expect(out.items[0]).toEqual({
      callId: 'c1',
      recordingId: 'r1',
      startedAt: '2026-05-23T00:00:00.000Z',
      endedAt: '2026-05-23T00:05:00.000Z',
      durationMs: 300_000,
      channelArn: 'arn:x',
      lobbyName: 'room:lounge',
      thumbnailUrl: 'http://thumb/1.png',
      // Shape grew: the row now also carries the raw session lifetime and
      // the re-derived people list. Both are `undefined` for this fixture
      // (no tracks, no roster) — asserted explicitly rather than relaxed to
      // toMatchObject, so an accidental `participantCount: 0` or a leaked
      // extra field still fails here.
      composedUrl: undefined,
      sessionDurationMs: undefined,
      participantCount: undefined,
      participants: undefined,
    });
    expect(out.cursor).toBeDefined();
    const decoded = JSON.parse(Buffer.from(out.cursor!, 'base64').toString('utf8'));
    expect(decoded).toEqual({ callId: 'c2', recordingId: 'r2' });
  });

  test('caps the Limit at 100 even when the caller asks for more', async () => {
    const repo = new CallRecordingRepository(docClient);
    mockSend.mockResolvedValueOnce({ Items: [] });

    await repo.listByLobbyName('room:lounge', 500);

    const scan = mockSend.mock.calls
      .map((c) => c[0])
      .find((cmd) => cmd instanceof ScanCommand) as ScanCommand | undefined;
    expect(scan?.input.Limit).toBe(100);
  });

  test('decodes the cursor back into ExclusiveStartKey', async () => {
    const repo = new CallRecordingRepository(docClient);
    mockSend.mockResolvedValueOnce({ Items: [] });

    const cursor = Buffer.from(
      JSON.stringify({ callId: 'cX', recordingId: 'rX' }),
      'utf8',
    ).toString('base64');
    await repo.listByLobbyName('room:lounge', 25, cursor);

    const scan = mockSend.mock.calls
      .map((c) => c[0])
      .find((cmd) => cmd instanceof ScanCommand) as ScanCommand | undefined;
    expect(scan?.input.ExclusiveStartKey).toEqual({ callId: 'cX', recordingId: 'rX' });
  });

  test('returns empty cursor when DDB has no LastEvaluatedKey', async () => {
    const repo = new CallRecordingRepository(docClient);
    mockSend.mockResolvedValueOnce({ Items: [] });

    const out = await repo.listByLobbyName('room:lounge', 50);
    expect(out.cursor).toBeUndefined();
  });

  test('silently ignores an unparseable cursor (starts from top)', async () => {
    const repo = new CallRecordingRepository(docClient);
    mockSend.mockResolvedValueOnce({ Items: [] });

    await repo.listByLobbyName('room:lounge', 25, 'not-base64-json');

    const scan = mockSend.mock.calls
      .map((c) => c[0])
      .find((cmd) => cmd instanceof ScanCommand) as ScanCommand | undefined;
    expect(scan?.input.ExclusiveStartKey).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// FINDING 2 — the person who STARTS a call never got their own recording.
//
// The fan-out set used to be "participants[], or startedBy ONLY IF
// participants[] is empty". That reads like a sensible fallback and is in
// fact the bug: `participants[]` is the SFU's LIVE roster at
// recording-complete time, so on a real DM the caller has already hung up
// and only the callee is in it — non-empty, so the startedBy fallback never
// fired, so the caller got no index row and the recording was permanently
// invisible to them in GET /api/recordings.
//
// Measured on live data 2026-08-25: 37 of 200 recordings (18%) had no
// fan-out row for their own startedBy. ALL 37 had a non-empty
// participants[].
// ---------------------------------------------------------------------------
describe('CallRecordingRepository.putRecording — initiator fan-out (FINDING 2)', () => {
  /** The real 22s hank→bob DM: good media, roster held the callee only. */
  const hankRingsBob = {
    callId: 'call-dm-1',
    recordingId: '2026-08-26T02-23-24-593Z-0d14eca7',
    channelArn: 'arn:local:ivs:channel/abc',
    lobbyName: 'dm:dev-bob:dev-hank',
    sessionId: 'sess-dm-1',
    startedAt: '2026-08-26T02:23:24.679Z',
    endedAt: '2026-08-26T02:23:42.558Z',
    durationMs: 21_800,
    totalBytes: 4096,
    segments: [],
    destination: 'local' as const,
    // NON-EMPTY, and missing the caller. This is the shape the old
    // empty-only fallback could not see.
    participants: [{ userId: 'dev-bob', displayName: 'Bob Martinez' }],
    startedBy: 'dev-hank',
  };

  function fanoutRows(): PutCommand[] {
    return mockSend.mock.calls
      .map((c) => c[0])
      .filter(
        (cmd) =>
          cmd instanceof PutCommand &&
          cmd.input.TableName === 'test-video-call-recording-participants',
      ) as PutCommand[];
  }

  test('non-empty participants[] WITHOUT startedBy still fans out to the initiator', async () => {
    const repo = new CallRecordingRepository(docClient);
    await repo.putRecording(hankRingsBob);

    const userIds = fanoutRows().map((p) => p.input.Item!.userId).sort();
    expect(userIds).toEqual(['dev-bob', 'dev-hank']);
  });

  test('the initiator row is a complete row, not a stub', async () => {
    const repo = new CallRecordingRepository(docClient);
    await repo.putRecording(hankRingsBob);

    const hank = fanoutRows().find((p) => p.input.Item!.userId === 'dev-hank');
    expect(hank).toBeDefined();
    const item = hank!.input.Item!;
    expect(item.recordingId).toBe(hankRingsBob.recordingId);
    expect(item.callId).toBe('call-dm-1');
    expect(item.lobbyName).toBe('dm:dev-bob:dev-hank');
    expect(item.startedAtRecordingId).toBe(
      `${hankRingsBob.startedAt}#${hankRingsBob.recordingId}`,
    );
  });

  test('startedBy already in participants[] does NOT produce a duplicate row', async () => {
    const repo = new CallRecordingRepository(docClient);
    await repo.putRecording({
      ...hankRingsBob,
      participants: [
        { userId: 'dev-bob' },
        { userId: 'dev-hank' },
      ],
    });
    expect(fanoutRows()).toHaveLength(2);
  });

  test('delete removes the initiator row it wrote — no stranded index row', async () => {
    const repo = new CallRecordingRepository(docClient);
    // getRecording → the stored row; the deletes that follow are what we assert on.
    mockSend.mockResolvedValueOnce({ Item: hankRingsBob });

    await repo.deleteRecording('call-dm-1', hankRingsBob.recordingId);

    const deletedUserIds = mockSend.mock.calls
      .map((c) => c[0])
      .filter(
        (cmd) =>
          cmd instanceof DeleteCommand &&
          cmd.input.TableName === 'test-video-call-recording-participants',
      )
      .map((cmd) => (cmd as DeleteCommand).input.Key!.userId)
      .sort();
    expect(deletedUserIds).toEqual(['dev-bob', 'dev-hank']);
  });
});

// ---------------------------------------------------------------------------
// FINDING 1 — durationMs on the read path.
//
// The webhook now stores media duration, but 200 rows already exist holding
// the session lifetime. The backfill rewrites them; this guard means a row
// the backfill missed (or one written by a pod that hasn't rolled) still
// can't re-surface the lie.
// ---------------------------------------------------------------------------
describe('CallRecordingRepository — duration normalization on read (FINDING 1)', () => {
  test('a legacy row (no sessionDurationMs) is re-derived from its tracks', async () => {
    const repo = new CallRecordingRepository(docClient);
    mockSend.mockResolvedValueOnce({
      Item: {
        callId: 'c1',
        recordingId: 'r1',
        durationMs: 18_736, // session lifetime — the lie
        tracks: [
          { legId: 'leg-001', startedAtMs: 3, endedAtMs: 8_671, durationMs: 8_668 },
          { legId: 'leg-002', startedAtMs: 21, endedAtMs: 8_640, durationMs: 8_619 },
        ],
      },
    });

    const rec = await repo.getRecording('c1', 'r1');
    expect(rec!.durationMs).toBe(8_668);
    // The raw number is preserved, not discarded.
    expect(rec!.sessionDurationMs).toBe(18_736);
  });

  test('an already-corrected row is passed through untouched', async () => {
    const repo = new CallRecordingRepository(docClient);
    mockSend.mockResolvedValueOnce({
      Item: {
        callId: 'c1',
        recordingId: 'r1',
        durationMs: 8_668,
        sessionDurationMs: 18_736,
        tracks: [{ startedAtMs: 3, endedAtMs: 8_671 }],
      },
    });

    const rec = await repo.getRecording('c1', 'r1');
    expect(rec!.durationMs).toBe(8_668);
    expect(rec!.sessionDurationMs).toBe(18_736);
  });

  test('a row with no tracks keeps its stored duration (nothing better to say)', async () => {
    const repo = new CallRecordingRepository(docClient);
    mockSend.mockResolvedValueOnce({
      Item: { callId: 'c1', recordingId: 'r1', durationMs: 60_000, tracks: [] },
    });

    const rec = await repo.getRecording('c1', 'r1');
    expect(rec!.durationMs).toBe(60_000);
  });
});

// ---------------------------------------------------------------------------
// FINDING 3 — the lobby-scoped list re-derives people from the primary row.
// ---------------------------------------------------------------------------
describe('CallRecordingRepository.listByLobbyName — people, not sessions (FINDING 3)', () => {
  test('a stored participantCount of 7 for a 2-person DM is corrected on read', async () => {
    const repo = new CallRecordingRepository(docClient);
    mockSend.mockResolvedValueOnce({
      Items: [
        {
          callId: 'c1',
          recordingId: 'r1',
          startedAt: '2026-08-24T22:07:23.349Z',
          endedAt: '2026-08-24T22:07:45.000Z',
          durationMs: 21_000,
          sessionDurationMs: 21_000,
          channelArn: 'arn:x',
          lobbyName: 'dm:dev-bob:dev-hank',
          startedBy: 'dev-hank',
          participantCount: 7,
          participants: [
            { userId: 'dev-hank', displayName: 'Hank Hill', joinedAt: '2026-08-24T22:07:20.000Z' },
            { userId: 'dev-bob', displayName: 'Bob Martinez', joinedAt: '2026-08-24T22:07:21.000Z' },
            { userId: 'dev-hank', joinedAt: '2026-08-24T22:07:22.000Z' },
            { userId: 'dev-bob', joinedAt: '2026-08-24T22:07:22.500Z' },
            { userId: 'dev-hank', joinedAt: '2026-08-24T22:07:23.000Z' },
            { userId: 'dev-bob', joinedAt: '2026-08-24T22:07:23.100Z' },
            { userId: 'dev-hank', joinedAt: '2026-08-24T22:07:23.200Z' },
          ],
          tracks: [{ legId: 'leg-001' }, { legId: 'leg-002' }],
        },
      ],
    });

    const { items } = await repo.listByLobbyName('dm:dev-bob:dev-hank');
    expect(items[0]!.participantCount).toBe(2);
    expect(items[0]!.participants!.map((p) => p.displayName).sort()).toEqual([
      'Bob Martinez', 'Hank Hill',
    ]);
  });
});
