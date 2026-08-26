// platform-api/src/routes/__tests__/lvsWebhooks.test.ts
//
// Regression coverage for the recording payload normalization path
// when LVS POSTs the *bare* envelope shape (NOT the EventBridge
// variant). Today's bug: the bare envelope's `channelArn` (root) and
// `data.recordingId` weren't surfaced consistently, leaving the
// downstream callRecordingRepo.putRecording call missing the arn or
// recordingId fields.
//
// We mock callRecordingRepo + videoSessionRepo so the test isolates
// the route's parsing + HMAC verification, not the persistence path.

import express from 'express';
import http from 'http';
import { AddressInfo } from 'net';
import { createHmac } from 'crypto';

// Lock the HMAC secret BEFORE importing the route — the module reads
// process.env.LVS_WEBHOOK_SECRET at load time.
process.env.LVS_WEBHOOK_SECRET = 'test-secret';

// Mock the repositories index so we can spy on callRecordingRepo.putRecording
// and stub videoSessionRepo to avoid touching DDB.
const putRecordingMock = jest.fn().mockResolvedValue(undefined);
const getSessionMock = jest.fn().mockResolvedValue(null);
const findByChannelArnMock = jest.fn().mockResolvedValue([]);
jest.mock('../../repositories', () => ({
  callRecordingRepo: { putRecording: putRecordingMock },
  videoSessionRepo: {
    getSession: getSessionMock,
    findByChannelArn: findByChannelArnMock,
  },
}));

// Stub redis-client to return null — webhook then proceeds with
// limited metadata (no arn-index hit) which is what we want for this test.
jest.mock('../../lib/redis-client', () => ({
  getRedisClient: jest.fn().mockResolvedValue(null),
}));

// Stub aws-clients ddbClient so resolveCallIdByLobby doesn't hit the network.
jest.mock('../../lib/aws-clients', () => ({
  ddbClient: { send: jest.fn().mockResolvedValue({ Items: [] }) },
  docClient: { send: jest.fn() },
}));

// Stub tableName so we don't depend on env config.
jest.mock('../../lib/ddb-table-name', () => ({
  tableName: (name: string) => `test-${name}`,
}));

// NOTE: import AFTER the mocks above so jest swaps the deps.
import { lvsWebhooksRouter } from '../lvsWebhooks';

// ---------------------------------------------------------------------------
// Raw http helper (avoids adding supertest dep)
// ---------------------------------------------------------------------------
function rawPost(
  server: http.Server,
  path: string,
  body: Buffer,
  headers: Record<string, string> = {},
): Promise<{ status: number; body: string }> {
  const { port } = server.address() as AddressInfo;
  return new Promise((resolve, reject) => {
    const req = http.request(
      {
        host: '127.0.0.1',
        port,
        method: 'POST',
        path,
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': String(body.length),
          ...headers,
        },
      },
      (res) => {
        let buf = '';
        res.on('data', (c: Buffer) => { buf += c.toString('utf-8'); });
        res.on('end', () => resolve({ status: res.statusCode ?? 0, body: buf }));
      },
    );
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

function signBody(body: Buffer, secret = 'test-secret'): string {
  return 'sha256=' + createHmac('sha256', secret).update(body).digest('hex');
}

/** Headers for a happy-path signed delivery with a fresh replay-window timestamp (hub#1260). */
function signedHeaders(body: Buffer, secret = 'test-secret'): Record<string, string> {
  return {
    'X-LVS-Signature': signBody(body, secret),
    'x-webhook-timestamp': new Date().toISOString(),
  };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('lvsWebhooks — POST /recording bare envelope normalization', () => {
  let server: http.Server;

  beforeAll(async () => {
    const app = express();
    app.use('/hooks/lvs', lvsWebhooksRouter);
    server = http.createServer(app);
    await new Promise<void>((r) => server.listen(0, '127.0.0.1', r));
  });

  afterAll(async () => {
    await new Promise<void>((r) => server.close(() => r()));
  });

  beforeEach(() => {
    putRecordingMock.mockClear();
    getSessionMock.mockClear();
    findByChannelArnMock.mockClear();
    findByChannelArnMock.mockResolvedValue([]);
  });

  test('bare recording.completed envelope → putRecording called with recordingId + channelArn', async () => {
    const payload = {
      type: 'recording.completed',
      channelArn: 'arn:x',
      data: {
        recordingId: 'r1',
        segments: [],
        totalSize: 0,
        durationMs: 1000,
        destination: 'local',
      },
    };
    const body = Buffer.from(JSON.stringify(payload), 'utf-8');

    const res = await rawPost(server, '/hooks/lvs/recording', body, signedHeaders(body));

    // 204 = persisted OK
    expect(res.status).toBe(204);

    // The repo MUST have been called exactly once with the normalized fields.
    expect(putRecordingMock).toHaveBeenCalledTimes(1);
    const arg = putRecordingMock.mock.calls[0]![0];
    expect(arg.recordingId).toBe('r1');
    expect(arg.channelArn).toBe('arn:x');
    // No Redis arn-index, no session → callId falls back to synthetic.
    expect(arg.callId).toContain('unknown-arn:x-r1');
    expect(arg.durationMs).toBe(1000);
    expect(arg.destination).toBe('local');
  });

  // Problem 1 — DM visibility. When Redis arn-index misses (test stub returns null),
  // the webhook MUST fall back to videoSessionRepo.findByChannelArn AND, when that
  // also returns nothing, derive participants from the `dm:<a>:<b>` lobby pattern
  // so the recording is reachable to both DM users from /recordings.
  test('DM lobby + redis miss + no session → derives participants from dm:<a>:<b>', async () => {
    // Pretend the DDB scan DID find a session row that maps the arn back
    // to the DM lobby — exercising the realistic fallback path.
    findByChannelArnMock.mockResolvedValueOnce([
      {
        documentId: 'dm:u-hank:u-carol',
        sessionId: 'sess-dm-1',
        channelArn: 'arn:dm',
        startedAt: '2026-05-23T12:00:00.000Z',
        startedBy: 'u-hank',
        participants: [], // empty — simulating mid-call Redis blip
      },
    ]);

    const payload = {
      type: 'recording.completed',
      channelArn: 'arn:dm',
      data: {
        recordingId: 'r-dm',
        segments: [],
        totalSize: 0,
        durationMs: 5000,
        destination: 'local',
      },
    };
    const body = Buffer.from(JSON.stringify(payload), 'utf-8');

    const res = await rawPost(server, '/hooks/lvs/recording', body, signedHeaders(body));
    expect(res.status).toBe(204);
    expect(findByChannelArnMock).toHaveBeenCalledWith('arn:dm');

    expect(putRecordingMock).toHaveBeenCalledTimes(1);
    const arg = putRecordingMock.mock.calls[0]![0];
    expect(arg.lobbyName).toBe('dm:u-hank:u-carol');
    // session.startedBy used — verifies the DDB fallback flowed through.
    expect(arg.startedBy).toBe('u-hank');
    // DM participants derived from lobby name (since session.participants was [])
    const userIds = arg.participants.map((p: { userId: string }) => p.userId).sort();
    expect(userIds).toEqual(['u-carol', 'u-hank']);
  });

  test('composedUrl + tracks from LVS payload are forwarded to putRecording', async () => {
    const payload = {
      type: 'recording.completed',
      channelArn: 'arn:multi',
      data: {
        recordingId: 'r-multi',
        segments: [],
        totalSize: 0,
        durationMs: 1000,
        destination: 'local',
        composedUrl: 'https://lvs.test/composed/r-multi.mp4',
        tracks: [
          { participantId: 'p1', key: 'k1', url: 'https://t/1.mp4', size: 1 },
          { participantId: 'p2', key: 'k2', url: 'https://t/2.mp4', size: 2 },
        ],
      },
    };
    const body = Buffer.from(JSON.stringify(payload), 'utf-8');
    const res = await rawPost(server, '/hooks/lvs/recording', body, signedHeaders(body));
    expect(res.status).toBe(204);
    const arg = putRecordingMock.mock.calls[0]![0];
    expect(arg.composedUrl).toBe('https://lvs.test/composed/r-multi.mp4');
    expect(arg.tracks).toHaveLength(2);
  });

  // Screenshot bug (2026-08): a composed video visibly playing THREE tiles
  // was captioned "1 participant". session.participants is a LIVE roster —
  // people who left before recording end are already removed — so counting
  // it at webhook time undercounts every real multi-leg recording. The
  // per-leg tracks[] on recording.completed are the truth.
  test('N legs → participantCount N even when the live roster shrank to 1', async () => {
    findByChannelArnMock.mockResolvedValueOnce([
      {
        documentId: 'global-hangout',
        sessionId: 'sess-gh-1',
        channelArn: 'arn:gh',
        startedAt: '2026-08-24T01:00:00.000Z',
        startedBy: 'u-alice',
        // Live roster at recording-completed: everyone but Alice already left.
        participants: [{ userId: 'u-alice', participantId: 'p-alice' }],
      },
    ]);

    const payload = {
      type: 'recording.completed',
      channelArn: 'arn:gh',
      data: {
        recordingId: 'r-gh',
        segments: [],
        totalSize: 0,
        durationMs: 60_000,
        destination: 'local',
        composedUrl: 'https://lvs.test/composed/r-gh.mp4',
        // Three legs. LVS multi-leg tracks carry participantId (no userId)
        // except when the SFU could attribute one — mix both shapes.
        tracks: [
          { legId: 'leg-1', participantId: 'p-alice', key: 'k1', url: 'https://t/1.mp4', size: 1 },
          { legId: 'leg-2', participantId: 'p-bob', userId: 'u-bob', key: 'k2', url: 'https://t/2.mp4', size: 2 },
          { legId: 'leg-3', participantId: 'p-carol', key: 'k3', url: 'https://t/3.mp4', size: 3 },
        ],
      },
    };
    const body = Buffer.from(JSON.stringify(payload), 'utf-8');
    const res = await rawPost(server, '/hooks/lvs/recording', body, signedHeaders(body));
    expect(res.status).toBe(204);

    expect(putRecordingMock).toHaveBeenCalledTimes(1);
    const arg = putRecordingMock.mock.calls[0]![0];
    // The stored scalar reflects the legs, not the shrunken roster.
    expect(arg.participantCount).toBe(3);
    // Bob's userId-bearing track was unioned into participants[] so his
    // fan-out row exists and the recording shows up in HIS list too.
    const userIds = arg.participants.map((p: { userId: string }) => p.userId).sort();
    expect(userIds).toEqual(['u-alice', 'u-bob']);
  });

  // Same-peer audio+video split tracks must not double-count.
  test('split a/v tracks for the same peer count as one participant', async () => {
    const payload = {
      type: 'recording.completed',
      channelArn: 'arn:av',
      data: {
        recordingId: 'r-av',
        segments: [],
        totalSize: 0,
        durationMs: 1000,
        destination: 'local',
        tracks: [
          { participantId: 'p1', kind: 'video', key: 'k1', url: 'https://t/1.mp4', size: 1 },
          { participantId: 'p1', kind: 'audio', key: 'k2', url: 'https://t/2.mp4', size: 2 },
          { participantId: 'p2', kind: 'av', key: 'k3', url: 'https://t/3.mp4', size: 3 },
        ],
      },
    };
    const body = Buffer.from(JSON.stringify(payload), 'utf-8');
    const res = await rawPost(server, '/hooks/lvs/recording', body, signedHeaders(body));
    expect(res.status).toBe(204);
    const arg = putRecordingMock.mock.calls[0]![0];
    expect(arg.participantCount).toBe(2);
  });

  // hub#1260 — replay-window guard, paired with HMAC.
  test('valid HMAC + missing x-webhook-timestamp → 401, repo NOT called', async () => {
    const payload = {
      type: 'recording.completed',
      channelArn: 'arn:x',
      data: { recordingId: 'r1', segments: [], totalSize: 0, durationMs: 1000, destination: 'local' },
    };
    const body = Buffer.from(JSON.stringify(payload), 'utf-8');
    const res = await rawPost(server, '/hooks/lvs/recording', body, {
      'X-LVS-Signature': signBody(body),
      // No x-webhook-timestamp.
    });
    expect(res.status).toBe(401);
    expect(putRecordingMock).not.toHaveBeenCalled();
  });

  test('valid HMAC + stale x-webhook-timestamp → 401', async () => {
    const payload = {
      type: 'recording.completed',
      channelArn: 'arn:x',
      data: { recordingId: 'r1', segments: [], totalSize: 0, durationMs: 1000, destination: 'local' },
    };
    const body = Buffer.from(JSON.stringify(payload), 'utf-8');
    const res = await rawPost(server, '/hooks/lvs/recording', body, {
      'X-LVS-Signature': signBody(body),
      'x-webhook-timestamp': new Date(Date.now() - 10 * 60_000).toISOString(),
    });
    expect(res.status).toBe(401);
    expect(putRecordingMock).not.toHaveBeenCalled();
  });

  test('invalid HMAC signature → 401, repo NOT called', async () => {
    const payload = {
      type: 'recording.completed',
      channelArn: 'arn:x',
      data: { recordingId: 'r1', segments: [], totalSize: 0, durationMs: 1000, destination: 'local' },
    };
    const body = Buffer.from(JSON.stringify(payload), 'utf-8');

    const res = await rawPost(server, '/hooks/lvs/recording', body, {
      'X-LVS-Signature': 'sha256=' + 'a'.repeat(64),
    });

    expect(res.status).toBe(401);
    expect(putRecordingMock).not.toHaveBeenCalled();
  });

  // hub#1006: production-safety gate. POST without an X-LVS-Signature header
  // is rejected when the secret is set — closes the gap where a misconfigured
  // ingress could forge channel-lifecycle events.
  test('POST without X-LVS-Signature header → 401 (hub#1006)', async () => {
    const payload = {
      type: 'recording.completed',
      channelArn: 'arn:x',
      data: { recordingId: 'r1', segments: [], totalSize: 0, durationMs: 1000, destination: 'local' },
    };
    const body = Buffer.from(JSON.stringify(payload), 'utf-8');

    const res = await rawPost(server, '/hooks/lvs/recording', body, {
      // Note: no X-LVS-Signature header.
    });

    expect(res.status).toBe(401);
    expect(putRecordingMock).not.toHaveBeenCalled();
  });
});

// hub#1006: dev-mode behavior. When the secret is unset, the route used to
// accept blindly — now it rejects unless SKIP_AUTH=true is set as the
// explicit dev opt-in.
describe('lvsWebhooks — no-secret gate (hub#1006)', () => {
  let server: http.Server;
  let originalSecret: string | undefined;
  let originalSkipAuth: string | undefined;

  beforeAll(async () => {
    originalSecret = process.env.LVS_WEBHOOK_SECRET;
    originalSkipAuth = process.env.SKIP_AUTH;
    const app = express();
    app.use('/hooks/lvs', lvsWebhooksRouter);
    server = http.createServer(app);
    await new Promise<void>((r) => server.listen(0, '127.0.0.1', r));
  });

  afterAll(async () => {
    if (originalSecret === undefined) delete process.env.LVS_WEBHOOK_SECRET;
    else process.env.LVS_WEBHOOK_SECRET = originalSecret;
    if (originalSkipAuth === undefined) delete process.env.SKIP_AUTH;
    else process.env.SKIP_AUTH = originalSkipAuth;
    await new Promise<void>((r) => server.close(() => r()));
  });

  beforeEach(() => {
    putRecordingMock.mockClear();
    delete process.env.LVS_WEBHOOK_SECRET;
    delete process.env.SKIP_AUTH;
  });

  test('no secret + no SKIP_AUTH → 401 (production-safe default)', async () => {
    const body = Buffer.from(JSON.stringify({ type: 'recording.completed' }), 'utf-8');
    const res = await rawPost(server, '/hooks/lvs/recording', body, {});
    expect(res.status).toBe(401);
    expect(putRecordingMock).not.toHaveBeenCalled();
  });

  test('no secret + SKIP_AUTH=true → 204 (explicit dev opt-in)', async () => {
    process.env.SKIP_AUTH = 'true';
    const payload = {
      type: 'recording.completed',
      channelArn: 'arn:dev',
      data: { recordingId: 'r-dev', segments: [], totalSize: 0, durationMs: 1000, destination: 'local' },
    };
    const body = Buffer.from(JSON.stringify(payload), 'utf-8');
    const res = await rawPost(server, '/hooks/lvs/recording', body, {});
    expect(res.status).toBe(204);
    expect(putRecordingMock).toHaveBeenCalledTimes(1);
  });
});

// ---------------------------------------------------------------------------
// The webhook is where all three lies were born, so it gets end-to-end
// coverage of what it now persists. Payloads are trimmed from real
// `recording.completed` deliveries captured on 2026-08-25.
// ---------------------------------------------------------------------------
describe('lvsWebhooks — what the persisted row is allowed to claim', () => {
  let server: http.Server;

  beforeAll(async () => {
    const app = express();
    app.use('/hooks/lvs', lvsWebhooksRouter);
    server = http.createServer(app);
    await new Promise<void>((r) => server.listen(0, '127.0.0.1', r));
  });

  afterAll(async () => {
    await new Promise<void>((r) => server.close(() => r()));
  });

  beforeEach(() => {
    putRecordingMock.mockClear();
    getSessionMock.mockClear();
    findByChannelArnMock.mockClear();
    findByChannelArnMock.mockResolvedValue([]);
  });

  async function deliver(payload: unknown) {
    const body = Buffer.from(JSON.stringify(payload), 'utf-8');
    const res = await rawPost(server, '/hooks/lvs/recording', body, signedHeaders(body));
    expect(res.status).toBe(204);
    expect(putRecordingMock).toHaveBeenCalledTimes(1);
    return putRecordingMock.mock.calls[0]![0];
  }

  // FINDING 1 -------------------------------------------------------------
  test('durationMs is the MEDIA length, not the channel-session lifetime', async () => {
    // The real hank↔bob DM: 18,736ms of session, 8,668ms of media. The UI
    // rendered "18s" over a player whose own timeline read 0:08.
    const arg = await deliver({
      type: 'recording.completed',
      channelArn: 'arn:dur',
      data: {
        recordingId: 'r-dur',
        segments: [],
        totalSize: 0,
        durationMs: 18_736,
        destination: 'local',
        tracks: [
          { legId: 'leg-001', participantId: 'p-1', startedAtMs: 3, endedAtMs: 8_671, durationMs: 8_668 },
          { legId: 'leg-002', participantId: 'p-2', startedAtMs: 21, endedAtMs: 8_640, durationMs: 8_619 },
        ],
      },
    });

    expect(arg.durationMs).toBe(8_668);
    // The raw number is kept, not thrown away — it's real, it just isn't
    // what "Duration" means to a viewer.
    expect(arg.sessionDurationMs).toBe(18_736);
  });

  test('the worst measured overstatement (23.3s claimed / 0.5s of media) is gone', async () => {
    const arg = await deliver({
      type: 'recording.completed',
      channelArn: 'arn:worst',
      data: {
        recordingId: 'r-worst',
        segments: [],
        totalSize: 0,
        durationMs: 23_278,
        destination: 'local',
        tracks: [{ legId: 'leg-001', startedAtMs: 4, endedAtMs: 512, durationMs: 508 }],
      },
    });

    expect(arg.durationMs).toBe(508);
    expect(arg.durationMs / 23_278).toBeLessThan(0.1);
  });

  // FINDING 2 -------------------------------------------------------------
  test('the caller is in participants[] even when the live roster dropped them', async () => {
    // The roster is snapshotted at recording-COMPLETE time, by which point
    // hank (who placed the call) has hung up. The row used to name only bob
    // — so hank never got a fan-out row and never saw his own recording.
    findByChannelArnMock.mockResolvedValueOnce([
      {
        documentId: 'dm:dev-bob:dev-hank',
        sessionId: 'sess-1',
        channelArn: 'arn:caller',
        startedBy: 'dev-hank',
        participants: [
          {
            userId: 'dev-bob',
            displayName: 'Bob Martinez',
            participantId: 'dev-bob-mt9h',
            joinedAt: '2026-08-26T02:22:42.704Z',
          },
        ],
      },
    ]);

    const arg = await deliver({
      type: 'recording.completed',
      channelArn: 'arn:caller',
      data: {
        recordingId: 'r-caller',
        segments: [],
        totalSize: 0,
        durationMs: 21_800,
        destination: 'local',
        startedAt: '2026-08-26T02:23:24.679Z',
        endedAt: '2026-08-26T02:23:42.558Z',
        tracks: [{ legId: 'leg-001', participantId: 'p-1', startedAtMs: 3, endedAtMs: 21_000 }],
      },
    });

    expect(arg.startedBy).toBe('dev-hank');
    expect(arg.participants.map((p: { userId: string }) => p.userId).sort()).toEqual([
      'dev-bob', 'dev-hank',
    ]);
    // …and therefore the DM reads as the 2-person call it was, not "1 👥".
    expect(arg.participantCount).toBe(2);
  });

  // FINDING 3 -------------------------------------------------------------
  test('a rejoin-inflated roster collapses to distinct people', async () => {
    // The lobby roster appends one entry per join. Four hank sessions and
    // three bob sessions in one 2-person DM rendered "7 👥".
    findByChannelArnMock.mockResolvedValueOnce([
      {
        documentId: 'dm:dev-bob:dev-hank',
        sessionId: 'sess-2',
        channelArn: 'arn:rejoin',
        startedBy: 'dev-hank',
        participants: [
          { userId: 'dev-hank', displayName: 'Hank Hill', joinedAt: '2026-08-24T22:07:20.000Z' },
          { userId: 'dev-bob', displayName: 'Bob Martinez', joinedAt: '2026-08-24T22:07:20.500Z' },
          { userId: 'dev-hank', joinedAt: '2026-08-24T22:07:21.000Z' },
          { userId: 'dev-bob', joinedAt: '2026-08-24T22:07:21.500Z' },
          { userId: 'dev-hank', joinedAt: '2026-08-24T22:07:22.000Z' },
          { userId: 'dev-bob', joinedAt: '2026-08-24T22:07:22.500Z' },
          { userId: 'dev-hank', joinedAt: '2026-08-24T22:07:23.000Z' },
        ],
      },
    ]);

    const arg = await deliver({
      type: 'recording.completed',
      channelArn: 'arn:rejoin',
      data: {
        recordingId: 'r-rejoin',
        segments: [],
        totalSize: 0,
        durationMs: 22_000,
        destination: 'local',
        startedAt: '2026-08-24T22:07:23.349Z',
        endedAt: '2026-08-24T22:07:45.000Z',
        tracks: [
          { legId: 'leg-001', participantId: 'p-1', startedAtMs: 3, endedAtMs: 21_000 },
          { legId: 'leg-002', participantId: 'p-2', startedAtMs: 8, endedAtMs: 21_100 },
        ],
      },
    });

    expect(arg.participantCount).toBe(2);
    expect(arg.participants).toHaveLength(2);
    // Display names survive the dedup — they're what the UI renders.
    expect(
      arg.participants.map((p: { displayName?: string }) => p.displayName).sort(),
    ).toEqual(['Bob Martinez', 'Hank Hill']);
  });

  test("a roster carried over from an earlier call in the same lobby is scoped out", async () => {
    findByChannelArnMock.mockResolvedValueOnce([
      {
        documentId: 'room:standup',
        sessionId: 'sess-3',
        channelArn: 'arn:stale',
        startedBy: 'dev-hank',
        participants: [
          { userId: 'dev-hank', joinedAt: '2026-08-23T21:26:30.000Z' },
          { userId: 'dev-bob', joinedAt: '2026-08-23T21:26:31.000Z' },
          // 46 minutes older than this recording's own startedAt.
          { userId: 'dev-alice', joinedAt: '2026-08-23T20:40:47.142Z' },
        ],
      },
    ]);

    const arg = await deliver({
      type: 'recording.completed',
      channelArn: 'arn:stale',
      data: {
        recordingId: 'r-stale',
        segments: [],
        totalSize: 0,
        durationMs: 30_000,
        destination: 'local',
        startedAt: '2026-08-23T21:26:40.268Z',
        endedAt: '2026-08-23T21:27:10.000Z',
        tracks: [
          { legId: 'leg-001', participantId: 'p-1', startedAtMs: 3, endedAtMs: 28_000 },
          { legId: 'leg-002', participantId: 'p-2', startedAtMs: 9, endedAtMs: 28_100 },
        ],
      },
    });

    expect(arg.participants.map((p: { userId: string }) => p.userId).sort()).toEqual([
      'dev-bob', 'dev-hank',
    ]);
    expect(arg.participantCount).toBe(2);
  });
});
