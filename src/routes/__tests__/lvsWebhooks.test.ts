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
    const sig = signBody(body);

    const res = await rawPost(server, '/hooks/lvs/recording', body, {
      'X-LVS-Signature': sig,
    });

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

    const res = await rawPost(server, '/hooks/lvs/recording', body, {
      'X-LVS-Signature': signBody(body),
    });
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
});
