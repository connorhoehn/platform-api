// platform-api/src/routes/__tests__/recordings.test.ts
//
// Route-level coverage for src/routes/recordings.ts. Audit (hub#1283)
// flagged this as one of two route files with no test — silent regressions
// in the breaker-error mapping or the playback-token mint flow would
// otherwise land with zero alarm.
//
// Mocks `callSfu` (not the underlying CircuitBreaker) to match the pattern
// used in videoSessions.test.ts.

import express, {
  type NextFunction,
  type Request,
  type Response,
} from 'express';

// --- mocks ------------------------------------------------------------------

const mockListByUser = jest.fn();
const mockGetRecording = jest.fn();
const mockGetSessionsByDocument = jest.fn();
const mockDocClientSend = jest.fn();
const mockCallSfu = jest.fn();
const mockMapSfuError = jest.fn();

jest.mock('../../middleware/auth', () => ({
  requireAuth: (req: Request, _res: Response, next: NextFunction) => {
    (req as unknown as { user: { sub: string } }).user = { sub: 'u-1' };
    next();
  },
}));

jest.mock('../../repositories', () => ({
  callRecordingRepo: {
    listByUser: (...args: unknown[]) => mockListByUser(...args),
    getRecording: (...args: unknown[]) => mockGetRecording(...args),
    listByLobbyName: jest.fn(),
    deleteRecording: jest.fn(),
  },
  videoSessionRepo: {
    getSessionsByDocument: (...args: unknown[]) => mockGetSessionsByDocument(...args),
  },
}));

jest.mock('../../lib/aws-clients', () => ({
  docClient: { send: (...args: unknown[]) => mockDocClientSend(...args) },
}));

jest.mock('../../lib/ddb-table-name', () => ({
  tableName: (n: string) => `test-${n}`,
}));

jest.mock('../../lib/video-auth', () => ({
  VIDEO_API_URL: 'https://video.test',
  VIDEO_PUBLIC_URL: 'https://public.video.test',
}));

jest.mock('../../lib/sfu-breaker', () => ({
  callSfu: (...args: unknown[]) => mockCallSfu(...args),
  mapSfuError: (e: unknown) => mockMapSfuError(e),
}));

// Lazy import after mocks.
import { recordingsRouter } from '../recordings';
import { errorHandler, AppError } from '../../middleware/error-handler';

// --- helpers ----------------------------------------------------------------

function buildApp(): express.Express {
  const app = express();
  app.use(express.json());
  app.use('/api', recordingsRouter);
  app.use(errorHandler);
  return app;
}

interface MockRes {
  statusCode: number;
  body: unknown;
  ended: boolean;
  finished: Promise<void>;
  status(c: number): MockRes;
  json(b: unknown): MockRes;
  send(b?: unknown): MockRes;
  setHeader(): MockRes;
  getHeader(): unknown;
  end(...args: unknown[]): MockRes;
}

function mockRes(): MockRes {
  let resolve!: () => void;
  const finished = new Promise<void>((r) => { resolve = r; });
  const r: MockRes = {
    statusCode: 200, body: undefined, ended: false, finished,
    status(c) { this.statusCode = c; return this; },
    json(b) { this.body = b; this.ended = true; resolve(); return this; },
    send(b) { this.body = b; this.ended = true; resolve(); return this; },
    setHeader() { return this; },
    getHeader() { return undefined; },
    end() { this.ended = true; resolve(); return this; },
  };
  return r;
}

async function run(method: string, url: string, body?: unknown): Promise<MockRes> {
  const app = buildApp();
  const res = mockRes();
  const req = {
    method,
    url,
    originalUrl: url,
    path: url.split('?')[0],
    headers: { 'content-type': 'application/json' },
    query: {},
    body: body ?? {},
    params: {},
    ip: '127.0.0.1',
  } as unknown as Request;
  await new Promise<void>((resolve, reject) => {
    const finalHandler = (err?: unknown): void => {
      if (err) return reject(err);
      if (!res.ended) res.status(404).json({ error: 'not found' });
      resolve();
    };
    (app as unknown as (req: Request, res: Response, next: NextFunction) => void)(
      req, res as unknown as Response, finalHandler,
    );
    res.finished.then(() => resolve()).catch(reject);
  });
  return res;
}

function lvsOk(data: Record<string, unknown>) {
  return { ok: true, status: 200, json: async () => data } as unknown as globalThis.Response;
}

function lvsFail(status: number) {
  return { ok: false, status, json: async () => ({}) } as unknown as globalThis.Response;
}

// --- tests ------------------------------------------------------------------

beforeEach(() => {
  mockListByUser.mockReset();
  mockGetRecording.mockReset();
  mockGetSessionsByDocument.mockReset();
  mockDocClientSend.mockReset();
  mockCallSfu.mockReset();
  mockMapSfuError.mockReset().mockImplementation((e) => {
    if (e?.name === 'BreakerOpenError') return new AppError(503, 'breaker open');
    if (e?.name === 'TimeoutError') return new AppError(504, 'timeout');
    return new AppError(502, 'unreachable');
  });
});

describe('GET /api/recordings/:recordingId', () => {
  it('returns the recording when the caller participated', async () => {
    mockListByUser.mockResolvedValue({ items: [{ recordingId: 'r-1', callId: 'c-1' }] });
    mockGetRecording.mockResolvedValue({ recordingId: 'r-1', callId: 'c-1', startedAt: 't0', endedAt: 't1' });
    const res = await run('GET', '/api/recordings/r-1');
    expect(res.statusCode).toBe(200);
    expect((res.body as { recordingId: string }).recordingId).toBe('r-1');
  });

  it('404 when caller did not participate (recording not in user index)', async () => {
    mockListByUser.mockResolvedValue({ items: [] });
    const res = await run('GET', '/api/recordings/r-x');
    expect(res.statusCode).toBe(404);
  });
});

describe('POST /api/recordings/:recordingId/playback-token', () => {
  const matchItem = { recordingId: 'r-1', callId: 'c-1', channelArn: 'arn:1' };
  const matchDetail = {
    recordingId: 'r-1',
    callId: 'c-1',
    channelArn: 'arn:1',
    lobbyName: 'doc-1',
    startedAt: '2026-05-30T00:00:00Z',
    endedAt: '2026-05-30T00:30:00Z',
  };

  it('returns playlistUrl + token on 200 from LVS (happy path)', async () => {
    mockListByUser.mockResolvedValue({ items: [matchItem] });
    mockGetRecording.mockResolvedValue(matchDetail);
    mockCallSfu.mockResolvedValue(lvsOk({ token: 'tok', expiresAt: '2026-05-30T01:00:00Z' }));

    const prevAdmin = process.env.VIDEO_ADMIN_TOKEN;
    process.env.VIDEO_ADMIN_TOKEN = 'admin-secret';
    let res;
    try {
      res = await run('POST', '/api/recordings/r-1/playback-token');
    } finally {
      if (prevAdmin === undefined) delete process.env.VIDEO_ADMIN_TOKEN;
      else process.env.VIDEO_ADMIN_TOKEN = prevAdmin;
    }
    expect(res.statusCode).toBe(200);
    const body = res.body as { playlistUrl: string; token: string; expiresAt: string };
    expect(body.token).toBe('tok');
    expect(body.expiresAt).toBe('2026-05-30T01:00:00Z');
    expect(body.playlistUrl).toContain('https://public.video.test/api/channels/');
    // Direct-MP4 playback (composed-url flow): playlistUrl points at the LVS
    // segments route with no token query param — the token rides separately
    // in body.token for private-channel callers.
    expect(body.playlistUrl).toContain('/recordings/r-1/segments/');
    expect((res.body as { composedUrl: string }).composedUrl).toBe(body.playlistUrl);

    // Post-Wave-7 the DDB streamKey is always '' (LVS no longer returns
    // one on POST /api/sessions), so this route uses VIDEO_ADMIN_TOKEN
    // as the LVS bearer. LVS bearerAuth treats it as a role=admin
    // wildcard and accepts it on /playback-tokens.
    const [, init] = mockCallSfu.mock.calls[0];
    expect((init as RequestInit).headers).toMatchObject({ Authorization: 'Bearer admin-secret' });
  });

  it('returns empty token when LVS responds 400 (public channel, playback open)', async () => {
    mockListByUser.mockResolvedValue({ items: [matchItem] });
    mockGetRecording.mockResolvedValue(matchDetail);
    mockGetSessionsByDocument.mockResolvedValue([]);
    mockCallSfu.mockResolvedValue(lvsFail(400));

    const res = await run('POST', '/api/recordings/r-1/playback-token');
    expect(res.statusCode).toBe(200);
    const body = res.body as { token: string | null; expiresAt: string | null };
    expect(body.token).toBeNull();
    expect(body.expiresAt).toBeNull();
  });

  it('503 when the SFU breaker is open (BreakerOpenError → mapSfuError)', async () => {
    mockListByUser.mockResolvedValue({ items: [matchItem] });
    mockGetRecording.mockResolvedValue(matchDetail);
    mockGetSessionsByDocument.mockResolvedValue([{ channelArn: 'arn:1', streamKey: 'sk-1' }]);
    const breakerOpen = new Error('breaker');
    breakerOpen.name = 'BreakerOpenError';
    mockCallSfu.mockRejectedValue(breakerOpen);

    const res = await run('POST', '/api/recordings/r-1/playback-token');
    expect(res.statusCode).toBe(503);
  });

  it('502 when LVS returns non-400 4xx/5xx (AppError thrown inside try)', async () => {
    mockListByUser.mockResolvedValue({ items: [matchItem] });
    mockGetRecording.mockResolvedValue(matchDetail);
    mockGetSessionsByDocument.mockResolvedValue([{ channelArn: 'arn:1', streamKey: 'sk-1' }]);
    mockCallSfu.mockResolvedValue(lvsFail(500));

    const res = await run('POST', '/api/recordings/r-1/playback-token');
    expect(res.statusCode).toBe(502);
  });

  it('404 when caller did not participate', async () => {
    mockListByUser.mockResolvedValue({ items: [] });
    const res = await run('POST', '/api/recordings/r-x/playback-token');
    expect(res.statusCode).toBe(404);
    expect(mockCallSfu).not.toHaveBeenCalled();
  });
});

// ---------------------------------------------------------------------------
// The detail response is the only place `tracks[]` is served, and LVS stamps
// each track with the recorder pod's absolute path — in local dev, literally
// `file:///Users/<developer>/Projects/...`. Unusable by a browser, and a
// filesystem-layout leak to every authenticated caller.
//
// Same response also re-derives people, so rows written before the webhook
// fix don't keep rendering rejoin counts on the detail card.
// ---------------------------------------------------------------------------
describe('GET /api/recordings/:recordingId — what leaves the process', () => {
  /** Trimmed from the real row 2026-08-26T02-35-09-652Z-546cf1e9. */
  const leakyDetail = {
    recordingId: 'r-1',
    callId: 'c-1',
    channelArn: 'arn:local:ivs:channel/fe74b6a1',
    lobbyName: 'dm:dev-bob:dev-hank',
    startedAt: '2026-08-26T02:35:09.768Z',
    endedAt: '2026-08-26T02:35:28.215Z',
    durationMs: 8_318,
    sessionDurationMs: 18_447,
    startedBy: 'dev-hank',
    participants: [
      { userId: 'dev-bob', displayName: 'Bob Martinez', joinedAt: '2026-08-26T02:34:49.825Z' },
      { userId: 'dev-bob', displayName: 'Bob Martinez', joinedAt: '2026-08-26T02:35:10.000Z' },
    ],
    tracks: [
      {
        legId: 'leg-001',
        participantId: 'a937dfa9-aeb8-5366-a90b-6d5599ba1239',
        file: '/Users/connorhoehn/Projects/realtime-examples/.tilt-lvs-recordings/x/peer_leg-001.mp4',
        key: 'arn_local_ivs_channel_fe74b6a1/2026-08-26T02-35-09-652Z-546cf1e9/peer_leg-001.mp4',
        url: 'file:///Users/connorhoehn/Projects/realtime-examples/.tilt-lvs-recordings/x/peer_leg-001.mp4',
        size: 796,
        kind: 'av',
      },
    ],
  };

  it('no file:// URI and no developer home directory anywhere in the response', async () => {
    mockListByUser.mockResolvedValue({ items: [{ recordingId: 'r-1', callId: 'c-1' }] });
    mockGetRecording.mockResolvedValue(leakyDetail);

    const res = await run('GET', '/api/recordings/r-1');
    expect(res.statusCode).toBe(200);

    const serialized = JSON.stringify(res.body);
    expect(serialized).not.toContain('file://');
    expect(serialized).not.toContain('/Users/');
    expect(serialized).not.toContain('.tilt-lvs-recordings');
  });

  it('tracks[] carry the same proxied /segments/ URL shape as the composed file', async () => {
    mockListByUser.mockResolvedValue({ items: [{ recordingId: 'r-1', callId: 'c-1' }] });
    mockGetRecording.mockResolvedValue(leakyDetail);

    const res = await run('GET', '/api/recordings/r-1');
    const track = (res.body as { tracks: Array<Record<string, unknown>> }).tracks[0]!;
    expect(track.url).toBe(
      'https://public.video.test/api/channels/'
      + encodeURIComponent('arn:local:ivs:channel/fe74b6a1')
      + '/recordings/r-1/segments/peer_leg-001.mp4',
    );
    expect(track).not.toHaveProperty('file');
    // Everything else the downloads list needs survives.
    expect(track.legId).toBe('leg-001');
    expect(track.size).toBe(796);
  });

  it('participants are deduped and include the initiator', async () => {
    mockListByUser.mockResolvedValue({ items: [{ recordingId: 'r-1', callId: 'c-1' }] });
    mockGetRecording.mockResolvedValue(leakyDetail);

    const res = await run('GET', '/api/recordings/r-1');
    const body = res.body as {
      participants: Array<{ userId: string }>;
      participantCount: number;
    };
    expect(body.participants.map((p) => p.userId).sort()).toEqual(['dev-bob', 'dev-hank']);
    expect(body.participantCount).toBe(2);
  });
});
