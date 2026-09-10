// platform-api/src/routes/__tests__/lvsAdmin.test.ts
//
// Route-level coverage for src/routes/lvsAdmin.ts. Audit (hub#1283) flagged
// this as one of two route files with no test. lvsAdmin doesn't call LVS
// directly — its endpoints scan DDB (videos, bindings, rooms) and read
// the in-memory webhook delivery ring buffer — so we mock docClient,
// hangoutRoomMemberRepo, requireAdmin, and the lvsWebhooks ring buffer.

import express, {
  type NextFunction,
  type Request,
  type Response,
} from 'express';

const mockDocClientSend = jest.fn();
const mockDdbRawSend = jest.fn();
const mockGetRecentDeliveries = jest.fn();
const mockListMembers = jest.fn();
const mockGetRedisClient = jest.fn();

jest.mock('../../middleware/requireAdmin', () => ({
  requireAdmin: (_req: Request, _res: Response, next: NextFunction) => next(),
}));

jest.mock('../../lib/aws-clients', () => ({
  docClient: { send: (...args: unknown[]) => mockDocClientSend(...args) },
}));

jest.mock('@aws-sdk/client-dynamodb', () => {
  const actual = jest.requireActual('@aws-sdk/client-dynamodb');
  return {
    ...actual,
    DynamoDBClient: jest.fn().mockImplementation(() => ({
      send: (...args: unknown[]) => mockDdbRawSend(...args),
    })),
  };
});

jest.mock('@aws-sdk/util-dynamodb', () => ({
  unmarshall: (row: Record<string, unknown>) => row,
}));

jest.mock('../../lib/ddb-table-name', () => ({
  tableName: (n: string) => `test-${n}`,
}));

const mockGetSessionsByDocument = jest.fn();

jest.mock('../../repositories', () => ({
  hangoutRoomMemberRepo: {
    listMembers: (...args: unknown[]) => mockListMembers(...args),
  },
  videoSessionRepo: {
    getSessionsByDocument: (...args: unknown[]) => mockGetSessionsByDocument(...args),
  },
}));

jest.mock('../../repositories/HangoutRoomRepository', () => ({
  HANGOUT_ROOMS_TABLE: 'test-hangout-rooms',
}));

jest.mock('../../repositories/VideoSessionRepository', () => ({
  VIDEO_SESSIONS_TABLE: 'test-video-sessions',
}));

jest.mock('../lvsWebhooks', () => ({
  getRecentDeliveries: () => mockGetRecentDeliveries(),
}));

jest.mock('../../lib/redis-client', () => ({
  getRedisClient: () => mockGetRedisClient(),
}));

// Lazy import after mocks.
import { lvsAdminRouter } from '../lvsAdmin';
import { errorHandler } from '../../middleware/error-handler';

// --- helpers ----------------------------------------------------------------

function buildApp(): express.Express {
  const app = express();
  app.use(express.json());
  app.use('/api/admin', lvsAdminRouter);
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

async function run(method: string, url: string): Promise<MockRes> {
  const app = buildApp();
  const res = mockRes();
  const req = {
    method, url, originalUrl: url, path: url.split('?')[0],
    headers: { 'content-type': 'application/json' },
    query: Object.fromEntries(new URLSearchParams(url.split('?')[1] ?? '')), body: {}, params: {},
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

beforeEach(() => {
  mockGetSessionsByDocument.mockReset().mockResolvedValue([]);
  mockDocClientSend.mockReset();
  mockDdbRawSend.mockReset();
  mockGetRecentDeliveries.mockReset();
  mockListMembers.mockReset();
  mockGetRedisClient.mockReset().mockResolvedValue(null);
});

// --- tests ------------------------------------------------------------------

describe('GET /api/admin/recordings/health', () => {
  it('aggregates hmacFailRate + persistFailRate from the delivery ring', async () => {
    mockGetRecentDeliveries.mockReturnValue([
      { timestamp: 't1', eventType: 'recording.completed', hmacOk: true, persisted: true },
      { timestamp: 't2', eventType: 'recording.completed', hmacOk: true, persisted: false },
      { timestamp: 't3', eventType: '', hmacOk: false, persisted: false },
    ]);
    const res = await run('GET', '/api/admin/recordings/health');
    expect(res.statusCode).toBe(200);
    const body = res.body as {
      deliveries: unknown[];
      aggregate: {
        totalReceived: number;
        hmacFailRate: number;
        persistFailRate: number;
        lastReceivedAt: string | null;
      };
    };
    expect(body.deliveries).toHaveLength(3);
    expect(body.aggregate.totalReceived).toBe(3);
    expect(body.aggregate.hmacFailRate).toBeCloseTo(1 / 3);
    // 2 persist attempts (hmacOk + recording.completed), 1 failed → 0.5.
    expect(body.aggregate.persistFailRate).toBe(0.5);
    // Newest first — t3 (last in the input) becomes the first in deliveries
    // (the route reverses), and lastReceivedAt comes from deliveries[0].
    expect(body.aggregate.lastReceivedAt).toBe('t3');
  });

  it('returns zero rates when the ring is empty', async () => {
    mockGetRecentDeliveries.mockReturnValue([]);
    const res = await run('GET', '/api/admin/recordings/health');
    expect(res.statusCode).toBe(200);
    const agg = (res.body as { aggregate: { hmacFailRate: number; persistFailRate: number; lastReceivedAt: null } }).aggregate;
    expect(agg.hmacFailRate).toBe(0);
    expect(agg.persistFailRate).toBe(0);
    expect(agg.lastReceivedAt).toBeNull();
  });
});

describe('GET /api/admin/sessions', () => {
  it('returns sessions sorted newest-first with arnIndexExists=false on no redis', async () => {
    mockDocClientSend.mockResolvedValue({
      Items: [
        { sessionId: 's-old', startedAt: '2026-05-01T00:00:00Z', channelArn: 'arn:1', participants: [] },
        { sessionId: 's-new', startedAt: '2026-05-31T00:00:00Z', channelArn: 'arn:2', participants: [{}, {}] },
      ],
    });
    const res = await run('GET', '/api/admin/sessions');
    expect(res.statusCode).toBe(200);
    const body = res.body as { sessions: Array<{ sessionId: string; participants: number; arnIndexExists: boolean }> };
    expect(body.sessions[0].sessionId).toBe('s-new');
    expect(body.sessions[0].participants).toBe(2);
    expect(body.sessions[0].arnIndexExists).toBe(false);
  });

  it('returns an empty list when the DDB scan fails (warn-and-continue)', async () => {
    mockDocClientSend.mockRejectedValue(new Error('DDB down'));
    const res = await run('GET', '/api/admin/sessions');
    expect(res.statusCode).toBe(200);
    expect((res.body as { sessions: unknown[] }).sessions).toEqual([]);
  });
});

describe('GET /api/admin/bindings', () => {
  it('returns unmarshalled bindings rows', async () => {
    mockDdbRawSend.mockResolvedValue({
      Items: [
        { callId: 'c-1', lobbyName: 'lobby-1' },
        { callId: 'c-2', lobbyName: 'lobby-2' },
      ],
    });
    const res = await run('GET', '/api/admin/bindings');
    expect(res.statusCode).toBe(200);
    const body = res.body as { bindings: Array<{ callId: string }>; count: number };
    expect(body.count).toBe(2);
    expect(body.bindings[0].callId).toBe('c-1');
  });

  it('returns empty list when the scan throws (warn-and-continue)', async () => {
    mockDdbRawSend.mockRejectedValue(new Error('access denied'));
    const res = await run('GET', '/api/admin/bindings');
    expect(res.statusCode).toBe(200);
    expect((res.body as { bindings: unknown[]; count: number }).count).toBe(0);
  });
});

describe('GET /api/admin/rooms/:slug', () => {
  it('404 when slug not found', async () => {
    mockDocClientSend.mockResolvedValue({ Items: [] });
    const res = await run('GET', '/api/admin/rooms/missing');
    expect(res.statusCode).toBe(404);
  });

  it('200 with room + members when slug matches', async () => {
    mockDocClientSend.mockResolvedValue({
      Items: [{
        roomId: 'r-1', slug: 'team-room', state: 'active',
        createdAt: 't0', lastActiveAt: 't1',
      }],
    });
    mockListMembers.mockResolvedValue([{ userId: 'u-1' }, { userId: 'u-2' }]);
    const res = await run('GET', '/api/admin/rooms/team-room');
    expect(res.statusCode).toBe(200);
    const body = res.body as {
      room: { roomId: string };
      members: unknown[];
      stats: { memberCount: number };
    };
    expect(body.room.roomId).toBe('r-1');
    expect(body.members).toHaveLength(2);
    expect(body.stats.memberCount).toBe(2);
  });
});

// The gateway reads this when a call ends, with only the lobby name in hand.
describe('GET /api/admin/video/recording-decision', () => {
  it('requires a lobby', async () => {
    const res = await run('GET', '/api/admin/video/recording-decision');
    expect(res.statusCode).toBe(400);
  });

  it('answers null, not 404, for a lobby that never had a call', async () => {
    const res = await run('GET', '/api/admin/video/recording-decision?lobbyName=room%3Aquiet');
    expect(res.statusCode).toBe(200);
    expect(res.body).toEqual({ lobbyName: 'room:quiet', sessionId: null, recording: null });
    expect(mockGetSessionsByDocument).toHaveBeenCalledWith('room:quiet');
  });

  it('returns the decision of the most recent session', async () => {
    mockGetSessionsByDocument.mockResolvedValue([
      { sessionId: 'old', status: 'ended', startedAt: '2026-09-01T00:00:00Z', recording: { enabled: true, reason: 'default-on' } },
      { sessionId: 'new', status: 'ended', startedAt: '2026-09-09T00:00:00Z', recording: { enabled: false, reason: 'room-setting-off' } },
    ]);
    const res = await run('GET', '/api/admin/video/recording-decision?lobbyName=room%3Adesign');
    expect(res.statusCode).toBe(200);
    expect(res.body).toMatchObject({ sessionId: 'new', recording: { enabled: false, reason: 'room-setting-off' } });
  });

  // A row written before decisions existed says nothing, and must not read as "off".
  it('returns recording: null for a row without a decision', async () => {
    mockGetSessionsByDocument.mockResolvedValue([{ sessionId: 's', status: 'ended', startedAt: '2026-09-09T00:00:00Z' }]);
    const res = await run('GET', '/api/admin/video/recording-decision?lobbyName=dm%3Aa%3Ab');
    expect((res.body as { recording: unknown }).recording).toBeNull();
  });
});
