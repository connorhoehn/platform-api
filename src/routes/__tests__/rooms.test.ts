// Integration tests for /api/rooms routes.
//
// Mocks roomRepo, cache, and docClient at module level so we exercise
// route param parsing, DM mutual-friend gating, DmExistsError custom
// response shape, and validation without touching DDB.

import express, { type NextFunction, type Request, type Response } from 'express';

jest.mock('uuid', () => ({ v4: () => 'test-uuid-1234' }));

const mockCreateRoom = jest.fn();
const mockCreateRoomConditional = jest.fn();
const mockAddMember = jest.fn();

jest.mock('../../repositories', () => ({
  roomRepo: {
    createRoom: mockCreateRoom,
    createRoomConditional: mockCreateRoomConditional,
    addMember: mockAddMember,
  },
}));

jest.mock('../../lib/cache', () => ({
  setCachedRoom: jest.fn().mockResolvedValue(undefined),
}));

const mockDocClientSend = jest.fn();
jest.mock('../../lib/aws-clients', () => ({
  docClient: { send: mockDocClientSend },
}));

import { roomsRouter } from '../rooms';
import { errorHandler } from '../../middleware/error-handler';

interface MockRes {
  statusCode: number;
  body: unknown;
  ended: boolean;
  finished: Promise<void>;
  status(code: number): MockRes;
  json(body: unknown): MockRes;
  send(body?: unknown): MockRes;
  setHeader(_n: string, _v: unknown): MockRes;
  getHeader(_n: string): unknown;
  end(...args: unknown[]): MockRes;
}

function mockRes(): MockRes {
  let resolve!: () => void;
  const finished = new Promise<void>((r) => { resolve = r; });
  const r: MockRes = {
    statusCode: 200, body: undefined, ended: false, finished,
    status(code) { this.statusCode = code; return this; },
    json(body) { this.body = body; this.ended = true; resolve(); return this; },
    send(body) { this.body = body; this.ended = true; resolve(); return this; },
    setHeader() { return this; },
    getHeader() { return undefined; },
    end(..._args) { this.ended = true; resolve(); return this; },
  };
  return r;
}

interface RunOpts { body?: unknown; userId?: string }

async function run(
  app: express.Express,
  method: string,
  url: string,
  opts: RunOpts = {},
): Promise<MockRes> {
  const res = mockRes();
  const qIdx = url.indexOf('?');
  const query: Record<string, string> = {};
  if (qIdx >= 0) {
    const params = new URLSearchParams(url.slice(qIdx + 1));
    for (const [k, v] of params.entries()) query[k] = v;
  }
  const req = {
    method: method.toUpperCase(),
    url,
    originalUrl: url,
    path: qIdx >= 0 ? url.slice(0, qIdx) : url,
    headers: { 'content-type': 'application/json' },
    query,
    body: opts.body,
    user: { sub: opts.userId ?? 'tester' },
    params: {},
    ip: '127.0.0.1',
  } as unknown as Request;
  await new Promise<void>((resolveOuter, reject) => {
    const finalHandler = (err?: unknown): void => {
      if (err) { reject(err); return; }
      if (!res.ended) res.status(404).json({ error: 'route not found' });
      resolveOuter();
    };
    (app as unknown as (req: Request, res: Response, next: NextFunction) => void)(
      req, res as unknown as Response, finalHandler,
    );
    res.finished.then(() => resolveOuter()).catch(reject);
  });
  return res;
}

function buildApp(): express.Express {
  const app = express();
  app.use(express.json());
  app.use('/api/rooms', roomsRouter);
  app.use(errorHandler);
  return app;
}

function setupMutualFriends(callerId: string, targetId: string): void {
  mockDocClientSend
    .mockResolvedValueOnce({ Items: [{ followerId: callerId, followeeId: targetId }] })
    .mockResolvedValueOnce({ Item: { followerId: targetId, followeeId: callerId } });
}

beforeEach(() => {
  mockCreateRoom.mockReset().mockResolvedValue(undefined);
  mockCreateRoomConditional.mockReset().mockResolvedValue(undefined);
  mockAddMember.mockReset().mockResolvedValue(undefined);
  mockDocClientSend.mockReset();
});

describe('POST /api/rooms — standalone room', () => {
  it('returns 201 with room details', async () => {
    const res = await run(buildApp(), 'POST', '/api/rooms', {
      body: { name: 'My Room' },
    });
    expect(res.statusCode).toBe(201);
    const body = res.body as { roomId: string; name: string; type: string; ownerId: string };
    expect(body.name).toBe('My Room');
    expect(body.type).toBe('standalone');
    expect(body.ownerId).toBe('tester');
    expect(body.roomId).toBeDefined();
    expect(mockCreateRoom).toHaveBeenCalledTimes(1);
    expect(mockAddMember).toHaveBeenCalledTimes(1);
  });

  it('rejects missing name with 400', async () => {
    const res = await run(buildApp(), 'POST', '/api/rooms', { body: {} });
    expect(res.statusCode).toBe(400);
  });

  it('rejects empty name with 400', async () => {
    const res = await run(buildApp(), 'POST', '/api/rooms', {
      body: { name: '' },
    });
    expect(res.statusCode).toBe(400);
  });

  it('rejects name over 100 chars with 400', async () => {
    const res = await run(buildApp(), 'POST', '/api/rooms', {
      body: { name: 'x'.repeat(101) },
    });
    expect(res.statusCode).toBe(400);
  });
});

describe('POST /api/rooms/dm — DM room', () => {
  it('returns 201 when mutual friends', async () => {
    setupMutualFriends('tester', 'friend-1');

    const res = await run(buildApp(), 'POST', '/api/rooms/dm', {
      body: { targetUserId: 'friend-1' },
    });
    expect(res.statusCode).toBe(201);
    const body = res.body as { roomId: string; type: string; dmPeerUserId: string };
    expect(body.type).toBe('dm');
    expect(body.dmPeerUserId).toBe('friend-1');
    expect(body.roomId).toContain('dm#');
    expect(mockCreateRoomConditional).toHaveBeenCalledTimes(1);
    expect(mockAddMember).toHaveBeenCalledTimes(2);
  });

  it('rejects missing targetUserId with 400', async () => {
    const res = await run(buildApp(), 'POST', '/api/rooms/dm', { body: {} });
    expect(res.statusCode).toBe(400);
  });

  it('rejects DM with self with 400', async () => {
    const res = await run(buildApp(), 'POST', '/api/rooms/dm', {
      body: { targetUserId: 'tester' },
    });
    expect(res.statusCode).toBe(400);
  });

  it('returns 403 when caller does not follow target', async () => {
    mockDocClientSend
      .mockResolvedValueOnce({ Items: [] })
      .mockResolvedValueOnce({ Item: { followerId: 'friend-1', followeeId: 'tester' } });

    const res = await run(buildApp(), 'POST', '/api/rooms/dm', {
      body: { targetUserId: 'friend-1' },
    });
    expect(res.statusCode).toBe(403);
  });

  it('returns 403 when target does not follow caller', async () => {
    mockDocClientSend
      .mockResolvedValueOnce({ Items: [{ followerId: 'tester', followeeId: 'friend-1' }] })
      .mockResolvedValueOnce({ Item: undefined });

    const res = await run(buildApp(), 'POST', '/api/rooms/dm', {
      body: { targetUserId: 'friend-1' },
    });
    expect(res.statusCode).toBe(403);
  });

  it('returns 409 with roomId when DM already exists', async () => {
    setupMutualFriends('tester', 'friend-1');
    mockCreateRoomConditional.mockRejectedValue(
      Object.assign(new Error('Condition not met'), { name: 'ConditionalCheckFailedException' }),
    );

    const res = await run(buildApp(), 'POST', '/api/rooms/dm', {
      body: { targetUserId: 'friend-1' },
    });
    expect(res.statusCode).toBe(409);
    const body = res.body as { error: string; roomId: string };
    expect(body.roomId).toContain('dm#');
    expect(body.error).toMatch(/already exists/i);
  });

  it('generates deterministic roomId regardless of caller order', async () => {
    setupMutualFriends('aaa', 'zzz');
    const res1 = await run(buildApp(), 'POST', '/api/rooms/dm', {
      body: { targetUserId: 'zzz' },
      userId: 'aaa',
    });

    mockCreateRoomConditional.mockReset().mockResolvedValue(undefined);
    mockAddMember.mockReset().mockResolvedValue(undefined);
    setupMutualFriends('zzz', 'aaa');
    const res2 = await run(buildApp(), 'POST', '/api/rooms/dm', {
      body: { targetUserId: 'aaa' },
      userId: 'zzz',
    });

    const id1 = (res1.body as { roomId: string }).roomId;
    const id2 = (res2.body as { roomId: string }).roomId;
    expect(id1).toBe(id2);
  });
});
