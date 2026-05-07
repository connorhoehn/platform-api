// Integration tests for /api/rooms/:roomId member routes (join, leave,
// list members) and GET /api/rooms (my rooms).
//
// Mocks roomRepo, cache, broadcastService, and publishSocialEvent at module
// level so we exercise membership guards, owner-leave block, and outbox
// wiring without touching DDB or Redis.

import express, { type NextFunction, type Request, type Response } from 'express';

jest.mock('ulid', () => ({ ulid: () => 'test-ulid-1234' }));

const mockGetRoom = jest.fn();
const mockGetMembership = jest.fn();
const mockAddMemberWithOutbox = jest.fn();
const mockRemoveMember = jest.fn();
const mockGetRoomMembers = jest.fn();
const mockGetRoomsByUser = jest.fn();

jest.mock('../../repositories', () => ({
  roomRepo: {
    getRoom: mockGetRoom,
    getMembership: mockGetMembership,
    addMemberWithOutbox: mockAddMemberWithOutbox,
    removeMember: mockRemoveMember,
    getRoomMembers: mockGetRoomMembers,
    getRoomsByUser: mockGetRoomsByUser,
  },
}));

jest.mock('../../lib/cache', () => ({
  getCachedRoom: jest.fn().mockResolvedValue(null),
  setCachedRoom: jest.fn().mockResolvedValue(undefined),
}));

jest.mock('../../services/broadcast', () => ({
  broadcastService: { emit: jest.fn().mockResolvedValue(undefined) },
}));

jest.mock('../../lib/aws-clients', () => ({
  publishSocialEvent: jest.fn().mockResolvedValue(undefined),
}));

import { roomMembersRouter, myRoomsRouter } from '../room-members';
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

const ROOM = 'room-1';

function buildApp(): express.Express {
  const app = express();
  app.use(express.json());
  app.use('/api/rooms/:roomId', roomMembersRouter);
  app.use('/api/rooms', myRoomsRouter);
  app.use(errorHandler);
  return app;
}

beforeEach(() => {
  mockGetRoom.mockReset();
  mockGetMembership.mockReset();
  mockAddMemberWithOutbox.mockReset().mockResolvedValue(undefined);
  mockRemoveMember.mockReset().mockResolvedValue(undefined);
  mockGetRoomMembers.mockReset();
  mockGetRoomsByUser.mockReset();
});

describe('POST /api/rooms/:roomId/join', () => {
  it('returns 201 when joining successfully', async () => {
    mockGetRoom.mockResolvedValue({ roomId: ROOM, channelId: 'ch-1' });
    mockGetMembership.mockResolvedValue(null);

    const res = await run(buildApp(), 'POST', `/api/rooms/${ROOM}/join`);
    expect(res.statusCode).toBe(201);
    const body = res.body as { roomId: string; role: string };
    expect(body.roomId).toBe(ROOM);
    expect(body.role).toBe('member');
    expect(mockAddMemberWithOutbox).toHaveBeenCalledTimes(1);
  });

  it('returns 404 when room not found', async () => {
    mockGetRoom.mockResolvedValue(null);
    const res = await run(buildApp(), 'POST', `/api/rooms/${ROOM}/join`);
    expect(res.statusCode).toBe(404);
  });

  it('returns 409 when already a member', async () => {
    mockGetRoom.mockResolvedValue({ roomId: ROOM, channelId: 'ch-1' });
    mockGetMembership.mockResolvedValue({ role: 'member' });

    const res = await run(buildApp(), 'POST', `/api/rooms/${ROOM}/join`);
    expect(res.statusCode).toBe(409);
  });
});

describe('DELETE /api/rooms/:roomId/leave', () => {
  it('returns 200 when member leaves', async () => {
    mockGetRoom.mockResolvedValue({ roomId: ROOM, channelId: 'ch-1' });
    mockGetMembership.mockResolvedValue({ role: 'member' });

    const res = await run(buildApp(), 'DELETE', `/api/rooms/${ROOM}/leave`);
    expect(res.statusCode).toBe(200);
    expect(mockRemoveMember).toHaveBeenCalledWith(ROOM, 'tester');
  });

  it('returns 404 when room not found', async () => {
    mockGetRoom.mockResolvedValue(null);
    const res = await run(buildApp(), 'DELETE', `/api/rooms/${ROOM}/leave`);
    expect(res.statusCode).toBe(404);
  });

  it('returns 404 when not a member', async () => {
    mockGetRoom.mockResolvedValue({ roomId: ROOM, channelId: 'ch-1' });
    mockGetMembership.mockResolvedValue(null);

    const res = await run(buildApp(), 'DELETE', `/api/rooms/${ROOM}/leave`);
    expect(res.statusCode).toBe(404);
  });

  it('returns 403 when owner tries to leave', async () => {
    mockGetRoom.mockResolvedValue({ roomId: ROOM, channelId: 'ch-1' });
    mockGetMembership.mockResolvedValue({ role: 'owner' });

    const res = await run(buildApp(), 'DELETE', `/api/rooms/${ROOM}/leave`);
    expect(res.statusCode).toBe(403);
  });
});

describe('GET /api/rooms/:roomId/members', () => {
  it('returns 200 with member list', async () => {
    mockGetRoom.mockResolvedValue({ roomId: ROOM });
    mockGetMembership.mockResolvedValue({ role: 'member' });
    mockGetRoomMembers.mockResolvedValue([
      { roomId: ROOM, userId: 'u1', role: 'owner', joinedAt: '2025-01-01' },
      { roomId: ROOM, userId: 'u2', role: 'member', joinedAt: '2025-01-02' },
    ]);

    const res = await run(buildApp(), 'GET', `/api/rooms/${ROOM}/members`);
    expect(res.statusCode).toBe(200);
    expect((res.body as { members: unknown[] }).members).toHaveLength(2);
  });

  it('returns 404 when room not found', async () => {
    mockGetRoom.mockResolvedValue(null);
    const res = await run(buildApp(), 'GET', `/api/rooms/${ROOM}/members`);
    expect(res.statusCode).toBe(404);
  });

  it('returns 403 when non-member requests', async () => {
    mockGetRoom.mockResolvedValue({ roomId: ROOM });
    mockGetMembership.mockResolvedValue(null);

    const res = await run(buildApp(), 'GET', `/api/rooms/${ROOM}/members`);
    expect(res.statusCode).toBe(403);
  });
});

describe('GET /api/rooms — my rooms', () => {
  it('returns 200 with user rooms', async () => {
    mockGetRoomsByUser.mockResolvedValue([
      { roomId: 'r-1', name: 'Room 1' },
      { roomId: 'r-2', name: 'Room 2' },
    ]);

    const res = await run(buildApp(), 'GET', '/api/rooms');
    expect(res.statusCode).toBe(200);
    expect((res.body as { rooms: unknown[] }).rooms).toHaveLength(2);
    expect(mockGetRoomsByUser).toHaveBeenCalledWith('tester');
  });
});
