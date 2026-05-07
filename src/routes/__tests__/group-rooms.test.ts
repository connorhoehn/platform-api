// Integration tests for /api/groups/:groupId/rooms routes.
//
// Mocks roomRepo, groupRepo, and cache at module level. Exercises
// group-admin guard, name validation, and room creation.

import express, { type NextFunction, type Request, type Response } from 'express';

jest.mock('uuid', () => ({ v4: () => 'test-uuid-1234' }));

const mockCreateRoom = jest.fn();
const mockAddMember = jest.fn();

jest.mock('../../repositories', () => ({
  roomRepo: {
    createRoom: mockCreateRoom,
    addMember: mockAddMember,
  },
  groupRepo: {
    getGroup: jest.fn(),
    getMembership: jest.fn(),
  },
}));

jest.mock('../../lib/cache', () => ({
  setCachedRoom: jest.fn().mockResolvedValue(undefined),
}));

import { groupRoomsRouter } from '../group-rooms';
import { errorHandler } from '../../middleware/error-handler';
import { groupRepo } from '../../repositories';

const mockGetGroup = groupRepo.getGroup as jest.Mock;
const mockGetMembership = groupRepo.getMembership as jest.Mock;

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

const GROUP = 'group-1';
const BASE = `/api/groups/${GROUP}/rooms`;

function buildApp(): express.Express {
  const app = express();
  app.use(express.json());
  app.use('/api/groups/:groupId/rooms', groupRoomsRouter);
  app.use(errorHandler);
  return app;
}

beforeEach(() => {
  mockCreateRoom.mockReset().mockResolvedValue(undefined);
  mockAddMember.mockReset().mockResolvedValue(undefined);
  mockGetGroup.mockReset();
  mockGetMembership.mockReset();
});

describe('POST /api/groups/:groupId/rooms', () => {
  it('returns 201 when group owner creates room', async () => {
    mockGetGroup.mockResolvedValue({ groupId: GROUP });
    mockGetMembership.mockResolvedValue({ role: 'owner', status: 'active' });

    const res = await run(buildApp(), 'POST', BASE, {
      body: { name: 'General' },
    });
    expect(res.statusCode).toBe(201);
    const body = res.body as { name: string; type: string; groupId: string };
    expect(body.name).toBe('General');
    expect(body.type).toBe('group');
    expect(body.groupId).toBe(GROUP);
    expect(mockCreateRoom).toHaveBeenCalledTimes(1);
    expect(mockAddMember).toHaveBeenCalledTimes(1);
  });

  it('returns 201 when admin creates room', async () => {
    mockGetGroup.mockResolvedValue({ groupId: GROUP });
    mockGetMembership.mockResolvedValue({ role: 'admin', status: 'active' });

    const res = await run(buildApp(), 'POST', BASE, {
      body: { name: 'Design' },
    });
    expect(res.statusCode).toBe(201);
  });

  it('rejects missing name with 400', async () => {
    const res = await run(buildApp(), 'POST', BASE, { body: {} });
    expect(res.statusCode).toBe(400);
  });

  it('rejects name over 100 chars with 400', async () => {
    const res = await run(buildApp(), 'POST', BASE, {
      body: { name: 'x'.repeat(101) },
    });
    expect(res.statusCode).toBe(400);
  });

  it('returns 404 when group not found', async () => {
    mockGetGroup.mockResolvedValue(null);
    const res = await run(buildApp(), 'POST', BASE, {
      body: { name: 'Room' },
    });
    expect(res.statusCode).toBe(404);
  });

  it('returns 403 for regular member', async () => {
    mockGetGroup.mockResolvedValue({ groupId: GROUP });
    mockGetMembership.mockResolvedValue({ role: 'member', status: 'active' });

    const res = await run(buildApp(), 'POST', BASE, {
      body: { name: 'Room' },
    });
    expect(res.statusCode).toBe(403);
  });

  it('returns 403 for invited-only user', async () => {
    mockGetGroup.mockResolvedValue({ groupId: GROUP });
    mockGetMembership.mockResolvedValue({ role: 'owner', status: 'invited' });

    const res = await run(buildApp(), 'POST', BASE, {
      body: { name: 'Room' },
    });
    expect(res.statusCode).toBe(403);
  });
});
