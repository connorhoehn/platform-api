// Integration tests for /api/groups/:groupId member routes (invite, accept/
// decline, join, leave, list members).
//
// Mocks groupRepo at module level so we exercise route param parsing,
// role/status guards, and error propagation without touching DDB.

import express, { type NextFunction, type Request, type Response } from 'express';

const mockGetGroup = jest.fn();
const mockGetMembership = jest.fn();
const mockAddMember = jest.fn();
const mockUpdateMemberStatus = jest.fn();
const mockRemoveMember = jest.fn();
const mockGetGroupMembers = jest.fn();

jest.mock('../../repositories', () => ({
  groupRepo: {
    getGroup: mockGetGroup,
    getMembership: mockGetMembership,
    addMember: mockAddMember,
    updateMemberStatus: mockUpdateMemberStatus,
    removeMember: mockRemoveMember,
    getGroupMembers: mockGetGroupMembers,
  },
}));

import { groupMembersRouter } from '../group-members';
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

const GROUP = 'group-1';
const BASE = `/api/groups/${GROUP}`;

function buildApp(): express.Express {
  const app = express();
  app.use(express.json());
  app.use('/api/groups/:groupId', groupMembersRouter);
  app.use(errorHandler);
  return app;
}

beforeEach(() => {
  mockGetGroup.mockReset();
  mockGetMembership.mockReset();
  mockAddMember.mockReset().mockResolvedValue(undefined);
  mockUpdateMemberStatus.mockReset().mockResolvedValue(undefined);
  mockRemoveMember.mockReset().mockResolvedValue(undefined);
  mockGetGroupMembers.mockReset();
});

describe('POST /api/groups/:groupId/invite', () => {
  it('returns 201 when admin invites user', async () => {
    mockGetGroup.mockResolvedValue({ groupId: GROUP });
    mockGetMembership
      .mockResolvedValueOnce({ role: 'owner', status: 'active' })
      .mockResolvedValueOnce(null);

    const res = await run(buildApp(), 'POST', `${BASE}/invite`, {
      body: { userId: 'invitee' },
    });
    expect(res.statusCode).toBe(201);
    expect(mockAddMember).toHaveBeenCalledTimes(1);
  });

  it('rejects missing userId with 400', async () => {
    const res = await run(buildApp(), 'POST', `${BASE}/invite`, { body: {} });
    expect(res.statusCode).toBe(400);
  });

  it('rejects self-invite with 400', async () => {
    const res = await run(buildApp(), 'POST', `${BASE}/invite`, {
      body: { userId: 'tester' },
    });
    expect(res.statusCode).toBe(400);
  });

  it('returns 404 when group not found', async () => {
    mockGetGroup.mockResolvedValue(null);
    const res = await run(buildApp(), 'POST', `${BASE}/invite`, {
      body: { userId: 'invitee' },
    });
    expect(res.statusCode).toBe(404);
  });

  it('returns 403 when non-admin invites', async () => {
    mockGetGroup.mockResolvedValue({ groupId: GROUP });
    mockGetMembership.mockResolvedValueOnce({ role: 'member', status: 'active' });

    const res = await run(buildApp(), 'POST', `${BASE}/invite`, {
      body: { userId: 'invitee' },
    });
    expect(res.statusCode).toBe(403);
  });

  it('returns 409 when target is already active member', async () => {
    mockGetGroup.mockResolvedValue({ groupId: GROUP });
    mockGetMembership
      .mockResolvedValueOnce({ role: 'owner', status: 'active' })
      .mockResolvedValueOnce({ status: 'active' });

    const res = await run(buildApp(), 'POST', `${BASE}/invite`, {
      body: { userId: 'invitee' },
    });
    expect(res.statusCode).toBe(409);
  });
});

describe('POST /api/groups/:groupId/invitations/:action', () => {
  it('accepts invitation — returns 200', async () => {
    mockGetGroup.mockResolvedValue({ groupId: GROUP });
    mockGetMembership.mockResolvedValue({ status: 'invited' });

    const res = await run(buildApp(), 'POST', `${BASE}/invitations/accept`);
    expect(res.statusCode).toBe(200);
    expect(mockUpdateMemberStatus).toHaveBeenCalledTimes(1);
  });

  it('declines invitation — returns 200', async () => {
    mockGetGroup.mockResolvedValue({ groupId: GROUP });
    mockGetMembership.mockResolvedValue({ status: 'invited' });

    const res = await run(buildApp(), 'POST', `${BASE}/invitations/decline`);
    expect(res.statusCode).toBe(200);
    expect(mockRemoveMember).toHaveBeenCalledTimes(1);
  });

  it('rejects invalid action with 400', async () => {
    const res = await run(buildApp(), 'POST', `${BASE}/invitations/maybe`);
    expect(res.statusCode).toBe(400);
  });

  it('returns 404 when no pending invitation', async () => {
    mockGetGroup.mockResolvedValue({ groupId: GROUP });
    mockGetMembership.mockResolvedValue(null);

    const res = await run(buildApp(), 'POST', `${BASE}/invitations/accept`);
    expect(res.statusCode).toBe(404);
  });
});

describe('POST /api/groups/:groupId/join', () => {
  it('returns 201 for public group', async () => {
    mockGetGroup.mockResolvedValue({ groupId: GROUP, visibility: 'public' });
    mockGetMembership.mockResolvedValue(null);

    const res = await run(buildApp(), 'POST', `${BASE}/join`);
    expect(res.statusCode).toBe(201);
    expect(mockAddMember).toHaveBeenCalledTimes(1);
  });

  it('returns 403 for private group', async () => {
    mockGetGroup.mockResolvedValue({ groupId: GROUP, visibility: 'private' });

    const res = await run(buildApp(), 'POST', `${BASE}/join`);
    expect(res.statusCode).toBe(403);
  });

  it('returns 409 when already a member', async () => {
    mockGetGroup.mockResolvedValue({ groupId: GROUP, visibility: 'public' });
    mockGetMembership.mockResolvedValue({ status: 'active' });

    const res = await run(buildApp(), 'POST', `${BASE}/join`);
    expect(res.statusCode).toBe(409);
  });
});

describe('DELETE /api/groups/:groupId/leave', () => {
  it('returns 200 for regular member leaving', async () => {
    mockGetGroup.mockResolvedValue({ groupId: GROUP });
    mockGetMembership.mockResolvedValue({ role: 'member', status: 'active' });

    const res = await run(buildApp(), 'DELETE', `${BASE}/leave`);
    expect(res.statusCode).toBe(200);
    expect(mockRemoveMember).toHaveBeenCalledWith(GROUP, 'tester');
  });

  it('returns 403 when owner tries to leave', async () => {
    mockGetGroup.mockResolvedValue({ groupId: GROUP });
    mockGetMembership.mockResolvedValue({ role: 'owner', status: 'active' });

    const res = await run(buildApp(), 'DELETE', `${BASE}/leave`);
    expect(res.statusCode).toBe(403);
  });

  it('returns 404 when not a member', async () => {
    mockGetGroup.mockResolvedValue({ groupId: GROUP });
    mockGetMembership.mockResolvedValue(null);

    const res = await run(buildApp(), 'DELETE', `${BASE}/leave`);
    expect(res.statusCode).toBe(404);
  });
});

describe('GET /api/groups/:groupId/members', () => {
  it('returns 200 with member list', async () => {
    mockGetGroup.mockResolvedValue({ groupId: GROUP, visibility: 'public' });
    mockGetMembership.mockResolvedValue({ role: 'member', status: 'active' });
    mockGetGroupMembers.mockResolvedValue([
      { userId: 'u1', role: 'owner', joinedAt: '2025-01-01' },
      { userId: 'u2', role: 'member', joinedAt: '2025-01-02' },
    ]);

    const res = await run(buildApp(), 'GET', `${BASE}/members`);
    expect(res.statusCode).toBe(200);
    expect((res.body as { members: unknown[] }).members).toHaveLength(2);
  });

  it('returns 403 for private group when non-member', async () => {
    mockGetGroup.mockResolvedValue({ groupId: GROUP, visibility: 'private' });
    mockGetMembership.mockResolvedValue(null);

    const res = await run(buildApp(), 'GET', `${BASE}/members`);
    expect(res.statusCode).toBe(403);
  });

  it('returns 404 when group not found', async () => {
    mockGetGroup.mockResolvedValue(null);
    const res = await run(buildApp(), 'GET', `${BASE}/members`);
    expect(res.statusCode).toBe(404);
  });
});
