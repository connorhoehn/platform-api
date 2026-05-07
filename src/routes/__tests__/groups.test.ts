// Integration tests for /api/groups routes.
//
// Mocks groupRepo and cache at module level so we exercise validation,
// visibility gating, ownership guards, and error propagation without DDB.

import express, { type NextFunction, type Request, type Response } from 'express';

jest.mock('uuid', () => ({ v4: () => 'test-uuid-1234' }));

const mockCreateGroupWithOwner = jest.fn();
const mockGetGroup = jest.fn();
const mockGetMembership = jest.fn();
const mockDeleteGroup = jest.fn();
const mockUpdateGroupVisibility = jest.fn();

jest.mock('../../repositories', () => ({
  groupRepo: {
    createGroupWithOwner: mockCreateGroupWithOwner,
    getGroup: mockGetGroup,
    getMembership: mockGetMembership,
    deleteGroup: mockDeleteGroup,
    updateGroupVisibility: mockUpdateGroupVisibility,
  },
}));

jest.mock('../../lib/cache', () => ({
  getCachedGroup: jest.fn().mockResolvedValue(null),
  setCachedGroup: jest.fn().mockResolvedValue(undefined),
  invalidateGroupCache: jest.fn().mockResolvedValue(undefined),
}));

import { groupsRouter } from '../groups';
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
  app.use('/api/groups', groupsRouter);
  app.use(errorHandler);
  return app;
}

beforeEach(() => {
  mockCreateGroupWithOwner.mockReset().mockResolvedValue(undefined);
  mockGetGroup.mockReset();
  mockGetMembership.mockReset();
  mockDeleteGroup.mockReset().mockResolvedValue(undefined);
  mockUpdateGroupVisibility.mockReset();
});

describe('POST /api/groups', () => {
  it('returns 201 with group details', async () => {
    const res = await run(buildApp(), 'POST', '/api/groups', {
      body: { name: 'Dev Team', description: 'Developers', visibility: 'public' },
    });
    expect(res.statusCode).toBe(201);
    const body = res.body as { name: string; visibility: string; role: string };
    expect(body.name).toBe('Dev Team');
    expect(body.visibility).toBe('public');
    expect(body.role).toBe('owner');
    expect(mockCreateGroupWithOwner).toHaveBeenCalledTimes(1);
  });

  it('rejects missing name with 400', async () => {
    const res = await run(buildApp(), 'POST', '/api/groups', { body: {} });
    expect(res.statusCode).toBe(400);
  });

  it('rejects name over 100 chars with 400', async () => {
    const res = await run(buildApp(), 'POST', '/api/groups', {
      body: { name: 'x'.repeat(101) },
    });
    expect(res.statusCode).toBe(400);
  });

  it('rejects description over 500 chars with 400', async () => {
    const res = await run(buildApp(), 'POST', '/api/groups', {
      body: { name: 'Team', description: 'x'.repeat(501) },
    });
    expect(res.statusCode).toBe(400);
  });

  it('rejects invalid visibility with 400', async () => {
    const res = await run(buildApp(), 'POST', '/api/groups', {
      body: { name: 'Team', visibility: 'secret' },
    });
    expect(res.statusCode).toBe(400);
  });

  it('returns 409 on TransactionCanceledException', async () => {
    const err = new Error('tx cancelled');
    Object.defineProperty(err, 'constructor', { value: { name: 'TransactionCanceledException' } });
    (err as unknown as { name: string }).name = 'TransactionCanceledException';
    // Use the actual AWS SDK error name
    const { TransactionCanceledException } = jest.requireActual('@aws-sdk/client-dynamodb') as { TransactionCanceledException: new (m: { message: string }) => Error };
    mockCreateGroupWithOwner.mockRejectedValue(new TransactionCanceledException({ message: 'tx' } as never));

    const res = await run(buildApp(), 'POST', '/api/groups', {
      body: { name: 'Team' },
    });
    expect(res.statusCode).toBe(409);
  });
});

describe('GET /api/groups/:groupId', () => {
  it('returns 200 with group and caller role', async () => {
    mockGetGroup.mockResolvedValue({
      groupId: 'g-1', name: 'Team', visibility: 'public', ownerId: 'tester',
    });
    mockGetMembership.mockResolvedValue({ role: 'owner', status: 'active' });

    const res = await run(buildApp(), 'GET', '/api/groups/g-1');
    expect(res.statusCode).toBe(200);
    const body = res.body as { name: string; role: string };
    expect(body.name).toBe('Team');
    expect(body.role).toBe('owner');
  });

  it('returns 404 when group not found', async () => {
    mockGetGroup.mockResolvedValue(null);
    const res = await run(buildApp(), 'GET', '/api/groups/nope');
    expect(res.statusCode).toBe(404);
  });

  it('returns 403 for private group when non-member', async () => {
    mockGetGroup.mockResolvedValue({
      groupId: 'g-1', name: 'Secret', visibility: 'private', ownerId: 'other',
    });
    mockGetMembership.mockResolvedValue(null);

    const res = await run(buildApp(), 'GET', '/api/groups/g-1');
    expect(res.statusCode).toBe(403);
  });

  it('returns null role for public group when non-member', async () => {
    mockGetGroup.mockResolvedValue({
      groupId: 'g-1', name: 'Open', visibility: 'public', ownerId: 'other',
    });
    mockGetMembership.mockResolvedValue(null);

    const res = await run(buildApp(), 'GET', '/api/groups/g-1');
    expect(res.statusCode).toBe(200);
    expect((res.body as { role: string | null }).role).toBeNull();
  });
});

describe('DELETE /api/groups/:groupId', () => {
  it('returns 200 when owner deletes', async () => {
    mockGetGroup.mockResolvedValue({ groupId: 'g-1', ownerId: 'tester' });
    const res = await run(buildApp(), 'DELETE', '/api/groups/g-1');
    expect(res.statusCode).toBe(200);
    expect(mockDeleteGroup).toHaveBeenCalledWith('g-1');
  });

  it('returns 404 when group not found', async () => {
    mockGetGroup.mockResolvedValue(null);
    const res = await run(buildApp(), 'DELETE', '/api/groups/nope');
    expect(res.statusCode).toBe(404);
  });

  it('returns 403 when non-owner deletes', async () => {
    mockGetGroup.mockResolvedValue({ groupId: 'g-1', ownerId: 'someone-else' });
    const res = await run(buildApp(), 'DELETE', '/api/groups/g-1');
    expect(res.statusCode).toBe(403);
  });
});

describe('PATCH /api/groups/:groupId/visibility', () => {
  it('returns 200 with updated group', async () => {
    mockGetGroup.mockResolvedValue({ groupId: 'g-1', ownerId: 'tester', visibility: 'public' });
    mockUpdateGroupVisibility.mockResolvedValue({ groupId: 'g-1', visibility: 'private' });

    const res = await run(buildApp(), 'PATCH', '/api/groups/g-1/visibility', {
      body: { visibility: 'private' },
    });
    expect(res.statusCode).toBe(200);
    expect((res.body as { visibility: string }).visibility).toBe('private');
  });

  it('rejects invalid visibility with 400', async () => {
    const res = await run(buildApp(), 'PATCH', '/api/groups/g-1/visibility', {
      body: { visibility: 'secret' },
    });
    expect(res.statusCode).toBe(400);
  });

  it('returns 403 when non-owner changes visibility', async () => {
    mockGetGroup.mockResolvedValue({ groupId: 'g-1', ownerId: 'someone-else' });
    const res = await run(buildApp(), 'PATCH', '/api/groups/g-1/visibility', {
      body: { visibility: 'private' },
    });
    expect(res.statusCode).toBe(403);
  });
});
