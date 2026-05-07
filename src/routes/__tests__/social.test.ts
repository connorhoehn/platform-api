// Integration tests for /api/social routes (follow, unfollow, followers,
// following, friends).
//
// Mocks docClient, publishWithOutbox, publishSocialEvent, and profileRepo
// at module level so we exercise validation, mutual-friend intersection,
// and error propagation without touching DDB.

import express, { type NextFunction, type Request, type Response } from 'express';

const mockDocClientSend = jest.fn();
jest.mock('../../lib/aws-clients', () => ({
  docClient: { send: mockDocClientSend },
  publishSocialEvent: jest.fn().mockResolvedValue(undefined),
}));

const mockPublishWithOutbox = jest.fn();
jest.mock('../../services/outbox-publisher', () => ({
  publishWithOutbox: mockPublishWithOutbox,
}));

const mockBatchGetProfiles = jest.fn();
jest.mock('../../repositories', () => ({
  profileRepo: { batchGetProfiles: mockBatchGetProfiles },
}));

import { socialRouter } from '../social';
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
  app.use('/api/social', socialRouter);
  app.use(errorHandler);
  return app;
}

beforeEach(() => {
  mockDocClientSend.mockReset();
  mockPublishWithOutbox.mockReset().mockResolvedValue(undefined);
  mockBatchGetProfiles.mockReset();
});

describe('POST /api/social/follow/:userId', () => {
  it('returns 201 on successful follow', async () => {
    const res = await run(buildApp(), 'POST', '/api/social/follow/user-2');
    expect(res.statusCode).toBe(201);
    expect((res.body as { followeeId: string }).followeeId).toBe('user-2');
    expect(mockPublishWithOutbox).toHaveBeenCalledTimes(1);
  });

  it('rejects self-follow with 400', async () => {
    const res = await run(buildApp(), 'POST', '/api/social/follow/tester');
    expect(res.statusCode).toBe(400);
  });
});

describe('DELETE /api/social/follow/:userId', () => {
  it('returns 200 on successful unfollow', async () => {
    mockDocClientSend.mockResolvedValueOnce({ Item: { followerId: 'tester', followeeId: 'user-2' } });
    mockDocClientSend.mockResolvedValueOnce({});

    const res = await run(buildApp(), 'DELETE', '/api/social/follow/user-2');
    expect(res.statusCode).toBe(200);
  });

  it('returns 404 when not following', async () => {
    mockDocClientSend.mockResolvedValueOnce({ Item: undefined });

    const res = await run(buildApp(), 'DELETE', '/api/social/follow/user-2');
    expect(res.statusCode).toBe(404);
  });
});

describe('GET /api/social/followers', () => {
  it('returns 200 with enriched follower profiles', async () => {
    mockDocClientSend.mockResolvedValueOnce({
      Items: [{ followerId: 'u1', followeeId: 'tester' }],
    });
    mockBatchGetProfiles.mockResolvedValue([
      { userId: 'u1', displayName: 'Alice', avatarUrl: '', visibility: 'public' },
    ]);

    const res = await run(buildApp(), 'GET', '/api/social/followers');
    expect(res.statusCode).toBe(200);
    expect((res.body as { followers: unknown[] }).followers).toHaveLength(1);
  });

  it('returns empty array when no followers', async () => {
    mockDocClientSend.mockResolvedValueOnce({ Items: [] });

    const res = await run(buildApp(), 'GET', '/api/social/followers');
    expect(res.statusCode).toBe(200);
    expect((res.body as { followers: unknown[] }).followers).toHaveLength(0);
  });
});

describe('GET /api/social/following', () => {
  it('returns 200 with enriched following profiles', async () => {
    mockDocClientSend.mockResolvedValueOnce({
      Items: [{ followerId: 'tester', followeeId: 'u2' }],
    });
    mockBatchGetProfiles.mockResolvedValue([
      { userId: 'u2', displayName: 'Bob', avatarUrl: '', visibility: 'public' },
    ]);

    const res = await run(buildApp(), 'GET', '/api/social/following');
    expect(res.statusCode).toBe(200);
    expect((res.body as { following: unknown[] }).following).toHaveLength(1);
  });
});

describe('GET /api/social/friends', () => {
  it('returns mutual follows only', async () => {
    mockDocClientSend
      .mockResolvedValueOnce({ Items: [{ followeeId: 'u1' }, { followeeId: 'u2' }] })
      .mockResolvedValueOnce({ Items: [{ followerId: 'u1' }] });
    mockBatchGetProfiles.mockResolvedValue([
      { userId: 'u1', displayName: 'Alice', avatarUrl: '', visibility: 'public' },
    ]);

    const res = await run(buildApp(), 'GET', '/api/social/friends');
    expect(res.statusCode).toBe(200);
    const body = res.body as { friends: { userId: string }[] };
    expect(body.friends).toHaveLength(1);
    expect(body.friends[0].userId).toBe('u1');
  });

  it('returns empty when no mutual follows', async () => {
    mockDocClientSend
      .mockResolvedValueOnce({ Items: [{ followeeId: 'u1' }] })
      .mockResolvedValueOnce({ Items: [] });

    const res = await run(buildApp(), 'GET', '/api/social/friends');
    expect(res.statusCode).toBe(200);
    expect((res.body as { friends: unknown[] }).friends).toHaveLength(0);
  });
});
