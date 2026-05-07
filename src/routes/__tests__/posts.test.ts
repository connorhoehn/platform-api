// Integration tests for /api/rooms/:roomId/posts and /api/posts routes.
//
// Mocks posts-service at module level so we exercise route param parsing,
// pagination query handling, and error propagation without touching DDB.

import express, { type NextFunction, type Request, type Response } from 'express';

const mockCreatePost = jest.fn();
const mockEditPost = jest.fn();
const mockDeletePost = jest.fn();
const mockListRoomPosts = jest.fn();
const mockListUserPosts = jest.fn();

jest.mock('../../services/posts-service', () => ({
  createPost: mockCreatePost,
  editPost: mockEditPost,
  deletePost: mockDeletePost,
  listRoomPosts: mockListRoomPosts,
  listUserPosts: mockListUserPosts,
}));

jest.mock('../../middleware/require-membership', () => ({
  requireRoomMembership: (_req: Request, _res: Response, next: NextFunction) => next(),
}));

import { postsRouter, userPostsRouter } from '../posts';
import { errorHandler } from '../../middleware/error-handler';
import { ValidationError, NotFoundError, ForbiddenError } from '../../middleware/error-handler';

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
  app.use('/api/rooms/:roomId/posts', postsRouter);
  app.use('/api/posts', userPostsRouter);
  app.use(errorHandler);
  return app;
}

const ROOM = 'room-1';
const BASE = `/api/rooms/${ROOM}/posts`;

beforeEach(() => {
  mockCreatePost.mockReset();
  mockEditPost.mockReset();
  mockDeletePost.mockReset();
  mockListRoomPosts.mockReset();
  mockListUserPosts.mockReset();
});

describe('POST /api/rooms/:roomId/posts', () => {
  it('returns 201 with the created post', async () => {
    const post = { postId: 'p-1', roomId: ROOM, content: 'Hello world' };
    mockCreatePost.mockResolvedValue(post);

    const res = await run(buildApp(), 'POST', BASE, {
      body: { content: 'Hello world' },
    });

    expect(res.statusCode).toBe(201);
    expect(res.body).toEqual(post);
    expect(mockCreatePost).toHaveBeenCalledWith(ROOM, 'tester', 'Hello world');
  });

  it('passes through ValidationError as 400', async () => {
    mockCreatePost.mockRejectedValue(new ValidationError('content is required'));
    const res = await run(buildApp(), 'POST', BASE, { body: {} });
    expect(res.statusCode).toBe(400);
  });
});

describe('PUT /api/rooms/:roomId/posts/:postId', () => {
  it('returns 200 with the updated post', async () => {
    const updated = { postId: 'p-1', content: 'Edited' };
    mockEditPost.mockResolvedValue(updated);

    const res = await run(buildApp(), 'PUT', `${BASE}/p-1`, {
      body: { content: 'Edited' },
    });

    expect(res.statusCode).toBe(200);
    expect(mockEditPost).toHaveBeenCalledWith(ROOM, 'p-1', 'tester', 'Edited');
  });

  it('passes through ForbiddenError as 403', async () => {
    mockEditPost.mockRejectedValue(new ForbiddenError('Not your post'));
    const res = await run(buildApp(), 'PUT', `${BASE}/p-1`, { body: { content: 'x' } });
    expect(res.statusCode).toBe(403);
  });
});

describe('DELETE /api/rooms/:roomId/posts/:postId', () => {
  it('returns 204 on successful deletion', async () => {
    mockDeletePost.mockResolvedValue(undefined);
    const res = await run(buildApp(), 'DELETE', `${BASE}/p-1`);
    expect(res.statusCode).toBe(204);
    expect(mockDeletePost).toHaveBeenCalledWith(ROOM, 'p-1', 'tester');
  });

  it('passes through NotFoundError as 404', async () => {
    mockDeletePost.mockRejectedValue(new NotFoundError('Post not found'));
    const res = await run(buildApp(), 'DELETE', `${BASE}/p-999`);
    expect(res.statusCode).toBe(404);
  });
});

describe('GET /api/rooms/:roomId/posts', () => {
  it('returns 200 with posts and pagination', async () => {
    const result = {
      posts: [{ postId: 'p-1' }, { postId: 'p-2' }],
      nextCursor: 'abc',
    };
    mockListRoomPosts.mockResolvedValue(result);

    const res = await run(buildApp(), 'GET', BASE);
    expect(res.statusCode).toBe(200);
    expect(res.body).toEqual(result);
    expect(mockListRoomPosts).toHaveBeenCalledWith(ROOM, { limit: 20 });
  });

  it('passes limit and cursor query params', async () => {
    mockListRoomPosts.mockResolvedValue({ posts: [], nextCursor: null });
    await run(buildApp(), 'GET', `${BASE}?limit=5&cursor=xyz`);
    expect(mockListRoomPosts).toHaveBeenCalledWith(ROOM, { limit: 5, cursor: 'xyz' });
  });

  it('defaults limit to 20 for invalid values', async () => {
    mockListRoomPosts.mockResolvedValue({ posts: [], nextCursor: null });
    await run(buildApp(), 'GET', `${BASE}?limit=notanumber`);
    expect(mockListRoomPosts).toHaveBeenCalledWith(ROOM, { limit: 20 });
  });
});

describe('GET /api/posts — user posts', () => {
  it('returns posts for a specified userId', async () => {
    mockListUserPosts.mockResolvedValue([{ postId: 'p-1' }]);
    const res = await run(buildApp(), 'GET', '/api/posts?userId=user-abc');
    expect(res.statusCode).toBe(200);
    expect((res.body as { posts: unknown[] }).posts).toHaveLength(1);
    expect(mockListUserPosts).toHaveBeenCalledWith('user-abc');
  });

  it('defaults to authenticated user when userId not specified', async () => {
    mockListUserPosts.mockResolvedValue([]);
    await run(buildApp(), 'GET', '/api/posts', { userId: 'me' });
    expect(mockListUserPosts).toHaveBeenCalledWith('me');
  });
});
