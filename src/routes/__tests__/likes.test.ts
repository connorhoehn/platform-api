// Integration tests for /api/rooms/:roomId/posts/:postId/likes and
// .../comments/:commentId/likes routes.
//
// Mocks the likes-service at module level so we exercise route param parsing,
// middleware wiring, and error propagation without touching DDB.

import express, { type NextFunction, type Request, type Response } from 'express';

const mockLikePost = jest.fn();
const mockUnlikePost = jest.fn();
const mockListPostLikes = jest.fn();
const mockLikeComment = jest.fn();
const mockUnlikeComment = jest.fn();

jest.mock('../../services/likes-service', () => ({
  likePost: mockLikePost,
  unlikePost: mockUnlikePost,
  listPostLikes: mockListPostLikes,
  likeComment: mockLikeComment,
  unlikeComment: mockUnlikeComment,
}));

jest.mock('../../middleware/require-membership', () => ({
  requireRoomMembership: (_req: Request, _res: Response, next: NextFunction) => next(),
}));

import { postLikesRouter, commentLikesRouter } from '../likes';
import { errorHandler } from '../../middleware/error-handler';
import { ConflictError, NotFoundError } from '../../middleware/error-handler';

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
  app.use('/api/rooms/:roomId/posts/:postId/likes', postLikesRouter);
  app.use('/api/rooms/:roomId/posts/:postId/comments/:commentId/likes', commentLikesRouter);
  app.use(errorHandler);
  return app;
}

const ROOM = 'room-1';
const POST = 'post-1';
const POST_BASE = `/api/rooms/${ROOM}/posts/${POST}/likes`;
const COMMENT = 'comment-1';
const COMMENT_BASE = `/api/rooms/${ROOM}/posts/${POST}/comments/${COMMENT}/likes`;

beforeEach(() => {
  mockLikePost.mockReset();
  mockUnlikePost.mockReset();
  mockListPostLikes.mockReset();
  mockLikeComment.mockReset();
  mockUnlikeComment.mockReset();
});

describe('POST /api/rooms/:roomId/posts/:postId/likes', () => {
  it('returns 201 with the like record', async () => {
    const record = { targetId: `post:${POST}`, userId: 'tester' };
    mockLikePost.mockResolvedValue(record);

    const res = await run(buildApp(), 'POST', POST_BASE);
    expect(res.statusCode).toBe(201);
    expect(res.body).toEqual(record);
    expect(mockLikePost).toHaveBeenCalledWith(ROOM, POST, 'tester');
  });

  it('passes through ConflictError as 409', async () => {
    mockLikePost.mockRejectedValue(new ConflictError('Already liked'));
    const res = await run(buildApp(), 'POST', POST_BASE);
    expect(res.statusCode).toBe(409);
  });
});

describe('DELETE /api/rooms/:roomId/posts/:postId/likes', () => {
  it('returns 204 on successful unlike', async () => {
    mockUnlikePost.mockResolvedValue(undefined);
    const res = await run(buildApp(), 'DELETE', POST_BASE);
    expect(res.statusCode).toBe(204);
    expect(mockUnlikePost).toHaveBeenCalledWith(POST, 'tester');
  });

  it('passes through NotFoundError as 404', async () => {
    mockUnlikePost.mockRejectedValue(new NotFoundError('Like not found'));
    const res = await run(buildApp(), 'DELETE', POST_BASE);
    expect(res.statusCode).toBe(404);
  });
});

describe('GET /api/rooms/:roomId/posts/:postId/likes', () => {
  it('returns 200 with liked-by list', async () => {
    const result = { count: 2, likedBy: [{ userId: 'u1', displayName: 'Alice' }, { userId: 'u2', displayName: 'Bob' }] };
    mockListPostLikes.mockResolvedValue(result);

    const res = await run(buildApp(), 'GET', POST_BASE);
    expect(res.statusCode).toBe(200);
    expect(res.body).toEqual(result);
    expect(mockListPostLikes).toHaveBeenCalledWith(ROOM, POST);
  });
});

describe('POST /api/rooms/.../comments/:commentId/likes', () => {
  it('returns 201 with the like record', async () => {
    const record = { targetId: `comment:${COMMENT}`, userId: 'tester' };
    mockLikeComment.mockResolvedValue(record);

    const res = await run(buildApp(), 'POST', COMMENT_BASE);
    expect(res.statusCode).toBe(201);
    expect(mockLikeComment).toHaveBeenCalledWith(ROOM, POST, COMMENT, 'tester');
  });
});

describe('DELETE /api/rooms/.../comments/:commentId/likes', () => {
  it('returns 204 on successful unlike', async () => {
    mockUnlikeComment.mockResolvedValue(undefined);
    const res = await run(buildApp(), 'DELETE', COMMENT_BASE);
    expect(res.statusCode).toBe(204);
    expect(mockUnlikeComment).toHaveBeenCalledWith(COMMENT, 'tester');
  });
});
