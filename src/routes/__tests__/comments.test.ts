// Integration tests for /api/rooms/:roomId/posts/:postId/comments routes.
//
// Mocks the comments-service at module level so we exercise route param
// parsing, middleware wiring, and error propagation without touching DDB.

import express, { type NextFunction, type Request, type Response } from 'express';

const mockCreateComment = jest.fn();
const mockListComments = jest.fn();
const mockDeleteComment = jest.fn();

jest.mock('../../services/comments-service', () => ({
  createComment: mockCreateComment,
  listComments: mockListComments,
  deleteComment: mockDeleteComment,
}));

jest.mock('../../middleware/require-membership', () => ({
  requireRoomMembership: (_req: Request, _res: Response, next: NextFunction) => next(),
}));

import { commentsRouter } from '../comments';
import { errorHandler } from '../../middleware/error-handler';
import { NotFoundError, ForbiddenError } from '../../middleware/error-handler';

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
  app.use('/api/rooms/:roomId/posts/:postId/comments', commentsRouter);
  app.use(errorHandler);
  return app;
}

const ROOM = 'room-1';
const POST = 'post-1';
const BASE = `/api/rooms/${ROOM}/posts/${POST}/comments`;

beforeEach(() => {
  mockCreateComment.mockReset();
  mockListComments.mockReset();
  mockDeleteComment.mockReset();
});

describe('POST /api/rooms/:roomId/posts/:postId/comments', () => {
  it('returns 201 with the created comment', async () => {
    const comment = { commentId: 'c-1', postId: POST, userId: 'tester', content: 'Great post!' };
    mockCreateComment.mockResolvedValue(comment);

    const res = await run(buildApp(), 'POST', BASE, {
      body: { content: 'Great post!' },
    });

    expect(res.statusCode).toBe(201);
    expect(res.body).toEqual(comment);
    expect(mockCreateComment).toHaveBeenCalledWith(ROOM, POST, 'tester', { content: 'Great post!' });
  });

  it('passes parentCommentId for reply comments', async () => {
    mockCreateComment.mockResolvedValue({ commentId: 'c-2' });

    await run(buildApp(), 'POST', BASE, {
      body: { content: 'Reply', parentCommentId: 'c-1' },
    });

    expect(mockCreateComment).toHaveBeenCalledWith(ROOM, POST, 'tester', {
      content: 'Reply',
      parentCommentId: 'c-1',
    });
  });
});

describe('GET /api/rooms/:roomId/posts/:postId/comments', () => {
  it('returns 200 with the comments array', async () => {
    const comments = [
      { commentId: 'c-1', content: 'First' },
      { commentId: 'c-2', content: 'Second' },
    ];
    mockListComments.mockResolvedValue(comments);

    const res = await run(buildApp(), 'GET', BASE);

    expect(res.statusCode).toBe(200);
    expect((res.body as { comments: unknown[] }).comments).toHaveLength(2);
    expect(mockListComments).toHaveBeenCalledWith(ROOM, POST);
  });

  it('returns 200 with empty array when no comments exist', async () => {
    mockListComments.mockResolvedValue([]);
    const res = await run(buildApp(), 'GET', BASE);
    expect(res.statusCode).toBe(200);
    expect((res.body as { comments: unknown[] }).comments).toHaveLength(0);
  });
});

describe('DELETE /api/rooms/:roomId/posts/:postId/comments/:commentId', () => {
  it('returns 204 on successful deletion', async () => {
    mockDeleteComment.mockResolvedValue(undefined);
    const res = await run(buildApp(), 'DELETE', `${BASE}/c-1`);
    expect(res.statusCode).toBe(204);
    expect(mockDeleteComment).toHaveBeenCalledWith(POST, 'c-1', 'tester');
  });

  it('passes through NotFoundError as 404', async () => {
    mockDeleteComment.mockRejectedValue(new NotFoundError('Comment not found'));
    const res = await run(buildApp(), 'DELETE', `${BASE}/c-999`);
    expect(res.statusCode).toBe(404);
  });

  it('passes through ForbiddenError as 403', async () => {
    mockDeleteComment.mockRejectedValue(new ForbiddenError('Not your comment'));
    const res = await run(buildApp(), 'DELETE', `${BASE}/c-1`);
    expect(res.statusCode).toBe(403);
  });
});
