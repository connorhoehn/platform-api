// Integration tests for /api/rooms/:roomId/posts/:postId/reactions routes.
//
// Mocks the reactions-service at module level so we exercise route param
// parsing, middleware wiring, and error propagation without touching DDB.

import express, { type NextFunction, type Request, type Response } from 'express';

const mockAddReaction = jest.fn();
const mockRemoveReaction = jest.fn();

jest.mock('../../services/reactions-service', () => ({
  addReaction: mockAddReaction,
  removeReaction: mockRemoveReaction,
}));

jest.mock('../../middleware/require-membership', () => ({
  requireRoomMembership: (_req: Request, _res: Response, next: NextFunction) => next(),
}));

import { reactionsRouter } from '../reactions';
import { errorHandler } from '../../middleware/error-handler';
import { ValidationError, NotFoundError } from '../../middleware/error-handler';

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
  app.use('/api/rooms/:roomId/posts/:postId', reactionsRouter);
  app.use(errorHandler);
  return app;
}

const ROOM = 'room-1';
const POST = 'post-1';
const BASE = `/api/rooms/${ROOM}/posts/${POST}/reactions`;

beforeEach(() => {
  mockAddReaction.mockReset();
  mockRemoveReaction.mockReset();
});

describe('POST /api/rooms/:roomId/posts/:postId/reactions', () => {
  it('returns 201 with the reaction record on success', async () => {
    const record = {
      targetId: `post:${POST}:reaction`,
      userId: 'tester',
      type: 'reaction',
      emoji: '❤️',
      createdAt: '2026-05-01T00:00:00Z',
    };
    mockAddReaction.mockResolvedValue(record);

    const res = await run(buildApp(), 'POST', BASE, {
      body: { emoji: '❤️' },
    });

    expect(res.statusCode).toBe(201);
    expect(res.body).toEqual(record);
    expect(mockAddReaction).toHaveBeenCalledWith(ROOM, POST, 'tester', '❤️');
  });

  it('passes through ValidationError from service as 400', async () => {
    mockAddReaction.mockRejectedValue(new ValidationError('Invalid emoji. Must be one of the 12 supported types'));
    const res = await run(buildApp(), 'POST', BASE, {
      body: { emoji: 'nope' },
    });
    expect(res.statusCode).toBe(400);
  });

  it('passes through NotFoundError from service as 404', async () => {
    mockAddReaction.mockRejectedValue(new NotFoundError('Post not found'));
    const res = await run(buildApp(), 'POST', BASE, {
      body: { emoji: '❤️' },
    });
    expect(res.statusCode).toBe(404);
  });
});

describe('DELETE /api/rooms/:roomId/posts/:postId/reactions/:emoji', () => {
  it('returns 204 on successful removal', async () => {
    mockRemoveReaction.mockResolvedValue(undefined);
    const emoji = encodeURIComponent('🚀');
    const res = await run(buildApp(), 'DELETE', `${BASE}/${emoji}`);
    expect(res.statusCode).toBe(204);
    expect(mockRemoveReaction).toHaveBeenCalledWith(POST, 'tester', '🚀');
  });

  it('URL-decodes emoji param before passing to service', async () => {
    mockRemoveReaction.mockResolvedValue(undefined);
    const emoji = encodeURIComponent('🔥');
    await run(buildApp(), 'DELETE', `${BASE}/${emoji}`);
    expect(mockRemoveReaction).toHaveBeenCalledWith(POST, 'tester', '🔥');
  });

  it('passes through NotFoundError from service as 404', async () => {
    mockRemoveReaction.mockRejectedValue(new NotFoundError('Reaction not found'));
    const emoji = encodeURIComponent('❤️');
    const res = await run(buildApp(), 'DELETE', `${BASE}/${emoji}`);
    expect(res.statusCode).toBe(404);
  });
});
