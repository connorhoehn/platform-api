// Integration tests for /api/documents/:documentId/comments routes.
//
// Mocks documentCommentRepo, broadcastService, and auth middleware at module
// level so we exercise validation, CRUD, and resolve/unresolve without DDB.

import express, { type NextFunction, type Request, type Response } from 'express';

const mockCreateComment = jest.fn();
const mockGetCommentsForSection = jest.fn();
const mockGetCommentsForDocument = jest.fn();
const mockResolveThread = jest.fn();
const mockUnresolveThread = jest.fn();
const mockDeleteComment = jest.fn();

jest.mock('../../repositories', () => ({
  documentCommentRepo: {
    createComment: mockCreateComment,
    getCommentsForSection: mockGetCommentsForSection,
    getCommentsForDocument: mockGetCommentsForDocument,
    resolveThread: mockResolveThread,
    unresolveThread: mockUnresolveThread,
    deleteComment: mockDeleteComment,
  },
}));

jest.mock('../../services/broadcast', () => ({
  broadcastService: { emit: jest.fn().mockResolvedValue(undefined) },
}));

jest.mock('../../middleware/auth', () => ({
  requireAuth: (_req: Request, _res: Response, next: NextFunction) => next(),
  optionalAuth: (_req: Request, _res: Response, next: NextFunction) => next(),
}));

import { documentCommentsRouter } from '../documentComments';
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
    user: { sub: opts.userId ?? 'tester', email: 'tester@example.com' },
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

const DOC = 'doc-1';
const BASE = `/api/documents/${DOC}/comments`;

function buildApp(): express.Express {
  const app = express();
  app.use(express.json());
  app.use('/api/documents', documentCommentsRouter);
  app.use(errorHandler);
  return app;
}

beforeEach(() => {
  mockCreateComment.mockReset();
  mockGetCommentsForSection.mockReset();
  mockGetCommentsForDocument.mockReset();
  mockResolveThread.mockReset().mockResolvedValue(undefined);
  mockUnresolveThread.mockReset().mockResolvedValue(undefined);
  mockDeleteComment.mockReset().mockResolvedValue(undefined);
});

describe('POST /api/documents/:documentId/comments', () => {
  it('returns 201 with created comment', async () => {
    const comment = { commentId: 'c-1', sectionId: 's-1', text: 'Looks good' };
    mockCreateComment.mockResolvedValue(comment);

    const res = await run(buildApp(), 'POST', BASE, {
      body: { sectionId: 's-1', text: 'Looks good' },
    });
    expect(res.statusCode).toBe(201);
    expect((res.body as { comment: typeof comment }).comment).toEqual(comment);
  });

  it('creates threaded reply with parentCommentId', async () => {
    mockCreateComment.mockResolvedValue({ commentId: 'c-2' });

    await run(buildApp(), 'POST', BASE, {
      body: { sectionId: 's-1', text: 'Reply', parentCommentId: 'c-1' },
    });
    expect(mockCreateComment).toHaveBeenCalledWith(
      expect.objectContaining({ parentCommentId: 'c-1' }),
    );
  });

  it('rejects missing sectionId with 400', async () => {
    const res = await run(buildApp(), 'POST', BASE, {
      body: { text: 'No section' },
    });
    expect(res.statusCode).toBe(400);
  });

  it('rejects missing text with 400', async () => {
    const res = await run(buildApp(), 'POST', BASE, {
      body: { sectionId: 's-1' },
    });
    expect(res.statusCode).toBe(400);
  });

  it('rejects text over 10000 chars with 400', async () => {
    const res = await run(buildApp(), 'POST', BASE, {
      body: { sectionId: 's-1', text: 'x'.repeat(10001) },
    });
    expect(res.statusCode).toBe(400);
  });
});

describe('GET /api/documents/:documentId/comments', () => {
  it('returns all comments for document', async () => {
    mockGetCommentsForDocument.mockResolvedValue({
      items: [{ commentId: 'c-1' }, { commentId: 'c-2' }],
    });

    const res = await run(buildApp(), 'GET', BASE);
    expect(res.statusCode).toBe(200);
    expect((res.body as { comments: unknown[] }).comments).toHaveLength(2);
    expect(mockGetCommentsForDocument).toHaveBeenCalledWith(DOC, 50);
  });

  it('filters by sectionId when provided', async () => {
    mockGetCommentsForSection.mockResolvedValue({ items: [{ commentId: 'c-1' }] });

    const res = await run(buildApp(), 'GET', `${BASE}?sectionId=s-1`);
    expect(res.statusCode).toBe(200);
    expect(mockGetCommentsForSection).toHaveBeenCalledWith('s-1', 50);
  });
});

describe('PATCH /api/documents/:documentId/comments/:commentId', () => {
  it('resolves thread — returns 200', async () => {
    const res = await run(buildApp(), 'PATCH', `${BASE}/c-1`, {
      body: { resolved: true },
    });
    expect(res.statusCode).toBe(200);
    expect(mockResolveThread).toHaveBeenCalledTimes(1);
  });

  it('unresolves thread — returns 200', async () => {
    const res = await run(buildApp(), 'PATCH', `${BASE}/c-1`, {
      body: { resolved: false },
    });
    expect(res.statusCode).toBe(200);
    expect(mockUnresolveThread).toHaveBeenCalledTimes(1);
  });

  it('rejects non-boolean resolved with 400', async () => {
    const res = await run(buildApp(), 'PATCH', `${BASE}/c-1`, {
      body: { resolved: 'yes' },
    });
    expect(res.statusCode).toBe(400);
  });
});

describe('DELETE /api/documents/:documentId/comments/:commentId', () => {
  it('returns 204 on successful deletion', async () => {
    const res = await run(buildApp(), 'DELETE', `${BASE}/c-1`);
    expect(res.statusCode).toBe(204);
    expect(mockDeleteComment).toHaveBeenCalledWith(DOC, 'c-1');
  });
});
