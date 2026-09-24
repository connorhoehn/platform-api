// Integration tests for /api/documents/:documentId/comments routes.
//
// Mocks documentCommentRepo, broadcastService, and auth middleware at module
// level so we exercise validation, CRUD, and resolve/unresolve without DDB.

import express, { type NextFunction, type Request, type Response } from 'express';

const mockCreateComment = jest.fn();
const mockGetComment = jest.fn();
const mockEditComment = jest.fn();
const mockGetCommentsForSection = jest.fn();
const mockGetCommentsForDocument = jest.fn();
const mockResolveThread = jest.fn();
const mockUnresolveThread = jest.fn();
const mockDeleteComment = jest.fn();

jest.mock('../../repositories', () => ({
  documentCommentRepo: {
    createComment: mockCreateComment,
    getComment: mockGetComment,
    editComment: mockEditComment,
    getCommentsForSection: mockGetCommentsForSection,
    getCommentsForDocument: mockGetCommentsForDocument,
    resolveThread: mockResolveThread,
    unresolveThread: mockUnresolveThread,
    deleteComment: mockDeleteComment,
  },
}));

jest.mock('../../lib/event-catalog', () => ({
  catalog: {
    declare: jest.fn(),
    publish: jest.fn().mockResolvedValue(undefined),
    getSignalTransport: jest.fn(),
    getDurableTransport: jest.fn(),
    connect: jest.fn().mockResolvedValue(undefined),
    disconnect: jest.fn().mockResolvedValue(undefined),
  },
  publishSignal: jest.fn().mockResolvedValue(undefined),
  publishDurable: jest.fn().mockResolvedValue(undefined),
  publishWithOutbox: jest.fn().mockResolvedValue(undefined),
  connectCatalog: jest.fn().mockResolvedValue(undefined),
  disconnectCatalog: jest.fn().mockResolvedValue(undefined),
}));

jest.mock('../../middleware/auth', () => ({
  requireAuth: (_req: Request, _res: Response, next: NextFunction) => next(),
  optionalAuth: (_req: Request, _res: Response, next: NextFunction) => next(),
}));

// NFR #154: the reader gate, over an in-memory document row per test.
const mockDocRow = jest.fn();
const mockLifecycleOwner = jest.fn();
let mockGate: import('../../documents/documentReadGate').DocumentReadGate;
jest.mock('../../documents/documentReadGate', () => {
  const actual = jest.requireActual('../../documents/documentReadGate');
  return { ...actual, documentReadGate: () => mockGate };
});

import { documentCommentsRouter } from '../documentComments';
import { makeDocumentReadGate } from '../../documents/documentReadGate';
import { errorHandler } from '../../middleware/error-handler';
import { createDocumentPolicy } from '../../middleware/document-policy';
import type { DocumentGrantClaims } from '@connorhoehn/realtime-modules/server';

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

interface RunOpts { body?: unknown; userId?: string; anonymous?: boolean }

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
    headers: opts.anonymous ? { 'content-type': 'application/json' } : { 'content-type': 'application/json', authorization: 'Bearer test-grant' },
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

function buildApp(grant?: DocumentGrantClaims): express.Express {
  const app = express();
  app.use(express.json());
  if (grant) app.use('/api/documents/:documentId', createDocumentPolicy({ tenantId: 'assessment', verify: async () => grant }));
  app.use('/api/documents', documentCommentsRouter);
  app.use(errorHandler);
  return app;
}

beforeEach(() => {
  // tester and other read the document through `viewers`; doc-owner owns it.
  mockDocRow.mockReset().mockResolvedValue({ createdBy: 'doc-owner', viewers: ['tester', 'other'] });
  mockLifecycleOwner.mockReset().mockResolvedValue(undefined);
  mockGate = makeDocumentReadGate({ getDocumentRow: mockDocRow, getLifecycleOwner: mockLifecycleOwner });
  mockCreateComment.mockReset();
  mockGetComment.mockReset().mockResolvedValue({ documentId: DOC, commentId: 'c-1', userId: 'tester' });
  mockEditComment.mockReset().mockResolvedValue({ documentId: DOC, commentId: 'c-1', text: 'Edited' });
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

  it('rejects a comment that says nothing about WHERE it points', async () => {
    // No section, no anchor, no parent: there is no answer to "what is this
    // comment about?", so there is nowhere to render it.
    const res = await run(buildApp(), 'POST', BASE, {
      body: { text: 'No section' },
    });
    expect(res.statusCode).toBe(400);
  });

  // -------------------------------------------------------------------------
  // Anchors — a canvas document has no sections, so the position IS the anchor
  // -------------------------------------------------------------------------

  /** Shaped like the real thing: base64 of an encoded Y.js relative position. */
  const ANCHOR = {
    v: 1,
    key: 'body',
    start: 'AQIDBAU=',
    end: 'BQQDAgE=',
    quote: 'the cache expired for every key at once',
  };

  it('accepts an anchored comment with NO sectionId', async () => {
    mockCreateComment.mockResolvedValue({ commentId: 'c-3', anchor: ANCHOR });

    const res = await run(buildApp(), 'POST', BASE, {
      body: { text: 'Was this the thundering herd?', anchor: ANCHOR },
    });
    expect(res.statusCode).toBe(201);
    expect(mockCreateComment).toHaveBeenCalledWith(expect.objectContaining({ anchor: ANCHOR }));
    // The absence is the point: a synthetic section id would be a position the
    // gutter would then try, and fail, to measure against.
    expect(mockCreateComment.mock.calls[0][0]).not.toHaveProperty('sectionId');
  });

  it('stores no anchor when none is sent — the pre-anchor comment still works', async () => {
    mockCreateComment.mockResolvedValue({ commentId: 'c-4' });

    const res = await run(buildApp(), 'POST', BASE, {
      body: { sectionId: 's-1', text: 'Looks good' },
    });
    expect(res.statusCode).toBe(201);
    expect(mockCreateComment.mock.calls[0][0]).not.toHaveProperty('anchor');
  });

  it('accepts a reply with neither section nor anchor — the thread positions it', async () => {
    mockCreateComment.mockResolvedValue({ commentId: 'c-5' });

    const res = await run(buildApp(), 'POST', BASE, {
      body: { text: 'Agreed', parentCommentId: 'c-3' },
    });
    expect(res.statusCode).toBe(201);
  });

  it.each([
    ['a missing version', { ...ANCHOR, v: undefined }],
    ['a zero version', { ...ANCHOR, v: 0 }],
    ['a missing root key', { ...ANCHOR, key: '' }],
    ['a missing start', { ...ANCHOR, start: '' }],
    ['a missing end', { ...ANCHOR, end: undefined }],
    ['a non-string quote', { ...ANCHOR, quote: 42 }],
    ['an oversized position', { ...ANCHOR, start: 'A'.repeat(2049) }],
    ['an array', []],
  ])('rejects an anchor with %s', async (_label, anchor) => {
    const res = await run(buildApp(), 'POST', BASE, {
      body: { text: 'Bad anchor', anchor },
    });
    expect(res.statusCode).toBe(400);
  });

  it('truncates an oversized quote rather than refusing the comment', async () => {
    mockCreateComment.mockResolvedValue({ commentId: 'c-6' });

    await run(buildApp(), 'POST', BASE, {
      body: { text: 'Long selection', anchor: { ...ANCHOR, quote: 'x'.repeat(900) } },
    });
    const sent = mockCreateComment.mock.calls[0][0] as { anchor: { quote: string } };
    expect(sent.anchor.quote).toHaveLength(500);
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
    expect(mockGetCommentsForDocument).toHaveBeenCalledWith(DOC, 50, undefined);
  });

  it('filters by sectionId when provided', async () => {
    mockGetCommentsForSection.mockResolvedValue({ items: [{ commentId: 'c-1' }] });

    const res = await run(buildApp(), 'GET', `${BASE}?sectionId=s-1`);
    expect(res.statusCode).toBe(200);
    expect(mockGetCommentsForSection).toHaveBeenCalledWith(DOC, 's-1', 50, undefined);
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


describe('document-wide thread compatibility', () => {
  it('creates an explicit document-wide root without fabricating a section or anchor', async () => {
    mockCreateComment.mockImplementation(async (comment) => ({ ...comment, commentId: 'new' }));
    const res = await run(buildApp(), 'POST', BASE, { body: { scope: 'document', text: 'General feedback' } });
    expect(res.statusCode).toBe(201);
    expect(mockCreateComment).toHaveBeenCalledWith(expect.objectContaining({ documentId: DOC, scope: 'document' }));
    expect(mockCreateComment.mock.calls[0][0]).not.toHaveProperty('sectionId');
    expect(mockCreateComment.mock.calls[0][0]).not.toHaveProperty('anchor');
  });
  it('refuses ambiguous document and span scope', async () => {
    const res = await run(buildApp(), 'POST', BASE, { body: { scope: 'document', sectionId: 's', text: 'Feedback' } });
    expect(res.statusCode).toBe(400);
    expect(mockCreateComment).not.toHaveBeenCalled();
  });
  it('refuses a parent belonging to another document', async () => {
    mockGetComment.mockResolvedValue(null);
    const res = await run(buildApp(), 'POST', BASE, { body: { parentCommentId: 'other-doc-comment', text: 'Reply' } });
    expect(res.statusCode).toBe(404);
    expect(mockGetComment).toHaveBeenCalledWith(DOC, 'other-doc-comment');
    expect(mockCreateComment).not.toHaveBeenCalled();
  });
  it('allows the author to edit text', async () => {
    const res = await run(buildApp(), 'PATCH', BASE + '/c-1', { body: { text: ' Edited ' } });
    expect(res.statusCode).toBe(200);
    expect(mockEditComment).toHaveBeenCalledWith(DOC, 'c-1', 'Edited');
  });
  it.each(['PATCH', 'DELETE'])('refuses another author %s', async (method) => {
    const res = await run(buildApp(), method, BASE + '/c-1', { body: { text: 'Edited' }, userId: 'other' });
    expect(res.statusCode).toBe(403);
    expect(mockEditComment).not.toHaveBeenCalled();
    expect(mockDeleteComment).not.toHaveBeenCalled();
  });
  it.each(['PATCH', 'DELETE'])('refuses a missing comment %s rather than creating a phantom record', async (method) => {
    mockGetComment.mockResolvedValue(null);
    const res = await run(buildApp(), method, BASE + '/missing', { body: { resolved: true } });
    expect(res.statusCode).toBe(404);
    expect(mockResolveThread).not.toHaveBeenCalled();
    expect(mockDeleteComment).not.toHaveBeenCalled();
  });
});


describe('mounted comment routes with document grants', () => {
  const grant = { documentId: DOC, tenantId: 'assessment', sub: 'tester', operations: ['read'] } as DocumentGrantClaims;
  it('allows a reader to list but refuses the same reader creating a comment', async () => {
    mockGetCommentsForDocument.mockResolvedValue({ items: [] });
    const app = buildApp(grant);
    expect((await run(app, 'GET', BASE)).statusCode).toBe(200);
    expect((await run(app, 'POST', BASE, { body: { scope: 'document', text: 'Unauthorised' } })).statusCode).toBe(403);
    expect(mockCreateComment).not.toHaveBeenCalled();
  });
  it('refuses a document-scoped token on another document even with a shared section id', async () => {
    const res = await run(buildApp(grant), 'GET', '/api/documents/other/comments?sectionId=s-1');
    expect(res.statusCode).toBe(403);
    expect(mockGetCommentsForSection).not.toHaveBeenCalled();
  });
  it('rejects a cursor for another document', async () => {
    const cursor = Buffer.from(JSON.stringify({ documentId: 'other', commentId: 'c' })).toString('base64url');
    const res = await run(buildApp(grant), 'GET', BASE + '?cursor=' + cursor);
    expect(res.statusCode).toBe(400);
    expect(mockGetCommentsForDocument).not.toHaveBeenCalled();
  });
});

describe('NFR #154: the comment routes ask document:read', () => {
  const post = { body: { scope: 'document', text: 'Looks good' } };
  beforeEach(() => {
    mockGetCommentsForDocument.mockResolvedValue({ items: [{ commentId: 'c-1' }] });
    mockCreateComment.mockResolvedValue({ commentId: 'c-new' });
  });

  it.each(['doc-owner', 'tester'])('%s (owner / reader) lists 200 and posts 201', async (userId) => {
    const app = buildApp();
    expect((await run(app, 'GET', BASE, { userId })).statusCode).toBe(200);
    expect((await run(app, 'POST', BASE, { ...post, userId })).statusCode).toBe(201);
  });

  it('a stranger gets 404 on every comment route and nothing is read or written', async () => {
    const app = buildApp();
    for (const [method, url] of [['GET', BASE], ['POST', BASE], ['PATCH', BASE + '/c-1'], ['DELETE', BASE + '/c-1']]) {
      const res = await run(app, method, url, { userId: 'mallory', body: method === 'PATCH' ? { resolved: true } : post.body });
      expect([method, res.statusCode]).toEqual([method, 404]);
    }
    expect(mockGetCommentsForDocument).not.toHaveBeenCalled();
    expect(mockCreateComment).not.toHaveBeenCalled();
    expect(mockGetComment).not.toHaveBeenCalled();
    expect(mockResolveThread).not.toHaveBeenCalled();
    expect(mockDeleteComment).not.toHaveBeenCalled();
  });

  it('a missing document is the same 404; a lifecycle-only document belongs to its lifecycle owner', async () => {
    mockDocRow.mockResolvedValue(null);
    expect((await run(buildApp(), 'GET', BASE, { userId: 'tester' })).statusCode).toBe(404);
    mockLifecycleOwner.mockResolvedValue('tester');
    expect((await run(buildApp(), 'GET', BASE, { userId: 'tester' })).statusCode).toBe(200);
  });

  it.each([['GET', BASE], ['POST', BASE], ['PATCH', BASE + '/c-1'], ['DELETE', BASE + '/c-1']])('anonymous %s is 401', async (method, url) => {
    const res = await run(buildApp(), method, url, { anonymous: true, body: method === 'PATCH' ? { resolved: true } : post.body });
    expect(res.statusCode).toBe(401);
    expect(mockDocRow).not.toHaveBeenCalled();
  });

  it('the document owner may delete a reader\'s comment but not rewrite it; another reader may do neither', async () => {
    mockGetComment.mockResolvedValue({ documentId: DOC, commentId: 'c-1', userId: 'tester' });
    const app = buildApp();
    expect((await run(app, 'PATCH', BASE + '/c-1', { userId: 'doc-owner', body: { text: 'Rewritten' } })).statusCode).toBe(403);
    expect((await run(app, 'DELETE', BASE + '/c-1', { userId: 'other' })).statusCode).toBe(403);
    expect(mockDeleteComment).not.toHaveBeenCalled();
    expect((await run(app, 'DELETE', BASE + '/c-1', { userId: 'doc-owner' })).statusCode).toBe(204);
    expect(mockDeleteComment).toHaveBeenCalledWith(DOC, 'c-1');
  });

  it('a reader may resolve a thread', async () => {
    expect((await run(buildApp(), 'PATCH', BASE + '/c-1', { userId: 'other', body: { resolved: true } })).statusCode).toBe(200);
  });

  it('an allow is cached per user and document; a refusal is not, so a fresh share opens at once', async () => {
    const app = buildApp();
    await run(app, 'GET', BASE, { userId: 'tester' });
    await run(app, 'POST', BASE, { ...post, userId: 'tester' });
    await run(app, 'GET', BASE, { userId: 'tester' });
    expect(mockDocRow).toHaveBeenCalledTimes(1);
    expect((await run(app, 'GET', BASE, { userId: 'erin' })).statusCode).toBe(404);
    mockDocRow.mockResolvedValue({ createdBy: 'doc-owner', viewers: ['tester', 'erin'] });
    expect((await run(app, 'GET', BASE, { userId: 'erin' })).statusCode).toBe(200);
  });

  it('a scoped document grant was checked by the policy middleware and is not re-checked', async () => {
    const grant = { documentId: DOC, tenantId: 'assessment', sub: 'mallory', operations: ['read'] } as DocumentGrantClaims;
    expect((await run(buildApp(grant), 'GET', BASE, { userId: 'mallory' })).statusCode).toBe(200);
    expect(mockDocRow).not.toHaveBeenCalled();
  });
});
