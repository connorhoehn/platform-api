/**
 * Route-level tests for named review requests.
 *
 * Same in-memory-fake approach as documentLifecycle.test.ts: the point of
 * these routes is authorization ("only the owner may appoint reviewers",
 * "only the addressed reviewer may respond"), and stubbed method mocks would
 * not catch a check that stopped running.
 */

import express, { type NextFunction, type Request, type Response } from 'express';
import type { DocumentReviewRequest } from '../../repositories/DocumentReviewRequestRepository';
import {
  buildRequestKey,
  isReviewOutstanding,
  isOverdue,
  DOC_SCOPE,
  WITHDRAWN_RETENTION_DAYS,
} from '../../repositories/DocumentReviewRequestRepository';

const lifecycleRows = new Map<string, Record<string, unknown>>();
const requestRows = new Map<string, DocumentReviewRequest>();
const mockEmit = jest.fn().mockResolvedValue(undefined);

const rowKey = (d: string, k: string): string => `${d}::${k}`;

const fakeLifecycleRepo = {
  async get(documentId: string): Promise<unknown | null> {
    const raw = lifecycleRows.get(documentId);
    return raw ? { ...raw, history: [], version: 1 } : null;
  },
  async initialize(): Promise<null> { return null; },
  async applyTransition(): Promise<never> { throw new Error('not used'); },
};

const fakeRequestRepo = {
  async createRequest(input: {
    documentId: string; reviewerId: string; reviewerName?: string;
    requestedBy: string; requestedByName?: string; sectionId?: string;
    dueDate?: string; note?: string; required?: boolean;
  }): Promise<DocumentReviewRequest> {
    const requestKey = buildRequestKey(input.reviewerId, input.sectionId);
    const now = new Date().toISOString();
    const existing = requestRows.get(rowKey(input.documentId, requestKey));
    const item: DocumentReviewRequest = {
      documentId: input.documentId,
      requestKey,
      requestId: existing?.requestId ?? `req-${requestRows.size + 1}`,
      scope: input.sectionId ? 'section' : 'document',
      ...(input.sectionId !== undefined ? { sectionId: input.sectionId } : {}),
      reviewerId: input.reviewerId,
      ...(input.reviewerName !== undefined ? { reviewerName: input.reviewerName } : {}),
      requestedBy: input.requestedBy,
      ...(input.requestedByName !== undefined ? { requestedByName: input.requestedByName } : {}),
      state: 'pending',
      required: input.required ?? true,
      ...(input.dueDate !== undefined ? { dueDate: input.dueDate } : {}),
      ...(input.note !== undefined ? { note: input.note } : {}),
      createdAt: existing?.createdAt ?? now,
      updatedAt: now,
    };
    requestRows.set(rowKey(input.documentId, requestKey), item);
    return item;
  },
  async getRequest(d: string, k: string): Promise<DocumentReviewRequest | null> {
    return requestRows.get(rowKey(d, k)) ?? null;
  },
  async getRequestsForDocument(d: string): Promise<DocumentReviewRequest[]> {
    return [...requestRows.values()].filter((r) => r.documentId === d);
  },
  async getRequestsForSection(d: string, s: string): Promise<DocumentReviewRequest[]> {
    return [...requestRows.values()].filter(
      (r) => r.documentId === d && r.requestKey.startsWith(`${s}:`),
    );
  },
  async getRequestsForReviewer(): Promise<DocumentReviewRequest[]> { return []; },
  async setState(p: {
    documentId: string; requestKey: string; state: string; actor: string; comment?: string;
  }): Promise<DocumentReviewRequest | null> {
    const key = rowKey(p.documentId, p.requestKey);
    const existing = requestRows.get(key);
    if (!existing) return null;
    const at = new Date().toISOString();
    const next: DocumentReviewRequest = {
      ...existing,
      state: p.state as DocumentReviewRequest['state'],
      updatedAt: at,
      respondedAt: at,
      ...(p.comment !== undefined ? { comment: p.comment } : {}),
      ...(p.state === 'withdrawn'
        ? { ttl: Math.floor(Date.parse(at) / 1000 + WITHDRAWN_RETENTION_DAYS * 86400) }
        : {}),
    };
    requestRows.set(key, next);
    return next;
  },
  async setDueDate(
    d: string, k: string, dueDate: string | null,
  ): Promise<DocumentReviewRequest | null> {
    const existing = requestRows.get(rowKey(d, k));
    if (!existing) return null;
    const next = { ...existing, updatedAt: new Date().toISOString() };
    if (dueDate === null) delete next.dueDate;
    else next.dueDate = dueDate;
    requestRows.set(rowKey(d, k), next);
    return next;
  },
};

let currentUser: string | undefined = 'alice';

jest.mock('../../middleware/auth', () => ({
  requireAuth: (req: Request, res: Response, next: NextFunction) => {
    if (!currentUser) { res.status(401).json({ error: 'Authorization required' }); return; }
    (req as Request & { user?: { sub: string } }).user = { sub: currentUser };
    next();
  },
  optionalAuth: (req: Request, _res: Response, next: NextFunction) => {
    if (currentUser) (req as Request & { user?: { sub: string } }).user = { sub: currentUser };
    next();
  },
}));

jest.mock('../../repositories', () => ({
  get documentLifecycleRepo() { return fakeLifecycleRepo; },
  get documentReviewRequestRepo() { return fakeRequestRepo; },
  typedDocumentRepo: { get: jest.fn().mockResolvedValue(null) },
  profileRepo: {
    getProfile: jest.fn(async (id: string) => ({ displayName: `Name(${id})` })),
  },
  sectionReviewRepo: { getUserReviews: jest.fn().mockResolvedValue([]) },
}));

jest.mock('../../lib/event-catalog', () => ({
  catalog: { declare: jest.fn(), publishSignal: jest.fn() },
  publishSignal: mockEmit,
  publishDurable: jest.fn().mockResolvedValue(undefined),
  publishWithOutbox: jest.fn().mockResolvedValue(undefined),
  connectCatalog: jest.fn().mockResolvedValue(undefined),
  disconnectCatalog: jest.fn().mockResolvedValue(undefined),
}));

import {
  documentReviewRequestsRouter,
  sectionReviewRequestsRouter,
} from '../documentReviewRequests';
import { errorHandler } from '../../middleware/error-handler';

interface MockRes {
  statusCode: number; body: unknown; ended: boolean; finished: Promise<void>;
  status(c: number): MockRes; json(b: unknown): MockRes; send(b?: unknown): MockRes;
  setHeader(n: string, v: unknown): MockRes; getHeader(n: string): unknown;
  end(...a: unknown[]): MockRes;
}

function mockRes(): MockRes {
  let resolve!: () => void;
  const finished = new Promise<void>((r) => { resolve = r; });
  const r: MockRes = {
    statusCode: 200, body: undefined, ended: false, finished,
    status(c) { this.statusCode = c; return this; },
    json(b) { this.body = b; this.ended = true; resolve(); return this; },
    send(b) { this.body = b; this.ended = true; resolve(); return this; },
    setHeader() { return this; },
    getHeader() { return undefined; },
    end() { this.ended = true; resolve(); return this; },
  };
  return r;
}

function buildApp(): express.Express {
  const app = express();
  app.use(express.json());
  app.use('/api/documents/:documentId/review-requests', documentReviewRequestsRouter);
  app.use(
    '/api/documents/:documentId/sections/:sectionId/review-requests',
    sectionReviewRequestsRouter,
  );
  app.use(errorHandler);
  return app;
}

async function call(
  method: string, url: string,
  opts: { body?: unknown; as?: string | undefined } = {},
): Promise<MockRes> {
  if ('as' in opts) currentUser = opts.as;
  const app = buildApp();
  const res = mockRes();
  const qIdx = url.indexOf('?');
  const query: Record<string, string> = {};
  if (qIdx >= 0) {
    for (const [k, v] of new URLSearchParams(url.slice(qIdx + 1)).entries()) query[k] = v;
  }
  const req = {
    method: method.toUpperCase(), url, originalUrl: url,
    path: qIdx >= 0 ? url.slice(0, qIdx) : url,
    headers: { 'content-type': 'application/json' },
    query, body: opts.body, params: {}, ip: '127.0.0.1',
  } as unknown as Request;
  await new Promise<void>((resolveOuter, reject) => {
    const finalHandler = (err?: unknown): void => {
      if (err) { reject(err); return; }
      if (!res.ended) res.status(404).json({ error: 'route not found' });
      resolveOuter();
    };
    (app as unknown as (q: Request, s: Response, n: NextFunction) => void)(
      req, res as unknown as Response, finalHandler,
    );
    res.finished.then(() => resolveOuter()).catch(reject);
  });
  return res;
}

const DOC = 'doc-1';

function seedOwner(ownerId = 'alice', status = 'draft'): void {
  const now = new Date().toISOString();
  lifecycleRows.set(DOC, {
    documentId: DOC, status, ownerId,
    createdAt: now, updatedAt: now, updatedBy: ownerId,
  });
}

beforeEach(() => {
  lifecycleRows.clear();
  requestRows.clear();
  mockEmit.mockClear();
  currentUser = 'alice';
  seedOwner();
});

describe('POST /api/documents/:documentId/review-requests', () => {
  it('the owner creates a document-scoped request with a due date', async () => {
    const res = await call('POST', `/api/documents/${DOC}/review-requests`, {
      as: 'alice',
      body: { reviewerId: 'bob', dueDate: '2030-03-01T00:00:00.000Z', note: 'rollback plan?' },
    });
    expect(res.statusCode).toBe(201);
    const req = (res.body as { request: DocumentReviewRequest }).request;
    expect(req.requestKey).toBe(`${DOC_SCOPE}:bob`);
    expect(req.scope).toBe('document');
    expect(req.state).toBe('pending');
    expect(req.required).toBe(true);
    expect(req.dueDate).toBe('2030-03-01T00:00:00.000Z');
    expect(req.reviewerName).toBe('Name(bob)');
  });

  it('a non-owner CANNOT appoint a reviewer (403)', async () => {
    // Otherwise anybody could appoint themselves and then approve.
    const res = await call('POST', `/api/documents/${DOC}/review-requests`, {
      as: 'mallory', body: { reviewerId: 'mallory' },
    });
    expect(res.statusCode).toBe(403);
    expect(requestRows.size).toBe(0);
  });

  it('requires reviewerId (400)', async () => {
    const res = await call('POST', `/api/documents/${DOC}/review-requests`, {
      as: 'alice', body: {},
    });
    expect(res.statusCode).toBe(400);
  });

  it('rejects a non-ISO dueDate (400)', async () => {
    const res = await call('POST', `/api/documents/${DOC}/review-requests`, {
      as: 'alice', body: { reviewerId: 'bob', dueDate: 'next tuesday' },
    });
    expect(res.statusCode).toBe(400);
  });

  it('supports optional (non-blocking) requests', async () => {
    const res = await call('POST', `/api/documents/${DOC}/review-requests`, {
      as: 'alice', body: { reviewerId: 'bob', required: false },
    });
    expect((res.body as { request: DocumentReviewRequest }).request.required).toBe(false);
  });

  it('is idempotent on (document, section, reviewer) — re-requesting resets to pending', async () => {
    await call('POST', `/api/documents/${DOC}/review-requests`, {
      as: 'alice', body: { reviewerId: 'bob' },
    });
    await call('PATCH', `/api/documents/${DOC}/review-requests/${DOC_SCOPE}:bob`, {
      as: 'bob', body: { state: 'approved' },
    });
    const res = await call('POST', `/api/documents/${DOC}/review-requests`, {
      as: 'alice', body: { reviewerId: 'bob', dueDate: '2030-05-05T00:00:00.000Z' },
    });
    expect(requestRows.size).toBe(1);
    expect((res.body as { request: DocumentReviewRequest }).request.state).toBe('pending');
  });

  it('emits doc:review_requested', async () => {
    await call('POST', `/api/documents/${DOC}/review-requests`, {
      as: 'alice', body: { reviewerId: 'bob' },
    });
    expect(mockEmit).toHaveBeenCalledWith(
      `doc:${DOC}`, 'doc:review_requested',
      expect.objectContaining({ documentId: DOC }),
    );
  });

  it('unauthenticated callers get 401', async () => {
    const res = await call('POST', `/api/documents/${DOC}/review-requests`, {
      as: undefined, body: { reviewerId: 'bob' },
    });
    expect(res.statusCode).toBe(401);
  });
});

describe('section-scoped review requests', () => {
  it('takes the sectionId from the path', async () => {
    const res = await call('POST', `/api/documents/${DOC}/sections/sec-3/review-requests`, {
      as: 'alice', body: { reviewerId: 'bob' },
    });
    expect(res.statusCode).toBe(201);
    const req = (res.body as { request: DocumentReviewRequest }).request;
    expect(req.scope).toBe('section');
    expect(req.sectionId).toBe('sec-3');
    expect(req.requestKey).toBe('sec-3:bob');
  });

  it('lists only that section’s requests', async () => {
    await call('POST', `/api/documents/${DOC}/sections/sec-3/review-requests`, {
      as: 'alice', body: { reviewerId: 'bob' },
    });
    await call('POST', `/api/documents/${DOC}/sections/sec-4/review-requests`, {
      as: 'alice', body: { reviewerId: 'carol' },
    });
    const res = await call('GET', `/api/documents/${DOC}/sections/sec-3/review-requests`);
    const list = (res.body as { requests: DocumentReviewRequest[] }).requests;
    expect(list).toHaveLength(1);
    expect(list[0]!.reviewerId).toBe('bob');
  });
});

describe('GET /api/documents/:documentId/review-requests', () => {
  it('returns every request on the document', async () => {
    await call('POST', `/api/documents/${DOC}/review-requests`, {
      as: 'alice', body: { reviewerId: 'bob' },
    });
    await call('POST', `/api/documents/${DOC}/review-requests`, {
      as: 'alice', body: { reviewerId: 'carol' },
    });
    const res = await call('GET', `/api/documents/${DOC}/review-requests`);
    expect(res.statusCode).toBe(200);
    expect((res.body as { requests: unknown[] }).requests).toHaveLength(2);
  });

  it('filters by state', async () => {
    await call('POST', `/api/documents/${DOC}/review-requests`, {
      as: 'alice', body: { reviewerId: 'bob' },
    });
    await call('POST', `/api/documents/${DOC}/review-requests`, {
      as: 'alice', body: { reviewerId: 'carol' },
    });
    await call('PATCH', `/api/documents/${DOC}/review-requests/${DOC_SCOPE}:bob`, {
      as: 'bob', body: { state: 'approved' },
    });
    const res = await call('GET', `/api/documents/${DOC}/review-requests?state=pending`);
    const list = (res.body as { requests: DocumentReviewRequest[] }).requests;
    expect(list).toHaveLength(1);
    expect(list[0]!.reviewerId).toBe('carol');
  });
});

describe('PATCH /api/documents/:documentId/review-requests/:requestKey', () => {
  beforeEach(async () => {
    await call('POST', `/api/documents/${DOC}/review-requests`, {
      as: 'alice', body: { reviewerId: 'bob' },
    });
    mockEmit.mockClear();
  });

  const KEY = `${DOC_SCOPE}:bob`;

  it('the addressed reviewer approves', async () => {
    const res = await call('PATCH', `/api/documents/${DOC}/review-requests/${KEY}`, {
      as: 'bob', body: { state: 'approved', comment: 'ship it' },
    });
    expect(res.statusCode).toBe(200);
    const req = (res.body as { request: DocumentReviewRequest }).request;
    expect(req.state).toBe('approved');
    expect(req.comment).toBe('ship it');
    expect(req.respondedAt).toBeDefined();
  });

  it('the addressed reviewer requests changes', async () => {
    const res = await call('PATCH', `/api/documents/${DOC}/review-requests/${KEY}`, {
      as: 'bob', body: { state: 'changes_requested' },
    });
    expect((res.body as { request: DocumentReviewRequest }).request.state)
      .toBe('changes_requested');
  });

  it('someone ELSE cannot respond on the reviewer’s behalf (403)', async () => {
    const res = await call('PATCH', `/api/documents/${DOC}/review-requests/${KEY}`, {
      as: 'mallory', body: { state: 'approved' },
    });
    expect(res.statusCode).toBe(403);
    expect(requestRows.get(rowKey(DOC, KEY))!.state).toBe('pending');
  });

  it('not even the OWNER may approve on the reviewer’s behalf (403)', async () => {
    const res = await call('PATCH', `/api/documents/${DOC}/review-requests/${KEY}`, {
      as: 'alice', body: { state: 'approved' },
    });
    expect(res.statusCode).toBe(403);
  });

  it('the owner may withdraw a request, and it gets a TTL', async () => {
    const res = await call('PATCH', `/api/documents/${DOC}/review-requests/${KEY}`, {
      as: 'alice', body: { state: 'withdrawn' },
    });
    expect(res.statusCode).toBe(200);
    const req = (res.body as { request: DocumentReviewRequest }).request;
    expect(req.state).toBe('withdrawn');
    expect(req.ttl).toBeGreaterThan(Math.floor(Date.now() / 1000));
  });

  it('the reviewer cannot withdraw the request addressed to them (403)', async () => {
    const res = await call('PATCH', `/api/documents/${DOC}/review-requests/${KEY}`, {
      as: 'bob', body: { state: 'withdrawn' },
    });
    expect(res.statusCode).toBe(403);
  });

  it('the owner may change the due date', async () => {
    const res = await call('PATCH', `/api/documents/${DOC}/review-requests/${KEY}`, {
      as: 'alice', body: { dueDate: '2031-01-01T00:00:00.000Z' },
    });
    expect(res.statusCode).toBe(200);
    expect((res.body as { request: DocumentReviewRequest }).request.dueDate)
      .toBe('2031-01-01T00:00:00.000Z');
  });

  it('the owner may clear the due date with null', async () => {
    await call('PATCH', `/api/documents/${DOC}/review-requests/${KEY}`, {
      as: 'alice', body: { dueDate: '2031-01-01T00:00:00.000Z' },
    });
    const res = await call('PATCH', `/api/documents/${DOC}/review-requests/${KEY}`, {
      as: 'alice', body: { dueDate: null },
    });
    expect((res.body as { request: DocumentReviewRequest }).request.dueDate).toBeUndefined();
  });

  it('a bystander cannot change the due date (403)', async () => {
    const res = await call('PATCH', `/api/documents/${DOC}/review-requests/${KEY}`, {
      as: 'mallory', body: { dueDate: '2031-01-01T00:00:00.000Z' },
    });
    expect(res.statusCode).toBe(403);
  });

  it('rejects an unknown state (400)', async () => {
    const res = await call('PATCH', `/api/documents/${DOC}/review-requests/${KEY}`, {
      as: 'bob', body: { state: 'lgtm' },
    });
    expect(res.statusCode).toBe(400);
  });

  it('rejects an empty patch (400)', async () => {
    const res = await call('PATCH', `/api/documents/${DOC}/review-requests/${KEY}`, {
      as: 'bob', body: {},
    });
    expect(res.statusCode).toBe(400);
  });

  it('404s for a request that does not exist', async () => {
    const res = await call('PATCH', `/api/documents/${DOC}/review-requests/${DOC_SCOPE}:nobody`, {
      as: 'bob', body: { state: 'approved' },
    });
    expect(res.statusCode).toBe(404);
  });

  it('emits doc:review_request_updated', async () => {
    await call('PATCH', `/api/documents/${DOC}/review-requests/${KEY}`, {
      as: 'bob', body: { state: 'approved' },
    });
    expect(mockEmit).toHaveBeenCalledWith(
      `doc:${DOC}`, 'doc:review_request_updated',
      expect.objectContaining({ documentId: DOC, requestKey: KEY, state: 'approved' }),
    );
  });

  it('reports the remaining outstanding reviews so the FE can chain to approve', async () => {
    await call('POST', `/api/documents/${DOC}/review-requests`, {
      as: 'alice', body: { reviewerId: 'carol' },
    });
    const res = await call('PATCH', `/api/documents/${DOC}/review-requests/${KEY}`, {
      as: 'bob', body: { state: 'approved' },
    });
    const body = res.body as { outstandingReviews: Array<{ reviewerId: string }> };
    expect(body.outstandingReviews.map((r) => r.reviewerId)).toEqual(['carol']);
  });
});

describe('isReviewOutstanding', () => {
  const base: DocumentReviewRequest = {
    documentId: DOC, requestKey: 'k', requestId: 'r', scope: 'document',
    reviewerId: 'bob', requestedBy: 'alice', state: 'pending', required: true,
    createdAt: 'x', updatedAt: 'x',
  };

  it.each([
    ['pending', true, true],
    ['changes_requested', true, true],
    ['approved', true, false],
    ['withdrawn', true, false],
    ['pending', false, false],
    ['changes_requested', false, false],
  ])('state=%s required=%s → outstanding=%s', (state, required, expected) => {
    expect(
      isReviewOutstanding({
        ...base,
        state: state as DocumentReviewRequest['state'],
        required: required as boolean,
      }),
    ).toBe(expected);
  });
});

describe('isOverdue', () => {
  const base: DocumentReviewRequest = {
    documentId: DOC, requestKey: 'k', requestId: 'r', scope: 'document',
    reviewerId: 'bob', requestedBy: 'alice', state: 'pending', required: true,
    createdAt: 'x', updatedAt: 'x',
  };
  const now = new Date('2026-06-01T00:00:00.000Z');

  it('is true for a pending request past its due date', () => {
    expect(isOverdue({ ...base, dueDate: '2026-05-01T00:00:00.000Z' }, now)).toBe(true);
  });
  it('is false for a pending request not yet due', () => {
    expect(isOverdue({ ...base, dueDate: '2026-07-01T00:00:00.000Z' }, now)).toBe(false);
  });
  it('is false with no due date', () => {
    expect(isOverdue(base, now)).toBe(false);
  });
  it('is false once the request has been answered', () => {
    expect(
      isOverdue({ ...base, state: 'approved', dueDate: '2026-05-01T00:00:00.000Z' }, now),
    ).toBe(false);
  });
  it('is false for an unparseable due date', () => {
    expect(isOverdue({ ...base, dueDate: 'whenever' }, now)).toBe(false);
  });
});

describe('buildRequestKey', () => {
  it('uses the doc scope marker when no section is given', () => {
    expect(buildRequestKey('bob')).toBe(`${DOC_SCOPE}:bob`);
  });
  it('uses the sectionId prefix so begins_with queries work', () => {
    expect(buildRequestKey('bob', 'sec-3')).toBe('sec-3:bob');
  });
});
