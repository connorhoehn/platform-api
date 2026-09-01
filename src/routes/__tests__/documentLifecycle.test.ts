/**
 * Route-level tests for the document review lifecycle.
 *
 * The repositories are replaced with in-memory fakes rather than jest.fn()
 * stubs so the whole path is exercised end to end: ownership resolution,
 * reviewer designation, the outstanding-review gate, and the legacy 'final'
 * migration. A stub-per-method mock would happily let a broken authorization
 * check pass.
 */

import express, { type NextFunction, type Request, type Response } from 'express';
import type { DocumentLifecycleItem } from '../../repositories/DocumentLifecycleRepository';
import type { DocumentReviewRequest } from '../../repositories/DocumentReviewRequestRepository';
import { buildRequestKey } from '../../repositories/DocumentReviewRequestRepository';
import { normalizeStatus } from '../../lib/document-lifecycle';

// ---------------------------------------------------------------------------
// In-memory substrate
// ---------------------------------------------------------------------------

const lifecycleRows = new Map<string, Record<string, unknown>>();
const requestRows = new Map<string, DocumentReviewRequest>();
const typedDocs = new Map<string, { createdBy: string }>();
const mockEmit = jest.fn().mockResolvedValue(undefined);

function rowKey(documentId: string, requestKey: string): string {
  return `${documentId}::${requestKey}`;
}

function hydrate(raw: Record<string, unknown>): DocumentLifecycleItem {
  const stored = raw['status'] as string | undefined;
  const normalized = normalizeStatus(stored);
  const item = {
    ...raw,
    status: normalized,
    version: (raw['version'] as number) ?? 0,
    history: (raw['history'] as DocumentLifecycleItem['history']) ?? [],
  } as unknown as DocumentLifecycleItem;
  if (stored !== undefined && stored !== normalized) item.legacyStatus = stored;
  return item;
}

const fakeLifecycleRepo = {
  async get(documentId: string): Promise<DocumentLifecycleItem | null> {
    const raw = lifecycleRows.get(documentId);
    return raw ? hydrate(raw) : null;
  },
  async initialize(
    documentId: string,
    ownerId: string,
    status = 'draft',
  ): Promise<DocumentLifecycleItem | null> {
    if (lifecycleRows.has(documentId)) return null;
    const now = new Date().toISOString();
    lifecycleRows.set(documentId, {
      documentId, status, ownerId,
      createdAt: now, updatedAt: now, updatedBy: ownerId,
      version: 0, history: [],
    });
    return hydrate(lifecycleRows.get(documentId)!);
  },
  async applyTransition(p: {
    documentId: string; expectedFrom: string; legacyFrom?: string;
    to: string; action: string; actor: string; at: string; reason?: string;
    history: DocumentLifecycleItem['history'];
  }): Promise<DocumentLifecycleItem> {
    const raw = lifecycleRows.get(p.documentId);
    if (!raw) throw new Error('missing row');
    const current = raw['status'] as string;
    // Same compare-and-swap the real repository does via ConditionExpression.
    if (current !== p.expectedFrom && current !== p.legacyFrom) {
      const { LifecycleConcurrencyError } =
        jest.requireActual<typeof import('../../repositories/DocumentLifecycleRepository')>(
          '../../repositories/DocumentLifecycleRepository',
        );
      throw new LifecycleConcurrencyError();
    }
    const next = {
      ...raw,
      status: p.to,
      updatedAt: p.at,
      updatedBy: p.actor,
      lastAction: p.action,
      ...(p.reason !== undefined ? { lastReason: p.reason } : {}),
      version: ((raw['version'] as number) ?? 0) + 1,
      history: [
        ...p.history,
        { from: p.expectedFrom, to: p.to, action: p.action, actor: p.actor, at: p.at },
      ],
    };
    lifecycleRows.set(p.documentId, next);
    return hydrate(next);
  },
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
  async getRequest(documentId: string, requestKey: string): Promise<DocumentReviewRequest | null> {
    return requestRows.get(rowKey(documentId, requestKey)) ?? null;
  },
  async getRequestsForDocument(documentId: string): Promise<DocumentReviewRequest[]> {
    return [...requestRows.values()].filter((r) => r.documentId === documentId);
  },
  async getRequestsForSection(documentId: string, sectionId: string): Promise<DocumentReviewRequest[]> {
    return [...requestRows.values()].filter(
      (r) => r.documentId === documentId && r.requestKey.startsWith(`${sectionId}:`),
    );
  },
  async getRequestsForReviewer(reviewerId: string, state?: string): Promise<DocumentReviewRequest[]> {
    const rows = [...requestRows.values()].filter((r) => r.reviewerId === reviewerId);
    return state ? rows.filter((r) => r.state === state) : rows;
  },
  async setState(p: {
    documentId: string; requestKey: string; state: string; actor: string;
    comment?: string; at?: string;
  }): Promise<DocumentReviewRequest | null> {
    const key = rowKey(p.documentId, p.requestKey);
    const existing = requestRows.get(key);
    if (!existing) return null;
    const at = p.at ?? new Date().toISOString();
    const next: DocumentReviewRequest = {
      ...existing,
      state: p.state as DocumentReviewRequest['state'],
      updatedAt: at,
      respondedAt: at,
      ...(p.comment !== undefined ? { comment: p.comment } : {}),
      ...(p.state === 'withdrawn'
        ? { ttl: Math.floor(Date.parse(at) / 1000 + 30 * 86400) }
        : {}),
    };
    requestRows.set(key, next);
    return next;
  },
  async setDueDate(
    documentId: string, requestKey: string, dueDate: string | null,
  ): Promise<DocumentReviewRequest | null> {
    const key = rowKey(documentId, requestKey);
    const existing = requestRows.get(key);
    if (!existing) return null;
    const next = { ...existing, updatedAt: new Date().toISOString() };
    if (dueDate === null) delete next.dueDate;
    else next.dueDate = dueDate;
    requestRows.set(key, next);
    return next;
  },
};

/**
 * When set, the creator lookup THROWS instead of returning a row — the case
 * that used to be swallowed into `null` and thereby into an ownership grant.
 */
let typedDocLookupError: Error | null = null;

const fakeTypedDocumentRepo = {
  async get(documentId: string): Promise<{ createdBy: string } | null> {
    if (typedDocLookupError) throw typedDocLookupError;
    return typedDocs.get(documentId) ?? null;
  },
};

// ---------------------------------------------------------------------------
// Module mocks
// ---------------------------------------------------------------------------

let currentUser: string | undefined = 'alice';

jest.mock('../../middleware/auth', () => ({
  requireAuth: (req: Request, res: Response, next: NextFunction) => {
    if (!currentUser) {
      res.status(401).json({ error: 'Authorization required' });
      return;
    }
    (req as Request & { user?: { sub: string } }).user = { sub: currentUser };
    next();
  },
  optionalAuth: (req: Request, _res: Response, next: NextFunction) => {
    if (currentUser) {
      (req as Request & { user?: { sub: string } }).user = { sub: currentUser };
    }
    next();
  },
}));

jest.mock('../../repositories', () => ({
  get documentLifecycleRepo() { return fakeLifecycleRepo; },
  get documentReviewRequestRepo() { return fakeRequestRepo; },
  get typedDocumentRepo() { return fakeTypedDocumentRepo; },
  profileRepo: { getProfile: jest.fn().mockResolvedValue(null) },
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

import { documentLifecycleRouter, lifecycleSchemaRouter } from '../documentLifecycle';
import {
  documentReviewRequestsRouter,
  sectionReviewRequestsRouter,
} from '../documentReviewRequests';
import { myReviewsRouter } from '../sectionReviews';
import { errorHandler } from '../../middleware/error-handler';

// ---------------------------------------------------------------------------
// Express harness (same shape as sectionReviews.test.ts)
// ---------------------------------------------------------------------------

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

function buildApp(): express.Express {
  const app = express();
  app.use(express.json());
  app.use('/api/documents/lifecycle-schema', lifecycleSchemaRouter);
  app.use('/api/documents/:documentId/lifecycle', documentLifecycleRouter);
  app.use('/api/documents/:documentId/review-requests', documentReviewRequestsRouter);
  app.use(
    '/api/documents/:documentId/sections/:sectionId/review-requests',
    sectionReviewRequestsRouter,
  );
  app.use('/api/reviews', myReviewsRouter);
  app.use(errorHandler);
  return app;
}

async function call(
  method: string,
  url: string,
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
    method: method.toUpperCase(),
    url, originalUrl: url,
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

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const DOC = 'doc-1';

/** alice owns DOC; bob is a designated reviewer; mallory is a bystander. */
async function seedOwnedDocument(status = 'draft'): Promise<void> {
  const now = new Date().toISOString();
  lifecycleRows.set(DOC, {
    documentId: DOC, status, ownerId: 'alice',
    createdAt: now, updatedAt: now, updatedBy: 'alice',
    version: 1, history: [],
  });
}

async function seedReviewRequest(
  reviewerId: string,
  overrides: Partial<DocumentReviewRequest> = {},
): Promise<void> {
  const requestKey = buildRequestKey(reviewerId, overrides.sectionId);
  const now = new Date().toISOString();
  requestRows.set(rowKey(DOC, requestKey), {
    documentId: DOC, requestKey, requestId: `req-${reviewerId}`,
    scope: overrides.sectionId ? 'section' : 'document',
    reviewerId, requestedBy: 'alice', state: 'pending', required: true,
    createdAt: now, updatedAt: now,
    ...overrides,
  });
}

beforeEach(() => {
  lifecycleRows.clear();
  requestRows.clear();
  typedDocs.clear();
  typedDocLookupError = null;
  mockEmit.mockClear();
  currentUser = 'alice';
});

// ---------------------------------------------------------------------------

describe('GET /api/documents/:documentId/lifecycle', () => {
  it('reports an implicit draft for a document that has never transitioned', async () => {
    const res = await call('GET', `/api/documents/${DOC}/lifecycle`);
    expect(res.statusCode).toBe(200);
    const body = res.body as { lifecycle: { status: string; implicit: boolean } };
    expect(body.lifecycle.status).toBe('draft');
    expect(body.lifecycle.implicit).toBe(true);
  });

  it('normalizes a legacy `final` row to approved and flags the legacy value', async () => {
    await seedOwnedDocument('final');
    const res = await call('GET', `/api/documents/${DOC}/lifecycle`);
    const body = res.body as { lifecycle: { status: string; legacyStatus?: string } };
    expect(body.lifecycle.status).toBe('approved');
    expect(body.lifecycle.legacyStatus).toBe('final');
  });

  it('gives the owner reopen as an available action on an approved doc', async () => {
    await seedOwnedDocument('approved');
    const res = await call('GET', `/api/documents/${DOC}/lifecycle`, { as: 'alice' });
    expect((res.body as { availableActions: string[] }).availableActions).toEqual(['reopen']);
  });

  it('gives a bystander NO available actions on an approved doc', async () => {
    await seedOwnedDocument('approved');
    const res = await call('GET', `/api/documents/${DOC}/lifecycle`, { as: 'mallory' });
    expect((res.body as { availableActions: string[] }).availableActions).toEqual([]);
  });

  it('lists outstanding required reviews', async () => {
    await seedOwnedDocument('in_review');
    await seedReviewRequest('bob');
    const res = await call('GET', `/api/documents/${DOC}/lifecycle`);
    const body = res.body as { outstandingReviews: Array<{ reviewerId: string }> };
    expect(body.outstandingReviews).toHaveLength(1);
    expect(body.outstandingReviews[0]!.reviewerId).toBe('bob');
  });
});

describe('GET /api/documents/lifecycle-schema', () => {
  it('publishes the transition table for the frontend', async () => {
    const res = await call('GET', '/api/documents/lifecycle-schema');
    expect(res.statusCode).toBe(200);
    const body = res.body as {
      statuses: string[]; actions: string[];
      transitions: unknown[]; legacyStatusAliases: Record<string, string>;
    };
    expect(body.statuses).toEqual(['draft', 'in_review', 'changes_requested', 'approved']);
    expect(body.transitions).toHaveLength(7);
    expect(body.legacyStatusAliases['final']).toBe('approved');
  });
});

describe('POST /api/documents/:documentId/lifecycle/transitions — happy path', () => {
  it('owner submits a draft for review', async () => {
    await seedOwnedDocument('draft');
    const res = await call('POST', `/api/documents/${DOC}/lifecycle/transitions`, {
      as: 'alice', body: { action: 'submit_for_review' },
    });
    expect(res.statusCode).toBe(200);
    const body = res.body as { from: string; to: string; lifecycle: { version: number } };
    expect(body.from).toBe('draft');
    expect(body.to).toBe('in_review');
    expect(body.lifecycle.version).toBe(2);
  });

  it('bumps the version and records history on each transition', async () => {
    await seedOwnedDocument('draft');
    await call('POST', `/api/documents/${DOC}/lifecycle/transitions`, {
      as: 'alice', body: { action: 'submit_for_review' },
    });
    const res = await call('POST', `/api/documents/${DOC}/lifecycle/transitions`, {
      as: 'alice', body: { action: 'withdraw', reason: 'not ready' },
    });
    const body = res.body as { lifecycle: { history: unknown[]; lastReason?: string } };
    expect(body.lifecycle.history).toHaveLength(2);
    expect(body.lifecycle.lastReason).toBe('not ready');
  });

  it('emits doc:lifecycle_changed on the document channel', async () => {
    await seedOwnedDocument('draft');
    await call('POST', `/api/documents/${DOC}/lifecycle/transitions`, {
      as: 'alice', body: { action: 'submit_for_review' },
    });
    expect(mockEmit).toHaveBeenCalledWith(
      `doc:${DOC}`,
      'doc:lifecycle_changed',
      expect.objectContaining({
        documentId: DOC, from: 'draft', to: 'in_review',
        action: 'submit_for_review', actor: 'alice',
      }),
    );
  });

  it('designated reviewer approves once no required review is outstanding', async () => {
    await seedOwnedDocument('in_review');
    await seedReviewRequest('bob', { state: 'approved' });
    const res = await call('POST', `/api/documents/${DOC}/lifecycle/transitions`, {
      as: 'bob', body: { action: 'approve' },
    });
    expect(res.statusCode).toBe(200);
    expect((res.body as { to: string }).to).toBe('approved');
  });

  it('designated reviewer sends the document back for changes', async () => {
    await seedOwnedDocument('in_review');
    await seedReviewRequest('bob');
    const res = await call('POST', `/api/documents/${DOC}/lifecycle/transitions`, {
      as: 'bob', body: { action: 'request_changes', reason: 'needs a rollback plan' },
    });
    expect(res.statusCode).toBe(200);
    expect((res.body as { to: string }).to).toBe('changes_requested');
  });

  it('owner revises back to draft from changes_requested', async () => {
    await seedOwnedDocument('changes_requested');
    const res = await call('POST', `/api/documents/${DOC}/lifecycle/transitions`, {
      as: 'alice', body: { action: 'revise' },
    });
    expect((res.body as { to: string }).to).toBe('draft');
  });

  it('owner resubmits straight from changes_requested', async () => {
    await seedOwnedDocument('changes_requested');
    const res = await call('POST', `/api/documents/${DOC}/lifecycle/transitions`, {
      as: 'alice', body: { action: 'submit_for_review' },
    });
    expect((res.body as { to: string }).to).toBe('in_review');
  });

  it('creates the lifecycle row on first transition of an implicit draft', async () => {
    typedDocs.set(DOC, { createdBy: 'alice' });
    const res = await call('POST', `/api/documents/${DOC}/lifecycle/transitions`, {
      as: 'alice', body: { action: 'submit_for_review' },
    });
    expect(res.statusCode).toBe(200);
    expect(lifecycleRows.get(DOC)!['status']).toBe('in_review');
    expect(lifecycleRows.get(DOC)!['ownerId']).toBe('alice');
  });
});

describe('the "anyone can unlock a finalized doc" bug', () => {
  it.each(['approved', 'final'])(
    'a bystander CANNOT reopen a %s document (403)',
    async (status) => {
      await seedOwnedDocument(status);
      const res = await call('POST', `/api/documents/${DOC}/lifecycle/transitions`, {
        as: 'mallory', body: { action: 'reopen' },
      });
      expect(res.statusCode).toBe(403);
      expect((res.body as { error: string }).error).toMatch(/Only the document owner/);
      // And the document is genuinely still locked.
      expect(lifecycleRows.get(DOC)!['status']).toBe(status);
    },
  );

  it('a designated REVIEWER cannot reopen an approved document either', async () => {
    await seedOwnedDocument('approved');
    await seedReviewRequest('bob', { state: 'approved' });
    const res = await call('POST', `/api/documents/${DOC}/lifecycle/transitions`, {
      as: 'bob', body: { action: 'reopen' },
    });
    expect(res.statusCode).toBe(403);
    expect(lifecycleRows.get(DOC)!['status']).toBe('approved');
  });

  it('the owner CAN reopen, and the legacy `final` value is migrated to draft', async () => {
    await seedOwnedDocument('final');
    const res = await call('POST', `/api/documents/${DOC}/lifecycle/transitions`, {
      as: 'alice', body: { action: 'reopen' },
    });
    expect(res.statusCode).toBe(200);
    expect((res.body as { from: string; to: string }).from).toBe('approved');
    expect(lifecycleRows.get(DOC)!['status']).toBe('draft');
  });

  it('an unauthenticated caller cannot transition at all (401)', async () => {
    await seedOwnedDocument('approved');
    const res = await call('POST', `/api/documents/${DOC}/lifecycle/transitions`, {
      as: undefined, body: { action: 'reopen' },
    });
    expect(res.statusCode).toBe(401);
  });
});

describe('authorization on the other transitions', () => {
  it('a bystander cannot submit someone else’s draft for review (403)', async () => {
    await seedOwnedDocument('draft');
    const res = await call('POST', `/api/documents/${DOC}/lifecycle/transitions`, {
      as: 'mallory', body: { action: 'submit_for_review' },
    });
    expect(res.statusCode).toBe(403);
  });

  it('a bystander cannot withdraw a document from review (403)', async () => {
    await seedOwnedDocument('in_review');
    const res = await call('POST', `/api/documents/${DOC}/lifecycle/transitions`, {
      as: 'mallory', body: { action: 'withdraw' },
    });
    expect(res.statusCode).toBe(403);
  });

  it('a user with NO review request cannot approve (403)', async () => {
    await seedOwnedDocument('in_review');
    const res = await call('POST', `/api/documents/${DOC}/lifecycle/transitions`, {
      as: 'mallory', body: { action: 'approve' },
    });
    expect(res.statusCode).toBe(403);
    expect(lifecycleRows.get(DOC)!['status']).toBe('in_review');
  });

  it('a user whose review request was WITHDRAWN is no longer a reviewer (403)', async () => {
    await seedOwnedDocument('in_review');
    await seedReviewRequest('bob', { state: 'withdrawn', required: false });
    const res = await call('POST', `/api/documents/${DOC}/lifecycle/transitions`, {
      as: 'bob', body: { action: 'approve' },
    });
    expect(res.statusCode).toBe(403);
  });

  it('the owner cannot approve their own document without being a reviewer (403)', async () => {
    await seedOwnedDocument('in_review');
    await seedReviewRequest('bob', { state: 'approved' });
    const res = await call('POST', `/api/documents/${DOC}/lifecycle/transitions`, {
      as: 'alice', body: { action: 'approve' },
    });
    expect(res.statusCode).toBe(403);
  });

  it('an owner who IS also a designated reviewer may approve', async () => {
    await seedOwnedDocument('in_review');
    await seedReviewRequest('alice', { reviewerId: 'alice', state: 'approved' });
    const res = await call('POST', `/api/documents/${DOC}/lifecycle/transitions`, {
      as: 'alice', body: { action: 'approve' },
    });
    expect(res.statusCode).toBe(200);
  });
});

describe('approve requires every outstanding required review to be satisfied', () => {
  it('409s while a required review is still pending', async () => {
    await seedOwnedDocument('in_review');
    await seedReviewRequest('bob', { state: 'approved' });
    await seedReviewRequest('carol', { state: 'pending' });
    const res = await call('POST', `/api/documents/${DOC}/lifecycle/transitions`, {
      as: 'bob', body: { action: 'approve' },
    });
    expect(res.statusCode).toBe(409);
    expect((res.body as { error: string }).error).toMatch(/carol/);
    expect(lifecycleRows.get(DOC)!['status']).toBe('in_review');
  });

  it('409s while a required review sits in changes_requested', async () => {
    await seedOwnedDocument('in_review');
    await seedReviewRequest('bob', { state: 'changes_requested' });
    const res = await call('POST', `/api/documents/${DOC}/lifecycle/transitions`, {
      as: 'bob', body: { action: 'approve' },
    });
    expect(res.statusCode).toBe(409);
  });

  it('an OPTIONAL pending review does not block approval', async () => {
    await seedOwnedDocument('in_review');
    await seedReviewRequest('bob', { state: 'approved' });
    await seedReviewRequest('carol', { state: 'pending', required: false });
    const res = await call('POST', `/api/documents/${DOC}/lifecycle/transitions`, {
      as: 'bob', body: { action: 'approve' },
    });
    expect(res.statusCode).toBe(200);
  });

  it('a WITHDRAWN required review does not block approval', async () => {
    await seedOwnedDocument('in_review');
    await seedReviewRequest('bob', { state: 'approved' });
    await seedReviewRequest('carol', { state: 'withdrawn' });
    const res = await call('POST', `/api/documents/${DOC}/lifecycle/transitions`, {
      as: 'bob', body: { action: 'approve' },
    });
    expect(res.statusCode).toBe(200);
  });
});

describe('illegal transitions return 409 with a reason', () => {
  it.each([
    ['draft', 'approve'],
    ['draft', 'withdraw'],
    ['draft', 'reopen'],
    ['draft', 'revise'],
    ['draft', 'request_changes'],
    ['in_review', 'submit_for_review'],
    ['in_review', 'reopen'],
    ['in_review', 'revise'],
    ['changes_requested', 'approve'],
    ['changes_requested', 'withdraw'],
    ['changes_requested', 'reopen'],
    ['changes_requested', 'request_changes'],
    ['approved', 'approve'],
    ['approved', 'submit_for_review'],
    ['approved', 'withdraw'],
    ['approved', 'request_changes'],
    ['approved', 'revise'],
  ])('%s + %s → 409', async (status, action) => {
    await seedOwnedDocument(status);
    await seedReviewRequest('alice', { reviewerId: 'alice' }); // maximal privilege
    const res = await call('POST', `/api/documents/${DOC}/lifecycle/transitions`, {
      as: 'alice', body: { action },
    });
    expect(res.statusCode).toBe(409);
    expect((res.body as { error: string }).error).toMatch(/Cannot/);
  });

  it('rejects a missing action with 400', async () => {
    await seedOwnedDocument('draft');
    const res = await call('POST', `/api/documents/${DOC}/lifecycle/transitions`, {
      as: 'alice', body: {},
    });
    expect(res.statusCode).toBe(400);
  });

  it('rejects the old "finalize" verb with 400', async () => {
    await seedOwnedDocument('draft');
    const res = await call('POST', `/api/documents/${DOC}/lifecycle/transitions`, {
      as: 'alice', body: { action: 'finalize' },
    });
    expect(res.statusCode).toBe(400);
  });
});

describe('ownership resolution', () => {
  it('prefers the recorded ownerId over the typed-document creator', async () => {
    await seedOwnedDocument('draft');
    typedDocs.set(DOC, { createdBy: 'mallory' });
    const res = await call('POST', `/api/documents/${DOC}/lifecycle/transitions`, {
      as: 'mallory', body: { action: 'submit_for_review' },
    });
    expect(res.statusCode).toBe(403);
  });

  it('falls back to the typed-document creator when no row exists', async () => {
    typedDocs.set(DOC, { createdBy: 'alice' });
    const denied = await call('POST', `/api/documents/${DOC}/lifecycle/transitions`, {
      as: 'mallory', body: { action: 'submit_for_review' },
    });
    expect(denied.statusCode).toBe(403);
  });

  it('the creator CAN act, and the materialized row records the creator as owner', async () => {
    typedDocs.set(DOC, { createdBy: 'alice' });
    const res = await call('POST', `/api/documents/${DOC}/lifecycle/transitions`, {
      as: 'alice', body: { action: 'submit_for_review' },
    });
    expect(res.statusCode).toBe(200);
    expect(lifecycleRows.get(DOC)!['ownerId']).toBe('alice');
  });
});

// ---------------------------------------------------------------------------
// The vulnerability this lane closes.
// ---------------------------------------------------------------------------

describe('a null owner does NOT grant ownership (no trust-on-first-use)', () => {
  it('the first caller on an ownerless document is refused, not made owner', async () => {
    const res = await call('POST', `/api/documents/${DOC}/lifecycle/transitions`, {
      as: 'mallory', body: { action: 'submit_for_review' },
    });
    expect(res.statusCode).toBe(403);
    expect((res.body as { code: string }).code).toBe('owner_not_established');
    // The decisive assertion: no row was written, so nothing was claimed.
    expect(lifecycleRows.has(DOC)).toBe(false);
  });

  it('an ownerless document offers NOBODY any available action', async () => {
    const res = await call('GET', `/api/documents/${DOC}/lifecycle`, { as: 'mallory' });
    expect((res.body as { availableActions: string[] }).availableActions).toEqual([]);
  });

  it('an ownerless approved document cannot be reopened by anyone', async () => {
    lifecycleRows.set(DOC, {
      documentId: DOC, status: 'approved', ownerId: '',
      createdAt: '', updatedAt: '', updatedBy: '', version: 1, history: [],
    });
    const res = await call('POST', `/api/documents/${DOC}/lifecycle/transitions`, {
      as: 'mallory', body: { action: 'reopen' },
    });
    expect(res.statusCode).toBe(403);
    expect(lifecycleRows.get(DOC)!['status']).toBe('approved');
  });

  it('a designated reviewer is unaffected — reviewer rights never came from ownership', async () => {
    // No lifecycle row owner and no creator, but bob has a review request.
    lifecycleRows.set(DOC, {
      documentId: DOC, status: 'in_review', ownerId: '',
      createdAt: '', updatedAt: '', updatedBy: '', version: 1, history: [],
    });
    await seedReviewRequest('bob', { state: 'approved' });
    const res = await call('POST', `/api/documents/${DOC}/lifecycle/transitions`, {
      as: 'bob', body: { action: 'approve' },
    });
    expect(res.statusCode).toBe(200);
  });
});

describe('a FAILED creator lookup fails closed', () => {
  it('does not grant ownership — 503, not a silent grant', async () => {
    typedDocLookupError = new Error('ddb unavailable');
    const res = await call('POST', `/api/documents/${DOC}/lifecycle/transitions`, {
      as: 'mallory', body: { action: 'submit_for_review' },
    });
    expect(res.statusCode).toBe(503);
    expect((res.body as { code: string }).code).toBe('owner_lookup_unavailable');
    expect(lifecycleRows.has(DOC)).toBe(false);
  });

  it('is distinguished from "definitely no owner" by its code', async () => {
    const absent = await call('POST', `/api/documents/${DOC}/lifecycle/transitions`, {
      as: 'mallory', body: { action: 'submit_for_review' },
    });
    typedDocLookupError = new Error('ddb unavailable');
    const unavailable = await call('POST', `/api/documents/${DOC}/lifecycle/transitions`, {
      as: 'mallory', body: { action: 'submit_for_review' },
    });
    expect((absent.body as { code: string }).code).toBe('owner_not_established');
    expect((unavailable.body as { code: string }).code).toBe('owner_lookup_unavailable');
    expect(absent.statusCode).not.toBe(unavailable.statusCode);
  });

  it('cannot be used to steal a document that already has a recorded owner', async () => {
    await seedOwnedDocument('draft');
    typedDocLookupError = new Error('ddb unavailable');
    const res = await call('POST', `/api/documents/${DOC}/lifecycle/transitions`, {
      as: 'mallory', body: { action: 'submit_for_review' },
    });
    // The recorded ownerId short-circuits the creator lookup entirely.
    expect(res.statusCode).toBe(403);
    expect((res.body as { code: string }).code).toBe('not_authorized');
  });

  it('the legitimate owner still cannot be locked out once ownership is recorded', async () => {
    await seedOwnedDocument('draft');
    typedDocLookupError = new Error('ddb unavailable');
    const res = await call('POST', `/api/documents/${DOC}/lifecycle/transitions`, {
      as: 'alice', body: { action: 'submit_for_review' },
    });
    expect(res.statusCode).toBe(200);
  });

  it('refuses an ownership CLAIM rather than granting it', async () => {
    typedDocLookupError = new Error('ddb unavailable');
    const res = await call('POST', `/api/documents/${DOC}/lifecycle/ownership`, {
      as: 'mallory', body: {},
    });
    expect(res.statusCode).toBe(503);
    expect((res.body as { code: string }).code).toBe('owner_lookup_unavailable');
    expect(lifecycleRows.has(DOC)).toBe(false);
  });
});

describe('POST /lifecycle/ownership — explicit, recorded first claim', () => {
  it('claims an ownerless document and records the owner', async () => {
    const res = await call('POST', `/api/documents/${DOC}/lifecycle/ownership`, {
      as: 'alice', body: {},
    });
    expect(res.statusCode).toBe(200);
    expect((res.body as { claimed: boolean }).claimed).toBe(true);
    expect(lifecycleRows.get(DOC)!['ownerId']).toBe('alice');
  });

  it('unblocks the owner-only transitions that were refused before the claim', async () => {
    const before = await call('POST', `/api/documents/${DOC}/lifecycle/transitions`, {
      as: 'alice', body: { action: 'submit_for_review' },
    });
    expect(before.statusCode).toBe(403);

    await call('POST', `/api/documents/${DOC}/lifecycle/ownership`, { as: 'alice', body: {} });

    const after = await call('POST', `/api/documents/${DOC}/lifecycle/transitions`, {
      as: 'alice', body: { action: 'submit_for_review' },
    });
    expect(after.statusCode).toBe(200);
  });

  it('is idempotent for the owner and does not re-claim', async () => {
    await call('POST', `/api/documents/${DOC}/lifecycle/ownership`, { as: 'alice', body: {} });
    const again = await call('POST', `/api/documents/${DOC}/lifecycle/ownership`, {
      as: 'alice', body: {},
    });
    expect(again.statusCode).toBe(200);
    expect((again.body as { claimed: boolean }).claimed).toBe(false);
    expect(lifecycleRows.get(DOC)!['ownerId']).toBe('alice');
  });

  it('a second user cannot take over an already-claimed document (409)', async () => {
    await call('POST', `/api/documents/${DOC}/lifecycle/ownership`, { as: 'alice', body: {} });
    const res = await call('POST', `/api/documents/${DOC}/lifecycle/ownership`, {
      as: 'mallory', body: {},
    });
    expect(res.statusCode).toBe(409);
    expect((res.body as { code: string }).code).toBe('owner_already_established');
    expect(lifecycleRows.get(DOC)!['ownerId']).toBe('alice');
  });

  it('cannot override a document whose creator is known (409)', async () => {
    typedDocs.set(DOC, { createdBy: 'alice' });
    const res = await call('POST', `/api/documents/${DOC}/lifecycle/ownership`, {
      as: 'mallory', body: {},
    });
    expect(res.statusCode).toBe(409);
    expect((res.body as { code: string }).code).toBe('owner_already_established');
  });

  it('requires authentication (401)', async () => {
    const res = await call('POST', `/api/documents/${DOC}/lifecycle/ownership`, {
      as: undefined, body: {},
    });
    expect(res.statusCode).toBe(401);
    expect(lifecycleRows.has(DOC)).toBe(false);
  });

  it('preserves the existing status when claiming a non-draft document', async () => {
    lifecycleRows.set(DOC, {
      documentId: DOC, status: 'approved', ownerId: '',
      createdAt: '', updatedAt: '', updatedBy: '', version: 3, history: [],
    });
    // A row exists, so `initialize` loses; the claim must not silently succeed
    // against an existing row it did not write.
    const res = await call('POST', `/api/documents/${DOC}/lifecycle/ownership`, {
      as: 'mallory', body: {},
    });
    expect(res.statusCode).toBe(409);
    expect(lifecycleRows.get(DOC)!['status']).toBe('approved');
  });
});

describe('error bodies carry a distinct machine-readable code', () => {
  it('409 illegal_transition', async () => {
    await seedOwnedDocument('draft');
    const res = await call('POST', `/api/documents/${DOC}/lifecycle/transitions`, {
      as: 'alice', body: { action: 'reopen' },
    });
    expect(res.statusCode).toBe(409);
    expect((res.body as { code: string }).code).toBe('illegal_transition');
  });

  it('409 reviews_outstanding — same status, different code', async () => {
    await seedOwnedDocument('in_review');
    await seedReviewRequest('bob', { state: 'approved' });
    await seedReviewRequest('carol', { state: 'pending' });
    const res = await call('POST', `/api/documents/${DOC}/lifecycle/transitions`, {
      as: 'bob', body: { action: 'approve' },
    });
    expect(res.statusCode).toBe(409);
    expect((res.body as { code: string }).code).toBe('reviews_outstanding');
  });

  it('409 concurrent_modification — a lost compare-and-swap', async () => {
    await seedOwnedDocument('draft');
    // Move the row out from under the decision, exactly as a racing writer
    // would; the repository's CAS then fails.
    const spy = jest
      .spyOn(fakeLifecycleRepo, 'applyTransition')
      .mockImplementationOnce(async () => {
        const { LifecycleConcurrencyError } = jest.requireActual<
          typeof import('../../repositories/DocumentLifecycleRepository')
        >('../../repositories/DocumentLifecycleRepository');
        throw new LifecycleConcurrencyError();
      });
    const res = await call('POST', `/api/documents/${DOC}/lifecycle/transitions`, {
      as: 'alice', body: { action: 'submit_for_review' },
    });
    spy.mockRestore();
    expect(res.statusCode).toBe(409);
    expect((res.body as { code: string }).code).toBe('concurrent_modification');
  });

  it('the three 409s are mutually distinguishable without reading the prose', async () => {
    const codes = new Set<string>();

    await seedOwnedDocument('draft');
    codes.add(
      ((await call('POST', `/api/documents/${DOC}/lifecycle/transitions`, {
        as: 'alice', body: { action: 'reopen' },
      })).body as { code: string }).code,
    );

    lifecycleRows.clear();
    requestRows.clear();
    await seedOwnedDocument('in_review');
    await seedReviewRequest('bob', { state: 'pending' });
    codes.add(
      ((await call('POST', `/api/documents/${DOC}/lifecycle/transitions`, {
        as: 'bob', body: { action: 'approve' },
      })).body as { code: string }).code,
    );

    lifecycleRows.clear();
    requestRows.clear();
    await seedOwnedDocument('draft');
    const spy = jest
      .spyOn(fakeLifecycleRepo, 'applyTransition')
      .mockImplementationOnce(async () => {
        const { LifecycleConcurrencyError } = jest.requireActual<
          typeof import('../../repositories/DocumentLifecycleRepository')
        >('../../repositories/DocumentLifecycleRepository');
        throw new LifecycleConcurrencyError();
      });
    codes.add(
      ((await call('POST', `/api/documents/${DOC}/lifecycle/transitions`, {
        as: 'alice', body: { action: 'submit_for_review' },
      })).body as { code: string }).code,
    );
    spy.mockRestore();

    expect(codes).toEqual(
      new Set(['illegal_transition', 'reviews_outstanding', 'concurrent_modification']),
    );
  });

  it('403 not_authorized', async () => {
    await seedOwnedDocument('approved');
    const res = await call('POST', `/api/documents/${DOC}/lifecycle/transitions`, {
      as: 'mallory', body: { action: 'reopen' },
    });
    expect(res.statusCode).toBe(403);
    expect((res.body as { code: string }).code).toBe('not_authorized');
  });

  it('400 action_required when no action is supplied', async () => {
    await seedOwnedDocument('draft');
    const res = await call('POST', `/api/documents/${DOC}/lifecycle/transitions`, {
      as: 'alice', body: {},
    });
    expect(res.statusCode).toBe(400);
    expect((res.body as { code: string }).code).toBe('action_required');
  });

  it('400 unknown_action', async () => {
    await seedOwnedDocument('draft');
    const res = await call('POST', `/api/documents/${DOC}/lifecycle/transitions`, {
      as: 'alice', body: { action: 'finalize' },
    });
    expect(res.statusCode).toBe(400);
    expect((res.body as { code: string }).code).toBe('unknown_action');
  });

  it('keeps the human-readable `error` string alongside the code', async () => {
    await seedOwnedDocument('approved');
    const res = await call('POST', `/api/documents/${DOC}/lifecycle/transitions`, {
      as: 'mallory', body: { action: 'reopen' },
    });
    const body = res.body as { error: string; code: string };
    expect(body.error).toMatch(/Only the document owner/);
    expect(body.code).toBe('not_authorized');
  });
});

describe('GET /lifecycle does not leak the audit trail to anonymous callers', () => {
  beforeEach(async () => {
    await seedOwnedDocument('in_review');
    await seedReviewRequest('bob');
    lifecycleRows.set(DOC, {
      ...lifecycleRows.get(DOC)!,
      updatedBy: 'alice',
      lastAction: 'submit_for_review',
      lastReason: 'needs a rollback plan',
      history: [
        {
          from: 'draft', to: 'in_review', action: 'submit_for_review',
          actor: 'alice', at: '2026-01-01T00:00:00.000Z',
          reason: 'needs a rollback plan',
        },
      ],
    });
  });

  it('withholds history, actor ids, reasons and the reviewer roster', async () => {
    const res = await call('GET', `/api/documents/${DOC}/lifecycle`, { as: undefined });
    expect(res.statusCode).toBe(200);
    const body = res.body as {
      lifecycle: Record<string, unknown>;
      outstandingReviews: unknown[];
      availableActions: string[];
    };
    expect(body.lifecycle['history']).toBeUndefined();
    expect(body.lifecycle['ownerId']).toBeUndefined();
    expect(body.lifecycle['updatedBy']).toBeUndefined();
    expect(body.lifecycle['lastReason']).toBeUndefined();
    expect(body.outstandingReviews).toEqual([]);
    expect(body.availableActions).toEqual([]);
    // No user id survives anywhere in the serialized response.
    expect(JSON.stringify(body)).not.toMatch(/alice|bob|rollback/);
  });

  it('still tells an anonymous reader the status, so the badge keeps working', async () => {
    const res = await call('GET', `/api/documents/${DOC}/lifecycle`, { as: undefined });
    const body = res.body as { lifecycle: { status: string; version: number; implicit: boolean } };
    expect(body.lifecycle.status).toBe('in_review');
    expect(body.lifecycle.version).toBe(1);
    expect(body.lifecycle.implicit).toBe(false);
  });

  it('an authenticated caller still gets the full view', async () => {
    const res = await call('GET', `/api/documents/${DOC}/lifecycle`, { as: 'alice' });
    const body = res.body as {
      lifecycle: { history: unknown[]; ownerId: string; lastReason?: string };
      outstandingReviews: unknown[];
    };
    expect(body.lifecycle.history).toHaveLength(1);
    expect(body.lifecycle.ownerId).toBe('alice');
    expect(body.lifecycle.lastReason).toBe('needs a rollback plan');
    expect(body.outstandingReviews).toHaveLength(1);
  });
});
