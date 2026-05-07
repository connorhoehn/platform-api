// Integration tests for sectionItems + sectionReviews routes.
//
// Both route groups are Phase 51 document-collaboration surfaces that had
// zero test coverage. The test fixture mocks repositories, auth, and
// broadcast at module level so we exercise the full route logic without
// touching DDB or Redis.
//
// Coverage:
//   sectionItems:
//     POST   /api/documents/:docId/sections/:secId/items           create
//     GET    /api/documents/:docId/sections/:secId/items           list
//     PATCH  /api/documents/:docId/sections/:secId/items/:itemId   update
//     DELETE /api/documents/:docId/sections/:secId/items/:itemId   delete
//     POST   /api/documents/:docId/sections/:secId/items/:itemId/ack  acknowledge
//     GET    /api/items/mine                                       cross-doc assignee
//
//   sectionReviews:
//     POST   /api/documents/:docId/sections/:secId/reviews         submit
//     GET    /api/documents/:docId/sections/:secId/reviews         list for section
//     GET    /api/documents/:docId/reviews                         list for document
//     GET    /api/reviews/mine                                     cross-doc user reviews

import express, { type NextFunction, type Request, type Response } from 'express';

// ---------------------------------------------------------------------------
// In-memory stores — wired into the mock factories below
// ---------------------------------------------------------------------------

interface ItemRecord extends Record<string, unknown> {
  sectionKey: string;
  itemId: string;
  documentId: string;
  sectionId: string;
  text: string;
}

interface ReviewRecord extends Record<string, unknown> {
  documentId: string;
  reviewKey: string;
  sectionId: string;
  userId: string;
  displayName: string;
  status: string;
}

const itemStore: Record<string, ItemRecord> = {};
const reviewStore: Record<string, ReviewRecord> = {};

let itemIdCounter = 0;

// ---------------------------------------------------------------------------
// Module-level mocks — must come before route imports
// ---------------------------------------------------------------------------

// Auth — bypass Cognito; inject req.user from the test
jest.mock('../../middleware/auth', () => ({
  requireAuth: (_req: Request, _res: Response, next: NextFunction) => {
    // req.user is already set by the test harness `run()` helper
    next();
  },
  optionalAuth: (_req: Request, _res: Response, next: NextFunction) => {
    next();
  },
}));

// Broadcast — no-op; we don't test WebSocket fanout here
jest.mock('../../services/broadcast', () => ({
  broadcastService: { emit: jest.fn() },
}));

jest.mock('../../repositories', () => ({
  sectionItemRepo: {
    createItem: jest.fn(async (input: Record<string, unknown>) => {
      itemIdCounter += 1;
      const itemId = `item-${itemIdCounter}`;
      const now = new Date().toISOString();
      const item: ItemRecord = {
        sectionKey: `${input.documentId}:${input.sectionId}`,
        itemId,
        documentId: input.documentId as string,
        sectionId: input.sectionId as string,
        text: (input.text as string) ?? '',
        ...(input.assignee !== undefined && { assignee: input.assignee }),
        ...(input.priority !== undefined && { priority: input.priority }),
        ...(input.dueDate !== undefined && { dueDate: input.dueDate }),
        ...(input.category !== undefined && { category: input.category }),
        status: 'open',
        createdAt: now,
        updatedAt: now,
      };
      itemStore[itemId] = item;
      return item;
    }),
    getItemsForSection: jest.fn(async (documentId: string, sectionId: string) => {
      const key = `${documentId}:${sectionId}`;
      return Object.values(itemStore).filter((i) => i.sectionKey === key);
    }),
    updateItemFields: jest.fn(async (
      documentId: string,
      sectionId: string,
      itemId: string,
      updates: Record<string, unknown>,
    ) => {
      const item = itemStore[itemId];
      if (!item || item.sectionKey !== `${documentId}:${sectionId}`) return undefined;
      const merged = { ...item, ...updates, updatedAt: new Date().toISOString() };
      itemStore[itemId] = merged as ItemRecord;
      return merged;
    }),
    deleteItemById: jest.fn(async (_documentId: string, _sectionId: string, itemId: string) => {
      delete itemStore[itemId];
    }),
    ackItem: jest.fn(async (
      documentId: string,
      sectionId: string,
      itemId: string,
      ackedBy: string,
      ackedAt: string,
    ) => {
      const item = itemStore[itemId];
      if (!item || item.sectionKey !== `${documentId}:${sectionId}`) return undefined;
      const merged = { ...item, ackedBy, ackedAt, updatedAt: new Date().toISOString() };
      itemStore[itemId] = merged as ItemRecord;
      return merged;
    }),
    getItemsByAssignee: jest.fn(async (assignee: string, status?: string) => {
      return Object.values(itemStore).filter((i) => {
        if (i.assignee !== assignee) return false;
        if (status && i.status !== status) return false;
        return true;
      });
    }),
  },
  sectionReviewRepo: {
    submitReview: jest.fn(async (review: Record<string, unknown>) => {
      const reviewKey = `${review.sectionId}:${review.userId}`;
      const item: ReviewRecord = {
        documentId: review.documentId as string,
        reviewKey,
        sectionId: review.sectionId as string,
        userId: review.userId as string,
        displayName: review.displayName as string,
        status: review.status as string,
        timestamp: review.timestamp as string,
        ...(review.comment !== undefined && { comment: review.comment }),
      };
      reviewStore[reviewKey] = item;
      return item;
    }),
    getReviewsForSection: jest.fn(async (documentId: string, sectionId: string) => {
      return Object.values(reviewStore).filter(
        (r) => r.documentId === documentId && r.sectionId === sectionId,
      );
    }),
    getReviewsForDocument: jest.fn(async (documentId: string) => {
      return Object.values(reviewStore).filter((r) => r.documentId === documentId);
    }),
    getUserReviews: jest.fn(async (userId: string) => {
      return Object.values(reviewStore).filter((r) => r.userId === userId);
    }),
  },
  profileRepo: {
    getProfile: jest.fn(async (userId: string) => {
      // Return a fake profile so reviews get a displayName
      return { userId, displayName: `User ${userId}` };
    }),
  },
}));

import { sectionItemsRouter, myItemsRouter } from '../sectionItems';
import { sectionReviewsRouter, documentReviewsRouter, myReviewsRouter } from '../sectionReviews';
import { errorHandler } from '../../middleware/error-handler';

// ---------------------------------------------------------------------------
// Test harness — mirrors the req/res rig in documentTypes.test.ts
// ---------------------------------------------------------------------------

interface MockRes {
  statusCode: number;
  body: unknown;
  ended: boolean;
  finished: Promise<void>;
  status(code: number): MockRes;
  json(body: unknown): MockRes;
  send(body?: unknown): MockRes;
  setHeader(_name: string, _value: unknown): MockRes;
  getHeader(_name: string): unknown;
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
  // Mount with the same param structure as routes/index.ts
  app.use('/api/documents/:documentId/sections/:sectionId/items', sectionItemsRouter);
  app.use('/api/items', myItemsRouter);
  app.use('/api/documents/:documentId/sections/:sectionId/reviews', sectionReviewsRouter);
  app.use('/api/documents/:documentId/reviews', documentReviewsRouter);
  app.use('/api/reviews', myReviewsRouter);
  app.use(errorHandler);
  return app;
}

beforeEach(() => {
  for (const k of Object.keys(itemStore)) delete itemStore[k];
  for (const k of Object.keys(reviewStore)) delete reviewStore[k];
  itemIdCounter = 0;
});

// ---------------------------------------------------------------------------
// Section Items
// ---------------------------------------------------------------------------

describe('sectionItems — CRUD', () => {
  const DOC = 'doc-1';
  const SEC = 'sec-a';
  const BASE = `/api/documents/${DOC}/sections/${SEC}/items`;

  test('POST creates an item and returns 201', async () => {
    const res = await run(buildApp(), 'POST', BASE, {
      body: { text: 'Fix the widget', assignee: 'alice', priority: 'high' },
    });
    expect(res.statusCode).toBe(201);
    const body = res.body as { item: { itemId: string; text: string; assignee: string; priority: string } };
    expect(body.item.itemId).toMatch(/^item-/);
    expect(body.item.text).toBe('Fix the widget');
    expect(body.item.assignee).toBe('alice');
    expect(body.item.priority).toBe('high');
  });

  test('POST allows empty text (inline creation pattern)', async () => {
    const res = await run(buildApp(), 'POST', BASE, { body: {} });
    expect(res.statusCode).toBe(201);
    const body = res.body as { item: { text: string } };
    expect(body.item.text).toBe('');
  });

  test('GET lists items for the section', async () => {
    const app = buildApp();
    await run(app, 'POST', BASE, { body: { text: 'Item A' } });
    await run(app, 'POST', BASE, { body: { text: 'Item B' } });

    const res = await run(app, 'GET', BASE);
    expect(res.statusCode).toBe(200);
    const body = res.body as { items: unknown[] };
    expect(body.items).toHaveLength(2);
  });

  test('PATCH updates fields and returns the merged item', async () => {
    const app = buildApp();
    const createRes = await run(app, 'POST', BASE, { body: { text: 'Draft' } });
    const { itemId } = (createRes.body as { item: { itemId: string } }).item;

    const patchRes = await run(app, 'PATCH', `${BASE}/${itemId}`, {
      body: { text: 'Final', status: 'done', notes: 'Resolved' },
    });
    expect(patchRes.statusCode).toBe(200);
    const patched = (patchRes.body as { item: Record<string, unknown> }).item;
    expect(patched.text).toBe('Final');
    expect(patched.status).toBe('done');
    expect(patched.notes).toBe('Resolved');
  });

  test('PATCH rejects empty update body (no fields to update)', async () => {
    const app = buildApp();
    const createRes = await run(app, 'POST', BASE, { body: { text: 'x' } });
    const { itemId } = (createRes.body as { item: { itemId: string } }).item;

    const res = await run(app, 'PATCH', `${BASE}/${itemId}`, { body: {} });
    expect(res.statusCode).toBe(400);
  });

  test('DELETE removes an item and returns 204', async () => {
    const app = buildApp();
    const createRes = await run(app, 'POST', BASE, { body: { text: 'To delete' } });
    const { itemId } = (createRes.body as { item: { itemId: string } }).item;

    const delRes = await run(app, 'DELETE', `${BASE}/${itemId}`);
    expect(delRes.statusCode).toBe(204);

    // Verify it's gone
    const listRes = await run(app, 'GET', BASE);
    expect((listRes.body as { items: unknown[] }).items).toHaveLength(0);
  });
});

describe('sectionItems — acknowledgment', () => {
  const DOC = 'doc-2';
  const SEC = 'sec-b';
  const BASE = `/api/documents/${DOC}/sections/${SEC}/items`;

  test('POST /:itemId/ack records ackedBy + ackedAt and returns 200', async () => {
    const app = buildApp();
    const createRes = await run(app, 'POST', BASE, { body: { text: 'Needs ack' } });
    const { itemId } = (createRes.body as { item: { itemId: string } }).item;

    const ackRes = await run(app, 'POST', `${BASE}/${itemId}/ack`, { userId: 'reviewer-1' });
    expect(ackRes.statusCode).toBe(200);
    const acked = (ackRes.body as { item: Record<string, unknown> }).item;
    expect(acked.ackedBy).toBe('reviewer-1');
    expect(acked.ackedAt).toMatch(/^\d{4}-/); // ISO date prefix
  });
});

describe('sectionItems — /items/mine cross-document query', () => {
  test('GET /api/items/mine returns items assigned to the calling user', async () => {
    const app = buildApp();
    // Create items with different assignees
    await run(app, 'POST', '/api/documents/d1/sections/s1/items', {
      body: { text: 'For alice', assignee: 'alice' },
    });
    await run(app, 'POST', '/api/documents/d2/sections/s2/items', {
      body: { text: 'For bob', assignee: 'bob' },
    });
    await run(app, 'POST', '/api/documents/d3/sections/s3/items', {
      body: { text: 'Also for alice', assignee: 'alice' },
    });

    const res = await run(app, 'GET', '/api/items/mine', { userId: 'alice' });
    expect(res.statusCode).toBe(200);
    const body = res.body as { items: { text: string }[] };
    expect(body.items).toHaveLength(2);
    expect(body.items.map((i) => i.text).sort()).toEqual(['Also for alice', 'For alice']);
  });

  test('GET /api/items/mine?status=open filters by status', async () => {
    const app = buildApp();
    await run(app, 'POST', '/api/documents/d1/sections/s1/items', {
      body: { text: 'Open', assignee: 'alice' },
    });
    // Patch one to done
    const cr = await run(app, 'POST', '/api/documents/d1/sections/s1/items', {
      body: { text: 'Done', assignee: 'alice' },
    });
    const { itemId } = (cr.body as { item: { itemId: string } }).item;
    await run(app, 'PATCH', `/api/documents/d1/sections/s1/items/${itemId}`, {
      body: { status: 'done' },
    });

    const res = await run(app, 'GET', '/api/items/mine?status=open', { userId: 'alice' });
    expect(res.statusCode).toBe(200);
    const body = res.body as { items: { text: string }[] };
    expect(body.items).toHaveLength(1);
    expect(body.items[0].text).toBe('Open');
  });
});

// ---------------------------------------------------------------------------
// Section Reviews
// ---------------------------------------------------------------------------

describe('sectionReviews — submit + list', () => {
  const DOC = 'doc-10';
  const SEC = 'sec-x';
  const BASE = `/api/documents/${DOC}/sections/${SEC}/reviews`;

  test('POST submits a review with status and comment, returns 201', async () => {
    const res = await run(buildApp(), 'POST', BASE, {
      body: { status: 'approved', comment: 'Looks good' },
      userId: 'reviewer-a',
    });
    expect(res.statusCode).toBe(201);
    const body = res.body as { review: { userId: string; displayName: string; status: string; comment: string } };
    expect(body.review.userId).toBe('reviewer-a');
    expect(body.review.displayName).toBe('User reviewer-a');
    expect(body.review.status).toBe('approved');
    expect(body.review.comment).toBe('Looks good');
  });

  test('POST rejects missing status with 400', async () => {
    const res = await run(buildApp(), 'POST', BASE, {
      body: { comment: 'no status' },
    });
    expect(res.statusCode).toBe(400);
  });

  test('GET lists reviews for the section', async () => {
    const app = buildApp();
    await run(app, 'POST', BASE, { body: { status: 'approved' }, userId: 'u1' });
    await run(app, 'POST', BASE, { body: { status: 'changes_requested' }, userId: 'u2' });

    const res = await run(app, 'GET', BASE);
    expect(res.statusCode).toBe(200);
    const body = res.body as { reviews: unknown[] };
    expect(body.reviews).toHaveLength(2);
  });

  test('POST without comment omits comment from the stored review', async () => {
    const res = await run(buildApp(), 'POST', BASE, {
      body: { status: 'approved' },
      userId: 'u3',
    });
    expect(res.statusCode).toBe(201);
    const body = res.body as { review: Record<string, unknown> };
    expect(body.review.comment).toBeUndefined();
  });
});

describe('sectionReviews — document-level + user-level queries', () => {
  test('GET /api/documents/:docId/reviews returns all reviews for the document', async () => {
    const app = buildApp();
    // Reviews in two different sections of the same document
    await run(app, 'POST', '/api/documents/doc-20/sections/s1/reviews', {
      body: { status: 'approved' }, userId: 'u1',
    });
    await run(app, 'POST', '/api/documents/doc-20/sections/s2/reviews', {
      body: { status: 'rejected' }, userId: 'u2',
    });
    // Review in a different document (should not appear)
    await run(app, 'POST', '/api/documents/doc-other/sections/s1/reviews', {
      body: { status: 'approved' }, userId: 'u3',
    });

    const res = await run(app, 'GET', '/api/documents/doc-20/reviews');
    expect(res.statusCode).toBe(200);
    const body = res.body as { reviews: { sectionId: string }[] };
    expect(body.reviews).toHaveLength(2);
  });

  test('GET /api/reviews/mine returns reviews submitted by the calling user', async () => {
    const app = buildApp();
    await run(app, 'POST', '/api/documents/d1/sections/s1/reviews', {
      body: { status: 'approved' }, userId: 'alice',
    });
    await run(app, 'POST', '/api/documents/d2/sections/s2/reviews', {
      body: { status: 'rejected' }, userId: 'bob',
    });
    await run(app, 'POST', '/api/documents/d3/sections/s3/reviews', {
      body: { status: 'changes_requested' }, userId: 'alice',
    });

    const res = await run(app, 'GET', '/api/reviews/mine', { userId: 'alice' });
    expect(res.statusCode).toBe(200);
    const body = res.body as { reviews: { userId: string }[] };
    expect(body.reviews).toHaveLength(2);
    expect(body.reviews.every((r) => r.userId === 'alice')).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// End-to-end loop: create items + review in one section, verify round-trip
// ---------------------------------------------------------------------------

describe('end-to-end: items + review in one section', () => {
  test('create two items, ack one, review the section, fetch everything back', async () => {
    const app = buildApp();
    const DOC = 'doc-e2e';
    const SEC = 'sec-e2e';
    const BASE = `/api/documents/${DOC}/sections/${SEC}`;

    // 1. Create two items
    const i1 = await run(app, 'POST', `${BASE}/items`, {
      body: { text: 'Action item A', assignee: 'tester' },
    });
    expect(i1.statusCode).toBe(201);
    const item1Id = (i1.body as { item: { itemId: string } }).item.itemId;

    await run(app, 'POST', `${BASE}/items`, {
      body: { text: 'Action item B' },
    });

    // 2. Acknowledge item 1
    const ackRes = await run(app, 'POST', `${BASE}/items/${item1Id}/ack`, { userId: 'tester' });
    expect(ackRes.statusCode).toBe(200);

    // 3. Submit a review
    const revRes = await run(app, 'POST', `${BASE}/reviews`, {
      body: { status: 'approved', comment: 'All items addressed' },
      userId: 'tester',
    });
    expect(revRes.statusCode).toBe(201);

    // 4. Fetch items — should have 2
    const items = await run(app, 'GET', `${BASE}/items`);
    expect((items.body as { items: unknown[] }).items).toHaveLength(2);

    // 5. Fetch reviews — should have 1
    const reviews = await run(app, 'GET', `${BASE}/reviews`);
    expect((reviews.body as { reviews: unknown[] }).reviews).toHaveLength(1);

    // 6. Fetch document-level reviews
    const docReviews = await run(app, 'GET', `/api/documents/${DOC}/reviews`);
    expect((docReviews.body as { reviews: unknown[] }).reviews).toHaveLength(1);
  });
});
