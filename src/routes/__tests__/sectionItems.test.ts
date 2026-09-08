// social-api/src/routes/__tests__/sectionItems.test.ts
//
// Unit tests for sectionItemsRouter + myItemsRouter.

import express, { type NextFunction, type Request, type Response } from 'express';

// --- module-level mocks (must precede route import) -------------------------

const mockCreateItem = jest.fn();
const mockGetItemsForSection = jest.fn();
const mockUpdateItemFields = jest.fn();
const mockDeleteItemById = jest.fn();
const mockAckItem = jest.fn();
const mockGetItemsAssignedTo = jest.fn();
const mockEmit = jest.fn().mockResolvedValue(undefined);

jest.mock('../../middleware/auth', () => ({
  requireAuth: (_req: Request, _res: Response, next: NextFunction) => {
    // Tests inject req.user in buildApp(); just pass through.
    next();
  },
}));

jest.mock('../../repositories', () => ({
  sectionItemRepo: {
    createItem: mockCreateItem,
    getItemsForSection: mockGetItemsForSection,
    updateItemFields: mockUpdateItemFields,
    deleteItemById: mockDeleteItemById,
    ackItem: mockAckItem,
    getItemsAssignedTo: mockGetItemsAssignedTo,
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
  publishSignal: mockEmit,
  publishDurable: jest.fn().mockResolvedValue(undefined),
  publishWithOutbox: jest.fn().mockResolvedValue(undefined),
  connectCatalog: jest.fn().mockResolvedValue(undefined),
  disconnectCatalog: jest.fn().mockResolvedValue(undefined),
}));

import { sectionItemsRouter, myItemsRouter } from '../sectionItems';
import { errorHandler } from '../../middleware/error-handler';

// ---------------------------------------------------------------------------
// Test rig
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
    json(b) { this.body = b; this.ended = true; resolve(); return this; },
    send(b) { this.body = b; this.ended = true; resolve(); return this; },
    setHeader() { return this; },
    getHeader() { return undefined; },
    end(..._args) { this.ended = true; resolve(); return this; },
  };
  return r;
}

async function run(
  app: express.Express,
  method: string,
  url: string,
  opts: { body?: unknown; userId?: string; query?: Record<string, string> } = {},
): Promise<MockRes> {
  const res = mockRes();
  const [pathname, qs] = url.split('?', 2);
  const query: Record<string, string> = { ...(opts.query ?? {}) };
  if (qs) {
    for (const part of qs.split('&')) {
      const [k, v] = part.split('=', 2);
      if (k) query[decodeURIComponent(k)] = v ? decodeURIComponent(v) : '';
    }
  }
  const req = {
    method: method.toUpperCase(),
    url,
    originalUrl: url,
    path: pathname,
    headers: { 'content-type': 'application/json' },
    query,
    body: opts.body ?? {},
    user: { sub: opts.userId ?? 'user-1' },
    params: {},
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
  app.use((req, _res, next) => {
    if (!(req as Request & { user?: unknown }).user) {
      (req as Request & { user?: { sub: string } }).user = { sub: 'user-1' };
    }
    next();
  });
  app.use('/documents/:documentId/sections/:sectionId/items', sectionItemsRouter);
  app.use('/items', myItemsRouter);
  app.use(errorHandler);
  return app;
}

const sampleItem = {
  itemId: 'item-1',
  documentId: 'doc-1',
  sectionId: 'sec-1',
  text: 'Buy groceries',
  status: 'pending',
  createdAt: '2026-01-01T00:00:00.000Z',
};

beforeEach(() => {
  jest.clearAllMocks();
});

// ---------------------------------------------------------------------------
// POST /documents/:documentId/sections/:sectionId/items
// ---------------------------------------------------------------------------

describe('POST /documents/:documentId/sections/:sectionId/items', () => {
  const app = buildApp();

  test('201 + item on success', async () => {
    mockCreateItem.mockResolvedValueOnce(sampleItem);

    const res = await run(app, 'POST', '/documents/doc-1/sections/sec-1/items', {
      body: { text: 'Buy groceries' },
    });

    expect(res.statusCode).toBe(201);
    expect((res.body as { item: unknown }).item).toEqual(sampleItem);
    expect(mockCreateItem).toHaveBeenCalledWith(
      expect.objectContaining({ documentId: 'doc-1', sectionId: 'sec-1', text: 'Buy groceries' }),
    );
  });

  test('allows empty text (item created inline)', async () => {
    mockCreateItem.mockResolvedValueOnce({ ...sampleItem, text: '' });

    const res = await run(app, 'POST', '/documents/doc-1/sections/sec-1/items', {
      body: {},
    });

    expect(res.statusCode).toBe(201);
    expect(mockCreateItem).toHaveBeenCalledWith(expect.objectContaining({ text: '' }));
  });
});

// ---------------------------------------------------------------------------
// GET /documents/:documentId/sections/:sectionId/items
// ---------------------------------------------------------------------------

describe('GET /documents/:documentId/sections/:sectionId/items', () => {
  const app = buildApp();

  test('200 + items array', async () => {
    mockGetItemsForSection.mockResolvedValueOnce([sampleItem]);

    const res = await run(app, 'GET', '/documents/doc-1/sections/sec-1/items');

    expect(res.statusCode).toBe(200);
    expect((res.body as { items: unknown[] }).items).toHaveLength(1);
    expect(mockGetItemsForSection).toHaveBeenCalledWith('doc-1', 'sec-1');
  });
});

// ---------------------------------------------------------------------------
// PATCH /documents/:documentId/sections/:sectionId/items/:itemId
// ---------------------------------------------------------------------------

describe('PATCH /documents/:documentId/sections/:sectionId/items/:itemId', () => {
  const app = buildApp();

  test('200 + updated item', async () => {
    const updated = { ...sampleItem, text: 'Buy more groceries' };
    mockUpdateItemFields.mockResolvedValueOnce(updated);

    const res = await run(app, 'PATCH', '/documents/doc-1/sections/sec-1/items/item-1', {
      body: { text: 'Buy more groceries' },
    });

    expect(res.statusCode).toBe(200);
    expect((res.body as { item: unknown }).item).toEqual(updated);
    expect(mockUpdateItemFields).toHaveBeenCalledWith(
      'doc-1', 'sec-1', 'item-1',
      expect.objectContaining({ text: 'Buy more groceries' }),
    );
  });

  test('400 when no fields provided', async () => {
    const res = await run(app, 'PATCH', '/documents/doc-1/sections/sec-1/items/item-1', {
      body: {},
    });

    expect(res.statusCode).toBe(400);
    expect(mockUpdateItemFields).not.toHaveBeenCalled();
  });
});

// ---------------------------------------------------------------------------
// DELETE /documents/:documentId/sections/:sectionId/items/:itemId
// ---------------------------------------------------------------------------

describe('DELETE /documents/:documentId/sections/:sectionId/items/:itemId', () => {
  const app = buildApp();

  test('204 on success', async () => {
    mockDeleteItemById.mockResolvedValueOnce(undefined);

    const res = await run(app, 'DELETE', '/documents/doc-1/sections/sec-1/items/item-1');

    expect(res.statusCode).toBe(204);
    expect(mockDeleteItemById).toHaveBeenCalledWith('doc-1', 'sec-1', 'item-1');
  });
});

// ---------------------------------------------------------------------------
// POST /documents/:documentId/sections/:sectionId/items/:itemId/ack
// ---------------------------------------------------------------------------

describe('POST .../items/:itemId/ack', () => {
  const app = buildApp();

  test('200 + acked item', async () => {
    const ackedItem = { ...sampleItem, ackedBy: 'user-1', ackedAt: '2026-01-01T00:01:00.000Z' };
    mockAckItem.mockResolvedValueOnce(ackedItem);

    const res = await run(app, 'POST', '/documents/doc-1/sections/sec-1/items/item-1/ack', {
      userId: 'user-1',
    });

    expect(res.statusCode).toBe(200);
    expect(mockAckItem).toHaveBeenCalledWith('doc-1', 'sec-1', 'item-1', 'user-1', expect.any(String));
  });
});

// ---------------------------------------------------------------------------
// GET /items/mine
// ---------------------------------------------------------------------------

describe('GET /items/mine', () => {
  const app = buildApp();

  test('200 + items assigned to the caller', async () => {
    mockGetItemsAssignedTo.mockResolvedValueOnce([sampleItem]);

    const res = await run(app, 'GET', '/items/mine', { userId: 'user-1' });

    expect(res.statusCode).toBe(200);
    expect((res.body as { items: unknown[] }).items).toHaveLength(1);
    expect(mockGetItemsAssignedTo).toHaveBeenCalledWith('user-1', undefined);
  });

  test('passes status query param through', async () => {
    mockGetItemsAssignedTo.mockResolvedValueOnce([]);

    await run(app, 'GET', '/items/mine?status=done', { userId: 'user-1' });

    expect(mockGetItemsAssignedTo).toHaveBeenCalledWith('user-1', 'done');
  });
});

// ---------------------------------------------------------------------------
// The label/identity split
//
// The bug: the app assigned by display name and this endpoint asked for a
// Cognito sub, so nothing assigned was ever findable. The fix is two fields,
// and these tests pin the halves that made it wrong.
// ---------------------------------------------------------------------------

describe('assignee label vs assignee identity', () => {
  const app = buildApp();

  test('POST stores the label and the identity as separate fields', async () => {
    mockCreateItem.mockResolvedValueOnce({ ...sampleItem, assignee: 'Alice Chen', assigneeId: 'dev-alice' });

    const res = await run(app, 'POST', '/documents/doc-1/sections/sec-1/items', {
      userId: 'user-1',
      body: { text: 'Ship it', assignee: 'Alice Chen', assigneeId: 'dev-alice' },
    });

    expect(res.statusCode).toBe(201);
    expect(mockCreateItem).toHaveBeenCalledWith(
      expect.objectContaining({ assignee: 'Alice Chen', assigneeId: 'dev-alice' }),
    );
  });

  test('PATCH carries the identity alongside the label', async () => {
    mockUpdateItemFields.mockResolvedValueOnce({ ...sampleItem, assignee: 'Bob Martinez', assigneeId: 'dev-bob' });

    await run(app, 'PATCH', '/documents/doc-1/sections/sec-1/items/item-1', {
      userId: 'user-1',
      body: { assignee: 'Bob Martinez', assigneeId: 'dev-bob' },
    });

    expect(mockUpdateItemFields).toHaveBeenCalledWith(
      'doc-1',
      'sec-1',
      'item-1',
      expect.objectContaining({ assignee: 'Bob Martinez', assigneeId: 'dev-bob' }),
    );
  });

  test('unassigning REMOVEs both fields instead of being a silent no-op', async () => {
    mockUpdateItemFields.mockResolvedValueOnce({ ...sampleItem });

    const res = await run(app, 'PATCH', '/documents/doc-1/sections/sec-1/items/item-1', {
      userId: 'user-1',
      body: { assignee: '', assigneeId: '' },
    });

    // Empty strings used to fail a truthiness test and vanish, leaving the row
    // assigned to whoever had it. They are now REMOVE instructions — and they
    // must not reach DynamoDB as '' either, since both are GSI hash keys.
    expect(res.statusCode).toBe(200);
    expect(mockUpdateItemFields).toHaveBeenCalledWith(
      'doc-1',
      'sec-1',
      'item-1',
      expect.objectContaining({ assignee: null, assigneeId: null }),
    );
  });

  test('a cleared assignee reaches peers as an empty string, never as null', async () => {
    mockUpdateItemFields.mockResolvedValueOnce({ ...sampleItem });
    mockEmit.mockClear();

    await run(app, 'PATCH', '/documents/doc-1/sections/sec-1/items/item-1', {
      userId: 'user-1',
      body: { assignee: '' },
    });

    const payload = mockEmit.mock.calls[0][2] as { updates: Record<string, unknown> };
    expect(payload.updates.assignee).toBe('');
  });
});
