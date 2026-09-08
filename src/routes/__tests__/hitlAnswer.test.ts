// POST /api/documents/:documentId/sections/:sectionId/items/:itemId/answer
//
// The endpoint that makes a `macro:hitl` callout answerable from somewhere
// other than the page it lives on. What is worth asserting is not that a happy
// path writes a row — it is every case where the endpoint REFUSES, because a
// refusal is what stops a surface from drawing a control that would silently
// discard an answer while an agent stays parked.

import express, { type NextFunction, type Request, type Response } from 'express';

const mockGetOne = jest.fn();
const mockUpdateItemFields = jest.fn();
const mockEmit = jest.fn().mockResolvedValue(undefined);

jest.mock('../../middleware/auth', () => ({
  requireAuth: (_req: Request, _res: Response, next: NextFunction) => next(),
}));

jest.mock('../../repositories', () => ({
  sectionItemRepo: {
    getOne: mockGetOne,
    updateItemFields: mockUpdateItemFields,
    createItem: jest.fn(),
    getItemsForSection: jest.fn(),
    deleteItemById: jest.fn(),
    ackItem: jest.fn(),
    getItemsAssignedTo: jest.fn(),
  },
}));

jest.mock('../../lib/event-catalog', () => ({
  publishSignal: mockEmit,
  publishDurable: jest.fn().mockResolvedValue(undefined),
  publishWithOutbox: jest.fn().mockResolvedValue(undefined),
  catalog: { declare: jest.fn(), publish: jest.fn().mockResolvedValue(undefined) },
  connectCatalog: jest.fn().mockResolvedValue(undefined),
  disconnectCatalog: jest.fn().mockResolvedValue(undefined),
}));

import { sectionItemsRouter, HITL_ITEM_CATEGORY } from '../sectionItems';
import { errorHandler } from '../../middleware/error-handler';

// ---------------------------------------------------------------------------
// Rig — the same shape `sectionItems.test.ts` uses.
// ---------------------------------------------------------------------------

interface MockRes {
  statusCode: number;
  body: unknown;
  ended: boolean;
  finished: Promise<void>;
  status(code: number): MockRes;
  json(body: unknown): MockRes;
  send(body?: unknown): MockRes;
  setHeader(n: string, v: unknown): MockRes;
  getHeader(n: string): unknown;
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
    end() { this.ended = true; resolve(); return this; },
  };
  return r;
}

function buildApp(): express.Express {
  const app = express();
  app.use(express.json());
  app.use('/documents/:documentId/sections/:sectionId/items', sectionItemsRouter);
  app.use(errorHandler);
  return app;
}

async function answer(
  body: unknown,
  userId = 'dev-bob',
): Promise<MockRes> {
  const app = buildApp();
  const url = '/documents/doc-1/sections/_macro/items/hitl-1/answer';
  const res = mockRes();
  const req = {
    method: 'POST',
    url,
    originalUrl: url,
    path: url,
    headers: { 'content-type': 'application/json' },
    query: {},
    body,
    user: { sub: userId },
    params: {},
  } as unknown as Request;
  await new Promise<void>((resolveOuter, reject) => {
    const done = (err?: unknown): void => {
      if (err) { reject(err); return; }
      if (!res.ended) res.status(404).json({ error: 'route not found' });
      resolveOuter();
    };
    (app as unknown as (q: Request, s: Response, n: NextFunction) => void)(
      req, res as unknown as Response, done,
    );
    res.finished.then(() => resolveOuter()).catch(reject);
  });
  return res;
}

/** The mirror row a callout addressed to dev-bob writes. */
const WAITING_ROW = {
  sectionKey: 'doc-1:_macro',
  itemId: 'hitl-1',
  documentId: 'doc-1',
  sectionId: '_macro',
  text: 'Ship 2.190.0 now, or hold for the changelog?',
  assignee: 'dev-bob',
  category: HITL_ITEM_CATEGORY,
  status: 'pending',
  createdAt: '2026-09-07T00:00:00.000Z',
  updatedAt: '2026-09-07T00:00:00.000Z',
};

beforeEach(() => {
  jest.clearAllMocks();
  mockGetOne.mockResolvedValue(WAITING_ROW);
  mockUpdateItemFields.mockImplementation(async (_d, _s, _i, updates) => ({
    ...WAITING_ROW,
    ...updates,
  }));
});

// ---------------------------------------------------------------------------

describe('answering a callout', () => {
  it('records the answer, who gave it and when, and closes the row in one write', async () => {
    const res = await answer({ answer: 'ship it' });

    expect(res.statusCode).toBe(200);
    expect(mockUpdateItemFields).toHaveBeenCalledTimes(1);
    const [documentId, sectionId, itemId, updates] = mockUpdateItemFields.mock.calls[0]!;
    expect([documentId, sectionId, itemId]).toEqual(['doc-1', '_macro', 'hitl-1']);
    expect(updates).toMatchObject({
      status: 'done',
      notes: 'ship it',
      ackedBy: 'dev-bob',
    });
    // Terminal, so the next `GET /api/items/mine?status=pending` no longer
    // returns it — which is what retires the inbox row.
    expect(updates.status).toBe('done');
    expect(typeof updates.ackedAt).toBe('string');
  });

  it('tells an open editor, so the page can catch its own body up', async () => {
    await answer({ answer: 'hold' });
    expect(mockEmit).toHaveBeenCalledTimes(1);
    const [channel, event, payload] = mockEmit.mock.calls[0]!;
    expect(channel).toBe('doc:doc-1');
    expect(event).toBe('doc:section_updated');
    expect(payload).toMatchObject({
      type: 'section:item:answered',
      itemId: 'hitl-1',
      answer: 'hold',
      answeredBy: 'dev-bob',
    });
  });

  it('trims the answer rather than storing whitespace as a decision', async () => {
    await answer({ answer: '  ship it \n' });
    expect(mockUpdateItemFields.mock.calls[0]![3].notes).toBe('ship it');
  });
});

describe('refusing to answer', () => {
  it('refuses an empty answer', async () => {
    const res = await answer({ answer: '   ' });
    expect(res.statusCode).toBe(400);
    expect(mockUpdateItemFields).not.toHaveBeenCalled();
  });

  it('refuses a row that does not exist', async () => {
    mockGetOne.mockResolvedValue(null);
    const res = await answer({ answer: 'ship it' });
    expect(res.statusCode).toBe(404);
  });

  it('refuses to be pointed at an ordinary task', async () => {
    // Otherwise this endpoint becomes a second, unguarded way to close any
    // section item in the product.
    mockGetOne.mockResolvedValue({ ...WAITING_ROW, category: undefined });
    const res = await answer({ answer: 'ship it' });
    expect(res.statusCode).toBe(400);
    expect(mockUpdateItemFields).not.toHaveBeenCalled();
  });

  it('refuses anyone but the person it is addressed to', async () => {
    const res = await answer({ answer: 'ship it' }, 'dev-ana');
    expect(res.statusCode).toBe(403);
    expect(mockUpdateItemFields).not.toHaveBeenCalled();
  });

  it('refuses EVERYONE when the callout is addressed to nobody', async () => {
    // Not "anyone may answer it". An unaddressed question is one everybody
    // assumes somebody else took; letting a passer-by close it is worse than
    // leaving it open.
    mockGetOne.mockResolvedValue({ ...WAITING_ROW, assignee: undefined });
    const res = await answer({ answer: 'ship it' });
    expect(res.statusCode).toBe(403);
  });

  it('refuses a second answer', async () => {
    mockGetOne.mockResolvedValue({ ...WAITING_ROW, status: 'done', notes: 'hold' });
    const res = await answer({ answer: 'ship it' }, 'dev-bob');
    expect(res.statusCode).toBe(409);
    expect(mockUpdateItemFields).not.toHaveBeenCalled();
  });

  it('refuses a callout whose asker gave up and retired the row', async () => {
    // This is how expiry reaches this endpoint AT ALL: the row has no
    // `expiresAt` field, so "the asker stopped waiting" is expressed by the
    // asker moving the row out of `pending`.
    mockGetOne.mockResolvedValue({ ...WAITING_ROW, status: 'rejected' });
    const res = await answer({ answer: 'ship it' });
    expect(res.statusCode).toBe(409);
  });

  it('accepts `open`, which is what createItem defaults to', async () => {
    mockGetOne.mockResolvedValue({ ...WAITING_ROW, status: 'open' });
    const res = await answer({ answer: 'ship it' });
    expect(res.statusCode).toBe(200);
  });
});
