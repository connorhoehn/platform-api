// platform-api/src/routes/__tests__/workItems.test.ts
//
// The queue's contract. The load-bearing assertions are the ones about
// `/applied` refusing a receipt that does not evidence a mutation — that is the
// server-side half of "accept must produce the edit", and without it this router
// degrades into the promote-button-that-does-nothing it was written to replace.

import express, { type NextFunction, type Request, type Response } from 'express';

// --- module-level mocks (must precede the route import) ---------------------

const mockCreateItem = jest.fn();
const mockUpdateItemFields = jest.fn();
const mockGetItemsForDocument = jest.fn();
const mockFindByItemId = jest.fn();
const mockMoveItem = jest.fn();
const mockPublishSignal = jest.fn().mockResolvedValue(undefined);
const mockDraftProposal = jest.fn();

jest.mock('../../middleware/auth', () => ({
  requireAuth: (_req: Request, _res: Response, next: NextFunction) => next(),
}));

jest.mock('../../repositories', () => ({
  sectionItemRepo: {
    createItem: mockCreateItem,
    updateItemFields: mockUpdateItemFields,
    getItemsForDocument: mockGetItemsForDocument,
    findByItemId: mockFindByItemId,
    moveItem: mockMoveItem,
  },
}));

jest.mock('../../lib/event-catalog', () => ({
  publishSignal: mockPublishSignal,
  publishDurable: jest.fn().mockResolvedValue(undefined),
}));

jest.mock('../../lib/proposalDrafter', () => {
  const actual = jest.requireActual('../../lib/proposalDrafter');
  return { ...actual, draftProposal: mockDraftProposal };
});

import { workItemsRouter } from '../workItems';
import { errorHandler } from '../../middleware/error-handler';
import { tierFromContext, attachStateForTier, type CapturedContext } from '../../types/workItem';

// ---------------------------------------------------------------------------
// Rig (same shape as sectionItems.test.ts — no supertest in this repo)
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

async function run(
  app: express.Express,
  method: string,
  url: string,
  opts: { body?: unknown; userId?: string } = {},
): Promise<MockRes> {
  const res = mockRes();
  const [pathname] = url.split('?', 2);
  const req = {
    method: method.toUpperCase(),
    url,
    originalUrl: url,
    path: pathname,
    headers: { 'content-type': 'application/json' },
    query: {},
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
  app.use('/documents/:documentId/work-items', workItemsRouter);
  app.use(errorHandler);
  return app;
}

function frame(over: Partial<CapturedContext> = {}): CapturedContext {
  return {
    entityType: 'document',
    entityId: 'doc-1',
    documentId: 'doc-1',
    sectionId: 'sec-a',
    anchor: { sectionId: 'sec-a' },
    tier: 'section',
    confidence: 'high',
    contextSplit: false,
    autoAttach: true,
    reason: 'section had focus',
    t0_ms: 1000,
    t1_ms: 3000,
    ...over,
  };
}

beforeEach(() => {
  jest.clearAllMocks();
  mockCreateItem.mockImplementation(async (input: Record<string, unknown>) => ({
    ...input,
    itemId: 'ITEM1',
    createdAt: 'now',
    updatedAt: 'now',
  }));
  mockUpdateItemFields.mockImplementation(async (_d, _s, itemId, updates) => ({ itemId, ...updates }));
});

// ---------------------------------------------------------------------------
// Tiering — never fans out
// ---------------------------------------------------------------------------

describe('confidence tiers', () => {
  it('maps an exact or focused target to HIGH, which attaches', () => {
    expect(tierFromContext(frame({ confidence: 'exact', tier: 'selection' }))).toBe('high');
    expect(tierFromContext(frame({ confidence: 'high' }))).toBe('high');
    expect(attachStateForTier('high')).toBe('attached');
  });

  it('maps a merely-visible target to MEDIUM, which stays author-visible', () => {
    expect(tierFromContext(frame({ confidence: 'medium', tier: 'viewport' }))).toBe('medium');
    expect(attachStateForTier('medium')).toBe('author_only');
  });

  it('demotes to LOW when the screen moved mid-utterance, however specific the start rung was', () => {
    // The speaker was looking at two things and we cannot know which they meant.
    expect(tierFromContext(frame({ confidence: 'exact', contextSplit: true }))).toBe('low');
    expect(attachStateForTier('low')).toBe('inbox');
  });

  it('demotes to LOW when autoAttach was withheld or no section was named', () => {
    expect(tierFromContext(frame({ autoAttach: false }))).toBe('low');
    expect(tierFromContext(frame({ sectionId: undefined as unknown as string }))).toBe('low');
    expect(tierFromContext(frame({ tier: 'route', confidence: 'low', sectionId: undefined as unknown as string }))).toBe('low');
  });
});

describe('POST /work-items', () => {
  it('attaches a high-confidence remark to the section the rules chose', async () => {
    const res = await run(buildApp(), 'post', '/documents/doc-1/work-items', {
      body: { utterance: 'change the header', context: frame() },
    });
    expect(res.statusCode).toBe(201);
    expect(mockCreateItem).toHaveBeenCalledTimes(1);
    const input = mockCreateItem.mock.calls[0][0];
    expect(input).toMatchObject({
      sectionId: 'sec-a',
      confidenceTier: 'high',
      attachState: 'attached',
      source: 'voice',
      utterance: 'change the header',
    });
  });

  it('writes exactly ONE row for an ambiguous remark, and it is the inbox', async () => {
    const res = await run(buildApp(), 'post', '/documents/doc-1/work-items', {
      body: {
        utterance: 'we should tighten that up',
        context: frame({ contextSplit: true }),
        candidates: [
          { sectionId: 'sec-a', score: 0.5, reason: 'mentions "tighten"' },
          { sectionId: 'sec-b', score: 0.2, reason: 'mentions "up"' },
        ],
      },
    });
    expect(res.statusCode).toBe(201);
    // The Fireflies failure is attaching to every candidate. One call, one row.
    expect(mockCreateItem).toHaveBeenCalledTimes(1);
    const input = mockCreateItem.mock.calls[0][0];
    expect(input.sectionId).toBe('_inbox');
    expect(input.attachState).toBe('inbox');
    expect(input.candidates).toHaveLength(2);
  });

  it('refuses a context frame that names a different document', async () => {
    const res = await run(buildApp(), 'post', '/documents/doc-1/work-items', {
      body: { utterance: 'hi', context: frame({ documentId: 'doc-OTHER' }) },
    });
    expect(res.statusCode).toBe(400);
    expect(mockCreateItem).not.toHaveBeenCalled();
  });

  it('refuses an empty utterance', async () => {
    const res = await run(buildApp(), 'post', '/documents/doc-1/work-items', {
      body: { utterance: '   ', context: frame() },
    });
    expect(res.statusCode).toBe(400);
  });

  it('refuses a request with no context frame rather than guessing a target', async () => {
    const res = await run(buildApp(), 'post', '/documents/doc-1/work-items', {
      body: { utterance: 'change the header' },
    });
    expect(res.statusCode).toBe(400);
  });
});

// ---------------------------------------------------------------------------
// THE LOAD-BEARING PART: acceptance requires evidence of a mutation
// ---------------------------------------------------------------------------

describe('POST /work-items/:itemId/applied', () => {
  const fieldItem = {
    itemId: 'ITEM1',
    documentId: 'doc-1',
    sectionId: 'sec-a',
    text: 'change the header',
    proposalStatus: 'ready',
    proposal: {
      kind: 'field',
      proposalId: 'p1',
      change: { path: 'meta.title', from: 'Old', to: 'New' },
    },
  };

  it('records the apply when the receipt names the value that was written', async () => {
    mockFindByItemId.mockResolvedValue(fieldItem);
    const res = await run(buildApp(), 'post', '/documents/doc-1/work-items/ITEM1/applied', {
      body: { receipt: { observedFrom: 'Old', wrote: 'New' } },
    });
    expect(res.statusCode).toBe(200);
    const updates = mockUpdateItemFields.mock.calls[0][3];
    expect(updates.proposalStatus).toBe('applied');
    expect(updates.applied).toMatchObject({ observedFrom: 'Old', wrote: 'New', by: 'user-1' });
  });

  it('REFUSES a receipt that reports writing something other than what was proposed', async () => {
    mockFindByItemId.mockResolvedValue(fieldItem);
    const res = await run(buildApp(), 'post', '/documents/doc-1/work-items/ITEM1/applied', {
      body: { receipt: { observedFrom: 'Old', wrote: 'Something else entirely' } },
    });
    expect(res.statusCode).toBe(409);
    expect(mockUpdateItemFields).not.toHaveBeenCalled();
  });

  it('REFUSES a receipt with no evidence at all — the promote-does-nothing case', async () => {
    mockFindByItemId.mockResolvedValue(fieldItem);
    const res = await run(buildApp(), 'post', '/documents/doc-1/work-items/ITEM1/applied', {
      body: { receipt: {} },
    });
    expect(res.statusCode).toBe(400);
    expect(mockUpdateItemFields).not.toHaveBeenCalled();
  });

  it('REFUSES a prose receipt claiming zero edits landed', async () => {
    mockFindByItemId.mockResolvedValue({
      ...fieldItem,
      proposal: { kind: 'prose', proposalId: 'p2', sectionId: 'sec-a', edits: [{ anchorQuote: 'a', replacement: 'b' }] },
    });
    const res = await run(buildApp(), 'post', '/documents/doc-1/work-items/ITEM1/applied', {
      body: { receipt: { editsApplied: 0 } },
    });
    expect(res.statusCode).toBe(400);
    expect(mockUpdateItemFields).not.toHaveBeenCalled();
  });

  it('refuses to apply the same proposal twice', async () => {
    mockFindByItemId.mockResolvedValue({ ...fieldItem, proposalStatus: 'applied' });
    const res = await run(buildApp(), 'post', '/documents/doc-1/work-items/ITEM1/applied', {
      body: { receipt: { wrote: 'New' } },
    });
    expect(res.statusCode).toBe(409);
  });

  it('refuses when there is no proposal to apply', async () => {
    mockFindByItemId.mockResolvedValue({ ...fieldItem, proposal: undefined });
    const res = await run(buildApp(), 'post', '/documents/doc-1/work-items/ITEM1/applied', {
      body: { receipt: { wrote: 'New' } },
    });
    expect(res.statusCode).toBe(400);
  });
});

// ---------------------------------------------------------------------------
// Re-attachment is real and non-destructive
// ---------------------------------------------------------------------------

describe('POST /work-items/:itemId/reattach', () => {
  it('moves the item, keeping its id', async () => {
    mockFindByItemId.mockResolvedValue({ itemId: 'ITEM1', documentId: 'doc-1', sectionId: '_inbox' });
    mockMoveItem.mockResolvedValue({ itemId: 'ITEM1', sectionId: 'sec-b', attachState: 'attached' });

    const res = await run(buildApp(), 'post', '/documents/doc-1/work-items/ITEM1/reattach', {
      body: { toSectionId: 'sec-b' },
    });
    expect(res.statusCode).toBe(200);
    expect(mockMoveItem).toHaveBeenCalledWith(
      expect.objectContaining({ fromSectionId: '_inbox', toSectionId: 'sec-b', via: 'human', attachState: 'attached' }),
    );
  });

  it('badges a quote-healed move rather than presenting it as a confirmed one', async () => {
    mockFindByItemId.mockResolvedValue({ itemId: 'ITEM1', documentId: 'doc-1', sectionId: 'sec-a' });
    mockMoveItem.mockResolvedValue({ itemId: 'ITEM1', sectionId: 'sec-c', attachState: 'healed' });

    await run(buildApp(), 'post', '/documents/doc-1/work-items/ITEM1/reattach', {
      body: { toSectionId: 'sec-c', via: 'heal' },
    });
    expect(mockMoveItem).toHaveBeenCalledWith(
      expect.objectContaining({ via: 'heal', attachState: 'healed' }),
    );
  });

  it('refuses a move with no destination', async () => {
    mockFindByItemId.mockResolvedValue({ itemId: 'ITEM1', documentId: 'doc-1', sectionId: '_inbox' });
    const res = await run(buildApp(), 'post', '/documents/doc-1/work-items/ITEM1/reattach', { body: {} });
    expect(res.statusCode).toBe(400);
    expect(mockMoveItem).not.toHaveBeenCalled();
  });
});

// ---------------------------------------------------------------------------
// Visibility
// ---------------------------------------------------------------------------

describe('GET /work-items', () => {
  it('hides an author-only item from everyone but its speaker', async () => {
    mockGetItemsForDocument.mockResolvedValue([
      { itemId: 'A', source: 'voice', attachState: 'attached', createdBy: 'someone' },
      { itemId: 'B', source: 'voice', attachState: 'author_only', createdBy: 'speaker' },
      { itemId: 'C', source: 'human' },
    ]);

    const mine = await run(buildApp(), 'get', '/documents/doc-1/work-items', { userId: 'speaker' });
    expect((mine.body as { items: Array<{ itemId: string }> }).items.map((i) => i.itemId)).toEqual(['A', 'B']);

    const theirs = await run(buildApp(), 'get', '/documents/doc-1/work-items', { userId: 'other' });
    expect((theirs.body as { items: Array<{ itemId: string }> }).items.map((i) => i.itemId)).toEqual(['A']);
  });
});

// ---------------------------------------------------------------------------
// Drafting
// ---------------------------------------------------------------------------

describe('POST /work-items/:itemId/propose', () => {
  const item = { itemId: 'ITEM1', documentId: 'doc-1', sectionId: 'sec-a', text: 'x', utterance: 'rename the header' };
  const snapshot = { meta: { title: 'Old' }, sections: [{ id: 'sec-a', title: 'A', text: 'body' }] };

  it('refuses to draft without a snapshot — the server has no Y.Doc to read', async () => {
    mockFindByItemId.mockResolvedValue(item);
    const res = await run(buildApp(), 'post', '/documents/doc-1/work-items/ITEM1/propose', { body: {} });
    expect(res.statusCode).toBe(400);
    expect(mockDraftProposal).not.toHaveBeenCalled();
  });

  it('stores a drafted proposal as ready', async () => {
    mockFindByItemId.mockResolvedValue(item);
    mockDraftProposal.mockResolvedValue({
      kind: 'field', proposalId: 'p1', change: { path: 'meta.title', from: 'Old', to: 'New' },
    });
    const res = await run(buildApp(), 'post', '/documents/doc-1/work-items/ITEM1/propose', { body: { snapshot } });
    expect(res.statusCode).toBe(200);
    const last = mockUpdateItemFields.mock.calls.at(-1)![3];
    expect(last.proposalStatus).toBe('ready');
  });

  it('records a refused draft as data, with its reason, rather than a 500', async () => {
    mockFindByItemId.mockResolvedValue(item);
    const { ProposalDraftError } = jest.requireActual('../../lib/proposalDrafter');
    mockDraftProposal.mockRejectedValue(new ProposalDraftError('proposal targets "sections.other.title"'));
    const res = await run(buildApp(), 'post', '/documents/doc-1/work-items/ITEM1/propose', { body: { snapshot } });
    expect(res.statusCode).toBe(422);
    const last = mockUpdateItemFields.mock.calls.at(-1)![3];
    expect(last.proposalStatus).toBe('failed');
    expect(last.proposalError).toContain('sections.other.title');
  });

  // Observed for real: the pod's `claude` CLI credentials had expired, so every
  // draft came back "claude exited 1: (no stderr)" and the route rethrew it as a
  // bare 500 logged as "unhandled error". Nothing in this service was broken —
  // and a person clicking "Draft a change" was shown a generic failure they
  // would reasonably read as a problem with the sentence they had just spoken.
  it('reports an unreachable model as an upstream outage, not as a server bug', async () => {
    mockFindByItemId.mockResolvedValue(item);
    const { ProposalProviderError } = jest.requireActual('../../lib/proposalDrafter');
    mockDraftProposal.mockRejectedValue(
      new ProposalProviderError('the claude-code provider could not be reached: Not logged in'),
    );
    const res = await run(buildApp(), 'post', '/documents/doc-1/work-items/ITEM1/propose', { body: { snapshot } });
    expect(res.statusCode).toBe(502);
    expect((res.body as { detail: string }).detail).toContain('Not logged in');
    const last = mockUpdateItemFields.mock.calls.at(-1)![3];
    expect(last.proposalStatus).toBe('failed');
    // The reason has to reach the row, or the card has nothing to show.
    expect(last.proposalError).toContain('Not logged in');
  });

  it('treats "not an edit request" as a normal outcome, not a failure', async () => {
    mockFindByItemId.mockResolvedValue(item);
    mockDraftProposal.mockResolvedValue(null);
    const res = await run(buildApp(), 'post', '/documents/doc-1/work-items/ITEM1/propose', { body: { snapshot } });
    expect(res.statusCode).toBe(200);
    expect((res.body as { proposal: unknown }).proposal).toBeNull();
    const last = mockUpdateItemFields.mock.calls.at(-1)![3];
    expect(last.proposalStatus).toBe('none');
  });
});
