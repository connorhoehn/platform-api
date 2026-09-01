// platform-api/src/routes/__tests__/workItemVerdict.test.ts
//
// The verdict endpoints. Three assertions carry the feature:
//
//   * editing the change's content produces a different revision, so the cached
//     verdict for the old text is no longer reachable and the model is asked
//     again. Without this the queue shows a confident judgement of text that no
//     longer exists.
//   * a failed model call answers a FAILED state and writes nothing. Never a
//     synthesised `pass`, and never a silent 200 with no verdict, which the UI
//     would render as "no concerns".
//   * a verdict changes NOTHING about the work item. It is advice attached to a
//     human decision, not an approval gate.

import express, { type NextFunction, type Request, type Response } from 'express';

// --- module-level mocks (must precede the route import) ---------------------

const mockFindByItemId = jest.fn();
const mockGetItemsForDocument = jest.fn();
const mockUpdateItemFields = jest.fn();
const mockVerdictGet = jest.fn();
const mockVerdictList = jest.fn();
const mockVerdictPut = jest.fn();
const mockJudgeChange = jest.fn();

jest.mock('../../middleware/auth', () => ({
  requireAuth: (_req: Request, _res: Response, next: NextFunction) => next(),
}));

jest.mock('../../repositories', () => ({
  sectionItemRepo: {
    findByItemId: mockFindByItemId,
    getItemsForDocument: mockGetItemsForDocument,
    updateItemFields: mockUpdateItemFields,
  },
  changeVerdictRepo: {
    get: mockVerdictGet,
    listForChange: mockVerdictList,
    put: mockVerdictPut,
  },
  toVerdict: (row: Record<string, unknown>) => ({
    verdict: row['verdict'],
    headline: row['headline'],
    ...(row['reasoning'] ? { reasoning: row['reasoning'] } : {}),
    producedAt: row['producedAt'],
    forRevision: row['forRevision'],
  }),
}));

jest.mock('../../lib/event-catalog', () => ({
  publishSignal: jest.fn().mockResolvedValue(undefined),
  publishDurable: jest.fn().mockResolvedValue(undefined),
}));

jest.mock('../../lib/changeVerdict', () => {
  const actual = jest.requireActual('../../lib/changeVerdict');
  return { ...actual, judgeChange: mockJudgeChange };
});

import { workItemsRouter } from '../workItems';
import { errorHandler } from '../../middleware/error-handler';
import {
  buildVerdictContext,
  computeChangeRevision,
  VerdictProviderError,
  VerdictTimeoutError,
  VerdictParseError,
} from '../../lib/changeVerdict';
import type { Proposal } from '../../types/workItem';

// --- rig (same shape as workItems.test.ts — no supertest in this repo) -------

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
  app.use('/documents/:documentId/work-items', workItemsRouter);
  app.use(errorHandler);
  return app;
}

// --- fixtures ---------------------------------------------------------------

const snapshot = {
  meta: { title: 'Retry Policy' },
  sections: [{ id: 'sec-a', title: 'Limits', text: 'The client retries at most 3 times.' }],
};

function proposal(replacement = 'at most 5 times'): Proposal {
  return {
    kind: 'prose',
    proposalId: 'p-1',
    sectionId: 'sec-a',
    edits: [{ anchorQuote: 'at most 3 times', replacement }],
  };
}

function item(over: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    sectionKey: 'doc-1:sec-a',
    itemId: 'item-1',
    documentId: 'doc-1',
    sectionId: 'sec-a',
    text: 'bump the retry ceiling',
    utterance: 'make it five retries',
    source: 'voice',
    proposal: proposal(),
    proposalStatus: 'ready',
    createdAt: 'now',
    updatedAt: 'now',
    ...over,
  };
}

const url = '/documents/doc-1/work-items/item-1/verdict';

/** The context digest a cached row must carry to still be servable. */
function digestFor(p: Proposal = proposal()): string {
  return buildVerdictContext({ proposal: p, snapshot, utterance: 'make it five retries' }).digest;
}

beforeEach(() => {
  jest.clearAllMocks();
  mockGetItemsForDocument.mockResolvedValue([]);
  mockVerdictGet.mockResolvedValue(null);
  mockVerdictList.mockResolvedValue([]);
  mockVerdictPut.mockResolvedValue(undefined);
  mockJudgeChange.mockImplementation(async (input: { forRevision: string }) => ({
    verdict: {
      verdict: 'concern',
      headline: 'The document still says 3 in the same sentence.',
      reasoning: 'Section Limits says 3 and nothing else was changed.',
      producedAt: '2026-09-01T00:00:00.000Z',
      forRevision: input.forRevision,
    },
    context: { block: 'ctx', digest: 'c1-ctx' },
    model: 'sonnet',
    elapsedMs: 12,
  }));
});

// ---------------------------------------------------------------------------

describe('POST .../work-items/:itemId/verdict', () => {
  it('refuses without a snapshot — a verdict on the change alone is a style opinion', async () => {
    mockFindByItemId.mockResolvedValue(item());
    const res = await run(buildApp(), 'post', url, { body: {} });
    expect(res.statusCode).toBe(400);
    expect(mockJudgeChange).not.toHaveBeenCalled();
  });

  it('refuses when the item has no proposed change', async () => {
    mockFindByItemId.mockResolvedValue(item({ proposal: undefined, proposalStatus: 'none' }));
    const res = await run(buildApp(), 'post', url, { body: { snapshot } });
    expect(res.statusCode).toBe(400);
  });

  it('produces a verdict stamped with the change revision, and caches it', async () => {
    mockFindByItemId.mockResolvedValue(item());
    const res = await run(buildApp(), 'post', url, { body: { snapshot } });

    expect(res.statusCode).toBe(200);
    const body = res.body as { verdict: { verdict: string; forRevision: string }; cached: boolean; currentRevision: string };
    expect(body.verdict.verdict).toBe('concern');
    expect(body.cached).toBe(false);
    expect(body.verdict.forRevision).toBe(computeChangeRevision(proposal()));
    expect(body.currentRevision).toBe(body.verdict.forRevision);

    expect(mockVerdictPut).toHaveBeenCalledWith(
      expect.objectContaining({
        documentId: 'doc-1',
        itemId: 'item-1',
        forRevision: computeChangeRevision(proposal()),
        verdict: 'concern',
        contextDigest: expect.stringMatching(/^c1-/),
      }),
    );
  });

  it('never touches the work item — a verdict is advice, not an approval', async () => {
    mockFindByItemId.mockResolvedValue(item());
    await run(buildApp(), 'post', url, { body: { snapshot } });
    expect(mockUpdateItemFields).not.toHaveBeenCalled();
  });

  it('shows the model the other pending changes on the document', async () => {
    mockFindByItemId.mockResolvedValue(item());
    mockGetItemsForDocument.mockResolvedValue([
      item(),
      item({ itemId: 'item-2', proposal: proposal('at most 9 times'), text: 'nine' }),
      item({ itemId: 'item-3', proposalStatus: 'rejected' }),
    ]);
    await run(buildApp(), 'post', url, { body: { snapshot } });

    // Assert on what actually reaches the model, not on an intermediate.
    const block = (mockJudgeChange.mock.calls[0]![0] as { context: { block: string } }).context.block;
    expect(block).toContain('OTHER CHANGES ALREADY PENDING');
    expect(block).toContain('item-2');
    expect(block).not.toContain('item-3'); // rejected — not pending
    expect(block).not.toContain('pending change item-1'); // not itself
  });

  it('serves a cache HIT without calling the model', async () => {
    const revision = computeChangeRevision(proposal());
    mockFindByItemId.mockResolvedValue(item());
    mockVerdictGet.mockResolvedValue({
      changeKey: 'doc-1#item-1',
      forRevision: revision,
      verdict: 'concern',
      headline: 'cached headline',
      producedAt: '2026-09-01T00:00:00.000Z',
      contextDigest: digestFor(),
      model: 'sonnet',
    });

    const res = await run(buildApp(), 'post', url, { body: { snapshot } });
    expect(res.statusCode).toBe(200);
    expect((res.body as { cached: boolean }).cached).toBe(true);
    expect((res.body as { verdict: { headline: string } }).verdict.headline).toBe('cached headline');
    expect(mockJudgeChange).not.toHaveBeenCalled();
  });

  it('MISSES after the change is edited — the old verdict is unreachable, not stale', async () => {
    const oldRevision = computeChangeRevision(proposal());
    // The cache still holds a row, but only under the OLD content hash. The
    // item now carries edited text, so the lookup addresses a key nothing is
    // stored at.
    mockVerdictGet.mockImplementation(async (_d: string, _i: string, rev: string) =>
      rev === oldRevision
        ? { forRevision: rev, verdict: 'pass', headline: 'stale pass', producedAt: 'then', contextDigest: digestFor(), model: 'sonnet' }
        : null,
    );
    mockFindByItemId.mockResolvedValue(item({ proposal: proposal('at most 500 times') }));

    const res = await run(buildApp(), 'post', url, { body: { snapshot } });
    expect((res.body as { cached: boolean }).cached).toBe(false);
    expect((res.body as { currentRevision: string }).currentRevision).not.toBe(oldRevision);
    expect(mockJudgeChange).toHaveBeenCalledTimes(1);
  });

  it('recomputes when the document around the change has moved', async () => {
    mockFindByItemId.mockResolvedValue(item());
    mockVerdictGet.mockResolvedValue({
      forRevision: computeChangeRevision(proposal()),
      verdict: 'pass',
      headline: 'judged against a document that no longer exists',
      producedAt: 'then',
      contextDigest: 'c1-some-other-document',
      model: 'sonnet',
    });
    const res = await run(buildApp(), 'post', url, { body: { snapshot } });
    expect((res.body as { cached: boolean }).cached).toBe(false);
    expect(mockJudgeChange).toHaveBeenCalledTimes(1);
  });

  it('re-asks on ?refresh', async () => {
    mockFindByItemId.mockResolvedValue(item());
    await run(buildApp(), 'post', url, { body: { snapshot, refresh: true } });
    expect(mockVerdictGet).not.toHaveBeenCalled();
    expect(mockJudgeChange).toHaveBeenCalledTimes(1);
  });
});

describe('the failure path', () => {
  it.each([
    [new VerdictProviderError('claude exited 1: Not logged in'), 502, 'provider_unavailable'],
    [new VerdictTimeoutError('did not answer within 60000ms'), 504, 'timed_out'],
    [new VerdictParseError('reply contained no JSON object'), 502, 'unusable_reply'],
  ])('reports a FAILED state (%#) rather than a verdict', async (err, status, reason) => {
    mockFindByItemId.mockResolvedValue(item());
    mockJudgeChange.mockRejectedValue(err);

    const res = await run(buildApp(), 'post', url, { body: { snapshot } });
    expect(res.statusCode).toBe(status);
    const body = res.body as Record<string, unknown>;
    expect(body['state']).toBe('failed');
    expect(body['reason']).toBe(reason);
    // The one thing that must never happen: something a caller could read as a
    // verdict, or as an empty 200 meaning "no concerns".
    expect(body['verdict']).toBeUndefined();
    expect(String(body['detail'])).not.toBe('');
    expect(mockVerdictPut).not.toHaveBeenCalled();
  });
});

describe('GET .../work-items/:itemId/verdict', () => {
  it('returns the verdict for the current revision', async () => {
    const revision = computeChangeRevision(proposal());
    mockFindByItemId.mockResolvedValue(item());
    mockVerdictList.mockResolvedValue([
      { forRevision: revision, verdict: 'pass', headline: 'fine', producedAt: 'b', contextDigest: 'c1-ctx', model: 'sonnet' },
    ]);
    const res = await run(buildApp(), 'get', url);
    expect((res.body as { cached: boolean }).cached).toBe(true);
    expect((res.body as { verdict: { verdict: string } }).verdict.verdict).toBe('pass');
  });

  it('reports a verdict for a different revision as history, not as the verdict', async () => {
    mockFindByItemId.mockResolvedValue(item());
    mockVerdictList.mockResolvedValue([
      { forRevision: 'v1-anolderhash', verdict: 'pass', headline: 'reviewed before your edit', producedAt: 'a', contextDigest: 'c1-ctx', model: 'sonnet' },
    ]);
    const res = await run(buildApp(), 'get', url);
    const body = res.body as { verdict: unknown; cached: boolean; stale?: { headline: string } };
    expect(body.verdict).toBeNull();
    expect(body.cached).toBe(false);
    expect(body.stale?.headline).toBe('reviewed before your edit');
  });

  it('calls no model', async () => {
    mockFindByItemId.mockResolvedValue(item());
    await run(buildApp(), 'get', url);
    expect(mockJudgeChange).not.toHaveBeenCalled();
  });
});

describe('GET .../work-items/verdicts', () => {
  it('returns a map keyed by item id, cache-only', async () => {
    const revision = computeChangeRevision(proposal());
    mockGetItemsForDocument.mockResolvedValue([
      item(),
      item({ itemId: 'item-2', proposal: proposal('at most 9 times') }),
      item({ itemId: 'item-3', proposal: undefined }),
    ]);
    mockVerdictList.mockImplementation(async (_d: string, itemId: string) =>
      itemId === 'item-1'
        ? [{ forRevision: revision, verdict: 'fail', headline: 'contradicts §Limits', producedAt: 'a', contextDigest: 'c1-ctx', model: 'sonnet' }]
        : [],
    );

    const res = await run(buildApp(), 'get', '/documents/doc-1/work-items/verdicts');
    const map = (res.body as { verdicts: Record<string, { verdict: { verdict: string } | null }> }).verdicts;
    expect(map['item-1']!.verdict!.verdict).toBe('fail');
    expect(map['item-2']!.verdict).toBeNull();
    expect(map['item-3']).toBeUndefined(); // no proposed change, so nothing to judge
    expect(mockJudgeChange).not.toHaveBeenCalled();
  });
});
