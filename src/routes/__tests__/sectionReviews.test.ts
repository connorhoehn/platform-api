// Integration tests for sectionReviews routes.
//
// Mocks sectionReviewRepo, profileRepo, broadcastService, and auth
// at module level so we exercise review CRUD and my-reviews listing.

import express, { type NextFunction, type Request, type Response } from 'express';

const mockSubmitReview = jest.fn();
const mockGetForSection = jest.fn();
const mockGetForDocument = jest.fn();
const mockGetUserReviews = jest.fn();
const mockGetProfile = jest.fn();
const mockGetRequestsForReviewer = jest.fn();
const mockEmit = jest.fn().mockResolvedValue(undefined);

jest.mock('../../middleware/auth', () => ({
  requireAuth: (_req: Request, _res: Response, next: NextFunction) => next(),
}));

jest.mock('../../repositories', () => ({
  sectionReviewRepo: {
    submitReview: mockSubmitReview,
    getReviewsForSection: mockGetForSection,
    getReviewsForDocument: mockGetForDocument,
    getUserReviews: mockGetUserReviews,
  },
  profileRepo: { getProfile: mockGetProfile },
  documentReviewRequestRepo: {
    getRequestsForReviewer: mockGetRequestsForReviewer,
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

import {
  sectionReviewsRouter,
  documentReviewsRouter,
  myReviewsRouter,
  SECTION_REVIEW_STATUSES,
} from '../sectionReviews';
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
  app.use('/api/documents/:documentId/sections/:sectionId/reviews', sectionReviewsRouter);
  app.use('/api/documents/:documentId/reviews', documentReviewsRouter);
  app.use('/api/reviews', myReviewsRouter);
  app.use(errorHandler);
  return app;
}

beforeEach(() => {
  mockSubmitReview.mockReset();
  mockGetForSection.mockReset();
  mockGetForDocument.mockReset();
  mockGetUserReviews.mockReset();
  mockGetProfile.mockReset();
  mockGetRequestsForReviewer.mockReset().mockResolvedValue([]);
  mockEmit.mockReset().mockResolvedValue(undefined);
});

describe('POST /api/documents/:documentId/sections/:sectionId/reviews', () => {
  it('returns 201 on successful review', async () => {
    mockGetProfile.mockResolvedValue({ displayName: 'Alice' });
    const review = { reviewId: 'r-1', status: 'approved' };
    mockSubmitReview.mockResolvedValue(review);

    const res = await run(buildApp(), 'POST',
      '/api/documents/doc-1/sections/sec-1/reviews',
      { body: { status: 'approved', comment: 'looks good' } },
    );
    expect(res.statusCode).toBe(201);
    expect((res.body as { review: unknown }).review).toEqual(review);
    expect(mockSubmitReview).toHaveBeenCalledWith(
      expect.objectContaining({
        documentId: 'doc-1',
        sectionId: 'sec-1',
        status: 'approved',
        displayName: 'Alice',
        comment: 'looks good',
      }),
    );
  });

  it('uses userId as displayName when profile has none', async () => {
    mockGetProfile.mockResolvedValue(null);
    mockSubmitReview.mockResolvedValue({ reviewId: 'r-2' });

    await run(buildApp(), 'POST',
      '/api/documents/doc-1/sections/sec-1/reviews',
      { body: { status: 'reviewed' } },
    );
    expect(mockSubmitReview).toHaveBeenCalledWith(
      expect.objectContaining({ displayName: 'tester' }),
    );
  });

  it('rejects missing status with 400', async () => {
    const res = await run(buildApp(), 'POST',
      '/api/documents/doc-1/sections/sec-1/reviews',
      { body: {} },
    );
    expect(res.statusCode).toBe(400);
    // Pinned: the missing-status message is unchanged by the enum check.
    expect((res.body as { error: string }).error).toBe('status is required');
    expect(mockSubmitReview).not.toHaveBeenCalled();
  });

  // --- write-boundary status validation -----------------------------------
  //
  // The route used to accept ANY truthy string. A status outside the known set
  // reached the review UI's `statusConfig[...]` lookup and threw inside
  // `sections.map`, blanking the whole review view. Both renderers now degrade
  // to a neutral chip, but bad statuses must not be WRITABLE in the first
  // place — once stored they persist forever.
  describe('status vocabulary', () => {
    it.each(SECTION_REVIEW_STATUSES)('accepts %s', async (status) => {
      mockGetProfile.mockResolvedValue(null);
      mockSubmitReview.mockResolvedValue({ reviewId: 'r-ok', status });

      const res = await run(buildApp(), 'POST',
        '/api/documents/doc-1/sections/sec-1/reviews',
        { body: { status } },
      );
      expect(res.statusCode).toBe(201);
      expect(mockSubmitReview).toHaveBeenCalledWith(
        expect.objectContaining({ status }),
      );
    });

    it('rejects an unknown status with 400 and never writes it', async () => {
      mockGetProfile.mockResolvedValue(null);

      const res = await run(buildApp(), 'POST',
        '/api/documents/doc-1/sections/sec-1/reviews',
        { body: { status: 'needs-work' } },
      );
      expect(res.statusCode).toBe(400);
      expect(mockSubmitReview).not.toHaveBeenCalled();
      expect(mockEmit).not.toHaveBeenCalled();
    });

    it('names both the rejected value and the allowed set', async () => {
      const res = await run(buildApp(), 'POST',
        '/api/documents/doc-1/sections/sec-1/reviews',
        { body: { status: 'needs-work' } },
      );
      const { error } = res.body as { error: string };
      // Same `{ error: message }` shape the rest of the file's 400s use.
      expect(typeof error).toBe('string');
      expect(error).toContain('needs-work');
      for (const allowed of SECTION_REVIEW_STATUSES) {
        expect(error).toContain(allowed);
      }
    });

    it('rejects a document-lifecycle status — the vocabularies are separate', async () => {
      const res = await run(buildApp(), 'POST',
        '/api/documents/doc-1/sections/sec-1/reviews',
        { body: { status: 'in_review' } },
      );
      expect(res.statusCode).toBe(400);
      expect(mockSubmitReview).not.toHaveBeenCalled();
    });

    it('rejects a non-string status', async () => {
      const res = await run(buildApp(), 'POST',
        '/api/documents/doc-1/sections/sec-1/reviews',
        { body: { status: { approved: true } } },
      );
      expect(res.statusCode).toBe(400);
      expect((res.body as { error: string }).error)
        .toContain('status must be one of');
      expect(mockSubmitReview).not.toHaveBeenCalled();
    });

    it('truncates a huge rejected value rather than echoing all of it', async () => {
      const res = await run(buildApp(), 'POST',
        '/api/documents/doc-1/sections/sec-1/reviews',
        { body: { status: 'x'.repeat(5000) } },
      );
      expect(res.statusCode).toBe(400);
      expect((res.body as { error: string }).error.length).toBeLessThan(300);
    });
  });

  describe('comment validation', () => {
    it('rejects a non-string comment', async () => {
      const res = await run(buildApp(), 'POST',
        '/api/documents/doc-1/sections/sec-1/reviews',
        { body: { status: 'approved', comment: { text: 'hi' } } },
      );
      expect(res.statusCode).toBe(400);
      expect(mockSubmitReview).not.toHaveBeenCalled();
    });

    it('rejects an over-length comment', async () => {
      const res = await run(buildApp(), 'POST',
        '/api/documents/doc-1/sections/sec-1/reviews',
        { body: { status: 'approved', comment: 'a'.repeat(10001) } },
      );
      expect(res.statusCode).toBe(400);
      expect(mockSubmitReview).not.toHaveBeenCalled();
    });

    it('accepts a comment at the cap', async () => {
      mockGetProfile.mockResolvedValue(null);
      mockSubmitReview.mockResolvedValue({ reviewId: 'r-cap' });
      const res = await run(buildApp(), 'POST',
        '/api/documents/doc-1/sections/sec-1/reviews',
        { body: { status: 'approved', comment: 'a'.repeat(10000) } },
      );
      expect(res.statusCode).toBe(201);
    });
  });

  it('broadcasts review event', async () => {
    mockGetProfile.mockResolvedValue(null);
    mockSubmitReview.mockResolvedValue({ reviewId: 'r-3' });

    await run(buildApp(), 'POST',
      '/api/documents/doc-1/sections/sec-1/reviews',
      { body: { status: 'approved' } },
    );
    expect(mockEmit).toHaveBeenCalledWith(
      'doc:doc-1',
      expect.any(String),
      expect.objectContaining({ type: 'section:review', documentId: 'doc-1', sectionId: 'sec-1' }),
    );
  });
});

describe('GET /api/documents/:documentId/sections/:sectionId/reviews', () => {
  it('returns 200 with reviews for section', async () => {
    mockGetForSection.mockResolvedValue([{ reviewId: 'r-1' }]);
    const res = await run(buildApp(), 'GET',
      '/api/documents/doc-1/sections/sec-1/reviews');
    expect(res.statusCode).toBe(200);
    expect((res.body as { reviews: unknown[] }).reviews).toHaveLength(1);
  });

  // The write boundary tightened; the store did not. Rows written before the
  // enum check must still come back verbatim — tightening reads would blank
  // real review history.
  it('still returns rows whose stored status the write boundary now rejects', async () => {
    mockGetForSection.mockResolvedValue([
      { reviewId: 'r-old', status: 'needs-work' },
      { reviewId: 'r-new', status: 'approved' },
    ]);
    const res = await run(buildApp(), 'GET',
      '/api/documents/doc-1/sections/sec-1/reviews');
    expect(res.statusCode).toBe(200);
    expect((res.body as { reviews: Array<{ status: string }> }).reviews
      .map((r) => r.status)).toEqual(['needs-work', 'approved']);
  });
});

describe('GET /api/documents/:documentId/reviews', () => {
  it('returns 200 with reviews for document', async () => {
    mockGetForDocument.mockResolvedValue([{ reviewId: 'r-1' }, { reviewId: 'r-2' }]);
    const res = await run(buildApp(), 'GET', '/api/documents/doc-1/reviews');
    expect(res.statusCode).toBe(200);
    expect((res.body as { reviews: unknown[] }).reviews).toHaveLength(2);
  });

  it('passes historical bad statuses through unchanged', async () => {
    mockGetForDocument.mockResolvedValue([{ reviewId: 'r-1', status: 'in_review' }]);
    const res = await run(buildApp(), 'GET', '/api/documents/doc-1/reviews');
    expect((res.body as { reviews: Array<{ status: string }> }).reviews[0]!.status)
      .toBe('in_review');
  });
});

describe('GET /api/reviews/mine', () => {
  // `reviews` is the pre-existing key. These first two assertions are the
  // backward-compatibility contract — they must not change shape.
  it('returns 200 with user reviews', async () => {
    mockGetUserReviews.mockResolvedValue([{ reviewId: 'r-1' }]);
    const res = await run(buildApp(), 'GET', '/api/reviews/mine');
    expect(res.statusCode).toBe(200);
    expect((res.body as { reviews: unknown[] }).reviews).toHaveLength(1);
    expect(mockGetUserReviews).toHaveBeenCalledWith('tester');
  });

  it('keeps `reviews` untouched when the caller has review requests too', async () => {
    mockGetUserReviews.mockResolvedValue([{ reviewId: 'r-1', status: 'approved' }]);
    mockGetRequestsForReviewer.mockResolvedValue([
      { documentId: 'doc-1', requestKey: '_doc:tester', state: 'pending', required: true },
    ]);
    const res = await run(buildApp(), 'GET', '/api/reviews/mine');
    expect((res.body as { reviews: unknown[] }).reviews).toEqual([
      { reviewId: 'r-1', status: 'approved' },
    ]);
  });

  it('returns the named review requests addressed to the caller', async () => {
    mockGetUserReviews.mockResolvedValue([]);
    mockGetRequestsForReviewer.mockResolvedValue([
      {
        documentId: 'doc-1', requestKey: '_doc:tester', reviewerId: 'tester',
        state: 'pending', required: true, dueDate: '2030-01-01T00:00:00.000Z',
      },
    ]);
    const res = await run(buildApp(), 'GET', '/api/reviews/mine');
    const body = res.body as { requests: Array<{ documentId: string }> };
    expect(body.requests).toHaveLength(1);
    expect(body.requests[0]!.documentId).toBe('doc-1');
    expect(mockGetRequestsForReviewer).toHaveBeenCalledWith('tester', undefined);
  });

  it('sorts requests by due date, undated last', async () => {
    mockGetUserReviews.mockResolvedValue([]);
    mockGetRequestsForReviewer.mockResolvedValue([
      { documentId: 'no-due', requestKey: 'a', state: 'pending' },
      { documentId: 'late', requestKey: 'b', state: 'pending', dueDate: '2030-06-01T00:00:00.000Z' },
      { documentId: 'soon', requestKey: 'c', state: 'pending', dueDate: '2030-01-01T00:00:00.000Z' },
    ]);
    const res = await run(buildApp(), 'GET', '/api/reviews/mine');
    const ids = (res.body as { requests: Array<{ documentId: string }> }).requests
      .map((r) => r.documentId);
    expect(ids).toEqual(['soon', 'late', 'no-due']);
  });

  it('passes the state filter through to the repository', async () => {
    mockGetUserReviews.mockResolvedValue([]);
    await run(buildApp(), 'GET', '/api/reviews/mine?state=pending');
    expect(mockGetRequestsForReviewer).toHaveBeenCalledWith('tester', 'pending');
  });

  it('filters to overdue requests when asked', async () => {
    mockGetUserReviews.mockResolvedValue([]);
    mockGetRequestsForReviewer.mockResolvedValue([
      { documentId: 'overdue', requestKey: 'a', state: 'pending', dueDate: '2000-01-01T00:00:00.000Z' },
      { documentId: 'fine', requestKey: 'b', state: 'pending', dueDate: '2100-01-01T00:00:00.000Z' },
      // Already answered — never overdue, however old the due date.
      { documentId: 'answered', requestKey: 'c', state: 'approved', dueDate: '2000-01-01T00:00:00.000Z' },
    ]);
    const res = await run(buildApp(), 'GET', '/api/reviews/mine?overdue=true');
    const ids = (res.body as { requests: Array<{ documentId: string }> }).requests
      .map((r) => r.documentId);
    expect(ids).toEqual(['overdue']);
  });

  it('summarizes pending / overdue / total counts', async () => {
    mockGetUserReviews.mockResolvedValue([]);
    mockGetRequestsForReviewer.mockResolvedValue([
      { documentId: 'a', requestKey: 'a', state: 'pending', dueDate: '2000-01-01T00:00:00.000Z' },
      { documentId: 'b', requestKey: 'b', state: 'pending' },
      { documentId: 'c', requestKey: 'c', state: 'approved' },
    ]);
    const res = await run(buildApp(), 'GET', '/api/reviews/mine');
    expect((res.body as { summary: unknown }).summary).toEqual({
      pending: 2, overdue: 1, total: 3,
    });
  });
});
