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
const mockEmit = jest.fn();

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
}));

jest.mock('../../services/broadcast', () => ({
  broadcastService: { emit: mockEmit },
}));

import { sectionReviewsRouter, documentReviewsRouter, myReviewsRouter } from '../sectionReviews';
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
      { body: { status: 'needs-work' } },
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
});

describe('GET /api/documents/:documentId/reviews', () => {
  it('returns 200 with reviews for document', async () => {
    mockGetForDocument.mockResolvedValue([{ reviewId: 'r-1' }, { reviewId: 'r-2' }]);
    const res = await run(buildApp(), 'GET', '/api/documents/doc-1/reviews');
    expect(res.statusCode).toBe(200);
    expect((res.body as { reviews: unknown[] }).reviews).toHaveLength(2);
  });
});

describe('GET /api/reviews/mine', () => {
  it('returns 200 with user reviews', async () => {
    mockGetUserReviews.mockResolvedValue([{ reviewId: 'r-1' }]);
    const res = await run(buildApp(), 'GET', '/api/reviews/mine');
    expect(res.statusCode).toBe(200);
    expect((res.body as { reviews: unknown[] }).reviews).toHaveLength(1);
    expect(mockGetUserReviews).toHaveBeenCalledWith('tester');
  });
});
