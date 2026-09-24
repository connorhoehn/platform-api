// platform-api/src/routes/__tests__/judgments.test.ts
//
// Unit tests for the v2 judgment routes:
//   GET /api/judgments/:judgmentId/raw-text
//   GET /api/judgments/aggregates?pipelineId=…
//   GET /api/judgments/aggregates?rubricId=…

// Loop 29c: run access has its own suite (runAccess.test.ts); these tests
// exercise the route behaviour behind it, so the gate passes here.
jest.mock('../../pipeline/runAccess', () => {
  const actual = jest.requireActual('../../pipeline/runAccess');
  const pass = (policy: string) => Object.assign((_q: unknown, _s: unknown, n: () => void) => n(), { accessPolicy: policy });
  return {
    ...actual,
    requireRunAccess: () => pass('run:read'),
    requirePipelineVisible: () => pass('pipeline:owner'),
    assertRunAccess: async () => ({ role: 'owner' }),
    assertItemRunAccess: async () => undefined,
    canSeeRun: async () => true,
    filterVisibleRunItems: async (_r: unknown, items: unknown[]) => items,
  };
});

import express, { type NextFunction, type Request, type Response } from 'express';

// --- module-level mocks (must precede route import) -------------------------

const mockGetRawText = jest.fn();
const mockListByPipeline = jest.fn();
const mockListByRubric = jest.fn();

jest.mock('../../pipeline/judge/judgeServices', () => ({
  getJudgmentRepo: () => ({ getRawText: (...args: unknown[]) => mockGetRawText(...args), getById: async (id: string) => ({ judgmentId: id, runId: 'run-1', pipelineId: 'pl_1' }) }),
  getJudgmentAggregateRepo: () => ({
    listByPipeline: (...args: unknown[]) => mockListByPipeline(...args),
    listByRubric: (...args: unknown[]) => mockListByRubric(...args),
  }),
}));

jest.mock('../../pipeline/definitions-repository', () => ({
  definitionsRepo: { get: async (_u: string, id: string) => (id === 'pl_1' ? { id } : null) },
}));

const mockLogger = {
  info: jest.fn(),
  warn: jest.fn(),
  error: jest.fn(),
  child: () => mockLogger,
};
jest.mock('../../lib/logger', () => ({
  __esModule: true,
  default: mockLogger,
  withContext: () => mockLogger,
}));

import { judgmentsRouter } from '../judgments';
import { errorHandler } from '../../middleware/error-handler';

// ---------------------------------------------------------------------------
// Test rig — same shape as pipelineAudit.test.ts
// ---------------------------------------------------------------------------

interface MockRes {
  statusCode: number;
  body: unknown;
  ended: boolean;
  finished: Promise<void>;
  status(code: number): MockRes;
  json(body: unknown): MockRes;
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
    setHeader() { return this; },
    getHeader() { return undefined; },
    end(..._args) { this.ended = true; resolve(); return this; },
  };
  return r;
}

async function dispatch(
  app: express.Express,
  method: string,
  url: string,
): Promise<MockRes> {
  const res = mockRes();
  const [path, queryStr] = url.split('?');
  const query: Record<string, string> = {};
  if (queryStr) {
    for (const pair of queryStr.split('&')) {
      const [k, v] = pair.split('=');
      if (k) query[decodeURIComponent(k)] = v ? decodeURIComponent(v) : '';
    }
  }
  const req = {
    method: method.toUpperCase(),
    url,
    originalUrl: url,
    path,
    headers: {},
    query,
    body: {},
    user: { sub: 'user-1' },
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
  app.use('/judgments', judgmentsRouter);
  app.use(errorHandler);
  return app;
}

const app = buildApp();

beforeEach(() => {
  jest.clearAllMocks();
});

// ---------------------------------------------------------------------------
// GET /:judgmentId/raw-text
// ---------------------------------------------------------------------------

describe('GET /judgments/:judgmentId/raw-text', () => {
  test('200 with the body when available', async () => {
    mockGetRawText.mockResolvedValueOnce('the full raw body');
    const res = await dispatch(app, 'GET', '/judgments/jdg_1/raw-text');
    expect(res.statusCode).toBe(200);
    expect(res.body).toEqual({ judgmentId: 'jdg_1', rawModelText: 'the full raw body' });
    expect(mockGetRawText).toHaveBeenCalledWith('jdg_1');
  });

  test('404 when repository returns null (truncated + no S3 store, or missing row)', async () => {
    mockGetRawText.mockResolvedValueOnce(null);
    const res = await dispatch(app, 'GET', '/judgments/jdg_missing/raw-text');
    expect(res.statusCode).toBe(404);
    expect(res.body).toEqual({ error: 'judgment raw-text not available' });
  });
});

// ---------------------------------------------------------------------------
// GET /aggregates?pipelineId=…   OR   ?rubricId=…
// ---------------------------------------------------------------------------

describe('GET /judgments/aggregates', () => {
  test('200 with items routed via listByPipeline when ?pipelineId=', async () => {
    const items = [{ aggregateKey: 'k1', pipelineId: 'pl_1', rubricId: 'r1', dayBucket: '2026-06-03', numJudgments: 4, numPassed: 3, numFailed: 1, sumOverallScore: 3.1, sumTokensIn: 8000, sumTokensOut: 1200, sumCostUsd: 0.04, firstJudgedAt: '2026-06-03T00:00:00Z', lastJudgedAt: '2026-06-03T23:00:00Z', ttlEpoch: 0 }];
    mockListByPipeline.mockResolvedValueOnce({ items });

    const res = await dispatch(app, 'GET', '/judgments/aggregates?pipelineId=pl_1');

    expect(res.statusCode).toBe(200);
    expect((res.body as { items: unknown[] }).items).toEqual(items);
    expect(mockListByPipeline).toHaveBeenCalledWith('pl_1', expect.any(Object));
    expect(mockListByRubric).not.toHaveBeenCalled();
  });

  test('200 with items routed via listByRubric when ?rubricId=', async () => {
    mockListByRubric.mockResolvedValueOnce({ items: [] });
    const res = await dispatch(app, 'GET', '/judgments/aggregates?rubricId=rubric.helpfulness');

    expect(res.statusCode).toBe(200);
    expect(mockListByRubric).toHaveBeenCalledWith('rubric.helpfulness', expect.any(Object));
    expect(mockListByPipeline).not.toHaveBeenCalled();
  });

  test('passes through since/until/limit', async () => {
    mockListByPipeline.mockResolvedValueOnce({ items: [] });
    await dispatch(
      app,
      'GET',
      '/judgments/aggregates?pipelineId=pl_1&since=2026-06-01&until=2026-06-07&limit=7',
    );
    expect(mockListByPipeline).toHaveBeenCalledWith(
      'pl_1',
      expect.objectContaining({ since: '2026-06-01', until: '2026-06-07', limit: 7 }),
    );
  });

  test('400 when neither pipelineId nor rubricId supplied', async () => {
    const res = await dispatch(app, 'GET', '/judgments/aggregates?since=2026-06-01');
    expect(res.statusCode).toBe(400);
    expect((res.body as { error: string }).error).toMatch(/one of `pipelineId` or `rubricId`/);
  });

  test('400 when BOTH pipelineId and rubricId supplied', async () => {
    const res = await dispatch(app, 'GET', '/judgments/aggregates?pipelineId=pl_1&rubricId=r1');
    expect(res.statusCode).toBe(400);
    expect((res.body as { error: string }).error).toMatch(/not both/);
  });

  test('400 when since is not yyyy-mm-dd', async () => {
    const res = await dispatch(app, 'GET', '/judgments/aggregates?pipelineId=pl_1&since=not-a-date');
    expect(res.statusCode).toBe(400);
    expect((res.body as { error: string }).error).toMatch(/yyyy-mm-dd/);
  });

  test('limit clamps to MAX_LIMIT (365)', async () => {
    mockListByPipeline.mockResolvedValueOnce({ items: [] });
    await dispatch(app, 'GET', '/judgments/aggregates?pipelineId=pl_1&limit=99999');
    expect(mockListByPipeline).toHaveBeenCalledWith(
      'pl_1',
      expect.objectContaining({ limit: 365 }),
    );
  });

  test('limit defaults to 30 when absent', async () => {
    mockListByPipeline.mockResolvedValueOnce({ items: [] });
    await dispatch(app, 'GET', '/judgments/aggregates?pipelineId=pl_1');
    expect(mockListByPipeline).toHaveBeenCalledWith(
      'pl_1',
      expect.objectContaining({ limit: 30 }),
    );
  });

  test('400 when limit is non-numeric', async () => {
    const res = await dispatch(app, 'GET', '/judgments/aggregates?pipelineId=pl_1&limit=abc');
    expect(res.statusCode).toBe(400);
    expect((res.body as { error: string }).error).toMatch(/limit must be/);
  });
});
