// social-api/src/ingest/routes/__tests__/ingestSourceMetrics.test.ts
//
// Coverage for the per-source SLO panel route (hub#2188). Verifies the
// returned shape, that all three time series have the same bucket count,
// and that latency / error rate are derived from the items repo.

import express, { type NextFunction, type Request, type Response } from 'express';

const listBySourceSpy = jest.fn(async () => [] as unknown[]);

jest.mock('../../repositories', () => ({
  ingestItemRepo: {
    listBySource: (...args: Parameters<typeof listBySourceSpy>) => listBySourceSpy(...args),
  },
}));

import { ingestSourceMetricsRouter } from '../ingestSourceMetrics';
import { errorHandler } from '../../../middleware/error-handler';

interface MockRes {
  statusCode: number;
  body: unknown;
  ended: boolean;
  finished: Promise<void>;
  status(code: number): MockRes;
  json(body: unknown): MockRes;
  setHeader(_name: string, _value: unknown): MockRes;
  getHeader(_name: string): unknown;
  end(...args: unknown[]): MockRes;
}

function mockRes(): MockRes {
  let resolveFinished!: () => void;
  const finished = new Promise<void>((resolve) => { resolveFinished = resolve; });
  const res: MockRes = {
    statusCode: 200,
    body: undefined,
    ended: false,
    finished,
    status(code: number) { this.statusCode = code; return this; },
    json(b: unknown) { this.body = b; this.ended = true; resolveFinished(); return this; },
    setHeader() { return this; },
    getHeader() { return undefined; },
    end() { this.ended = true; resolveFinished(); return this; },
  };
  return res;
}

async function run(app: express.Express, method: string, url: string, opts: { params?: Record<string, string>; query?: Record<string, string> } = {}): Promise<MockRes> {
  const req = {
    method,
    url,
    body: {},
    headers: {},
    params: opts.params ?? {},
    query: opts.query ?? {},
  } as unknown as Request;
  const res = mockRes();
  await new Promise<void>((resolve, reject) => {
    const finalHandler = (err?: unknown) => {
      if (err) errorHandler(err as Error, req as unknown as Request, res as unknown as Response, () => {});
      resolve();
    };
    (app as unknown as { handle: (req: Request, res: Response, next: NextFunction) => void }).handle(
      req,
      res as unknown as Response,
      finalHandler as NextFunction,
    );
    res.finished.then(() => resolve()).catch(reject);
  });
  return res;
}

function buildApp(): express.Express {
  const app = express();
  app.use(express.json());
  app.use('/ingest/sources/:id/metrics', ingestSourceMetricsRouter);
  return app;
}

describe('ingest source metrics route', () => {
  beforeEach(() => {
    listBySourceSpy.mockReset();
    listBySourceSpy.mockResolvedValue([]);
  });

  test('returns 3-series shape with default 1h window', async () => {
    const res = await run(buildApp(), 'GET', '/ingest/sources/src-x/metrics', { params: { id: 'src-x' } });
    expect(res.statusCode).toBe(200);
    const body = res.body as {
      sourceId: string;
      window: string;
      bucketSizeMs: number;
      latencyP95: Array<{ at: string; value: number | null }>;
      errorRate: Array<{ at: string; value: number | null }>;
      queueDepth: Array<{ at: string; value: number | null }>;
    };
    expect(body.sourceId).toBe('src-x');
    expect(body.window).toBe('1h');
    expect(body.bucketSizeMs).toBe(5 * 60 * 1000);
    // 1h / 5min = 12 buckets.
    expect(body.latencyP95).toHaveLength(12);
    expect(body.errorRate).toHaveLength(12);
    expect(body.queueDepth).toHaveLength(12);
  });

  test('error-rate fires when failed items land in window', async () => {
    const now = Date.now();
    const recentDone = {
      ingestId: 'i-1', sourceId: 'src-x', filePath: '/x', status: 'done',
      mappedTypeId: null, mappingConfidence: null, mappingMethod: null,
      documentId: null, pipelineRunId: null, error: null, attempts: 0,
      mimeType: 'application/json', fileSize: 0, fileHash: '',
      createdAt: new Date(now - 30_000).toISOString(),
      updatedAt: new Date(now - 5_000).toISOString(),
    };
    const recentFailed = { ...recentDone, ingestId: 'i-2', status: 'failed' as const };
    listBySourceSpy.mockResolvedValue([recentDone, recentFailed]);

    const res = await run(buildApp(), 'GET', '/ingest/sources/src-x/metrics?window=1h', {
      params: { id: 'src-x' }, query: { window: '1h' },
    });
    expect(res.statusCode).toBe(200);
    const body = res.body as { errorRate: Array<{ value: number | null }> };
    // 1 failed of 2 total in the latest bucket → 50%.
    const lastNonNull = [...body.errorRate].reverse().find((p) => p.value !== null);
    expect(lastNonNull?.value).toBeCloseTo(50, 1);
  });

  test('window query param defaults to 1h when missing or invalid', async () => {
    const res = await run(buildApp(), 'GET', '/ingest/sources/src-x/metrics?window=garbage', {
      params: { id: 'src-x' }, query: { window: 'garbage' },
    });
    expect(res.statusCode).toBe(200);
    const body = res.body as { window: string; latencyP95: unknown[] };
    expect(body.window).toBe('1h');
    expect(body.latencyP95).toHaveLength(12);
  });

  test('respects 24h window', async () => {
    const res = await run(buildApp(), 'GET', '/ingest/sources/src-x/metrics?window=24h', {
      params: { id: 'src-x' }, query: { window: '24h' },
    });
    expect(res.statusCode).toBe(200);
    const body = res.body as { window: string; queueDepth: unknown[] };
    expect(body.window).toBe('24h');
    // 24h / 5min = 288 buckets.
    expect(body.queueDepth).toHaveLength(288);
  });
});
