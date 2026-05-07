// social-api/src/routes/__tests__/pipelineCost.test.ts
//
// Unit tests for pipelineCostRouter: GET /cost.

import express, { type NextFunction, type Request, type Response } from 'express';

// --- module-level mocks (must precede route import) -------------------------

const mockListByPipeline = jest.fn();

jest.mock('../../repositories/PipelineRunRepository', () => ({
  PipelineRunRepository: jest.fn().mockImplementation(() => ({
    listByPipeline: mockListByPipeline,
  })),
}));

jest.mock('../../lib/ddb-client', () => ({
  getDynamoClient: jest.fn().mockReturnValue({}),
}));

// computeCost: simple per-token rate so tests can verify aggregation math.
jest.mock('../../pipeline/llm-pricing', () => ({
  computeCost: (_model: string, tokensIn: number, tokensOut: number) =>
    (tokensIn + tokensOut) * 0.000001,
}));

import { pipelineCostRouter } from '../pipelineCost';
import { errorHandler } from '../../middleware/error-handler';
import type { PipelineCostResponse } from '../pipelineCost';

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

async function run(
  app: express.Express,
  method: string,
  url: string,
): Promise<MockRes> {
  const res = mockRes();
  const req = {
    method: method.toUpperCase(),
    url,
    originalUrl: url,
    path: url,
    headers: {},
    query: {},
    body: {},
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
  app.use('/pipelines/:pipelineId/cost', pipelineCostRouter);
  app.use(errorHandler);
  return app;
}

beforeEach(() => {
  jest.clearAllMocks();
});

// ---------------------------------------------------------------------------
// GET /pipelines/:pipelineId/cost
// ---------------------------------------------------------------------------

describe('GET /pipelines/:pipelineId/cost', () => {
  const app = buildApp();

  test('returns zeros for pipeline with no runs', async () => {
    mockListByPipeline.mockResolvedValueOnce([]);

    const res = await run(app, 'GET', '/pipelines/p1/cost');

    expect(res.statusCode).toBe(200);
    const body = res.body as PipelineCostResponse;
    expect(body.totalCostUsd).toBe(0);
    expect(body.totalTokensIn).toBe(0);
    expect(body.totalTokensOut).toBe(0);
    expect(body.runCount).toBe(0);
    expect(body.byModel).toEqual({});
  });

  test('returns zeros when runs have no tokenUsage', async () => {
    mockListByPipeline.mockResolvedValueOnce([
      { runId: 'r1', pipelineId: 'p1', status: 'succeeded', triggeredBy: 'u1', triggerType: 'manual', startedAt: 't1' },
      { runId: 'r2', pipelineId: 'p1', status: 'failed', triggeredBy: 'u1', triggerType: 'manual', startedAt: 't2' },
    ]);

    const res = await run(app, 'GET', '/pipelines/p1/cost');

    expect(res.statusCode).toBe(200);
    const body = res.body as PipelineCostResponse;
    expect(body.runCount).toBe(0);
    expect(body.totalTokensIn).toBe(0);
  });

  test('aggregates token usage across runs and models', async () => {
    mockListByPipeline.mockResolvedValueOnce([
      {
        runId: 'r1',
        pipelineId: 'p1',
        status: 'succeeded',
        triggeredBy: 'u1',
        triggerType: 'manual',
        startedAt: 't1',
        tokenUsage: {
          'claude-3-5-sonnet': { tokensIn: 1000, tokensOut: 500 },
        },
        estimatedCostUsd: 0.001,
      },
      {
        runId: 'r2',
        pipelineId: 'p1',
        status: 'succeeded',
        triggeredBy: 'u1',
        triggerType: 'manual',
        startedAt: 't2',
        tokenUsage: {
          'claude-3-5-sonnet': { tokensIn: 2000, tokensOut: 1000 },
          'claude-3-haiku': { tokensIn: 500, tokensOut: 200 },
        },
        estimatedCostUsd: 0.003,
      },
    ]);

    const res = await run(app, 'GET', '/pipelines/p1/cost');

    expect(res.statusCode).toBe(200);
    const body = res.body as PipelineCostResponse;
    expect(body.runCount).toBe(2);
    expect(body.totalTokensIn).toBe(3500);
    expect(body.totalTokensOut).toBe(1700);
    expect(body.totalCostUsd).toBeCloseTo(0.004, 5);

    // Per-model aggregation
    expect(body.byModel['claude-3-5-sonnet'].tokensIn).toBe(3000);
    expect(body.byModel['claude-3-5-sonnet'].tokensOut).toBe(1500);
    expect(body.byModel['claude-3-haiku'].tokensIn).toBe(500);
    expect(body.byModel['claude-3-haiku'].tokensOut).toBe(200);
  });

  test('500 when listByPipeline rejects (asyncHandler forwards to errorHandler)', async () => {
    mockListByPipeline.mockRejectedValueOnce(new Error('DDB unavailable'));

    const res = await run(app, 'GET', '/pipelines/p1/cost');

    expect(res.statusCode).toBe(500);
  });

  test('per-model costUsd is computed from aggregated tokens', async () => {
    mockListByPipeline.mockResolvedValueOnce([
      {
        runId: 'r1',
        pipelineId: 'p1',
        status: 'succeeded',
        triggeredBy: 'u1',
        triggerType: 'manual',
        startedAt: 't1',
        tokenUsage: {
          'gpt-4': { tokensIn: 1_000_000, tokensOut: 0 },
        },
        estimatedCostUsd: 1.0,
      },
    ]);

    const res = await run(app, 'GET', '/pipelines/p1/cost');
    const body = res.body as PipelineCostResponse;
    // computeCost mock: (1_000_000 + 0) * 0.000001 = 1.0
    expect(body.byModel['gpt-4'].costUsd).toBeCloseTo(1.0, 5);
  });
});
