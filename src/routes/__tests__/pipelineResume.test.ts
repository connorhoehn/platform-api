// social-api/src/routes/__tests__/pipelineResume.test.ts
//
// Unit tests for pipelineResumeRouter: POST /resume?fromNodeId=<id>
// (hub#2223 — restart-from-failure).

import express, { type NextFunction, type Request, type Response } from 'express';

// --- module-level mocks (must precede route import) -------------------------

const mockRunRepoGet = jest.fn();

jest.mock('../../repositories/PipelineRunRepository', () => ({
  PipelineRunRepository: jest.fn().mockImplementation(() => ({
    get: mockRunRepoGet,
  })),
  readAttempts: jest.fn().mockReturnValue([]),
}));

const mockDefRepoGet = jest.fn();

jest.mock('../../pipeline/definitions-repository', () => ({
  DefinitionsRepository: jest.fn().mockImplementation(() => ({
    get: mockDefRepoGet,
  })),
}));

jest.mock('../../lib/ddb-client', () => ({
  getDynamoClient: jest.fn().mockReturnValue({}),
}));

jest.mock('../../lib/redis-client', () => ({
  getRedisClient: async () => null,
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

jest.mock('../pipelineTriggers', () => ({
  getPipelineBridge: jest.fn().mockReturnValue(null),
}));

import { pipelineResumeRouter } from '../pipelineResume';
import { errorHandler } from '../../middleware/error-handler';
import { getPipelineBridge } from '../pipelineTriggers';

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

async function dispatch(
  app: express.Express,
  method: string,
  url: string,
  opts: { body?: unknown; userId?: string } = {},
): Promise<MockRes> {
  const res = mockRes();
  // Build the URL once and parse the query the same way Express would.
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
  // Mirror routes/index.ts mount.
  app.use('/pipelines/:pipelineId/runs/:runId/resume', pipelineResumeRouter);
  app.use(errorHandler);
  return app;
}

const app = buildApp();

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const failedRun = {
  pipelineId: 'p1',
  runId: 'r1',
  status: 'failed',
  triggeredBy: 'user-1',
  triggerType: 'manual',
  startedAt: '2026-01-01T00:00:00.000Z',
  stepsSummary: {
    'node-A': { status: 'completed' },
    'node-B': { status: 'failed' },
  },
  error: 'node-B: synthetic failure',
};

const baseDef = {
  nodes: [{ id: 'node-A' }, { id: 'node-B' }, { id: 'node-C' }],
  edges: [],
};

const mockBridge = {
  resumeFromStep: jest.fn(),
};

beforeEach(() => {
  jest.clearAllMocks();
  (getPipelineBridge as jest.Mock).mockReturnValue(null);
  mockBridge.resumeFromStep.mockReset();
});

// ---------------------------------------------------------------------------
// POST /pipelines/:pipelineId/runs/:runId/resume?fromNodeId=<nodeId>
// ---------------------------------------------------------------------------

describe('POST /pipelines/:pipelineId/runs/:runId/resume', () => {
  test('202 with runId + replayAttempts on the happy path', async () => {
    mockRunRepoGet.mockResolvedValueOnce(failedRun);
    mockDefRepoGet.mockResolvedValueOnce(baseDef);
    (getPipelineBridge as jest.Mock).mockReturnValue(mockBridge);
    mockBridge.resumeFromStep.mockResolvedValueOnce({
      id: 'r1',
      status: 'completed',
      replayAttempts: 1,
    });

    const res = await dispatch(
      app,
      'POST',
      '/pipelines/p1/runs/r1/resume?fromNodeId=node-B',
    );

    expect(res.statusCode).toBe(202);
    const body = res.body as {
      runId: string;
      fromNodeId: string;
      replayAttempts: number;
      originalFailedStepId?: string;
      at: string;
    };
    expect(body.runId).toBe('r1');
    expect(body.fromNodeId).toBe('node-B');
    expect(body.replayAttempts).toBe(1);
    // From the stepsSummary, node-B was the failed step.
    expect(body.originalFailedStepId).toBe('node-B');
    expect(typeof body.at).toBe('string');

    expect(mockBridge.resumeFromStep).toHaveBeenCalledWith('r1', 'node-B', {});
  });

  test('400 when fromNodeId query param is missing', async () => {
    const res = await dispatch(app, 'POST', '/pipelines/p1/runs/r1/resume');
    expect(res.statusCode).toBe(400);
    const body = res.body as { error: string };
    expect(body.error).toMatch(/fromNodeId/);
  });

  test('404 when the run does not exist', async () => {
    mockRunRepoGet.mockResolvedValueOnce(null);
    const res = await dispatch(
      app,
      'POST',
      '/pipelines/p1/runs/missing/resume?fromNodeId=node-B',
    );
    expect(res.statusCode).toBe(404);
    const body = res.body as { error: string };
    expect(body.error).toMatch(/not found/i);
  });

  test('400 when the run is not in a failed terminal state', async () => {
    mockRunRepoGet.mockResolvedValueOnce({ ...failedRun, status: 'running' });
    const res = await dispatch(
      app,
      'POST',
      '/pipelines/p1/runs/r1/resume?fromNodeId=node-B',
    );
    expect(res.statusCode).toBe(400);
    const body = res.body as { error: string };
    expect(body.error).toMatch(/running/);
  });

  test('400 when the run is succeeded and allowCompleted is not set', async () => {
    mockRunRepoGet.mockResolvedValueOnce({ ...failedRun, status: 'succeeded' });
    const res = await dispatch(
      app,
      'POST',
      '/pipelines/p1/runs/r1/resume?fromNodeId=node-B',
    );
    expect(res.statusCode).toBe(400);
    const body = res.body as { error: string };
    expect(body.error).toMatch(/succeeded/);
  });

  test('202 when the run is succeeded and allowCompleted=true', async () => {
    mockRunRepoGet.mockResolvedValueOnce({ ...failedRun, status: 'succeeded' });
    mockDefRepoGet.mockResolvedValueOnce(baseDef);
    (getPipelineBridge as jest.Mock).mockReturnValue(mockBridge);
    mockBridge.resumeFromStep.mockResolvedValueOnce({
      id: 'r1',
      status: 'completed',
      replayAttempts: 2,
    });

    const res = await dispatch(
      app,
      'POST',
      '/pipelines/p1/runs/r1/resume?fromNodeId=node-A',
      { body: { allowCompleted: true } },
    );

    expect(res.statusCode).toBe(202);
    expect(mockBridge.resumeFromStep).toHaveBeenCalledWith('r1', 'node-A', {
      allowCompleted: true,
    });
  });

  test('404 when the pipeline definition is missing', async () => {
    mockRunRepoGet.mockResolvedValueOnce(failedRun);
    mockDefRepoGet.mockResolvedValueOnce(null);
    const res = await dispatch(
      app,
      'POST',
      '/pipelines/p1/runs/r1/resume?fromNodeId=node-B',
    );
    expect(res.statusCode).toBe(404);
  });

  test('400 when fromNodeId is not in the original DAG', async () => {
    mockRunRepoGet.mockResolvedValueOnce(failedRun);
    mockDefRepoGet.mockResolvedValueOnce(baseDef);
    const res = await dispatch(
      app,
      'POST',
      '/pipelines/p1/runs/r1/resume?fromNodeId=node-MISSING',
    );
    expect(res.statusCode).toBe(400);
    const body = res.body as { error: string };
    expect(body.error).toMatch(/not found/i);
  });

  test('503 when the bridge is not wired', async () => {
    mockRunRepoGet.mockResolvedValueOnce(failedRun);
    mockDefRepoGet.mockResolvedValueOnce(baseDef);
    (getPipelineBridge as jest.Mock).mockReturnValue(null);
    const res = await dispatch(
      app,
      'POST',
      '/pipelines/p1/runs/r1/resume?fromNodeId=node-B',
    );
    expect(res.statusCode).toBe(503);
    const body = res.body as { error: string };
    expect(body.error).toMatch(/bridge/i);
  });

  test('rethrows when bridge.resumeFromStep rejects', async () => {
    mockRunRepoGet.mockResolvedValueOnce(failedRun);
    mockDefRepoGet.mockResolvedValueOnce(baseDef);
    (getPipelineBridge as jest.Mock).mockReturnValue(mockBridge);
    mockBridge.resumeFromStep.mockRejectedValueOnce(new Error('executor down'));

    const res = await dispatch(
      app,
      'POST',
      '/pipelines/p1/runs/r1/resume?fromNodeId=node-B',
    );
    expect(res.statusCode).toBe(500);
  });

  test('replayAttempts defaults to 0 when the runtime omits the field', async () => {
    mockRunRepoGet.mockResolvedValueOnce(failedRun);
    mockDefRepoGet.mockResolvedValueOnce(baseDef);
    (getPipelineBridge as jest.Mock).mockReturnValue(mockBridge);
    mockBridge.resumeFromStep.mockResolvedValueOnce({ id: 'r1', status: 'completed' });

    const res = await dispatch(
      app,
      'POST',
      '/pipelines/p1/runs/r1/resume?fromNodeId=node-B',
    );

    expect(res.statusCode).toBe(202);
    const body = res.body as { replayAttempts: number };
    expect(body.replayAttempts).toBe(0);
  });
});
