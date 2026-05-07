// social-api/src/routes/__tests__/pipelineRetry.test.ts
//
// Unit tests for pipelineRunActionsRouter: POST /force-fail (hub#1285),
// and pipelineRetryRouter: POST /:nodeId retry-from-node handler.

import express, { type NextFunction, type Request, type Response } from 'express';

// --- module-level mocks (must precede route import) -------------------------

const mockRunRepoGet = jest.fn();
const mockRunRepoPut = jest.fn();
const mockReadAttempts = jest.fn().mockReturnValue([]);

jest.mock('../../repositories/PipelineRunRepository', () => ({
  PipelineRunRepository: jest.fn().mockImplementation(() => ({
    get: mockRunRepoGet,
    put: mockRunRepoPut,
  })),
  readAttempts: (...args: unknown[]) => mockReadAttempts(...args),
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

import { pipelineRunActionsRouter, pipelineRetryRouter } from '../pipelineRetry';
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

async function run(
  app: express.Express,
  method: string,
  url: string,
  opts: { body?: unknown; userId?: string } = {},
): Promise<MockRes> {
  const res = mockRes();
  const req = {
    method: method.toUpperCase(),
    url,
    originalUrl: url,
    path: url,
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
  // Mirror routes/index.ts mount.
  app.use('/pipelines/:pipelineId/runs/:runId', pipelineRunActionsRouter);
  app.use(errorHandler);
  return app;
}

beforeEach(() => {
  jest.clearAllMocks();
  mockReadAttempts.mockReturnValue([]);
  (getPipelineBridge as jest.Mock).mockReturnValue(null);
});

// ---------------------------------------------------------------------------
// POST /pipelines/:pipelineId/runs/:runId/force-fail
// ---------------------------------------------------------------------------

describe('POST /pipelines/:pipelineId/runs/:runId/force-fail', () => {
  const app = buildApp();

  test('202 + interrupted status for a running+stuck run', async () => {
    const stuckRun = {
      pipelineId: 'p1',
      runId: 'r1',
      status: 'running',
      stuckAt: '2026-01-01T00:00:00.000Z',
      triggeredBy: 'user-1',
      triggerType: 'manual',
      startedAt: '2026-01-01T00:00:00.000Z',
    };
    mockRunRepoGet.mockResolvedValueOnce(stuckRun);
    mockRunRepoPut.mockResolvedValueOnce(undefined);

    const res = await run(app, 'POST', '/pipelines/p1/runs/r1/force-fail');

    expect(res.statusCode).toBe(202);
    const body = res.body as { runId: string; status: string; hint: string; at: string };
    expect(body.runId).toBe('r1');
    expect(body.status).toBe('interrupted');
    expect(typeof body.hint).toBe('string');
    expect(typeof body.at).toBe('string');
  });

  test('put() is called with status=interrupted and stuckAt cleared', async () => {
    const stuckRun = {
      pipelineId: 'p1',
      runId: 'r1',
      status: 'running',
      stuckAt: '2026-01-01T00:00:00.000Z',
      triggeredBy: 'user-1',
      triggerType: 'manual',
      startedAt: '2026-01-01T00:00:00.000Z',
    };
    mockRunRepoGet.mockResolvedValueOnce(stuckRun);
    mockRunRepoPut.mockResolvedValueOnce(undefined);

    await run(app, 'POST', '/pipelines/p1/runs/r1/force-fail');

    expect(mockRunRepoPut).toHaveBeenCalledTimes(1);
    const saved = (mockRunRepoPut.mock.calls[0] as [Record<string, unknown>])[0];
    expect(saved.status).toBe('interrupted');
    expect(saved.stuckAt).toBeUndefined();
    expect(typeof saved.completedAt).toBe('string');
  });

  test('404 when run does not exist', async () => {
    mockRunRepoGet.mockResolvedValueOnce(null);

    const res = await run(app, 'POST', '/pipelines/p1/runs/missing/force-fail');

    expect(res.statusCode).toBe(404);
    const body = res.body as { error: string };
    expect(body.error).toMatch(/not found/i);
  });

  test('400 when run status is not running', async () => {
    mockRunRepoGet.mockResolvedValueOnce({
      pipelineId: 'p1',
      runId: 'r1',
      status: 'succeeded',
      stuckAt: undefined,
      triggeredBy: 'user-1',
      triggerType: 'manual',
      startedAt: '2026-01-01T00:00:00.000Z',
    });

    const res = await run(app, 'POST', '/pipelines/p1/runs/r1/force-fail');

    expect(res.statusCode).toBe(400);
    const body = res.body as { error: string };
    expect(body.error).toMatch(/succeeded/);
  });

  test('400 when run is running but not flagged stuck', async () => {
    mockRunRepoGet.mockResolvedValueOnce({
      pipelineId: 'p1',
      runId: 'r1',
      status: 'running',
      stuckAt: undefined,
      triggeredBy: 'user-1',
      triggerType: 'manual',
      startedAt: '2026-01-01T00:00:00.000Z',
    });

    const res = await run(app, 'POST', '/pipelines/p1/runs/r1/force-fail');

    expect(res.statusCode).toBe(400);
    const body = res.body as { error: string };
    expect(body.error).toMatch(/not.*stuck|stuck/i);
  });

  test('best-effort bridge.cancelRun is attempted when bridge is wired', async () => {
    const stuckRun = {
      pipelineId: 'p1',
      runId: 'r1',
      status: 'running',
      stuckAt: '2026-01-01T00:00:00.000Z',
      triggeredBy: 'user-1',
      triggerType: 'manual',
      startedAt: '2026-01-01T00:00:00.000Z',
    };
    mockRunRepoGet.mockResolvedValueOnce(stuckRun);
    mockRunRepoPut.mockResolvedValueOnce(undefined);

    const cancelRun = jest.fn().mockResolvedValue(undefined);
    (getPipelineBridge as jest.Mock).mockReturnValue({ cancelRun });

    await run(app, 'POST', '/pipelines/p1/runs/r1/force-fail');

    // Give the fire-and-forget promise a tick to run.
    await new Promise((r) => setImmediate(r));
    expect(cancelRun).toHaveBeenCalledWith('r1', 'user-1');
  });
});

// ---------------------------------------------------------------------------
// POST /pipelines/:pipelineId/runs/:runId/retry/:nodeId
// ---------------------------------------------------------------------------

function buildRetryApp(): express.Express {
  const app = express();
  app.use(express.json());
  app.use((req, _res, next) => {
    if (!(req as Request & { user?: unknown }).user) {
      (req as Request & { user?: { sub: string } }).user = { sub: 'user-1' };
    }
    next();
  });
  // Mirror routes/index.ts mount: retry router at /:pipelineId/runs/:runId/retry
  app.use('/pipelines/:pipelineId/runs/:runId/retry', pipelineRetryRouter);
  app.use(errorHandler);
  return app;
}

const retryApp = buildRetryApp();

// Fixtures
const baseRun = {
  pipelineId: 'p1',
  runId: 'r1',
  status: 'failed',
  triggeredBy: 'user-1',
  triggerType: 'manual',
  startedAt: '2026-01-01T00:00:00.000Z',
  stepsSummary: { 'node-A': { status: 'completed' }, 'node-B': { status: 'failed' } },
};

const baseDef = {
  nodes: [{ id: 'node-A' }, { id: 'node-B' }],
  edges: [],
};

const mockBridge = {
  trigger: jest.fn().mockResolvedValue({ runId: 'new-run-42' }),
};

describe('POST /pipelines/:pipelineId/runs/:runId/retry/:nodeId', () => {
  test('202 with newRunId when all validations pass', async () => {
    mockRunRepoGet.mockResolvedValueOnce(baseRun);
    mockDefRepoGet.mockResolvedValueOnce(baseDef);
    (getPipelineBridge as jest.Mock).mockReturnValue(mockBridge);
    mockBridge.trigger.mockResolvedValueOnce({ runId: 'new-run-42' });

    const res = await run(retryApp, 'POST', '/pipelines/p1/runs/r1/retry/node-B');

    expect(res.statusCode).toBe(202);
    const body = res.body as { newRunId: string; originalRunId: string; retryFromNodeId: string; skippedNodes: string[] };
    expect(body.newRunId).toBe('new-run-42');
    expect(body.originalRunId).toBe('r1');
    expect(body.retryFromNodeId).toBe('node-B');
    expect(Array.isArray(body.skippedNodes)).toBe(true);
  });

  test('skippedNodes contains all nodes except the retry target', async () => {
    mockRunRepoGet.mockResolvedValueOnce(baseRun);
    mockDefRepoGet.mockResolvedValueOnce(baseDef);
    (getPipelineBridge as jest.Mock).mockReturnValue(mockBridge);
    mockBridge.trigger.mockResolvedValueOnce({ runId: 'new-run-43' });

    const res = await run(retryApp, 'POST', '/pipelines/p1/runs/r1/retry/node-B');

    const body = res.body as { skippedNodes: string[] };
    expect(body.skippedNodes).toEqual(['node-A']);
  });

  test('404 when original run does not exist', async () => {
    mockRunRepoGet.mockResolvedValueOnce(null);

    const res = await run(retryApp, 'POST', '/pipelines/p1/runs/missing/retry/node-B');

    expect(res.statusCode).toBe(404);
    const body = res.body as { error: string };
    expect(body.error).toMatch(/not found/i);
  });

  test('400 when run is not in a terminal state', async () => {
    mockRunRepoGet.mockResolvedValueOnce({ ...baseRun, status: 'running' });

    const res = await run(retryApp, 'POST', '/pipelines/p1/runs/r1/retry/node-B');

    expect(res.statusCode).toBe(400);
    const body = res.body as { error: string };
    expect(body.error).toMatch(/running/);
  });

  test('accepts succeeded and canceled runs as terminal', async () => {
    for (const status of ['succeeded', 'canceled']) {
      mockRunRepoGet.mockResolvedValueOnce({ ...baseRun, status });
      mockDefRepoGet.mockResolvedValueOnce(baseDef);
      (getPipelineBridge as jest.Mock).mockReturnValue(mockBridge);
      mockBridge.trigger.mockResolvedValueOnce({ runId: 'rX' });

      const res = await run(retryApp, 'POST', '/pipelines/p1/runs/r1/retry/node-B');
      expect(res.statusCode).toBe(202);
    }
  });

  test('404 when pipeline definition does not exist', async () => {
    mockRunRepoGet.mockResolvedValueOnce(baseRun);
    mockDefRepoGet.mockResolvedValueOnce(null);

    const res = await run(retryApp, 'POST', '/pipelines/p1/runs/r1/retry/node-B');

    expect(res.statusCode).toBe(404);
    const body = res.body as { error: string };
    expect(body.error).toMatch(/not found/i);
  });

  test('400 when nodeId is not in the pipeline definition', async () => {
    mockRunRepoGet.mockResolvedValueOnce(baseRun);
    mockDefRepoGet.mockResolvedValueOnce(baseDef);

    const res = await run(retryApp, 'POST', '/pipelines/p1/runs/r1/retry/node-MISSING');

    expect(res.statusCode).toBe(400);
    const body = res.body as { error: string };
    expect(body.error).toMatch(/not found/i);
  });

  test('400 when nodeId was not executed in the original run', async () => {
    const runWithoutNodeC = {
      ...baseRun,
      stepsSummary: { 'node-A': { status: 'completed' } },
    };
    const defWithNodeC = { nodes: [{ id: 'node-A' }, { id: 'node-C' }], edges: [] };

    mockRunRepoGet.mockResolvedValueOnce(runWithoutNodeC);
    mockDefRepoGet.mockResolvedValueOnce(defWithNodeC);

    const res = await run(retryApp, 'POST', '/pipelines/p1/runs/r1/retry/node-C');

    expect(res.statusCode).toBe(400);
    const body = res.body as { error: string };
    expect(body.error).toMatch(/not executed|didn.t run/i);
  });

  test('503 when bridge is not wired', async () => {
    mockRunRepoGet.mockResolvedValueOnce(baseRun);
    mockDefRepoGet.mockResolvedValueOnce(baseDef);
    (getPipelineBridge as jest.Mock).mockReturnValue(null);

    const res = await run(retryApp, 'POST', '/pipelines/p1/runs/r1/retry/node-B');

    expect(res.statusCode).toBe(503);
    const body = res.body as { error: string };
    expect(body.error).toMatch(/bridge/i);
  });

  test('bridge.trigger called with retryPayload including __retry metadata', async () => {
    mockRunRepoGet.mockResolvedValueOnce(baseRun);
    mockDefRepoGet.mockResolvedValueOnce(baseDef);
    (getPipelineBridge as jest.Mock).mockReturnValue(mockBridge);
    mockBridge.trigger.mockResolvedValueOnce({ runId: 'new-run-99' });

    await run(retryApp, 'POST', '/pipelines/p1/runs/r1/retry/node-B');

    expect(mockBridge.trigger).toHaveBeenCalledTimes(1);
    const [triggerArg] = mockBridge.trigger.mock.calls[0] as [{ pipelineId: string; triggerPayload: { __retry: unknown } }];
    expect(triggerArg.pipelineId).toBe('p1');
    expect(triggerArg.triggerPayload.__retry).toMatchObject({
      originalRunId: 'r1',
      retryFromNodeId: 'node-B',
    });
  });

  test('body.inputs merged into triggerPayload', async () => {
    mockRunRepoGet.mockResolvedValueOnce(baseRun);
    mockDefRepoGet.mockResolvedValueOnce(baseDef);
    (getPipelineBridge as jest.Mock).mockReturnValue(mockBridge);
    mockBridge.trigger.mockResolvedValueOnce({ runId: 'new-run-50' });

    await run(retryApp, 'POST', '/pipelines/p1/runs/r1/retry/node-B', {
      body: { inputs: { myInput: 'override' } },
    });

    const [triggerArg] = mockBridge.trigger.mock.calls[0] as [{ triggerPayload: Record<string, unknown> }];
    expect(triggerArg.triggerPayload['myInput']).toBe('override');
  });

  test('snapshotInput included in payload when snapshotInput=true and attempts exist', async () => {
    const storedInput = { data: 'persisted' };
    mockReadAttempts.mockReturnValue([{ input: storedInput }]);
    mockRunRepoGet.mockResolvedValueOnce(baseRun);
    mockDefRepoGet.mockResolvedValueOnce(baseDef);
    (getPipelineBridge as jest.Mock).mockReturnValue(mockBridge);
    mockBridge.trigger.mockResolvedValueOnce({ runId: 'snap-run' });

    await run(retryApp, 'POST', '/pipelines/p1/runs/r1/retry/node-B', {
      body: { snapshotInput: true },
    });

    const [triggerArg] = mockBridge.trigger.mock.calls[0] as [{ triggerPayload: Record<string, unknown> }];
    expect(triggerArg.triggerPayload['__snapshotInput']).toEqual(storedInput);
  });

  test('snapshotInput omitted when snapshotInput=true but no attempts', async () => {
    mockReadAttempts.mockReturnValue([]);
    mockRunRepoGet.mockResolvedValueOnce(baseRun);
    mockDefRepoGet.mockResolvedValueOnce(baseDef);
    (getPipelineBridge as jest.Mock).mockReturnValue(mockBridge);
    mockBridge.trigger.mockResolvedValueOnce({ runId: 'no-snap' });

    await run(retryApp, 'POST', '/pipelines/p1/runs/r1/retry/node-B', {
      body: { snapshotInput: true },
    });

    const [triggerArg] = mockBridge.trigger.mock.calls[0] as [{ triggerPayload: Record<string, unknown> }];
    expect(triggerArg.triggerPayload['__snapshotInput']).toBeUndefined();
  });

  test('rethrows when bridge.trigger rejects', async () => {
    mockRunRepoGet.mockResolvedValueOnce(baseRun);
    mockDefRepoGet.mockResolvedValueOnce(baseDef);
    (getPipelineBridge as jest.Mock).mockReturnValue(mockBridge);
    mockBridge.trigger.mockRejectedValueOnce(new Error('executor down'));

    // errorHandler converts unhandled errors to 500
    const res = await run(retryApp, 'POST', '/pipelines/p1/runs/r1/retry/node-B');
    expect(res.statusCode).toBe(500);
  });
});
