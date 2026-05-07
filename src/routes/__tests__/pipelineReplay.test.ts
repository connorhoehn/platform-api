// social-api/src/routes/__tests__/pipelineReplay.test.ts
//
// Unit tests for pipelineReplayRouter: POST /replay.

import express, { type NextFunction, type Request, type Response } from 'express';

// --- module-level mocks (must precede route import) -------------------------

const mockRunRepoGet = jest.fn();

jest.mock('../../repositories/PipelineRunRepository', () => ({
  PipelineRunRepository: jest.fn().mockImplementation(() => ({
    get: mockRunRepoGet,
  })),
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

jest.mock('../../lib/logger', () => ({
  withContext: () => ({
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
    child: () => ({ info: jest.fn(), warn: jest.fn(), error: jest.fn() }),
  }),
}));

jest.mock('../../observability/metrics', () => ({
  recordPipelineTrigger: jest.fn(),
  recordPipelineError: jest.fn(),
}));

jest.mock('@opentelemetry/api', () => ({
  trace: {
    getTracer: () => ({
      startActiveSpan: jest.fn(
        (_name: string, _opts: unknown, cb: (span: unknown) => Promise<void>) =>
          cb({
            setAttribute: jest.fn(),
            setStatus: jest.fn(),
            recordException: jest.fn(),
            end: jest.fn(),
          }),
      ),
    }),
  },
  SpanStatusCode: { OK: 1, ERROR: 2 },
}));

const mockGetPipelineBridge = jest.fn().mockReturnValue(null);
jest.mock('../pipelineTriggers', () => ({
  getPipelineBridge: (...args: unknown[]) => mockGetPipelineBridge(...args),
}));

import { pipelineReplayRouter } from '../pipelineReplay';
import { errorHandler } from '../../middleware/error-handler';

// ---------------------------------------------------------------------------
// Test rig (same pattern as other route tests in this directory)
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
  app.use('/pipelines/:pipelineId/runs/:runId/replay', pipelineReplayRouter);
  app.use(errorHandler);
  return app;
}

const completedRun = {
  pipelineId: 'p1',
  runId: 'r1',
  status: 'failed',
  triggeredBy: 'user-1',
  triggerType: 'manual',
  startedAt: '2026-01-01T00:00:00.000Z',
  completedAt: '2026-01-01T00:01:00.000Z',
};

const definition = { id: 'p1', name: 'Test Pipeline', nodes: [], edges: [] };

beforeEach(() => {
  jest.clearAllMocks();
  mockGetPipelineBridge.mockReturnValue(null);
});

// ---------------------------------------------------------------------------
// POST /pipelines/:pipelineId/runs/:runId/replay
// ---------------------------------------------------------------------------

describe('POST /pipelines/:pipelineId/runs/:runId/replay', () => {
  const app = buildApp();

  test('503 when bridge is not wired', async () => {
    mockRunRepoGet.mockResolvedValueOnce(completedRun);

    const res = await run(app, 'POST', '/pipelines/p1/runs/r1/replay');

    expect(res.statusCode).toBe(503);
    const body = res.body as { error: string; action: string };
    expect(body.action).toBe('replay');
  });

  test('202 + new runId when bridge is wired and run is terminal', async () => {
    mockRunRepoGet.mockResolvedValueOnce(completedRun);
    mockDefRepoGet.mockResolvedValueOnce(definition);

    const triggerMock = jest.fn().mockResolvedValue({ runId: 'new-run-1' });
    mockGetPipelineBridge.mockReturnValue({ trigger: triggerMock });

    const res = await run(app, 'POST', '/pipelines/p1/runs/r1/replay');

    expect(res.statusCode).toBe(202);
    const body = res.body as { newRunId: string; originalRunId: string; pipelineId: string; at: string };
    expect(body.newRunId).toBe('new-run-1');
    expect(body.originalRunId).toBe('r1');
    expect(body.pipelineId).toBe('p1');
    expect(typeof body.at).toBe('string');
  });

  test('404 when run does not exist', async () => {
    mockRunRepoGet.mockResolvedValueOnce(null);

    const res = await run(app, 'POST', '/pipelines/p1/runs/missing/replay');

    expect(res.statusCode).toBe(404);
    const body = res.body as { error: string };
    expect(body.error).toMatch(/not found/i);
  });

  test('400 when run is not in terminal state', async () => {
    mockRunRepoGet.mockResolvedValueOnce({ ...completedRun, status: 'running', completedAt: undefined });

    const res = await run(app, 'POST', '/pipelines/p1/runs/r1/replay');

    expect(res.statusCode).toBe(400);
    const body = res.body as { error: string };
    expect(body.error).toMatch(/running/);
  });

  test('404 when pipeline definition not found', async () => {
    mockRunRepoGet.mockResolvedValueOnce({ ...completedRun, status: 'succeeded' });
    mockDefRepoGet.mockResolvedValueOnce(null);

    const triggerMock = jest.fn();
    mockGetPipelineBridge.mockReturnValue({ trigger: triggerMock });

    const res = await run(app, 'POST', '/pipelines/p1/runs/r1/replay');

    expect(res.statusCode).toBe(404);
    expect(triggerMock).not.toHaveBeenCalled();
  });

  test('accepts all terminal statuses (succeeded, canceled)', async () => {
    for (const status of ['succeeded', 'canceled']) {
      mockRunRepoGet.mockResolvedValueOnce({ ...completedRun, status });
      mockDefRepoGet.mockResolvedValueOnce(definition);
      const triggerMock = jest.fn().mockResolvedValue({ runId: `new-${status}` });
      mockGetPipelineBridge.mockReturnValue({ trigger: triggerMock });

      const res = await run(app, 'POST', `/pipelines/p1/runs/r1/replay`);
      expect(res.statusCode).toBe(202);
    }
  });

  // triggerPayload forwarding contract — pipelineReplay.ts:106
  //   body.triggerPayload ?? originalRun.stepsSummary ?? {}

  test('body.triggerPayload is forwarded to bridge.trigger when provided', async () => {
    mockRunRepoGet.mockResolvedValueOnce(completedRun);
    mockDefRepoGet.mockResolvedValueOnce(definition);
    const triggerMock = jest.fn().mockResolvedValue({ runId: 'new-r2' });
    mockGetPipelineBridge.mockReturnValue({ trigger: triggerMock });

    await run(app, 'POST', '/pipelines/p1/runs/r1/replay', {
      body: { triggerPayload: { foo: 'bar' } },
    });

    const callArgs = triggerMock.mock.calls[0]![0] as { triggerPayload: unknown };
    expect(callArgs.triggerPayload).toEqual({ foo: 'bar' });
  });

  test('stepsSummary is used as triggerPayload fallback when body.triggerPayload absent', async () => {
    const runWithSteps = {
      ...completedRun,
      stepsSummary: { 'node-a': { status: 'completed', output: { answer: 42 } } },
    };
    mockRunRepoGet.mockResolvedValueOnce(runWithSteps);
    mockDefRepoGet.mockResolvedValueOnce(definition);
    const triggerMock = jest.fn().mockResolvedValue({ runId: 'new-r3' });
    mockGetPipelineBridge.mockReturnValue({ trigger: triggerMock });

    await run(app, 'POST', '/pipelines/p1/runs/r1/replay');

    const callArgs = triggerMock.mock.calls[0]![0] as { triggerPayload: unknown };
    expect(callArgs.triggerPayload).toEqual(runWithSteps.stepsSummary);
  });

  test('empty object used as triggerPayload when both body and stepsSummary are absent', async () => {
    // completedRun has no stepsSummary — both fallbacks resolve to {}
    mockRunRepoGet.mockResolvedValueOnce(completedRun);
    mockDefRepoGet.mockResolvedValueOnce(definition);
    const triggerMock = jest.fn().mockResolvedValue({ runId: 'new-r4' });
    mockGetPipelineBridge.mockReturnValue({ trigger: triggerMock });

    await run(app, 'POST', '/pipelines/p1/runs/r1/replay');

    const callArgs = triggerMock.mock.calls[0]![0] as { triggerPayload: unknown };
    expect(callArgs.triggerPayload).toEqual({});
  });
});
