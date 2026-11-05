// social-api/src/routes/__tests__/pipelineResume.test.ts
//
// Unit tests for pipelineResumeRouter: POST /resume?fromNodeId=<id>
// (hub#2223 — restart-from-failure).

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

const mockSystemPipelineGet = jest.fn();
jest.mock('../../pipeline/systemPipelines', () => ({
  getSystemPipeline: (id: string) => mockSystemPipelineGet(id),
}));

jest.mock('../../lib/ddb-client', () => ({
  getDynamoClient: jest.fn().mockReturnValue({}),
}));

// Route behavior is isolated from the background audit database writer.
// Real SDK requests can outlive this file's Jest environment.
jest.mock('../../pipeline/audit-repository', () => ({
  auditRepo: { record: jest.fn().mockResolvedValue(undefined) },
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

import { pipelineResumeRouter, runRefusal } from '../pipelineResume';
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

  test('400, with the refusal verbatim, when the run was refused — resuming would decide the same way', async () => {
    mockRunRepoGet.mockResolvedValueOnce({
      ...failedRun,
      error: 'node-B: deck-revise constraint-not-met: The edit did not do what was asked: Title has 2 words; it must have exactly 0. Nothing was saved.',
    });
    const res = await dispatch(app, 'POST', '/pipelines/p1/runs/r1/resume?fromNodeId=node-B');
    expect(res.statusCode).toBe(400);
    const body = res.body as { error: string };
    expect(body.error).toMatch(/refused \(constraint-not-met\) and cannot be resumed/);
    expect(body.error).toContain('deck-revise constraint-not-met: The edit did not do what was asked: Title has 2 words; it must have exactly 0. Nothing was saved.');
    expect(body.error).toMatch(/start a new run/);
    expect((getPipelineBridge as jest.Mock)).not.toHaveBeenCalled();
  });

  test('runRefusal reads the stored row and an executor error object alike; a plain failure is not one', () => {
    expect(runRefusal({ error: 'node-B: deck-revise forbidden: Only the owner may edit.' }))
      .toEqual({ code: 'forbidden', message: 'deck-revise forbidden: Only the owner may edit.' });
    expect(runRefusal({ error: { message: 'deck-revise revision-conflict: v7 moved to v8.' } }))
      .toEqual({ code: 'revision-conflict', message: 'deck-revise revision-conflict: v7 moved to v8.' });
    expect(runRefusal({ error: 'node-B: synthetic failure' })).toBeNull();
    expect(runRefusal({ error: 'node-B: connect ECONNREFUSED' })).toBeNull();
    expect(runRefusal({})).toBeNull();
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

  test('resumes a run of a platform-owned pipeline from the system registry', async () => {
    mockRunRepoGet.mockResolvedValueOnce({ ...failedRun, pipelineId: 'document-agent-edit' });
    mockDefRepoGet.mockResolvedValueOnce(null);
    mockSystemPipelineGet.mockReturnValueOnce(baseDef);
    const res = await dispatch(
      app,
      'POST',
      '/pipelines/document-agent-edit/runs/r1/resume?fromNodeId=node-B',
    );
    expect(res.statusCode).not.toBe(404);
    expect(mockSystemPipelineGet).toHaveBeenCalledWith('document-agent-edit');
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

// ---------------------------------------------------------------------------
// After a restart: no executor holds the run, the stored row is the source
// ---------------------------------------------------------------------------

describe('POST …/resume after a restart — the stored checkpoint goes along', () => {
  const snapshot = {
    id: 'p1',
    nodes: [{ id: 'trigger', type: 'trigger' }, { id: 'node-A', type: 'action' }, { id: 'node-B', type: 'action' }],
    edges: [{ id: 'e0', source: 'trigger', target: 'node-A' }, { id: 'e1', source: 'node-A', target: 'node-B' }],
  };
  const storedRun = {
    ...failedRun,
    definitionSnapshot: snapshot,
    checkpoint: {
      sequenceNumber: 4,
      at: '2026-01-01T00:01:00.000Z',
      context: { steps: { trigger: {}, 'node-A': { executed: 'A' } }, executed: 'A' },
      currentStepIds: ['node-B'],
      steps: {
        trigger: { nodeId: 'trigger', status: 'completed', completedAt: '2026-01-01T00:00:01.000Z' },
        'node-A': { nodeId: 'node-A', status: 'completed', completedAt: '2026-01-01T00:00:02.000Z' },
        'node-B': { nodeId: 'node-B', status: 'failed', error: 'synthetic failure' },
      },
      completedStepCount: 2,
    },
    stepsSummary: {
      'node-A': { status: 'completed', output: { display: 'A (truncated copy)' } },
      'node-B': { status: 'failed' },
    },
  };

  test('a failed run: the snapshot, the rewindable state, the stored status and the allowed statuses', async () => {
    mockRunRepoGet.mockResolvedValueOnce(storedRun);
    mockDefRepoGet.mockResolvedValueOnce(baseDef);
    (getPipelineBridge as jest.Mock).mockReturnValue(mockBridge);
    mockBridge.resumeFromStep.mockResolvedValueOnce({ id: 'r1', status: 'completed', replayAttempts: 1 });

    const res = await dispatch(app, 'POST', '/pipelines/p1/runs/r1/resume?fromNodeId=node-B');

    expect(res.statusCode).toBe(202);
    expect((res.body as { replayAttempts: number }).replayAttempts).toBe(1);
    const [runId, fromNodeId, options] = mockBridge.resumeFromStep.mock.calls[0]!;
    expect([runId, fromNodeId]).toEqual(['r1', 'node-B']);
    expect(options).toMatchObject({ definition: snapshot, runStatus: 'failed', allowStatuses: ['failed'] });
    expect(options.resumeState).toMatchObject({
      runId: 'r1',
      sequenceNumber: 4,
      replayAttempts: 0,
      triggeredBy: { userId: 'user-1', triggerType: 'manual', payload: {} },
    });
    // The rewind rebuilds context from step outputs: the executor's output, not the display copy.
    expect(options.resumeState.steps['node-A'].output).toEqual({ executed: 'A' });
  });

  test('a completed run with allowCompleted: stored status normalized, completed allowed', async () => {
    mockRunRepoGet.mockResolvedValueOnce({ ...storedRun, status: 'succeeded' });
    mockDefRepoGet.mockResolvedValueOnce(baseDef);
    (getPipelineBridge as jest.Mock).mockReturnValue(mockBridge);
    mockBridge.resumeFromStep.mockResolvedValueOnce({ id: 'r1', status: 'completed', replayAttempts: 1 });

    const res = await dispatch(app, 'POST', '/pipelines/p1/runs/r1/resume?fromNodeId=node-A', { body: { allowCompleted: true } });

    expect(res.statusCode).toBe(202);
    expect(mockBridge.resumeFromStep.mock.calls[0]![2]).toMatchObject({
      allowCompleted: true, runStatus: 'completed', allowStatuses: ['failed', 'completed'], definition: snapshot,
    });
  });

  test('400 when the node is in the current definition but not in the graph the run executed', async () => {
    mockRunRepoGet.mockResolvedValueOnce(storedRun);
    mockDefRepoGet.mockResolvedValueOnce(baseDef); // has node-C; the snapshot does not
    (getPipelineBridge as jest.Mock).mockReturnValue(mockBridge);

    const res = await dispatch(app, 'POST', '/pipelines/p1/runs/r1/resume?fromNodeId=node-C');

    expect(res.statusCode).toBe(400);
    expect((res.body as { error: string }).error).toMatch(/not in the graph run r1 executed/);
    expect(mockBridge.resumeFromStep).not.toHaveBeenCalled();
  });

  test('a row without a snapshot rewinds over the current definition', async () => {
    mockRunRepoGet.mockResolvedValueOnce({ ...storedRun, definitionSnapshot: undefined });
    mockDefRepoGet.mockResolvedValueOnce(baseDef);
    (getPipelineBridge as jest.Mock).mockReturnValue(mockBridge);
    mockBridge.resumeFromStep.mockResolvedValueOnce({ id: 'r1', status: 'completed', replayAttempts: 1 });

    const res = await dispatch(app, 'POST', '/pipelines/p1/runs/r1/resume?fromNodeId=node-B');

    expect(res.statusCode).toBe(202);
    expect(mockBridge.resumeFromStep.mock.calls[0]![2]).toMatchObject({ definition: baseDef });
  });
});
