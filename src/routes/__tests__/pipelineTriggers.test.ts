// Tests for pipelineTriggers + pipelineApprovals routes.
//
// Covers:
//   - POST  /  → 202 with synthesized runId when no bridge wired
//   - POST  /  → forwards to bridge.trigger when wired
//   - POST  /  → 400 when definition.id mismatches :pipelineId
//   - GET   /  → list past runs (paginated) from in-memory stub or bridge
//   - GET   /:runId → 404 when no bridge / unknown runId; 200 with snapshot otherwise
//   - GET   /:runId/history → [] when no bridge; pass-through otherwise
//   - GET   /:runId/history?fromVersion=-1 → 400
//   - GET   /pipelines/runs/active → list active runs (stub or bridge)
//   - POST  /pipelines/:runId/cancel → 200 / 404 / 400 paths
//   - POST  /api/pipelines/:runId/approvals → 204 (stub) and via bridge
//   - approvals route validates body shape (stepId, decision)

import express, { type NextFunction, type Request, type Response } from 'express';

// Stub redis-client so the idempotency middleware doesn't try to dial Redis
// in unit tests (saves ~30ms/test and keeps stderr clean).
const mockRunRepoGet = jest.fn();
jest.mock('../../repositories/PipelineRunRepository', () => ({
  PipelineRunRepository: jest.fn().mockImplementation(() => ({ get: mockRunRepoGet })),
}));
jest.mock('../../lib/ddb-client', () => ({ getDynamoClient: () => ({}) }));

// NFR #115: the approvals POST consults the rollup subscriber (pipelineId
// for a run this process never saw), the rollup row (is the gate on it) and
// the forwarder (does this replica hold the gate). Each is a seam here.
const mockResolvePipelineId = jest.fn(async (_runId: string): Promise<string | null> => null);
const mockLookupPipelineIdByRunId = jest.fn(async (_runId: string): Promise<string | null> => null);
jest.mock('../../pipeline/run-rollup-subscriber', () => ({
  getRunRollupSubscriber: () => ({ resolvePipelineId: mockResolvePipelineId }),
  lookupPipelineIdByRunId: (runId: string) => mockLookupPipelineIdByRunId(runId),
}));
const mockRollupGet = jest.fn(async (_pipelineId: string): Promise<unknown> => null);
jest.mock('../../repositories/PipelineRunRollupRepository', () => ({
  pipelineRunRollupRepo: { get: (pipelineId: string) => mockRollupGet(pipelineId), listWithOpenApprovals: async () => [] },
}));

// The run-detail GET overlays per-step `attempts` from the persistence
// service, and the trigger POST registers the trigger on it. Those are the
// only two methods the routes under test call; `getRunPersistenceService` is
// the only export the route graph imports from this module.
const mockStepAttempts = jest.fn((_runId: string): Record<string, unknown[]> => ({}));
jest.mock('../../pipeline/runPersistence', () => ({
  getRunPersistenceService: () => ({ getStepAttempts: mockStepAttempts, registerTrigger: jest.fn() }),
}));

jest.mock('../../lib/redis-client', () => ({
  getRedisClient: async () => null,
}));

// Stub aws-clients so BaseRepository's docClient calls don't trigger the
// AWS SDK credential-provider's dynamic ESM import — Jest aborts with
// ERR_VM_DYNAMIC_IMPORT_CALLBACK_MISSING_FLAG otherwise. The tests below
// don't exercise real DDB behavior; route logic is what's covered.
jest.mock('../../lib/aws-clients', () => ({
  docClient: { send: jest.fn().mockResolvedValue({ Item: undefined, Items: [] }) },
  ddbClient: { send: jest.fn().mockResolvedValue({}) },
  sqsClient: { send: jest.fn().mockResolvedValue({}) },
}));

import { errorHandler } from '../../middleware/error-handler';
import { setApprovalForwarder, type ApprovalForwarder } from '../../pipeline/approval-forwarder';
import { setRunControlForwarder, type RunControlForwarder } from '../../pipeline/run-control-forwarder';
import {
  pipelineTriggersRouter,
  pipelineApprovalsRouter,
  pipelineActiveRunsRouter,
  pipelineCancelRouter,
  pipelineBreakpointRouter,
  setPipelineBridge,
  stubRunStore,
  type PipelineBridge,
  type PipelineRunSnapshot,
} from '../pipelineTriggers';

// ---------------------------------------------------------------------------
// Test rig — build a tiny Express app, invoke handlers via the router stack
// directly. We don't use supertest (not in the social-api dep tree); instead,
// we call into the router with mock req/res that capture status/body/headers.
// ---------------------------------------------------------------------------

interface MockRes {
  statusCode: number;
  headers: Record<string, string | number | readonly string[]>;
  body: unknown;
  ended: boolean;
  finished: Promise<void>;
  status(code: number): MockRes;
  json(body: unknown): MockRes;
  setHeader(name: string, value: number | string | readonly string[]): MockRes;
  getHeader(name: string): unknown;
  end(...args: unknown[]): MockRes;
}

function mockRes(): MockRes {
  let resolveFinished!: () => void;
  const finished = new Promise<void>((r) => {
    resolveFinished = r;
  });
  const r: MockRes = {
    statusCode: 200,
    headers: {},
    body: undefined,
    ended: false,
    finished,
    status(code) {
      this.statusCode = code;
      return this;
    },
    json(body) {
      this.body = body;
      this.ended = true;
      resolveFinished();
      return this;
    },
    setHeader(name, value) {
      this.headers[String(name)] = value;
      return this;
    },
    getHeader(name) {
      return this.headers[String(name)];
    },
    end(..._args) {
      this.ended = true;
      resolveFinished();
      return this;
    },
  };
  return r;
}

/**
 * Drive a request through an Express app, returning the captured response.
 * Bypasses sockets entirely; we hand-roll the req/res pair.
 */
async function run(
  app: express.Express,
  method: string,
  url: string,
  opts: { body?: unknown; headers?: Record<string, string>; userId?: string } = {},
): Promise<MockRes> {
  const res = mockRes();
  const headers: Record<string, string> = {};
  for (const [k, v] of Object.entries(opts.headers ?? {})) {
    headers[k.toLowerCase()] = v;
  }
  // Parse out path/query manually to avoid pulling in URL helpers.
  const [pathname, qs] = url.split('?', 2);
  const query: Record<string, string> = {};
  if (qs) {
    for (const part of qs.split('&')) {
      const [k, v] = part.split('=', 2);
      if (k) query[decodeURIComponent(k)] = v ? decodeURIComponent(v) : '';
    }
  }
  const req = {
    method: method.toUpperCase(),
    url,
    originalUrl: url,
    path: pathname,
    headers,
    query,
    body: opts.body,
    user: { sub: opts.userId ?? 'user-1' },
  } as unknown as Request;

  await new Promise<void>((resolve, reject) => {
    const finalHandler = (err?: unknown): void => {
      if (err) {
        reject(err);
        return;
      }
      // 404 fallback if the router didn't match anything.
      if (!res.ended) {
        res.status(404).json({ error: 'route not found' });
      }
      resolve();
    };
    // Drive the app's handler stack.
    (app as unknown as (
      req: Request,
      res: Response,
      next: NextFunction,
    ) => void)(req, res as unknown as Response, finalHandler);
    // Resolve once res is finished.
    res.finished.then(() => resolve()).catch(reject);
  });
  return res;
}

function buildApp(): express.Express {
  const app = express();
  app.use(express.json());
  // Simulate auth middleware.
  app.use((req, _res, next) => {
    if (!(req as Request & { user?: unknown }).user) {
      (req as Request & { user?: { sub: string } }).user = { sub: 'user-1' };
    }
    next();
  });
  // Active-runs router must be mounted BEFORE the :pipelineId one, mirroring
  // routes/index.ts (Express resolves mounts in registration order).
  app.use('/pipelines/runs/active', pipelineActiveRunsRouter);
  app.use('/pipelines/:pipelineId/runs', pipelineTriggersRouter);
  app.use('/pipelines/:runId/cancel', pipelineCancelRouter);
  app.use('/pipelines/:runId/approvals', pipelineApprovalsRouter);
  app.use('/pipelines/:pipelineId/runs/:runId/breakpoint/:stepId/resolve', pipelineBreakpointRouter);
  app.use(errorHandler);
  return app;
}

afterEach(() => {
  setPipelineBridge(null);
  stubRunStore.__resetForTests();
});

// ---------------------------------------------------------------------------
// POST /api/pipelines/:pipelineId/runs
// ---------------------------------------------------------------------------

describe('POST /pipelines/:pipelineId/runs (trigger)', () => {
  test('synthesizes runId + returns 202 when no bridge wired', async () => {
    // Wave 2: stub-branch logging moved from `console.log` to the pino
    // structured logger, so we no longer spy on console here. The 202 +
    // synthesized runId shape is the durable contract this test guards.
    const app = buildApp();
    const res = await run(app, 'POST', '/pipelines/p1/runs', { body: {} });
    expect(res.statusCode).toBe(202);
    const body = res.body as {
      runId: string;
      pipelineId: string;
      triggeredBy: { userId: string; triggerType: string };
      at: string;
    };
    expect(body.pipelineId).toBe('p1');
    expect(body.triggeredBy).toEqual({ userId: 'user-1', triggerType: 'manual' });
    expect(typeof body.runId).toBe('string');
    expect(body.runId).toMatch(/^[0-9a-f-]{36}$/);
  });

  test('forwards to bridge.trigger when wired', async () => {
    const trigger = jest.fn(async () => ({ runId: 'bridge-run-1' }));
    const bridge: PipelineBridge = {
      trigger,
      getRun: async () => null,
      getHistory: async () => [],
      resolveApproval: async () => {},
    };
    setPipelineBridge(bridge);

    const app = buildApp();
    const res = await run(app, 'POST', '/pipelines/p1/runs', {
      body: { triggerPayload: { foo: 'bar' } },
    });
    expect(res.statusCode).toBe(202);
    expect((res.body as { runId: string }).runId).toBe('bridge-run-1');
    expect(trigger).toHaveBeenCalledWith({
      pipelineId: 'p1',
      definition: undefined,
      triggerPayload: { foo: 'bar', requestedBy: 'user-1' },
      triggeredBy: { userId: 'user-1' },
    });
  });

  // NFR #47: the payload becomes the run context, and the deck steps act AS
  // `requestedBy` — a caller cannot name someone else.
  test('pins requestedBy (and trigger.requestedBy) to the verified caller', async () => {
    const trigger = jest.fn(async () => ({ runId: 'bridge-run-2' }));
    setPipelineBridge({ trigger, getRun: async () => null, getHistory: async () => [], resolveApproval: async () => {} } as never);
    const app = buildApp();
    const res = await run(app, 'POST', '/pipelines/p1/runs', {
      userId: 'dev-frank',
      body: { triggerPayload: { documentId: 'connors-deck', requestedBy: 'dev-connor', trigger: { requestedBy: 'dev-connor', x: 1 } } },
    });
    expect(res.statusCode).toBe(202);
    expect(trigger).toHaveBeenCalledWith(expect.objectContaining({
      triggerPayload: { documentId: 'connors-deck', requestedBy: 'dev-frank', trigger: { requestedBy: 'dev-frank', x: 1 } },
      triggeredBy: { userId: 'dev-frank' },
    }));
  });

  test('rejects 400 when definition.id does not match :pipelineId', async () => {
    const app = buildApp();
    const res = await run(app, 'POST', '/pipelines/p1/runs', {
      body: { definition: { id: 'p2' } },
    });
    expect(res.statusCode).toBe(400);
    expect((res.body as { error: string }).error).toMatch(/does not match/);
  });

  test('Idempotency-Key replay returns same runId without re-invoking bridge', async () => {
    const trigger = jest.fn(async () => ({ runId: 'idem-run-1' }));
    setPipelineBridge({
      trigger,
      getRun: async () => null,
      getHistory: async () => [],
      resolveApproval: async () => {},
    });

    const app = buildApp();
    const headers = { 'Idempotency-Key': 'idem-1' };
    const r1 = await run(app, 'POST', '/pipelines/p1/runs', { body: { x: 1 }, headers });
    expect(r1.statusCode).toBe(202);
    expect((r1.body as { runId: string }).runId).toBe('idem-run-1');
    // Allow fire-and-forget persist.
    await new Promise((r) => setImmediate(r));

    const r2 = await run(app, 'POST', '/pipelines/p1/runs', { body: { x: 1 }, headers });
    expect(r2.statusCode).toBe(202);
    expect((r2.body as { runId: string }).runId).toBe('idem-run-1');
    expect(r2.headers['X-Idempotent-Replay']).toBe('true');
    expect(trigger).toHaveBeenCalledTimes(1);
  });
});

// ---------------------------------------------------------------------------
// GET /api/pipelines/:pipelineId/runs/:runId
// ---------------------------------------------------------------------------

describe('GET /pipelines/:pipelineId/runs/:runId (snapshot)', () => {
  test('404 when no bridge wired', async () => {
    const app = buildApp();
    const res = await run(app, 'GET', '/pipelines/p1/runs/r1');
    expect(res.statusCode).toBe(404);
    expect((res.body as { error: string }).error).toBe('run not found');
  });

  test('404 when bridge.getRun returns null', async () => {
    setPipelineBridge({
      getRun: async () => null,
      getHistory: async () => [],
      resolveApproval: async () => {},
    });
    const app = buildApp();
    const res = await run(app, 'GET', '/pipelines/p1/runs/missing');
    expect(res.statusCode).toBe(404);
  });

  // The bridge only knows runs the CURRENT process executed (in-memory
  // PipelineModule). Before the repository fallback, every run predating the
  // running pod 404'd from detail while the list endpoint — which has always
  // read the repository — returned it complete with steps. Reproduced live:
  // a run created by the running pod resolved; one from before the last
  // restart did not.
  test('falls back to the repository for a run this process never executed', async () => {
    mockRunRepoGet.mockResolvedValueOnce({
      runId: 'old-run',
      pipelineId: 'p1',
      status: 'succeeded', // legacy spelling — must be folded on the way out
      startedAt: '2026-08-24T00:00:00.000Z',
      completedAt: '2026-08-24T00:00:01.000Z',
      stepsSummary: {},
      triggeredBy: 'user-1',
      triggerType: 'manual',
    });
    setPipelineBridge({
      getRun: async () => null,
      getHistory: async () => [],
      resolveApproval: async () => {},
    });

    const app = buildApp();
    const res = await run(app, 'GET', '/pipelines/p1/runs/old-run');

    expect(res.statusCode).toBe(200);
    const body = res.body as { runId: string; status: string };
    expect(body.runId).toBe('old-run');
    // Same mapping as the list endpoint, so the two agree on vocabulary.
    expect(body.status).toBe('completed');
  });

  test('still 404s when neither the bridge nor the repository has the run', async () => {
    mockRunRepoGet.mockResolvedValueOnce(null);
    setPipelineBridge({
      getRun: async () => null,
      getHistory: async () => [],
      resolveApproval: async () => {},
    });
    const app = buildApp();
    const res = await run(app, 'GET', '/pipelines/p1/runs/nope');
    expect(res.statusCode).toBe(404);
  });

  test('200 with snapshot when bridge returns one', async () => {
    setPipelineBridge({
      getRun: async (runId) => ({ runId, status: 'running', steps: {} }),
      getHistory: async () => [],
      resolveApproval: async () => {},
    });
    const app = buildApp();
    const res = await run(app, 'GET', '/pipelines/p1/runs/r1');
    expect(res.statusCode).toBe(200);
    expect(res.body).toMatchObject({ runId: 'r1', status: 'running' });
  });

  // Reproduced live on document-agent-edit and recording-finalize: every step
  // that was polled while running stayed `running` with no output on this
  // endpoint after the run completed (the persisted row was right). The
  // attempts overlay was writing `steps[stepId] = {...}` straight into the
  // object the bridge returned — which aliased the executor's live steps map —
  // so the executor finished the step on a record nobody could see any more.
  test('the attempts overlay decorates the response without rewriting the step records the bridge returned', async () => {
    const liveRecord: Record<string, unknown> = { nodeId: 'plan', status: 'running', startedAt: '2026-09-10T00:00:00Z' };
    const live = { runId: 'r-live', pipelineId: 'p1', status: 'running', steps: { plan: liveRecord } };
    mockStepAttempts.mockReturnValueOnce({
      plan: [{ attemptNumber: 1, startedAt: '2026-09-10T00:00:00Z', idempotencyKey: 'r-live:plan:1' }],
    });
    setPipelineBridge({
      getRun: async () => live as unknown as PipelineRunSnapshot,
      getHistory: async () => [],
      resolveApproval: async () => {},
    });
    const app = buildApp();
    const res = await run(app, 'GET', '/pipelines/p1/runs/r-live');
    expect(res.statusCode).toBe(200);
    const body = res.body as { steps: Record<string, { status: string; attempts?: unknown[] }> };
    expect(body.steps.plan.status).toBe('running');
    expect(body.steps.plan.attempts).toHaveLength(1);
    expect(mockStepAttempts).toHaveBeenCalledWith('r-live');

    // The bridge's object — the executor's, in production — keeps its own record.
    expect(live.steps.plan).toBe(liveRecord);
    expect('attempts' in liveRecord).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// GET /api/pipelines/:pipelineId/runs/:runId/history
// ---------------------------------------------------------------------------

describe('GET /pipelines/:pipelineId/runs/:runId/history (replay)', () => {
  test('returns [] when no bridge wired (per WAL-disabled handoff contract)', async () => {
    const app = buildApp();
    const res = await run(app, 'GET', '/pipelines/p1/runs/r1/history');
    expect(res.statusCode).toBe(200);
    expect(res.body).toEqual([]);
  });

  test('returns [] when bridge.getHistory returns []', async () => {
    setPipelineBridge({
      getRun: async () => null,
      getHistory: async () => [],
      resolveApproval: async () => {},
    });
    const app = buildApp();
    const res = await run(app, 'GET', '/pipelines/p1/runs/r1/history?fromVersion=5');
    expect(res.statusCode).toBe(200);
    expect(res.body).toEqual([]);
  });

  test('passes fromVersion to bridge and returns events', async () => {
    const getHistory = jest.fn(async () => [{ type: 'pipeline.step.started', version: 7 }]);
    setPipelineBridge({
      getRun: async () => null,
      getHistory,
      resolveApproval: async () => {},
    });
    const app = buildApp();
    const res = await run(app, 'GET', '/pipelines/p1/runs/r1/history?fromVersion=5');
    expect(res.statusCode).toBe(200);
    expect(res.body).toEqual([{ type: 'pipeline.step.started', version: 7 }]);
    expect(getHistory).toHaveBeenCalledWith('r1', 5);
  });

  test('400 on negative fromVersion', async () => {
    const app = buildApp();
    const res = await run(app, 'GET', '/pipelines/p1/runs/r1/history?fromVersion=-1');
    expect(res.statusCode).toBe(400);
  });
});

// ---------------------------------------------------------------------------
// POST /api/pipelines/:runId/approvals
// ---------------------------------------------------------------------------

describe('POST /pipelines/:runId/approvals (resolveApproval)', () => {
  test('204 stub when no bridge wired', async () => {
    // Wave 2: stub-branch logging moved from `console.log` to the pino
    // structured logger; the 204 contract is what this test guards.
    const app = buildApp();
    const res = await run(app, 'POST', '/pipelines/run-1/approvals', {
      body: { stepId: 'approval-1', decision: 'approve', comment: 'lgtm' },
    });
    expect(res.statusCode).toBe(204);
    expect(res.body).toBeUndefined();
  });

  test('forwards to bridge.resolveApproval when wired', async () => {
    const resolveApproval = jest.fn(async () => {});
    setPipelineBridge({
      getRun: async () => null,
      getHistory: async () => [],
      resolveApproval,
    });
    const app = buildApp();
    const res = await run(app, 'POST', '/pipelines/run-1/approvals', {
      body: { stepId: 'approval-1', decision: 'reject', comment: 'no' },
      userId: 'alice',
    });
    expect(res.statusCode).toBe(204);
    expect(resolveApproval).toHaveBeenCalledWith('run-1', 'approval-1', 'alice', 'reject', 'no');
  });

  test('400 when stepId missing', async () => {
    const app = buildApp();
    const res = await run(app, 'POST', '/pipelines/run-1/approvals', {
      body: { decision: 'approve' },
    });
    expect(res.statusCode).toBe(400);
  });

  test('400 when decision is not approve/reject', async () => {
    const app = buildApp();
    const res = await run(app, 'POST', '/pipelines/run-1/approvals', {
      body: { stepId: 's1', decision: 'maybe' },
    });
    expect(res.statusCode).toBe(400);
  });

  test('500 with structured body when bridge.resolveApproval throws (Wave 2 audit-fix)', async () => {
    // Regression: prior behaviour let a bridge throw bubble up to the
    // generic errorHandler, returning `{ error: "Internal server error" }`
    // with no action context. Wave 2 wraps the bridge call so the response
    // surfaces the bridge error message AND the action that failed, plus
    // emits a Prometheus error counter and an audit row (fire-and-forget).
    const resolveApproval = jest.fn(async () => {
      throw new Error('downstream bus unavailable');
    });
    setPipelineBridge({
      getRun: async () => null,
      getHistory: async () => [],
      resolveApproval,
    });
    const app = buildApp();
    const res = await run(app, 'POST', '/pipelines/run-1/approvals', {
      body: { stepId: 's1', decision: 'approve' },
    });
    expect(res.statusCode).toBe(500);
    expect(res.body).toEqual({
      error: 'downstream bus unavailable',
      action: 'resolveApproval',
    });
  });
});

// ---------------------------------------------------------------------------
// POST /api/pipelines/:runId/approvals — NFR #115: a gate held elsewhere
// ---------------------------------------------------------------------------

describe('POST /pipelines/:runId/approvals — gate held by another replica (NFR #115)', () => {
  const forward = jest.fn(async (d: { decisionId: string }) => ({ ...d, v: 1, fromNodeId: 'node-b' }));
  const holdsGate = jest.fn((_runId: string, _stepId: string) => false);
  const resolveApproval = jest.fn(async () => {});

  beforeEach(() => {
    forward.mockClear();
    holdsGate.mockReset();
    holdsGate.mockReturnValue(false);
    resolveApproval.mockClear();
    mockRunRepoGet.mockReset();
    mockRollupGet.mockReset();
    mockRollupGet.mockResolvedValue(null);
    mockResolvePipelineId.mockReset();
    mockResolvePipelineId.mockResolvedValue('deck-revise');
    setApprovalForwarder({ forward, holdsGate } as unknown as ApprovalForwarder);
    setPipelineBridge({ getRun: async () => null, getHistory: async () => [], resolveApproval });
  });
  afterEach(() => setApprovalForwarder(null));

  const liveRow = { pipelineId: 'deck-revise', runId: 'run-1', status: 'running', ownerNodeId: 'node-a', triggeredBy: 'alice', triggerType: 'manual', startedAt: '2026-09-23T16:00:00.000Z' };

  test('202 with the decision id: the decision is forwarded, the local bridge is never asked', async () => {
    mockRunRepoGet.mockResolvedValue(liveRow);
    mockRollupGet.mockResolvedValue({ pipelineId: 'deck-revise', recent: [{ runId: 'run-1' }], approvals: [{ runId: 'run-1', stepId: 'gate', at: '2026-09-23T16:00:01.000Z' }] });
    const res = await run(buildApp(), 'POST', '/pipelines/run-1/approvals', {
      body: { stepId: 'gate', decision: 'approve', comment: 'ship' },
      userId: 'alice',
      headers: { 'Idempotency-Key': 'key-1' },
    });
    expect(res.statusCode).toBe(202);
    expect(res.body).toMatchObject({ decisionId: 'key-1', runId: 'run-1', stepId: 'gate', decision: 'approve', pipelineId: 'deck-revise', ownerNodeId: 'node-a', forwarded: true });
    expect(forward).toHaveBeenCalledWith(expect.objectContaining({ decisionId: 'key-1', pipelineId: 'deck-revise', runId: 'run-1', stepId: 'gate', userId: 'alice', decision: 'approve', comment: 'ship' }));
    expect(resolveApproval).not.toHaveBeenCalled();
  });

  test('202 mints a decision id when the request carries no Idempotency-Key', async () => {
    mockRunRepoGet.mockResolvedValue(liveRow);
    const res = await run(buildApp(), 'POST', '/pipelines/run-1/approvals', { body: { stepId: 'gate', decision: 'reject' } });
    expect(res.statusCode).toBe(202);
    expect(typeof (res.body as { decisionId: string }).decisionId).toBe('string');
    expect((res.body as { decisionId: string }).decisionId.length).toBeGreaterThan(20);
  });

  test('204 and the local bridge when this replica holds the gate', async () => {
    holdsGate.mockReturnValue(true);
    const res = await run(buildApp(), 'POST', '/pipelines/run-1/approvals', { body: { stepId: 'gate', decision: 'approve' }, userId: 'alice' });
    expect(res.statusCode).toBe(204);
    expect(resolveApproval).toHaveBeenCalledWith('run-1', 'gate', 'alice', 'approve', undefined);
    expect(forward).not.toHaveBeenCalled();
  });

  test('404 run-not-found when no replica and no run row knows the run — never a silent 204', async () => {
    mockResolvePipelineId.mockResolvedValue(null);
    const res = await run(buildApp(), 'POST', '/pipelines/run-x/approvals', { body: { stepId: 'gate', decision: 'approve' } });
    expect(res.statusCode).toBe(404);
    expect(res.body).toMatchObject({ error: 'run-not-found' });
    expect(forward).not.toHaveBeenCalled();
    expect(resolveApproval).not.toHaveBeenCalled();
  });

  test('409 run-not-awaiting-approval when the run row is terminal', async () => {
    mockRunRepoGet.mockResolvedValue({ ...liveRow, status: 'completed' });
    const res = await run(buildApp(), 'POST', '/pipelines/run-1/approvals', { body: { stepId: 'gate', decision: 'approve' } });
    expect(res.statusCode).toBe(409);
    expect(res.body).toMatchObject({ error: 'run-not-awaiting-approval', status: 'completed' });
    expect(forward).not.toHaveBeenCalled();
  });

  test('409 gate-not-pending when the rollup row knows the run but holds no such gate', async () => {
    mockRunRepoGet.mockResolvedValue(liveRow);
    mockRollupGet.mockResolvedValue({ pipelineId: 'deck-revise', recent: [{ runId: 'run-1' }], approvals: [] });
    const res = await run(buildApp(), 'POST', '/pipelines/run-1/approvals', { body: { stepId: 'other-step', decision: 'approve' } });
    expect(res.statusCode).toBe(409);
    expect(res.body).toMatchObject({ error: 'gate-not-pending' });
    expect(forward).not.toHaveBeenCalled();
  });

  test('202 for an interrupted run (owner dead): the decision waits on the row for the resume', async () => {
    mockRunRepoGet.mockResolvedValue({ ...liveRow, status: 'interrupted' });
    mockRollupGet.mockResolvedValue({ pipelineId: 'deck-revise', recent: [{ runId: 'run-1' }], approvals: [] });
    const res = await run(buildApp(), 'POST', '/pipelines/run-1/approvals', { body: { stepId: 'gate', decision: 'approve' } });
    expect(res.statusCode).toBe(202);
    expect(forward).toHaveBeenCalledTimes(1);
  });

  test('503 approval-forwarding-unavailable when neither the row nor the wire took the decision', async () => {
    mockRunRepoGet.mockResolvedValue(liveRow);
    forward.mockRejectedValueOnce(new Error('redis down'));
    const res = await run(buildApp(), 'POST', '/pipelines/run-1/approvals', { body: { stepId: 'gate', decision: 'approve' }, headers: { 'Idempotency-Key': 'key-2' } });
    expect(res.statusCode).toBe(503);
    expect(res.body).toMatchObject({ error: 'approval-forwarding-unavailable', decisionId: 'key-2' });
  });
});

// ---------------------------------------------------------------------------
// POST /api/pipelines/:runId/approvals — AUDIT-03 authorization check
// ---------------------------------------------------------------------------

describe('POST /pipelines/:runId/approvals — AUDIT-03 approver authorization', () => {
  afterEach(() => setPipelineBridge(null));

  function makeApproverBridge(opts: {
    approvers: Array<{ type: 'user' | 'role'; value: string; userId?: string }>;
    resolveApprovalFn?: () => Promise<void>;
  }) {
    return {
      getRun: async () => null,
      getHistory: async () => [],
      resolveApproval: opts.resolveApprovalFn ?? jest.fn(async () => {}),
      getPendingApprovals: async () => [
        {
          runId: 'run-1',
          stepId: 'approval-1',
          pipelineId: 'pipe-1',
          approvers: opts.approvers,
          requestedAt: new Date().toISOString(),
        },
      ],
    };
  }

  test('204 when user is listed as a user-type approver (value match)', async () => {
    setPipelineBridge(makeApproverBridge({ approvers: [{ type: 'user', value: 'alice' }] }));
    const res = await run(buildApp(), 'POST', '/pipelines/run-1/approvals', {
      body: { stepId: 'approval-1', decision: 'approve' },
      userId: 'alice',
    });
    expect(res.statusCode).toBe(204);
  });

  test('204 when user matches via userId convenience alias', async () => {
    setPipelineBridge(makeApproverBridge({
      approvers: [{ type: 'user', value: 'alice', userId: 'alice' }],
    }));
    const res = await run(buildApp(), 'POST', '/pipelines/run-1/approvals', {
      body: { stepId: 'approval-1', decision: 'reject' },
      userId: 'alice',
    });
    expect(res.statusCode).toBe(204);
  });

  test('403 when user is not in user-type approvers list', async () => {
    setPipelineBridge(makeApproverBridge({
      approvers: [{ type: 'user', value: 'alice' }, { type: 'user', value: 'bob' }],
    }));
    const res = await run(buildApp(), 'POST', '/pipelines/run-1/approvals', {
      body: { stepId: 'approval-1', decision: 'approve' },
      userId: 'eve',
    });
    expect(res.statusCode).toBe(403);
    const body = res.body as { error: string };
    expect(body.error).toBe('not authorized to resolve this approval step');
  });

  test('204 when all approvers are role-type (gateway has no role resolver — pass through)', async () => {
    setPipelineBridge(makeApproverBridge({
      approvers: [{ type: 'role', value: 'admin' }, { type: 'role', value: 'manager' }],
    }));
    const res = await run(buildApp(), 'POST', '/pipelines/run-1/approvals', {
      body: { stepId: 'approval-1', decision: 'approve' },
      userId: 'anyone',
    });
    expect(res.statusCode).toBe(204);
  });

  test('204 when pending row not found (wrong stepId) — bridge enforces', async () => {
    setPipelineBridge(makeApproverBridge({ approvers: [{ type: 'user', value: 'alice' }] }));
    const res = await run(buildApp(), 'POST', '/pipelines/run-1/approvals', {
      body: { stepId: 'other-step', decision: 'approve' },
      userId: 'eve',
    });
    // No matching (runId, stepId) → gateway cannot decide → let bridge handle it
    expect(res.statusCode).toBe(204);
  });

  test('204 when getPendingApprovals throws — logs warn and continues', async () => {
    const resolveApproval = jest.fn(async () => {});
    setPipelineBridge({
      getRun: async () => null,
      getHistory: async () => [],
      resolveApproval,
      getPendingApprovals: async () => { throw new Error('authz lookup boom'); },
    });
    const res = await run(buildApp(), 'POST', '/pipelines/run-1/approvals', {
      body: { stepId: 'approval-1', decision: 'approve' },
      userId: 'eve',
    });
    expect(res.statusCode).toBe(204);
    expect(resolveApproval).toHaveBeenCalled();
  });

  test('204 when bridge has no getPendingApprovals (older bridge — pass through)', async () => {
    const resolveApproval = jest.fn(async () => {});
    setPipelineBridge({
      getRun: async () => null,
      getHistory: async () => [],
      resolveApproval,
      // no getPendingApprovals
    });
    const res = await run(buildApp(), 'POST', '/pipelines/run-1/approvals', {
      body: { stepId: 'approval-1', decision: 'approve' },
      userId: 'eve',
    });
    expect(res.statusCode).toBe(204);
    expect(resolveApproval).toHaveBeenCalled();
  });
});

// ---------------------------------------------------------------------------
// GET /api/pipelines/:pipelineId/runs (history listing)
// ---------------------------------------------------------------------------

describe('GET /pipelines/:pipelineId/runs (history)', () => {
  test('returns runs from the in-memory stub store, newest first', async () => {
    const logSpy = jest.spyOn(console, 'log').mockImplementation(() => undefined);
    try {
      const app = buildApp();
      // Trigger 3 runs through the stub branch — they all land in stubRunStore.
      for (let i = 0; i < 3; i++) {
        const r = await run(app, 'POST', '/pipelines/p1/runs', { body: {} });
        expect(r.statusCode).toBe(202);
      }
      const res = await run(app, 'GET', '/pipelines/p1/runs');
      expect(res.statusCode).toBe(200);
      const body = res.body as { runs: Array<{ pipelineId: string; status: string }> };
      expect(body.runs).toHaveLength(3);
      expect(body.runs[0]!.pipelineId).toBe('p1');
      expect(body.runs[0]!.status).toBe('pending');
    } finally {
      logSpy.mockRestore();
    }
  });

  test('paginates via limit + cursor', async () => {
    const logSpy = jest.spyOn(console, 'log').mockImplementation(() => undefined);
    try {
      const app = buildApp();
      for (let i = 0; i < 5; i++) {
        await run(app, 'POST', '/pipelines/p1/runs', { body: {} });
        // Tiny delay so triggeredAt timestamps differ.
        await new Promise((r) => setTimeout(r, 2));
      }
      const r1 = await run(app, 'GET', '/pipelines/p1/runs?limit=2');
      const b1 = r1.body as { runs: Array<{ runId: string }>; nextCursor?: string };
      expect(b1.runs).toHaveLength(2);
      expect(b1.nextCursor).toBeDefined();

      const r2 = await run(
        app,
        'GET',
        `/pipelines/p1/runs?limit=2&cursor=${encodeURIComponent(b1.nextCursor!)}`,
      );
      const b2 = r2.body as { runs: Array<{ runId: string }>; nextCursor?: string };
      expect(b2.runs).toHaveLength(2);
      // No overlap between pages.
      expect(b2.runs.map((r) => r.runId)).not.toEqual(
        expect.arrayContaining(b1.runs.map((r) => r.runId)),
      );
    } finally {
      logSpy.mockRestore();
    }
  });

  test('400 when limit is out of range', async () => {
    const app = buildApp();
    const res = await run(app, 'GET', '/pipelines/p1/runs?limit=0');
    expect(res.statusCode).toBe(400);
  });

  test('forwards to bridge.listRuns when wired', async () => {
    const listRuns = jest.fn(async () => ({
      runs: [{ runId: 'br-1', status: 'running' }],
      nextCursor: undefined,
    }));
    setPipelineBridge({
      getRun: async () => null,
      getHistory: async () => [],
      resolveApproval: async () => {},
      listRuns,
    });
    const app = buildApp();
    const res = await run(app, 'GET', '/pipelines/p1/runs?limit=5');
    expect(res.statusCode).toBe(200);
    expect(listRuns).toHaveBeenCalledWith('p1', { limit: 5, cursor: undefined });
    expect(res.body).toMatchObject({ runs: [{ runId: 'br-1' }] });
  });

  test("filters by user — user-2 doesn't see user-1's runs", async () => {
    const logSpy = jest.spyOn(console, 'log').mockImplementation(() => undefined);
    try {
      const app = buildApp();
      // Trigger as user-1 (default).
      await run(app, 'POST', '/pipelines/p1/runs', { body: {} });
      // List as user-2 — must be empty.
      const res = await run(app, 'GET', '/pipelines/p1/runs', { userId: 'user-2' });
      expect(res.statusCode).toBe(200);
      expect((res.body as { runs: unknown[] }).runs).toEqual([]);
    } finally {
      logSpy.mockRestore();
    }
  });
});

// ---------------------------------------------------------------------------
// GET /api/pipelines/runs/active
// ---------------------------------------------------------------------------

describe('GET /pipelines/runs/active', () => {
  test('lists pending/running runs from the stub store', async () => {
    const logSpy = jest.spyOn(console, 'log').mockImplementation(() => undefined);
    try {
      const app = buildApp();
      await run(app, 'POST', '/pipelines/p1/runs', { body: {} });
      await run(app, 'POST', '/pipelines/p2/runs', { body: {} });

      const res = await run(app, 'GET', '/pipelines/runs/active');
      expect(res.statusCode).toBe(200);
      const body = res.body as { runs: Array<{ pipelineId: string; status: string }> };
      expect(body.runs).toHaveLength(2);
      expect(body.runs.every((r) => r.status === 'pending')).toBe(true);
    } finally {
      logSpy.mockRestore();
    }
  });

  test("returns [] when caller has no active runs", async () => {
    const app = buildApp();
    const res = await run(app, 'GET', '/pipelines/runs/active');
    expect(res.statusCode).toBe(200);
    expect((res.body as { runs: unknown[] }).runs).toEqual([]);
  });

  test('forwards to bridge.listActiveRuns when wired', async () => {
    const listActiveRuns = jest.fn(async () => [
      { runId: 'br-active-1', status: 'running' },
    ]);
    setPipelineBridge({
      getRun: async () => null,
      getHistory: async () => [],
      resolveApproval: async () => {},
      listActiveRuns,
    });
    const app = buildApp();
    const res = await run(app, 'GET', '/pipelines/runs/active');
    expect(res.statusCode).toBe(200);
    expect(listActiveRuns).toHaveBeenCalled();
    expect((res.body as { runs: Array<{ runId: string }> }).runs[0]!.runId).toBe('br-active-1');
  });
});

// ---------------------------------------------------------------------------
// POST /api/pipelines/:runId/cancel
// ---------------------------------------------------------------------------

describe('POST /pipelines/:runId/cancel', () => {
  test('cancels an active stub run and returns the updated record', async () => {
    const logSpy = jest.spyOn(console, 'log').mockImplementation(() => undefined);
    try {
      const app = buildApp();
      const trig = await run(app, 'POST', '/pipelines/p1/runs', { body: {} });
      const runId = (trig.body as { runId: string }).runId;

      const res = await run(app, 'POST', `/pipelines/${runId}/cancel`, {
        body: { reason: 'no longer needed' },
      });
      expect(res.statusCode).toBe(200);
      expect(res.body).toMatchObject({
        runId,
        status: 'cancelled',
        reason: 'no longer needed',
      });
    } finally {
      logSpy.mockRestore();
    }
  });

  test('404 when runId is unknown', async () => {
    const app = buildApp();
    const res = await run(app, 'POST', '/pipelines/missing-run/cancel', { body: {} });
    expect(res.statusCode).toBe(404);
    expect((res.body as { error: string }).error).toMatch(/run not found/);
  });

  test('400 when run is already in terminal state', async () => {
    const logSpy = jest.spyOn(console, 'log').mockImplementation(() => undefined);
    try {
      const app = buildApp();
      const trig = await run(app, 'POST', '/pipelines/p1/runs', { body: {} });
      const runId = (trig.body as { runId: string }).runId;
      // First cancel succeeds.
      await run(app, 'POST', `/pipelines/${runId}/cancel`, { body: {} });
      // Second cancel hits the terminal-state branch.
      const res = await run(app, 'POST', `/pipelines/${runId}/cancel`, { body: {} });
      expect(res.statusCode).toBe(400);
      expect((res.body as { error: string }).error).toMatch(/terminal state/);
    } finally {
      logSpy.mockRestore();
    }
  });

  test('404 when a different user tries to cancel a run they do not own', async () => {
    // Ownership is enforced in the stub path: stubRunStore.cancel() returns null
    // when userId !== run.userId, and the route then re-checks and throws 404.
    // This pins the behavior so a user cannot cancel another user's run.
    const logSpy = jest.spyOn(console, 'log').mockImplementation(() => undefined);
    try {
      const app = buildApp();
      // user-1 (the default) triggers a run
      const trig = await run(app, 'POST', '/pipelines/p1/runs', { body: {} });
      const runId = (trig.body as { runId: string }).runId;
      // user-2 tries to cancel it — must get 404 (not 403, to avoid leaking run existence)
      const res = await run(app, 'POST', `/pipelines/${runId}/cancel`, {
        body: {},
        userId: 'user-2',
      });
      expect(res.statusCode).toBe(404);
      expect((res.body as { error: string }).error).toMatch(/run not found/);
    } finally {
      logSpy.mockRestore();
    }
  });

  test('forwards to bridge.cancelRun when wired', async () => {
    const cancelRun = jest.fn(async () => {});
    const getRun = jest.fn(async (runId: string) => ({ runId, status: 'cancelled' }));
    setPipelineBridge({
      getRun,
      getHistory: async () => [],
      resolveApproval: async () => {},
      cancelRun,
    });
    const app = buildApp();
    const res = await run(app, 'POST', '/pipelines/run-xyz/cancel', { body: {} });
    expect(res.statusCode).toBe(200);
    expect(cancelRun).toHaveBeenCalledWith('run-xyz', 'user-1');
    expect(res.body).toMatchObject({ runId: 'run-xyz', status: 'cancelled' });
  });
});

describe('Error classification', () => {
  beforeEach(() => {
    // Clear any wired bridge before each test so they start clean
    setPipelineBridge(null);
  });

  test('classifyPipelineError helper returns ValidationError for validation messages', () => {
    // Import and test the classification function directly
    const { classifyPipelineError } = require('../pipelineTriggers');
    expect(classifyPipelineError('Validation failed: required field missing')).toBe('ValidationError');
    expect(classifyPipelineError('Invalid type: expected string')).toBe('ValidationError');
  });

  test('classifyPipelineError helper returns NetworkError for network failures', () => {
    const { classifyPipelineError } = require('../pipelineTriggers');
    expect(classifyPipelineError('fetch failed', 'Error: ECONNREFUSED\n  at ...')).toBe('NetworkError');
    expect(classifyPipelineError('socket hang up')).toBe('NetworkError');
  });

  test('classifyPipelineError helper returns TimeoutError for timeout messages', () => {
    const { classifyPipelineError } = require('../pipelineTriggers');
    expect(classifyPipelineError('Operation timed out')).toBe('TimeoutError');
    expect(classifyPipelineError('Request timeout')).toBe('TimeoutError');
  });

  test('classifyPipelineError helper returns LLMError for LLM-specific errors', () => {
    const { classifyPipelineError } = require('../pipelineTriggers');
    expect(classifyPipelineError('Anthropic API rate limit exceeded')).toBe('LLMError');
    expect(classifyPipelineError('rate_limit_error: quota exceeded')).toBe('LLMError');
    expect(classifyPipelineError('bedrock model error')).toBe('LLMError');
  });

  test('classifyPipelineError helper returns RegistryConflict for registry errors', () => {
    const { classifyPipelineError } = require('../pipelineTriggers');
    expect(classifyPipelineError('Resource lock timeout')).toBe('RegistryConflict');
    expect(classifyPipelineError('Version conflict detected')).toBe('RegistryConflict');
  });

  test('classifyPipelineError helper returns UnknownError for unclassifiable messages', () => {
    const { classifyPipelineError } = require('../pipelineTriggers');
    expect(classifyPipelineError('Something unexpected went wrong')).toBe('UnknownError');
  });
});

// ---------------------------------------------------------------------------
// POST /api/pipelines/:pipelineId/runs/:runId/breakpoint/:stepId/resolve
// ---------------------------------------------------------------------------

describe('POST /pipelines/:pipelineId/runs/:runId/breakpoint/:stepId/resolve', () => {
  test('503 when bridge is not wired', async () => {
    const app = buildApp();
    const res = await run(
      app,
      'POST',
      '/pipelines/p1/runs/run-1/breakpoint/step-1/resolve',
      { body: { action: 'continue' } },
    );
    expect(res.statusCode).toBe(503);
  });

  test('503 when bridge has no resolveBreakpoint method', async () => {
    setPipelineBridge({
      getRun: async () => null,
      getHistory: async () => [],
      resolveApproval: async () => {},
      // resolveBreakpoint intentionally absent
    });
    const app = buildApp();
    const res = await run(
      app,
      'POST',
      '/pipelines/p1/runs/run-1/breakpoint/step-1/resolve',
      { body: {} },
    );
    expect(res.statusCode).toBe(503);
  });

  test('204 on success — continue action', async () => {
    const resolveBreakpoint = jest.fn();
    setPipelineBridge({
      getRun: async () => null,
      getHistory: async () => [],
      resolveApproval: async () => {},
      resolveBreakpoint,
    });
    const app = buildApp();
    const res = await run(
      app,
      'POST',
      '/pipelines/p1/runs/run-1/breakpoint/step-2/resolve',
      { body: { action: 'continue' } },
    );
    expect(res.statusCode).toBe(204);
    expect(resolveBreakpoint).toHaveBeenCalledWith('run-1', 'step-2', 'continue');
  });

  test('204 on success — resume_all action', async () => {
    const resolveBreakpoint = jest.fn();
    setPipelineBridge({
      getRun: async () => null,
      getHistory: async () => [],
      resolveApproval: async () => {},
      resolveBreakpoint,
    });
    const app = buildApp();
    const res = await run(
      app,
      'POST',
      '/pipelines/p1/runs/run-1/breakpoint/step-3/resolve',
      { body: { action: 'resume_all' } },
    );
    expect(res.statusCode).toBe(204);
    expect(resolveBreakpoint).toHaveBeenCalledWith('run-1', 'step-3', 'resume_all');
  });

  test('defaults action to "continue" when body is empty', async () => {
    const resolveBreakpoint = jest.fn();
    setPipelineBridge({
      getRun: async () => null,
      getHistory: async () => [],
      resolveApproval: async () => {},
      resolveBreakpoint,
    });
    const app = buildApp();
    const res = await run(
      app,
      'POST',
      '/pipelines/p1/runs/run-1/breakpoint/step-1/resolve',
      { body: {} },
    );
    expect(res.statusCode).toBe(204);
    expect(resolveBreakpoint).toHaveBeenCalledWith('run-1', 'step-1', 'continue');
  });

  test('400 when action is invalid', async () => {
    const resolveBreakpoint = jest.fn();
    setPipelineBridge({
      getRun: async () => null,
      getHistory: async () => [],
      resolveApproval: async () => {},
      resolveBreakpoint,
    });
    const app = buildApp();
    const res = await run(
      app,
      'POST',
      '/pipelines/p1/runs/run-1/breakpoint/step-1/resolve',
      { body: { action: 'invalid-action' } },
    );
    expect(res.statusCode).toBe(400);
    expect(resolveBreakpoint).not.toHaveBeenCalled();
  });
});

// ---------------------------------------------------------------------------
// NFR #120 — cancel / breakpoint resolve on the replica that does not hold the run
// ---------------------------------------------------------------------------
describe('run controls on the replica that does not hold the run (NFR #120)', () => {
  const forward = jest.fn(async (c: { controlId: string; kind: string }) => ({ message: { ...c, v: 1, type: 'control', fromReplica: 'node-b' }, applied: { v: 1, type: 'applied', controlId: c.controlId, runId: 'run-1', kind: c.kind, nodeId: 'node-a', ok: true, at: 'now' }, appliedInMs: 12, recorded: true }));
  const canApply = jest.fn((_c: { kind: string; runId: string }) => false);
  const release = jest.fn(async () => {});
  const cancelRun = jest.fn();
  const resolveBreakpoint = jest.fn();
  const liveRow = { pipelineId: 'deck-revise', runId: 'run-1', status: 'running', ownerNodeId: 'node-a', triggeredBy: 'alice', triggerType: 'manual', startedAt: '2026-09-23T17:00:00.000Z' };

  beforeEach(() => {
    forward.mockClear();
    canApply.mockReset();
    canApply.mockReturnValue(false);
    release.mockClear();
    cancelRun.mockClear();
    resolveBreakpoint.mockClear();
    mockRunRepoGet.mockReset();
    mockResolvePipelineId.mockReset();
    mockResolvePipelineId.mockResolvedValue('deck-revise');
    setRunControlForwarder({ forward, canApply, release } as unknown as RunControlForwarder);
    setPipelineBridge({ getRun: async () => null, getHistory: async () => [], cancelRun, resolveBreakpoint });
  });
  afterEach(() => setRunControlForwarder(null));

  test('cancel: 202 applied by the holder; the local bridge is never asked', async () => {
    mockRunRepoGet.mockResolvedValue(liveRow);
    const res = await run(buildApp(), 'POST', '/pipelines/run-1/cancel', { body: { reason: 'operator' }, userId: 'alice', headers: { 'Idempotency-Key': 'ctl-1' } });
    expect(res.statusCode).toBe(202);
    expect(res.body).toMatchObject({ controlId: 'ctl-1', kind: 'cancel', runId: 'run-1', pipelineId: 'deck-revise', ownerNodeId: 'node-a', forwarded: true, applied: true, appliedBy: 'node-a', appliedInMs: 12 });
    expect(forward).toHaveBeenCalledWith(expect.objectContaining({ kind: 'cancel', controlId: 'ctl-1', runId: 'run-1', userId: 'alice', reason: 'operator', pipelineId: 'deck-revise' }), expect.anything());
    expect(cancelRun).not.toHaveBeenCalled();
  });

  test('cancel: 202 recorded-not-applied when no replica acknowledges (owner dead: the resume path applies it)', async () => {
    mockRunRepoGet.mockResolvedValue({ ...liveRow, status: 'interrupted' });
    forward.mockResolvedValueOnce({ message: {}, applied: null, appliedInMs: null, recorded: true } as never);
    const res = await run(buildApp(), 'POST', '/pipelines/run-1/cancel', { body: {}, userId: 'alice' });
    expect(res.statusCode).toBe(202);
    expect(res.body).toMatchObject({ kind: 'cancel', forwarded: true, recorded: true, applied: false, appliedBy: null, status: 'interrupted' });
    expect(release).not.toHaveBeenCalled();
  });

  test('cancel: 404 when no replica and no row know the run; 409 when the run already ended', async () => {
    mockRunRepoGet.mockResolvedValue(null);
    const missing = await run(buildApp(), 'POST', '/pipelines/run-1/cancel', { body: {}, userId: 'alice' });
    expect(missing.statusCode).toBe(404);
    expect(missing.body).toMatchObject({ error: 'run-not-found', kind: 'cancel' });
    mockRunRepoGet.mockResolvedValue({ ...liveRow, status: 'completed' });
    const ended = await run(buildApp(), 'POST', '/pipelines/run-1/cancel', { body: {}, userId: 'alice' });
    expect(ended.statusCode).toBe(409);
    expect(ended.body).toMatchObject({ error: 'run-already-ended', status: 'completed' });
    expect(forward).not.toHaveBeenCalled();
  });

  test('cancel: 503 when neither the row nor the wire took it; 409 when the holder refused', async () => {
    mockRunRepoGet.mockResolvedValue(liveRow);
    forward.mockRejectedValueOnce(new Error('redis down'));
    const down = await run(buildApp(), 'POST', '/pipelines/run-1/cancel', { body: {}, userId: 'alice' });
    expect(down.statusCode).toBe(503);
    expect(down.body).toMatchObject({ error: 'run-control-forwarding-unavailable', kind: 'cancel' });
    forward.mockResolvedValueOnce({ message: {}, applied: { ok: false, nodeId: 'node-a', error: 'InvalidRunStateError' }, appliedInMs: 3, recorded: true } as never);
    const refused = await run(buildApp(), 'POST', '/pipelines/run-1/cancel', { body: {}, userId: 'alice' });
    expect(refused.statusCode).toBe(409);
    expect(refused.body).toMatchObject({ error: 'run-control-refused', ownerNodeId: 'node-a', message: 'InvalidRunStateError' });
  });

  test('cancel: the local path is untouched when this replica holds the run', async () => {
    canApply.mockReturnValue(true);
    setPipelineBridge({ getRun: async () => ({ runId: 'run-1', pipelineId: 'deck-revise', status: 'cancelled' }) as never, getHistory: async () => [], cancelRun, resolveBreakpoint });
    const res = await run(buildApp(), 'POST', '/pipelines/run-1/cancel', { body: {}, userId: 'alice' });
    expect(res.statusCode).toBe(200);
    expect(cancelRun).toHaveBeenCalledWith('run-1', 'alice');
    expect(forward).not.toHaveBeenCalled();
  });

  test('breakpoint resolve: 202 forwarded with the step and action; the local bridge is never asked', async () => {
    mockRunRepoGet.mockResolvedValue({ ...liveRow, status: 'paused_at_breakpoint' });
    const res = await run(buildApp(), 'POST', '/pipelines/deck-revise/runs/run-1/breakpoint/write/resolve', { body: { action: 'step_over' }, userId: 'alice' });
    expect(res.statusCode).toBe(202);
    expect(res.body).toMatchObject({ kind: 'breakpoint', runId: 'run-1', pipelineId: 'deck-revise', forwarded: true, applied: true });
    expect(forward).toHaveBeenCalledWith(expect.objectContaining({ kind: 'breakpoint', stepId: 'write', action: 'step_over', pipelineId: 'deck-revise' }), expect.anything());
    expect(resolveBreakpoint).not.toHaveBeenCalled();
  });
});
