// Tests for GET /api/pipelines/approvals (the pending-approvals queue).
//
// Exercises every branch of `pipelinePendingApprovalsRouter`:
//   - bridge null → { approvals: [] } with 200
//   - bridge wired but `getPendingApprovals` undefined → { approvals: [] } with 200
//   - bridge wired with data → forwards
//   - `?userId=` filters to rows where user appears in `approvers`
//   - mount-ordering: GET /pipelines/approvals must NOT fall through to the
//     `:runId/approvals` POST handler (that route only accepts POST, but we
//     also verify the path resolves to the static segment).
//
// Auth: mirrors the sibling pipelineTriggers.test.ts pattern — a stub
// middleware sets `req.user.sub`, and we exercise the unauth path by
// stripping it back out.

import express, { type NextFunction, type Request, type Response } from 'express';

// Stub redis-client so the idempotency middleware doesn't try to dial Redis
// in unit tests (saves ~30ms/test and keeps stderr clean).
jest.mock('../../lib/redis-client', () => ({
  getRedisClient: async () => null,
}));

// NFR #115: the list joins the rollup rows (the shared source) with the local
// queue. No rollup subscriber in the process → local queue only.
let rollupSubscriberInstalled = false;
jest.mock('../../pipeline/run-rollup-subscriber', () => ({
  getRunRollupSubscriber: () => (rollupSubscriberInstalled ? { resolvePipelineId: async () => null } : null),
  lookupPipelineIdByRunId: async () => null,
}));
const mockListWithOpenApprovals = jest.fn(async (): Promise<unknown[]> => []);
jest.mock('../../repositories/PipelineRunRollupRepository', () => ({
  pipelineRunRollupRepo: { get: async () => null, listWithOpenApprovals: () => mockListWithOpenApprovals() },
}));
const mockRunRepoGet = jest.fn(async (_pipelineId: string, _runId: string): Promise<unknown> => null);
jest.mock('../../repositories/PipelineRunRepository', () => ({
  PipelineRunRepository: jest.fn().mockImplementation(() => ({ get: mockRunRepoGet })),
  // `runItemToSnapshot` (the remote run's enrichment) reads attempts through it.
  readAttempts: (summary: { attempts?: unknown[] }) => summary.attempts ?? [],
}));
jest.mock('../../lib/ddb-client', () => ({ getDynamoClient: () => ({}) }));

import { errorHandler } from '../../middleware/error-handler';
import {
  pipelinePendingApprovalsRouter,
  pipelineApprovalsRouter,
  pipelineTriggersRouter,
  setPipelineBridge,
  stubRunStore,
  type PipelineBridge,
  type PendingApprovalRow,
} from '../pipelineTriggers';

// ---------------------------------------------------------------------------
// Test rig — same hand-rolled req/res scaffolding as pipelineTriggers.test.ts.
// (We can't share a helper module without growing the test surface; copying
// the ~50 LOC keeps the test self-contained.)
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

async function run(
  app: express.Express,
  method: string,
  url: string,
  opts: { body?: unknown; headers?: Record<string, string>; userId?: string | null } = {},
): Promise<MockRes> {
  const res = mockRes();
  const headers: Record<string, string> = {};
  for (const [k, v] of Object.entries(opts.headers ?? {})) {
    headers[k.toLowerCase()] = v;
  }
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
    // Setting userId === null on the test request suppresses the auth stub so
    // we can exercise the unauthenticated path.
    ...(opts.userId === null ? {} : { user: { sub: opts.userId ?? 'user-1' } }),
  } as unknown as Request;

  await new Promise<void>((resolve, reject) => {
    const finalHandler = (err?: unknown): void => {
      if (err) {
        reject(err);
        return;
      }
      if (!res.ended) {
        res.status(404).json({ error: 'route not found' });
      }
      resolve();
    };
    (app as unknown as (
      req: Request,
      res: Response,
      next: NextFunction,
    ) => void)(req, res as unknown as Response, finalHandler);
    res.finished.then(() => resolve()).catch(reject);
  });
  return res;
}

// Build an app that mirrors the production mount ordering — the static
// `/pipelines/approvals` segment must register before the param-bearing
// `:pipelineId` and `:runId/approvals` mounts so it isn't swallowed.
function buildApp(opts: { authStub?: boolean } = { authStub: true }): express.Express {
  const app = express();
  app.use(express.json());
  if (opts.authStub) {
    app.use((req, res, next) => {
      const r = req as Request & { user?: { sub: string } };
      if (!r.user) {
        // Mirror the prod 401 shape from middleware/auth so the test asserts
        // against a realistic envelope.
        res.status(401).json({ error: 'unauthorized' });
        return;
      }
      next();
    });
  }
  // Static segment FIRST.
  app.use('/pipelines/approvals', pipelinePendingApprovalsRouter);
  // Then param-bearing mounts. If the static one was missing or registered
  // after these, GET /pipelines/approvals would be captured by `:pipelineId`
  // (and 404) or `:runId/approvals` (and 404 since that router only handles
  // POST).
  app.use('/pipelines/:pipelineId/runs', pipelineTriggersRouter);
  app.use('/pipelines/:runId/approvals', pipelineApprovalsRouter);
  app.use(errorHandler);
  return app;
}

afterEach(() => {
  setPipelineBridge(null);
  stubRunStore.__resetForTests();
  rollupSubscriberInstalled = false;
  mockListWithOpenApprovals.mockReset();
  mockListWithOpenApprovals.mockResolvedValue([]);
  mockRunRepoGet.mockReset();
  mockRunRepoGet.mockResolvedValue(null);
});

// ---------------------------------------------------------------------------
// NFR #115: the shared source — gates on the rollup rows, whichever replica
// owns the run.
// ---------------------------------------------------------------------------

describe('GET /pipelines/approvals — joined with the rollup rows (NFR #115)', () => {
  const remoteRun = {
    pipelineId: 'deck-revise', runId: 'run-b', status: 'running', ownerNodeId: 'node-b',
    triggeredBy: 'bob', triggerType: 'manual', startedAt: '2026-09-23T16:00:00.000Z',
    stepsSummary: { review: { status: 'completed', output: { verdict: 'big edit' } } },
    definitionSnapshot: {
      name: 'Deck revise',
      nodes: [
        { id: 'review', type: 'llm', data: { type: 'llm', label: 'Review' } },
        { id: 'gate', type: 'approval', data: { type: 'approval', label: 'Approve a big edit', approvers: [{ type: 'user', value: 'alice' }], requiredCount: 1, message: 'A big edit waits.' } },
      ],
      edges: [{ id: 'e1', source: 'review', sourceHandle: 'out', target: 'gate', targetHandle: 'in' }],
    },
  };

  test('a gate on a run another replica owns is listed from the rollup row + run table, with approvers and upstream output', async () => {
    rollupSubscriberInstalled = true;
    mockListWithOpenApprovals.mockResolvedValue([
      { pipelineId: 'deck-revise', recent: [{ runId: 'run-b' }], approvals: [{ runId: 'run-b', stepId: 'gate', at: '2026-09-23T16:00:05.000Z' }], pendingApprovals: 1 },
    ]);
    mockRunRepoGet.mockResolvedValue(remoteRun);
    setPipelineBridge(makeBridge({ getPendingApprovals: async () => [], getRun: async () => null }));

    const res = await run(buildApp(), 'GET', '/pipelines/approvals');
    expect(res.statusCode).toBe(200);
    const { approvals } = res.body as { approvals: Array<Record<string, unknown>> };
    expect(approvals).toHaveLength(1);
    expect(approvals[0]).toMatchObject({
      runId: 'run-b', stepId: 'gate', pipelineId: 'deck-revise',
      approvers: [{ type: 'user', value: 'alice' }], message: 'A big edit waits.', requestedAt: '2026-09-23T16:00:05.000Z',
      pipelineName: 'Deck revise', stepName: 'Approve a big edit', requestedBy: 'bob',
      upstream: [expect.objectContaining({ stepId: 'review', output: { verdict: 'big edit' } })],
    });
    expect(mockRunRepoGet).toHaveBeenCalledWith('deck-revise', 'run-b');
    // `?userId=` filters the shared rows too.
    const filtered = await run(buildApp(), 'GET', '/pipelines/approvals?userId=carol');
    expect((filtered.body as { approvals: unknown[] }).approvals).toEqual([]);
  });

  test('the local queue wins for a gate this replica holds; a row unknown to the run table is listed without approvers', async () => {
    rollupSubscriberInstalled = true;
    mockListWithOpenApprovals.mockResolvedValue([
      { pipelineId: 'deck-revise', recent: [], approvals: [
        { runId: 'run-a', stepId: 'gate', at: '2026-09-23T16:00:01.000Z' },
        { runId: 'run-gone', stepId: 'gate', at: '2026-09-23T16:00:02.000Z' },
      ], pendingApprovals: 2 },
    ]);
    const localRow = { runId: 'run-a', stepId: 'gate', pipelineId: 'deck-revise', approvers: [{ type: 'user' as const, value: 'alice' }], message: 'local', requestedAt: '2026-09-23T16:00:01.500Z' };
    setPipelineBridge(makeBridge({ getPendingApprovals: async () => [localRow], getRun: async () => null }));

    const res = await run(buildApp(), 'GET', '/pipelines/approvals');
    expect(res.statusCode).toBe(200);
    const { approvals } = res.body as { approvals: Array<Record<string, unknown>> };
    expect(approvals.map((a) => a.runId)).toEqual(['run-a', 'run-gone']);
    expect(approvals[0]).toMatchObject({ message: 'local', requestedAt: '2026-09-23T16:00:01.500Z' });
    expect(approvals[1]).toMatchObject({ runId: 'run-gone', approvers: [], requestedAt: '2026-09-23T16:00:02.000Z' });
  });

  test('rollup rows unavailable → the local queue alone, still 200', async () => {
    rollupSubscriberInstalled = true;
    mockListWithOpenApprovals.mockRejectedValue(new Error('ddb down'));
    setPipelineBridge(makeBridge({ getPendingApprovals: async () => SAMPLE_ROWS }));
    const res = await run(buildApp(), 'GET', '/pipelines/approvals');
    expect(res.statusCode).toBe(200);
    expect(res.body).toEqual({ approvals: SAMPLE_ROWS });
  });

  test('no rollup subscriber in the process → the rows are never read', async () => {
    setPipelineBridge(makeBridge({ getPendingApprovals: async () => SAMPLE_ROWS }));
    const res = await run(buildApp(), 'GET', '/pipelines/approvals');
    expect(res.statusCode).toBe(200);
    expect(mockListWithOpenApprovals).not.toHaveBeenCalled();
  });
});

// Sample row used across the bridge-wired tests.
const SAMPLE_ROWS: PendingApprovalRow[] = [
  {
    runId: 'run-1',
    stepId: 'approval-A',
    pipelineId: 'pipe-1',
    approvers: [
      { type: 'user', value: 'alice', userId: 'alice' },
      { type: 'user', value: 'bob', userId: 'bob', role: 'lead' },
    ],
    message: 'Ship it?',
    requestedAt: '2026-04-25T12:00:00.000Z',
  },
  {
    runId: 'run-2',
    stepId: 'approval-B',
    pipelineId: 'pipe-1',
    approvers: [{ type: 'user', value: 'carol', userId: 'carol' }],
    requestedAt: '2026-04-25T12:05:00.000Z',
  },
];

// Build a partial bridge — the route only depends on `getPendingApprovals`,
// but `PipelineBridge` requires the three core methods as non-optional, so
// we stub them as no-ops.
function makeBridge(extra: Partial<PipelineBridge>): PipelineBridge {
  return {
    getRun: async () => null,
    getHistory: async () => [],
    resolveApproval: async () => {},
    ...extra,
  };
}

// ---------------------------------------------------------------------------
// GET /api/pipelines/approvals
// ---------------------------------------------------------------------------

describe('GET /pipelines/approvals (pending-approvals queue)', () => {
  test('returns { approvals: [] } with 200 when bridge is null (stub mode)', async () => {
    setPipelineBridge(null);
    const app = buildApp();
    const res = await run(app, 'GET', '/pipelines/approvals');
    expect(res.statusCode).toBe(200);
    expect(res.body).toEqual({ approvals: [] });
  });

  test('returns { approvals: [] } with 200 when bridge omits getPendingApprovals', async () => {
    // A bridge that intentionally does NOT implement getPendingApprovals —
    // mirrors the older Phase-1 bridges before the surface was added.
    setPipelineBridge(makeBridge({}));
    const app = buildApp();
    const res = await run(app, 'GET', '/pipelines/approvals');
    expect(res.statusCode).toBe(200);
    expect(res.body).toEqual({ approvals: [] });
  });

  test('forwards bridge.getPendingApprovals() rows verbatim', async () => {
    const getPendingApprovals = jest.fn(async () => SAMPLE_ROWS);
    setPipelineBridge(makeBridge({ getPendingApprovals }));

    const app = buildApp();
    const res = await run(app, 'GET', '/pipelines/approvals');
    expect(res.statusCode).toBe(200);
    expect(getPendingApprovals).toHaveBeenCalledTimes(1);
    expect(res.body).toEqual({ approvals: SAMPLE_ROWS });
  });

  test('synchronous bridge return is awaited correctly', async () => {
    // The bridge contract allows sync OR async — verify the sync arm too.
    const getPendingApprovals = jest.fn((): PendingApprovalRow[] => SAMPLE_ROWS);
    setPipelineBridge(makeBridge({ getPendingApprovals }));

    const app = buildApp();
    const res = await run(app, 'GET', '/pipelines/approvals');
    expect(res.statusCode).toBe(200);
    expect(res.body).toEqual({ approvals: SAMPLE_ROWS });
  });

  test('?userId=alice filters to rows where alice appears in approvers', async () => {
    setPipelineBridge(makeBridge({ getPendingApprovals: async () => SAMPLE_ROWS }));
    const app = buildApp();
    const res = await run(app, 'GET', '/pipelines/approvals?userId=alice');
    expect(res.statusCode).toBe(200);
    const body = res.body as { approvals: PendingApprovalRow[] };
    expect(body.approvals).toHaveLength(1);
    expect(body.approvals[0]!.runId).toBe('run-1');
  });

  test('?userId=carol filters to a different row', async () => {
    setPipelineBridge(makeBridge({ getPendingApprovals: async () => SAMPLE_ROWS }));
    const app = buildApp();
    const res = await run(app, 'GET', '/pipelines/approvals?userId=carol');
    expect(res.statusCode).toBe(200);
    const body = res.body as { approvals: PendingApprovalRow[] };
    expect(body.approvals).toHaveLength(1);
    expect(body.approvals[0]!.runId).toBe('run-2');
  });

  test('?userId=nobody returns an empty list (no rows match)', async () => {
    setPipelineBridge(makeBridge({ getPendingApprovals: async () => SAMPLE_ROWS }));
    const app = buildApp();
    const res = await run(app, 'GET', '/pipelines/approvals?userId=nobody');
    expect(res.statusCode).toBe(200);
    expect(res.body).toEqual({ approvals: [] });
  });

  test('omitting userId returns ALL rows (no implicit caller filter)', async () => {
    // Sanity: even though the request is from user-1, we should not filter
    // to rows that mention user-1 unless `?userId=` is explicitly passed.
    setPipelineBridge(makeBridge({ getPendingApprovals: async () => SAMPLE_ROWS }));
    const app = buildApp();
    const res = await run(app, 'GET', '/pipelines/approvals', { userId: 'user-1' });
    expect(res.statusCode).toBe(200);
    expect((res.body as { approvals: PendingApprovalRow[] }).approvals).toHaveLength(2);
  });

  test('401 when the request is unauthenticated', async () => {
    setPipelineBridge(makeBridge({ getPendingApprovals: async () => SAMPLE_ROWS }));
    const app = buildApp();
    const res = await run(app, 'GET', '/pipelines/approvals', { userId: null });
    expect(res.statusCode).toBe(401);
  });

  test('mount ordering: /pipelines/approvals does NOT fall through to :runId/approvals', async () => {
    // The bridge IS wired here but with no rows; if mount-ordering were wrong
    // this GET would hit the POST-only `:runId/approvals` router (that
    // pipelineApprovalsRouter only registers POST, so the request would
    // fall through to errorHandler / 404). Asserting on a 200 + the
    // documented body shape proves the static segment took precedence.
    setPipelineBridge(makeBridge({ getPendingApprovals: async () => [] }));
    const app = buildApp();
    const res = await run(app, 'GET', '/pipelines/approvals');
    expect(res.statusCode).toBe(200);
    expect(res.body).toEqual({ approvals: [] });
  });

  test('?userId= filter matches approver with only value set (no userId alias)', async () => {
    // Approver rows created without the optional `userId` convenience alias —
    // only the canonical `value` field is set. Prior to the fix the filter
    // used `a.userId === filterUserId`, which would miss these rows entirely.
    const valueOnlyRows: PendingApprovalRow[] = [
      {
        runId: 'run-3',
        stepId: 'approval-C',
        pipelineId: 'pipe-2',
        approvers: [{ type: 'user', value: 'diana' }], // no userId alias
        requestedAt: '2026-04-26T10:00:00.000Z',
      },
    ];
    setPipelineBridge(makeBridge({ getPendingApprovals: async () => valueOnlyRows }));
    const app = buildApp();

    const res = await run(app, 'GET', '/pipelines/approvals?userId=diana');
    expect(res.statusCode).toBe(200);
    const body = res.body as { approvals: PendingApprovalRow[] };
    expect(body.approvals).toHaveLength(1);
    expect(body.approvals[0]!.runId).toBe('run-3');

    // A different userId should NOT match this row.
    const otherRes = await run(app, 'GET', '/pipelines/approvals?userId=eve');
    expect((otherRes.body as { approvals: PendingApprovalRow[] }).approvals).toHaveLength(0);
  });

  test('?userId= filter does not match role-type approvers by value', async () => {
    // Role-type approvers cannot be resolved without a role-membership service.
    // The filter must only match user-type approvers; a role named "diana"
    // should NOT match a ?userId=diana query.
    const roleRow: PendingApprovalRow[] = [
      {
        runId: 'run-4',
        stepId: 'approval-D',
        pipelineId: 'pipe-3',
        approvers: [{ type: 'role', value: 'diana' }], // role named "diana"
        requestedAt: '2026-04-26T11:00:00.000Z',
      },
    ];
    setPipelineBridge(makeBridge({ getPendingApprovals: async () => roleRow }));
    const app = buildApp();

    const res = await run(app, 'GET', '/pipelines/approvals?userId=diana');
    expect(res.statusCode).toBe(200);
    const body = res.body as { approvals: PendingApprovalRow[] };
    // Role-type approver with value="diana" must NOT surface for ?userId=diana.
    expect(body.approvals).toHaveLength(0);
  });
});
