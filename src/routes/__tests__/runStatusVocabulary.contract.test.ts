// GATE — the runs *list* and runs *detail* endpoints must report the SAME
// status string for the SAME run.
//
// The bug this pins:
//   The two endpoints read from two different stores. `GET /runs` (list) is
//   served from DynamoDB via `createBridge.listRuns` → `PipelineRunRepository`.
//   `GET /runs/:runId` (detail) is served from the live executor via
//   `bridge.getRun`. `runPersistence` wrote the legacy alias `succeeded` into
//   DynamoDB while `PipelineExecutor` used the canonical `completed`, so the
//   same run reported `succeeded` from one endpoint and `completed` from the
//   other. Observed live on run 78576b07-df33-40a8-a8ab-99291214de18:
//   list said "succeeded", detail said "completed".
//
//   Nothing in the type system caught it: `PipelineRunSnapshot['status']` is
//   deliberately widened to `string` so deployments can carry their own
//   lifecycle states, which is exactly the hole the two spellings slipped
//   through. The frontend keys every comparison off `completed`/`cancelled`,
//   so the list endpoint's `succeeded` was silently dropped by the status
//   filter and uncounted by the stats page.
//
// Why this bites: it drives the REAL `createBridge` (not a hand-written fake
// bridge) against a fake repository + fake executor module, so removing the
// `normalizeRunStatus` call at the read boundary makes the equality assertion
// fail rather than passing on a mock that never had the bug.

// Loop 29c: run access has its own suite (runAccess.routes.test.ts); this
// contract is about status vocabulary, so the gate passes here.
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
import {
  PIPELINE_RUN_STATUSES,
  normalizeRunStatus,
} from '@connorhoehn/pipeline-runtime';

jest.mock('../../lib/redis-client', () => ({
  getRedisClient: async () => null,
}));

jest.mock('../../lib/aws-clients', () => ({
  docClient: { send: jest.fn().mockResolvedValue({ Item: undefined, Items: [] }) },
  ddbClient: { send: jest.fn().mockResolvedValue({}) },
  sqsClient: { send: jest.fn().mockResolvedValue({}) },
}));

import { errorHandler } from '../../middleware/error-handler';
import { pipelineTriggersRouter, setPipelineBridge } from '../pipelineTriggers';
import { createBridge } from '../../pipeline/createBridge';
import type { PipelineRunItem, PipelineRunRepository } from '../../repositories/PipelineRunRepository';

// ---------------------------------------------------------------------------
// Minimal request rig — mirrors pipelineTriggers.test.ts (no supertest in tree).
// ---------------------------------------------------------------------------

interface MockRes {
  statusCode: number;
  body: unknown;
  ended: boolean;
  finished: Promise<void>;
  status(code: number): MockRes;
  json(body: unknown): MockRes;
  setHeader(): MockRes;
  getHeader(): unknown;
  end(): MockRes;
}

function mockRes(): MockRes {
  let resolveFinished!: () => void;
  const finished = new Promise<void>((r) => {
    resolveFinished = r;
  });
  const r: MockRes = {
    statusCode: 200,
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
    setHeader() {
      return this;
    },
    getHeader() {
      return undefined;
    },
    end() {
      this.ended = true;
      resolveFinished();
      return this;
    },
  };
  return r;
}

async function get(app: express.Express, url: string): Promise<MockRes> {
  const res = mockRes();
  const [pathname] = url.split('?', 2);
  const req = {
    method: 'GET',
    url,
    originalUrl: url,
    path: pathname,
    headers: {},
    query: {},
    body: undefined,
    user: { sub: 'user-1' },
  } as unknown as Request;

  await new Promise<void>((resolve, reject) => {
    const finalHandler = (err?: unknown): void => {
      if (err) return reject(err);
      if (!res.ended) res.status(404).json({ error: 'route not found' });
      resolve();
    };
    (app as unknown as (r: Request, s: Response, n: NextFunction) => void)(
      req,
      res as unknown as Response,
      finalHandler,
    );
    res.finished.then(() => resolve()).catch(reject);
  });
  return res;
}

function buildApp(): express.Express {
  const app = express();
  app.use(express.json());
  app.use((req, _res, next) => {
    (req as Request & { user?: { sub: string } }).user = { sub: 'user-1' };
    next();
  });
  app.use('/pipelines/:pipelineId/runs', pipelineTriggersRouter);
  app.use(errorHandler);
  return app;
}

// ---------------------------------------------------------------------------
// Fakes — one run, two stores, exactly as production has it.
// ---------------------------------------------------------------------------

const PIPELINE_ID = 'probe-pipeline-001';

/**
 * The executor side (feeds `GET /runs/:runId`). `PipelineExecutor` sets
 * `status` from its own `RunStatus` union, which is canonical by construction.
 */
function fakeModule(runId: string, executorStatus: string) {
  return {
    getRun: (id: string) =>
      id === runId
        ? { id: runId, pipelineId: PIPELINE_ID, status: executorStatus, steps: {} }
        : null,
    listActiveRuns: () => [],
    getHistory: () => [],
    getMetrics: () => ({}),
    getPendingApprovals: () => [],
  } as never;
}

/**
 * The DynamoDB side (feeds `GET /runs`). `storedStatus` is whatever
 * `runPersistence` wrote — which for historical rows is a legacy alias.
 */
function fakeRepo(runId: string, storedStatus: string) {
  return {
    listByPipeline: async (): Promise<PipelineRunItem[]> => [
      {
        pipelineId: PIPELINE_ID,
        runId,
        status: storedStatus,
        startedAt: '2026-08-24T02:35:31.494Z',
        completedAt: '2026-08-24T02:35:31.700Z',
        triggeredBy: 'user-1',
        triggerType: 'manual',
        stepsSummary: {},
      } as unknown as PipelineRunItem,
    ],
  } as unknown as PipelineRunRepository;
}

async function statusesFromBothEndpoints(
  runId: string,
  storedStatus: string,
  executorStatus: string,
): Promise<{ list: unknown; detail: unknown }> {
  setPipelineBridge(
    createBridge(fakeModule(runId, executorStatus), null, fakeRepo(runId, storedStatus)),
  );
  const app = buildApp();

  const listRes = await get(app, `/pipelines/${PIPELINE_ID}/runs`);
  expect(listRes.statusCode).toBe(200);
  const listed = (listRes.body as { runs: Array<{ runId: string; status: unknown }> }).runs.find(
    (r) => r.runId === runId,
  );
  expect(listed).toBeDefined();

  const detailRes = await get(app, `/pipelines/${PIPELINE_ID}/runs/${runId}`);
  expect(detailRes.statusCode).toBe(200);
  const detail = detailRes.body as { status: unknown };

  return { list: listed!.status, detail: detail.status };
}

afterEach(() => {
  setPipelineBridge(null);
});

// ---------------------------------------------------------------------------

describe('GATE: runs list and runs detail agree on status', () => {
  it('agrees for a row persisted with the legacy `succeeded` alias', async () => {
    // This is the exact production shape at the time of the bug: DynamoDB holds
    // `succeeded`, the executor holds `completed`.
    const { list, detail } = await statusesFromBothEndpoints(
      'da861753-e551-4c8a-b19c-8141a6d6262f',
      'succeeded',
      'completed',
    );

    expect(list).toBe(detail);
    expect(list).toBe('completed');
  });

  it('agrees for a row persisted with the legacy `canceled` alias', async () => {
    const { list, detail } = await statusesFromBothEndpoints(
      'c750b241-2da0-4263-b1be-0cecbc063900',
      'canceled',
      'cancelled',
    );

    expect(list).toBe(detail);
    expect(list).toBe('cancelled');
  });

  it.each(['completed', 'failed', 'cancelled', 'running'])(
    'agrees for canonical status %s',
    async (status) => {
      const { list, detail } = await statusesFromBothEndpoints(
        `run-${status}`,
        status,
        status,
      );

      expect(list).toBe(detail);
      expect(list).toBe(status);
    },
  );

  it('never serves a legacy alias to a client from either endpoint', async () => {
    for (const legacy of ['succeeded', 'canceled', 'done', 'awaiting-approval']) {
      const { list, detail } = await statusesFromBothEndpoints(
        `run-${legacy}`,
        legacy,
        normalizeRunStatus(legacy),
      );

      expect(list).toBe(detail);
      // Whatever is served must be a member of the canonical union — never the
      // alias the row was written with.
      expect(PIPELINE_RUN_STATUSES).toContain(list);
      expect(list).not.toBe(legacy);
    }
  });

  it('passes through platform-api lifecycle states it does not own', async () => {
    // `interrupted` is set by the orphan scan and drives the UI's Resume
    // affordance. Normalization must not coerce or drop it.
    const { list, detail } = await statusesFromBothEndpoints(
      'run-interrupted',
      'interrupted',
      'interrupted',
    );

    expect(list).toBe(detail);
    expect(list).toBe('interrupted');
  });
});

describe('GATE: runPersistence writes canonical spellings', () => {
  // The read boundary folds legacy aliases, but new writes must be canonical
  // so the aliases stay a finite migration set rather than a growing one.
  // Asserted against source text because the handlers are private and driving
  // the full event bus here would test the bus, not the vocabulary.
  it('does not assign a legacy alias to run status', () => {
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const src = require('fs').readFileSync(
      require('path').join(__dirname, '../../pipeline/runPersistence.ts'),
      'utf8',
    ) as string;

    // The right-hand side is matched loosely (`[^;\n]*`) because a status
    // assignment is not always a bare literal: `handleRunCompleted` became a
    // ternary when `rejected` landed in pipeline-runtime v1.2.0
    //   state.status = event.status === 'rejected' ? 'rejected' : 'completed';
    // and a literal-only regex silently stopped matching the very line it
    // exists to guard. What this gate actually asserts is which SPELLINGS may
    // appear on the write side, so match the spelling, not the syntax.
    expect(src).not.toMatch(/state\.status\s*=\s*[^;\n]*'succeeded'/);
    expect(src).not.toMatch(/state\.status\s*=\s*[^;\n]*'canceled'/);
    expect(src).toMatch(/state\.status\s*=\s*[^;\n]*'completed'/);
    expect(src).toMatch(/state\.status\s*=\s*[^;\n]*'cancelled'/);
    // `rejected` is terminal and distinct from `completed` — it must be a
    // spelling the writer can actually produce, or the read side folds a
    // status nothing ever persists.
    expect(src).toMatch(/state\.status\s*=\s*[^;\n]*'rejected'/);
  });
});
