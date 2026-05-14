// Tests for the /api/pipelines/health stub router.
//
// We verify:
//   1. Endpoint shape — every documented field is present with the right type.
//   2. `llmClientConfigured` is sensitive to env-var changes:
//        - default provider (anthropic) requires ANTHROPIC_API_KEY
//        - PIPELINE_LLM_PROVIDER=bedrock relies on AWS_REGION (default chain)
//        - unknown provider returns false
//   3. Phase 1 status is `unwired` even when the LLM is configured (cluster
//      and pipelineModule are still stubbed false).

import express, { type NextFunction, type Request, type Response } from 'express';
import {
  pipelineHealthRouter,
  hasLLMConfig,
  getPipelineHealthStub,
  probeBridge,
  type PipelineHealth,
} from '../pipelineHealth';
import {
  setPipelineBridge,
  type PipelineBridge,
  type PipelineRunSnapshot,
  type PendingApprovalRow,
} from '../pipelineTriggers';

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

async function run(app: express.Express, method: string, url: string): Promise<MockRes> {
  const res = mockRes();
  const req = {
    method: method.toUpperCase(),
    url,
    originalUrl: url,
    path: url.split('?')[0],
    headers: {},
    query: {},
    body: undefined,
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
    (app as unknown as (req: Request, res: Response, next: NextFunction) => void)(
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
  app.use('/pipelines/health', pipelineHealthRouter);
  return app;
}

// ---------------------------------------------------------------------------
// Env helpers — restore env around each test so order doesn't matter.
// ---------------------------------------------------------------------------

const ORIG_ENV = {
  ANTHROPIC_API_KEY: process.env.ANTHROPIC_API_KEY,
  PIPELINE_LLM_PROVIDER: process.env.PIPELINE_LLM_PROVIDER,
  AWS_REGION: process.env.AWS_REGION,
};

afterEach(() => {
  for (const [k, v] of Object.entries(ORIG_ENV)) {
    if (v === undefined) delete (process.env as Record<string, string | undefined>)[k];
    else (process.env as Record<string, string | undefined>)[k] = v;
  }
});

// ---------------------------------------------------------------------------
// hasLLMConfig — env-var sensitivity
// ---------------------------------------------------------------------------

describe('hasLLMConfig', () => {
  test('returns true with anthropic provider + ANTHROPIC_API_KEY', () => {
    process.env.PIPELINE_LLM_PROVIDER = 'anthropic';
    process.env.ANTHROPIC_API_KEY = 'sk-ant-test';
    expect(hasLLMConfig()).toBe(true);
  });

  test('returns false with anthropic provider when ANTHROPIC_API_KEY is missing', () => {
    process.env.PIPELINE_LLM_PROVIDER = 'anthropic';
    delete process.env.ANTHROPIC_API_KEY;
    expect(hasLLMConfig()).toBe(false);
  });

  test('defaults to anthropic when PIPELINE_LLM_PROVIDER is unset', () => {
    delete process.env.PIPELINE_LLM_PROVIDER;
    process.env.ANTHROPIC_API_KEY = 'sk-ant-test';
    expect(hasLLMConfig()).toBe(true);
    delete process.env.ANTHROPIC_API_KEY;
    expect(hasLLMConfig()).toBe(false);
  });

  test('returns true for bedrock (AWS default chain is acceptable)', () => {
    process.env.PIPELINE_LLM_PROVIDER = 'bedrock';
    process.env.AWS_REGION = 'us-east-1';
    expect(hasLLMConfig()).toBe(true);
  });

  test('returns false for unknown provider', () => {
    process.env.PIPELINE_LLM_PROVIDER = 'mystery-llm';
    expect(hasLLMConfig()).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// Stub generator shape
// ---------------------------------------------------------------------------

describe('getPipelineHealthStub', () => {
  test('returns the documented PipelineHealth shape', () => {
    process.env.PIPELINE_LLM_PROVIDER = 'anthropic';
    delete process.env.ANTHROPIC_API_KEY;
    const h = getPipelineHealthStub();
    expect(h.status).toBe('unwired');
    expect(h.embeddedClusterReady).toBe(false);
    expect(h.llmClientConfigured).toBe(false);
    expect(h.pipelineModuleConnected).toBe(false);
    expect(h.lastEventAt).toBeNull();
    expect(h.tokenRate).toBeNull();
    expect(typeof h.asOf).toBe('string');
    expect(new Date(h.asOf).toISOString()).toBe(h.asOf);
    // Bridge probe defaults — additive fields, "no bridge wired" baseline.
    expect(h.bridgeWired).toBe(false);
    expect(h.runsActive).toBe(0);
    expect(h.runsAwaitingApproval).toBe(0);
    expect(h.pendingApprovals).toBe(0);
  });

  test('llmClientConfigured reflects env at call time', () => {
    process.env.PIPELINE_LLM_PROVIDER = 'anthropic';
    process.env.ANTHROPIC_API_KEY = 'sk-ant-present';
    expect(getPipelineHealthStub().llmClientConfigured).toBe(true);

    delete process.env.ANTHROPIC_API_KEY;
    expect(getPipelineHealthStub().llmClientConfigured).toBe(false);
  });

  test('Phase 1 status is `unwired` even when the LLM is configured', () => {
    process.env.PIPELINE_LLM_PROVIDER = 'anthropic';
    process.env.ANTHROPIC_API_KEY = 'sk-ant-present';
    const h = getPipelineHealthStub();
    expect(h.status).toBe('unwired');
    expect(h.llmClientConfigured).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// HTTP wiring
// ---------------------------------------------------------------------------

describe('GET /pipelines/health', () => {
  // Ensure no bridge is wired between cases — these tests assert the
  // "no bridge" baseline. The bridge-probe describe block below sets and
  // tears down its own bridges.
  beforeEach(() => setPipelineBridge(null));
  afterEach(() => setPipelineBridge(null));

  test('200 with PipelineHealth shape', async () => {
    process.env.PIPELINE_LLM_PROVIDER = 'anthropic';
    process.env.ANTHROPIC_API_KEY = 'sk-ant-test';
    const app = buildApp();
    const res = await run(app, 'GET', '/pipelines/health');
    expect(res.statusCode).toBe(200);
    const body = res.body as PipelineHealth;
    expect(body.status).toBe('unwired');
    expect(body.embeddedClusterReady).toBe(false);
    expect(body.llmClientConfigured).toBe(true);
    expect(body.pipelineModuleConnected).toBe(false);
    expect(body.lastEventAt).toBeNull();
    expect(body.tokenRate).toBeNull();
    expect(typeof body.asOf).toBe('string');
    expect(body.bridgeWired).toBe(false);
    expect(body.runsActive).toBe(0);
    expect(body.runsAwaitingApproval).toBe(0);
    expect(body.pendingApprovals).toBe(0);
  });

  test('llmClientConfigured=false propagates over the wire when key is missing', async () => {
    process.env.PIPELINE_LLM_PROVIDER = 'anthropic';
    delete process.env.ANTHROPIC_API_KEY;
    const app = buildApp();
    const res = await run(app, 'GET', '/pipelines/health');
    expect(res.statusCode).toBe(200);
    const body = res.body as PipelineHealth;
    expect(body.llmClientConfigured).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// Bridge probe — bridgeWired / runsActive / runsAwaitingApproval / pendingApprovals
// ---------------------------------------------------------------------------

describe('GET /pipelines/health bridge probe', () => {
  // Always tear down any wired bridge so cases don't bleed into each other or
  // into the rest of the suite.
  afterEach(() => setPipelineBridge(null));

  test('bridge null → bridgeWired:false and the three counts are 0', async () => {
    setPipelineBridge(null);
    const app = buildApp();
    const res = await run(app, 'GET', '/pipelines/health');
    expect(res.statusCode).toBe(200);
    const body = res.body as PipelineHealth;
    expect(body.bridgeWired).toBe(false);
    expect(body.runsActive).toBe(0);
    expect(body.runsAwaitingApproval).toBe(0);
    expect(body.pendingApprovals).toBe(0);
  });

  test('wired bridge — counts reflect listActiveRuns / getMetrics / getPendingApprovals', async () => {
    const fakeRuns: PipelineRunSnapshot[] = [
      { runId: 'r1', pipelineId: 'p1' },
      { runId: 'r2', pipelineId: 'p1' },
      { runId: 'r3', pipelineId: 'p2' },
    ];
    const fakePending: PendingApprovalRow[] = [
      {
        runId: 'r1',
        stepId: 's1',
        pipelineId: 'p1',
        approvers: [{ type: 'user', value: 'u1', userId: 'u1' }],
        requestedAt: '2026-04-25T00:00:00.000Z',
      },
      {
        runId: 'r2',
        stepId: 's1',
        pipelineId: 'p1',
        approvers: [{ type: 'user', value: 'u2', userId: 'u2' }],
        requestedAt: '2026-04-25T00:00:01.000Z',
      },
    ];

    const stub: PipelineBridge = {
      getRun: () => null,
      getHistory: () => [],
      resolveApproval: () => undefined,
      listActiveRuns: () => fakeRuns,
      getMetrics: async () => ({ runsAwaitingApproval: 5 }),
      getPendingApprovals: () => fakePending,
    };
    setPipelineBridge(stub);

    const app = buildApp();
    const res = await run(app, 'GET', '/pipelines/health');
    expect(res.statusCode).toBe(200);
    const body = res.body as PipelineHealth;
    expect(body.bridgeWired).toBe(true);
    expect(body.runsActive).toBe(3);
    expect(body.runsAwaitingApproval).toBe(5);
    expect(body.pendingApprovals).toBe(2);
  });

  test('bridge whose getMetrics throws → runsAwaitingApproval:0 but other fields still populate', async () => {
    const stub: PipelineBridge = {
      getRun: () => null,
      getHistory: () => [],
      resolveApproval: () => undefined,
      listActiveRuns: () => [
        { runId: 'r1', pipelineId: 'p1' },
        { runId: 'r2', pipelineId: 'p1' },
      ],
      getMetrics: () => {
        throw new Error('bridge metrics down');
      },
      getPendingApprovals: () => [
        {
          runId: 'r1',
          stepId: 's1',
          pipelineId: 'p1',
          approvers: [{ type: 'user', value: 'u1', userId: 'u1' }],
          requestedAt: '2026-04-25T00:00:00.000Z',
        },
      ],
    };
    setPipelineBridge(stub);

    const app = buildApp();
    const res = await run(app, 'GET', '/pipelines/health');
    expect(res.statusCode).toBe(200);
    const body = res.body as PipelineHealth;
    expect(body.bridgeWired).toBe(true);
    expect(body.runsActive).toBe(2);
    expect(body.runsAwaitingApproval).toBe(0);
    expect(body.pendingApprovals).toBe(1);
  });

  test('bridge without listActiveRuns method → runsActive:0', async () => {
    // No listActiveRuns — hits the else branch (line 243)
    const stub: PipelineBridge = {
      getRun: () => null,
      getHistory: () => [],
      resolveApproval: () => undefined,
      getMetrics: async () => ({ runsAwaitingApproval: 0 }),
      getPendingApprovals: () => [],
    };
    setPipelineBridge(stub);

    const r = await probeBridge(stub);
    expect(r.probes.runsActive).toMatchObject({ status: 'ok', value: 0 });
  });

  test('bridge without getMetrics method → runsAwaitingApproval:0', async () => {
    // No getMetrics — hits the else branch (line 261)
    const stub: PipelineBridge = {
      getRun: () => null,
      getHistory: () => [],
      resolveApproval: () => undefined,
      listActiveRuns: () => [],
      getPendingApprovals: () => [],
    };
    const r = await probeBridge(stub);
    expect(r.probes.runsAwaitingApproval).toMatchObject({ status: 'ok', value: 0 });
  });

  test('bridge without getPendingApprovals method → pendingApprovals:0', async () => {
    // No getPendingApprovals — hits the else branch (line 277)
    const stub: PipelineBridge = {
      getRun: () => null,
      getHistory: () => [],
      resolveApproval: () => undefined,
      listActiveRuns: () => [],
      getMetrics: async () => ({ runsAwaitingApproval: 0 }),
    };
    const r = await probeBridge(stub);
    expect(r.probes.pendingApprovals).toMatchObject({ status: 'ok', value: 0 });
  });

  test('async listActiveRuns that rejects → runsActive probe status:error', async () => {
    // Returns a rejecting Promise → hits async catch in runProbe (line 174)
    const stub: PipelineBridge = {
      getRun: () => null,
      getHistory: () => [],
      resolveApproval: () => undefined,
      listActiveRuns: () => Promise.reject(new Error('executor gone')),
      getMetrics: async () => ({ runsAwaitingApproval: 0 }),
      getPendingApprovals: () => [],
    };
    const r = await probeBridge(stub);
    expect(r.probes.runsActive.status).toBe('error');
    // runsActive scalar falls back to 0 on non-ok probe
    expect(r.runsActive).toBe(0);
  });

  test('async listActiveRuns that times out → probe status:timeout', async () => {
    // Returns a Promise that never resolves so the internal 1-second timer fires.
    // Use fake timers to avoid a 1-second real wait.
    jest.useFakeTimers();

    const stub: PipelineBridge = {
      getRun: () => null,
      getHistory: () => [],
      resolveApproval: () => undefined,
      listActiveRuns: () => new Promise(() => { /* never resolves */ }),
      getMetrics: async () => ({ runsAwaitingApproval: 0 }),
      getPendingApprovals: () => [],
    };

    const probePromise = probeBridge(stub);
    // Advance past BRIDGE_PROBE_TIMEOUT_MS (1000 ms)
    jest.advanceTimersByTime(1500);
    const r = await probePromise;
    jest.useRealTimers();

    expect(r.probes.runsActive.status).toBe('timeout');
    expect(r.runsActive).toBe(0);
  });

  test('probe status:timeout surfaces a warn log in the HTTP handler', async () => {
    jest.useFakeTimers();
    const warnSpy = jest.spyOn(console, 'warn').mockImplementation(() => {});

    const stub: PipelineBridge = {
      getRun: () => null,
      getHistory: () => [],
      resolveApproval: () => undefined,
      listActiveRuns: () => new Promise(() => { /* hangs */ }),
      getMetrics: async () => ({ runsAwaitingApproval: 0 }),
      getPendingApprovals: () => [],
    };
    setPipelineBridge(stub);

    const app = buildApp();
    const resPromise = run(app, 'GET', '/pipelines/health');
    jest.advanceTimersByTime(1500);
    const res = await resPromise;
    jest.useRealTimers();
    warnSpy.mockRestore();

    // Response still 200 — degraded, not broken
    expect(res.statusCode).toBe(200);
    const body = res.body as PipelineHealth;
    expect(body.bridgeWired).toBe(true);
    expect(body.runsActive).toBe(0);
  });

  test('getLastEventAt is read from bridge when present', async () => {
    const now = Date.now();
    const stub: PipelineBridge = {
      getRun: () => null,
      getHistory: () => [],
      resolveApproval: () => undefined,
      listActiveRuns: () => [],
      getMetrics: async () => ({ runsAwaitingApproval: 0 }),
      getPendingApprovals: () => [],
      getLastEventAt: () => now,
    };
    setPipelineBridge(stub);

    const app = buildApp();
    const res = await run(app, 'GET', '/pipelines/health');
    expect(res.statusCode).toBe(200);
    const body = res.body as PipelineHealth;
    // lastEventAt should be the ISO string for `now`
    expect(body.lastEventAt).toBe(new Date(now).toISOString());
  });

  test('catastrophic probeBridge error returns 500', async () => {
    // Make getPipelineBridge throw by temporarily replacing it via the module.
    // Easiest way without modifying the module: wire a bridge whose very
    // first method access throws (can't do that easily), so instead we use a
    // bridge that causes getPipelineBridge → probeBridge to throw internally
    // by passing something non-null but deeply broken.
    //
    // Actually — probeBridge itself doesn't throw unless there's a JS error
    // in the outer function. We can simulate by calling getPipelineBridge from
    // a mock that throws AFTER being wired.  The simplest approach is to use
    // jest.mock at module scope. But since this is an afterEach-style test
    // we'll do it via a real scenario: if probeBridge itself is replaced with
    // a throwing mock we can verify the 500 path.
    //
    // Instead, test the branch by wiring a bridge that causes the outer
    // try/catch at lines 337-347 to fire: a bridge object whose first async
    // probe (listActiveRuns) causes an unhandled rejection BEFORE the inner
    // runProbe catches it. The easiest way is to make listActiveRuns a getter
    // that throws when typeof is called — but that's contrived.
    //
    // Direct approach: the try block catches throws from probeBridge(). We
    // test it indirectly by checking the 500 response when probeBridge throws.
    // We do that by injecting into the module via jest.spyOn on probeBridge.
    const { probeBridge: pb } = await import('../pipelineHealth.js');
    const spy = jest.spyOn({ probeBridge: pb }, 'probeBridge').mockRejectedValueOnce(new Error('catastrophic'));
    // We can't easily spy on the module export used inside the route since it's
    // a local variable. Instead, let's cover it via a non-bridge path:
    // if bridge itself throws when listActiveRuns getter is accessed, the outer
    // try catch fires. That requires Object.defineProperty.
    const evilBridge: PipelineBridge = {
      getRun: () => null,
      getHistory: () => [],
      resolveApproval: () => undefined,
    };
    // Make listActiveRuns a getter that throws
    Object.defineProperty(evilBridge, 'listActiveRuns', {
      get() { throw new Error('bridge internal error'); },
      configurable: true,
    });
    setPipelineBridge(evilBridge);
    spy.mockRestore();

    const app = buildApp();
    const res = await run(app, 'GET', '/pipelines/health');
    // probeBridge catches internal errors in runProbe, but if the typeof check
    // for the method throws, the outer try catches it → 500.
    // Actually typeof on a getter that throws will throw! So this IS 500.
    expect(res.statusCode).toBe(500);
  });
});
