// Tests for pipelineMetrics + observability stub routers.
//
// We verify the stub generators produce the expected shape and that the
// routers wire them into HTTP handlers correctly. Both endpoints are
// stub-filled (no live cluster) so we don't need a bridge here.

import express, { type NextFunction, type Request, type Response } from 'express';
import {
  pipelineMetricsRouter,
  observabilityRouter,
  pipelineDashboardDataRouter,
  getPipelineMetricsStub,
  getObservabilityDashboardStub,
  getObservabilityMetricsStub,
  getPipelineDashboardDataStub,
} from '../pipelineMetrics';

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
  app.use('/pipelines/metrics', pipelineMetricsRouter);
  app.use('/pipelines/dashboard-data', pipelineDashboardDataRouter);
  app.use('/observability', observabilityRouter);
  return app;
}

// ---------------------------------------------------------------------------
// Stub generators (shape sanity checks).
// ---------------------------------------------------------------------------

describe('getPipelineMetricsStub', () => {
  test('returns expected shape with non-negative numbers', () => {
    const m = getPipelineMetricsStub();
    expect(typeof m.runsStarted).toBe('number');
    expect(m.runsStarted).toBeGreaterThanOrEqual(0);
    expect(m.runsCompleted).toBeGreaterThanOrEqual(0);
    expect(m.runsFailed).toBeGreaterThanOrEqual(0);
    expect(m.runsActive).toBeGreaterThanOrEqual(0);
    expect(m.runsAwaitingApproval).toBeGreaterThanOrEqual(0);
    expect(m.avgDurationMs).toBeGreaterThan(0);
    expect(typeof m.asOf).toBe('string');
    expect(new Date(m.asOf).toISOString()).toBe(m.asOf);
  });
});

describe('getObservabilityDashboardStub', () => {
  test('returns 3 healthy nodes and 0 alerts', () => {
    const d = getObservabilityDashboardStub();
    expect(d.cluster.healthy).toBe(3);
    expect(d.cluster.degraded).toBe(0);
    expect(d.cluster.down).toBe(0);
    expect(d.alerts).toEqual([]);
    expect(d.nodes).toHaveLength(3);
    expect(d.nodes.every((n) => n.status === 'healthy')).toBe(true);
    const leaders = d.nodes.filter((n) => n.role === 'leader');
    expect(leaders).toHaveLength(1);
    expect(d.cluster.leaderId).toBe(leaders[0].nodeId);
  });

  test('pipelines summary mirrors metrics stub', () => {
    const d = getObservabilityDashboardStub();
    expect(typeof d.pipelines.active).toBe('number');
    expect(typeof d.pipelines.awaitingApproval).toBe('number');
    expect(d.pipelines.active).toBeGreaterThanOrEqual(0);
  });
});

describe('getObservabilityMetricsStub', () => {
  test('p50 < p95 < p99', () => {
    const m = getObservabilityMetricsStub();
    expect(m.p50LatencyMs).toBeLessThan(m.p95LatencyMs);
    expect(m.p95LatencyMs).toBeLessThan(m.p99LatencyMs);
  });
  test('errorRatePct is bounded 0..100', () => {
    const m = getObservabilityMetricsStub();
    expect(m.errorRatePct).toBeGreaterThanOrEqual(0);
    expect(m.errorRatePct).toBeLessThanOrEqual(100);
  });
});

// ---------------------------------------------------------------------------
// HTTP wiring.
// ---------------------------------------------------------------------------

describe('GET /pipelines/metrics', () => {
  test('200 with metrics shape (no bridge wired → source=stub)', async () => {
    const app = buildApp();
    const res = await run(app, 'GET', '/pipelines/metrics');
    expect(res.statusCode).toBe(200);
    const body = res.body as ReturnType<typeof getPipelineMetricsStub> & { source: string };
    // New contract: response carries `source` so the frontend can render
    // a "demo data" banner when the bridge is unwired.
    expect(body.source).toBe('stub');
    // Backward-compat: original PipelineMetrics fields still present at top level.
    expect(body).toHaveProperty('runsStarted');
    expect(body).toHaveProperty('runsAwaitingApproval');
    expect(body).toHaveProperty('asOf');
  });

  test('200 with source=bridge when bridge returns full metrics — every field passes through', async () => {
    const triggers = await import('../pipelineTriggers.js');
    const fake = {
      getMetrics: async () => ({
        runsStarted: 100,
        runsCompleted: 80,
        runsFailed: 5,
        runsActive: 12,
        runsAwaitingApproval: 7,
        avgDurationMs: 8200,
        llmTokensIn: 250_000,
        llmTokensOut: 90_000,
        avgFirstTokenLatencyMs: 420,
        asOf: '2026-04-28T12:00:00.000Z',
      }),
    } as unknown as Parameters<typeof triggers.setPipelineBridge>[0];
    const prev = triggers.getPipelineBridge();
    triggers.setPipelineBridge(fake);
    try {
      const app = buildApp();
      const res = await run(app, 'GET', '/pipelines/metrics');
      expect(res.statusCode).toBe(200);
      const body = res.body as {
        source: string;
        runsStarted: number | null;
        runsCompleted: number | null;
        runsFailed: number | null;
        runsActive: number | null;
        runsAwaitingApproval: number | null;
        avgDurationMs: number | null;
        llmTokensIn: number | null;
        llmTokensOut: number | null;
        avgFirstTokenLatencyMs: number | null;
        estimatedCostUsd: number | null;
        asOf: string;
      };
      expect(body.source).toBe('bridge');
      // Every field the bridge returned now flows through — no more hard-coded null.
      expect(body.runsStarted).toBe(100);
      expect(body.runsCompleted).toBe(80);
      expect(body.runsFailed).toBe(5);
      expect(body.runsActive).toBe(12);
      expect(body.runsAwaitingApproval).toBe(7);
      expect(body.avgDurationMs).toBe(8200);
      expect(body.llmTokensIn).toBe(250_000);
      expect(body.llmTokensOut).toBe(90_000);
      // distributed-core v0.3.7+: avg first-token latency forwarded.
      expect(body.avgFirstTokenLatencyMs).toBe(420);
      // estimatedCostUsd is derived from token counts on the gateway side
      // (AUDIT-05 ask #2): (250000/1M)*3 + (90000/1M)*15 = 0.75 + 1.35 = 2.1
      expect(body.estimatedCostUsd).toBeCloseTo(2.1, 5);
      // Bridge-supplied asOf preserved (not regenerated to "now").
      expect(body.asOf).toBe('2026-04-28T12:00:00.000Z');
    } finally {
      triggers.setPipelineBridge(prev);
    }
  });

  test('200 with source=bridge when only runsAwaitingApproval is returned — other fields null', async () => {
    const triggers = await import('../pipelineTriggers.js');
    // Older distributed-core (pre-v0.3.x) only exposed the approval count.
    const fake = {
      getMetrics: async () => ({ runsAwaitingApproval: 7 }),
    } as unknown as Parameters<typeof triggers.setPipelineBridge>[0];
    const prev = triggers.getPipelineBridge();
    triggers.setPipelineBridge(fake);
    try {
      const app = buildApp();
      const res = await run(app, 'GET', '/pipelines/metrics');
      expect(res.statusCode).toBe(200);
      const body = res.body as {
        source: string;
        runsAwaitingApproval: number | null;
        runsStarted: number | null;
        avgDurationMs: number | null;
        avgFirstTokenLatencyMs: number | null;
        estimatedCostUsd: number | null;
        asOf: string;
      };
      expect(body.source).toBe('bridge');
      expect(body.runsAwaitingApproval).toBe(7);
      // Older bridge → unsupplied fields are null, never fabricated.
      expect(body.runsStarted).toBeNull();
      expect(body.avgDurationMs).toBeNull();
      expect(body.avgFirstTokenLatencyMs).toBeNull();
      expect(body.estimatedCostUsd).toBeNull();
      expect(typeof body.asOf).toBe('string');
    } finally {
      triggers.setPipelineBridge(prev);
    }
  });

  test('estimatedCostUsd is null when only one token side is available', async () => {
    const triggers = await import('../pipelineTriggers.js');
    const fake = {
      getMetrics: async () => ({
        runsAwaitingApproval: 3,
        llmTokensIn: 100_000,
        // llmTokensOut absent — cost cannot be derived
      }),
    } as unknown as Parameters<typeof triggers.setPipelineBridge>[0];
    const prev = triggers.getPipelineBridge();
    triggers.setPipelineBridge(fake);
    try {
      const app = buildApp();
      const res = await run(app, 'GET', '/pipelines/metrics');
      expect(res.statusCode).toBe(200);
      const body = res.body as { estimatedCostUsd: number | null };
      expect(body.estimatedCostUsd).toBeNull();
    } finally {
      triggers.setPipelineBridge(prev);
    }
  });

  test('500 with source=error when bridge.getMetrics throws', async () => {
    const triggers = await import('../pipelineTriggers.js');
    const fake = {
      getMetrics: async () => {
        throw new Error('boom');
      },
    } as unknown as Parameters<typeof triggers.setPipelineBridge>[0];
    const prev = triggers.getPipelineBridge();
    triggers.setPipelineBridge(fake);
    try {
      const app = buildApp();
      const res = await run(app, 'GET', '/pipelines/metrics');
      expect(res.statusCode).toBe(500);
      const body = res.body as { source: string; error: string };
      expect(body.source).toBe('error');
      expect(body.error).toBe('boom');
    } finally {
      triggers.setPipelineBridge(prev);
    }
  });
});

describe('GET /observability/dashboard', () => {
  test('200 with cluster + nodes + alerts', async () => {
    const app = buildApp();
    const res = await run(app, 'GET', '/observability/dashboard');
    expect(res.statusCode).toBe(200);
    const body = res.body as ReturnType<typeof getObservabilityDashboardStub>;
    expect(body.cluster.healthy).toBe(3);
    expect(body.nodes).toHaveLength(3);
    expect(body.alerts).toEqual([]);
  });

  test('`?include=pendingApprovals` is ignored: still 200 with the same stub body (the list lives at GET /pipelines/approvals)', async () => {
    // The pending-approvals hook labels its console error with this URL while
    // fetching /api/pipelines/approvals; this route never answers anything but
    // the stub, so a 500 seen under that label is the other route (or the
    // dev proxy while the pod restarts), never this one.
    const app = buildApp();
    const res = await run(app, 'GET', '/observability/dashboard?include=pendingApprovals');
    expect(res.statusCode).toBe(200);
    const body = res.body as ReturnType<typeof getObservabilityDashboardStub> & { pendingApprovals?: unknown };
    expect(body.cluster.healthy).toBe(3);
    expect(body.pipelines.awaitingApproval).toEqual(expect.any(Number));
    expect(body.pendingApprovals).toBeUndefined();
  });
});

describe('GET /observability/metrics', () => {
  test('200 with prometheus-ish summary', async () => {
    const app = buildApp();
    const res = await run(app, 'GET', '/observability/metrics');
    expect(res.statusCode).toBe(200);
    const body = res.body as ReturnType<typeof getObservabilityMetricsStub>;
    expect(body).toHaveProperty('runsPerMinute');
    expect(body).toHaveProperty('p50LatencyMs');
    expect(body).toHaveProperty('p95LatencyMs');
    expect(body).toHaveProperty('errorRatePct');
  });
});

// ---------------------------------------------------------------------------
// Pipeline module dashboard data — pass-through of getDashboardData()
// ---------------------------------------------------------------------------

describe('getPipelineDashboardDataStub', () => {
  test('returns the ApplicationModuleDashboardData shape', () => {
    const out = getPipelineDashboardDataStub();
    expect(out.moduleId).toBe('pipeline');
    expect(out.moduleName).toBe('Pipeline');
    expect(out.charts).toHaveLength(3);
    expect(out.charts.map((c) => c.title)).toEqual([
      'Run Outcomes',
      'LLM Token Usage',
      'Average Run Duration (ms)',
    ]);
    expect(out.summary).toHaveProperty('totalResources');
    expect(out.summary).toHaveProperty('healthyResources');
    expect(out.summary).toHaveProperty('activeConnections');
    expect(out.summary).toHaveProperty('throughput');
    expect(Array.isArray(out.alerts)).toBe(true);
  });
});

describe('GET /pipelines/dashboard-data', () => {
  test('serves stub when no bridge wired', async () => {
    const app = buildApp();
    const res = await run(app, 'GET', '/pipelines/dashboard-data');
    expect(res.statusCode).toBe(200);
    const body = res.body as { source: string; charts: unknown[]; moduleId: string };
    expect(body.source).toBe('stub');
    expect(body.moduleId).toBe('pipeline');
    expect(body.charts).toHaveLength(3);
  });

  test('passes through bridge payload when wired', async () => {
    const triggers = await import('../pipelineTriggers.js');
    const fake = {
      getDashboardData: async () => ({
        moduleId: 'pipeline',
        moduleName: 'Pipeline',
        state: 'running',
        summary: {
          totalResources: 12,
          healthyResources: 10,
          activeConnections: 2,
          throughput: 4.5,
        },
        charts: [
          { title: 'Run Outcomes', type: 'bar', data: { completed: 10, failed: 1, active: 2 } },
        ],
        alerts: [
          { level: 'warning', message: 'High failure rate: 12%', timestamp: 1_700_000_000_000 },
        ],
      }),
    } as unknown as Parameters<typeof triggers.setPipelineBridge>[0];
    const prev = triggers.getPipelineBridge();
    triggers.setPipelineBridge(fake);
    try {
      const app = buildApp();
      const res = await run(app, 'GET', '/pipelines/dashboard-data');
      expect(res.statusCode).toBe(200);
      const body = res.body as {
        source: string;
        summary: { totalResources: number; healthyResources: number };
        alerts: Array<{ level: string; message: string }>;
      };
      expect(body.source).toBe('bridge');
      expect(body.summary.totalResources).toBe(12);
      expect(body.summary.healthyResources).toBe(10);
      expect(body.alerts).toHaveLength(1);
      expect(body.alerts[0].level).toBe('warning');
    } finally {
      triggers.setPipelineBridge(prev);
    }
  });

  test('500 with source=error when bridge.getDashboardData throws', async () => {
    const triggers = await import('../pipelineTriggers.js');
    const fake = {
      getDashboardData: async () => {
        throw new Error('boom');
      },
    } as unknown as Parameters<typeof triggers.setPipelineBridge>[0];
    const prev = triggers.getPipelineBridge();
    triggers.setPipelineBridge(fake);
    try {
      const app = buildApp();
      const res = await run(app, 'GET', '/pipelines/dashboard-data');
      expect(res.statusCode).toBe(500);
      const body = res.body as { source: string; error: string };
      expect(body.source).toBe('error');
      expect(body.error).toBe('boom');
    } finally {
      triggers.setPipelineBridge(prev);
    }
  });
});
