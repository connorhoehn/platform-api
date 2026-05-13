// Phase M4.2a — round-trip integration tests for the worker-router.
//
// Two layers:
//   1. Direct supertest against the router with a fake bridge — proves
//      the wire format on the server side.
//   2. End-to-end HTTPPipelineBridge → Express server → worker-router
//      → fake bridge → response → client. Proves the FULL bridge wire
//      works for every routed method.

import express, { type Express } from 'express';
import http from 'http';
import type { AddressInfo } from 'net';

import { createWorkerRouter } from '../worker-router';
import { HTTPPipelineBridge } from '../http-bridge';
import type { PipelineBridge } from '../bridge-types';
import { signEnvelope } from '@connorhoehn/service-runtime';

// Tiny supertest-shaped helper so the existing `request(app).post(...)` calls
// below stay readable. Spins up the express app on an ephemeral port for the
// duration of a single request — fine for unit-test latency.
async function send(
  app: Express,
  path: string,
  body: unknown,
  headers: Record<string, string> = {},
): Promise<{ status: number; body: any }> {
  const { url, close } = await listenOn(app);
  try {
    const res = await fetch(`${url}${path}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...headers },
      body: JSON.stringify(body),
    });
    let parsed: any = {};
    const text = await res.text();
    if (text) {
      try {
        parsed = JSON.parse(text);
      } catch {
        parsed = text;
      }
    }
    return { status: res.status, body: parsed };
  } finally {
    await close();
  }
}
const request = (app: Express) => ({
  post: (path: string) => ({
    send: (body: unknown) => send(app, path, body),
    set: (header: string, value: string) => ({
      send: (body: unknown) => send(app, path, body, { [header]: value }),
    }),
  }),
});

// ─── Fake bridge ──────────────────────────────────────────────────────

function makeFakeBridge(): PipelineBridge & {
  calls: Array<{ method: string; args: unknown[] }>;
  setReturn: (method: string, value: unknown) => void;
} {
  const calls: Array<{ method: string; args: unknown[] }> = [];
  const returns = new Map<string, unknown>();
  const record = (method: string, args: unknown[]): unknown => {
    calls.push({ method, args });
    return returns.has(method) ? returns.get(method) : undefined;
  };

  const inspector = {
    paginatePending: async (opts: unknown) => record('inspector.paginatePending', [opts]),
    peekPending: async (opts: unknown) => record('inspector.peekPending', [opts]),
    listInflight: async (opts: unknown) => record('inspector.listInflight', [opts]),
    findStuckLeases: async (opts: unknown) => record('inspector.findStuckLeases', [opts]),
    summarizeQueue: async (opts: unknown) => record('inspector.summarizeQueue', [opts]),
    aggregateFailures: async (opts: unknown) => record('inspector.aggregateFailures', [opts]),
  };

  const bridge: PipelineBridge & {
    calls: Array<{ method: string; args: unknown[] }>;
    setReturn: (method: string, value: unknown) => void;
  } = {
    calls,
    setReturn: (m, v) => returns.set(m, v),
    trigger: async (args) => record('trigger', [args]) as { runId: string },
    resolveBreakpoint: (runId, stepId, action) => {
      record('resolveBreakpoint', [runId, stepId, action]);
    },
    resumeFromStep: async (runId, fromNodeId, opts) =>
      record('resumeFromStep', [runId, fromNodeId, opts]),
    patchPausedContext: async (runId, stepId, patch, appliedBy) =>
      record('patchPausedContext', [runId, stepId, patch, appliedBy]) as never,
    revertContextPatch: async (runId, stepId, patchId) => {
      record('revertContextPatch', [runId, stepId, patchId]);
    },
    getRun: (runId) => record('getRun', [runId]) as never,
    getHistory: (runId, fromVersion) => record('getHistory', [runId, fromVersion]) as never,
    resolveApproval: async (runId, stepId, userId, decision, comment) => {
      record('resolveApproval', [runId, stepId, userId, decision, comment]);
    },
    listRuns: async (pipelineId, opts) => record('listRuns', [pipelineId, opts]) as never,
    listActiveRuns: async () => (record('listActiveRuns', []) as never) ?? [],
    cancelRun: async (runId, userId) => {
      record('cancelRun', [runId, userId]);
    },
    getPendingApprovals: async () => (record('getPendingApprovals', []) as never) ?? [],
    getLastEventAt: () => record('getLastEventAt', []) as number | null,
    getMetrics: async () => record('getMetrics', []) as never,
    getDashboardData: async () => record('getDashboardData', []) as never,
    getInspector: () => inspector as never,
  };
  return bridge;
}

// ─── App / server helpers ────────────────────────────────────────────

function makeApp(bridge: PipelineBridge, opts?: Parameters<typeof createWorkerRouter>[1]): Express {
  const app = express();
  app.use(express.json());
  app.use('/internal/pipeline', createWorkerRouter(bridge, opts));
  return app;
}

async function listenOn(app: Express): Promise<{ url: string; close: () => Promise<void> }> {
  return new Promise((resolve) => {
    const server = http.createServer(app);
    server.listen(0, () => {
      const port = (server.address() as AddressInfo).port;
      resolve({
        url: `http://127.0.0.1:${port}`,
        close: () =>
          new Promise((res, rej) =>
            server.close((err) => (err ? rej(err) : res())),
          ),
      });
    });
  });
}

// ─── Layer 1: direct supertest against the router ────────────────────

describe('createWorkerRouter — direct dispatch', () => {
  it('routes trigger and returns the bridge result wrapped in {result}', async () => {
    const bridge = makeFakeBridge();
    bridge.setReturn('trigger', { runId: 'r-123' });
    const app = makeApp(bridge);

    const res = await request(app)
      .post('/internal/pipeline/trigger')
      .send({ args: [{ pipelineId: 'p1', triggeredBy: { userId: 'u1' } }] });

    expect(res.status).toBe(200);
    expect(res.body).toEqual({ result: { runId: 'r-123' } });
    expect(bridge.calls).toHaveLength(1);
    expect(bridge.calls[0]).toEqual({
      method: 'trigger',
      args: [{ pipelineId: 'p1', triggeredBy: { userId: 'u1' } }],
    });
  });

  it('returns {result: null} for getRun miss', async () => {
    const bridge = makeFakeBridge();
    bridge.setReturn('getRun', null);
    const app = makeApp(bridge);

    const res = await request(app)
      .post('/internal/pipeline/getRun')
      .send({ args: ['unknown-run'] });

    expect(res.status).toBe(200);
    expect(res.body).toEqual({ result: null });
  });

  it('serializes void-returning methods as {result: undefined}', async () => {
    const bridge = makeFakeBridge();
    const app = makeApp(bridge);

    const res = await request(app)
      .post('/internal/pipeline/cancelRun')
      .send({ args: ['r1', 'u1'] });

    expect(res.status).toBe(200);
    // JSON elides undefined values; client side parses {} as result=undefined.
    expect(res.body).toEqual({});
  });

  it('returns 500 with {error:{message}} when bridge throws', async () => {
    const bridge = makeFakeBridge();
    bridge.trigger = async () => {
      throw new Error('pipeline def not found');
    };
    const app = makeApp(bridge);

    const res = await request(app)
      .post('/internal/pipeline/trigger')
      .send({ args: [{ pipelineId: 'missing', triggeredBy: { userId: 'u1' } }] });

    expect(res.status).toBe(500);
    expect(res.body).toEqual({ error: { message: 'pipeline def not found' } });
  });

  it('returns 501 when the bridge does not implement an optional method', async () => {
    const bridge = makeFakeBridge();
    // Remove an optional method
    delete (bridge as Partial<PipelineBridge>).trigger;
    const app = makeApp(bridge as PipelineBridge);

    const res = await request(app)
      .post('/internal/pipeline/trigger')
      .send({ args: [{}] });

    expect(res.status).toBe(501);
    expect(res.body.error.message).toContain('does not implement trigger');
  });

  it('handles empty args array', async () => {
    const bridge = makeFakeBridge();
    bridge.setReturn('listActiveRuns', []);
    const app = makeApp(bridge);

    const res = await request(app)
      .post('/internal/pipeline/listActiveRuns')
      .send({});

    expect(res.status).toBe(200);
    expect(res.body).toEqual({ result: [] });
  });

  it('routes inspector verbs through getInspector()', async () => {
    const bridge = makeFakeBridge();
    const app = makeApp(bridge);

    const res = await request(app)
      .post('/internal/pipeline/inspector.paginatePending')
      .send({ args: [{ limit: 10 }] });

    expect(res.status).toBe(200);
    expect(bridge.calls.some((c) => c.method === 'inspector.paginatePending')).toBe(true);
  });

  it('returns 501 for inspector verbs when getInspector is absent', async () => {
    const bridge = makeFakeBridge();
    delete (bridge as Partial<PipelineBridge>).getInspector;
    const app = makeApp(bridge as PipelineBridge);

    const res = await request(app)
      .post('/internal/pipeline/inspector.paginatePending')
      .send({ args: [{}] });

    expect(res.status).toBe(501);
  });
});

// ─── Layer 2: end-to-end HTTPPipelineBridge ↔ worker-router ───────────

describe('worker-router ↔ HTTPPipelineBridge end-to-end', () => {
  it('trigger round-trips through HTTP', async () => {
    const bridge = makeFakeBridge();
    bridge.setReturn('trigger', { runId: 'r-e2e' });
    const { url, close } = await listenOn(makeApp(bridge));
    try {
      const client = new HTTPPipelineBridge({ workerBaseUrl: url });
      const result = await client.trigger({
        pipelineId: 'p1',
        triggeredBy: { userId: 'u1' },
      });
      expect(result).toEqual({ runId: 'r-e2e' });
      expect(bridge.calls[0].method).toBe('trigger');
    } finally {
      await close();
    }
  });

  it('getRun round-trips with null result', async () => {
    const bridge = makeFakeBridge();
    bridge.setReturn('getRun', null);
    const { url, close } = await listenOn(makeApp(bridge));
    try {
      const client = new HTTPPipelineBridge({ workerBaseUrl: url });
      const result = await client.getRun('missing');
      expect(result).toBeNull();
    } finally {
      await close();
    }
  });

  it('getMetrics round-trips a complex object', async () => {
    const bridge = makeFakeBridge();
    bridge.setReturn('getMetrics', { runsStarted: 7, runsActive: 2 });
    const { url, close } = await listenOn(makeApp(bridge));
    try {
      const client = new HTTPPipelineBridge({ workerBaseUrl: url });
      const result = await client.getMetrics();
      expect(result).toEqual({ runsStarted: 7, runsActive: 2 });
    } finally {
      await close();
    }
  });

  it('bridge errors surface as BridgeRpcError on the client', async () => {
    const bridge = makeFakeBridge();
    bridge.trigger = async () => {
      throw new Error('boom');
    };
    const { url, close } = await listenOn(makeApp(bridge));
    try {
      const client = new HTTPPipelineBridge({ workerBaseUrl: url });
      await expect(
        client.trigger({ pipelineId: 'p1', triggeredBy: { userId: 'u1' } }),
      ).rejects.toMatchObject({ name: 'BridgeRpcError', status: 500 });
    } finally {
      await close();
    }
  });
});

// ─── Layer 3: auth middleware ────────────────────────────────────────

describe('worker-router auth middleware', () => {
  const verifyOpts = {
    secret: 'shared-secret',
    allowedServices: ['platform-api'],
  };

  it('accepts a correctly-signed request', async () => {
    const bridge = makeFakeBridge();
    bridge.setReturn('trigger', { runId: 'r-signed' });
    const app = makeApp(bridge, { verifyOpts });
    const { url, close } = await listenOn(app);
    try {
      const client = new HTTPPipelineBridge({
        workerBaseUrl: url,
        signOpts: { secret: 'shared-secret', serviceId: 'platform-api' },
      });
      const result = await client.trigger({
        pipelineId: 'p1',
        triggeredBy: { userId: 'u1' },
      });
      expect(result).toEqual({ runId: 'r-signed' });
    } finally {
      await close();
    }
  });

  it('rejects missing X-Service-Auth with 401', async () => {
    const bridge = makeFakeBridge();
    const app = makeApp(bridge, { verifyOpts });

    const res = await request(app)
      .post('/internal/pipeline/trigger')
      .send({ args: [{ pipelineId: 'p1', triggeredBy: { userId: 'u1' } }] });

    expect(res.status).toBe(401);
    expect(res.body.error.message).toContain('missing X-Service-Auth');
  });

  it('rejects bad signature with 401', async () => {
    const bridge = makeFakeBridge();
    const app = makeApp(bridge, { verifyOpts });

    const body = { args: [{ pipelineId: 'p1', triggeredBy: { userId: 'u1' } }] };
    // Sign with a wrong secret
    const badHeader = signEnvelope(JSON.stringify(body), {
      secret: 'WRONG',
      serviceId: 'platform-api',
    });

    const res = await request(app)
      .post('/internal/pipeline/trigger')
      .set('X-Service-Auth', badHeader)
      .send(body);

    expect(res.status).toBe(401);
    expect(res.body.error.message).toContain('auth:');
  });

  it('rejects unknown service with 401', async () => {
    const bridge = makeFakeBridge();
    const app = makeApp(bridge, { verifyOpts });

    const body = { args: [{ pipelineId: 'p1', triggeredBy: { userId: 'u1' } }] };
    const wrongServiceHeader = signEnvelope(JSON.stringify(body), {
      secret: 'shared-secret',
      serviceId: 'rogue-service',
    });

    const res = await request(app)
      .post('/internal/pipeline/trigger')
      .set('X-Service-Auth', wrongServiceHeader)
      .send(body);

    expect(res.status).toBe(401);
  });
});
