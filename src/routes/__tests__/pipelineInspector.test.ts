// Tests for the pipelineInspector router — limit clamping, cursor
// passthrough, peek 200/404, and the 503 unwired-bridge fallback.
//
// Same rig as pipelineDLQ.test.ts: mount the router on a fresh express
// app, inject a fake bridge via setPipelineBridge() so we can observe the
// QueueInspector method args the route forwards.
//
// Coverage:
//   - GET /pending   default limit=50, payload pass-through
//   - GET /pending   ?limit=500 clamps to 200 (cardinality cap)
//   - GET /pending   ?limit=-99 falls back to default; ?cursor passes through
//   - GET /inflight  wraps the items array as { items: [...] }
//   - GET /summary   pass-through
//   - GET /peek/:id  200 + envelope when found
//   - GET /peek/:id  404 + { error, runId } when not found
//   - 503 fallback   when bridge is null OR getInspector is missing

import express, { type NextFunction, type Request, type Response } from 'express';
import { pipelineInspectorRouter } from '../pipelineInspector';
import { setPipelineBridge, type PipelineBridge } from '../pipelineTriggers';

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
  const finished = new Promise<void>((r) => { resolveFinished = r; });
  const r: MockRes = {
    statusCode: 200,
    headers: {},
    body: undefined,
    ended: false,
    finished,
    status(code) { this.statusCode = code; return this; },
    json(body) { this.body = body; this.ended = true; resolveFinished(); return this; },
    setHeader(name, value) { this.headers[String(name)] = value; return this; },
    getHeader(name) { return this.headers[String(name)]; },
    end(..._args) { this.ended = true; resolveFinished(); return this; },
  };
  return r;
}

async function run(
  app: express.Express,
  method: string,
  url: string,
): Promise<MockRes> {
  const res = mockRes();
  const qIdx = url.indexOf('?');
  const query: Record<string, string> = {};
  if (qIdx >= 0) {
    const params = new URLSearchParams(url.slice(qIdx + 1));
    for (const [k, v] of params.entries()) query[k] = v;
  }
  const req = {
    method: method.toUpperCase(),
    url,
    originalUrl: url,
    path: qIdx >= 0 ? url.slice(0, qIdx) : url,
    headers: { 'content-type': 'application/json' },
    query,
    user: { sub: 'tester' },
    ip: '127.0.0.1',
  } as unknown as Request;
  await new Promise<void>((resolve, reject) => {
    const finalHandler = (err?: unknown): void => {
      if (err) { reject(err); return; }
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
  app.use('/api/pipelines/inspector', pipelineInspectorRouter);
  return app;
}

// ---------------------------------------------------------------------------
// Fake QueueInspector — observable call args.
// ---------------------------------------------------------------------------

interface FakeInspector {
  listPending: jest.Mock;
  listInflight: jest.Mock;
  summary: jest.Mock;
  peekPending: jest.Mock;
}

function makeFakeInspector(overrides: Partial<FakeInspector> = {}): FakeInspector {
  return {
    listPending: jest.fn(async () => ({ items: [], nextCursor: null })),
    listInflight: jest.fn(async () => []),
    summary: jest.fn(async () => ({
      pending: 0,
      inflight: 0,
      failed: 0,
      oldestPendingAgeMs: 0,
    })),
    peekPending: jest.fn(async () => null),
    ...overrides,
  };
}

function wireBridgeWithInspector(inspector: FakeInspector | null): void {
  const bridge: PipelineBridge = {
    getRun: () => null,
    getHistory: () => [],
    resolveApproval: () => undefined,
    listActiveRuns: () => [],
    getMetrics: async () => ({}),
    getPendingApprovals: () => [],
    ...(inspector
      ? { getInspector: () => inspector as unknown as ReturnType<NonNullable<PipelineBridge['getInspector']>> }
      : {}),
  };
  setPipelineBridge(bridge);
}

afterEach(() => {
  setPipelineBridge(null);
});

// ---------------------------------------------------------------------------
// GET /pending
// ---------------------------------------------------------------------------

describe('GET /api/pipelines/inspector/pending', () => {
  test('200 — passes through inspector.listPending payload with default limit=50, no cursor', async () => {
    const inspector = makeFakeInspector({
      listPending: jest.fn(async () => ({
        items: [{ id: 'run-a', body: { runId: 'run-a' }, attemptCount: 0, enqueuedAtMs: 1 }],
        nextCursor: 'next-page',
      })),
    });
    wireBridgeWithInspector(inspector);

    const res = await run(buildApp(), 'GET', '/api/pipelines/inspector/pending');

    expect(res.statusCode).toBe(200);
    expect(inspector.listPending).toHaveBeenCalledTimes(1);
    expect(inspector.listPending).toHaveBeenCalledWith({ limit: 50, cursor: undefined });
    expect(res.body).toEqual({
      items: [{ id: 'run-a', body: { runId: 'run-a' }, attemptCount: 0, enqueuedAtMs: 1 }],
      nextCursor: 'next-page',
    });
  });

  test('?limit=500 clamps to 200 (cardinality cap)', async () => {
    const inspector = makeFakeInspector();
    wireBridgeWithInspector(inspector);

    const res = await run(buildApp(), 'GET', '/api/pipelines/inspector/pending?limit=500');

    expect(res.statusCode).toBe(200);
    expect(inspector.listPending).toHaveBeenCalledWith({ limit: 200, cursor: undefined });
  });

  test('negative limit falls back to default 50; cursor passes through verbatim', async () => {
    const inspector = makeFakeInspector();
    wireBridgeWithInspector(inspector);

    const res = await run(
      buildApp(),
      'GET',
      '/api/pipelines/inspector/pending?limit=-99&cursor=opaque-token',
    );

    expect(res.statusCode).toBe(200);
    expect(inspector.listPending).toHaveBeenCalledWith({ limit: 50, cursor: 'opaque-token' });
  });
});

// ---------------------------------------------------------------------------
// GET /inflight
// ---------------------------------------------------------------------------

describe('GET /api/pipelines/inspector/inflight', () => {
  test('200 — wraps inspector.listInflight array as { items: [...] }', async () => {
    const lease = { id: 'lease-1', body: {}, attemptCount: 1, enqueuedAtMs: 1, leaseExpiresAtMs: 2 };
    const inspector = makeFakeInspector({
      listInflight: jest.fn(async () => [lease]),
    });
    wireBridgeWithInspector(inspector);

    const res = await run(buildApp(), 'GET', '/api/pipelines/inspector/inflight?limit=10');

    expect(res.statusCode).toBe(200);
    expect(inspector.listInflight).toHaveBeenCalledWith({ limit: 10 });
    expect(res.body).toEqual({ items: [lease] });
  });
});

// ---------------------------------------------------------------------------
// GET /summary
// ---------------------------------------------------------------------------

describe('GET /api/pipelines/inspector/summary', () => {
  test('200 — passes through inspector.summary snapshot', async () => {
    const snapshot = { pending: 3, inflight: 1, failed: 2, oldestPendingAgeMs: 12_345 };
    const inspector = makeFakeInspector({
      summary: jest.fn(async () => snapshot),
    });
    wireBridgeWithInspector(inspector);

    const res = await run(buildApp(), 'GET', '/api/pipelines/inspector/summary');

    expect(res.statusCode).toBe(200);
    expect(res.body).toEqual(snapshot);
  });
});

// ---------------------------------------------------------------------------
// GET /peek/:runId
// ---------------------------------------------------------------------------

describe('GET /api/pipelines/inspector/peek/:runId', () => {
  test('200 — returns the envelope when peekPending finds it', async () => {
    const env = { id: 'run-a', body: { runId: 'run-a' }, attemptCount: 0, enqueuedAtMs: 1 };
    const inspector = makeFakeInspector({
      peekPending: jest.fn(async (id: string) => (id === 'run-a' ? env : null)),
    });
    wireBridgeWithInspector(inspector);

    const res = await run(buildApp(), 'GET', '/api/pipelines/inspector/peek/run-a');

    expect(res.statusCode).toBe(200);
    expect(inspector.peekPending).toHaveBeenCalledWith('run-a');
    expect(res.body).toEqual(env);
  });

  test('404 — returns { error, runId } when peekPending returns null', async () => {
    const inspector = makeFakeInspector({
      peekPending: jest.fn(async () => null),
    });
    wireBridgeWithInspector(inspector);

    const res = await run(buildApp(), 'GET', '/api/pipelines/inspector/peek/missing');

    expect(res.statusCode).toBe(404);
    expect(res.body).toEqual({ error: 'run not in pending queue', runId: 'missing' });
  });
});

// ---------------------------------------------------------------------------
// 503 unwired-bridge fallback
// ---------------------------------------------------------------------------

describe('503 fallback when the inspector is unavailable', () => {
  test('GET /pending returns 503 when no bridge is wired', async () => {
    setPipelineBridge(null);

    const res = await run(buildApp(), 'GET', '/api/pipelines/inspector/pending');

    expect(res.statusCode).toBe(503);
    expect(res.body).toEqual({ error: 'pipeline inspector unavailable' });
  });

  test('GET /summary returns 503 when bridge is wired but exposes no getInspector', async () => {
    // Bridge present but missing getInspector (e.g. older version, partial wiring).
    wireBridgeWithInspector(null);

    const res = await run(buildApp(), 'GET', '/api/pipelines/inspector/summary');

    expect(res.statusCode).toBe(503);
    expect(res.body).toEqual({ error: 'pipeline inspector unavailable' });
  });
});
