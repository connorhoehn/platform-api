// Tests for the pipelineDLQ router — preview mode + write rate limit.
//
// We don't boot the full app (no supertest dep in this package). Instead we
// mount the router on a fresh express app, drive it with shaped req/res
// stand-ins, and inject a fake DLQ via the existing setPipelineBridge()
// shim so peek/redrive/purge call counts are observable.
//
// Coverage:
//   - POST /redrive { preview:true }      -> dlq.redrive NOT called; summary
//   - POST /redrive { preview:true } w/ unknown id -> notFound entry
//   - POST /purge   { preview:true }      -> dlq.purge NOT called; summary
//   - POST /redrive (no preview)          -> dlq.redrive called once
//   - 11 rapid POSTs against /redrive     -> at least one 429 from the
//                                            shared write rate limit bucket

import express, { type NextFunction, type Request, type Response } from 'express';
import { pipelineDLQRouter } from '../pipelineDLQ';
import { setPipelineBridge, type PipelineBridge } from '../pipelineTriggers';
import { _resetRateLimitMemFallback } from '../../middleware/rateLimit';

// The middleware lazily resolves Redis on first request; force the in-memory
// fallback path so test runs don't need infra and `_resetRateLimitMemFallback`
// can deterministically clear bucket state between cases.
jest.mock('../../lib/redis-client', () => ({
  getRedisClient: async () => null,
}));

// ---------------------------------------------------------------------------
// req/res mocks (mirror the rig in pipelineHealth.test.ts / pipelineMetrics.test.ts)
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
    status(code) { this.statusCode = code; return this; },
    json(body) { this.body = body; this.ended = true; resolveFinished(); return this; },
    setHeader(name, value) { this.headers[String(name)] = value; return this; },
    getHeader(name) { return this.headers[String(name)]; },
    end(..._args) { this.ended = true; resolveFinished(); return this; },
  };
  return r;
}

interface RunOpts {
  body?: unknown;
  userId?: string;
}

async function run(
  app: express.Express,
  method: string,
  url: string,
  opts: RunOpts = {},
): Promise<MockRes> {
  const res = mockRes();
  // Parse the URL's query string into an object — Express's built-in query
  // parser would normally do this, but we feed the mock req directly to the
  // app handler and bypass that step.
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
    body: opts.body,
    user: opts.userId ? { sub: opts.userId } : { sub: 'tester' },
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
  app.use('/api/pipelines/dlq', pipelineDLQRouter);
  return app;
}

// ---------------------------------------------------------------------------
// Fake DLQ + bridge — observable peek / redrive / purge counts.
// ---------------------------------------------------------------------------

interface FakeDLQ {
  peek: jest.Mock;
  redrive: jest.Mock;
  purge: jest.Mock;
  list: jest.Mock;
  size: jest.Mock;
  redriveAll: jest.Mock;
  purgeAll: jest.Mock;
  put: jest.Mock;
}

function makeFakeDLQ(known: Record<string, unknown> = {}): FakeDLQ {
  return {
    peek: jest.fn(async (id: string) => known[id] ?? null),
    redrive: jest.fn(async (ids: readonly string[]) => ({
      redriven: ids.length,
      failed: [],
    })),
    purge: jest.fn(async (ids: readonly string[]) => ({ purged: ids.length })),
    list: jest.fn(async () => ({ items: [], nextCursor: null })),
    size: jest.fn(async () => 0),
    redriveAll: jest.fn(async () => ({ redriven: 0, failed: [] })),
    purgeAll: jest.fn(async () => ({ purged: 0 })),
    put: jest.fn(async () => undefined),
  };
}

function wireBridgeWithDLQ(dlq: FakeDLQ): void {
  const bridge: PipelineBridge = {
    getRun: () => null,
    getHistory: () => [],
    resolveApproval: () => undefined,
    listActiveRuns: () => [],
    getMetrics: async () => ({}),
    getPendingApprovals: () => [],
    // The route only checks typeof === 'function' on getDLQ before calling.
    getDLQ: () => dlq as unknown as ReturnType<NonNullable<PipelineBridge['getDLQ']>>,
  };
  setPipelineBridge(bridge);
}

beforeEach(() => {
  _resetRateLimitMemFallback();
});
afterEach(() => {
  setPipelineBridge(null);
});

// ---------------------------------------------------------------------------
// Preview mode — redrive
// ---------------------------------------------------------------------------

describe('POST /api/pipelines/dlq/redrive — preview mode', () => {
  test('preview:true returns wouldRedrive summary and does NOT call dlq.redrive', async () => {
    const e1 = { envelope: { id: 'e1' }, lastError: 'boom', failedAtMs: 1, totalAttempts: 1, attemptHistory: [] };
    const e2 = { envelope: { id: 'e2' }, lastError: 'boom', failedAtMs: 2, totalAttempts: 1, attemptHistory: [] };
    const dlq = makeFakeDLQ({ e1, e2 });
    wireBridgeWithDLQ(dlq);

    const app = buildApp();
    const res = await run(app, 'POST', '/api/pipelines/dlq/redrive', {
      body: { ids: ['e1', 'e2'], preview: true, resetAttempts: true },
    });

    expect(res.statusCode).toBe(200);
    expect(dlq.redrive).not.toHaveBeenCalled();
    expect(dlq.peek).toHaveBeenCalledTimes(2);

    const body = res.body as {
      preview: true;
      resetAttempts: boolean;
      wouldRedrive: unknown[];
      notFound: { id: string }[];
    };
    expect(body.preview).toBe(true);
    expect(body.resetAttempts).toBe(true);
    // Entries are enriched with `errorKind` (null here — the fixture
    // `lastError: 'boom'` has no `class:` prefix).
    expect(body.wouldRedrive).toEqual([
      { ...e1, errorKind: null },
      { ...e2, errorKind: null },
    ]);
    expect(body.notFound).toEqual([]);
  });

  test('preview:true with an unknown id returns the id under notFound', async () => {
    const e1 = { envelope: { id: 'e1' }, lastError: 'boom', failedAtMs: 1, totalAttempts: 1, attemptHistory: [] };
    const dlq = makeFakeDLQ({ e1 });
    wireBridgeWithDLQ(dlq);

    const app = buildApp();
    const res = await run(app, 'POST', '/api/pipelines/dlq/redrive', {
      body: { ids: ['e1', 'missing-id'], preview: true },
    });

    expect(res.statusCode).toBe(200);
    expect(dlq.redrive).not.toHaveBeenCalled();
    const body = res.body as {
      preview: true;
      wouldRedrive: unknown[];
      notFound: { id: string }[];
    };
    expect(body.wouldRedrive).toEqual([{ ...e1, errorKind: null }]);
    expect(body.notFound).toEqual([{ id: 'missing-id' }]);
  });

  test('happy path (no preview) still calls dlq.redrive exactly once with the supplied ids + resetAttempts', async () => {
    const dlq = makeFakeDLQ();
    wireBridgeWithDLQ(dlq);

    const app = buildApp();
    const res = await run(app, 'POST', '/api/pipelines/dlq/redrive', {
      body: { ids: ['e1', 'e2'], resetAttempts: true },
    });

    expect(res.statusCode).toBe(200);
    expect(dlq.redrive).toHaveBeenCalledTimes(1);
    expect(dlq.redrive).toHaveBeenCalledWith(['e1', 'e2'], { resetAttempts: true });
    expect(res.body).toEqual({ redriven: 2, failed: [] });
  });
});

// ---------------------------------------------------------------------------
// Preview mode — purge
// ---------------------------------------------------------------------------

describe('POST /api/pipelines/dlq/purge — preview mode', () => {
  test('preview:true returns wouldPurge summary and does NOT call dlq.purge', async () => {
    const e1 = { envelope: { id: 'e1' }, lastError: 'boom', failedAtMs: 1, totalAttempts: 1, attemptHistory: [] };
    const dlq = makeFakeDLQ({ e1 });
    wireBridgeWithDLQ(dlq);

    const app = buildApp();
    const res = await run(app, 'POST', '/api/pipelines/dlq/purge', {
      body: { ids: ['e1', 'missing'], preview: true },
    });

    expect(res.statusCode).toBe(200);
    expect(dlq.purge).not.toHaveBeenCalled();
    const body = res.body as {
      preview: true;
      wouldPurge: unknown[];
      notFound: { id: string }[];
    };
    expect(body.wouldPurge).toEqual([{ ...e1, errorKind: null }]);
    expect(body.notFound).toEqual([{ id: 'missing' }]);
  });
});

// ---------------------------------------------------------------------------
// Write rate limit — shared 10/min/user bucket across both POST routes.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// errorKind filter + enrichment
// ---------------------------------------------------------------------------

describe('GET /api/pipelines/dlq — errorKind filter + enrichment', () => {
  test('?errorKind=NetworkError translates to a failureKindMatches RegExp anchored to ^NetworkError:', async () => {
    const dlq = makeFakeDLQ();
    // Override list to capture the options the route passed through.
    let captured: { failureKindMatches?: RegExp } | null = null;
    dlq.list = jest.fn(async (opts: { failureKindMatches?: RegExp } = {}) => {
      captured = opts;
      return { items: [], nextCursor: null };
    });
    wireBridgeWithDLQ(dlq);

    const app = buildApp();
    const res = await run(app, 'GET', '/api/pipelines/dlq?errorKind=NetworkError');

    expect(res.statusCode).toBe(200);
    expect(captured).not.toBeNull();
    expect(captured!.failureKindMatches).toBeInstanceOf(RegExp);
    expect(captured!.failureKindMatches!.source).toBe('^NetworkError:');
    // Sanity: the regex matches what bootstrap actually stores.
    expect(captured!.failureKindMatches!.test('NetworkError: ECONNREFUSED')).toBe(true);
    expect(captured!.failureKindMatches!.test('Error: something else')).toBe(false);
  });

  test('rejects malformed errorKind with 400 (operator typo must not silently drop the filter)', async () => {
    const dlq = makeFakeDLQ();
    wireBridgeWithDLQ(dlq);

    const app = buildApp();
    const res = await run(app, 'GET', '/api/pipelines/dlq?errorKind=bad%20kind');

    expect(res.statusCode).toBe(400);
    expect(dlq.list).not.toHaveBeenCalled();
    const body = res.body as { error: string };
    expect(body.error).toMatch(/errorKind/);
  });

  test('list response items carry derived errorKind (parsed from the lastError prefix)', async () => {
    const dlq = makeFakeDLQ();
    dlq.list = jest.fn(async () => ({
      items: [
        { envelope: { id: 'a' }, lastError: 'NetworkError: ECONNREFUSED', failedAtMs: 1, totalAttempts: 1, attemptHistory: [] },
        { envelope: { id: 'b' }, lastError: 'TimeoutError: peer slow',     failedAtMs: 2, totalAttempts: 1, attemptHistory: [] },
        { envelope: { id: 'c' }, lastError: 'no prefix here',              failedAtMs: 3, totalAttempts: 1, attemptHistory: [] },
      ],
      nextCursor: null,
    }));
    wireBridgeWithDLQ(dlq);

    const app = buildApp();
    const res = await run(app, 'GET', '/api/pipelines/dlq');

    expect(res.statusCode).toBe(200);
    const body = res.body as { items: { errorKind: string | null }[] };
    expect(body.items.map((i) => i.errorKind)).toEqual([
      'NetworkError',
      'TimeoutError',
      null,
    ]);
  });

  test('peek response is enriched with errorKind too', async () => {
    const e1 = { envelope: { id: 'e1' }, lastError: 'RegistryConflictError: dup', failedAtMs: 1, totalAttempts: 1, attemptHistory: [] };
    const dlq = makeFakeDLQ({ e1 });
    wireBridgeWithDLQ(dlq);

    const app = buildApp();
    const res = await run(app, 'GET', '/api/pipelines/dlq/peek/e1');

    expect(res.statusCode).toBe(200);
    const body = res.body as { errorKind: string | null; lastError: string };
    expect(body.errorKind).toBe('RegistryConflictError');
    expect(body.lastError).toBe('RegistryConflictError: dup');
  });
});

describe('pipeline:write rate limit on DLQ POST routes', () => {
  test('11 rapid POSTs against /redrive triggers at least one 429 with Retry-After', async () => {
    const dlq = makeFakeDLQ();
    wireBridgeWithDLQ(dlq);

    const app = buildApp();

    const results: MockRes[] = [];
    for (let i = 0; i < 11; i++) {
      // eslint-disable-next-line no-await-in-loop
      results.push(await run(app, 'POST', '/api/pipelines/dlq/redrive', {
        body: { ids: ['e1'], preview: true },
        userId: 'rl-user',
      }));
    }

    const allowed = results.filter((r) => r.statusCode === 200);
    const blocked = results.filter((r) => r.statusCode === 429);
    expect(allowed.length).toBe(10);
    expect(blocked.length).toBe(1);
    expect(blocked[0].headers['Retry-After']).toBeDefined();
    const body = blocked[0].body as { status: number; title: string; detail: string };
    expect(body.status).toBe(429);
    expect(body.title).toBe('Too Many Requests');
    expect(body.detail).toMatch(/pipeline:write/);
  });

  test('budget is shared between /redrive and /purge for the same user', async () => {
    const dlq = makeFakeDLQ();
    wireBridgeWithDLQ(dlq);

    const app = buildApp();

    // Burn 10 on redrive...
    for (let i = 0; i < 10; i++) {
      // eslint-disable-next-line no-await-in-loop
      await run(app, 'POST', '/api/pipelines/dlq/redrive', {
        body: { ids: ['e1'], preview: true },
        userId: 'shared-user',
      });
    }
    // ...then a purge from the same user must 429 because the bucket is shared.
    const purgeRes = await run(app, 'POST', '/api/pipelines/dlq/purge', {
      body: { ids: ['e1'], preview: true },
      userId: 'shared-user',
    });
    expect(purgeRes.statusCode).toBe(429);
  });
});
