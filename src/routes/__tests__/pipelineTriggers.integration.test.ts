import express, { type NextFunction, type Request, type Response } from 'express';

jest.mock('../../lib/redis-client', () => ({
  getRedisClient: async () => null,
}));

jest.mock('../../lib/logger', () => {
  const stub: Record<string, jest.Mock> = {
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
    debug: jest.fn(),
    child: jest.fn(),
    withContext: jest.fn(),
  };
  stub.child.mockReturnValue(stub);
  stub.withContext.mockReturnValue(stub);
  (globalThis as unknown as { __integrationMockLog: typeof stub }).__integrationMockLog = stub;
  return {
    __esModule: true,
    default: stub,
    withContext: jest.fn(() => stub),
  };
});

jest.mock('../../pipeline/audit-repository', () => ({
  auditRepo: {
    record: jest.fn(() => Promise.resolve()),
  },
}));

jest.mock('../../pipeline/definitions-repository', () => {
  const store = new Map<string, unknown>();
  const k = (userId: string, pipelineId: string): string =>
    `${userId}|${pipelineId}`;
  const repo = {
    get: jest.fn(async (userId: string, pipelineId: string) => {
      return store.get(k(userId, pipelineId)) ?? null;
    }),
    list: jest.fn(async (userId: string) => {
      const out: unknown[] = [];
      for (const [key, value] of store.entries()) {
        if (key.startsWith(`${userId}|`)) out.push(value);
      }
      return out;
    }),
    listAll: jest.fn(async () => Array.from(store.values())),
    put: jest.fn(async (userId: string, def: { id: string }) => {
      store.set(k(userId, def.id), def);
    }),
    delete: jest.fn(async (userId: string, pipelineId: string) => {
      store.delete(k(userId, pipelineId));
    }),
    __resetForTests: () => store.clear(),
  };
  (globalThis as unknown as { __integrationDefRepo: typeof repo }).__integrationDefRepo = repo;
  return { definitionsRepo: repo };
});

import { errorHandler } from '../../middleware/error-handler';
import { _resetIdempotencyMemFallback } from '../../middleware/idempotency';
import { _resetRateLimitMemFallback } from '../../middleware/rateLimit';
import {
  pipelineTriggersRouter,
  pipelineActiveRunsRouter,
  pipelineCancelRouter,
  setPipelineBridge,
  stubRunStore,
  type PipelineBridge,
} from '../pipelineTriggers';
import { pipelineWebhooksRouter } from '../pipelineWebhooks';
import { pipelineDefinitionsRouter } from '../pipelineDefinitions';
import { pipelineDefinitionsCache } from '../../pipeline/definitions-cache';
import {
  computeSignature,
  generateWebhookSecret,
  SIGNATURE_HEADER,
} from 'distributed-core';
import { auditRepo } from '../../pipeline/audit-repository';

const mockLog = (globalThis as unknown as {
  __integrationMockLog: {
    info: jest.Mock;
    warn: jest.Mock;
    error: jest.Mock;
    debug: jest.Mock;
    child: jest.Mock;
  };
}).__integrationMockLog;

const mockedAuditRecord = auditRepo.record as jest.Mock;

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
  opts: { body?: unknown; headers?: Record<string, string>; userId?: string } = {},
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
    user: { sub: opts.userId ?? 'user-1' },
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

function buildApp(): express.Express {
  const app = express();
  app.use(express.json());
  app.use((req, _res, next) => {
    if (!(req as Request & { user?: unknown }).user) {
      (req as Request & { user?: { sub: string } }).user = { sub: 'user-1' };
    }
    next();
  });
  app.use('/pipelines/runs/active', pipelineActiveRunsRouter);
  app.use('/pipelines/:pipelineId/runs', pipelineTriggersRouter);
  app.use('/pipelines/:runId/cancel', pipelineCancelRouter);
  app.use('/hooks/pipeline', pipelineWebhooksRouter);
  app.use('/defs', pipelineDefinitionsRouter);
  app.use(errorHandler);
  return app;
}

afterEach(() => {
  setPipelineBridge(null);
  stubRunStore.__resetForTests();
  _resetIdempotencyMemFallback();
  _resetRateLimitMemFallback();
  mockLog.info.mockClear();
  mockLog.warn.mockClear();
  mockLog.error.mockClear();
  mockLog.debug.mockClear();
  mockedAuditRecord.mockClear();
  mockedAuditRecord.mockImplementation(() => Promise.resolve());
});

describe('Pipeline bridge unavailable (bridge === null)', () => {
  test('POST trigger returns 202 with stub runId when bridge is null', async () => {
    setPipelineBridge(null);
    const app = buildApp();
    const res = await run(app, 'POST', '/pipelines/p1/runs', { body: {} });
    expect(res.statusCode).toBe(202);
    const body = res.body as { runId: string; pipelineId: string };
    expect(body.pipelineId).toBe('p1');
    expect(body.runId).toMatch(/^[0-9a-f-]{36}$/);
  });

  test('GET run snapshot returns 404 when bridge is null', async () => {
    setPipelineBridge(null);
    const app = buildApp();
    const res = await run(app, 'GET', '/pipelines/p1/runs/r1');
    expect(res.statusCode).toBe(404);
    expect((res.body as { error: string }).error).toBe('run not found');
  });

  test('GET history returns [] when bridge is null', async () => {
    setPipelineBridge(null);
    const app = buildApp();
    const res = await run(app, 'GET', '/pipelines/p1/runs/r1/history');
    expect(res.statusCode).toBe(200);
    expect(res.body).toEqual([]);
  });

  test('GET active runs returns empty array from stub store when bridge is null', async () => {
    setPipelineBridge(null);
    const app = buildApp();
    const res = await run(app, 'GET', '/pipelines/runs/active');
    expect(res.statusCode).toBe(200);
    expect((res.body as { runs: unknown[] }).runs).toEqual([]);
  });

  test('webhook POST returns 503 when bridge is null', async () => {
    setPipelineBridge(null);
    const app = buildApp();
    const res = await run(app, 'POST', '/hooks/pipeline/some-hook', { body: { x: 1 } });
    expect(res.statusCode).toBe(503);
    const body = res.body as { accepted: boolean; error: string };
    expect(body.accepted).toBe(false);
    expect(body.error).toMatch(/unavailable/i);
  });
});

describe('Rate limiter fallback (degraded mode)', () => {
  test('requests proceed when rate limiter store throws', async () => {
    setPipelineBridge(null);
    const app = buildApp();
    const res = await run(app, 'POST', '/pipelines/p1/runs', { body: {} });
    expect(res.statusCode).toBe(202);
  });

  test('multiple requests succeed in degraded mode without Redis', async () => {
    setPipelineBridge(null);
    const app = buildApp();
    for (let i = 0; i < 5; i++) {
      const res = await run(app, 'POST', '/pipelines/p1/runs', { body: {} });
      expect(res.statusCode).toBe(202);
    }
  });
});

describe('Audit write failures logged at warn', () => {
  test('request succeeds and warning is logged when audit repository throws', async () => {
    mockedAuditRecord.mockImplementation(() =>
      Promise.reject(new Error('DynamoDB connection refused')),
    );

    setPipelineBridge(null);
    const app = buildApp();
    const res = await run(app, 'POST', '/pipelines/p1/runs', { body: {} });
    expect(res.statusCode).toBe(202);
    expect((res.body as { runId: string }).runId).toMatch(/^[0-9a-f-]{36}$/);

    await new Promise((r) => setImmediate(r));
    expect(mockLog.warn).toHaveBeenCalledWith(
      expect.objectContaining({ err: 'DynamoDB connection refused' }),
      'audit write failed',
    );
  });

  test('webhook trigger succeeds and logs error when audit throws', async () => {
    mockedAuditRecord.mockImplementation(() =>
      Promise.reject(new Error('audit table unavailable')),
    );

    setPipelineBridge({
      trigger: async () => ({ runId: 'wh-run-1' }),
      getRun: async () => null,
      getHistory: async () => [],
      resolveApproval: async () => {},
    });
    const app = buildApp();
    const res = await run(app, 'POST', '/hooks/pipeline/audit-fail', {
      body: { test: true },
    });
    expect(res.statusCode).toBe(202);
    expect((res.body as { runId: string }).runId).toBe('wh-run-1');

    await new Promise((r) => setImmediate(r));
    expect(mockLog.error).toHaveBeenCalledWith(
      expect.objectContaining({ err: 'audit table unavailable' }),
      'audit write failed',
    );
  });
});

describe('Webhook trigger end-to-end with signature verification', () => {
  beforeEach(async () => {
    (globalThis as unknown as {
      __integrationDefRepo: { __resetForTests: () => void };
    }).__integrationDefRepo.__resetForTests();
    pipelineDefinitionsCache.__setSnapshotForTests([]);
  });

  async function seedDefinition(webhookPath: string, secret: string): Promise<void> {
    const app = express();
    app.use(express.json());
    app.use((req, _res, next) => {
      (req as unknown as { user: { sub: string } }).user = { sub: 'test-user' };
      next();
    });
    app.use('/defs', pipelineDefinitionsRouter);

    const pipelineId = `pipe-${webhookPath}`;
    const def = {
      id: pipelineId,
      name: 'sig integration test',
      version: 1,
      status: 'draft',
      nodes: [],
      edges: [],
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      createdBy: 'test-user',
      triggerBinding: {
        event: 'webhook',
        webhookPath,
        webhookSecret: secret,
      },
    };

    await run(app, 'PUT', `/defs/${pipelineId}`, { body: def });
    await pipelineDefinitionsCache.refresh();
  }

  test('valid HMAC signature triggers the pipeline and returns 202', async () => {
    const secret = generateWebhookSecret();
    await seedDefinition('sig-valid', secret);

    setPipelineBridge({
      trigger: async () => ({ runId: 'sig-run-1' }),
      getRun: async () => null,
      getHistory: async () => [],
      resolveApproval: async () => {},
    });

    const rawBody = Buffer.from(JSON.stringify({ event: 'deploy' }), 'utf8');
    const sig = computeSignature(secret, rawBody);

    const app = buildApp();
    const res = await run(app, 'POST', '/hooks/pipeline/sig-valid', {
      body: rawBody,
      // Signed traffic also passes the hub#1260 replay-window guard, which
      // requires a fresh x-webhook-timestamp alongside the HMAC header.
      headers: { [SIGNATURE_HEADER]: sig, 'x-webhook-timestamp': new Date().toISOString() },
    });

    expect(res.statusCode).toBe(202);
    const body = res.body as { accepted: boolean; runId: string; webhookPath: string };
    expect(body.accepted).toBe(true);
    expect(body.runId).toBe('sig-run-1');
    expect(body.webhookPath).toBe('sig-valid');
  });

  test('invalid HMAC signature returns 401', async () => {
    const secret = generateWebhookSecret();
    await seedDefinition('sig-invalid', secret);

    setPipelineBridge({
      trigger: async () => ({ runId: 'should-not-reach' }),
      getRun: async () => null,
      getHistory: async () => [],
      resolveApproval: async () => {},
    });

    const rawBody = Buffer.from(JSON.stringify({ event: 'deploy' }), 'utf8');
    const wrongSig = computeSignature('wrong-secret-' + 'x'.repeat(50), rawBody);

    const app = buildApp();
    const res = await run(app, 'POST', '/hooks/pipeline/sig-invalid', {
      body: rawBody,
      headers: { [SIGNATURE_HEADER]: wrongSig },
    });

    expect(res.statusCode).toBe(401);
    const body = res.body as { title: string; status: number; detail: string };
    expect(body.title).toBe('Invalid webhook signature');
    expect(body.status).toBe(401);
    expect(body.detail).toMatch(/did not match/i);
  });

  test('missing signature header when secret is configured returns 401', async () => {
    const secret = generateWebhookSecret();
    await seedDefinition('sig-missing-hdr', secret);

    setPipelineBridge({
      trigger: async () => ({ runId: 'should-not-reach' }),
      getRun: async () => null,
      getHistory: async () => [],
      resolveApproval: async () => {},
    });

    const rawBody = Buffer.from(JSON.stringify({ event: 'deploy' }), 'utf8');

    const app = buildApp();
    const res = await run(app, 'POST', '/hooks/pipeline/sig-missing-hdr', {
      body: rawBody,
    });

    expect(res.statusCode).toBe(401);
    const body = res.body as { detail: string };
    expect(body.detail).toMatch(/required/i);
  });
});

describe('Idempotency cache conflict (409)', () => {
  test('same idempotency key with different body returns 409', async () => {
    setPipelineBridge(null);
    const app = buildApp();
    const headers = { 'Idempotency-Key': 'conflict-key-1' };

    const r1 = await run(app, 'POST', '/pipelines/p1/runs', {
      body: { payload: 'first' },
      headers,
    });
    expect(r1.statusCode).toBe(202);

    await new Promise((r) => setImmediate(r));

    const r2 = await run(app, 'POST', '/pipelines/p1/runs', {
      body: { payload: 'different' },
      headers,
    });
    expect(r2.statusCode).toBe(409);
    const body = r2.body as { error: string; message: string };
    expect(body.error).toBe('idempotency_key_conflict');
    expect(body.message).toMatch(/different request body/);
  });

  test('same idempotency key with same body replays the original response', async () => {
    setPipelineBridge(null);
    const app = buildApp();
    const headers = { 'Idempotency-Key': 'replay-key-1' };

    const r1 = await run(app, 'POST', '/pipelines/p1/runs', {
      body: { payload: 'same' },
      headers,
    });
    expect(r1.statusCode).toBe(202);
    const runId = (r1.body as { runId: string }).runId;

    await new Promise((r) => setImmediate(r));

    const r2 = await run(app, 'POST', '/pipelines/p1/runs', {
      body: { payload: 'same' },
      headers,
    });
    expect(r2.statusCode).toBe(202);
    expect((r2.body as { runId: string }).runId).toBe(runId);
    expect(r2.headers['X-Idempotent-Replay']).toBe('true');
  });
});
