// Integration tests for /api/pipelines/defs routes.
//
// Mocks definitionsRepo, pipelineDefinitionsCache, and logger at module
// level so we exercise CRUD, validation, publish version bump, and
// webhook secret minting without touching DDB.

import express, { type NextFunction, type Request, type Response } from 'express';

const mockList = jest.fn();
const mockGet = jest.fn();
const mockPut = jest.fn();
const mockDelete = jest.fn();

jest.mock('../../pipeline/definitions-repository', () => ({
  definitionsRepo: {
    list: mockList,
    get: mockGet,
    put: mockPut,
    delete: mockDelete,
  },
}));

jest.mock('../../pipeline/definitions-cache', () => ({
  pipelineDefinitionsCache: {
    refresh: jest.fn().mockResolvedValue(undefined),
  },
}));

jest.mock('../../lib/logger', () => ({
  withContext: () => ({
    info: jest.fn(),
    error: jest.fn(),
    warn: jest.fn(),
  }),
}));

jest.mock('@connorhoehn/service-runtime', () => ({
  generateWebhookSecret: () => 'wh-secret-test',
}));

jest.mock('distributed-core', () => ({
  ...jest.requireActual('distributed-core'),
  generateWebhookSecret: () => 'wh-secret-test',
}));

import { pipelineDefinitionsRouter } from '../pipelineDefinitions';
import { errorHandler } from '../../middleware/error-handler';

interface MockRes {
  statusCode: number;
  body: unknown;
  ended: boolean;
  finished: Promise<void>;
  status(code: number): MockRes;
  json(body: unknown): MockRes;
  send(body?: unknown): MockRes;
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
    json(body) { this.body = body; this.ended = true; resolve(); return this; },
    send(body) { this.body = body; this.ended = true; resolve(); return this; },
    setHeader() { return this; },
    getHeader() { return undefined; },
    end(..._args) { this.ended = true; resolve(); return this; },
  };
  return r;
}

interface RunOpts { body?: unknown; userId?: string }

async function run(
  app: express.Express,
  method: string,
  url: string,
  opts: RunOpts = {},
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
    body: opts.body,
    user: { sub: opts.userId ?? 'tester' },
    params: {},
    ip: '127.0.0.1',
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
  app.use('/api/pipelines/defs', pipelineDefinitionsRouter);
  app.use(errorHandler);
  return app;
}

beforeEach(() => {
  mockList.mockReset();
  mockGet.mockReset();
  mockPut.mockReset().mockResolvedValue(undefined);
  mockDelete.mockReset().mockResolvedValue(undefined);
});

describe('GET /api/pipelines/defs', () => {
  it('returns 200 with pipeline list', async () => {
    mockList.mockResolvedValue([{ id: 'p-1', name: 'My Pipeline' }]);
    const res = await run(buildApp(), 'GET', '/api/pipelines/defs');
    expect(res.statusCode).toBe(200);
    expect((res.body as { pipelines: unknown[] }).pipelines).toHaveLength(1);
    expect(mockList).toHaveBeenCalledWith('tester');
  });

  it('returns 500 on repo failure', async () => {
    mockList.mockRejectedValue(new Error('DDB error'));
    const res = await run(buildApp(), 'GET', '/api/pipelines/defs');
    expect(res.statusCode).toBe(500);
  });
});

describe('GET /api/pipelines/defs/:pipelineId', () => {
  it('returns 200 with the definition', async () => {
    mockGet.mockResolvedValue({ id: 'p-1', name: 'Pipeline' });
    const res = await run(buildApp(), 'GET', '/api/pipelines/defs/p-1');
    expect(res.statusCode).toBe(200);
    expect((res.body as { id: string }).id).toBe('p-1');
  });

  it('returns 404 when not found', async () => {
    mockGet.mockResolvedValue(null);
    const res = await run(buildApp(), 'GET', '/api/pipelines/defs/nope');
    expect(res.statusCode).toBe(404);
  });
});

describe('PUT /api/pipelines/defs/:pipelineId', () => {
  it('returns 200 on successful upsert', async () => {
    const def = { id: 'p-1', name: 'Updated' };
    const res = await run(buildApp(), 'PUT', '/api/pipelines/defs/p-1', {
      body: def,
    });
    expect(res.statusCode).toBe(200);
    expect(mockPut).toHaveBeenCalledWith('tester', def);
  });

  it('rejects body with mismatched id as 400', async () => {
    const res = await run(buildApp(), 'PUT', '/api/pipelines/defs/p-1', {
      body: { id: 'wrong-id' },
    });
    expect(res.statusCode).toBe(400);
  });

  it('rejects missing body as 400', async () => {
    const res = await run(buildApp(), 'PUT', '/api/pipelines/defs/p-1', {
      body: undefined,
    });
    expect(res.statusCode).toBe(400);
  });

  it('mints webhook secret when trigger binding has empty secret', async () => {
    const def = {
      id: 'p-1',
      triggerBinding: { event: 'webhook', webhookPath: '/hook/test', webhookSecret: '' },
    };
    const res = await run(buildApp(), 'PUT', '/api/pipelines/defs/p-1', {
      body: def,
    });
    expect(res.statusCode).toBe(200);
    expect(mockPut).toHaveBeenCalledWith('tester', expect.objectContaining({
      triggerBinding: expect.objectContaining({ webhookSecret: 'wh-secret-test' }),
    }));
  });

  it('preserves existing webhook secret', async () => {
    const def = {
      id: 'p-1',
      triggerBinding: { event: 'webhook', webhookPath: '/hook/test', webhookSecret: 'existing' },
    };
    const res = await run(buildApp(), 'PUT', '/api/pipelines/defs/p-1', {
      body: def,
    });
    expect(res.statusCode).toBe(200);
    expect(mockPut).toHaveBeenCalledWith('tester', expect.objectContaining({
      triggerBinding: expect.objectContaining({ webhookSecret: 'existing' }),
    }));
  });
});

describe('DELETE /api/pipelines/defs/:pipelineId', () => {
  it('returns 204 on successful delete', async () => {
    const res = await run(buildApp(), 'DELETE', '/api/pipelines/defs/p-1');
    expect(res.statusCode).toBe(204);
    expect(mockDelete).toHaveBeenCalledWith('tester', 'p-1');
  });

  it('returns 500 on repo failure', async () => {
    mockDelete.mockRejectedValue(new Error('DDB error'));
    const res = await run(buildApp(), 'DELETE', '/api/pipelines/defs/p-1');
    expect(res.statusCode).toBe(500);
  });
});

describe('GET /api/pipelines/defs/:pipelineId — error path', () => {
  it('returns 500 when repo.get throws', async () => {
    mockGet.mockRejectedValue(new Error('DDB timeout'));
    const res = await run(buildApp(), 'GET', '/api/pipelines/defs/p-1');
    expect(res.statusCode).toBe(500);
    const body = res.body as { error: string };
    expect(body.error).toBe('internal_error');
  });
});

describe('PUT /api/pipelines/defs/:pipelineId — error path', () => {
  it('returns 500 when repo.put throws', async () => {
    mockPut.mockRejectedValue(new Error('DDB write failed'));
    const def = { id: 'p-2', name: 'Failing' };
    const res = await run(buildApp(), 'PUT', '/api/pipelines/defs/p-2', { body: def });
    expect(res.statusCode).toBe(500);
    const body = res.body as { error: string };
    expect(body.error).toBe('internal_error');
  });
});

describe('POST /api/pipelines/defs/:pipelineId/publish', () => {
  it('returns 200 with bumped version and published status', async () => {
    mockGet.mockResolvedValue({ id: 'p-1', version: 2, status: 'draft' });
    const res = await run(buildApp(), 'POST', '/api/pipelines/defs/p-1/publish');
    expect(res.statusCode).toBe(200);
    const body = res.body as { version: number; status: string; publishedVersion: number };
    expect(body.version).toBe(3);
    expect(body.status).toBe('published');
    expect(body.publishedVersion).toBe(3);
  });

  it('defaults version to 1 when not set', async () => {
    mockGet.mockResolvedValue({ id: 'p-1' });
    const res = await run(buildApp(), 'POST', '/api/pipelines/defs/p-1/publish');
    expect(res.statusCode).toBe(200);
    expect((res.body as { version: number }).version).toBe(1);
  });

  it('returns 404 when definition not found', async () => {
    mockGet.mockResolvedValue(null);
    const res = await run(buildApp(), 'POST', '/api/pipelines/defs/nope/publish');
    expect(res.statusCode).toBe(404);
  });

  it('returns 500 when repo.get throws during publish', async () => {
    mockGet.mockRejectedValue(new Error('DDB read error'));
    const res = await run(buildApp(), 'POST', '/api/pipelines/defs/p-1/publish');
    expect(res.statusCode).toBe(500);
    const body = res.body as { error: string };
    expect(body.error).toBe('internal_error');
  });

  it('returns 500 when repo.put throws during publish write', async () => {
    mockGet.mockResolvedValue({ id: 'p-1', version: 1, status: 'draft' });
    mockPut.mockRejectedValue(new Error('DDB write error'));
    const res = await run(buildApp(), 'POST', '/api/pipelines/defs/p-1/publish');
    expect(res.statusCode).toBe(500);
    const body = res.body as { error: string };
    expect(body.error).toBe('internal_error');
  });
});

describe('pokeCacheAfterWrite — refresh error is swallowed', () => {
  it('does not affect the response when cache refresh rejects', async () => {
    // Import the cache mock to make refresh reject
    const { pipelineDefinitionsCache } = await import('../../pipeline/definitions-cache.js');
    (pipelineDefinitionsCache.refresh as jest.Mock).mockRejectedValueOnce(new Error('cache exploded'));

    mockGet.mockResolvedValue({ id: 'p-1', status: 'draft' });
    const res = await run(buildApp(), 'POST', '/api/pipelines/defs/p-1/publish');
    // Response still succeeds; the refresh error is fire-and-forget
    expect(res.statusCode).toBe(200);

    // Give the microtask queue a tick so the catch fires
    await Promise.resolve();
  });
});
