// social-api/src/routes/__tests__/pipelineAudit.test.ts
//
// Unit tests for hub#2225 audit query routes:
//   GET /api/pipelines/:pipelineId/runs/:runId/audit
//   GET /api/pipelines/audit/search

// Loop 29c: run access has its own suite (runAccess.test.ts); these tests
// exercise the route behaviour behind it, so the gate passes here.
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

// --- module-level mocks (must precede route import) -------------------------

const mockListByRun = jest.fn();
const mockSearch = jest.fn();

jest.mock('../../pipeline/audit-repository', () => ({
  auditRepo: {
    listByRun: (...args: unknown[]) => mockListByRun(...args),
    search: (...args: unknown[]) => mockSearch(...args),
  },
}));

jest.mock('../../lib/redis-client', () => ({
  getRedisClient: async () => null,
}));

const mockLogger = {
  info: jest.fn(),
  warn: jest.fn(),
  error: jest.fn(),
  child: () => mockLogger,
};
jest.mock('../../lib/logger', () => ({
  __esModule: true,
  default: mockLogger,
  withContext: () => mockLogger,
}));

import {
  pipelineRunAuditRouter,
  pipelineAuditSearchRouter,
} from '../pipelineAudit';
import { errorHandler } from '../../middleware/error-handler';

// ---------------------------------------------------------------------------
// Test rig — mirrors pipelineResume.test.ts
// ---------------------------------------------------------------------------

interface MockRes {
  statusCode: number;
  body: unknown;
  ended: boolean;
  finished: Promise<void>;
  status(code: number): MockRes;
  json(body: unknown): MockRes;
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
    json(b) { this.body = b; this.ended = true; resolve(); return this; },
    setHeader() { return this; },
    getHeader() { return undefined; },
    end(..._args) { this.ended = true; resolve(); return this; },
  };
  return r;
}

async function dispatch(
  app: express.Express,
  method: string,
  url: string,
): Promise<MockRes> {
  const res = mockRes();
  const [path, queryStr] = url.split('?');
  const query: Record<string, string> = {};
  if (queryStr) {
    for (const pair of queryStr.split('&')) {
      const [k, v] = pair.split('=');
      if (k) query[decodeURIComponent(k)] = v ? decodeURIComponent(v) : '';
    }
  }
  const req = {
    method: method.toUpperCase(),
    url,
    originalUrl: url,
    path,
    headers: {},
    query,
    body: {},
    user: { sub: 'user-1' },
    params: {},
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
  app.use((req, _res, next) => {
    if (!(req as Request & { user?: unknown }).user) {
      (req as Request & { user?: { sub: string } }).user = { sub: 'user-1' };
    }
    next();
  });
  // Mirror routes/index.ts mounts.
  app.use('/pipelines/audit/search', pipelineAuditSearchRouter);
  app.use('/pipelines/:pipelineId/runs/:runId/audit', pipelineRunAuditRouter);
  app.use(errorHandler);
  return app;
}

const app = buildApp();

beforeEach(() => {
  jest.clearAllMocks();
});

// ---------------------------------------------------------------------------
// Per-run audit listing
// ---------------------------------------------------------------------------

describe('GET /pipelines/:pipelineId/runs/:runId/audit', () => {
  test('200 with items + nextCursor on the happy path', async () => {
    mockListByRun.mockResolvedValueOnce({
      items: [
        { auditId: 'a1', timestamp: '2026-05-06T00:00:01Z', action: 'pipeline.step.started', actorUserId: 'u', pipelineId: 'p1', runId: 'r1', ttlEpoch: 0 },
        { auditId: 'a2', timestamp: '2026-05-06T00:00:00Z', action: 'pipeline.run.started', actorUserId: 'u', pipelineId: 'p1', runId: 'r1', ttlEpoch: 0 },
      ],
      nextCursor: 'next-token',
    });

    const res = await dispatch(app, 'GET', '/pipelines/p1/runs/r1/audit?limit=2');

    expect(res.statusCode).toBe(200);
    const body = res.body as { items: unknown[]; nextCursor?: string };
    expect(body.items.length).toBe(2);
    expect(body.nextCursor).toBe('next-token');
    expect(mockListByRun).toHaveBeenCalledWith('r1', expect.objectContaining({ limit: 2 }));
  });

  test('passes ?since/?until/?actions/?cursor through to the repo', async () => {
    mockListByRun.mockResolvedValueOnce({ items: [] });
    await dispatch(
      app,
      'GET',
      '/pipelines/p1/runs/r1/audit?since=2026-05-06T00:00:00Z&until=2026-05-06T01:00:00Z&actions=pipeline.step.started,pipeline.step.completed&cursor=opaque',
    );
    expect(mockListByRun).toHaveBeenCalledWith('r1', expect.objectContaining({
      since: '2026-05-06T00:00:00Z',
      until: '2026-05-06T01:00:00Z',
      actions: ['pipeline.step.started', 'pipeline.step.completed'],
      cursor: 'opaque',
    }));
  });

  test('400 when ?since is not an ISO 8601 string', async () => {
    const res = await dispatch(app, 'GET', '/pipelines/p1/runs/r1/audit?since=not-a-date');
    expect(res.statusCode).toBe(400);
    const body = res.body as { error: string };
    expect(body.error).toMatch(/since/);
  });

  test('400 when ?limit is non-numeric', async () => {
    const res = await dispatch(app, 'GET', '/pipelines/p1/runs/r1/audit?limit=abc');
    expect(res.statusCode).toBe(400);
    const body = res.body as { error: string };
    expect(body.error).toMatch(/limit/);
  });

  test('200 with empty array when run has no audit rows (predates Wave 2)', async () => {
    mockListByRun.mockResolvedValueOnce({ items: [] });
    const res = await dispatch(app, 'GET', '/pipelines/p1/runs/r1/audit');
    expect(res.statusCode).toBe(200);
    const body = res.body as { items: unknown[]; nextCursor?: string };
    expect(body.items).toEqual([]);
    expect(body.nextCursor).toBeUndefined();
  });

  test('clamps ?limit above 1000 down to 1000', async () => {
    mockListByRun.mockResolvedValueOnce({ items: [] });
    await dispatch(app, 'GET', '/pipelines/p1/runs/r1/audit?limit=5000');
    expect(mockListByRun).toHaveBeenCalledWith('r1', expect.objectContaining({ limit: 1000 }));
  });
});

// ---------------------------------------------------------------------------
// Cross-run audit search
// ---------------------------------------------------------------------------

describe('GET /pipelines/audit/search', () => {
  test('400 when no filter is supplied', async () => {
    const res = await dispatch(app, 'GET', '/pipelines/audit/search');
    expect(res.statusCode).toBe(400);
    const body = res.body as { error: string };
    expect(body.error).toMatch(/actor.*pipelineId|pipelineId.*actor/);
  });

  test('200 with items when ?actor is supplied', async () => {
    mockSearch.mockResolvedValueOnce({ items: [{ auditId: 'a1' }] });
    const res = await dispatch(app, 'GET', '/pipelines/audit/search?actor=alice@example.com&limit=10');
    expect(res.statusCode).toBe(200);
    expect(mockSearch).toHaveBeenCalledWith(expect.objectContaining({
      actor: 'user-1', // Loop 29c: another actor is pinned to the caller unless the pipeline is theirs
      limit: 10,
    }));
  });

  test('200 with items when ?pipelineId is supplied', async () => {
    mockSearch.mockResolvedValueOnce({ items: [] });
    const res = await dispatch(app, 'GET', '/pipelines/audit/search?pipelineId=p1');
    expect(res.statusCode).toBe(200);
    expect(mockSearch).toHaveBeenCalledWith(expect.objectContaining({ pipelineId: 'p1' }));
  });

  test('accepts singular ?action= alias', async () => {
    mockSearch.mockResolvedValueOnce({ items: [] });
    await dispatch(app, 'GET', '/pipelines/audit/search?actor=alice&action=pipeline.trigger');
    expect(mockSearch).toHaveBeenCalledWith(expect.objectContaining({
      actor: 'user-1',
      actions: ['pipeline.trigger'],
    }));
  });

  test('400 when ?since is malformed', async () => {
    const res = await dispatch(app, 'GET', '/pipelines/audit/search?actor=u&since=garbage');
    expect(res.statusCode).toBe(400);
  });
});
