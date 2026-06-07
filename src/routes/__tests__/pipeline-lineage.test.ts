// platform-api/src/routes/__tests__/pipeline-lineage.test.ts
//
// Tests for GET /api/pipeline/runs/:runId/image-lineage

import express, { type NextFunction, type Request, type Response } from 'express';

// --- module-level mocks (must precede route import) -------------------------

const mockRunRepoGetRunImageLineage = jest.fn();
const mockRunRepoGet = jest.fn();

jest.mock('../../repositories/PipelineRunRepository', () => ({
  PipelineRunRepository: jest.fn().mockImplementation(() => ({
    get: mockRunRepoGet,
    getRunImageLineage: mockRunRepoGetRunImageLineage,
  })),
}));

jest.mock('../../lib/ddb-client', () => ({
  getDynamoClient: jest.fn().mockReturnValue({}),
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

// Mock requireAuth so tests don't need a real JWT
jest.mock('../../middleware/auth', () => ({
  requireAuth: (_req: Request, _res: Response, next: NextFunction) => next(),
}));

import { pipelineLineageRouter } from '../pipeline-lineage';
import { errorHandler } from '../../middleware/error-handler';

// ---------------------------------------------------------------------------
// Test rig (mirrors pipelineResume.test.ts)
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
  url: string,
  opts: { userId?: string } = {},
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
    method: 'GET',
    url,
    originalUrl: url,
    path,
    headers: { 'content-type': 'application/json' },
    query,
    body: {},
    user: { sub: opts.userId ?? 'user-1' },
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
  app.use('/pipeline/runs/:runId/image-lineage', pipelineLineageRouter);
  app.use(errorHandler);
  return app;
}

const app = buildApp();

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const sourceEntry = {
  imageId: 'img-source-1',
  producedByStepId: 'node-source',
  producedByNodeKind: 'image-source',
  parentImageId: undefined,
  width: 1568,
  height: 1024,
  mimeType: 'image/jpeg',
  at: '2026-01-01T00:01:00.000Z',
};

const cropEntry1 = {
  imageId: 'img-crop-1',
  producedByStepId: 'node-crop',
  producedByNodeKind: 'image:crop',
  parentImageId: 'img-source-1',
  width: 400,
  height: 300,
  mimeType: 'image/png',
  bbox: { x: 10, y: 20, width: 400, height: 300 },
  at: '2026-01-01T00:01:05.000Z',
};

const cropEntry2 = {
  imageId: 'img-crop-2',
  producedByStepId: 'node-crop',
  producedByNodeKind: 'image:crop',
  parentImageId: 'img-source-1',
  width: 200,
  height: 150,
  mimeType: 'image/png',
  bbox: { x: 50, y: 60, width: 200, height: 150 },
  at: '2026-01-01T00:01:06.000Z',
};

const cropEntry3 = {
  imageId: 'img-crop-3',
  producedByStepId: 'node-crop',
  producedByNodeKind: 'image:crop',
  parentImageId: 'img-source-1',
  width: 100,
  height: 80,
  mimeType: 'image/png',
  bbox: { x: 100, y: 110, width: 100, height: 80 },
  at: '2026-01-01T00:01:07.000Z',
};

beforeEach(() => {
  jest.clearAllMocks();
});

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('GET /pipeline/runs/:runId/image-lineage', () => {
  test('400 when pipelineId query param is missing', async () => {
    const res = await dispatch(app, '/pipeline/runs/run-1/image-lineage');
    expect(res.statusCode).toBe(400);
    const body = res.body as { error: string };
    expect(body.error).toMatch(/pipelineId/);
  });

  test('404 when the run does not exist', async () => {
    mockRunRepoGetRunImageLineage.mockResolvedValueOnce(null);
    const res = await dispatch(app, '/pipeline/runs/missing-run/image-lineage?pipelineId=p1');
    expect(res.statusCode).toBe(404);
    const body = res.body as { error: string };
    expect(body.error).toMatch(/not found/i);
  });

  test('200 with empty entries for a run with no image-producing steps', async () => {
    mockRunRepoGetRunImageLineage.mockResolvedValueOnce([]);
    const res = await dispatch(app, '/pipeline/runs/run-empty/image-lineage?pipelineId=p1');
    expect(res.statusCode).toBe(200);
    const body = res.body as { entries: unknown[] };
    expect(body.entries).toEqual([]);
  });

  test('200 with source + 3 crop entries, parent edges intact', async () => {
    const fourEntries = [sourceEntry, cropEntry1, cropEntry2, cropEntry3];
    mockRunRepoGetRunImageLineage.mockResolvedValueOnce(fourEntries);

    const res = await dispatch(app, '/pipeline/runs/run-vision/image-lineage?pipelineId=p1');
    expect(res.statusCode).toBe(200);

    const body = res.body as { entries: typeof fourEntries };
    expect(body.entries).toHaveLength(4);

    // Source node has no parent
    const source = body.entries.find((e) => e.producedByNodeKind === 'image-source');
    expect(source).toBeDefined();
    expect(source?.parentImageId).toBeUndefined();
    expect(source?.imageId).toBe('img-source-1');

    // All crop entries point back to the source
    const crops = body.entries.filter((e) => e.producedByNodeKind === 'image:crop');
    expect(crops).toHaveLength(3);
    for (const crop of crops) {
      expect(crop.parentImageId).toBe('img-source-1');
      expect((crop as Record<string, unknown>)['bbox']).toBeDefined();
    }

    // Verify the repo was called with the correct args
    expect(mockRunRepoGetRunImageLineage).toHaveBeenCalledWith('p1', 'run-vision');
  });
});
