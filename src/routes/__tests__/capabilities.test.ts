// Tests for /api/capabilities routes.
//
// Covers:
//   1. GET /api/capabilities/pipelines — lists all registered pipelines
//   2. GET /api/capabilities/pipelines/:name — returns detail for a known pipeline
//   3. GET /api/capabilities/pipelines/:name — 404 for unknown pipeline
//   4. GET /api/capabilities/pipelines — empty list when no pipelines registered
//   5. disabled flag is reflected in the enabled field
//   6. name fallback to id when name field absent

import express, { type NextFunction, type Request, type Response } from 'express';
import { createCapabilitiesRouter } from '../capabilities';
import type { DefinitionsRepository, PipelineDefinition } from '../../pipeline/definitions-repository';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

interface MockRes {
  statusCode: number;
  body: unknown;
  ended: boolean;
  finished: Promise<void>;
  status(code: number): MockRes;
  json(body: unknown): MockRes;
  setHeader(name: string, value: unknown): MockRes;
  getHeader(name: string): unknown;
  end(...args: unknown[]): MockRes;
}

function mockRes(): MockRes {
  let resolveFinished!: () => void;
  const finished = new Promise<void>((r) => { resolveFinished = r; });
  const r: MockRes = {
    statusCode: 200,
    body: undefined,
    ended: false,
    finished,
    status(code) { this.statusCode = code; return this; },
    json(body) { this.body = body; this.ended = true; resolveFinished(); return this; },
    setHeader(_n, _v) { return this; },
    getHeader(_n) { return undefined; },
    end(..._args) { this.ended = true; resolveFinished(); return this; },
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
    params: {},
    user: { sub: 'dev-alice' },
    body: undefined,
  } as unknown as Request;
  await new Promise<void>((resolve, reject) => {
    const finalHandler = (err?: unknown): void => {
      if (err) { reject(err); return; }
      if (!res.ended) { res.status(404).json({ error: 'route not found' }); }
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

// ---------------------------------------------------------------------------
// Mock definitions repository
// ---------------------------------------------------------------------------

function makeRepo(defs: PipelineDefinition[]): DefinitionsRepository {
  return {
    listAll: jest.fn().mockResolvedValue(defs),
    list: jest.fn().mockResolvedValue(defs),
    get: jest.fn().mockResolvedValue(null),
    put: jest.fn().mockResolvedValue(undefined),
    delete: jest.fn().mockResolvedValue(undefined),
  } as unknown as DefinitionsRepository;
}

function buildApp(defs: PipelineDefinition[]): express.Express {
  const app = express();
  app.use(express.json());
  app.use('/api/capabilities', createCapabilitiesRouter({ definitions: makeRepo(defs), system: () => [] }));
  return app;
}

// ---------------------------------------------------------------------------
// Test data
// ---------------------------------------------------------------------------

const PIPELINE_A: PipelineDefinition = {
  id: 'pipeline-alpha',
  name: 'pipeline-alpha',
  version: '1.0.0',
  disabled: false,
  triggers: [{ type: 'webhook', config: {} }, { type: 'schedule', config: {} }],
  steps: [{ id: 's1' }, { id: 's2' }, { id: 's3' }],
};

const PIPELINE_B: PipelineDefinition = {
  id: 'pipeline-beta',
  name: 'pipeline-beta',
  version: '2.1.0',
  disabled: true,
  triggers: [],
  steps: [{ id: 's1' }],
};

// Pipeline with no name field — should fall back to id
const PIPELINE_C: PipelineDefinition = {
  id: 'pipeline-gamma',
  // no name field
  version: '0.5.0',
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('GET /api/capabilities/pipelines', () => {
  it('returns summary list of all pipelines', async () => {
    const app = buildApp([PIPELINE_A, PIPELINE_B]);
    const res = await run(app, 'GET', '/api/capabilities/pipelines');

    expect(res.statusCode).toBe(200);
    const body = res.body as { pipelines: unknown[] };
    expect(body.pipelines).toHaveLength(2);
  });

  it('includes name, enabled, version, triggers count', async () => {
    const app = buildApp([PIPELINE_A]);
    const res = await run(app, 'GET', '/api/capabilities/pipelines');

    const body = res.body as { pipelines: Array<Record<string, unknown>> };
    const [p] = body.pipelines;
    expect(p.name).toBe('pipeline-alpha');
    expect(p.enabled).toBe(true);
    expect(p.version).toBe('1.0.0');
    expect(p.triggers).toBe(2);
  });

  it('reflects disabled=true as enabled=false', async () => {
    const app = buildApp([PIPELINE_B]);
    const res = await run(app, 'GET', '/api/capabilities/pipelines');

    const body = res.body as { pipelines: Array<Record<string, unknown>> };
    expect(body.pipelines[0].enabled).toBe(false);
  });

  it('returns empty array when no pipelines registered', async () => {
    const app = buildApp([]);
    const res = await run(app, 'GET', '/api/capabilities/pipelines');

    expect(res.statusCode).toBe(200);
    const body = res.body as { pipelines: unknown[] };
    expect(body.pipelines).toHaveLength(0);
  });

  it('uses id as name when name field is absent', async () => {
    const app = buildApp([PIPELINE_C]);
    const res = await run(app, 'GET', '/api/capabilities/pipelines');

    const body = res.body as { pipelines: Array<Record<string, unknown>> };
    expect(body.pipelines[0].name).toBe('pipeline-gamma');
  });
});

describe('GET /api/capabilities/pipelines/:name', () => {
  it('returns full detail for a known pipeline', async () => {
    const app = buildApp([PIPELINE_A, PIPELINE_B]);
    const res = await run(app, 'GET', '/api/capabilities/pipelines/pipeline-alpha');

    expect(res.statusCode).toBe(200);
    const body = res.body as Record<string, unknown>;
    expect(body.name).toBe('pipeline-alpha');
    expect(body.enabled).toBe(true);
    expect(body.version).toBe('1.0.0');
    expect(body.steps).toBe(3);
    expect(Array.isArray(body.triggers)).toBe(true);
  });

  it('returns 404 for unknown pipeline', async () => {
    const app = buildApp([PIPELINE_A]);
    const res = await run(app, 'GET', '/api/capabilities/pipelines/does-not-exist');

    expect(res.statusCode).toBe(404);
    const body = res.body as { error: string };
    expect(body.error).toBe('pipeline_not_found');
  });

  it('can look up a pipeline by id when name field is absent', async () => {
    const app = buildApp([PIPELINE_C]);
    const res = await run(app, 'GET', '/api/capabilities/pipelines/pipeline-gamma');

    expect(res.statusCode).toBe(200);
    const body = res.body as Record<string, unknown>;
    expect(body.name).toBe('pipeline-gamma');
  });

  it('returns steps count and triggers array for detailed view', async () => {
    const app = buildApp([PIPELINE_B]);
    const res = await run(app, 'GET', '/api/capabilities/pipelines/pipeline-beta');

    expect(res.statusCode).toBe(200);
    const body = res.body as Record<string, unknown>;
    expect(body.steps).toBe(1);
    expect(body.triggers).toEqual([]);
  });
});

// Loop 29c (NFR #174): the catalogue is the caller's own definitions plus the
// system pipelines — never every user's (`listAll`).
describe('capabilities — caller scope (NFR #174)', () => {
  it('lists the caller\'s definitions and system pipelines, never listAll', async () => {
    const repo = makeRepo([PIPELINE_A]);
    const app = express();
    app.use(express.json());
    app.use('/api/capabilities', createCapabilitiesRouter({ definitions: repo, system: () => [{ id: 'sys-one', name: 'sys-one' }] }));
    const res = await run(app, 'GET', '/api/capabilities/pipelines');
    expect(res.statusCode).toBe(200);
    expect((res.body as { pipelines: Array<{ name: string }> }).pipelines.map((p) => p.name).sort()).toEqual(['pipeline-alpha', 'sys-one']);
    expect(repo.list).toHaveBeenCalledWith('dev-alice');
    expect(repo.listAll).not.toHaveBeenCalled();
  });

  it('another user\'s pipeline is 404 by name', async () => {
    const repo = makeRepo([]);
    const app = express();
    app.use(express.json());
    app.use('/api/capabilities', createCapabilitiesRouter({ definitions: repo, system: () => [] }));
    const res = await run(app, 'GET', '/api/capabilities/pipelines/pipeline-alpha');
    expect(res.statusCode).toBe(404);
    expect(repo.listAll).not.toHaveBeenCalled();
  });
});
