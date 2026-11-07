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

// These background effects are outside this CRUD fixture. Leaving the real
// audit client running lets SDK imports occur after Jest tears the file down.
jest.mock('../../pipeline/audit-repository', () => ({
  auditRepo: { record: jest.fn().mockResolvedValue(undefined) },
}));
jest.mock('../../pipeline/definitions-invalidator', () => ({
  publishInvalidate: jest.fn().mockResolvedValue(undefined),
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
    // The person's own definitions, followed by the platform-owned system
    // pipelines (flagged `system: true`) that ship with the code.
    const pipelines = (res.body as { pipelines: Array<{ id: string; system?: boolean }> }).pipelines;
    expect(pipelines.filter((p) => !p.system)).toHaveLength(1);
    expect(pipelines.filter((p) => p.system).map((p) => p.id)).toEqual(expect.arrayContaining(['document-agent-edit', 'call-transcription']));
    expect(mockList).toHaveBeenCalledWith('tester');
  });

  // Editing a system pipeline saves a row for it, after which it is in BOTH
  // the repo and the seed. The list used to concatenate the two and return it
  // twice.
  it('lists an edited system pipeline once, preferring the stored version', async () => {
    mockList.mockResolvedValue([
      { id: 'document-agent-edit', name: 'Edited by me', version: 6 },
    ]);
    const res = await run(buildApp(), 'GET', '/api/pipelines/defs');
    const pipelines = (res.body as { pipelines: Array<{ id: string; name?: string; version?: number }> }).pipelines;
    const matches = pipelines.filter((p) => p.id === 'document-agent-edit');
    expect(matches).toHaveLength(1);
    // Stored wins — the same precedence GET /:id uses. An agent building a map
    // by id must not get handed the original seed over the saved definition.
    expect(matches[0].name).toBe('Edited by me');
    expect(matches[0].version).toBe(6);
  });

  it('keeps every id unique across the whole response', async () => {
    mockList.mockResolvedValue([
      { id: 'document-agent-edit', name: 'Edited' },
      { id: 'call-transcription', name: 'Also edited' },
      { id: 'p-1', name: 'Mine' },
    ]);
    const res = await run(buildApp(), 'GET', '/api/pipelines/defs');
    const ids = (res.body as { pipelines: Array<{ id: string }> }).pipelines.map((p) => p.id);
    expect(new Set(ids).size).toBe(ids.length);
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

  // A user-written agent step may not ask for tools (security review §8,
  // package 1): the tool list reached `claude --tools` in this container.
  it('rejects an agent step requesting Bash as 400 and stores nothing', async () => {
    const def = {
      id: 'p-1',
      nodes: [
        { id: 'trigger', type: 'trigger', data: { type: 'trigger' } },
        { id: 'a1', type: 'agent', data: { type: 'agent', prompt: 'run it', allowedTools: ['Bash'] } },
      ],
      edges: [],
    };
    const res = await run(buildApp(), 'PUT', '/api/pipelines/defs/p-1', { body: def });
    expect(res.statusCode).toBe(400);
    expect(res.body).toMatchObject({ error: 'agent_tools_not_allowed', steps: [{ nodeId: 'a1', tools: ['Bash'] }] });
    expect(mockPut).not.toHaveBeenCalled();
  });

  // A generated pipeline (POST /generate) stores devbox agent steps WITH tools.
  // Re-saving it from the editor must not 400; only new or changed tool steps do.
  describe('re-saving a stored definition that already has devbox tool steps', () => {
    const toolNode = (over: Record<string, unknown> = {}) => ({
      id: 'loop-1', type: 'agent',
      data: { type: 'agent', prompt: 'do the step', runtime: 'devbox', allowedTools: ['Read', 'Bash'], ...over },
    });
    const stored = { id: 'p-1', version: 3, nodes: [toolNode()], edges: [] };

    it('accepts the unchanged tool step (prompt and layout edits are fine)', async () => {
      mockGet.mockResolvedValue(stored);
      const res = await run(buildApp(), 'PUT', '/api/pipelines/defs/p-1', {
        body: { id: 'p-1', nodes: [{ ...toolNode({ prompt: 'reworded' }), position: { x: 5, y: 5 } }], edges: [] },
      });
      expect(res.statusCode).toBe(200);
      expect(mockPut).toHaveBeenCalled();
    });

    it('refuses a changed tool list on a stored node', async () => {
      mockGet.mockResolvedValue(stored);
      const res = await run(buildApp(), 'PUT', '/api/pipelines/defs/p-1', {
        body: { id: 'p-1', nodes: [toolNode({ allowedTools: ['Read', 'Bash', 'Write'] })], edges: [] },
      });
      expect(res.statusCode).toBe(400);
      expect(mockPut).not.toHaveBeenCalled();
    });

    it('refuses moving a stored tool step off the devbox runtime', async () => {
      mockGet.mockResolvedValue(stored);
      const res = await run(buildApp(), 'PUT', '/api/pipelines/defs/p-1', {
        body: { id: 'p-1', nodes: [toolNode({ runtime: 'local' })], edges: [] },
      });
      expect(res.statusCode).toBe(400);
    });

    it('refuses a new tool step beside the stored ones', async () => {
      mockGet.mockResolvedValue(stored);
      const res = await run(buildApp(), 'PUT', '/api/pipelines/defs/p-1', {
        body: { id: 'p-1', nodes: [toolNode(), toolNode({ id: undefined })].map((n, i) => (i === 1 ? { ...n, id: 'loop-new' } : n)), edges: [] },
      });
      expect(res.statusCode).toBe(400);
      expect(res.body).toMatchObject({ steps: [{ nodeId: 'loop-new' }] });
    });

    it('refuses a tool step whose stored twin was not on devbox', async () => {
      mockGet.mockResolvedValue({ ...stored, nodes: [toolNode({ runtime: 'local' })] });
      const res = await run(buildApp(), 'PUT', '/api/pipelines/defs/p-1', { body: { id: 'p-1', nodes: [toolNode()], edges: [] } });
      expect(res.statusCode).toBe(400);
    });

    it('refuses when no definition is stored', async () => {
      mockGet.mockResolvedValue(null);
      const res = await run(buildApp(), 'PUT', '/api/pipelines/defs/p-1', { body: { id: 'p-1', nodes: [toolNode()], edges: [] } });
      expect(res.statusCode).toBe(400);
    });
  });

  it('accepts an agent step with no tools', async () => {
    const def = {
      id: 'p-1',
      nodes: [{ id: 'a1', type: 'agent', data: { type: 'agent', prompt: 'summarise', allowedTools: [] } }],
      edges: [],
    };
    const res = await run(buildApp(), 'PUT', '/api/pipelines/defs/p-1', { body: def });
    expect(res.statusCode).toBe(200);
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

describe('PUT optimistic lock only applies to a stored row', () => {
  it('accepts the first edit of a system pipeline, which has no row yet', async () => {
    // System pipelines are seeded in code and served by getSystemPipeline when
    // the repo has nothing. The editor loads one at its declared version and
    // sends that as expectedVersion, which was compared against a stored 0 — a
    // conflict with nothing. Every edit to document-agent-review 409'd and the
    // control snapped back with no error shown.
    mockGet.mockResolvedValue(null);
    const res = await run(buildApp(), 'PUT', '/api/pipelines/defs/document-agent-review', {
      body: { id: 'document-agent-review', name: 'Doc review', expectedVersion: 1 },
    });
    expect(res.statusCode).toBe(200);
    expect(mockPut).toHaveBeenCalled();
  });

  it('still rejects a stale edit when a row does exist', async () => {
    mockGet.mockResolvedValue({ id: 'p-1', version: 3 });
    const res = await run(buildApp(), 'PUT', '/api/pipelines/defs/p-1', {
      body: { id: 'p-1', name: 'x', expectedVersion: 1 },
    });
    expect(res.statusCode).toBe(409);
    expect(mockPut).not.toHaveBeenCalled();
  });

  it('starts a first-written system pipeline at version 1', async () => {
    mockGet.mockResolvedValue(null);
    const res = await run(buildApp(), 'PUT', '/api/pipelines/defs/document-agent-review', {
      body: { id: 'document-agent-review', name: 'Doc review', expectedVersion: 1 },
    });
    expect((res.body as { version: number }).version).toBe(1);
  });
});

describe('POST /api/pipelines/defs/:pipelineId/publish', () => {
  /** A structurally sound graph — publish now refuses anything less. */
  // Sound means publishable, which now includes carrying the configuration the
  // step cannot run without — the same rule the editor has always applied. A
  // bare `{ id: 'a', type: 'llm' }` was sound when only the graph shape was
  // checked; it is an llm with no prompt, and it is not sound any more.
  const soundGraph = {
    nodes: [
      { id: 't', type: 'trigger', data: { type: 'trigger', triggerType: 'manual' } },
      {
        id: 'a',
        type: 'llm',
        data: {
          type: 'llm',
          provider: 'anthropic',
          model: 'claude-opus-5',
          systemPrompt: 'Be brief.',
          userPromptTemplate: 'Summarise {{context.document}}',
        },
      },
    ],
    edges: [{ id: 'e1', source: 't', target: 'a' }],
  };

  it('returns 200 with bumped version and published status', async () => {
    mockGet.mockResolvedValue({ id: 'p-1', version: 2, status: 'draft', ...soundGraph });
    const res = await run(buildApp(), 'POST', '/api/pipelines/defs/p-1/publish');
    expect(res.statusCode).toBe(200);
    const body = res.body as { version: number; status: string; publishedVersion: number };
    expect(body.version).toBe(3);
    expect(body.status).toBe('published');
    expect(body.publishedVersion).toBe(3);
  });

  it('defaults version to 1 when not set', async () => {
    mockGet.mockResolvedValue({ id: 'p-1', ...soundGraph });
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
    mockGet.mockResolvedValue({ id: 'p-1', version: 1, status: 'draft', ...soundGraph });
    mockPut.mockRejectedValue(new Error('DDB write error'));
    const res = await run(buildApp(), 'POST', '/api/pipelines/defs/p-1/publish');
    expect(res.statusCode).toBe(500);
    const body = res.body as { error: string };
    expect(body.error).toBe('internal_error');
  });

  it('refuses to publish a definition with a dangling edge', async () => {
    mockGet.mockResolvedValue({
      id: 'p-1',
      version: 1,
      nodes: [{ id: 't', type: 'trigger' }],
      edges: [{ id: 'e1', source: 't', target: 'deleted-node' }],
    });
    const res = await run(buildApp(), 'POST', '/api/pipelines/defs/p-1/publish');
    expect(res.statusCode).toBe(422);
    const body = res.body as { error: string; validation: { errors: { code: string }[] } };
    expect(body.error).toBe('invalid_pipeline');
    expect(body.validation.errors.map((e) => e.code)).toContain('EDGE_ENDPOINT_UNKNOWN');
    // Nothing was stored: a rejected publish must not bump the version.
    expect(mockPut).not.toHaveBeenCalled();
  });

  it('refuses to publish a definition with no trigger', async () => {
    mockGet.mockResolvedValue({
      id: 'p-1',
      nodes: [{ id: 'a' }, { id: 'b' }],
      edges: [{ id: 'e1', source: 'a', target: 'b' }],
    });
    const res = await run(buildApp(), 'POST', '/api/pipelines/defs/p-1/publish');
    expect(res.statusCode).toBe(422);
    expect(mockPut).not.toHaveBeenCalled();
  });

  it('publishes despite advisory warnings, and reports them', async () => {
    mockGet.mockResolvedValue({
      id: 'p-1',
      version: 1,
      nodes: [...soundGraph.nodes, { id: 'lonely' }],
      edges: soundGraph.edges,
    });
    const res = await run(buildApp(), 'POST', '/api/pipelines/defs/p-1/publish');
    expect(res.statusCode).toBe(200);
    const body = res.body as { validation: { warnings: { code: string }[] } };
    // `lonely` is both unwired and typeless, so it draws both advisories.
    expect(body.validation.warnings.map((w) => w.code).sort()).toEqual([
      'NODE_KIND_UNKNOWN',
      'NODE_ORPHANED',
    ]);
    expect(mockPut).toHaveBeenCalled();
  });
});

describe('pokeCacheAfterWrite — refresh error is swallowed', () => {
  it('does not affect the response when cache refresh rejects', async () => {
    // Import the cache mock to make refresh reject
    const { pipelineDefinitionsCache } = await import('../../pipeline/definitions-cache.js');
    (pipelineDefinitionsCache.refresh as jest.Mock).mockRejectedValueOnce(new Error('cache exploded'));

    // Sound graph: publish validates before it writes, so a definition with no
    // nodes would be rejected before the cache poke this test is about.
    mockGet.mockResolvedValue({
      id: 'p-1',
      status: 'draft',
      nodes: [{ id: 't', type: 'trigger' }, { id: 'a' }],
      edges: [{ id: 'e1', source: 't', target: 'a' }],
    });
    const res = await run(buildApp(), 'POST', '/api/pipelines/defs/p-1/publish');
    // Response still succeeds; the refresh error is fire-and-forget
    expect(res.statusCode).toBe(200);

    // Give the microtask queue a tick so the catch fires
    await Promise.resolve();
  });
});
