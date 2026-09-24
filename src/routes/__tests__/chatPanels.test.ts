// GET /api/chat/panels/:kind/:id over stubbed run lookups: live run first,
// persisted checkpoint second, 404 when the run is unknown, answered with a
// different panel kind, or left no panelData. Same hand-rolled Express runner
// as chatCommands.test.ts.

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

jest.mock('../../repositories', () => ({ pipelineRunRepo: { get: jest.fn() } }));
jest.mock('../../lib/logger', () => {
  const l = { info: jest.fn(), warn: jest.fn(), error: jest.fn(), debug: jest.fn(), child: () => l };
  return { __esModule: true, default: l, withContext: () => l };
});

import { createChatPanelsRouter, parsePanelId, type ChatPanelsDeps } from '../chatPanels';
import { registerPanelDataProvider, clearPanelDataProviders } from '../../chat/chatPanelProviders';
import { errorHandler } from '../../middleware/error-handler';

interface MockRes {
  statusCode: number; body: unknown; ended: boolean; finished: Promise<void>;
  status(code: number): MockRes; json(body: unknown): MockRes; send(body?: unknown): MockRes;
  setHeader(_n: string, _v: unknown): MockRes; getHeader(_n: string): unknown; end(...args: unknown[]): MockRes;
}
function mockRes(): MockRes {
  let resolve!: () => void;
  const finished = new Promise<void>((r) => { resolve = r; });
  return {
    statusCode: 200, body: undefined, ended: false, finished,
    status(code) { this.statusCode = code; return this; },
    json(body) { this.body = body; this.ended = true; resolve(); return this; },
    send(body) { this.body = body; this.ended = true; resolve(); return this; },
    setHeader() { return this; }, getHeader() { return undefined; },
    end() { this.ended = true; resolve(); return this; },
  };
}
async function run(app: express.Express, url: string): Promise<MockRes> {
  const res = mockRes();
  const req = {
    method: 'GET', url, originalUrl: url, path: url,
    headers: {}, query: {}, body: undefined, params: {}, ip: '127.0.0.1', user: { sub: 'dev-grace' },
  } as unknown as Request;
  await new Promise<void>((resolveOuter, reject) => {
    const finalHandler = (err?: unknown): void => {
      if (err) { reject(err); return; }
      if (!res.ended) res.status(404).json({ error: 'route not found' });
      resolveOuter();
    };
    (app as unknown as (req: Request, res: Response, next: NextFunction) => void)(req, res as unknown as Response, finalHandler);
    res.finished.then(() => resolveOuter()).catch(reject);
  });
  return res;
}
function buildApp(deps: ChatPanelsDeps): express.Express {
  const app = express();
  app.use('/api/chat/panels', createChatPanelsRouter(deps));
  app.use(errorHandler);
  return app;
}

const panelData = { title: 'People: kubernetes', people: [{ id: 'dev-hank', name: 'Hank', tags: ['kubernetes'], score: 1 }], query: 'find me all kubernetes experts' };
const context = { query: 'find me all kubernetes experts', panelData, result: { kind: 'panel', panel: { kind: 'graph', id: 'people-search:run-9', title: 'People: kubernetes' } } };

let getLiveRun: jest.Mock;
let getStoredRunContext: jest.Mock;
beforeEach(() => {
  getLiveRun = jest.fn().mockReturnValue(null);
  getStoredRunContext = jest.fn().mockResolvedValue(null);
});
const app = () => buildApp({ getLiveRun, getStoredRunContext });

test('parsePanelId splits at the first colon and tolerates a bare run id', () => {
  expect(parsePanelId('people-search:run-9')).toEqual({ pipelineId: 'people-search', runId: 'run-9' });
  expect(parsePanelId('people-search:a:b')).toEqual({ pipelineId: 'people-search', runId: 'a:b' });
  expect(parsePanelId('run-9')).toEqual({ runId: 'run-9' });
  expect(parsePanelId(':run-9')).toEqual({ runId: ':run-9' });
});

describe('GET /api/chat/panels/:kind/:id', () => {
  it('serves panelData from the live run and never touches the store', async () => {
    getLiveRun.mockReturnValue({ runId: 'run-9', context });
    const res = await run(app(), '/api/chat/panels/graph/people-search:run-9');
    expect(res.statusCode).toBe(200);
    expect(res.body).toEqual(panelData);
    expect(getLiveRun).toHaveBeenCalledWith('run-9');
    expect(getStoredRunContext).not.toHaveBeenCalled();
  });

  it('falls back to the persisted checkpoint context after a restart', async () => {
    getStoredRunContext.mockResolvedValue(context);
    const res = await run(app(), '/api/chat/panels/graph/people-search:run-9');
    expect(res.statusCode).toBe(200);
    expect(res.body).toEqual(panelData);
    expect(getStoredRunContext).toHaveBeenCalledWith('people-search', 'run-9');
  });

  it('404s an unknown run, and a bare id never reaches the store', async () => {
    expect((await run(app(), '/api/chat/panels/graph/people-search:nope')).statusCode).toBe(404);
    expect((await run(app(), '/api/chat/panels/graph/run-9')).statusCode).toBe(404);
    expect(getStoredRunContext).toHaveBeenCalledTimes(1);
  });

  it('404s when the run answered with a different panel kind or left no panelData', async () => {
    getLiveRun.mockReturnValue({ context });
    expect((await run(app(), '/api/chat/panels/table/people-search:run-9')).statusCode).toBe(404);
    getLiveRun.mockReturnValue({ context: { result: context.result } });
    expect((await run(app(), '/api/chat/panels/graph/people-search:run-9')).statusCode).toBe(404);
    getLiveRun.mockReturnValue({ context: { panelData } });
    expect((await run(app(), '/api/chat/panels/graph/people-search:run-9')).statusCode).toBe(404);
  });

  it('asks a registered panel data provider first, with the whole id and the caller; null falls back to the run', async () => {
    const provider = jest.fn(async (id: string) => (id === 'org-graph:run-1' ? { title: 'Org graph', nodes: [], edges: [] } : null));
    registerPanelDataProvider('graph', provider);
    try {
      const res = await run(app(), '/api/chat/panels/graph/org-graph:run-1');
      expect(res.statusCode).toBe(200);
      expect(res.body).toEqual({ title: 'Org graph', nodes: [], edges: [] });
      expect(provider).toHaveBeenCalledWith('org-graph:run-1', { userId: 'dev-grace' });
      expect(getLiveRun).not.toHaveBeenCalled();
      // Not the provider's id: the run path answers as before.
      getLiveRun.mockReturnValue({ context });
      expect((await run(app(), '/api/chat/panels/graph/people-search:run-9')).body).toEqual(panelData);
      // A provider for another kind is never consulted.
      expect((await run(app(), '/api/chat/panels/table/people-search:run-9')).statusCode).toBe(404);
      expect(provider).toHaveBeenCalledTimes(2);
    } finally { clearPanelDataProviders(); }
  });

  it('a throwing live lookup is a miss, not a 500', async () => {
    getLiveRun.mockImplementation(() => { throw new Error('bridge gone'); });
    getStoredRunContext.mockResolvedValue(context);
    expect((await run(app(), '/api/chat/panels/graph/people-search:run-9')).statusCode).toBe(200);
  });
});
