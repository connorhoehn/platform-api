// /api/agent-loops — who may see a loop, and what the prompt does while a
// step is running. Same hand-rolled Express runner as chatCommands.test.ts.
import express, { type NextFunction, type Request, type Response } from 'express';

jest.mock('../../lib/logger', () => {
  const l = { info: jest.fn(), warn: jest.fn(), error: jest.fn(), debug: jest.fn(), child: () => l };
  return { __esModule: true, default: l, withContext: () => l };
});

import { createAgentLoopsRouter, isMidTask, viewOf, type AgentLoopsDeps, type LoadedAgentLoop } from '../agentLoops';
import { errorHandler } from '../../middleware/error-handler';
import { SERVICE_AUTH_HEADER, signEnvelope } from '@connorhoehn/service-runtime';

interface MockRes {
  statusCode: number; body: unknown; ended: boolean; finished: Promise<void>;
  status(code: number): MockRes; json(body: unknown): MockRes; send(body?: unknown): MockRes;
  setHeader(_n: string, _v: unknown): MockRes; getHeader(_n: string): unknown; end(...args: unknown[]): MockRes;
  // Express re-parents a response onto its own prototype, so the SSE route's
  // flushHeaders/write would reach the real implementations and their socket
  // internals. Own properties shadow them.
  flushHeaders(): MockRes; write(chunk: unknown): boolean;
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
    flushHeaders() { return this; }, write() { return true; },
    end() { this.ended = true; resolve(); return this; },
  };
}
async function call(app: express.Express, method: string, url: string, userId: string, body?: unknown): Promise<MockRes> {
  const res = mockRes();
  const req = {
    method: method.toUpperCase(), url, originalUrl: url, path: url,
    headers: { 'content-type': 'application/json' }, query: {}, body, params: {}, ip: '127.0.0.1',
    user: { sub: userId },
    on: () => undefined,
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

const PLAN = {
  source: 'planner',
  steps: [
    { id: 's1', title: 'Book the meeting', kind: 'meeting', prompt: 'book', dependsOn: [], done: 'invite exists' },
    { id: 's2', title: 'Build the deck', kind: 'deck', prompt: 'build', dependsOn: ['s1'], done: 'deck exists' },
  ],
};

function loadedRun(over: Partial<LoadedAgentLoop> = {}): LoadedAgentLoop {
  return {
    runId: 'run-1',
    pipelineId: 'chat-agent-loop',
    status: 'running',
    context: {
      requestedBy: 'dev-frank', chatChannel: 'room:x', model: 'opus',
      contextBudgetTokens: 200_000, plan: PLAN, lab: { sessionId: 'sess-1', boxId: 'box-1' },
    },
    steps: {
      'agent:s1': { status: 'completed', output: { iterations: 3, contextTokens: 42_000, tokensIn: 40_000, tokensOut: 900, stopReason: 'complete' } },
      'agent:s2': { status: 'pending' },
    },
    ...over,
  };
}

let deps: AgentLoopsDeps;
let startTask: jest.Mock;
let shellUrl: jest.Mock;
let deleteSession: jest.Mock;
let cancelRun: jest.Mock;
let isChannelViewer: jest.Mock;
let run: LoadedAgentLoop | null;

function buildApp(): express.Express {
  const app = express();
  app.use(express.json());
  app.use('/api/agent-loops', createAgentLoopsRouter(deps));
  app.use(errorHandler);
  return app;
}

beforeEach(() => {
  run = loadedRun();
  startTask = jest.fn().mockResolvedValue({ body: { cancel: () => Promise.resolve() } });
  shellUrl = jest.fn().mockResolvedValue({ url: 'https://shell.example/abc', expiresAt: 'later' });
  deleteSession = jest.fn().mockResolvedValue(true);
  cancelRun = jest.fn().mockResolvedValue(undefined);
  isChannelViewer = jest.fn().mockResolvedValue(false);
  deps = {
    loadRun: async () => run,
    isChannelViewer: (...a: unknown[]) => isChannelViewer(...a) as Promise<boolean>,
    devbox: () => ({ startTask, shellUrl, deleteSession } as never),
    cancelRun: (...a: unknown[]) => cancelRun(...a) as Promise<void>,
    fetchImpl: jest.fn(),
  };
});

describe('GET /api/agent-loops/:runId', () => {
  it('answers the requester with the loop, its steps and its lab', async () => {
    const res = await call(buildApp(), 'get', '/api/agent-loops/run-1', 'dev-frank');
    expect(res.statusCode).toBe(200);
    expect(res.body).toMatchObject({
      runId: 'run-1', status: 'running', model: 'opus',
      contextBudgetTokens: 200_000, contextTokens: 42_000, iterations: 3,
      lab: { sessionId: 'sess-1', boxId: 'box-1', runtime: 'devbox' },
      plan: { source: 'planner', steps: [{ id: 's1', title: 'Book the meeting', kind: 'meeting' }, { id: 's2', title: 'Build the deck', kind: 'deck' }] },
    });
    expect((res.body as { steps: unknown[] }).steps).toEqual([
      { id: 's1', nodeId: 'agent:s1', title: 'Book the meeting', kind: 'meeting', status: 'completed', iterations: 3, contextTokens: 42_000, tokensIn: 40_000, tokensOut: 900, stopReason: 'complete' },
      { id: 's2', nodeId: 'agent:s2', title: 'Build the deck', kind: 'deck', status: 'pending', iterations: 0, contextTokens: 0, tokensIn: 0, tokensOut: 0 },
    ]);
  });

  it('answers a viewer of the channel the command was typed in', async () => {
    isChannelViewer.mockResolvedValue(true);
    const res = await call(buildApp(), 'get', '/api/agent-loops/run-1', 'dev-grace');
    expect(res.statusCode).toBe(200);
    expect(isChannelViewer).toHaveBeenCalledWith('room:x', 'dev-grace');
  });

  it('is a 404, not a 403, for someone who cannot see the channel', async () => {
    const res = await call(buildApp(), 'get', '/api/agent-loops/run-1', 'dev-mallory');
    expect(res.statusCode).toBe(404);
  });

  it('404s an unknown run', async () => {
    run = null;
    expect((await call(buildApp(), 'get', '/api/agent-loops/nope', 'dev-frank')).statusCode).toBe(404);
  });
});

describe('POST /api/agent-loops/:runId/instruct', () => {
  it('accepts a line at the prompt and resumes the session with it', async () => {
    run = loadedRun({ status: 'completed', steps: { 'agent:s1': { status: 'completed' }, 'agent:s2': { status: 'completed' } } });
    const res = await call(buildApp(), 'post', '/api/agent-loops/run-1/instruct', 'dev-frank', { text: 'also add a summary slide' });
    expect(res.statusCode).toBe(202);
    expect(startTask).toHaveBeenCalledWith('sess-1', expect.objectContaining({ prompt: 'also add a summary slide', resume: true }));
  });

  it('409s while a step is mid-task rather than talking over it', async () => {
    run = loadedRun({ steps: { 'agent:s1': { status: 'running' } } });
    const res = await call(buildApp(), 'post', '/api/agent-loops/run-1/instruct', 'dev-frank', { text: 'hello' });
    expect(res.statusCode).toBe(409);
    expect(startTask).not.toHaveBeenCalled();
  });

  it('409s when the loop has no workspace yet', async () => {
    run = loadedRun({ context: { ...loadedRun().context, lab: undefined }, steps: {} });
    expect((await call(buildApp(), 'post', '/api/agent-loops/run-1/instruct', 'dev-frank', { text: 'hi' })).statusCode).toBe(409);
  });

  it('refuses an empty line', async () => {
    run = loadedRun({ steps: {} });
    expect((await call(buildApp(), 'post', '/api/agent-loops/run-1/instruct', 'dev-frank', { text: '  ' })).statusCode).toBe(400);
  });

  it('is open to a channel viewer, like the rest of the loop', async () => {
    run = loadedRun({ steps: {} });
    isChannelViewer.mockResolvedValue(true);
    expect((await call(buildApp(), 'post', '/api/agent-loops/run-1/instruct', 'dev-grace', { text: 'hi' })).statusCode).toBe(202);
  });
});

describe('POST /api/agent-loops/:runId/shell-url', () => {
  it('hands the requester a shell on the box', async () => {
    const res = await call(buildApp(), 'post', '/api/agent-loops/run-1/shell-url', 'dev-frank');
    expect(res.statusCode).toBe(200);
    expect(res.body).toEqual({ url: 'https://shell.example/abc', expiresAt: 'later' });
  });

  it('refuses a channel viewer — reading the chat is not typing into the box', async () => {
    isChannelViewer.mockResolvedValue(true);
    const res = await call(buildApp(), 'post', '/api/agent-loops/run-1/shell-url', 'dev-grace');
    expect(res.statusCode).toBe(403);
    expect(shellUrl).not.toHaveBeenCalled();
  });
});

describe('POST /api/agent-loops/:runId/stop', () => {
  it('cancels the run and then stops the session', async () => {
    const res = await call(buildApp(), 'post', '/api/agent-loops/run-1/stop', 'dev-frank');
    expect(res.statusCode).toBe(202);
    expect(res.body).toEqual({ stopped: true, runId: 'run-1', sessionStopped: true });
    expect(cancelRun).toHaveBeenCalledWith('run-1', 'dev-frank');
    expect(deleteSession).toHaveBeenCalledWith('sess-1');
  });

  it('still reports the run stopped when the session will not delete', async () => {
    deleteSession.mockResolvedValue(false);
    const res = await call(buildApp(), 'post', '/api/agent-loops/run-1/stop', 'dev-frank');
    expect(res.body).toMatchObject({ stopped: true, sessionStopped: false });
  });
});

describe('viewOf', () => {
  it('says the lab is pending, not devbox, before a session exists', () => {
    const view = viewOf(loadedRun({ context: { ...loadedRun().context, lab: undefined } }));
    expect(view.lab).toEqual({ runtime: 'pending' });
  });

  it('surfaces a fallback plan and its notice rather than passing it off as planned', () => {
    const view = viewOf(loadedRun({
      context: { ...loadedRun().context, plan: { source: 'fallback', notice: 'Planner unavailable · single step', steps: [PLAN.steps[0]] } },
    }));
    expect(view.plan).toMatchObject({ source: 'fallback', notice: 'Planner unavailable · single step' });
  });

  it('takes the highest context any step reported and the sum of their iterations', () => {
    const view = viewOf(loadedRun({
      steps: {
        'agent:s1': { status: 'completed', output: { iterations: 3, contextTokens: 42_000 } },
        'agent:s2': { status: 'completed', output: { iterations: 5, contextTokens: 180_000 } },
      },
    }));
    expect({ contextTokens: view.contextTokens, iterations: view.iterations }).toEqual({ contextTokens: 180_000, iterations: 8 });
  });
});

describe('isMidTask', () => {
  it('is true only while a step is actually running', () => {
    expect(isMidTask(loadedRun({ steps: { a: { status: 'running' } } }))).toBe(true);
    expect(isMidTask(loadedRun({ steps: { a: { status: 'completed' }, b: { status: 'pending' } } }))).toBe(false);
    expect(isMidTask(loadedRun({ steps: {} }))).toBe(false);
  });
});


describe('GET /api/agent-loops/:runId/tail', () => {
  const SECRET = 'tail-secret';
  let fetchImpl: jest.Mock;

  beforeEach(() => {
    process.env.AGENTCORE_DEVBOX_URL = 'http://middleware.test:14030';
    process.env.WORK_GRAPH_SERVICE_AUTH_SECRET = SECRET;
    fetchImpl = jest.fn().mockResolvedValue({ ok: true, status: 200, body: { getReader: () => ({ read: async () => ({ done: true }), cancel: async () => undefined }) } });
    deps = { ...deps, fetchImpl: fetchImpl as unknown as AgentLoopsDeps['fetchImpl'] };
  });

  afterEach(() => {
    delete process.env.AGENTCORE_DEVBOX_URL;
    delete process.env.WORK_GRAPH_SERVICE_AUTH_SECRET;
  });

  // The envelope binds the signature to the payload, and a GET has no body,
  // so the middleware verifies the tail against `{ boxId }`. Signing anything
  // else is a valid signature over the wrong thing: a 401 on every run.
  it('signs the boxId itself, not a principal envelope', async () => {
    await call(buildApp(), 'get', '/api/agent-loops/run-1/tail', 'dev-frank');
    expect(fetchImpl).toHaveBeenCalledTimes(1);
    const [url, init] = fetchImpl.mock.calls[0] as [string, { headers: Record<string, string> }];
    expect(url).toBe('http://middleware.test:14030/internal/devbox/boxes/box-1/tail');
    expect(init.headers[SERVICE_AUTH_HEADER]).toBe(signEnvelope({ boxId: 'box-1' }, { secret: SECRET, serviceId: 'platform-api' }));
  });

  it('says devbox runtime unavailable when the middleware has no tail for the box', async () => {
    fetchImpl.mockResolvedValue({ ok: false, status: 404, body: null });
    expect((await call(buildApp(), 'get', '/api/agent-loops/run-1/tail', 'dev-frank')).statusCode).toBe(503);
  });

  it('409s before a session exists, rather than tailing nothing', async () => {
    run = loadedRun({ context: { ...loadedRun().context, lab: undefined } });
    expect((await call(buildApp(), 'get', '/api/agent-loops/run-1/tail', 'dev-frank')).statusCode).toBe(409);
  });
});
