// /api/agent-loops — who may see a loop, and what the prompt does while a
// step is running. Same hand-rolled Express runner as chatCommands.test.ts.
import express, { type NextFunction, type Request, type Response } from 'express';

jest.mock('../../lib/logger', () => {
  const l = { info: jest.fn(), warn: jest.fn(), error: jest.fn(), debug: jest.fn(), child: () => l };
  return { __esModule: true, default: l, withContext: () => l };
});

import {
  createAgentLoopsRouter, environmentOf, isMidTask, labelOf, viewOf, workItemOf,
  type AgentLoopsDeps, type LoadedAgentLoop,
} from '../agentLoops';
import { DevboxUnavailableError } from '../../pipeline/agent/DevboxAgentClient';
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
    headers: { 'content-type': 'application/json' },
    // Express parses this itself; the hand-rolled runner has to, or every
    // query-parameter assertion silently tests the defaults instead.
    query: Object.fromEntries(new URL(url, 'http://test.local').searchParams),
    body, params: {}, ip: '127.0.0.1',
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
    { id: 's1', title: 'Gather the board work', kind: 'research', prompt: 'gather', dependsOn: [], done: 'notes exist' },
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
      instruction: 'create a meeting with X team',
      contextBudgetTokens: 200_000, plan: PLAN, lab: { sessionId: 'sess-1', boxId: 'box-1' },
    },
    steps: {
      'agent:s1': { status: 'completed', output: { iterations: 3, contextTokens: 42_000, tokensIn: 40_000, tokensOut: 900, stopReason: 'complete', runtime: 'devbox' } },
      'agent:s2': { status: 'pending' },
    },
    startedAt: '2026-09-21T11:30:00.000Z',
    costUsd: 0.04,
    ...over,
  };
}

let deps: AgentLoopsDeps;
let startTask: jest.Mock;
let shellUrl: jest.Mock;
let deleteSession: jest.Mock;
let cancelRun: jest.Mock;
let isChannelViewer: jest.Mock;
let listFiles: jest.Mock;
let fetchFile: jest.Mock;
let listRunsFor: jest.Mock;
let run: LoadedAgentLoop | null;

/** A fixed clock so elapsedMs is a fact, not a race. */
const NOW = Date.parse('2026-09-21T12:00:00.000Z');

/** A readable stream over one buffer, as fetch would hand it back. */
function bodyOf(bytes: Buffer): { getReader: () => { read: () => Promise<{ done: boolean; value?: Uint8Array }>; cancel: () => Promise<void> } } {
  let sent = false;
  return { getReader: () => ({
    read: async () => (sent ? { done: true } : ((sent = true), { done: false, value: new Uint8Array(bytes) })),
    cancel: async () => undefined,
  }) };
}

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
  listFiles = jest.fn().mockResolvedValue([]);
  fetchFile = jest.fn();
  listRunsFor = jest.fn().mockResolvedValue([]);
  deps = {
    loadRun: async () => run,
    isChannelViewer: (...a: unknown[]) => isChannelViewer(...a) as Promise<boolean>,
    devbox: () => ({ startTask, shellUrl, deleteSession, listFiles, fetchFile } as never),
    cancelRun: (...a: unknown[]) => cancelRun(...a) as Promise<void>,
    fetchImpl: jest.fn(),
    listRunsForDay: (...a: unknown[]) => listRunsFor(...a) as Promise<LoadedAgentLoop[]>,
    now: () => NOW,
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
      plan: { source: 'planner', steps: [{ id: 's1', title: 'Gather the board work', kind: 'research' }, { id: 's2', title: 'Build the deck', kind: 'deck' }] },
    });
    // s2 is a deck step, so it is a draft row and a render row.
    expect((res.body as { steps: unknown[] }).steps).toEqual([
      { id: 's1', nodeId: 'agent:s1', title: 'Gather the board work', kind: 'research', status: 'completed', iterations: 3, contextTokens: 42_000, tokensIn: 40_000, tokensOut: 900, stopReason: 'complete' },
      { id: 's2', nodeId: 'agent:s2', title: 'Draft slides for Build the deck', kind: 'deck', status: 'pending', iterations: 0, contextTokens: 0, tokensIn: 0, tokensOut: 0 },
      { id: 's2:render', nodeId: 'deck:s2', title: 'Render Build the deck (.pptx)', kind: 'deck', status: 'pending', iterations: 0, contextTokens: 0, tokensIn: 0, tokensOut: 0 },
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

describe('POST /api/agent-loops/:runId/pause|resume', () => {
  it('answers 501 with the reason for someone who can see the run, and touches nothing', async () => {
    for (const action of ['pause', 'resume']) {
      const res = await call(buildApp(), 'post', `/api/agent-loops/run-1/${action}`, 'dev-frank');
      expect(res.statusCode).toBe(501);
      expect(res.body).toMatchObject({ supported: false, action, runId: 'run-1', error: expect.stringContaining('Pausing is not supported') });
    }
    expect(cancelRun).not.toHaveBeenCalled();
    expect(deleteSession).not.toHaveBeenCalled();
  });

  it('is a 404 for someone who cannot see the run', async () => {
    const res = await call(buildApp(), 'post', '/api/agent-loops/run-1/pause', 'dev-stranger');
    expect(res.statusCode).toBe(404);
    expect(res.body).toMatchObject({ error: 'agent loop not found' });
  });
});

describe('controls', () => {
  it('offers stop while the run is live, never pause, and says why', () => {
    expect(viewOf(loadedRun()).controls).toEqual({ stop: true, pause: false, resume: false, pauseUnsupportedReason: expect.stringContaining('Stop ends the run') });
    expect(viewOf(loadedRun({ status: 'completed', completedAt: '2026-09-21T12:00:00.000Z' })).controls.stop).toBe(false);
    expect(viewOf(loadedRun({ status: 'failed' })).controls.stop).toBe(false);
  });

  it('names the executor run when the loop id differs, for the live channel', () => {
    expect(viewOf(loadedRun({ executorRunId: 'exec-9' })).executorRunId).toBe('exec-9');
    expect(viewOf(loadedRun()).executorRunId).toBeUndefined();
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

describe('the detail pane fields', () => {
  it('names the box, the environment and the clock', async () => {
    const res = await call(buildApp(), 'get', '/api/agent-loops/run-1', 'dev-frank');
    expect(res.body).toMatchObject({
      label: 'box-1',                     // the boxId is the session label
      environment: 'devbox:task',
      startedAt: '2026-09-21T11:30:00.000Z',
      elapsedMs: 30 * 60_000,             // 11:30 → the fixed 12:00 clock
      costUsd: 0.04,
    });
  });

  it('measures a finished run start-to-finish, not start-to-now', () => {
    const view = viewOf(loadedRun({ completedAt: '2026-09-21T11:45:00.000Z', status: 'completed' }), NOW);
    expect(view.elapsedMs).toBe(15 * 60_000);
  });

  it('falls back to pipeline · short run id when there is no box', () => {
    expect(labelOf(loadedRun({ context: { ...loadedRun().context, lab: undefined } })))
      .toBe('chat-agent-loop · run-1');
  });

  it('says local:agent when nothing ran in a devbox', () => {
    const local = loadedRun({
      context: { ...loadedRun().context, lab: undefined },
      steps: { 'agent:s1': { status: 'completed', output: { runtime: 'local' } } },
    });
    expect(environmentOf(local)).toBe('local:agent');
  });

  it('omits cost entirely rather than reporting a run with none as $0', () => {
    expect(viewOf(loadedRun({ costUsd: undefined }), NOW)).not.toHaveProperty('costUsd');
  });

  it('omits project and workItem unless the run named them', () => {
    const bare = viewOf(loadedRun(), NOW);
    expect(bare).not.toHaveProperty('project');
    expect(bare).not.toHaveProperty('workItem');

    const named = viewOf(loadedRun({ context: {
      ...loadedRun().context,
      project: 'auth-migration',
      workItem: { key: 'ENG-412', title: 'Migrate auth', status: 'in_progress', tags: ['auth'] },
    } }), NOW);
    expect(named.project).toBe('auth-migration');
    expect(named.workItem).toEqual({ key: 'ENG-412', title: 'Migrate auth', status: 'in_progress', tags: ['auth'] });
  });

  it('ignores a half-named work item rather than rendering a blank card', () => {
    expect(workItemOf(loadedRun({ context: { ...loadedRun().context, workItem: { key: 'ENG-1' } } }))).toBeUndefined();
  });
});

describe('artifacts on the detail pane', () => {
  it('lists the workspace files, asking the middleware for this session', async () => {
    listFiles.mockResolvedValue([{ id: 'f1', name: 'notes.md', sizeBytes: 812, updatedAt: '2026-09-21T11:40:00.000Z' }]);
    const res = await call(buildApp(), 'get', '/api/agent-loops/run-1', 'dev-frank');
    expect(listFiles).toHaveBeenCalledWith('sess-1');
    expect((res.body as { artifacts: unknown[] }).artifacts).toEqual([
      { id: 'f1', name: 'notes.md', sizeBytes: 812, updatedAt: '2026-09-21T11:40:00.000Z' },
    ]);
    expect(res.body).not.toHaveProperty('artifactsNotice');
  });

  // An older middleware has no files route. The pane still renders; the empty
  // list says why it is empty instead of implying the run produced nothing.
  it('answers with an empty list and a notice when the middleware is older', async () => {
    listFiles.mockRejectedValue(new DevboxUnavailableError('middleware has no /internal/devbox/sess-1/files'));
    const res = await call(buildApp(), 'get', '/api/agent-loops/run-1', 'dev-frank');
    expect(res.statusCode).toBe(200);
    expect(res.body).toMatchObject({ artifacts: [], artifactsNotice: expect.stringMatching(/did not answer/) });
  });

  it('says so when the run never had a workspace', async () => {
    run = loadedRun({ context: { ...loadedRun().context, lab: undefined } });
    const res = await call(buildApp(), 'get', '/api/agent-loops/run-1', 'dev-frank');
    expect(res.body).toMatchObject({ artifacts: [], artifactsNotice: 'No cloud workspace for this run' });
    expect(listFiles).not.toHaveBeenCalled();
  });
});

describe('GET /api/agent-loops/:runId/files/:fileId', () => {
  it('streams the bytes with the upstream content type', async () => {
    fetchFile.mockResolvedValue({
      headers: new Map([['content-type', 'text/markdown'], ['content-length', '5']]) as unknown as Headers,
      body: bodyOf(Buffer.from('hello')),
    });
    // `Map` has .get, which is all the route uses.
    const res = await call(buildApp(), 'get', '/api/agent-loops/run-1/files/f1', 'dev-frank');
    expect(fetchFile).toHaveBeenCalledWith('sess-1', 'f1');
    expect(res.statusCode).toBe(200);
  });

  it('413s on the declared size before reading a byte', async () => {
    fetchFile.mockResolvedValue({
      headers: new Map([['content-length', String(11 * 1024 * 1024)]]) as unknown as Headers,
      body: bodyOf(Buffer.alloc(0)),
    });
    const res = await call(buildApp(), 'get', '/api/agent-loops/run-1/files/big.bin', 'dev-frank');
    expect(res.statusCode).toBe(413);
  });

  it('503s rather than 500s when the middleware has no files route', async () => {
    fetchFile.mockRejectedValue(new DevboxUnavailableError('middleware has no /internal/devbox/sess-1/files/f1'));
    expect((await call(buildApp(), 'get', '/api/agent-loops/run-1/files/f1', 'dev-frank')).statusCode).toBe(503);
  });

  it('is closed to someone who cannot see the channel', async () => {
    expect((await call(buildApp(), 'get', '/api/agent-loops/run-1/files/f1', 'dev-mallory')).statusCode).toBe(404);
    expect(fetchFile).not.toHaveBeenCalled();
  });

  it('is open to a channel viewer', async () => {
    isChannelViewer.mockResolvedValue(true);
    fetchFile.mockResolvedValue({ headers: new Map() as unknown as Headers, body: bodyOf(Buffer.from('x')) });
    expect((await call(buildApp(), 'get', '/api/agent-loops/run-1/files/f1', 'dev-grace')).statusCode).toBe(200);
  });
});

describe('GET /api/agent-loops?person=&day=&timezone=', () => {
  const at = (iso: string, over: Partial<LoadedAgentLoop> = {}): LoadedAgentLoop =>
    loadedRun({ runId: `run-${iso}`, startedAt: iso, ...over });

  it('keeps only the runs inside the person\'s local day', async () => {
    listRunsFor.mockResolvedValue([
      at('2026-09-21T04:59:00.000Z'), // 23:59 on the 20th in Chicago — out
      at('2026-09-21T05:00:00.000Z'), // 00:00 on the 21st — in
      at('2026-09-22T04:59:00.000Z'), // 23:59 on the 21st — in
      at('2026-09-22T05:00:00.000Z'), // midnight rolls over — out
    ]);
    const res = await call(buildApp(), 'get', '/api/agent-loops?person=dev-frank&day=2026-09-21&timezone=America/Chicago', 'dev-frank');
    expect(res.statusCode).toBe(200);
    expect(res.body).toMatchObject({ day: '2026-09-21', timezone: 'America/Chicago', from: '2026-09-21T05:00:00.000Z', to: '2026-09-22T05:00:00.000Z' });
    expect((res.body as { loops: Array<{ runId: string }> }).loops.map((l) => l.runId))
      .toEqual(['run-2026-09-22T04:59:00.000Z', 'run-2026-09-21T05:00:00.000Z']); // newest first
  });

  it('summarises a loop with what a row needs', async () => {
    listRunsFor.mockResolvedValue([at('2026-09-21T11:30:00.000Z')]);
    const res = await call(buildApp(), 'get', '/api/agent-loops?day=2026-09-21&timezone=UTC', 'dev-frank');
    expect((res.body as { loops: unknown[] }).loops[0]).toMatchObject({
      label: 'box-1', title: 'create a meeting with X team', model: 'opus',
      environment: 'devbox:task', costUsd: 0.04, steps: 3, chatChannel: 'room:x',
    });
  });

  it('defaults to the viewer and to today in their zone', async () => {
    const res = await call(buildApp(), 'get', '/api/agent-loops?timezone=America/Chicago', 'dev-frank');
    expect(listRunsFor).toHaveBeenCalledWith('dev-frank', '2026-09-21T05:00:00.000Z', '2026-09-22T05:00:00.000Z', 200);
    // The fixed clock is 12:00Z on the 21st = 07:00 in Chicago, same day.
    expect(res.body).toMatchObject({ person: 'dev-frank', day: '2026-09-21' });
  });

  it('shows another person\'s loop only where the viewer can see its channel', async () => {
    listRunsFor.mockResolvedValue([
      at('2026-09-21T11:00:00.000Z', { context: { ...loadedRun().context, chatChannel: 'room:open' } }),
      at('2026-09-21T11:10:00.000Z', { context: { ...loadedRun().context, chatChannel: 'room:closed' } }),
    ]);
    isChannelViewer.mockImplementation(async (channel: string) => channel === 'room:open');
    const res = await call(buildApp(), 'get', '/api/agent-loops?person=dev-frank&day=2026-09-21&timezone=UTC', 'dev-grace');
    const loops = (res.body as { loops: Array<{ chatChannel: string }> }).loops;
    expect(loops.map((l) => l.chatChannel)).toEqual(['room:open']);
  });

  it('caps the day at 50 rows', async () => {
    listRunsFor.mockResolvedValue(Array.from({ length: 80 }, (_, i) => at(`2026-09-21T${String(i % 24).padStart(2, '0')}:00:00.000Z`, { runId: `r${i}` })));
    const res = await call(buildApp(), 'get', '/api/agent-loops?day=2026-09-21&timezone=UTC', 'dev-frank');
    expect((res.body as { loops: unknown[] }).loops).toHaveLength(50);
  });
});

describe('a loop that is still being planned', () => {
  const planning = (over: Partial<LoadedAgentLoop> = {}): LoadedAgentLoop => loadedRun({
    status: 'pending',
    steps: {},
    context: {
      requestedBy: 'dev-frank', chatChannel: 'room:x', model: 'sonnet',
      instruction: 'book the X sync', contextBudgetTokens: 200_000,
      plan: { source: 'pending', steps: [] },
    },
    ...over,
  });

  // The row says 'pending' because that is a status the pipeline machinery
  // understands; a person is looking at a planner still thinking.
  it('reports status planning and a pending plan, not an empty finished run', async () => {
    run = planning();
    const res = await call(buildApp(), 'get', '/api/agent-loops/run-1', 'dev-frank');
    expect(res.statusCode).toBe(200);
    expect(res.body).toMatchObject({
      status: 'planning',
      plan: { source: 'pending', steps: [] },
      steps: [],
      environment: 'local:agent',
    });
  });

  it('stops saying planning once the plan lands', () => {
    expect(viewOf(loadedRun({ status: 'running' }), NOW).status).toBe('running');
  });

  it('does not call a loop planning once it has finished', () => {
    expect(viewOf(planning({ status: 'failed', completedAt: '2026-09-21T11:40:00.000Z' }), NOW).status).toBe('failed');
  });

  it('cancels the EXECUTOR run, not the loop id the client polls', async () => {
    run = loadedRun({ executorRunId: 'executor-run-1' });
    await call(buildApp(), 'post', '/api/agent-loops/run-1/stop', 'dev-frank');
    expect(cancelRun).toHaveBeenCalledWith('executor-run-1', 'dev-frank');
  });
});

describe('a step that failed', () => {
  it('carries its error message through, so the pane can show why', async () => {
    run = loadedRun({
      status: 'failed',
      steps: {
        'agent:s1': { status: 'completed', output: { iterations: 4, contextTokens: 145_800, stopReason: 'complete' } },
        'agent:s2': { status: 'failed', error: 'agent step stopped: timeout', output: { iterations: 2, contextTokens: 180_000, stopReason: 'timeout' } },
      },
    });
    const res = await call(buildApp(), 'get', '/api/agent-loops/run-1', 'dev-frank');
    const steps = (res.body as { steps: Array<Record<string, unknown>> }).steps;
    expect(steps[1]).toMatchObject({ id: 's2', status: 'failed', stopReason: 'timeout', error: 'agent step stopped: timeout' });
    // A step that did not fail carries no error field at all.
    expect(steps[0]).not.toHaveProperty('error');
  });

  it('marks a step the budget skipped, so it does not read as an unexplained blank', async () => {
    run = loadedRun({
      steps: { 'agent:s2': { status: 'completed', output: { stopReason: 'budget', iterations: 0, skipped: true } } },
    });
    const res = await call(buildApp(), 'get', '/api/agent-loops/run-1', 'dev-frank');
    expect((res.body as { steps: Array<Record<string, unknown>> }).steps[1])
      .toMatchObject({ id: 's2', stopReason: 'budget', skipped: true, iterations: 0 });
  });
});

describe('a GET after the process restarted', () => {
  // The live defect: runPersistence REPLACES an over-large step output with
  // `{truncated:true,size}`, so after a run with a real tool trace the reload
  // found no iterations, no context and no stop reason — and the lab read
  // `failed` for a run that had finished.
  const persistedAfterRestart = (): LoadedAgentLoop => loadedRun({
    status: 'completed',
    completedAt: '2026-09-21T11:50:00.000Z',
    steps: {
      // Exactly what storage keeps when the trace blew the 100KB cap.
      'agent:s1': { status: 'completed', output: { truncated: true, size: 812_344 } },
      'agent:s2': { status: 'completed', output: { truncated: true, size: 402_110 } },
    },
    context: {
      ...loadedRun().context,
      // The small mirror the agent step writes into the run context.
      loopSteps: {
        'agent:s1': { iterations: 4, contextTokens: 145_800, stopReason: 'complete', tokensIn: 140_000, tokensOut: 5_800, model: 'opus', runtime: 'devbox', sessionId: 'sess-1', boxId: 'box-1' },
        'agent:s2': { iterations: 1, contextTokens: 205_000, stopReason: 'budget', tokensIn: 60_000, tokensOut: 900, model: 'opus', runtime: 'devbox', sessionId: 'sess-1', boxId: 'box-1' },
      },
    },
  });

  it('reports the same numbers a live GET did, from the persisted run alone', async () => {
    run = persistedAfterRestart();
    const res = await call(buildApp(), 'get', '/api/agent-loops/run-1', 'dev-frank');
    const body = res.body as { steps: Array<Record<string, unknown>>; contextTokens: number; iterations: number; status: string };

    expect(body.steps[0]).toMatchObject({ id: 's1', iterations: 4, contextTokens: 145_800, stopReason: 'complete', tokensIn: 140_000, tokensOut: 5_800 });
    expect(body.steps[1]).toMatchObject({ id: 's2', iterations: 1, contextTokens: 205_000, stopReason: 'budget' });
    // The run totals are the same ones the live poll showed.
    expect({ contextTokens: body.contextTokens, iterations: body.iterations })
      .toEqual({ contextTokens: 205_000, iterations: 5 });
  });

  it('calls that run budget, not failed — it ran out of what it was given', async () => {
    run = persistedAfterRestart();
    const res = await call(buildApp(), 'get', '/api/agent-loops/run-1', 'dev-frank');
    expect((res.body as { status: string }).status).toBe('budget');
  });

  it('prefers a real output over the mirror, so live numbers are never stale', async () => {
    run = loadedRun({
      steps: { 'agent:s1': { status: 'running', output: { iterations: 7, contextTokens: 190_000, stopReason: 'complete' } } },
      context: {
        ...loadedRun().context,
        loopSteps: { 'agent:s1': { iterations: 2, contextTokens: 50_000, stopReason: 'complete' } },
      },
    });
    const res = await call(buildApp(), 'get', '/api/agent-loops/run-1', 'dev-frank');
    expect((res.body as { steps: Array<Record<string, unknown>> }).steps[0])
      .toMatchObject({ iterations: 7, contextTokens: 190_000 });
  });

  it('leaves a genuinely failed run reading failed, budget stop or not', async () => {
    run = loadedRun({
      status: 'failed', completedAt: '2026-09-21T11:50:00.000Z',
      steps: { 'agent:s1': { status: 'failed', error: 'agent step stopped: timeout', output: { stopReason: 'timeout', iterations: 3 } } },
    });
    expect((await call(buildApp(), 'get', '/api/agent-loops/run-1', 'dev-frank')).body)
      .toMatchObject({ status: 'failed' });
  });

  it('does not call a still-running loop budget before it is over', async () => {
    run = loadedRun({
      status: 'running',
      steps: { 'agent:s1': { status: 'completed', output: { stopReason: 'budget', iterations: 1, contextTokens: 205_000 } } },
    });
    expect((await call(buildApp(), 'get', '/api/agent-loops/run-1', 'dev-frank')).body)
      .toMatchObject({ status: 'running' });
  });
});

describe('a loop with a deck step', () => {
  const deckPlan = {
    source: 'planner',
    steps: [
      { id: 's1', title: 'Gather board work', kind: 'research', prompt: 'g', dependsOn: [], done: 'notes' },
      { id: 's2', title: 'Platform review deck', kind: 'deck', prompt: 'd', dependsOn: ['s1'], done: 'deck' },
    ],
  };
  const withDeck = (over: Record<string, { status?: string; output?: unknown; error?: string }>): LoadedAgentLoop => loadedRun({
    context: { ...loadedRun().context, plan: deckPlan },
    steps: {
      'agent:s1': { status: 'completed', output: { iterations: 2, contextTokens: 40_000 } },
      ...over,
    },
  });

  it('shows the draft and the render as their own rows', async () => {
    run = withDeck({ 'agent:s2': { status: 'completed' }, 'deck:s2': { status: 'running' } });
    const res = await call(buildApp(), 'get', '/api/agent-loops/run-1', 'dev-frank');
    const steps = (res.body as { steps: Array<Record<string, unknown>> }).steps;
    expect(steps.map((s) => [s.nodeId, s.kind, s.title])).toEqual([
      ['agent:s1', 'research', 'Gather board work'],
      ['agent:s2', 'deck', 'Draft slides for Platform review deck'],
      ['deck:s2', 'deck', 'Render Platform review deck (.pptx)'],
    ]);
  });

  it('carries what the render produced onto the step and onto the loop', async () => {
    run = withDeck({
      'agent:s2': { status: 'completed' },
      'deck:s2': { status: 'completed', output: { documentId: 'doc-9', revisionId: 'rev-2', slideCount: 12, title: 'Platform review' } },
    });
    const res = await call(buildApp(), 'get', '/api/agent-loops/run-1', 'dev-frank');
    const body = res.body as { steps: Array<Record<string, unknown>>; produced: unknown[] };
    expect(body.steps[2].result).toEqual({ documentId: 'doc-9', revisionId: 'rev-2', slideCount: 12, title: 'Platform review' });
    expect(body.produced).toEqual([{ documentId: 'doc-9', title: 'Platform review', revisionId: 'rev-2', slideCount: 12 }]);
  });

  it('produces nothing until the render has actually produced something', async () => {
    run = withDeck({ 'deck:s2': { status: 'running' } });
    const res = await call(buildApp(), 'get', '/api/agent-loops/run-1', 'dev-frank');
    expect((res.body as { produced: unknown[] }).produced).toEqual([]);
    expect((res.body as { steps: Array<Record<string, unknown>> }).steps[2]).not.toHaveProperty('result');
  });

  // Under MOCK_AGENTCORE the drafting agent answers with prose. Rendering that
  // would be a fabricated document with a download button.
  it('says the draft was not a DeckSpec, and keeps the generator\'s own words beside it', async () => {
    run = withDeck({
      'agent:s2': { status: 'completed' },
      'deck:s2': { status: 'failed', error: 'deck: no DeckSpec found — set the node\'s specFrom to the step that drafts it' },
    });
    const res = await call(buildApp(), 'get', '/api/agent-loops/run-1', 'dev-frank');
    const render = (res.body as { steps: Array<Record<string, unknown>> }).steps[2];
    expect(render.error).toBe('draft was not a DeckSpec');
    expect(render.errorDetail).toMatch(/no DeckSpec found/);
    expect((res.body as { produced: unknown[] }).produced).toEqual([]);
  });

  it('passes a real renderer failure through as itself', async () => {
    run = withDeck({ 'deck:s2': { status: 'failed', error: 'deck: the blob store refused the upload' } });
    const render = (await call(buildApp(), 'get', '/api/agent-loops/run-1', 'dev-frank')).body as { steps: Array<Record<string, unknown>> };
    expect(render.steps[2].error).toBe('deck: the blob store refused the upload');
    expect(render.steps[2]).not.toHaveProperty('errorDetail');
  });

  it('lists what the day\'s loops produced, so the Computer Lab tab can link them', async () => {
    listRunsFor.mockResolvedValue([withDeck({
      'deck:s2': { status: 'completed', output: { documentId: 'doc-9', revisionId: 'rev-1', slideCount: 8, title: 'Deck' } },
    })]);
    const res = await call(buildApp(), 'get', '/api/agent-loops?day=2026-09-21&timezone=UTC', 'dev-frank');
    expect((res.body as { loops: Array<{ produced: unknown[] }> }).loops[0].produced)
      .toEqual([{ documentId: 'doc-9', title: 'Deck', revisionId: 'rev-1', slideCount: 8 }]);
  });
});

describe('a loop whose steps are not all agent nodes', () => {
  // The live defect: the executor ran agent:s1, meeting:create:s2, agent:s3,
  // deck:s3, but the GET listed agent:s1, agent:s2, agent:s3, deck:s3 — so a
  // dead-lettered meeting:create:s2 showed as a PENDING agent:s2 with no
  // error, and the run-level error was null.
  const fourKinds = {
    source: 'planner',
    steps: [
      { id: 's1', title: 'Write the agenda', kind: 'agenda', prompt: 'a', dependsOn: [], done: 'x' },
      { id: 's2', title: 'Book the platform sync', kind: 'meeting', prompt: 'm', dependsOn: ['s1'], done: 'x' },
      { id: 's3', title: 'Build the review deck', kind: 'deck', prompt: 'd', dependsOn: ['s2'], done: 'x' },
      { id: 's4', title: 'Chase the stragglers', kind: 'other', prompt: 'o', dependsOn: ['s3'], done: 'x' },
    ],
  };
  const withNodes = (steps: Record<string, { status?: string; output?: unknown; error?: string }>, over: Partial<LoadedAgentLoop> = {}): LoadedAgentLoop =>
    loadedRun({ context: { ...loadedRun().context, plan: fourKinds }, steps, ...over });

  it('lists the real node ids the definition generated, not agent:<planId>', async () => {
    run = withNodes({});
    const res = await call(buildApp(), 'get', '/api/agent-loops/run-1', 'dev-frank');
    expect((res.body as { steps: Array<{ nodeId: string }> }).steps.map((s) => s.nodeId)).toEqual([
      'agent:s1', 'agenda:write:s1', 'meeting:create:s2', 'agent:s3', 'deck:s3', 'agent:s4',
    ]);
  });

  it('shows a failed capability node as failed, with its error', async () => {
    run = withNodes({
      'agent:s1': { status: 'completed' },
      'agenda:write:s1': { status: 'completed', output: { documentId: 'doc-1', title: 'Agenda' } },
      'meeting:create:s2': { status: 'failed', error: 'meeting: no team named "platform"' },
    }, { status: 'failed' });

    const res = await call(buildApp(), 'get', '/api/agent-loops/run-1', 'dev-frank');
    const body = res.body as { steps: Array<Record<string, unknown>>; error?: string; errorNodeId?: string };
    const meeting = body.steps.find((s) => s.nodeId === 'meeting:create:s2')!;
    expect(meeting).toMatchObject({ status: 'failed', kind: 'meeting', error: 'meeting: no team named "platform"' });
    // And the run says what it failed of, instead of a null error.
    expect(body.error).toBe('meeting: no team named "platform"');
    expect(body.errorNodeId).toBe('meeting:create:s2');
  });

  it('prefers the run row\'s own error when it recorded one', async () => {
    run = withNodes(
      { 'meeting:create:s2': { status: 'failed', error: 'meeting: no team named "platform"' } },
      { status: 'failed', error: 'run failed at agent:s3' },
    );
    const body = (await call(buildApp(), 'get', '/api/agent-loops/run-1', 'dev-frank')).body as { error: string; errorNodeId: string };
    expect(body.error).toBe('run failed at agent:s3');
    expect(body.errorNodeId).toBe('meeting:create:s2');
  });

  it('reports no error at all for a run where nothing failed', async () => {
    run = withNodes({ 'agent:s1': { status: 'completed' }, 'meeting:create:s2': { status: 'completed' } });
    const body = (await call(buildApp(), 'get', '/api/agent-loops/run-1', 'dev-frank')).body as Record<string, unknown>;
    expect(body).not.toHaveProperty('error');
    expect(body).not.toHaveProperty('errorNodeId');
  });

  it('collects what each capability node produced, whatever kind made it', async () => {
    run = withNodes({
      'agenda:write:s1': { status: 'completed', output: { documentId: 'doc-1', title: 'Agenda' } },
      'meeting:create:s2': { status: 'completed', output: { roomId: 'room-7', title: 'Platform sync' } },
      'deck:s3': { status: 'completed', output: { documentId: 'doc-9', revisionId: 'rev-1', slideCount: 10, title: 'Review' } },
    });
    const body = (await call(buildApp(), 'get', '/api/agent-loops/run-1', 'dev-frank')).body as { produced: Array<Record<string, unknown>> };
    expect(body.produced).toEqual([
      { documentId: 'doc-1', title: 'Agenda' },
      { documentId: 'room-7', title: 'Platform sync' },
      { documentId: 'doc-9', title: 'Review', revisionId: 'rev-1', slideCount: 10 },
    ]);
  });

  it('keeps the drafting agent rows distinct from the nodes that act', async () => {
    run = withNodes({ 'agent:s1': { status: 'running' } });
    const steps = ((await call(buildApp(), 'get', '/api/agent-loops/run-1', 'dev-frank')).body as { steps: Array<Record<string, unknown>> }).steps;
    expect(steps.slice(0, 2).map((s) => [s.id, s.title])).toEqual([
      ['s1', 'Draft the agenda for Write the agenda'],
      ['s1:write', 'Write Write the agenda as a page'],
    ]);
  });
});
