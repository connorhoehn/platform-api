// POST /api/pipelines/defs/generate — a draft the caller owns, idempotent by
// requestId through the DynamoDB-claim idempotency middleware (memory store
// under NODE_ENV=test), and mode:'run' through the bridge.

import express, { type NextFunction, type Request, type Response } from 'express';

const mockPut = jest.fn();
const mockTrigger = jest.fn();
let bridge: { trigger?: typeof mockTrigger } | null = null;

jest.mock('../../pipeline/definitions-repository', () => ({
  definitionsRepo: { list: jest.fn(), get: jest.fn(), put: mockPut, delete: jest.fn() },
}));
jest.mock('../../pipeline/definitions-cache', () => ({ pipelineDefinitionsCache: { refresh: jest.fn().mockResolvedValue(undefined) } }));
jest.mock('../../lib/logger', () => {
  const l = { info: jest.fn(), error: jest.fn(), warn: jest.fn(), debug: jest.fn(), child: () => l };
  return { withContext: () => l, default: l, logger: l };
});
jest.mock('../../lib/aws-clients', () => ({ docClient: {}, ddbClient: {} }));
jest.mock('../../repositories/PipelineRunRollupRepository', () => ({ pipelineRunRollupRepo: { getMany: jest.fn(), putIfNotNewer: jest.fn() } }));
jest.mock('../../repositories/PipelineRunRepository', () => ({ PipelineRunRepository: jest.fn() }));
jest.mock('../../pipeline/bridge-types', () => ({ getPipelineBridge: () => bridge }));
jest.mock('../../pipeline/runPersistence', () => ({ getRunPersistenceService: () => ({ registerTrigger: jest.fn() }) }));
// No model in tests: the planner falls back to a single step and says so.
jest.mock('../../chat/builtinChatCommands', () => ({ getAgentLoopPlannerClient: () => null }));
jest.mock('distributed-core', () => ({ ...jest.requireActual('distributed-core'), generateWebhookSecret: () => 'wh-secret-test' }));

import { pipelineDefinitionsRouter } from '../pipelineDefinitions';
import { errorHandler } from '../../middleware/error-handler';
import { _resetIdempotencyMemFallback } from '../../middleware/idempotency';

interface MockRes {
  statusCode: number; body: unknown; ended: boolean; finished: Promise<void>; headers: Record<string, string>;
  status(code: number): MockRes; json(body: unknown): MockRes; send(body?: unknown): MockRes;
  setHeader(n: string, v: unknown): MockRes; getHeader(): unknown; end(): MockRes; once(): MockRes; on(): MockRes;
}
function mockRes(): MockRes {
  let resolve!: () => void;
  const finished = new Promise<void>((r) => { resolve = r; });
  const r: MockRes = {
    statusCode: 200, body: undefined, ended: false, finished, headers: {},
    status(code) { this.statusCode = code; return this; },
    json(body) { this.body = body; this.ended = true; resolve(); return this; },
    send(body) { this.body = body; this.ended = true; resolve(); return this; },
    setHeader(n, v) { this.headers[n] = String(v); return this; }, getHeader() { return undefined; },
    end() { this.ended = true; resolve(); return this; }, once() { return this; }, on() { return this; },
  };
  return r;
}
async function post(app: express.Express, url: string, body: unknown, userId = 'dev-connor'): Promise<MockRes> {
  const res = mockRes();
  const req = {
    method: 'POST', url, originalUrl: url, path: url, headers: { 'content-type': 'application/json' }, query: {}, body,
    user: { sub: userId, email: `${userId}@local.dev`, orgId: 'dev-org' }, params: {}, ip: '127.0.0.1',
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
  // The idempotency commit settles after res.json returns.
  await new Promise((r) => setImmediate(r));
  return res;
}
function buildApp(): express.Express {
  const app = express();
  app.use(express.json());
  app.use('/api/pipelines/defs', pipelineDefinitionsRouter);
  app.use(errorHandler);
  return app;
}

const INSTRUCTION = 'Create a pipeline that reviews a document, asks me to approve the plan, then suggests edits.';

beforeEach(() => {
  mockPut.mockReset().mockResolvedValue(undefined);
  mockTrigger.mockReset().mockResolvedValue({ runId: 'run-abc' });
  bridge = null;
  _resetIdempotencyMemFallback();
});

describe('POST /api/pipelines/defs/generate', () => {
  it('mode draft: stores a draft owned by the caller with origin.kind agent and the instruction verbatim', async () => {
    const res = await post(buildApp(), '/api/pipelines/defs/generate', { instruction: INSTRUCTION, mode: 'draft', requestId: 'req-1' });
    expect(res.statusCode).toBe(200);
    const body = res.body as { pipeline: Record<string, any>; runId?: string; planner: { source: string; steps: number } };
    expect(body.runId).toBeUndefined();
    expect(body.planner).toMatchObject({ source: 'fallback', steps: 1 });
    expect(body.pipeline).toMatchObject({
      status: 'draft', version: 1, workType: 'agents', kind: 'agent', createdBy: 'dev-connor',
      description: INSTRUCTION, sourceInstruction: INSTRUCTION,
      origin: { kind: 'agent', instruction: INSTRUCTION, plannerSource: 'fallback' },
    });
    expect(body.pipeline.id).toMatch(/^agent-[a-z0-9-]+-[a-z0-9]{6}$/);
    expect(body.pipeline.publishedVersion).toBeUndefined();
    expect(body.pipeline.inferred).toBeUndefined();
    expect(body.pipeline.route[0]).toBe('/agent');
    expect(body.pipeline.name).toBe('Create a pipeline that reviews a…');
    expect(mockPut).toHaveBeenCalledTimes(1);
    expect(mockPut.mock.calls[0][0]).toBe('dev-connor');
    expect(mockPut.mock.calls[0][1]).toMatchObject({ id: body.pipeline.id, status: 'draft', createdBy: 'dev-connor' });
  });

  it('the same requestId replays the first answer without a second draft; a different requestId makes a new one', async () => {
    const app = buildApp();
    const first = await post(app, '/api/pipelines/defs/generate', { instruction: INSTRUCTION, mode: 'draft', requestId: 'req-same' });
    const again = await post(app, '/api/pipelines/defs/generate', { instruction: INSTRUCTION, mode: 'draft', requestId: 'req-same' });
    expect(again.statusCode).toBe(200);
    expect(again.headers['X-Idempotent-Replay']).toBe('true');
    expect((again.body as any).pipeline.id).toBe((first.body as any).pipeline.id);
    expect(mockPut).toHaveBeenCalledTimes(1);
    const other = await post(app, '/api/pipelines/defs/generate', { instruction: INSTRUCTION, mode: 'draft', requestId: 'req-other' });
    expect((other.body as any).pipeline.id).not.toBe((first.body as any).pipeline.id);
    expect(mockPut).toHaveBeenCalledTimes(2);
  });

  it('a requestId is scoped to the caller: another user with the same id gets their own draft', async () => {
    const app = buildApp();
    await post(app, '/api/pipelines/defs/generate', { instruction: INSTRUCTION, mode: 'draft', requestId: 'shared' }, 'dev-connor');
    const frank = await post(app, '/api/pipelines/defs/generate', { instruction: INSTRUCTION, mode: 'draft', requestId: 'shared' }, 'dev-frank');
    expect(frank.headers['X-Idempotent-Replay']).toBeUndefined();
    expect((frank.body as any).pipeline.createdBy).toBe('dev-frank');
    expect(mockPut).toHaveBeenCalledTimes(2);
    expect(mockPut.mock.calls[1][0]).toBe('dev-frank');
  });

  it('mode run: publishes and triggers through the bridge with the caller pinned as requester', async () => {
    bridge = { trigger: mockTrigger };
    const res = await post(buildApp(), '/api/pipelines/defs/generate', { instruction: `/research ${INSTRUCTION} 200k`, mode: 'run', requestId: 'req-run' });
    expect(res.statusCode).toBe(200);
    const body = res.body as any;
    expect(body.runId).toBe('run-abc');
    expect(body.pipeline).toMatchObject({ status: 'published', publishedVersion: 1, tags: ['research'] });
    expect(mockTrigger).toHaveBeenCalledTimes(1);
    const args = mockTrigger.mock.calls[0][0];
    expect(args.pipelineId).toBe(body.pipeline.id);
    expect(args.triggeredBy).toEqual({ userId: 'dev-connor' });
    expect(args.triggerPayload).toMatchObject({ requestedBy: 'dev-connor', organizationId: 'dev-org', contextBudgetTokens: 200000, hints: ['research'] });
    expect(args.triggerPayload.plan.steps).toHaveLength(1);
  });

  it('mode run with no bridge still stores the definition and says the run did not start', async () => {
    const res = await post(buildApp(), '/api/pipelines/defs/generate', { instruction: INSTRUCTION, action: 'draft-and-run' });
    expect(res.statusCode).toBe(200);
    expect((res.body as any).runError).toBe('bridge_unavailable');
    expect((res.body as any).runId).toBeUndefined();
    expect(mockPut).toHaveBeenCalledTimes(1);
  });

  it('rejects an empty or over-long instruction with 422 and a bad mode with 400; nothing stored', async () => {
    const app = buildApp();
    expect((await post(app, '/api/pipelines/defs/generate', { instruction: '   ', mode: 'draft' })).statusCode).toBe(422);
    expect((await post(app, '/api/pipelines/defs/generate', { instruction: 'x'.repeat(4001), mode: 'draft' })).statusCode).toBe(422);
    const bad = await post(app, '/api/pipelines/defs/generate', { instruction: INSTRUCTION, mode: 'publish' });
    expect(bad.statusCode).toBe(400);
    expect(mockPut).not.toHaveBeenCalled();
  });

  it('a 422 releases the requestId claim so a corrected retry with the same id runs', async () => {
    const app = buildApp();
    expect((await post(app, '/api/pipelines/defs/generate', { instruction: '', mode: 'draft', requestId: 'r' })).statusCode).toBe(422);
    const ok = await post(app, '/api/pipelines/defs/generate', { instruction: INSTRUCTION, mode: 'draft', requestId: 'r' });
    expect(ok.statusCode).toBe(200);
    expect(mockPut).toHaveBeenCalledTimes(1);
  });
});
