// Loop 29c (realtime-examples access sweep, NFR #173 / #174): the run-scoped
// and operator routes, through the REAL gate (src/pipeline/runAccess.ts,
// src/pipeline/image/imageAccess.ts). One run, owned by dev-owner, targeting
// doc-1 (dev-reader may read doc-1, dev-frank may not):
//
//   read routes   owner 200 · document reader 200 · stranger 404 · anonymous 401
//   owner routes  owner passes · reader 403 · stranger 404 · anonymous 401
//
// plus list filtering (runs, active runs, approvals queue, DLQ, inspector),
// the wrong-pipeline 404, the gateway's signed call, image uploader scope,
// judgments via their run, and the run-channel document binding.

import express, { type NextFunction, type Request, type Response } from 'express';
import { signEnvelope, SERVICE_AUTH_HEADER } from '@connorhoehn/service-runtime';

const mockLogger = { info: jest.fn(), warn: jest.fn(), error: jest.fn(), debug: jest.fn(), child: () => mockLogger };
jest.mock('../../lib/logger', () => ({ __esModule: true, default: mockLogger, withContext: () => mockLogger }));
jest.mock('../../repositories/PipelineRunRepository', () => ({
  PipelineRunRepository: jest.fn().mockImplementation(() => ({ get: jest.fn(async () => null) })),
}));
jest.mock('../../lib/ddb-client', () => ({ getDynamoClient: () => ({}) }));
jest.mock('../../pipeline/run-rollup-subscriber', () => ({
  getRunRollupSubscriber: () => null,
  lookupPipelineIdByRunId: async () => null,
}));
jest.mock('../../repositories/PipelineRunRollupRepository', () => ({
  pipelineRunRollupRepo: { get: async () => null, listWithOpenApprovals: async () => [] },
}));
jest.mock('../../pipeline/runPersistence', () => ({
  getRunPersistenceService: () => ({ getStepAttempts: () => ({}), registerTrigger: jest.fn() }),
}));
jest.mock('../../lib/redis-client', () => ({ getRedisClient: async () => null }));
jest.mock('../../lib/aws-clients', () => ({
  docClient: { send: jest.fn().mockResolvedValue({ Item: undefined, Items: [] }) },
  ddbClient: { send: jest.fn().mockResolvedValue({}) },
  sqsClient: { send: jest.fn().mockResolvedValue({}) },
}));
const mockImageMeta = jest.fn();
jest.mock('../../pipeline/image/visionServices', () => ({
  getImageBlobStore: () => ({
    getMetadata: (id: string) => mockImageMeta(id),
    get: async (id: string) => {
      const meta = await mockImageMeta(id);
      return meta ? { body: Buffer.from('png'), meta: { mimeType: 'image/png', sizeBytes: 3, width: 1, height: 1, uploadedAt: 'x', ...meta } } : null;
    },
  }),
  getVisionClient: () => null,
}));
jest.mock('../../pipeline/judge/judgeServices', () => ({
  getJudgmentRepo: () => ({
    getById: async (id: string) => (id === 'j-1' ? { judgmentId: 'j-1', runId: 'run-1', pipelineId: 'pl-1' } : null),
    getRawText: async () => 'the model said',
  }),
  getJudgmentAggregateRepo: () => ({ listByPipeline: async () => ({ items: [] }), listByRubric: async () => ({ items: [] }) }),
}));

import { setPipelineBridge } from '../../pipeline/bridge-types';
import { setRunAccessDeps, type RunFacts } from '../../pipeline/runAccess';
import { errorHandler } from '../../middleware/error-handler';
import {
  pipelineTriggersRouter,
  pipelineCancelRouter,
  pipelineActiveRunsRouter,
  pipelinePendingApprovalsRouter,
  pipelineBreakpointRouter,
  pipelineApprovalsRouter,
} from '../pipelineTriggers';
import { pipelineBreakpointPatchRouter } from '../pipelineBreakpointPatch';
import { pipelineRunActionsRouter } from '../pipelineRetry';
import { pipelineRunAuditRouter } from '../pipelineAudit';
import { pipelineDLQRouter } from '../pipelineDLQ';
import { pipelineInspectorRouter } from '../pipelineInspector';
import { imagesRouter } from '../images';
import { judgmentsRouter } from '../judgments';
import { decidePipelineAccess } from '../internal-pipeline-access';

// ── fixtures ────────────────────────────────────────────────────────────────
const RUNS: Record<string, RunFacts> = {
  'run-1': { ownerId: 'dev-owner', pipelineId: 'pl-1', documentId: 'doc-1' },
  'run-2': { ownerId: 'dev-frank', pipelineId: 'pl-1', documentId: 'doc-private' },
};
const DOCS: Record<string, Record<string, unknown>> = {
  'doc-1': { documentId: 'doc-1', createdBy: 'dev-owner', viewers: ['dev-reader'] },
  'doc-private': { documentId: 'doc-private', createdBy: 'dev-frank' },
};

const cancelRun = jest.fn(async (runId: string) => ({ runId, status: 'cancelled' }));
const patchRunContext = jest.fn(async () => ({ ok: true }));
const dlqEntries: Record<string, unknown> = {
  'dl-1': { id: 'dl-1', payload: { runId: 'run-1', pipelineId: 'pl-1' }, lastError: 'Error: x' },
  'dl-2': { id: 'dl-2', payload: { runId: 'run-2', pipelineId: 'pl-1' }, lastError: 'Error: y' },
};
const dlq = {
  list: jest.fn(async () => ({ items: Object.values(dlqEntries) })),
  peek: jest.fn(async (id: string) => dlqEntries[id] ?? null),
  redrive: jest.fn(async (ids: string[]) => ({ redriven: ids.length })),
  purge: jest.fn(async (ids: string[]) => ({ purged: ids.length })),
};
const inspector = {
  listPending: jest.fn(async () => ({ items: [{ id: 'e1', payload: { runId: 'run-1' } }, { id: 'e2', payload: { runId: 'run-2' } }] })),
  listInflight: jest.fn(async () => []),
  summary: jest.fn(async () => ({})),
  peekPending: jest.fn(async (runId: string) => ({ id: 'e', payload: { runId } })),
};

beforeEach(() => {
  jest.clearAllMocks();
  delete process.env.SKIP_AUTH;
  process.env.SERVICE_AUTH_SECRET = 'test-secret';
  setRunAccessDeps({
    liveRun: async (runId) => RUNS[runId] ?? null,
    storedRun: async () => null,
    documentRow: async (id) => (DOCS[id] as never) ?? null,
  });
  setPipelineBridge({
    trigger: jest.fn(),
    getRun: async (runId: string) => (RUNS[runId] ? { runId, pipelineId: RUNS[runId].pipelineId, status: 'running', triggeredBy: { userId: RUNS[runId].ownerId } } : null),
    getHistory: async () => [],
    listRuns: async () => ({ runs: [{ runId: 'run-1', pipelineId: 'pl-1' }, { runId: 'run-2', pipelineId: 'pl-1' }] }),
    listActiveRuns: async () => [{ runId: 'run-1', pipelineId: 'pl-1' }, { runId: 'run-2', pipelineId: 'pl-1' }],
    getPendingApprovals: async () => [
      { runId: 'run-1', pipelineId: 'pl-1', stepId: 'gate', approvers: [{ type: 'role', value: 'any' }] },
      { runId: 'run-2', pipelineId: 'pl-1', stepId: 'gate', approvers: [{ type: 'role', value: 'any' }] },
    ],
    resolveApproval: jest.fn(async () => undefined),
    cancelRun,
    patchRunContext,
    getDLQ: () => dlq,
    getInspector: () => inspector,
  } as never);
  mockImageMeta.mockImplementation(async (id: string) => ({
    'img-own': { uploadedBy: 'dev-owner' },
    'img-output': {},
  } as Record<string, unknown>)[id] ?? null);
});
afterAll(() => { setRunAccessDeps(null); setPipelineBridge(null); });

// ── harness (no supertest; the documentAgentEdit.test.ts style) ─────────────
const app = express();
app.use(express.json());
app.use('/api/pipelines/runs/active', pipelineActiveRunsRouter);
app.use('/api/pipelines/approvals', pipelinePendingApprovalsRouter);
app.use('/api/pipelines/dlq', pipelineDLQRouter);
app.use('/api/pipelines/inspector', pipelineInspectorRouter);
app.use('/api/pipelines/:pipelineId/runs', pipelineTriggersRouter);
app.use('/api/pipelines/:pipelineId/runs/:runId/audit', pipelineRunAuditRouter);
app.use('/api/pipelines/:pipelineId/runs/:runId', pipelineRunActionsRouter);
app.use('/api/pipelines/:pipelineId/runs/:runId/breakpoint/:stepId/resolve', pipelineBreakpointRouter);
app.use('/api/pipelines/:pipelineId/runs/:runId/breakpoint/:stepId/patch', pipelineBreakpointPatchRouter);
app.use('/api/pipelines/:runId/cancel', pipelineCancelRouter);
app.use('/api/pipelines/:runId/approvals', pipelineApprovalsRouter);
app.use('/api/images', imagesRouter);
app.use('/api/judgments', judgmentsRouter);
app.use(errorHandler);

interface MockRes { statusCode: number; body: unknown; ended: boolean; finished: Promise<void>; status(c: number): MockRes; json(b: unknown): MockRes; send(b: unknown): MockRes; setHeader(): MockRes; getHeader(): unknown; set(): MockRes; end(): MockRes }
function mockRes(): MockRes {
  let resolve!: () => void;
  const finished = new Promise<void>((r) => { resolve = r; });
  return {
    statusCode: 200, body: undefined, ended: false, finished,
    status(c) { this.statusCode = c; return this; },
    json(b) { this.body = b; this.ended = true; resolve(); return this; },
    send(b) { this.body = b; this.ended = true; resolve(); return this; },
    setHeader() { return this; }, getHeader() { return undefined; }, set() { return this; },
    end() { this.ended = true; resolve(); return this; },
  };
}

async function call(method: string, url: string, opts: { user?: string; anonymous?: boolean; body?: unknown; headers?: Record<string, string> } = {}): Promise<MockRes> {
  const res = mockRes();
  const headers: Record<string, string> = { 'content-type': 'application/json', ...(opts.anonymous ? {} : { authorization: 'Bearer t' }), ...(opts.headers ?? {}) };
  const [path, qs] = url.split('?');
  const query = Object.fromEntries(new URLSearchParams(qs ?? ''));
  const req = {
    method, url, originalUrl: url, path, headers, query, body: opts.body ?? {},
    user: { sub: opts.user ?? 'dev-owner' }, params: {},
    header(name: string) { return headers[name.toLowerCase()]; },
    get(name: string) { return headers[name.toLowerCase()]; },
  } as unknown as Request;
  await new Promise<void>((done, reject) => {
    (app as unknown as (req: Request, res: Response, next: NextFunction) => void)(req, res as unknown as Response, (err?: unknown) => {
      if (err) { reject(err); return; }
      if (!res.ended) res.status(404).json({ error: 'route not found' });
      done();
    });
    res.finished.then(() => done()).catch(reject);
  });
  return res;
}

// ── read routes ─────────────────────────────────────────────────────────────
describe('run read — owner or document reader (NFR #173)', () => {
  const url = '/api/pipelines/pl-1/runs/run-1';
  it('owner 200, document reader 200', async () => {
    expect((await call('GET', url)).statusCode).toBe(200);
    expect((await call('GET', url, { user: 'dev-reader' })).statusCode).toBe(200);
    expect((await call('GET', `${url}/history`, { user: 'dev-reader' })).statusCode).toBe(200);
  });
  it('stranger 404, anonymous 401', async () => {
    expect((await call('GET', url, { user: 'dev-frank' })).statusCode).toBe(404);
    expect((await call('GET', `${url}/history`, { user: 'dev-frank' })).statusCode).toBe(404);
    expect((await call('GET', url, { anonymous: true })).statusCode).toBe(401);
  });
  it('a run named under another pipeline is 404, even for its owner', async () => {
    expect((await call('GET', '/api/pipelines/pl-other/runs/run-1')).statusCode).toBe(404);
  });
  it('run audit: reader 200-path, stranger 404', async () => {
    expect((await call('GET', `${url}/audit`, { user: 'dev-frank' })).statusCode).toBe(404);
    expect((await call('GET', `${url}/audit`, { anonymous: true })).statusCode).toBe(401);
  });
  it('the gateway\'s service-signed call with no person passes', async () => {
    const header = signEnvelope('', { secret: 'test-secret', serviceId: 'realtime-examples' });
    const res = await call('GET', '/api/pipelines/_/runs/run-2', { anonymous: true, headers: { [SERVICE_AUTH_HEADER.toLowerCase()]: header } });
    expect(res.statusCode).toBe(200);
  });
  it('a forged service header does not', async () => {
    const header = signEnvelope('', { secret: 'wrong', serviceId: 'realtime-examples' });
    const res = await call('GET', '/api/pipelines/_/runs/run-2', { anonymous: true, headers: { [SERVICE_AUTH_HEADER.toLowerCase()]: header } });
    expect(res.statusCode).toBe(401);
  });
});

describe('lists keep only the caller\'s runs (NFR #173)', () => {
  it('GET /pipelines/:id/runs', async () => {
    const res = await call('GET', '/api/pipelines/pl-1/runs', { user: 'dev-reader' });
    expect((res.body as { runs: Array<{ runId: string }> }).runs.map((r) => r.runId)).toEqual(['run-1']);
  });
  it('GET /pipelines/runs/active', async () => {
    const res = await call('GET', '/api/pipelines/runs/active', { user: 'dev-frank' });
    expect((res.body as { runs: Array<{ runId: string }> }).runs.map((r) => r.runId)).toEqual(['run-2']);
  });
  it('GET /pipelines/approvals', async () => {
    const res = await call('GET', '/api/pipelines/approvals', { user: 'dev-owner' });
    expect((res.body as { approvals: Array<{ runId: string }> }).approvals.map((r) => r.runId)).toEqual(['run-1']);
  });
});

// ── owner routes ────────────────────────────────────────────────────────────
describe('run control — owner only (NFR #173)', () => {
  it('cancel: owner reaches the executor; reader 403; stranger 404; anonymous 401', async () => {
    const owner = await call('POST', '/api/pipelines/run-1/cancel');
    expect([401, 403, 404]).not.toContain(owner.statusCode);
    expect(cancelRun).toHaveBeenCalledWith('run-1', 'dev-owner');
    cancelRun.mockClear();
    expect((await call('POST', '/api/pipelines/run-1/cancel', { user: 'dev-reader' })).statusCode).toBe(403);
    expect((await call('POST', '/api/pipelines/run-1/cancel', { user: 'dev-frank' })).statusCode).toBe(404);
    expect((await call('POST', '/api/pipelines/run-1/cancel', { anonymous: true })).statusCode).toBe(401);
    expect(cancelRun).not.toHaveBeenCalled();
  });
  it('force-fail, breakpoint resolve, breakpoint patch: reader 403, stranger 404', async () => {
    for (const [method, url] of [
      ['POST', '/api/pipelines/pl-1/runs/run-1/force-fail'],
      ['POST', '/api/pipelines/pl-1/runs/run-1/breakpoint/s1/resolve'],
      ['POST', '/api/pipelines/pl-1/runs/run-1/breakpoint/s1/patch'],
      ['DELETE', '/api/pipelines/pl-1/runs/run-1/breakpoint/s1/patch/p1'],
    ]) {
      expect((await call(method, url, { user: 'dev-reader' })).statusCode).toBe(403);
      expect((await call(method, url, { user: 'dev-frank' })).statusCode).toBe(404);
      expect((await call(method, url, { anonymous: true })).statusCode).toBe(401);
    }
    expect(patchRunContext).not.toHaveBeenCalled();
  });
});

describe('approval gate — fail closed (NFR #173)', () => {
  const body = { stepId: 'gate', decision: 'approve' };
  it('a gate with no user approvers is the run owner\'s: reader 403, stranger 404, anonymous 401', async () => {
    expect((await call('POST', '/api/pipelines/run-1/approvals', { user: 'dev-reader', body })).statusCode).toBe(403);
    expect((await call('POST', '/api/pipelines/run-1/approvals', { user: 'dev-frank', body })).statusCode).toBe(404);
    expect((await call('POST', '/api/pipelines/run-1/approvals', { anonymous: true, body })).statusCode).toBe(401);
    const owner = await call('POST', '/api/pipelines/run-1/approvals', { body });
    expect([401, 403, 404]).not.toContain(owner.statusCode);
  });
});

// ── operator surfaces (NFR #174) ────────────────────────────────────────────
describe('DLQ and inspector — only the caller\'s runs (NFR #174)', () => {
  it('list and peek', async () => {
    const list = await call('GET', '/api/pipelines/dlq', { user: 'dev-owner' });
    expect((list.body as { items: Array<{ id: string }> }).items.map((e) => e.id)).toEqual(['dl-1']);
    expect((await call('GET', '/api/pipelines/dlq/peek/dl-2', { user: 'dev-owner' })).statusCode).toBe(404);
    expect((await call('GET', '/api/pipelines/dlq/peek/dl-1', { user: 'dev-reader' })).statusCode).toBe(200);
  });
  it('redrive/purge: owner ok, reader 403, stranger 404 — nothing redriven for them', async () => {
    expect((await call('POST', '/api/pipelines/dlq/purge', { body: { ids: ['dl-1'] } })).statusCode).toBe(200);
    dlq.purge.mockClear();
    expect((await call('POST', '/api/pipelines/dlq/redrive', { user: 'dev-reader', body: { ids: ['dl-1'] } })).statusCode).toBe(403);
    expect((await call('POST', '/api/pipelines/dlq/purge', { user: 'dev-owner', body: { ids: ['dl-1', 'dl-2'] } })).statusCode).toBe(404);
    expect(dlq.purge).not.toHaveBeenCalled();
    expect(dlq.redrive).not.toHaveBeenCalled();
  });
  it('inspector pending is filtered; peek of a stranger\'s run is 404', async () => {
    const res = await call('GET', '/api/pipelines/inspector/pending', { user: 'dev-owner' });
    expect((res.body as { items: Array<{ id: string }> }).items.map((e) => e.id)).toEqual(['e1']);
    expect((await call('GET', '/api/pipelines/inspector/peek/run-2', { user: 'dev-owner' })).statusCode).toBe(404);
    expect((await call('GET', '/api/pipelines/inspector/peek/run-1', { anonymous: true })).statusCode).toBe(401);
  });
});

describe('images — an upload is its uploader\'s (NFR #174)', () => {
  it('uploader 200, stranger 404, anonymous 401; a pipeline output stays readable', async () => {
    expect((await call('GET', '/api/images/img-own')).statusCode).toBe(200);
    expect((await call('GET', '/api/images/img-own/metadata', { user: 'dev-frank' })).statusCode).toBe(404);
    expect((await call('GET', '/api/images/img-own', { user: 'dev-frank' })).statusCode).toBe(404);
    expect((await call('GET', '/api/images/img-own', { anonymous: true })).statusCode).toBe(401);
    expect((await call('GET', '/api/images/img-output', { user: 'dev-frank' })).statusCode).toBe(200);
  });
});

describe('judgments — through their run (NFR #173)', () => {
  it('owner/reader 200, stranger 404, anonymous 401', async () => {
    expect((await call('GET', '/api/judgments/j-1/raw-text')).statusCode).toBe(200);
    expect((await call('GET', '/api/judgments/j-1/raw-text', { user: 'dev-reader' })).statusCode).toBe(200);
    expect((await call('GET', '/api/judgments/j-1/raw-text', { user: 'dev-frank' })).statusCode).toBe(404);
    expect((await call('GET', '/api/judgments/j-1/raw-text', { anonymous: true })).statusCode).toBe(401);
  });
});

describe('run channel access binds to the run\'s own document (NFR #173)', () => {
  const deps = {
    liveRun: async () => ({ ownerId: 'dev-owner', documentId: 'doc-private' }),
    documentRow: async (id: string) => ((id === 'doc-mine' ? { documentId: id, createdBy: 'dev-frank' } : { documentId: id, createdBy: 'dev-owner' }) as never),
  };
  it('naming a document the caller can read does not open someone else\'s run', async () => {
    const out = await decidePipelineAccess({ organizationId: 'o', userId: 'dev-frank', runId: 'run-x', documentId: 'doc-mine' }, deps);
    expect(out.allow).toBe(false);
  });
});
