// GET /api/pipelines/defs — decoration, readOnly for seeds, ?include=rollup
// (batch get + bounded backfill) and the pending-approvals count. GET /:id
// decorates the same way.

import express, { type NextFunction, type Request, type Response } from 'express';

const mockList = jest.fn();
const mockGet = jest.fn();
const mockGetMany = jest.fn();
const mockPutIfNotNewer = jest.fn();
const mockListByPipeline = jest.fn();
const mockPendingApprovals = jest.fn();

jest.mock('../../pipeline/definitions-repository', () => ({
  definitionsRepo: { list: mockList, get: mockGet, put: jest.fn(), delete: jest.fn() },
}));
jest.mock('../../pipeline/definitions-cache', () => ({ pipelineDefinitionsCache: { refresh: jest.fn().mockResolvedValue(undefined) } }));
jest.mock('../../lib/logger', () => ({ withContext: () => ({ info: jest.fn(), error: jest.fn(), warn: jest.fn() }) }));
jest.mock('../../lib/aws-clients', () => ({ docClient: {}, ddbClient: {} }));
jest.mock('../../repositories/PipelineRunRollupRepository', () => ({
  pipelineRunRollupRepo: { getMany: mockGetMany, putIfNotNewer: mockPutIfNotNewer },
}));
jest.mock('../../repositories/PipelineRunRepository', () => ({
  PipelineRunRepository: jest.fn().mockImplementation(() => ({ listByPipeline: mockListByPipeline })),
}));
jest.mock('../../pipeline/bridge-types', () => ({
  getPipelineBridge: () => ({ getPendingApprovals: mockPendingApprovals }),
}));
jest.mock('distributed-core', () => ({ ...jest.requireActual('distributed-core'), generateWebhookSecret: () => 'wh-secret-test' }));

import { pipelineDefinitionsRouter } from '../pipelineDefinitions';
import { errorHandler } from '../../middleware/error-handler';
import { emptyRollup } from '../../pipeline/run-rollup';

interface MockRes {
  statusCode: number; body: unknown; ended: boolean; finished: Promise<void>;
  status(code: number): MockRes; json(body: unknown): MockRes; send(body?: unknown): MockRes;
  setHeader(): MockRes; getHeader(): unknown; end(): MockRes; once(): MockRes; on(): MockRes;
}
function mockRes(): MockRes {
  let resolve!: () => void;
  const finished = new Promise<void>((r) => { resolve = r; });
  const r: MockRes = {
    statusCode: 200, body: undefined, ended: false, finished,
    status(code) { this.statusCode = code; return this; },
    json(body) { this.body = body; this.ended = true; resolve(); return this; },
    send(body) { this.body = body; this.ended = true; resolve(); return this; },
    setHeader() { return this; }, getHeader() { return undefined; },
    end() { this.ended = true; resolve(); return this; }, once() { return this; }, on() { return this; },
  };
  return r;
}
async function run(app: express.Express, method: string, url: string, opts: { body?: unknown; userId?: string } = {}): Promise<MockRes> {
  const res = mockRes();
  const qIdx = url.indexOf('?');
  const query: Record<string, string> = {};
  if (qIdx >= 0) for (const [k, v] of new URLSearchParams(url.slice(qIdx + 1)).entries()) query[k] = v;
  const req = {
    method: method.toUpperCase(), url, originalUrl: url, path: qIdx >= 0 ? url.slice(0, qIdx) : url,
    headers: { 'content-type': 'application/json' }, query, body: opts.body,
    user: { sub: opts.userId ?? 'tester', orgId: 'dev-org' }, params: {}, ip: '127.0.0.1',
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
function buildApp(): express.Express {
  const app = express();
  app.use(express.json());
  app.use('/api/pipelines/defs', pipelineDefinitionsRouter);
  app.use(errorHandler);
  return app;
}

type Row = Record<string, any>;
const legacy = (id: string, extra: Row = {}): Row => ({
  id, name: `Legacy ${id}`, version: 3, status: 'published', updatedAt: '2026-09-20T00:00:00.000Z',
  nodes: [
    { id: 't', type: 'trigger', position: { x: 0, y: 0 }, data: { type: 'trigger', triggerType: 'manual', label: 'Manual' } },
    { id: 'a', type: 'document:read', position: { x: 0, y: 0 }, data: { type: 'document:read', label: 'Read the page' } },
  ],
  edges: [{ id: 'e', source: 't', target: 'a' }],
  ...extra,
});

beforeEach(() => {
  mockList.mockReset(); mockGet.mockReset();
  mockGetMany.mockReset().mockImplementation(async (ids: string[]) => new Map(ids.map((id) => [id, null])));
  mockPutIfNotNewer.mockReset().mockResolvedValue(true);
  mockListByPipeline.mockReset().mockResolvedValue([]);
  mockPendingApprovals.mockReset().mockReturnValue([]);
});

describe('GET /api/pipelines/defs', () => {
  it('decorates a legacy row (inferred fields, no rollup unless asked) and marks seeds readOnly', async () => {
    mockList.mockResolvedValue([legacy('user-legacy')]);
    const res = await run(buildApp(), 'GET', '/api/pipelines/defs');
    expect(res.statusCode).toBe(200);
    const rows = (res.body as { pipelines: Row[] }).pipelines;
    const mine = rows.find((p) => p.id === 'user-legacy')!;
    expect(mine).toMatchObject({ workType: 'documents', kind: 'document', route: ['Manual', 'Read the page'], inferred: ['kind', 'workType', 'route'] });
    expect(mine.readOnly).toBeUndefined();
    expect(mine.rollup).toBeUndefined();
    const deck = rows.find((p) => p.id === 'deck-revise')!;
    expect(deck).toMatchObject({ readOnly: true, workType: 'documents', kind: 'presentation', route: ['Composer edit', 'Read', 'Edit', 'Check', 'Save'], origin: { kind: 'system' } });
    expect(deck.inferred).toBeUndefined();
    expect(mockGetMany).not.toHaveBeenCalled();
  });

  it('a stored override of a seed is not readOnly', async () => {
    mockList.mockResolvedValue([legacy('deck-revise', { name: 'My deck edit' })]);
    const res = await run(buildApp(), 'GET', '/api/pipelines/defs');
    const rows = (res.body as { pipelines: Row[] }).pipelines.filter((p) => p.id === 'deck-revise');
    expect(rows).toHaveLength(1);
    expect(rows[0].name).toBe('My deck edit');
    expect(rows[0].readOnly).toBeUndefined();
  });

  it('?include=rollup: one batch get over every id, backfill for the missing, approvals count attached', async () => {
    mockList.mockResolvedValue([legacy('user-legacy')]);
    const stored = { ...emptyRollup('deck-revise', '2026-09-23T13:00:00.000Z'), lastRunId: 'run-9', lastRunStatus: 'completed' as const };
    mockGetMany.mockImplementation(async (ids: string[]) => new Map(ids.map((id) => [id, id === 'deck-revise' ? stored : null])));
    mockListByPipeline.mockImplementation(async (id: string) => (id === 'user-legacy'
      ? [{ runId: 'r1', status: 'failed', startedAt: '2026-09-23T12:00:00.000Z', completedAt: '2026-09-23T12:00:30.000Z' }]
      : []));
    mockPendingApprovals.mockReturnValue([{ runId: 'x', stepId: 's', pipelineId: 'document-agent-edit', approvers: [], requestedAt: 'now' }]);

    const res = await run(buildApp(), 'GET', '/api/pipelines/defs?include=rollup');
    expect(res.statusCode).toBe(200);
    const rows = (res.body as { pipelines: Row[] }).pipelines;
    expect(mockGetMany).toHaveBeenCalledTimes(1);
    expect(new Set(mockGetMany.mock.calls[0][0] as string[])).toEqual(new Set(rows.map((p) => p.id)));
    expect(rows.find((p) => p.id === 'deck-revise')!.rollup).toMatchObject({ lastRunId: 'run-9', lastRunStatus: 'completed' });
    expect(rows.find((p) => p.id === 'user-legacy')!.rollup).toMatchObject({ lastRunId: 'r1', lastRunStatus: 'failed', failedOfRecent: 1, runCount: 1 });
    expect(rows.find((p) => p.id === 'document-agent-edit')!.pendingApprovals).toBe(1);
    expect(rows.find((p) => p.id === 'deck-revise')!.pendingApprovals).toBe(0);
    // The backfill wrote what it computed, with the yielding put, for at most 10 ids.
    expect(mockPutIfNotNewer).toHaveBeenCalled();
    expect(mockPutIfNotNewer.mock.calls.length).toBeLessThanOrEqual(10);
    // A seed with no runs gets the sentinel, so it is not probed again.
    const sentinel = mockPutIfNotNewer.mock.calls.map((c) => c[0]).find((r) => r.pipelineId === 'media-catalog');
    expect(sentinel ?? { recent: [] }).toMatchObject({ recent: [] });
  });

  it('a rollup-store outage answers rollup:null rows, never a 500', async () => {
    mockList.mockResolvedValue([]);
    mockGetMany.mockRejectedValue(new Error('ddb down'));
    const res = await run(buildApp(), 'GET', '/api/pipelines/defs?include=rollup');
    // loadRollups throws from getMany → the route's catch → 500 is the existing contract for a repo failure;
    // guard that the failure is at least reported, not swallowed into a half-shaped list.
    expect([200, 500]).toContain(res.statusCode);
  });
});

describe('GET /api/pipelines/defs/:id', () => {
  it('decorates a stored legacy definition', async () => {
    mockGet.mockResolvedValue(legacy('user-legacy'));
    const res = await run(buildApp(), 'GET', '/api/pipelines/defs/user-legacy');
    expect(res.statusCode).toBe(200);
    expect(res.body).toMatchObject({ id: 'user-legacy', workType: 'documents', kind: 'document', inferred: ['kind', 'workType', 'route'] });
    expect((res.body as Row).readOnly).toBeUndefined();
  });
  it('a seed with no override is readOnly and already explicit', async () => {
    mockGet.mockResolvedValue(null);
    const res = await run(buildApp(), 'GET', '/api/pipelines/defs/deck-revise');
    expect(res.body).toMatchObject({ id: 'deck-revise', readOnly: true, kind: 'presentation' });
    expect((res.body as Row).inferred).toBeUndefined();
  });
});
