// social-api/src/ingest/routes/__tests__/ingestAudit.test.ts
//
// Coverage for the audit-log read route (hub#2214). Verifies filter
// dispatch: targetId → listByTarget GSI, actor → listByActor GSI,
// otherwise → listAll with since/until/eventType pass-through. Bad
// eventType is silently dropped (not 400-ed).

import express, { type NextFunction, type Request, type Response } from 'express';

const listAll = jest.fn(async () => [] as unknown[]);
const listByTarget = jest.fn(async () => [] as unknown[]);
const listByActor = jest.fn(async () => [] as unknown[]);

jest.mock('../../repositories', () => ({
  ingestAuditRepo: {
    listAll: (...args: Parameters<typeof listAll>) => listAll(...args),
    listByTarget: (...args: Parameters<typeof listByTarget>) => listByTarget(...args),
    listByActor: (...args: Parameters<typeof listByActor>) => listByActor(...args),
  },
}));

import { ingestAuditRouter } from '../ingestAudit';
import { errorHandler } from '../../../middleware/error-handler';

interface MockRes {
  statusCode: number; body: unknown; ended: boolean; finished: Promise<void>;
  status(c: number): MockRes; json(b: unknown): MockRes;
  setHeader(_n: string, _v: unknown): MockRes; getHeader(_n: string): unknown;
  end(...a: unknown[]): MockRes;
}

function mockRes(): MockRes {
  let resolveFinished!: () => void;
  const finished = new Promise<void>((r) => { resolveFinished = r; });
  const res: MockRes = {
    statusCode: 200, body: undefined, ended: false, finished,
    status(c) { this.statusCode = c; return this; },
    json(b) { this.body = b; this.ended = true; resolveFinished(); return this; },
    setHeader() { return this; }, getHeader() { return undefined; },
    end() { this.ended = true; resolveFinished(); return this; },
  };
  return res;
}

async function run(app: express.Express, method: string, url: string, opts: { query?: Record<string, string> } = {}): Promise<MockRes> {
  const req = {
    method, url, body: {}, headers: {}, params: {}, query: opts.query ?? {},
  } as unknown as Request;
  const res = mockRes();
  await new Promise<void>((resolveP, reject) => {
    const finalHandler = (err?: unknown) => {
      if (err) errorHandler(err as Error, req as unknown as Request, res as unknown as Response, () => {});
      resolveP();
    };
    (app as unknown as { handle: (q: Request, s: Response, n: NextFunction) => void }).handle(
      req, res as unknown as Response, finalHandler as NextFunction,
    );
    res.finished.then(() => resolveP()).catch(reject);
  });
  return res;
}

function buildApp(): express.Express {
  const app = express();
  app.use(express.json());
  app.use('/ingest/audit', ingestAuditRouter);
  return app;
}

describe('GET /ingest/audit', () => {
  beforeEach(() => {
    listAll.mockReset(); listAll.mockResolvedValue([]);
    listByTarget.mockReset(); listByTarget.mockResolvedValue([]);
    listByActor.mockReset(); listByActor.mockResolvedValue([]);
  });

  test('no filters → listAll with default limit 100 + no filter args', async () => {
    const res = await run(buildApp(), 'GET', '/ingest/audit');
    expect(res.statusCode).toBe(200);
    expect(listAll).toHaveBeenCalledWith({
      limit: 100,
      since: undefined,
      until: undefined,
      eventType: undefined,
    });
    expect(listByTarget).not.toHaveBeenCalled();
    expect(listByActor).not.toHaveBeenCalled();
  });

  test('?targetId=X dispatches to listByTarget with the limit', async () => {
    const res = await run(buildApp(), 'GET', '/ingest/audit', { query: { targetId: 'src-1', limit: '50' } });
    expect(res.statusCode).toBe(200);
    expect(listByTarget).toHaveBeenCalledWith('src-1', 50);
    expect(listAll).not.toHaveBeenCalled();
  });

  test('?actor=X dispatches to listByActor', async () => {
    const res = await run(buildApp(), 'GET', '/ingest/audit', { query: { actor: 'op@example.com' } });
    expect(res.statusCode).toBe(200);
    expect(listByActor).toHaveBeenCalledWith('op@example.com', 100);
  });

  test('?since + ?until + ?eventType pass through to listAll', async () => {
    const res = await run(buildApp(), 'GET', '/ingest/audit', {
      query: { since: '2026-04-23T00:00:00Z', until: '2026-04-24T00:00:00Z', eventType: 'source.paused' },
    });
    expect(res.statusCode).toBe(200);
    expect(listAll).toHaveBeenCalledWith({
      limit: 100,
      since: '2026-04-23T00:00:00Z',
      until: '2026-04-24T00:00:00Z',
      eventType: 'source.paused',
    });
  });

  test('invalid eventType silently dropped', async () => {
    await run(buildApp(), 'GET', '/ingest/audit', { query: { eventType: 'garbage.type' } });
    expect(listAll).toHaveBeenCalledWith({
      limit: 100,
      since: undefined,
      until: undefined,
      eventType: undefined,
    });
  });

  test('targetId beats actor when both present (more selective wins)', async () => {
    await run(buildApp(), 'GET', '/ingest/audit', { query: { targetId: 'doc-1', actor: 'op' } });
    expect(listByTarget).toHaveBeenCalled();
    expect(listByActor).not.toHaveBeenCalled();
  });
});
