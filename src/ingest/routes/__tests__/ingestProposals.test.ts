// social-api/src/ingest/routes/__tests__/ingestProposals.test.ts
//
// Coverage for the DocType-proposal queue route (hub#2184). Verifies
// list (with + without sourceId filter), promote happy path, reject
// happy path, 404 for missing proposal.

import express, { type NextFunction, type Request, type Response } from 'express';

const listAllPending = jest.fn(async () => [] as unknown[]);
const listPendingBySource = jest.fn(async () => [] as unknown[]);
const getById = jest.fn(async (_id: string) => null as unknown);
const resolve = jest.fn(async () => {});

jest.mock('../../repositories', () => ({
  docTypeProposalRepo: {
    listAllPending: (...args: unknown[]) => listAllPending(...args),
    listPendingBySource: (...args: unknown[]) => listPendingBySource(...args),
    getById: (id: string) => getById(id),
    resolve: (...args: unknown[]) => resolve(...args),
  },
}));

import { ingestProposalsRouter } from '../ingestProposals';
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
    setHeader() { return this; },
    getHeader() { return undefined; },
    end() { this.ended = true; resolveFinished(); return this; },
  };
  return res;
}

async function run(app: express.Express, method: string, url: string, opts: { body?: unknown; params?: Record<string, string>; query?: Record<string, string> } = {}): Promise<MockRes> {
  const req = {
    method, url, body: opts.body ?? {}, headers: {},
    params: opts.params ?? {}, query: opts.query ?? {},
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
  app.use('/ingest/mapping/proposals', ingestProposalsRouter);
  return app;
}

describe('ingest proposals route', () => {
  beforeEach(() => {
    listAllPending.mockReset(); listAllPending.mockResolvedValue([]);
    listPendingBySource.mockReset(); listPendingBySource.mockResolvedValue([]);
    getById.mockReset(); getById.mockResolvedValue(null);
    resolve.mockReset();
  });

  test('GET / without sourceId calls listAllPending', async () => {
    listAllPending.mockResolvedValue([{ proposalId: 'p1', status: 'pending' }]);
    const res = await run(buildApp(), 'GET', '/ingest/mapping/proposals');
    expect(res.statusCode).toBe(200);
    const body = res.body as { proposals: Array<{ proposalId: string }> };
    expect(body.proposals).toHaveLength(1);
    expect(listAllPending).toHaveBeenCalled();
    expect(listPendingBySource).not.toHaveBeenCalled();
  });

  test('GET /?sourceId=X calls listPendingBySource', async () => {
    listPendingBySource.mockResolvedValue([{ proposalId: 'p2', sourceId: 'src-x', status: 'pending' }]);
    const res = await run(buildApp(), 'GET', '/ingest/mapping/proposals?sourceId=src-x', { query: { sourceId: 'src-x' } });
    expect(res.statusCode).toBe(200);
    expect(listPendingBySource).toHaveBeenCalledWith('src-x');
  });

  test('POST /:id/promote 404 when proposal not found', async () => {
    getById.mockResolvedValue(null);
    const res = await run(buildApp(), 'POST', '/ingest/mapping/proposals/p-x/promote', { params: { id: 'p-x' } });
    expect(res.statusCode).toBe(404);
  });

  test('POST /:id/promote calls resolve with promoted', async () => {
    const existing = { proposalId: 'p-1', status: 'pending', sourceId: 'src-1' };
    getById
      .mockResolvedValueOnce(existing)
      .mockResolvedValueOnce({ ...existing, status: 'promoted', reviewedBy: 'op' });
    const res = await run(buildApp(), 'POST', '/ingest/mapping/proposals/p-1/promote', {
      params: { id: 'p-1' }, body: { reviewedBy: 'op' },
    });
    expect(res.statusCode).toBe(200);
    expect(resolve).toHaveBeenCalledWith('p-1', 'promoted', 'op');
    const body = res.body as { proposal: { status: string; reviewedBy: string } };
    expect(body.proposal.status).toBe('promoted');
  });

  test('POST /:id/reject calls resolve with rejected', async () => {
    const existing = { proposalId: 'p-2', status: 'pending', sourceId: 'src-1' };
    getById
      .mockResolvedValueOnce(existing)
      .mockResolvedValueOnce({ ...existing, status: 'rejected' });
    const res = await run(buildApp(), 'POST', '/ingest/mapping/proposals/p-2/reject', {
      params: { id: 'p-2' },
    });
    expect(res.statusCode).toBe(200);
    expect(resolve).toHaveBeenCalledWith('p-2', 'rejected', 'operator');
  });
});
