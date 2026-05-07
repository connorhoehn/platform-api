// Phase 51 / hub#62 — integration tests for the approvals route.
//
// Mocks the repository at module level so the route logic can be
// exercised without DDB. Same rig as documentTypes.test.ts.

import express, { type NextFunction, type Request, type Response } from 'express';

interface FakeStore { [workflowId: string]: Record<string, unknown> }
const store: FakeStore = {};

jest.mock('../../repositories', () => ({
  approvalRepo: {
    create: jest.fn(async (item: Record<string, unknown>) => {
      store[item.workflowId as string] = { ...item };
    }),
    listByDocument: jest.fn(async (documentId: string) =>
      Object.values(store).filter((e) => e.documentId === documentId),
    ),
    listByStatus: jest.fn(async (status: string) =>
      Object.values(store).filter((e) => e.workflowStatus === status),
    ),
  },
}));

import { approvalsRouter } from '../approvals';
import { errorHandler } from '../../middleware/error-handler';

interface MockRes {
  statusCode: number;
  body: unknown;
  ended: boolean;
  finished: Promise<void>;
  status(code: number): MockRes;
  json(body: unknown): MockRes;
  setHeader(_name: string, _value: unknown): MockRes;
  getHeader(_name: string): unknown;
  end(...args: unknown[]): MockRes;
}

function mockRes(): MockRes {
  let resolve!: () => void;
  const finished = new Promise<void>((r) => { resolve = r; });
  const r: MockRes = {
    statusCode: 200, body: undefined, ended: false, finished,
    status(code) { this.statusCode = code; return this; },
    json(body) { this.body = body; this.ended = true; resolve(); return this; },
    setHeader() { return this; },
    getHeader() { return undefined; },
    end(..._args) { this.ended = true; resolve(); return this; },
  };
  return r;
}

interface RunOpts { body?: unknown; userId?: string }

async function run(app: express.Express, method: string, url: string, opts: RunOpts = {}): Promise<MockRes> {
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
  app.use('/api/approvals', approvalsRouter);
  app.use(errorHandler);
  return app;
}

beforeEach(() => {
  for (const k of Object.keys(store)) delete store[k];
});

describe('POST /api/approvals', () => {
  test('201 when documentId + sectionId + decision are valid', async () => {
    const res = await run(buildApp(), 'POST', '/api/approvals', {
      body: { documentId: 'doc-1', sectionId: 'sec-a', decision: 'approved', comment: 'lgtm' },
      userId: 'reviewer-1',
    });
    expect(res.statusCode).toBe(201);
    const body = res.body as {
      workflowId: string; documentId: string; sectionId: string;
      decision: string; reviewerId: string; workflowStatus: string;
    };
    expect(body.workflowId).toMatch(/.+/);
    expect(body.documentId).toBe('doc-1');
    expect(body.sectionId).toBe('sec-a');
    expect(body.decision).toBe('approved');
    expect(body.reviewerId).toBe('reviewer-1');
    expect(body.workflowStatus).toBe('approved'); // mirrored for GSI
  });

  test('400 when documentId is missing', async () => {
    const res = await run(buildApp(), 'POST', '/api/approvals', {
      body: { sectionId: 's', decision: 'approved' },
    });
    expect(res.statusCode).toBe(400);
  });

  test('400 when sectionId is missing', async () => {
    const res = await run(buildApp(), 'POST', '/api/approvals', {
      body: { documentId: 'd', decision: 'approved' },
    });
    expect(res.statusCode).toBe(400);
  });

  test('400 when decision is not in the allowed enum', async () => {
    const res = await run(buildApp(), 'POST', '/api/approvals', {
      body: { documentId: 'd', sectionId: 's', decision: 'maybe' },
    });
    expect(res.statusCode).toBe(400);
  });

  test('writes the entry into the store with reviewerName + comment', async () => {
    const res = await run(buildApp(), 'POST', '/api/approvals', {
      body: {
        documentId: 'doc-2', sectionId: 'sec-b', decision: 'changes_requested',
        reviewerName: 'Alice Chen', comment: 'please clarify',
      },
    });
    expect(res.statusCode).toBe(201);
    const stored = Object.values(store)[0] as { reviewerName: string; comment: string; decision: string };
    expect(stored.reviewerName).toBe('Alice Chen');
    expect(stored.comment).toBe('please clarify');
    expect(stored.decision).toBe('changes_requested');
  });
});

describe('GET /api/approvals', () => {
  beforeEach(() => {
    store['w1'] = { workflowId: 'w1', documentId: 'doc-1', sectionId: 's', decision: 'approved',         workflowStatus: 'approved',         reviewerId: 'u', createdAt: 't' };
    store['w2'] = { workflowId: 'w2', documentId: 'doc-1', sectionId: 's', decision: 'rejected',         workflowStatus: 'rejected',         reviewerId: 'u', createdAt: 't' };
    store['w3'] = { workflowId: 'w3', documentId: 'doc-2', sectionId: 's', decision: 'approved',         workflowStatus: 'approved',         reviewerId: 'u', createdAt: 't' };
    store['w4'] = { workflowId: 'w4', documentId: 'doc-1', sectionId: 's', decision: 'changes_requested', workflowStatus: 'changes_requested', reviewerId: 'u', createdAt: 't' };
  });

  test('?documentId returns entries for that document', async () => {
    const res = await run(buildApp(), 'GET', '/api/approvals?documentId=doc-1');
    expect(res.statusCode).toBe(200);
    const body = res.body as { items: { workflowId: string }[] };
    expect(body.items).toHaveLength(3);
  });

  test('?status returns entries by decision via the GSI mapping', async () => {
    const res = await run(buildApp(), 'GET', '/api/approvals?status=approved');
    expect(res.statusCode).toBe(200);
    const body = res.body as { items: unknown[] };
    expect(body.items).toHaveLength(2);
  });

  test('400 when neither documentId nor status is supplied', async () => {
    const res = await run(buildApp(), 'GET', '/api/approvals');
    expect(res.statusCode).toBe(400);
  });

  test('400 when status is not in the allowed enum', async () => {
    const res = await run(buildApp(), 'GET', '/api/approvals?status=maybe');
    expect(res.statusCode).toBe(400);
  });
});
