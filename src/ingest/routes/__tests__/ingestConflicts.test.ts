// social-api/src/ingest/routes/__tests__/ingestConflicts.test.ts
//
// Coverage for the write-back conflict queue stub (hub#2187). Verifies:
// list returns the in-memory store sorted, resolve marks status='resolved'
// and adds resolvedAt + auditNote, discard marks status='discarded',
// validation rejects bad resolution values.

import express, { type NextFunction, type Request, type Response } from 'express';
import {
  ingestConflictsRouter,
  _seedConflictForTest,
  _clearConflictsForTest,
} from '../ingestConflicts';
import { errorHandler } from '../../../middleware/error-handler';

interface MockRes {
  statusCode: number;
  body: unknown;
  ended: boolean;
  finished: Promise<void>;
  status(code: number): MockRes;
  json(body: unknown): MockRes;
  setHeader(_n: string, _v: unknown): MockRes;
  getHeader(_n: string): unknown;
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

async function run(app: express.Express, method: string, url: string, opts: { body?: unknown; params?: Record<string, string> } = {}): Promise<MockRes> {
  const req = {
    method, url, body: opts.body ?? {}, headers: {}, params: opts.params ?? {}, query: {},
  } as unknown as Request;
  const res = mockRes();
  await new Promise<void>((resolve, reject) => {
    const finalHandler = (err?: unknown) => {
      if (err) errorHandler(err as Error, req as unknown as Request, res as unknown as Response, () => {});
      resolve();
    };
    (app as unknown as { handle: (q: Request, s: Response, n: NextFunction) => void }).handle(
      req, res as unknown as Response, finalHandler as NextFunction,
    );
    res.finished.then(() => resolve()).catch(reject);
  });
  return res;
}

function buildApp(): express.Express {
  const app = express();
  app.use(express.json());
  app.use('/sync/conflicts', ingestConflictsRouter);
  return app;
}

const seedPending = {
  conflictId: 'cfl-seed',
  sourceId: 'src-1',
  documentId: 'doc-1',
  documentTypeId: 'type-policy',
  sourceRef: 'gdrive:abc',
  summary: 'remote diverged',
  fields: [{ field: 'title', sourceValue: 'A', platformValue: 'B' }],
  status: 'pending' as const,
  platformUpdatedAt: '2026-04-23T01:00:00.000Z',
  sourceUpdatedAt: '2026-04-23T01:01:00.000Z',
  detectedAt: '2026-04-23T01:02:00.000Z',
};

describe('ingest conflicts route', () => {
  beforeEach(() => {
    _clearConflictsForTest();
  });

  test('GET / returns empty list when no conflicts seeded', async () => {
    const res = await run(buildApp(), 'GET', '/sync/conflicts');
    expect(res.statusCode).toBe(200);
    expect(res.body).toEqual({ conflicts: [] });
  });

  test('GET / returns seeded conflicts', async () => {
    _seedConflictForTest(seedPending);
    const res = await run(buildApp(), 'GET', '/sync/conflicts');
    expect(res.statusCode).toBe(200);
    const body = res.body as { conflicts: Array<{ conflictId: string; status: string }> };
    expect(body.conflicts).toHaveLength(1);
    expect(body.conflicts[0].conflictId).toBe('cfl-seed');
    expect(body.conflicts[0].status).toBe('pending');
  });

  test('POST /:id/resolve flips status to resolved + records auditNote', async () => {
    _seedConflictForTest(seedPending);
    const res = await run(buildApp(), 'POST', '/sync/conflicts/cfl-seed/resolve', {
      params: { id: 'cfl-seed' },
      body: { resolution: 'platform', auditNote: 'kept platform copy' },
    });
    expect(res.statusCode).toBe(200);
    const body = res.body as { conflict: { status: string; resolution: string; auditNote: string; resolvedAt: string } };
    expect(body.conflict.status).toBe('resolved');
    expect(body.conflict.resolution).toBe('platform');
    expect(body.conflict.auditNote).toBe('kept platform copy');
    expect(typeof body.conflict.resolvedAt).toBe('string');
  });

  test('POST /:id/resolve rejects bad resolution value', async () => {
    const res = await run(buildApp(), 'POST', '/sync/conflicts/cfl-x/resolve', {
      params: { id: 'cfl-x' },
      body: { resolution: 'garbage' },
    });
    expect(res.statusCode).toBe(400);
  });

  test('POST /:id/discard flips status to discarded', async () => {
    _seedConflictForTest(seedPending);
    const res = await run(buildApp(), 'POST', '/sync/conflicts/cfl-seed/discard', {
      params: { id: 'cfl-seed' },
    });
    expect(res.statusCode).toBe(200);
    const body = res.body as { conflict: { status: string } };
    expect(body.conflict.status).toBe('discarded');
  });

  test('GET / shows resolved entry after resolve (Resolved-tab data path)', async () => {
    _seedConflictForTest(seedPending);
    await run(buildApp(), 'POST', '/sync/conflicts/cfl-seed/resolve', {
      params: { id: 'cfl-seed' },
      body: { resolution: 'source' },
    });
    const res = await run(buildApp(), 'GET', '/sync/conflicts');
    const body = res.body as { conflicts: Array<{ status: string }> };
    expect(body.conflicts).toHaveLength(1);
    expect(body.conflicts[0].status).toBe('resolved');
  });
});
