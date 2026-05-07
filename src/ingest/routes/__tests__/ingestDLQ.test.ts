// social-api/src/ingest/routes/__tests__/ingestDLQ.test.ts
//
// Coverage for the ingest DLQ inspect/replay/purge route (hub#2189).
// Verifies: 503 when DLQ not wired, list happy path, replay redrives,
// delete purges, 404 when entry missing.

import express, { type NextFunction, type Request, type Response } from 'express';
import { setIngestDLQ } from '../../dlq-singleton';
import { ingestDLQRouter } from '../ingestDLQ';
import { errorHandler } from '../../../middleware/error-handler';

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
  let resolveFinished!: () => void;
  const finished = new Promise<void>((resolve) => { resolveFinished = resolve; });
  const res: MockRes = {
    statusCode: 200,
    body: undefined,
    ended: false,
    finished,
    status(code: number) { this.statusCode = code; return this; },
    json(b: unknown) { this.body = b; this.ended = true; resolveFinished(); return this; },
    setHeader() { return this; },
    getHeader() { return undefined; },
    end() { this.ended = true; resolveFinished(); return this; },
  };
  return res;
}

async function run(app: express.Express, method: string, url: string, opts: { body?: unknown; params?: Record<string, string> } = {}): Promise<MockRes> {
  const req = {
    method,
    url,
    body: opts.body ?? {},
    headers: {},
    params: opts.params ?? {},
    query: {},
  } as unknown as Request;
  const res = mockRes();
  await new Promise<void>((resolve, reject) => {
    const finalHandler = (err?: unknown) => {
      if (err) errorHandler(err as Error, req as unknown as Request, res as unknown as Response, () => {});
      resolve();
    };
    (app as unknown as { handle: (req: Request, res: Response, next: NextFunction) => void }).handle(
      req,
      res as unknown as Response,
      finalHandler as NextFunction,
    );
    res.finished.then(() => resolve()).catch(reject);
  });
  return res;
}

function buildApp(): express.Express {
  const app = express();
  app.use(express.json());
  app.use('/ingest/dlq', ingestDLQRouter);
  return app;
}

describe('ingest DLQ routes', () => {
  afterEach(() => {
    setIngestDLQ(null);
  });

  test('GET / returns 503 when DLQ is not wired', async () => {
    setIngestDLQ(null);
    const res = await run(buildApp(), 'GET', '/ingest/dlq');
    expect(res.statusCode).toBe(503);
    expect(res.body).toEqual({ error: 'ingest DLQ not initialized' });
  });

  test('GET / returns the DLQ list when wired', async () => {
    const fake = {
      list: jest.fn(async () => ({
        items: [
          { id: 'env-1', envelope: { id: 'env-1', body: { itemId: 'i-1' } }, lastError: 'boom', failedAtMs: 1, totalAttempts: 3 },
        ],
        nextCursor: undefined,
      })),
    } as never;
    setIngestDLQ(fake);

    const res = await run(buildApp(), 'GET', '/ingest/dlq');
    expect(res.statusCode).toBe(200);
    const body = res.body as { items: Array<{ id: string }>; nextCursor: string | null };
    expect(body.items).toHaveLength(1);
    expect(body.items[0].id).toBe('env-1');
    expect(body.nextCursor).toBeNull();
  });

  test('POST /:id/replay calls redrive and returns ok=true', async () => {
    const redrive = jest.fn(async () => ({ redriven: 1, failed: {} }));
    setIngestDLQ({ list: jest.fn(), redrive, purge: jest.fn() } as never);

    const res = await run(buildApp(), 'POST', '/ingest/dlq/env-1/replay', { params: { id: 'env-1' } });
    expect(res.statusCode).toBe(200);
    const body = res.body as { ok: boolean; redriven: number };
    expect(body.ok).toBe(true);
    expect(body.redriven).toBe(1);
    expect(redrive).toHaveBeenCalledWith(['env-1']);
  });

  test('POST /:id/replay 404 when redrive removes nothing', async () => {
    const redrive = jest.fn(async () => ({ redriven: 0, failed: {} }));
    setIngestDLQ({ list: jest.fn(), redrive, purge: jest.fn() } as never);

    const res = await run(buildApp(), 'POST', '/ingest/dlq/env-missing/replay', { params: { id: 'env-missing' } });
    expect(res.statusCode).toBe(404);
  });

  test('DELETE /:id calls purge and returns ok=true', async () => {
    const purge = jest.fn(async () => ({ purged: 1 }));
    setIngestDLQ({ list: jest.fn(), redrive: jest.fn(), purge } as never);

    const res = await run(buildApp(), 'DELETE', '/ingest/dlq/env-2', { params: { id: 'env-2' } });
    expect(res.statusCode).toBe(200);
    const body = res.body as { ok: boolean; purged: number };
    expect(body.ok).toBe(true);
    expect(body.purged).toBe(1);
    expect(purge).toHaveBeenCalledWith(['env-2']);
  });

  test('DELETE /:id 404 when entry not found', async () => {
    const purge = jest.fn(async () => ({ purged: 0 }));
    setIngestDLQ({ list: jest.fn(), redrive: jest.fn(), purge } as never);

    const res = await run(buildApp(), 'DELETE', '/ingest/dlq/env-missing', { params: { id: 'env-missing' } });
    expect(res.statusCode).toBe(404);
  });
});
