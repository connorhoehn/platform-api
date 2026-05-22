// social-api/src/ingest/routes/__tests__/ingestDevSeed.test.ts
//
// Coverage for the dev-only seed-fake-content route (hub#2193). Verifies the
// production gate (must 403 in NODE_ENV=production), happy path (creates N
// items via the repo), and the count-cap behavior.

import express, { type NextFunction, type Request, type Response } from 'express';

const putSpy = jest.fn(async () => {});

jest.mock('../../repositories', () => ({
  ingestItemRepo: {
    put: (...args: Parameters<typeof putSpy>) => putSpy(...args),
  },
}));

import { ingestDevSeedRouter } from '../ingestDevSeed';
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

async function run(app: express.Express, method: string, url: string, opts: { body?: unknown } = {}): Promise<MockRes> {
  const req = {
    method,
    url,
    body: opts.body ?? {},
    headers: {},
    params: {},
    query: {},
  } as unknown as Request;
  const res = mockRes();
  await new Promise<void>((resolve, reject) => {
    const finalHandler = (err?: unknown) => {
      if (err) {
        // route through the real error handler to set 500 etc.
        errorHandler(err as Error, req as unknown as Request, res as unknown as Response, () => {});
      }
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
  app.use('/ingest/dev', ingestDevSeedRouter);
  return app;
}

describe('POST /ingest/dev/seed-fake-content', () => {
  const originalEnv = process.env.NODE_ENV;
  afterEach(() => {
    process.env.NODE_ENV = originalEnv;
    putSpy.mockClear();
  });

  test('seeds N items via the repo (default count = 5)', async () => {
    process.env.NODE_ENV = 'development';
    const res = await run(buildApp(), 'POST', '/ingest/dev/seed-fake-content', { body: {} });
    expect(res.statusCode).toBe(200);
    const body = res.body as { ok: boolean; count: number; ingestIds: string[] };
    expect(body.ok).toBe(true);
    expect(body.count).toBe(5);
    expect(body.ingestIds).toHaveLength(5);
    expect(putSpy).toHaveBeenCalledTimes(5);
  });

  test('respects an explicit count and the max cap', async () => {
    process.env.NODE_ENV = 'development';
    const res = await run(buildApp(), 'POST', '/ingest/dev/seed-fake-content', {
      body: { count: 500 },
    });
    expect(res.statusCode).toBe(200);
    const body = res.body as { count: number };
    expect(body.count).toBe(100); // capped
    expect(putSpy).toHaveBeenCalledTimes(100);
  });

  test('rejects with 403 in production', async () => {
    process.env.NODE_ENV = 'production';
    const res = await run(buildApp(), 'POST', '/ingest/dev/seed-fake-content', {
      body: { count: 5 },
    });
    expect(res.statusCode).toBe(403);
    const body = res.body as { error: string };
    expect(body.error).toMatch(/disabled in production/i);
    expect(putSpy).not.toHaveBeenCalled();
  });
});
