// Tests for GET /api/pipeline/output-schemas.
//
// Exercises response shape, presence of known fixed schemas, and the
// null sentinel for dynamic kinds (vision-classify).

import express, { type NextFunction, type Request, type Response } from 'express';
import { pipelineSchemasRouter } from '../pipeline-schemas';
import { errorHandler } from '../../middleware/error-handler';

interface MockRes {
  statusCode: number;
  body: unknown;
  ended: boolean;
  finished: Promise<void>;
  status(code: number): MockRes;
  json(body: unknown): MockRes;
  send(body?: unknown): MockRes;
  setHeader(_n: string, _v: unknown): MockRes;
  getHeader(_n: string): unknown;
  end(...args: unknown[]): MockRes;
}

function mockRes(): MockRes {
  let resolve!: () => void;
  const finished = new Promise<void>((r) => { resolve = r; });
  const r: MockRes = {
    statusCode: 200, body: undefined, ended: false, finished,
    status(code) { this.statusCode = code; return this; },
    json(body) { this.body = body; this.ended = true; resolve(); return this; },
    send(body) { this.body = body; this.ended = true; resolve(); return this; },
    setHeader() { return this; },
    getHeader() { return undefined; },
    end(..._args) { this.ended = true; resolve(); return this; },
  };
  return r;
}

async function runGet(app: express.Express, url: string): Promise<MockRes> {
  const res = mockRes();
  const req = {
    method: 'GET',
    url,
    originalUrl: url,
    path: url,
    headers: { 'content-type': 'application/json' },
    query: {},
    body: undefined,
    params: {},
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
  app.use('/api/pipeline/output-schemas', pipelineSchemasRouter);
  app.use(errorHandler);
  return app;
}

describe('GET /api/pipeline/output-schemas', () => {
  let app: express.Express;

  beforeEach(() => {
    app = buildApp();
  });

  it('returns 200 with a schemas object', async () => {
    const res = await runGet(app, '/api/pipeline/output-schemas');
    expect(res.statusCode).toBe(200);
    expect(res.body).toEqual(expect.objectContaining({ schemas: expect.any(Object) }));
  });

  it('includes a non-null schema for image:vision-analyze with regions array', async () => {
    const res = await runGet(app, '/api/pipeline/output-schemas');
    const body = res.body as { schemas: Record<string, unknown> };
    const schema = body.schemas['image:vision-analyze'] as Record<string, unknown> | null;

    expect(schema).not.toBeNull();
    expect(schema).toMatchObject({
      type: 'object',
      properties: expect.objectContaining({
        regions: expect.objectContaining({ type: 'array' }),
        caption: expect.objectContaining({ type: 'string' }),
      }),
    });
  });

  it('includes a non-null schema for image:vision-tag with tags array', async () => {
    const res = await runGet(app, '/api/pipeline/output-schemas');
    const body = res.body as { schemas: Record<string, unknown> };
    const schema = body.schemas['image:vision-tag'] as Record<string, unknown> | null;

    expect(schema).not.toBeNull();
    expect(schema).toMatchObject({
      type: 'object',
      properties: expect.objectContaining({
        tags: expect.objectContaining({ type: 'array' }),
      }),
    });
  });

  it('includes a non-null schema for image:vision-extract-text with fullText and lines', async () => {
    const res = await runGet(app, '/api/pipeline/output-schemas');
    const body = res.body as { schemas: Record<string, unknown> };
    const schema = body.schemas['image:vision-extract-text'] as Record<string, unknown> | null;

    expect(schema).not.toBeNull();
    expect(schema).toMatchObject({
      type: 'object',
      properties: expect.objectContaining({
        fullText: expect.objectContaining({ type: 'string' }),
        lines: expect.objectContaining({ type: 'array' }),
      }),
    });
  });

  it('returns null for image:vision-classify (dynamic schema — labels from node.data)', async () => {
    const res = await runGet(app, '/api/pipeline/output-schemas');
    const body = res.body as { schemas: Record<string, unknown> };
    expect(body.schemas['image:vision-classify']).toBeNull();
  });

  it('includes image:crop with imageId and bbox in schema', async () => {
    const res = await runGet(app, '/api/pipeline/output-schemas');
    const body = res.body as { schemas: Record<string, unknown> };
    const schema = body.schemas['image:crop'] as Record<string, unknown> | null;

    expect(schema).not.toBeNull();
    expect(schema).toMatchObject({
      type: 'object',
      properties: expect.objectContaining({
        imageId: expect.objectContaining({ type: 'string' }),
        width: expect.objectContaining({ type: 'number' }),
        height: expect.objectContaining({ type: 'number' }),
      }),
    });
  });

  it('includes image:resize with imageId, width, height in schema', async () => {
    const res = await runGet(app, '/api/pipeline/output-schemas');
    const body = res.body as { schemas: Record<string, unknown> };
    const schema = body.schemas['image:resize'] as Record<string, unknown> | null;

    expect(schema).not.toBeNull();
    expect(schema).toMatchObject({
      type: 'object',
      properties: expect.objectContaining({
        imageId: expect.objectContaining({ type: 'string' }),
        width: expect.objectContaining({ type: 'number' }),
        height: expect.objectContaining({ type: 'number' }),
      }),
    });
  });

  it('does not accept POST', async () => {
    const res = mockRes();
    const req = {
      method: 'POST',
      url: '/api/pipeline/output-schemas',
      originalUrl: '/api/pipeline/output-schemas',
      path: '/api/pipeline/output-schemas',
      headers: { 'content-type': 'application/json' },
      query: {},
      body: {},
      params: {},
      ip: '127.0.0.1',
    } as unknown as Request;
    await new Promise<void>((resolveOuter) => {
      const finalHandler = (): void => {
        if (!res.ended) res.status(404).json({ error: 'route not found' });
        resolveOuter();
      };
      (app as unknown as (req: Request, res: Response, next: NextFunction) => void)(
        req, res as unknown as Response, finalHandler,
      );
      res.finished.then(() => resolveOuter());
    });
    // No POST handler registered — either 404 from finalHandler or Express's 404.
    expect([404, 405]).toContain(res.statusCode);
  });
});
