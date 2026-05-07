// Integration tests for /api/activity route — paginated activity log.
//
// Mocks docClient at module level. Exercises pagination (limit clamping,
// base64 lastKey round-trip), error handling, and the timestamp-strip logic.

import express, { type NextFunction, type Request, type Response } from 'express';

const mockDocClientSend = jest.fn();

jest.mock('../../lib/aws-clients', () => ({
  docClient: { send: mockDocClientSend },
}));

import { activityRouter } from '../activity';
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

interface RunOpts { body?: unknown; userId?: string }

async function run(
  app: express.Express,
  method: string,
  url: string,
  opts: RunOpts = {},
): Promise<MockRes> {
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
  app.use('/api/activity', activityRouter);
  app.use(errorHandler);
  return app;
}

beforeEach(() => {
  mockDocClientSend.mockReset();
});

describe('GET /api/activity', () => {
  it('returns items with stripped timestamp suffix and null nextKey when no more pages', async () => {
    mockDocClientSend.mockResolvedValue({
      Items: [
        { eventType: 'post.created', timestamp: '2026-05-01T00:00:00Z#evt-1', detail: '{"text":"hello"}' },
        { eventType: 'reaction.added', timestamp: '2026-04-30T12:00:00Z#evt-2', detail: '{"emoji":"fire"}' },
      ],
      LastEvaluatedKey: undefined,
    });

    const res = await run(buildApp(), 'GET', '/api/activity');
    expect(res.statusCode).toBe(200);
    const body = res.body as { items: { eventType: string; timestamp: string; detail: unknown }[]; nextKey: string | null };
    expect(body.items).toHaveLength(2);
    expect(body.items[0].timestamp).toBe('2026-05-01T00:00:00Z');
    expect(body.items[0].detail).toEqual({ text: 'hello' });
    expect(body.nextKey).toBeNull();
  });

  it('returns base64-encoded nextKey when LastEvaluatedKey is present', async () => {
    const lastKey = { userId: 'tester', timestamp: '2026-04-30T00:00:00Z#evt-3' };
    mockDocClientSend.mockResolvedValue({
      Items: [{ eventType: 'test', timestamp: '2026-05-01T00:00:00Z#x', detail: '{}' }],
      LastEvaluatedKey: lastKey,
    });

    const res = await run(buildApp(), 'GET', '/api/activity');
    const body = res.body as { nextKey: string };
    expect(body.nextKey).toBe(Buffer.from(JSON.stringify(lastKey)).toString('base64'));
  });

  it('decodes lastKey query param and passes as ExclusiveStartKey', async () => {
    const startKey = { userId: 'tester', timestamp: '2026-04-29T00:00:00Z#y' };
    const encoded = Buffer.from(JSON.stringify(startKey)).toString('base64');
    mockDocClientSend.mockResolvedValue({ Items: [], LastEvaluatedKey: undefined });

    await run(buildApp(), 'GET', `/api/activity?lastKey=${encoded}`);
    const sentCommand = mockDocClientSend.mock.calls[0][0];
    expect(sentCommand.input.ExclusiveStartKey).toEqual(startKey);
  });

  it('clamps limit to 100 maximum', async () => {
    mockDocClientSend.mockResolvedValue({ Items: [], LastEvaluatedKey: undefined });
    await run(buildApp(), 'GET', '/api/activity?limit=500');
    const sentCommand = mockDocClientSend.mock.calls[0][0];
    expect(sentCommand.input.Limit).toBe(100);
  });

  it('defaults limit to 20 when not specified', async () => {
    mockDocClientSend.mockResolvedValue({ Items: [], LastEvaluatedKey: undefined });
    await run(buildApp(), 'GET', '/api/activity');
    const sentCommand = mockDocClientSend.mock.calls[0][0];
    expect(sentCommand.input.Limit).toBe(20);
  });

  it('returns 400 for malformed lastKey', async () => {
    const res = await run(buildApp(), 'GET', '/api/activity?lastKey=not-valid-base64!!!');
    expect(res.statusCode).toBe(400);
  });

  it('returns 500 when DynamoDB query fails', async () => {
    mockDocClientSend.mockRejectedValue(new Error('DDB timeout'));
    const res = await run(buildApp(), 'GET', '/api/activity');
    expect(res.statusCode).toBe(500);
  });

  it('queries with authenticated user sub as partition key', async () => {
    mockDocClientSend.mockResolvedValue({ Items: [], LastEvaluatedKey: undefined });
    await run(buildApp(), 'GET', '/api/activity', { userId: 'user-abc' });
    const sentCommand = mockDocClientSend.mock.calls[0][0];
    expect(sentCommand.input.ExpressionAttributeValues[':uid']).toBe('user-abc');
  });

  it('returns empty items array when no activity exists', async () => {
    mockDocClientSend.mockResolvedValue({ Items: undefined, LastEvaluatedKey: undefined });
    const res = await run(buildApp(), 'GET', '/api/activity');
    const body = res.body as { items: unknown[]; nextKey: null };
    expect(body.items).toEqual([]);
    expect(body.nextKey).toBeNull();
  });
});
