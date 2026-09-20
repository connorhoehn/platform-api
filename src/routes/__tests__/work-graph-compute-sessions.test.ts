import express, { type NextFunction, type Request, type Response } from 'express';
import type { StoredComputeCapabilitySession } from '../../work-graph/runtime';

const mockLoad = jest.fn<Promise<StoredComputeCapabilitySession | null>, [string, string, string, string, string]>();
const mockRevalidate = jest.fn<Promise<boolean>, [StoredComputeCapabilitySession]>();
const mockUse = jest.fn<
Promise<{ allowed: true; result?: { status?: string; cancelled?: boolean } } | { allowed: false }>,
[StoredComputeCapabilitySession]
>();
const mockClose = jest.fn<Promise<void>, [StoredComputeCapabilitySession]>();

jest.mock('../../work-graph/runtime', () => ({
  loadComputeCapabilitySession: (...args: [string, string, string, string, string]) => mockLoad(...args),
  revalidateComputeCapabilitySession: (session: StoredComputeCapabilitySession) => mockRevalidate(session),
  useComputeCapabilitySession: (session: StoredComputeCapabilitySession) => mockUse(session),
  closeComputeCapabilitySession: (session: StoredComputeCapabilitySession) => mockClose(session),
}));

import { workGraphComputeSessionsRouter } from '../work-graph-compute-sessions';

interface MockRes {
  statusCode: number;
  body: unknown;
  ended: boolean;
  finished: Promise<void>;
  status(code: number): MockRes;
  json(body: unknown): MockRes;
  send(body?: unknown): MockRes;
  setHeader(name: string, value: unknown): MockRes;
  getHeader(name: string): unknown;
  end(): MockRes;
}

function mockRes(): MockRes {
  let resolve!: () => void;
  const finished = new Promise<void>((done) => { resolve = done; });
  const headers = new Map<string, unknown>();
  return {
    statusCode: 200,
    body: undefined,
    ended: false,
    finished,
    status(code) { this.statusCode = code; return this; },
    json(body) { this.body = body; this.ended = true; resolve(); return this; },
    send(body) { this.body = body; this.ended = true; resolve(); return this; },
    setHeader(name, value) { headers.set(name.toLowerCase(), value); return this; },
    getHeader(name) { return headers.get(name.toLowerCase()); },
    end() { this.ended = true; resolve(); return this; },
  };
}

async function run(
  app: express.Express,
  method: string,
  url: string,
  user: { sub: string; orgId: string },
  body?: unknown,
): Promise<MockRes> {
  const res = mockRes();
  const req = {
    method,
    url,
    originalUrl: url,
    headers: { 'content-type': 'application/json' },
    body,
    params: {},
    ip: '127.0.0.1',
    user,
  } as unknown as Request;
  await new Promise<void>((resolve, reject) => {
    const done = (error?: unknown): void => {
      if (error) reject(error);
      else {
        if (!res.ended) res.status(404).json({ error: 'route not found' });
        resolve();
      }
    };
    (app as unknown as (request: Request, response: Response, next: NextFunction) => void)(
      req,
      res as unknown as Response,
      done,
    );
    res.finished.then(resolve, reject);
  });
  return res;
}

const stored: StoredComputeCapabilitySession = {
  organizationId: 'org-1',
  viewerId: 'viewer-1',
  personId: 'owner-1',
  requestKey: 'request-1',
  sessionId: 'session-1',
  source: 'cloud-compute',
  capability: 'observe-terminal',
  expiresAt: '2099-09-19T18:00:00.000Z',
};

const unavailable = {
  error: 'WORK_GRAPH_UNAVAILABLE',
  message: 'This work item is unavailable.',
};

function setup(): express.Express {
  const app = express();
  app.use('/api/work-graph/compute-sessions', workGraphComputeSessionsRouter);
  app.use((error: unknown, _req: Request, res: Response, _next: NextFunction) => {
    res.status(500).json({ error: (error as Error).message });
  });
  return app;
}

beforeEach(() => {
  jest.clearAllMocks();
  mockLoad.mockImplementation(async (organizationId, viewerId, personId, requestKey, sessionId) => (
    organizationId === stored.organizationId
      && viewerId === stored.viewerId
      && personId === stored.personId
      && requestKey === stored.requestKey
      && sessionId === stored.sessionId
      ? stored
      : null
  ));
  mockRevalidate.mockResolvedValue(true);
  mockUse.mockResolvedValue({ allowed: true, result: { status: 'running' } });
  mockClose.mockResolvedValue(undefined);
});

describe('work graph compute session browser router', () => {
  it('returns only the safe active session shape after source revalidation', async () => {
    const result = await run(
      setup(),
      'GET',
      '/api/work-graph/compute-sessions/owner-1/request-1/session-1',
      { sub: 'viewer-1', orgId: 'org-1' },
    );

    expect(result.statusCode).toBe(200);
    expect(result.body).toEqual({
      sessionId: 'session-1',
      capability: 'observe-terminal',
      expiresAt: '2099-09-19T18:00:00.000Z',
      active: true,
    });
    expect(mockRevalidate).toHaveBeenCalledWith(stored);
  });

  it.each([
    ['viewer', '/api/work-graph/compute-sessions/owner-1/request-1/session-1', { sub: 'viewer-2', orgId: 'org-1' }],
    ['person', '/api/work-graph/compute-sessions/owner-2/request-1/session-1', { sub: 'viewer-1', orgId: 'org-1' }],
    ['request', '/api/work-graph/compute-sessions/owner-1/request-2/session-1', { sub: 'viewer-1', orgId: 'org-1' }],
    ['session', '/api/work-graph/compute-sessions/owner-1/request-1/session-2', { sub: 'viewer-1', orgId: 'org-1' }],
  ] as const)('fails closed and generically for the wrong %s', async (_kind, url, user) => {
    const result = await run(setup(), 'GET', url, user);

    expect(result.statusCode).toBe(404);
    expect(result.body).toEqual(unavailable);
    expect(JSON.stringify(result.body)).not.toMatch(/owner-1|request-1|session-1|viewer-1|cloud-compute/);
    expect(mockRevalidate).not.toHaveBeenCalled();
  });

  it('fails the active-session read generically when source revalidation is revoked', async () => {
    mockRevalidate.mockResolvedValue(false);
    const result = await run(
      setup(),
      'GET',
      '/api/work-graph/compute-sessions/owner-1/request-1/session-1',
      { sub: 'viewer-1', orgId: 'org-1' },
    );

    expect(result.statusCode).toBe(404);
    expect(result.body).toEqual(unavailable);
    expect(mockRevalidate).toHaveBeenCalledWith(stored);
  });

  it('denies a mismatched capability without invoking the source session', async () => {
    const result = await run(
      setup(),
      'POST',
      '/api/work-graph/compute-sessions/owner-1/request-1/session-1/use',
      { sub: 'viewer-1', orgId: 'org-1' },
      { capability: 'control-terminal' },
    );

    expect(result.statusCode).toBe(404);
    expect(result.body).toEqual(unavailable);
    expect(mockUse).not.toHaveBeenCalled();
  });

  it('uses only the stored exact capability and returns the source-safe result', async () => {
    const result = await run(
      setup(),
      'POST',
      '/api/work-graph/compute-sessions/owner-1/request-1/session-1/use',
      { sub: 'viewer-1', orgId: 'org-1' },
      { capability: 'observe-terminal' },
    );

    expect(result.statusCode).toBe(200);
    expect(result.body).toEqual({ capability: 'observe-terminal', result: { status: 'running' } });
    expect(mockUse).toHaveBeenCalledWith(stored);
  });

  it('fails use generically when the source revokes the live session', async () => {
    mockUse.mockResolvedValue({ allowed: false });
    const result = await run(
      setup(),
      'POST',
      '/api/work-graph/compute-sessions/owner-1/request-1/session-1/use',
      { sub: 'viewer-1', orgId: 'org-1' },
      { capability: 'observe-terminal' },
    );

    expect(result.statusCode).toBe(404);
    expect(result.body).toEqual(unavailable);
  });

  it('closes a matching session and keeps missing-session close idempotent', async () => {
    const app = setup();
    const closed = await run(
      app,
      'DELETE',
      '/api/work-graph/compute-sessions/owner-1/request-1/session-1',
      { sub: 'viewer-1', orgId: 'org-1' },
    );
    const missing = await run(
      app,
      'DELETE',
      '/api/work-graph/compute-sessions/owner-1/request-1/session-2',
      { sub: 'viewer-1', orgId: 'org-1' },
    );

    expect(closed.statusCode).toBe(204);
    expect(missing.statusCode).toBe(204);
    expect(mockClose).toHaveBeenCalledTimes(1);
    expect(mockClose).toHaveBeenCalledWith(stored);
  });
});
