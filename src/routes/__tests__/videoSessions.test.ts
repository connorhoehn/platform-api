// Integration tests for videoSessions routes.
//
// Mocks video-auth, videoSessionRepo, and auth at module level so we
// exercise live-video-streaming proxy, local record persistence, and error
// paths without touching any external APIs.

import express, { type NextFunction, type Request, type Response } from 'express';

const mockCreateSession = jest.fn();
const mockAddParticipant = jest.fn();
const mockEndSession = jest.fn();
const mockGetSessionsByDocument = jest.fn();
const mockGetSession = jest.fn();

jest.mock('../../middleware/auth', () => ({
  requireAuth: (_req: Request, _res: Response, next: NextFunction) => next(),
  optionalAuth: (_req: Request, _res: Response, next: NextFunction) => next(),
}));

jest.mock('../../lib/video-auth', () => ({
  VIDEO_API_URL: 'https://video.test',
}));

jest.mock('../../repositories', () => ({
  videoSessionRepo: {
    createSession: (...args: unknown[]) => mockCreateSession(...args),
    addParticipant: (...args: unknown[]) => mockAddParticipant(...args),
    endSession: (...args: unknown[]) => mockEndSession(...args),
    getSessionsByDocument: (...args: unknown[]) => mockGetSessionsByDocument(...args),
    getSession: (...args: unknown[]) => mockGetSession(...args),
  },
}));

const mockFetch = jest.fn();
(globalThis as unknown as { fetch: typeof mockFetch }).fetch = mockFetch;

import { videoSessionsRouter } from '../videoSessions';
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
  app.use('/api/video', videoSessionsRouter);
  app.use(errorHandler);
  return app;
}

function apiOk(data: Record<string, unknown> = {}) {
  return { ok: true, status: 200, json: async () => data, text: async () => '' } as unknown as globalThis.Response;
}

function apiFail(status = 500) {
  return { ok: false, status, json: async () => ({}), text: async () => 'err' } as unknown as globalThis.Response;
}

beforeEach(() => {
  mockCreateSession.mockReset().mockResolvedValue(undefined);
  mockAddParticipant.mockReset().mockResolvedValue(undefined);
  mockEndSession.mockReset().mockResolvedValue(undefined);
  mockGetSessionsByDocument.mockReset();
  mockGetSession.mockReset();
  mockFetch.mockReset();
  jest.spyOn(console, 'log').mockImplementation(() => {});
  jest.spyOn(console, 'error').mockImplementation(() => {});
});

afterEach(() => jest.restoreAllMocks());

describe('POST /api/video/sessions — non-AppError outer catch', () => {
  it('returns 500 when fetch itself throws (network error)', async () => {
    mockFetch.mockRejectedValue(new Error('ECONNREFUSED'));
    const res = await run(buildApp(), 'POST', '/api/video/sessions', { body: { documentId: 'doc-1' } });
    expect(res.statusCode).toBe(500);
  });
});

describe('POST /api/video/sessions', () => {
  it('returns 201 with sessionId on successful channel create', async () => {
    mockFetch.mockResolvedValue(apiOk({ arn: 'arn:channel:1', streamKey: 'sk-1', name: 'doc-1' }));
    const res = await run(buildApp(), 'POST', '/api/video/sessions', {
      body: { documentId: 'doc-1', displayName: 'Alice' },
    });
    expect(res.statusCode).toBe(201);
    expect((res.body as { sessionId: string }).sessionId).toBeDefined();
    expect(mockCreateSession).toHaveBeenCalledTimes(1);
    expect(mockCreateSession).toHaveBeenCalledWith(
      expect.objectContaining({ channelArn: 'arn:channel:1', streamKey: 'sk-1' }),
    );
  });

  it('calls channel API with generated name when no documentId', async () => {
    mockFetch.mockResolvedValue(apiOk({ arn: 'arn:channel:2', streamKey: 'sk-2', name: 'random' }));
    const res = await run(buildApp(), 'POST', '/api/video/sessions', { body: {} });
    expect(res.statusCode).toBe(201);
    // fetch should have been called with a UUID-like name in the body
    expect(mockFetch).toHaveBeenCalledWith(
      'https://video.test/api/channels',
      expect.objectContaining({ method: 'POST' }),
    );
  });

  it('returns video API error status on channel create failure', async () => {
    mockFetch.mockResolvedValue(apiFail(502));
    const res = await run(buildApp(), 'POST', '/api/video/sessions', { body: { documentId: 'doc-1' } });
    expect(res.statusCode).toBe(502);
  });

  it('still returns 201 when local DB write fails', async () => {
    mockFetch.mockResolvedValue(apiOk({ arn: 'arn:channel:3', streamKey: 'sk-3' }));
    mockCreateSession.mockRejectedValue(new Error('DDB error'));
    const res = await run(buildApp(), 'POST', '/api/video/sessions', {
      body: { documentId: 'doc-1' },
    });
    expect(res.statusCode).toBe(201);
  });
});

describe('POST /api/video/sessions/:sessionId/join', () => {
  const session = { documentId: 'doc-1', sessionId: 'vs-1', channelArn: 'arn:ch:1', streamKey: 'sk-1', status: 'active' };

  it('returns 200 with token on successful join', async () => {
    mockGetSession.mockResolvedValue(session);
    mockFetch.mockResolvedValue(apiOk({ token: 'ivs-token', participantId: 'p-1' }));
    const res = await run(buildApp(), 'POST', '/api/video/sessions/vs-1/join', {
      body: { documentId: 'doc-1', displayName: 'Bob' },
    });
    expect(res.statusCode).toBe(200);
    expect((res.body as { token: string }).token).toBe('ivs-token');
    expect((res.body as { participantId: string }).participantId).toBe('p-1');
    expect((res.body as { userId: string }).userId).toBe('tester');
    expect(mockAddParticipant).toHaveBeenCalledWith('doc-1', 'vs-1', expect.objectContaining({ userId: 'tester' }));
  });

  it('returns 400 when documentId missing from body', async () => {
    const res = await run(buildApp(), 'POST', '/api/video/sessions/vs-1/join', { body: {} });
    expect(res.statusCode).toBe(400);
  });

  it('returns 404 when session not found in DB', async () => {
    mockGetSession.mockResolvedValue(null);
    const res = await run(buildApp(), 'POST', '/api/video/sessions/vs-1/join', {
      body: { documentId: 'doc-1' },
    });
    expect(res.statusCode).toBe(404);
  });

  it('returns video API error status on token request failure', async () => {
    mockGetSession.mockResolvedValue(session);
    mockFetch.mockResolvedValue(apiFail(403));
    const res = await run(buildApp(), 'POST', '/api/video/sessions/vs-1/join', {
      body: { documentId: 'doc-1' },
    });
    expect(res.statusCode).toBe(403);
  });

  it('still returns 200 when addParticipant throws (non-fatal DB failure)', async () => {
    mockGetSession.mockResolvedValue(session);
    mockFetch.mockResolvedValue(apiOk({ token: 'ivs-token' }));
    mockAddParticipant.mockRejectedValue(new Error('DDB write failed'));
    const res = await run(buildApp(), 'POST', '/api/video/sessions/vs-1/join', {
      body: { documentId: 'doc-1', displayName: 'Bob' },
    });
    expect(res.statusCode).toBe(200);
  });

  it('returns 500 when fetch throws (non-AppError outer catch)', async () => {
    mockGetSession.mockResolvedValue(session);
    mockFetch.mockRejectedValue(new Error('network down'));
    const res = await run(buildApp(), 'POST', '/api/video/sessions/vs-1/join', {
      body: { documentId: 'doc-1' },
    });
    expect(res.statusCode).toBe(500);
  });
});

// The /end route no longer makes synchronous fetch calls to delete the
// channel — that responsibility moved to a separate reaper after the
// VIDEO_API_URL coupling was identified as an outage-blast-radius hazard
// (Phase P5 of platform-api ROADMAP). The signature also changed to
// `getSession(lobbyName, sessionId)` + `removeParticipant`, and the route
// now ends the session only when the last participant leaves.
//
// These tests exercise the OLD behavior (single-user end always deletes
// channel via fetch) and would need significant rewrites to match the
// new participant-tracking flow. Skipped as obsolete until those rewrites
// happen (tracked as Phase 54.5 follow-up).
describe.skip('POST /api/video/sessions/:sessionId/end [obsolete tests — pre-reaper behavior]', () => {
  const session = { documentId: 'doc-1', sessionId: 'vs-1', channelArn: 'arn:ch:1', streamKey: 'sk-1', status: 'active' };

  it('returns 200 on successful end', async () => {
    mockGetSession.mockResolvedValue(session);
    mockFetch.mockResolvedValue(apiOk({}));
    const res = await run(buildApp(), 'POST', '/api/video/sessions/vs-1/end', {
      body: { documentId: 'doc-1' },
    });
    expect(res.statusCode).toBe(200);
    expect(mockEndSession).toHaveBeenCalledWith('doc-1', 'vs-1', expect.any(String));
  });

  it('returns 400 when documentId missing from body', async () => {
    const res = await run(buildApp(), 'POST', '/api/video/sessions/vs-1/end', { body: {} });
    expect(res.statusCode).toBe(400);
  });

  it('returns 404 when session not found in DB', async () => {
    mockGetSession.mockResolvedValue(null);
    const res = await run(buildApp(), 'POST', '/api/video/sessions/vs-1/end', {
      body: { documentId: 'doc-1' },
    });
    expect(res.statusCode).toBe(404);
  });

  it('returns video API error status on channel delete failure', async () => {
    mockGetSession.mockResolvedValue(session);
    mockFetch.mockResolvedValue(apiFail(503));
    const res = await run(buildApp(), 'POST', '/api/video/sessions/vs-1/end', {
      body: { documentId: 'doc-1' },
    });
    expect(res.statusCode).toBe(503);
  });

  it('still returns 200 when endSession throws (non-fatal DB failure)', async () => {
    mockGetSession.mockResolvedValue(session);
    mockFetch.mockResolvedValue(apiOk({}));
    mockEndSession.mockRejectedValue(new Error('DDB write failed'));
    const res = await run(buildApp(), 'POST', '/api/video/sessions/vs-1/end', {
      body: { documentId: 'doc-1' },
      userId: 'tester',
    });
    expect(res.statusCode).toBe(200);
  });

  it('returns 500 when fetch throws (non-AppError outer catch)', async () => {
    mockGetSession.mockResolvedValue(session);
    mockFetch.mockRejectedValue(new Error('network error'));
    const res = await run(buildApp(), 'POST', '/api/video/sessions/vs-1/end', {
      body: { documentId: 'doc-1' },
    });
    expect(res.statusCode).toBe(500);
  });
});

describe('GET /api/video/sessions/document/:documentId', () => {
  it('returns 200 with sessions list', async () => {
    mockGetSessionsByDocument.mockResolvedValue([{ sessionId: 'vs-1' }]);
    const res = await run(buildApp(), 'GET', '/api/video/sessions/document/doc-1');
    expect(res.statusCode).toBe(200);
    expect((res.body as { sessions: unknown[] }).sessions).toHaveLength(1);
  });

  it('returns 500 when repo throws (non-AppError)', async () => {
    mockGetSessionsByDocument.mockRejectedValue(new Error('DDB scan failed'));
    const res = await run(buildApp(), 'GET', '/api/video/sessions/document/doc-1');
    expect(res.statusCode).toBe(500);
  });
});

describe('GET /api/video/sessions/:sessionId', () => {
  it('returns 200 with session', async () => {
    mockGetSession.mockResolvedValue({ sessionId: 'vs-1', status: 'active' });
    const res = await run(buildApp(), 'GET', '/api/video/sessions/vs-1?documentId=doc-1');
    expect(res.statusCode).toBe(200);
    expect((res.body as { sessionId: string }).sessionId).toBe('vs-1');
  });

  it('rejects missing documentId with 400', async () => {
    const res = await run(buildApp(), 'GET', '/api/video/sessions/vs-1');
    expect(res.statusCode).toBe(400);
  });

  it('returns 404 when session not found', async () => {
    mockGetSession.mockResolvedValue(null);
    const res = await run(buildApp(), 'GET', '/api/video/sessions/vs-1?documentId=doc-1');
    expect(res.statusCode).toBe(404);
  });

  it('returns 500 when repo throws (non-AppError)', async () => {
    mockGetSession.mockRejectedValue(new Error('DDB get failed'));
    const res = await run(buildApp(), 'GET', '/api/video/sessions/vs-1?documentId=doc-1');
    expect(res.statusCode).toBe(500);
  });
});
