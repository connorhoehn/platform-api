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
// findActiveByLobbyName default returns null (no existing active session)
// so the create/findOrCreate paths in videoSessions.ts:169 + :182 fall
// through to the create branch. Individual tests can mockResolvedValue
// a record to exercise the reuse branch.
const mockFindActiveByLobbyName = jest.fn().mockResolvedValue(null);
const mockRemoveParticipant = jest.fn();
const mockRemoveParticipantByUserId = jest.fn();

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
    findActiveByLobbyName: (...args: unknown[]) => mockFindActiveByLobbyName(...args),
    removeParticipant: (...args: unknown[]) => mockRemoveParticipant(...args),
    removeParticipantByUserId: (...args: unknown[]) => mockRemoveParticipantByUserId(...args),
  },
}));

const mockFetch = jest.fn();
(globalThis as unknown as { fetch: typeof mockFetch }).fetch = mockFetch;

import {
  videoSessionsRouter,
  __resetSfuBreakerForTests,
  __configureSfuBreakerForTests,
  reapLvsParticipant,
} from '../videoSessions';
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

// Shape of the error Node's fetch throws when AbortSignal.timeout fires.
function makeTimeoutError(): Error {
  const e = new Error('The operation was aborted due to timeout');
  e.name = 'TimeoutError';
  return e;
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
  // Reset breaker to CLOSED + restore default thresholds between cases so
  // failure counts from one test don't leak into the next.
  __resetSfuBreakerForTests();
  __configureSfuBreakerForTests({
    failureThreshold: 5,
    successThreshold: 2,
    resetTimeout: 30_000,
    halfOpenMaxCalls: 1,
  });
});

afterEach(() => jest.restoreAllMocks());

describe('POST /api/video/sessions — non-AppError outer catch', () => {
  it('returns 502 when fetch itself throws (network error → bad-gateway)', async () => {
    mockFetch.mockRejectedValue(new Error('ECONNREFUSED'));
    const res = await run(buildApp(), 'POST', '/api/video/sessions', { body: { documentId: 'doc-1' } });
    expect(res.statusCode).toBe(502);
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

  it('forwards 4xx from upstream without tripping the breaker', async () => {
    mockFetch.mockResolvedValue(apiFail(404));
    const res = await run(buildApp(), 'POST', '/api/video/sessions', { body: { documentId: 'doc-1' } });
    expect(res.statusCode).toBe(404);
  });

  it('forwards upstream 5xx status (counted as breaker failure)', async () => {
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

  it('returns 200 with token on successful join (breaker stays closed)', async () => {
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

  it('forwards 4xx from upstream on token request failure', async () => {
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

  it('returns 502 when fetch throws (network error → bad-gateway)', async () => {
    mockGetSession.mockResolvedValue(session);
    mockFetch.mockRejectedValue(new Error('network down'));
    const res = await run(buildApp(), 'POST', '/api/video/sessions/vs-1/join', {
      body: { documentId: 'doc-1' },
    });
    expect(res.statusCode).toBe(502);
  });
});

// --- Circuit breaker + timeout coverage (P1 #4) ---------------------------

describe('SFU resilience — fetch timeouts', () => {
  it('returns 504 when the upstream fetch times out (TimeoutError)', async () => {
    mockFetch.mockRejectedValue(makeTimeoutError());
    const res = await run(buildApp(), 'POST', '/api/video/sessions', { body: { documentId: 'doc-1' } });
    expect(res.statusCode).toBe(504);
  });

  it('returns 504 on AbortError shape too', async () => {
    const abort = new Error('aborted');
    abort.name = 'AbortError';
    mockFetch.mockRejectedValue(abort);
    const res = await run(buildApp(), 'POST', '/api/video/sessions', { body: { documentId: 'doc-1' } });
    expect(res.statusCode).toBe(504);
  });

  it('passes an AbortSignal on every SFU fetch', async () => {
    mockFetch.mockResolvedValue(apiOk({ arn: 'a', streamKey: 's' }));
    await run(buildApp(), 'POST', '/api/video/sessions', { body: { documentId: 'doc-1' } });
    const init = mockFetch.mock.calls[0][1] as { signal?: AbortSignal };
    expect(init.signal).toBeDefined();
    expect(typeof init.signal!.aborted).toBe('boolean');
  });
});

describe('SFU resilience — circuit breaker', () => {
  it('counts 5xx responses as failures and opens the breaker after threshold', async () => {
    // Threshold = 5. After 5 consecutive 5xx responses the breaker should
    // open and the 6th call should fast-fail with 503 — note that fetch is
    // NOT invoked for the 6th call (no timer waste).
    mockFetch.mockResolvedValue(apiFail(502));
    for (let i = 0; i < 5; i++) {
      const res = await run(buildApp(), 'POST', '/api/video/sessions', { body: { documentId: `doc-${i}` } });
      expect(res.statusCode).toBe(502);
    }
    expect(mockFetch).toHaveBeenCalledTimes(5);

    const tripped = await run(buildApp(), 'POST', '/api/video/sessions', { body: { documentId: 'doc-after-open' } });
    expect(tripped.statusCode).toBe(503);
    // Crucial: with breaker OPEN, we MUST NOT have called fetch again.
    expect(mockFetch).toHaveBeenCalledTimes(5);
  });

  it('counts thrown fetch errors (e.g. ECONNREFUSED) as failures and trips after threshold', async () => {
    mockFetch.mockRejectedValue(new Error('ECONNREFUSED'));
    for (let i = 0; i < 5; i++) {
      const res = await run(buildApp(), 'POST', '/api/video/sessions', { body: { documentId: `doc-${i}` } });
      expect(res.statusCode).toBe(502);
    }
    const tripped = await run(buildApp(), 'POST', '/api/video/sessions', { body: { documentId: 'doc-after-open' } });
    expect(tripped.statusCode).toBe(503);
    expect(mockFetch).toHaveBeenCalledTimes(5);
  });

  it('counts upstream timeouts as failures and opens after threshold', async () => {
    mockFetch.mockRejectedValue(makeTimeoutError());
    for (let i = 0; i < 5; i++) {
      const res = await run(buildApp(), 'POST', '/api/video/sessions', { body: { documentId: `doc-${i}` } });
      expect(res.statusCode).toBe(504);
    }
    const tripped = await run(buildApp(), 'POST', '/api/video/sessions', { body: { documentId: 'doc-after-open' } });
    expect(tripped.statusCode).toBe(503);
  });

  it('half-open probe: success closes the breaker after cooldown', async () => {
    jest.useFakeTimers();
    __resetSfuBreakerForTests();
    __configureSfuBreakerForTests({
      failureThreshold: 2,
      successThreshold: 1,
      resetTimeout: 100,
      halfOpenMaxCalls: 1,
    });

    // Trip the breaker with 2 failures.
    mockFetch.mockResolvedValue(apiFail(503));
    let r = await run(buildApp(), 'POST', '/api/video/sessions', { body: { documentId: 'a' } });
    expect(r.statusCode).toBe(503);
    r = await run(buildApp(), 'POST', '/api/video/sessions', { body: { documentId: 'b' } });
    expect(r.statusCode).toBe(503);

    // Breaker open → next call is fast-fail 503 without hitting fetch.
    const callsBeforeProbe = mockFetch.mock.calls.length;
    r = await run(buildApp(), 'POST', '/api/video/sessions', { body: { documentId: 'c' } });
    expect(r.statusCode).toBe(503);
    expect(mockFetch.mock.calls.length).toBe(callsBeforeProbe);

    // Advance past cooldown — breaker transitions to HALF_OPEN.
    jest.advanceTimersByTime(150);

    // Probe call succeeds → breaker closes. The probe consumes a fetch.
    mockFetch.mockResolvedValue(apiOk({ arn: 'arn:probe', streamKey: 'sk-probe' }));
    r = await run(buildApp(), 'POST', '/api/video/sessions', { body: { documentId: 'd' } });
    expect(r.statusCode).toBe(201);
    expect(mockFetch.mock.calls.length).toBe(callsBeforeProbe + 1);

    // Subsequent calls succeed normally (breaker closed).
    r = await run(buildApp(), 'POST', '/api/video/sessions', { body: { documentId: 'e' } });
    expect(r.statusCode).toBe(201);

    jest.useRealTimers();
  });

  it('half-open probe: failure re-opens the breaker', async () => {
    jest.useFakeTimers();
    __resetSfuBreakerForTests();
    __configureSfuBreakerForTests({
      failureThreshold: 2,
      successThreshold: 1,
      resetTimeout: 100,
      halfOpenMaxCalls: 1,
    });

    // Trip the breaker with 2 failures.
    mockFetch.mockResolvedValue(apiFail(503));
    await run(buildApp(), 'POST', '/api/video/sessions', { body: { documentId: 'a' } });
    await run(buildApp(), 'POST', '/api/video/sessions', { body: { documentId: 'b' } });

    // Advance past cooldown → HALF_OPEN.
    jest.advanceTimersByTime(150);

    // Probe still fails → breaker re-opens.
    mockFetch.mockResolvedValue(apiFail(503));
    let r = await run(buildApp(), 'POST', '/api/video/sessions', { body: { documentId: 'probe' } });
    expect(r.statusCode).toBe(503);

    // Next call (no further cooldown) is fast-fail without fetch.
    const callsAfterProbe = mockFetch.mock.calls.length;
    r = await run(buildApp(), 'POST', '/api/video/sessions', { body: { documentId: 'after' } });
    expect(r.statusCode).toBe(503);
    expect(mockFetch.mock.calls.length).toBe(callsAfterProbe);

    jest.useRealTimers();
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

// --- LVS participant reap on /end (hub#1281) ----------------------------

describe('reapLvsParticipant — LVS reclamation on /end', () => {
  const args = {
    channelArn: 'arn:lvs:channel/abc',
    streamKey: 'sk-test',
    participantId: 'p-1',
    sessionId: 'sess-1',
  };

  it('returns "reaped" on 204', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 204,
      json: async () => ({}),
      text: async () => '',
    } as unknown as globalThis.Response);

    expect(await reapLvsParticipant(args)).toBe('reaped');
    expect(mockFetch).toHaveBeenCalledTimes(1);
    const [url, init] = mockFetch.mock.calls[0];
    expect(url).toContain('/api/channels/');
    expect(url).toContain('/participants/');
    expect(url).toContain(encodeURIComponent(args.participantId));
    expect((init as RequestInit).method).toBe('DELETE');
    expect((init as RequestInit).headers).toMatchObject({
      Authorization: `Bearer ${args.streamKey}`,
    });
  });

  it('returns "not_found" on 404 (idempotent — already reaped)', async () => {
    mockFetch.mockResolvedValueOnce(apiFail(404));
    expect(await reapLvsParticipant(args)).toBe('not_found');
    expect(mockFetch).toHaveBeenCalledTimes(1);
  });

  it('returns "error" on non-404 4xx without retry', async () => {
    mockFetch.mockResolvedValueOnce(apiFail(400));
    expect(await reapLvsParticipant(args)).toBe('error');
    // 4xx (non-404) is non-retriable.
    expect(mockFetch).toHaveBeenCalledTimes(1);
  });

  it('retries once on 5xx then returns "error"', async () => {
    mockFetch.mockResolvedValueOnce(apiFail(503));
    mockFetch.mockResolvedValueOnce(apiFail(503));
    expect(await reapLvsParticipant(args)).toBe('error');
    expect(mockFetch).toHaveBeenCalledTimes(2);
  });

  it('retries once on a thrown error then returns "error"', async () => {
    mockFetch.mockRejectedValueOnce(new Error('ECONNRESET'));
    mockFetch.mockRejectedValueOnce(new Error('ECONNRESET'));
    expect(await reapLvsParticipant(args)).toBe('error');
    expect(mockFetch).toHaveBeenCalledTimes(2);
  });

  it('5xx then 204 → "reaped" (transient blip)', async () => {
    mockFetch.mockResolvedValueOnce(apiFail(502));
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 204,
      json: async () => ({}),
      text: async () => '',
    } as unknown as globalThis.Response);
    expect(await reapLvsParticipant(args)).toBe('reaped');
    expect(mockFetch).toHaveBeenCalledTimes(2);
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
