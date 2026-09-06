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

interface RunOpts { body?: unknown; userId?: string; headers?: Record<string, string> }

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
    headers: { 'content-type': 'application/json', ...(opts.headers || {}) },
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

// LVS POST /api/sessions response shape — used by the new thin-proxy
// path. createdAt + hostToken are present on the LVS reply but the
// platform-api response only surfaces { sessionId, reused }.
function lvsSessionCreated(channelArn = 'arn:channel:1') {
  return apiOk({
    sessionId: channelArn,  // LVS sessionId === channelArn
    channelArn,
    sessionFound: false,
    createdAt: '2026-06-02T13:00:00Z',
    hostToken: 'host.jwt.here',
  });
}

function lvsSessionReused(channelArn = 'arn:channel:1') {
  return apiOk({
    sessionId: channelArn,
    channelArn,
    sessionFound: true,
    createdAt: '2026-06-02T12:00:00Z',
    hostToken: 'host.jwt.here',
  });
}

describe('POST /api/video/sessions — non-AppError outer catch', () => {
  it('returns 502 when fetch itself throws (network error → bad-gateway)', async () => {
    mockFetch.mockRejectedValue(new Error('ECONNREFUSED'));
    const res = await run(buildApp(), 'POST', '/api/video/sessions', {
      body: { documentId: 'doc-1' }, headers: { authorization: 'Bearer u' },
      headers: { authorization: 'Bearer user-jwt' },
    });
    expect(res.statusCode).toBe(502);
  });
});

describe('POST /api/video/sessions (thin proxy → LVS)', () => {
  it('returns 201 with sessionId on successful LVS create', async () => {
    mockFetch.mockResolvedValue(lvsSessionCreated('arn:channel:1'));
    const res = await run(buildApp(), 'POST', '/api/video/sessions', {
      body: { documentId: 'doc-1', displayName: 'Alice' },
      headers: { authorization: 'Bearer user-jwt' },
    });
    expect(res.statusCode).toBe(201);
    expect((res.body as { sessionId: string }).sessionId).toBeDefined();
    expect((res.body as { reused: boolean }).reused).toBe(false);
    expect(mockCreateSession).toHaveBeenCalledTimes(1);
    expect(mockCreateSession).toHaveBeenCalledWith(
      expect.objectContaining({ channelArn: 'arn:channel:1', streamKey: '' }),
    );
  });

  it('forwards to LVS /api/sessions with lobbyName, findOrCreate, creatorId, recordingConfiguration', async () => {
    mockFetch.mockResolvedValue(lvsSessionCreated('arn:channel:2'));
    await run(buildApp(), 'POST', '/api/video/sessions', {
      body: { documentId: 'doc-x', findOrCreate: true },
      headers: { authorization: 'Bearer user-jwt' },
      userId: 'alice',
    });
    expect(mockFetch).toHaveBeenCalledWith(
      'https://video.test/api/sessions',
      expect.objectContaining({ method: 'POST' }),
    );
    const lastCall = mockFetch.mock.calls[mockFetch.mock.calls.length - 1];
    const init = lastCall[1] as RequestInit;
    const sentBody = JSON.parse(init.body as string);
    expect(sentBody).toMatchObject({
      lobbyName: 'doc-x',
      findOrCreate: true,
      creatorId: 'alice',
      recordingConfiguration: { enabled: true, destination: 'local' },
    });
    expect((init.headers as Record<string, string>).Authorization).toBe('Bearer user-jwt');
  });

  it('findOrCreate hit: reuses DDB row when channelArn matches', async () => {
    mockFetch.mockResolvedValue(lvsSessionReused('arn:channel:reused'));
    mockFindActiveByLobbyName.mockResolvedValue({
      sessionId: 'existing-vs-id',
      channelArn: 'arn:channel:reused',
      status: 'active',
    });
    const res = await run(buildApp(), 'POST', '/api/video/sessions', {
      body: { documentId: 'doc-1', findOrCreate: true },
      headers: { authorization: 'Bearer user-jwt' },
    });
    expect(res.statusCode).toBe(200);
    expect((res.body as { sessionId: string; reused: boolean })).toEqual({
      sessionId: 'existing-vs-id',
      reused: true,
    });
    // No new DDB row created — we reused the existing one.
    expect(mockCreateSession).not.toHaveBeenCalled();
  });

  // Wave 3 / ADR-002 — recordingProfile field flow
  it('omits recordingProfile when body omits recordingConfiguration (legacy back-compat)', async () => {
    mockFetch.mockResolvedValue(lvsSessionCreated('arn:channel:p1'));
    await run(buildApp(), 'POST', '/api/video/sessions', {
      body: { documentId: 'doc-prof-default', findOrCreate: true },
      headers: { authorization: 'Bearer user-jwt' },
      userId: 'alice',
    });
    const sentBody = JSON.parse((mockFetch.mock.calls[mockFetch.mock.calls.length - 1][1] as RequestInit).body as string);
    // No recordingProfile in default path — LVS falls through to
    // legacy-flag synthesis. New behavior is opt-in via FE.
    expect(sentBody.recordingConfiguration).toEqual({ enabled: true, destination: 'local', transcribe: false });
  });

  // LVS starts recording on producer-create — the instant the FIRST person
  // publishes — so a 1:1 call recorded the callee's ringing. LVS cannot tell a
  // call from a broadcast; we can, from the lobby.
  // A viewer needs the channelArn to watch (HLS is addressed by channel), and
  // the only way to learn it was POST /sessions with findOrCreate — which
  // CREATES a session and enrolls you as a participant. Watching a room
  // therefore required joining it, which defeats the reason to watch: an HLS
  // audience is supposed to cost nothing per head.
  describe('GET /sessions/by-lobby/:lobbyName', () => {
    it('resolves a live lobby to its channel without joining', async () => {
      mockFindActiveByLobbyName.mockResolvedValueOnce({
        sessionId: 'sess-watch', channelArn: 'arn:channel:watch',
        startedAt: '2026-09-06T00:00:00.000Z', startedBy: 'hank',
        participants: [{ participantId: 'p1', userId: 'hank' }],
      });
      const res = await run(buildApp(), 'GET', '/api/video/sessions/by-lobby/room%3Aasdf', {
        headers: { authorization: 'Bearer user-jwt' },
        userId: 'alice',
      });
      // Either the repo has a live session (200 with a channelArn) or it does
      // not (404). What must NOT happen is a session being created.
      expect(res.statusCode).toBe(200);
      expect(res.body.channelArn).toBe('arn:channel:watch');
      expect(res.body.lobbyName).toBe('room:asdf');
      // A viewer gets what they need to watch and nothing more — no
      // participant list, no token. Watching is a weaker right than joining.
      expect(res.body).not.toHaveProperty('participants');
      expect(res.body).not.toHaveProperty('streamKey');
      // The decisive part: no LVS session-create was proxied.
      const created = mockFetch.mock.calls.filter(
        ([url, init]) => String(url).includes('/api/sessions') && (init as RequestInit)?.method === 'POST',
      );
      expect(created).toHaveLength(0);
    });

    it('404s when nothing is live — the honest answer before anyone starts', async () => {
      mockFindActiveByLobbyName.mockResolvedValueOnce(null);
      const res = await run(buildApp(), 'GET', '/api/video/sessions/by-lobby/room%3Anever-used', {
        headers: { authorization: 'Bearer user-jwt' },
        userId: 'alice',
      });
      expect(res.statusCode).toBe(404);
    });

    // `status: 'active'` is a lifecycle flag, not evidence of a broadcast. It
    // only flips to 'ended' when the last participant LEAVES through the leave
    // route, so a session whose people closed their tabs, crashed, or were a
    // headless test that never said goodbye stays 'active' forever. Reporting
    // one of those as watchable is how a room that once had a call ended up
    // permanently offering a Watch button onto a black rectangle marked LIVE.
    it('404s on a leaked session that nobody is publishing to', async () => {
      mockFindActiveByLobbyName.mockResolvedValueOnce({
        sessionId: 'sess-leaked', channelArn: 'arn:channel:leaked',
        startedAt: '2026-09-01T00:00:00.000Z', startedBy: 'hank',
        participants: [],
      });
      const res = await run(buildApp(), 'GET', '/api/video/sessions/by-lobby/room%3Afresh-probe-room', {
        headers: { authorization: 'Bearer user-jwt' },
        userId: 'alice',
      });
      expect(res.statusCode).toBe(404);
    });

    it('404s when the record predates participant tracking entirely', async () => {
      mockFindActiveByLobbyName.mockResolvedValueOnce({
        sessionId: 'sess-old', channelArn: 'arn:channel:old',
        startedAt: null, startedBy: null,
      });
      const res = await run(buildApp(), 'GET', '/api/video/sessions/by-lobby/room%3Aold', {
        headers: { authorization: 'Bearer user-jwt' },
        userId: 'alice',
      });
      expect(res.statusCode).toBe(404);
    });

    // The roster cannot answer "is this watchable": a room call joins TWICE
    // (room bridge + call widget) and hangs up ONCE, so every call leaves a
    // participant behind and the count never reaches zero. Measured on a real
    // room: three participants from three calls that were all hung up. So the
    // SFU is asked instead — it holds the producer objects.
    const streamState = (state: 'LIVE' | 'OFFLINE') => ({
      ok: true,
      status: 200,
      json: async () => ({ stream: { state } }),
      text: async () => JSON.stringify({ stream: { state } }),
    });

    it('404s when the SFU says nothing is being broadcast, roster be damned', async () => {
      mockFindActiveByLobbyName.mockResolvedValueOnce({
        sessionId: 'sess-leaked', channelArn: 'arn:channel:leaked',
        startedAt: '2026-09-01T00:00:00.000Z', startedBy: 'hank',
        // Three leaked participants — exactly the state the bug produces.
        participants: [
          { participantId: 'p1', userId: 'hank' },
          { participantId: 'p2', userId: 'hank' },
          { participantId: 'p3', userId: 'hank' },
        ],
      });
      mockFetch.mockResolvedValue(streamState('OFFLINE'));
      const res = await run(buildApp(), 'GET', '/api/video/sessions/by-lobby/room%3Aleaked', {
        headers: { authorization: 'Bearer user-jwt' }, userId: 'alice',
      });
      expect(res.statusCode).toBe(404);
    });

    it('200s when the SFU says LIVE, even with an empty roster', async () => {
      mockFindActiveByLobbyName.mockResolvedValueOnce({
        sessionId: 'sess-live', channelArn: 'arn:channel:live',
        startedAt: null, startedBy: null, participants: [],
      });
      mockFetch.mockResolvedValue(streamState('LIVE'));
      const res = await run(buildApp(), 'GET', '/api/video/sessions/by-lobby/room%3Alive', {
        headers: { authorization: 'Bearer user-jwt' }, userId: 'alice',
      });
      expect(res.statusCode).toBe(200);
      expect(res.body.channelArn).toBe('arn:channel:live');
    });

    // "Could not ask" is not "nothing is on". Telling a viewer a running
    // broadcast does not exist is the worse failure, so an unreachable SFU
    // falls back to the (too generous) roster rather than to silence.
    it('falls back to the roster when the SFU cannot be reached', async () => {
      mockFindActiveByLobbyName.mockResolvedValueOnce({
        sessionId: 'sess-unknown', channelArn: 'arn:channel:unknown',
        startedAt: null, startedBy: null,
        participants: [{ participantId: 'p1', userId: 'hank' }],
      });
      mockFetch.mockRejectedValue(new Error('connect ECONNREFUSED'));
      const res = await run(buildApp(), 'GET', '/api/video/sessions/by-lobby/room%3Aunknown', {
        headers: { authorization: 'Bearer user-jwt' }, userId: 'alice',
      });
      expect(res.statusCode).toBe(200);
    });

    it('404s when the SFU is unreachable AND the roster is empty', async () => {
      mockFindActiveByLobbyName.mockResolvedValueOnce({
        sessionId: 'sess-none', channelArn: 'arn:channel:none',
        startedAt: null, startedBy: null, participants: [],
      });
      mockFetch.mockRejectedValue(new Error('connect ECONNREFUSED'));
      const res = await run(buildApp(), 'GET', '/api/video/sessions/by-lobby/room%3Anone', {
        headers: { authorization: 'Bearer user-jwt' }, userId: 'alice',
      });
      expect(res.statusCode).toBe(404);
    });

    it('decodes the lobby name out of the path', async () => {
      mockFindActiveByLobbyName.mockResolvedValueOnce({
        sessionId: 's', channelArn: 'a', startedAt: null, startedBy: null,
        participants: [{ participantId: 'p1', userId: 'hank' }],
      });
      const res = await run(buildApp(), 'GET', '/api/video/sessions/by-lobby/dm%3Aalice%3Abob', {
        headers: { authorization: 'Bearer user-jwt' },
        userId: 'alice',
      });
      expect(res.body.lobbyName).toBe('dm:alice:bob');
    });
  });

  it('asks LVS to wait for the second party on a DM call', async () => {
    mockFetch.mockResolvedValue(lvsSessionCreated('arn:channel:dm-wait'));
    await run(buildApp(), 'POST', '/api/video/sessions', {
      body: { lobbyName: 'dm:alice:bob', findOrCreate: true },
      headers: { authorization: 'Bearer user-jwt' },
      userId: 'alice',
    });
    const sentBody = JSON.parse((mockFetch.mock.calls[mockFetch.mock.calls.length - 1][1] as RequestInit).body as string);
    expect(sentBody.recordingConfiguration.startWhenParticipants).toBe(2);
  });

  // A room or a broadcast can legitimately hold one person worth recording,
  // so they keep LVS' default of 1.
  // A big group chat is addressed by a hash rather than by its members, but it
  // rings exactly the same way. Matching only `dm:` left the large groups
  // recording their own ringing.
  it('waits for the second party on a hashed group call too', async () => {
    mockFetch.mockResolvedValue(lvsSessionCreated('arn:channel:dmg-wait'));
    await run(buildApp(), 'POST', '/api/video/sessions', {
      body: { lobbyName: 'dmg:9f2c1a4b55de7788aa11bb22', findOrCreate: true },
      headers: { authorization: 'Bearer user-jwt' },
      userId: 'alice',
    });
    const sentBody = JSON.parse((mockFetch.mock.calls[mockFetch.mock.calls.length - 1][1] as RequestInit).body as string);
    expect(sentBody.recordingConfiguration.startWhenParticipants).toBe(2);
  });

  it('does not impose a participant threshold on rooms', async () => {
    mockFetch.mockResolvedValue(lvsSessionCreated('arn:channel:room-nowait'));
    await run(buildApp(), 'POST', '/api/video/sessions', {
      body: { lobbyName: 'room:design', findOrCreate: true },
      headers: { authorization: 'Bearer user-jwt' },
      userId: 'alice',
    });
    const sentBody = JSON.parse((mockFetch.mock.calls[mockFetch.mock.calls.length - 1][1] as RequestInit).body as string);
    expect(sentBody.recordingConfiguration.startWhenParticipants).toBeUndefined();
  });

  it('keeps the threshold alongside an explicit recordingConfiguration', async () => {
    mockFetch.mockResolvedValue(lvsSessionCreated('arn:channel:dm-explicit'));
    await run(buildApp(), 'POST', '/api/video/sessions', {
      body: {
        lobbyName: 'dm:alice:bob',
        findOrCreate: true,
        recordingConfiguration: { enabled: true, recordingProfile: 'dm' },
      },
      headers: { authorization: 'Bearer user-jwt' },
      userId: 'alice',
    });
    const sentBody = JSON.parse((mockFetch.mock.calls[mockFetch.mock.calls.length - 1][1] as RequestInit).body as string);
    expect(sentBody.recordingConfiguration.startWhenParticipants).toBe(2);
    expect(sentBody.recordingConfiguration.recordingProfile).toBe('dm');
  });

  it('accepts explicit recordingProfile in body and forwards it to LVS', async () => {
    mockFetch.mockResolvedValue(lvsSessionCreated('arn:channel:p2'));
    await run(buildApp(), 'POST', '/api/video/sessions', {
      body: {
        documentId: 'doc-prof-dm',
        findOrCreate: true,
        recordingConfiguration: { enabled: true, recordingProfile: 'dm' },
      },
      headers: { authorization: 'Bearer user-jwt' },
      userId: 'alice',
    });
    const sentBody = JSON.parse((mockFetch.mock.calls[mockFetch.mock.calls.length - 1][1] as RequestInit).body as string);
    expect(sentBody.recordingConfiguration.recordingProfile).toBe('dm');
  });

  it('rejects unknown recordingProfile strings with 400', async () => {
    mockFetch.mockResolvedValue(lvsSessionCreated('arn:channel:p3'));
    const res = await run(buildApp(), 'POST', '/api/video/sessions', {
      body: {
        documentId: 'doc-prof-bad',
        findOrCreate: true,
        recordingConfiguration: { enabled: true, recordingProfile: 'masterclass' },
      },
      headers: { authorization: 'Bearer user-jwt' },
      userId: 'alice',
    });
    expect(res.statusCode).toBe(400);
  });

  it('legacy transcribe=true (no recordingProfile) maps to recordingProfile=hangout', async () => {
    mockFetch.mockResolvedValue(lvsSessionCreated('arn:channel:p4'));
    await run(buildApp(), 'POST', '/api/video/sessions', {
      body: {
        documentId: 'doc-prof-legacy',
        findOrCreate: true,
        recordingConfiguration: { enabled: true, transcribe: true },
      },
      headers: { authorization: 'Bearer user-jwt' },
      userId: 'alice',
    });
    const sentBody = JSON.parse((mockFetch.mock.calls[mockFetch.mock.calls.length - 1][1] as RequestInit).body as string);
    expect(sentBody.recordingConfiguration.recordingProfile).toBe('hangout');
    expect(sentBody.recordingConfiguration.transcribe).toBe(true);
  });

  it('findOrCreate hit but no matching DDB row: creates a fresh DDB row pointing at the LVS-owned channel', async () => {
    mockFetch.mockResolvedValue(lvsSessionReused('arn:channel:9'));
    mockFindActiveByLobbyName.mockResolvedValue(null);
    const res = await run(buildApp(), 'POST', '/api/video/sessions', {
      body: { documentId: 'doc-1', findOrCreate: true },
      headers: { authorization: 'Bearer user-jwt' },
    });
    expect(res.statusCode).toBe(200);
    expect((res.body as { reused: boolean }).reused).toBe(true);
    expect(mockCreateSession).toHaveBeenCalled();
  });

  it('forwards 4xx from LVS without tripping the breaker', async () => {
    mockFetch.mockResolvedValue(apiFail(404));
    const res = await run(buildApp(), 'POST', '/api/video/sessions', {
      body: { documentId: 'doc-1' }, headers: { authorization: 'Bearer u' },
      headers: { authorization: 'Bearer user-jwt' },
    });
    expect(res.statusCode).toBe(404);
  });

  it('forwards LVS 5xx (counted as breaker failure)', async () => {
    mockFetch.mockResolvedValue(apiFail(502));
    const res = await run(buildApp(), 'POST', '/api/video/sessions', {
      body: { documentId: 'doc-1' }, headers: { authorization: 'Bearer u' },
      headers: { authorization: 'Bearer user-jwt' },
    });
    expect(res.statusCode).toBe(502);
  });

  it('LVS 409 LOBBY_TAKEN surfaces verbatim to the FE', async () => {
    mockFetch.mockResolvedValue({
      ok: false, status: 409,
      json: async () => ({ error: 'lobbyName already claimed', lobbyName: 'doc-1' }),
      text: async () => '',
    } as unknown as globalThis.Response);
    const res = await run(buildApp(), 'POST', '/api/video/sessions', {
      body: { documentId: 'doc-1', findOrCreate: true },
      headers: { authorization: 'Bearer user-jwt' },
    });
    expect(res.statusCode).toBe(409);
  });

  it('still returns 201 when local DB write fails', async () => {
    mockFetch.mockResolvedValue(lvsSessionCreated('arn:channel:3'));
    mockCreateSession.mockRejectedValue(new Error('DDB error'));
    const res = await run(buildApp(), 'POST', '/api/video/sessions', {
      body: { documentId: 'doc-1' }, headers: { authorization: 'Bearer u' },
      headers: { authorization: 'Bearer user-jwt' },
    });
    expect(res.statusCode).toBe(201);
  });

  it('401 when Authorization header missing', async () => {
    mockFetch.mockResolvedValue(lvsSessionCreated());
    const res = await run(buildApp(), 'POST', '/api/video/sessions', {
      body: { documentId: 'doc-1' },
    });
    expect(res.statusCode).toBe(401);
  });
});

describe('POST /api/video/sessions/:sessionId/join (thin proxy → LVS)', () => {
  const session = { documentId: 'doc-1', sessionId: 'vs-1', channelArn: 'arn:ch:1', streamKey: '', status: 'active' };

  function lvsJoinOk() {
    return apiOk({
      token: 'lvs.jwt.token',
      websocketUrl: 'wss://video.test/stage/arn:ch:1',
      expiresAt: '2026-06-02T14:00:00Z',
      participantId: 'p-1',
      role: 'publisher',
      displayName: 'Bob',
    });
  }

  it('returns 200 with token on successful LVS /join', async () => {
    mockGetSession.mockResolvedValue(session);
    mockFetch.mockResolvedValue(lvsJoinOk());
    const res = await run(buildApp(), 'POST', '/api/video/sessions/vs-1/join', {
      body: { documentId: 'doc-1', displayName: 'Bob' },
      headers: { authorization: 'Bearer user-jwt' },
    });
    expect(res.statusCode).toBe(200);
    expect((res.body as { token: string }).token).toBe('lvs.jwt.token');
    expect((res.body as { participantId: string }).participantId).toBe('p-1');
    expect((res.body as { userId: string }).userId).toBe('tester');
    expect(mockAddParticipant).toHaveBeenCalledWith('doc-1', 'vs-1', expect.objectContaining({ userId: 'tester' }));
  });

  it('forwards to LVS /api/sessions/:arn/join with userId, displayName, role', async () => {
    mockGetSession.mockResolvedValue(session);
    mockFetch.mockResolvedValue(lvsJoinOk());
    await run(buildApp(), 'POST', '/api/video/sessions/vs-1/join', {
      body: { documentId: 'doc-1', displayName: 'Bob', role: 'subscriber' },
      headers: { authorization: 'Bearer user-jwt' },
      userId: 'alice',
    });
    const lastCall = mockFetch.mock.calls[mockFetch.mock.calls.length - 1];
    const url = lastCall[0] as string;
    const init = lastCall[1] as RequestInit;
    expect(url).toContain('/api/sessions/');
    expect(url).toContain(encodeURIComponent('arn:ch:1'));
    expect(url).toContain('/join');
    expect(JSON.parse(init.body as string)).toMatchObject({
      userId: 'alice', displayName: 'Bob', role: 'subscriber',
    });
  });

  it('returns 400 when documentId missing from body', async () => {
    const res = await run(buildApp(), 'POST', '/api/video/sessions/vs-1/join', {
      body: {},
      headers: { authorization: 'Bearer user-jwt' },
    });
    expect(res.statusCode).toBe(400);
  });

  it('returns 404 when session not found in DB', async () => {
    mockGetSession.mockResolvedValue(null);
    const res = await run(buildApp(), 'POST', '/api/video/sessions/vs-1/join', {
      body: { documentId: 'doc-1' }, headers: { authorization: 'Bearer u' },
      headers: { authorization: 'Bearer user-jwt' },
    });
    expect(res.statusCode).toBe(404);
  });

  it('forwards 4xx from LVS on token request failure', async () => {
    mockGetSession.mockResolvedValue(session);
    mockFetch.mockResolvedValue(apiFail(403));
    const res = await run(buildApp(), 'POST', '/api/video/sessions/vs-1/join', {
      body: { documentId: 'doc-1' }, headers: { authorization: 'Bearer u' },
      headers: { authorization: 'Bearer user-jwt' },
    });
    expect(res.statusCode).toBe(403);
  });

  it('LVS 410 (session ended) propagates verbatim', async () => {
    mockGetSession.mockResolvedValue(session);
    mockFetch.mockResolvedValue({
      ok: false, status: 410,
      json: async () => ({ error: 'session ended' }),
      text: async () => '',
    } as unknown as globalThis.Response);
    const res = await run(buildApp(), 'POST', '/api/video/sessions/vs-1/join', {
      body: { documentId: 'doc-1' }, headers: { authorization: 'Bearer u' },
      headers: { authorization: 'Bearer user-jwt' },
    });
    expect(res.statusCode).toBe(410);
  });

  it('still returns 200 when addParticipant throws (non-fatal DB failure)', async () => {
    mockGetSession.mockResolvedValue(session);
    mockFetch.mockResolvedValue(lvsJoinOk());
    mockAddParticipant.mockRejectedValue(new Error('DDB write failed'));
    const res = await run(buildApp(), 'POST', '/api/video/sessions/vs-1/join', {
      body: { documentId: 'doc-1', displayName: 'Bob' },
      headers: { authorization: 'Bearer user-jwt' },
    });
    expect(res.statusCode).toBe(200);
  });

  it('returns 502 when fetch throws (network error → bad-gateway)', async () => {
    mockGetSession.mockResolvedValue(session);
    mockFetch.mockRejectedValue(new Error('network down'));
    const res = await run(buildApp(), 'POST', '/api/video/sessions/vs-1/join', {
      body: { documentId: 'doc-1' }, headers: { authorization: 'Bearer u' },
      headers: { authorization: 'Bearer user-jwt' },
    });
    expect(res.statusCode).toBe(502);
  });
});

// --- Circuit breaker + timeout coverage (P1 #4) ---------------------------

describe('SFU resilience — fetch timeouts', () => {
  it('returns 504 when the upstream fetch times out (TimeoutError)', async () => {
    mockFetch.mockRejectedValue(makeTimeoutError());
    const res = await run(buildApp(), 'POST', '/api/video/sessions', { body: { documentId: 'doc-1' }, headers: { authorization: 'Bearer u' } });
    expect(res.statusCode).toBe(504);
  });

  it('returns 504 on AbortError shape too', async () => {
    const abort = new Error('aborted');
    abort.name = 'AbortError';
    mockFetch.mockRejectedValue(abort);
    const res = await run(buildApp(), 'POST', '/api/video/sessions', { body: { documentId: 'doc-1' }, headers: { authorization: 'Bearer u' } });
    expect(res.statusCode).toBe(504);
  });

  it('passes an AbortSignal on every SFU fetch', async () => {
    mockFetch.mockResolvedValue(apiOk({ sessionId: 'arn:a', channelArn: 'arn:a', sessionFound: false }));
    await run(buildApp(), 'POST', '/api/video/sessions', { body: { documentId: 'doc-1' }, headers: { authorization: 'Bearer u' } });
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
      const res = await run(buildApp(), 'POST', '/api/video/sessions', { body: { documentId: `doc-${i}` }, headers: { authorization: 'Bearer u' } });
      expect(res.statusCode).toBe(502);
    }
    expect(mockFetch).toHaveBeenCalledTimes(5);

    const tripped = await run(buildApp(), 'POST', '/api/video/sessions', { body: { documentId: 'doc-after-open' }, headers: { authorization: 'Bearer u' } });
    expect(tripped.statusCode).toBe(503);
    // Crucial: with breaker OPEN, we MUST NOT have called fetch again.
    expect(mockFetch).toHaveBeenCalledTimes(5);
  });

  it('counts thrown fetch errors (e.g. ECONNREFUSED) as failures and trips after threshold', async () => {
    mockFetch.mockRejectedValue(new Error('ECONNREFUSED'));
    for (let i = 0; i < 5; i++) {
      const res = await run(buildApp(), 'POST', '/api/video/sessions', { body: { documentId: `doc-${i}` }, headers: { authorization: 'Bearer u' } });
      expect(res.statusCode).toBe(502);
    }
    const tripped = await run(buildApp(), 'POST', '/api/video/sessions', { body: { documentId: 'doc-after-open' }, headers: { authorization: 'Bearer u' } });
    expect(tripped.statusCode).toBe(503);
    expect(mockFetch).toHaveBeenCalledTimes(5);
  });

  it('counts upstream timeouts as failures and opens after threshold', async () => {
    mockFetch.mockRejectedValue(makeTimeoutError());
    for (let i = 0; i < 5; i++) {
      const res = await run(buildApp(), 'POST', '/api/video/sessions', { body: { documentId: `doc-${i}` }, headers: { authorization: 'Bearer u' } });
      expect(res.statusCode).toBe(504);
    }
    const tripped = await run(buildApp(), 'POST', '/api/video/sessions', { body: { documentId: 'doc-after-open' }, headers: { authorization: 'Bearer u' } });
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
    let r = await run(buildApp(), 'POST', '/api/video/sessions', { body: { documentId: 'a' }, headers: { authorization: 'Bearer u' } });
    expect(r.statusCode).toBe(503);
    r = await run(buildApp(), 'POST', '/api/video/sessions', { body: { documentId: 'b' }, headers: { authorization: 'Bearer u' } });
    expect(r.statusCode).toBe(503);

    // Breaker open → next call is fast-fail 503 without hitting fetch.
    const callsBeforeProbe = mockFetch.mock.calls.length;
    r = await run(buildApp(), 'POST', '/api/video/sessions', { body: { documentId: 'c' }, headers: { authorization: 'Bearer u' } });
    expect(r.statusCode).toBe(503);
    expect(mockFetch.mock.calls.length).toBe(callsBeforeProbe);

    // Advance past cooldown — breaker transitions to HALF_OPEN.
    jest.advanceTimersByTime(150);

    // Probe call succeeds → breaker closes. The probe consumes a fetch.
    mockFetch.mockResolvedValue(apiOk({ sessionId: 'arn:probe', channelArn: 'arn:probe', sessionFound: false }));
    r = await run(buildApp(), 'POST', '/api/video/sessions', { body: { documentId: 'd' }, headers: { authorization: 'Bearer u' } });
    expect(r.statusCode).toBe(201);
    expect(mockFetch.mock.calls.length).toBe(callsBeforeProbe + 1);

    // Subsequent calls succeed normally (breaker closed).
    r = await run(buildApp(), 'POST', '/api/video/sessions', { body: { documentId: 'e' }, headers: { authorization: 'Bearer u' } });
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
    await run(buildApp(), 'POST', '/api/video/sessions', { body: { documentId: 'a' }, headers: { authorization: 'Bearer u' } });
    await run(buildApp(), 'POST', '/api/video/sessions', { body: { documentId: 'b' }, headers: { authorization: 'Bearer u' } });

    // Advance past cooldown → HALF_OPEN.
    jest.advanceTimersByTime(150);

    // Probe still fails → breaker re-opens.
    mockFetch.mockResolvedValue(apiFail(503));
    let r = await run(buildApp(), 'POST', '/api/video/sessions', { body: { documentId: 'probe' }, headers: { authorization: 'Bearer u' } });
    expect(r.statusCode).toBe(503);

    // Next call (no further cooldown) is fast-fail without fetch.
    const callsAfterProbe = mockFetch.mock.calls.length;
    r = await run(buildApp(), 'POST', '/api/video/sessions', { body: { documentId: 'after' }, headers: { authorization: 'Bearer u' } });
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
      body: { documentId: 'doc-1' }, headers: { authorization: 'Bearer u' },
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
      body: { documentId: 'doc-1' }, headers: { authorization: 'Bearer u' },
    });
    expect(res.statusCode).toBe(404);
  });

  it('returns video API error status on channel delete failure', async () => {
    mockGetSession.mockResolvedValue(session);
    mockFetch.mockResolvedValue(apiFail(503));
    const res = await run(buildApp(), 'POST', '/api/video/sessions/vs-1/end', {
      body: { documentId: 'doc-1' }, headers: { authorization: 'Bearer u' },
    });
    expect(res.statusCode).toBe(503);
  });

  it('still returns 200 when endSession throws (non-fatal DB failure)', async () => {
    mockGetSession.mockResolvedValue(session);
    mockFetch.mockResolvedValue(apiOk({}));
    mockEndSession.mockRejectedValue(new Error('DDB write failed'));
    const res = await run(buildApp(), 'POST', '/api/video/sessions/vs-1/end', {
      body: { documentId: 'doc-1' }, headers: { authorization: 'Bearer u' },
      userId: 'tester',
    });
    expect(res.statusCode).toBe(200);
  });

  it('returns 500 when fetch throws (non-AppError outer catch)', async () => {
    mockGetSession.mockResolvedValue(session);
    mockFetch.mockRejectedValue(new Error('network error'));
    const res = await run(buildApp(), 'POST', '/api/video/sessions/vs-1/end', {
      body: { documentId: 'doc-1' }, headers: { authorization: 'Bearer u' },
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
    // Post-Wave-7 thin-proxy migration: reap uses VIDEO_ADMIN_TOKEN
    // unconditionally (DDB streamKey no longer populated on new sessions).
    expect((init as RequestInit).headers).toMatchObject({
      Authorization: expect.stringMatching(/^Bearer /),
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

describe('POST /api/video/sessions/:sessionId/end — LVS authoritative path', () => {
  it('forwards to LVS /end when auth + participantId present; mirrors soft-end to DDB', async () => {
    mockGetSession.mockResolvedValue({
      sessionId: 'vs-1', channelArn: 'arn:lvs:1', streamKey: 'sk', status: 'active',
      participants: [{ participantId: 'p-1' }, { participantId: 'p-2' }],
    });
    mockRemoveParticipant.mockResolvedValueOnce({ remaining: 1 });

    // First mockFetch = reapLvsParticipant DELETE; second = LVS POST /end.
    mockFetch.mockResolvedValueOnce(apiOk({}));  // reap 200
    mockFetch.mockResolvedValueOnce(apiOk({ participantsRemaining: 1, endedAt: null }));

    const res = await run(buildApp(), 'POST', '/api/video/sessions/vs-1/end', {
      body: { documentId: 'doc-1', participantId: 'p-1' },
      headers: { authorization: 'Bearer user-jwt' },
      userId: 'alice',
    });

    expect(res.statusCode).toBe(200);
    expect((res.body as { remaining: number; ended: boolean })).toMatchObject({ remaining: 1, ended: false });
    expect(mockFetch).toHaveBeenCalledTimes(2);
    // Second fetch is the LVS /end forward.
    const [url, init] = mockFetch.mock.calls[1];
    expect(url).toContain('/api/sessions/');
    expect(url).toContain(encodeURIComponent('arn:lvs:1'));
    expect(url).toContain('/end');
    expect((init as RequestInit).method).toBe('POST');
    expect(JSON.parse((init as RequestInit).body as string)).toMatchObject({ participantId: 'p-1' });
    expect(mockRemoveParticipant).toHaveBeenCalledWith('doc-1', 'vs-1', 'p-1');
  });

  it('hard-end path: LVS returns remaining=0, DDB mirror calls endSession', async () => {
    mockGetSession.mockResolvedValue({
      sessionId: 'vs-1', channelArn: 'arn:lvs:1', streamKey: 'sk', status: 'active',
      participants: [{ participantId: 'p-1' }],
    });
    mockFetch.mockResolvedValueOnce(apiOk({}));  // reap
    mockFetch.mockResolvedValueOnce(apiOk({ participantsRemaining: 0, endedAt: '2026-06-02T13:00:00Z' }));

    const res = await run(buildApp(), 'POST', '/api/video/sessions/vs-1/end', {
      body: { documentId: 'doc-1', participantId: 'p-1' },
      headers: { authorization: 'Bearer user-jwt' },
    });

    expect(res.statusCode).toBe(200);
    expect((res.body as { ended: boolean })).toMatchObject({ remaining: 0, ended: true });
    expect(mockEndSession).toHaveBeenCalledWith('doc-1', 'vs-1', '2026-06-02T13:00:00Z');
  });

  it('LVS 404 (session already ended) → 200 ended:true (idempotent)', async () => {
    mockGetSession.mockResolvedValue({
      sessionId: 'vs-1', channelArn: 'arn:lvs:1', streamKey: 'sk', status: 'active',
      participants: [{ participantId: 'p-1' }],
    });
    mockFetch.mockResolvedValueOnce(apiOk({}));  // reap
    mockFetch.mockResolvedValueOnce(apiFail(404));  // LVS /end 404

    const res = await run(buildApp(), 'POST', '/api/video/sessions/vs-1/end', {
      body: { documentId: 'doc-1', participantId: 'p-1' },
      headers: { authorization: 'Bearer user-jwt' },
    });

    expect(res.statusCode).toBe(200);
    expect((res.body as { ended: boolean }).ended).toBe(true);
  });

  it('falls back to DDB-direct when no auth header (sendBeacon path)', async () => {
    mockGetSession.mockResolvedValue({
      sessionId: 'vs-1', channelArn: 'arn:lvs:1', streamKey: 'sk', status: 'active',
      participants: [{ participantId: 'p-1' }, { participantId: 'p-2' }],
    });
    mockRemoveParticipant.mockResolvedValueOnce({ remaining: 1 });
    mockFetch.mockResolvedValueOnce(apiOk({}));  // reap only (no LVS /end forward)

    const res = await run(buildApp(), 'POST', '/api/video/sessions/vs-1/end', {
      body: { documentId: 'doc-1', participantId: 'p-1' },
      // No authorization header — sendBeacon path.
    });

    expect(res.statusCode).toBe(200);
    expect((res.body as { remaining: number; ended: boolean })).toMatchObject({ remaining: 1, ended: false });
    // Exactly one fetch (the reap); no LVS /end forward.
    expect(mockFetch).toHaveBeenCalledTimes(1);
    expect(mockRemoveParticipant).toHaveBeenCalled();
  });

  it('LVS upstream 5xx surfaces as 5xx error', async () => {
    mockGetSession.mockResolvedValue({
      sessionId: 'vs-1', channelArn: 'arn:lvs:1', streamKey: 'sk', status: 'active',
      participants: [{ participantId: 'p-1' }],
    });
    mockFetch.mockResolvedValueOnce(apiOk({}));  // reap
    mockFetch.mockResolvedValueOnce(apiFail(503));  // LVS /end 503

    const res = await run(buildApp(), 'POST', '/api/video/sessions/vs-1/end', {
      body: { documentId: 'doc-1', participantId: 'p-1' },
      headers: { authorization: 'Bearer user-jwt' },
    });

    expect(res.statusCode).toBe(503);
  });
});

describe('POST /api/video/sessions/:sessionId/refresh-token', () => {
  it('400 without lobbyName / documentId', async () => {
    const res = await run(buildApp(), 'POST', '/api/video/sessions/vs-1/refresh-token', {
      body: { participantId: 'p-1' },
    });
    expect(res.statusCode).toBe(400);
  });

  it('400 without participantId', async () => {
    const res = await run(buildApp(), 'POST', '/api/video/sessions/vs-1/refresh-token', {
      body: { documentId: 'doc-1' }, headers: { authorization: 'Bearer u' },
    });
    expect(res.statusCode).toBe(400);
  });

  it('404 when DDB session record missing', async () => {
    mockGetSession.mockResolvedValue(null);
    const res = await run(buildApp(), 'POST', '/api/video/sessions/vs-1/refresh-token', {
      body: { documentId: 'doc-1', participantId: 'p-1' },
      headers: { authorization: 'Bearer user-jwt' },
    });
    expect(res.statusCode).toBe(404);
  });

  it('401 when Authorization header missing (cannot forward to LVS)', async () => {
    mockGetSession.mockResolvedValue({
      sessionId: 'vs-1', channelArn: 'arn:lvs:1', streamKey: 'sk', status: 'active',
    });
    const res = await run(buildApp(), 'POST', '/api/video/sessions/vs-1/refresh-token', {
      body: { documentId: 'doc-1', participantId: 'p-1' },
    });
    expect(res.statusCode).toBe(401);
  });

  it('200 on happy path — forwards user auth + body to LVS, returns LVS response verbatim', async () => {
    mockGetSession.mockResolvedValue({
      sessionId: 'vs-1', channelArn: 'arn:lvs:1', streamKey: 'sk', status: 'active',
    });
    mockFetch.mockResolvedValueOnce(apiOk({
      token: 'new.jwt.here', expiresAt: '2026-06-02T13:00:00Z',
      participantId: 'p-1', role: 'publisher',
    }));
    const res = await run(buildApp(), 'POST', '/api/video/sessions/vs-1/refresh-token', {
      body: { documentId: 'doc-1', participantId: 'p-1', role: 'publisher' },
      headers: { authorization: 'Bearer user-jwt' },
    });
    expect(res.statusCode).toBe(200);
    expect((res.body as { token: string }).token).toBe('new.jwt.here');
    expect(mockFetch).toHaveBeenCalledTimes(1);
    const [url, init] = mockFetch.mock.calls[0];
    expect(url).toContain('/api/sessions/');
    expect(url).toContain('/refresh-token');
    expect(url).toContain(encodeURIComponent('arn:lvs:1'));
    expect((init as RequestInit).method).toBe('POST');
    expect(JSON.parse((init as RequestInit).body as string)).toMatchObject({
      participantId: 'p-1', role: 'publisher',
    });
  });

  it('propagates LVS 404 (participant not registered) verbatim to the FE', async () => {
    mockGetSession.mockResolvedValue({
      sessionId: 'vs-1', channelArn: 'arn:lvs:1', streamKey: 'sk', status: 'active',
    });
    mockFetch.mockResolvedValueOnce({
      ok: false, status: 404,
      json: async () => ({ error: 'participant not registered in session' }),
      text: async () => '',
    } as unknown as globalThis.Response);
    const res = await run(buildApp(), 'POST', '/api/video/sessions/vs-1/refresh-token', {
      body: { documentId: 'doc-1', participantId: 'p-stale' },
      headers: { authorization: 'Bearer user-jwt' },
    });
    expect(res.statusCode).toBe(404);
  });

  it('propagates LVS 410 (session ended) verbatim', async () => {
    mockGetSession.mockResolvedValue({
      sessionId: 'vs-1', channelArn: 'arn:lvs:1', streamKey: 'sk', status: 'active',
    });
    mockFetch.mockResolvedValueOnce({
      ok: false, status: 410,
      json: async () => ({ error: 'session ended' }),
      text: async () => '',
    } as unknown as globalThis.Response);
    const res = await run(buildApp(), 'POST', '/api/video/sessions/vs-1/refresh-token', {
      body: { documentId: 'doc-1', participantId: 'p-1' },
      headers: { authorization: 'Bearer user-jwt' },
    });
    expect(res.statusCode).toBe(410);
  });

  it('502 on LVS network error (mapSfuError translation)', async () => {
    mockGetSession.mockResolvedValue({
      sessionId: 'vs-1', channelArn: 'arn:lvs:1', streamKey: 'sk', status: 'active',
    });
    mockFetch.mockRejectedValueOnce(new Error('ECONNREFUSED'));
    const res = await run(buildApp(), 'POST', '/api/video/sessions/vs-1/refresh-token', {
      body: { documentId: 'doc-1', participantId: 'p-1' },
      headers: { authorization: 'Bearer user-jwt' },
    });
    expect(res.statusCode).toBe(502);
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
