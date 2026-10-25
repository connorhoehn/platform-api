// Integration tests for videoSessions routes.
//
// Mocks video-auth, videoSessionRepo, and auth at module level so we
// exercise live-video-streaming proxy, local record persistence, and error
// paths without touching any external APIs.

import express, { type NextFunction, type Request, type Response } from 'express';

const mockCreateSession = jest.fn();
const mockHangoutBySlug = jest.fn().mockResolvedValue(null);
const mockAddParticipant = jest.fn();
const mockAddOrReplaceParticipant = jest.fn();
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
const mockListActiveCandidates = jest.fn().mockResolvedValue([]);
const mockAddGuests = jest.fn();
const mockFindByChannelArn = jest.fn().mockResolvedValue([{ documentId: 'room:standup' }]);
// Profiles decide whether a DM records: both parties must have recordCalls on.
const mockGetProfile = jest.fn().mockResolvedValue(null);

// NFR #175: lobby access is its own unit (lib/__tests__/lobbyAccess.test.ts);
// here it allows unless a test says otherwise.
const mockCanUseLobby = jest.fn(async (..._a: unknown[]) => true);
// A test may put the REAL lobby rule (makeLobbyAccess over stub rooms) here.
let mockRealLobbyAccess: { assertLobbyAccess: (...a: never[]) => Promise<void> } | null = null;
jest.mock('../../lib/lobbyAccess', () => ({
  tenantLobby: jest.requireActual('../../lib/lobbyAccess').tenantLobby,
  lobbyAccess: () => ({
    canUseLobby: (...a: unknown[]) => mockCanUseLobby(...a),
    assertLobbyAccess: async (...a: unknown[]) => {
      if (mockRealLobbyAccess) return mockRealLobbyAccess.assertLobbyAccess(...(a as never[]));
      if (!(await mockCanUseLobby(...a))) {
        const { NotFoundError } = jest.requireActual('../../middleware/error-handler');
        throw new NotFoundError('Session not found');
      }
    },
  }),
}));

jest.mock('../../middleware/auth', () => ({
  requireAuth: (_req: Request, _res: Response, next: NextFunction) => next(),
  optionalAuth: (_req: Request, _res: Response, next: NextFunction) => next(),
}));

jest.mock('../../lib/video-auth', () => ({
  VIDEO_API_URL: 'https://video.test',
}));

jest.mock('../../repositories', () => ({
  hangoutRoomRepo: { getRoomBySlug: (...args: unknown[]) => mockHangoutBySlug(...args) },
  videoSessionRepo: {
    createSession: (...args: unknown[]) => mockCreateSession(...args),
    addParticipant: (...args: unknown[]) => mockAddParticipant(...args),
    addOrReplaceParticipant: (...args: unknown[]) => mockAddOrReplaceParticipant(...args),
    endSession: (...args: unknown[]) => mockEndSession(...args),
    getSessionsByDocument: (...args: unknown[]) => mockGetSessionsByDocument(...args),
    getSession: (...args: unknown[]) => mockGetSession(...args),
    findActiveByLobbyName: (...args: unknown[]) => mockFindActiveByLobbyName(...args),
    removeParticipant: (...args: unknown[]) => mockRemoveParticipant(...args),
    removeParticipantByUserId: (...args: unknown[]) => mockRemoveParticipantByUserId(...args),
    listActiveCandidates: (...args: unknown[]) => mockListActiveCandidates(...args),
    findByChannelArn: (...args: unknown[]) => mockFindByChannelArn(...args),
    addGuests: (...args: unknown[]) => mockAddGuests(...args),
  },
  profileRepo: {
    getProfile: (...args: unknown[]) => mockGetProfile(...args),
  },
}));

const mockFetch = jest.fn();
(globalThis as unknown as { fetch: typeof mockFetch }).fetch = mockFetch;

// The captions toggle starts/stops a `live-captions` run; the run itself is
// tested in recordings/__tests__/liveCaptions.test.ts.
const mockStartLiveCaptions = jest.fn();
const mockStopLiveCaptions = jest.fn();
jest.mock('../../recordings/liveCaptions', () => ({
  startLiveCaptions: (...args: unknown[]) => mockStartLiveCaptions(...args),
  stopLiveCaptions: (...args: unknown[]) => mockStopLiveCaptions(...args),
}));

import {
  videoSessionsRouter,
  __resetSfuBreakerForTests,
  __resetLiveLobbyCacheForTests,
  __configureSfuBreakerForTests,
  reapLvsParticipant,
  reapStaleSessions,
  deferLvsEnd,
  __setLvsEndRetryDelaysForTests,
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

interface RunOpts { body?: unknown; userId?: string; headers?: Record<string, string>; user?: Record<string, unknown> }

/** GET /api/video/sessions/by-lobby/:lobbyName */
type ByLobbyBody = { lobbyName: string; live: boolean; channelArn: string | null };

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
    user: opts.user ?? { sub: opts.userId ?? 'tester' },
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
  mockHangoutBySlug.mockReset().mockResolvedValue(null);
  mockAddParticipant.mockReset().mockResolvedValue(undefined);
  mockAddOrReplaceParticipant.mockReset().mockResolvedValue({ displaced: [], remaining: 1 });
  mockRemoveParticipant.mockReset();
  mockRemoveParticipantByUserId.mockReset();
  mockFindActiveByLobbyName.mockReset().mockResolvedValue(null);
  mockEndSession.mockReset().mockResolvedValue(undefined);
  mockGetSessionsByDocument.mockReset().mockResolvedValue([]);
  mockGetSession.mockReset();
  mockFetch.mockReset();
  mockListActiveCandidates.mockReset().mockResolvedValue([]);
  mockGetProfile.mockReset().mockResolvedValue(null);
  __resetLiveLobbyCacheForTests();
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
      body: { documentId: 'doc-1' },
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
      // A row written before decisions existed: the joiner's own decision fills in.
      recording: { enabled: true, reason: 'default-on' },
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
  describe('GET /sessions/live', () => {
    // The whole reason this route probes instead of returning the rows: a
    // conversation list badges every row it is given, so an unprobed list
    // marks every room that has ever hosted a call as live, permanently.
    it('drops a leaked active session that nobody is publishing to', async () => {
      mockListActiveCandidates.mockResolvedValueOnce([
        { documentId: 'room:live', channelArn: 'arn:channel:live', startedAt: '2026-09-07T00:00:00.000Z', startedBy: 'bob' },
        { documentId: 'room:leaked', channelArn: 'arn:channel:leaked', startedAt: '2026-09-01T00:00:00.000Z', startedBy: 'hank' },
      ]);
      // Probes run in registration order within a batch.
      mockFetch.mockImplementation((url: unknown) =>
        Promise.resolve(
          String(url).includes('arn%3Achannel%3Alive')
            ? apiOk({ stream: { state: 'LIVE' } })
            : apiOk({ stream: { state: 'OFFLINE' } }),
        ),
      );

      const res = await run(buildApp(), 'GET', '/api/video/sessions/live', {
        headers: { authorization: 'Bearer user-jwt' },
        userId: 'alice',
      });

      expect(res.statusCode).toBe(200);
      const body = res.body as { lobbies: { lobbyName: string }[] };
      expect(body.lobbies.map((l) => l.lobbyName)).toEqual(['room:live']);
    });

    // Unlike by-lobby, which falls back to the roster so an SFU outage cannot
    // hide a running broadcast from someone asking about one specific room.
    // A badge is a passive claim about rooms nobody asked about, and a stale
    // LIVE on a dead room is a worse lie than a missing badge on a live one.
    it('drops candidates the SFU could not be asked about', async () => {
      mockListActiveCandidates.mockResolvedValueOnce([
        { documentId: 'room:unknown', channelArn: 'arn:channel:unknown', startedAt: null, startedBy: null },
      ]);
      mockFetch.mockResolvedValue(apiFail(500));

      const res = await run(buildApp(), 'GET', '/api/video/sessions/live', {
        headers: { authorization: 'Bearer user-jwt' },
        userId: 'alice',
      });
      expect(res.statusCode).toBe(200);
      expect((res.body as { lobbies: unknown[] }).lobbies).toEqual([]);
    });

    // The caller is a conversation list drawing badges: it has to render
    // whether or not this could be answered.
    it('answers an empty list rather than an error when the store fails', async () => {
      mockListActiveCandidates.mockRejectedValueOnce(new Error('ddb down'));
      const res = await run(buildApp(), 'GET', '/api/video/sessions/live', {
        headers: { authorization: 'Bearer user-jwt' },
        userId: 'alice',
      });
      expect(res.statusCode).toBe(200);
      expect((res.body as { lobbies: unknown[]; degraded?: boolean }).lobbies).toEqual([]);
      expect((res.body as { degraded?: boolean }).degraded).toBe(true);
    });

    // N clients polling a badge must not become N x M requests at the SFU.
    it('serves a second caller from cache without re-probing', async () => {
      mockListActiveCandidates.mockResolvedValue([
        { documentId: 'room:live', channelArn: 'arn:channel:live', startedAt: null, startedBy: null },
      ]);
      mockFetch.mockResolvedValue(apiOk({ stream: { state: 'LIVE' } }));

      const app = buildApp();
      await run(app, 'GET', '/api/video/sessions/live', {
        headers: { authorization: 'Bearer user-jwt' }, userId: 'alice',
      });
      const probesAfterFirst = mockFetch.mock.calls.length;

      const second = await run(app, 'GET', '/api/video/sessions/live', {
        headers: { authorization: 'Bearer user-jwt' }, userId: 'bob',
      });
      expect((second.body as { cached?: boolean }).cached).toBe(true);
      expect(mockFetch.mock.calls.length).toBe(probesAfterFirst);
    });
  });

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
      // Whether or not anything is live, the answer is a 200 — what must NOT
      // happen is a session being created.
      expect(res.statusCode).toBe(200);
      expect((res.body as ByLobbyBody).live).toBe(true);
      expect((res.body as ByLobbyBody).channelArn).toBe('arn:channel:watch');
      expect((res.body as ByLobbyBody).lobbyName).toBe('room:asdf');
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

    // Not an error: most conversations have no stream most of the time. As a
    // 404 this painted a red console error every time someone opened a chat.
    it('answers a plain no when nothing is live', async () => {
      mockFindActiveByLobbyName.mockResolvedValueOnce(null);
      const res = await run(buildApp(), 'GET', '/api/video/sessions/by-lobby/room%3Anever-used', {
        headers: { authorization: 'Bearer user-jwt' },
        userId: 'alice',
      });
      expect(res.statusCode).toBe(200);
      expect((res.body as ByLobbyBody).live).toBe(false);
      expect((res.body as ByLobbyBody).channelArn).toBeNull();
    });

    // `status: 'active'` is a lifecycle flag, not evidence of a broadcast. It
    // only flips to 'ended' when the last participant LEAVES through the leave
    // route, so a session whose people closed their tabs, crashed, or were a
    // headless test that never said goodbye stays 'active' forever. Reporting
    // one of those as watchable is how a room that once had a call ended up
    // permanently offering a Watch button onto a black rectangle marked LIVE.
    it('reports not-live for a leaked session that nobody is publishing to', async () => {
      mockFindActiveByLobbyName.mockResolvedValueOnce({
        sessionId: 'sess-leaked', channelArn: 'arn:channel:leaked',
        startedAt: '2026-09-01T00:00:00.000Z', startedBy: 'hank',
        participants: [],
      });
      const res = await run(buildApp(), 'GET', '/api/video/sessions/by-lobby/room%3Afresh-probe-room', {
        headers: { authorization: 'Bearer user-jwt' },
        userId: 'alice',
      });
      expect(res.statusCode).toBe(200);
      expect((res.body as ByLobbyBody).live).toBe(false);
    });

    it('reports not-live when the record predates participant tracking entirely', async () => {
      mockFindActiveByLobbyName.mockResolvedValueOnce({
        sessionId: 'sess-old', channelArn: 'arn:channel:old',
        startedAt: null, startedBy: null,
      });
      const res = await run(buildApp(), 'GET', '/api/video/sessions/by-lobby/room%3Aold', {
        headers: { authorization: 'Bearer user-jwt' },
        userId: 'alice',
      });
      expect(res.statusCode).toBe(200);
      expect((res.body as ByLobbyBody).live).toBe(false);
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
      expect(res.statusCode).toBe(200);
      expect((res.body as ByLobbyBody).live).toBe(false);
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
      expect((res.body as ByLobbyBody).channelArn).toBe('arn:channel:live');
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

    it('reports not-live when the SFU is unreachable AND the roster is empty', async () => {
      mockFindActiveByLobbyName.mockResolvedValueOnce({
        sessionId: 'sess-none', channelArn: 'arn:channel:none',
        startedAt: null, startedBy: null, participants: [],
      });
      mockFetch.mockRejectedValue(new Error('connect ECONNREFUSED'));
      const res = await run(buildApp(), 'GET', '/api/video/sessions/by-lobby/room%3Anone', {
        headers: { authorization: 'Bearer user-jwt' }, userId: 'alice',
      });
      expect(res.statusCode).toBe(200);
      expect((res.body as ByLobbyBody).live).toBe(false);
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
      expect((res.body as ByLobbyBody).lobbyName).toBe('dm:alice:bob');
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
      body: { documentId: 'doc-1' },
      headers: { authorization: 'Bearer user-jwt' },
    });
    expect(res.statusCode).toBe(404);
  });

  it('forwards LVS 5xx (counted as breaker failure)', async () => {
    mockFetch.mockResolvedValue(apiFail(502));
    const res = await run(buildApp(), 'POST', '/api/video/sessions', {
      body: { documentId: 'doc-1' },
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
      body: { documentId: 'doc-1' },
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
    expect(mockAddOrReplaceParticipant).toHaveBeenCalledWith('doc-1', 'vs-1', expect.objectContaining({ userId: 'tester', participantId: 'p-1' }));
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

  it('stores only the authenticated actor, ignoring a userId supplied in the join body', async () => {
    mockGetSession.mockResolvedValue(session);
    mockFetch.mockResolvedValue(lvsJoinOk());
    const res = await run(buildApp(), 'POST', '/api/video/sessions/vs-1/join', {
      body: { documentId: 'doc-1', userId: 'unrelated-person', displayName: 'Nora' },
      headers: { authorization: 'Bearer user-jwt' }, userId: 'u-nora',
    });
    expect(res.statusCode).toBe(200);
    expect(mockAddOrReplaceParticipant).toHaveBeenCalledWith('doc-1', 'vs-1', expect.objectContaining({ userId: 'u-nora', participantId: 'p-1' }));
    expect(JSON.parse(mockFetch.mock.calls[0][1].body)).toMatchObject({ userId: 'u-nora' });
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
      body: { documentId: 'doc-1' },
      headers: { authorization: 'Bearer user-jwt' },
    });
    expect(res.statusCode).toBe(404);
  });

  it('forwards 4xx from LVS on token request failure', async () => {
    mockGetSession.mockResolvedValue(session);
    mockFetch.mockResolvedValue(apiFail(403));
    const res = await run(buildApp(), 'POST', '/api/video/sessions/vs-1/join', {
      body: { documentId: 'doc-1' },
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
      body: { documentId: 'doc-1' },
      headers: { authorization: 'Bearer user-jwt' },
    });
    expect(res.statusCode).toBe(410);
  });

  it('still returns 200 when addParticipant throws (non-fatal DB failure)', async () => {
    mockGetSession.mockResolvedValue(session);
    mockFetch.mockResolvedValue(lvsJoinOk());
    mockAddOrReplaceParticipant.mockRejectedValue(new Error('DDB write failed'));
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
      body: { documentId: 'doc-1' },
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

  // A15: hanging up ends the call even when LVS cannot hear it. The record
  // is settled here and LVS is told later (deferLvsEnd); before, the /end
  // answered 503 and the session stayed `active` — Live after everyone left.
  it('LVS upstream 5xx: the leave still settles the record (200), the LVS /end is deferred', async () => {
    __setLvsEndRetryDelaysForTests([]);
    try {
      mockGetSession.mockResolvedValue({
        sessionId: 'vs-1', channelArn: 'arn:lvs:1', streamKey: 'sk', status: 'active',
        participants: [{ participantId: 'p-1' }],
      });
      mockRemoveParticipant.mockResolvedValueOnce({ remaining: 0 });
      mockFetch.mockResolvedValueOnce(apiOk({}));  // reap
      mockFetch.mockResolvedValueOnce(apiFail(503));  // LVS /end 503

      const res = await run(buildApp(), 'POST', '/api/video/sessions/vs-1/end', {
        body: { documentId: 'doc-1', participantId: 'p-1' },
        headers: { authorization: 'Bearer user-jwt' },
      });

      expect(res.statusCode).toBe(200);
      expect(res.body).toMatchObject({ remaining: 0, ended: true, lvsDeferred: true });
      expect(mockRemoveParticipant).toHaveBeenCalledWith('doc-1', 'vs-1', 'p-1');
      expect(mockEndSession).toHaveBeenCalledWith('doc-1', 'vs-1', expect.any(String));
    } finally {
      __setLvsEndRetryDelaysForTests(null);
    }
  });

  it('SFU refusing connections: one of two leaves, the other stays; 200 with the remaining count', async () => {
    __setLvsEndRetryDelaysForTests([]);
    try {
      mockGetSession.mockResolvedValue({
        sessionId: 'vs-1', channelArn: 'arn:lvs:1', streamKey: 'sk', status: 'active',
        participants: [{ participantId: 'p-1' }, { participantId: 'p-2' }],
      });
      mockRemoveParticipant.mockResolvedValueOnce({ remaining: 1 });
      mockFetch.mockResolvedValueOnce(apiOk({}));  // reap
      mockFetch.mockRejectedValueOnce(Object.assign(new TypeError('fetch failed'), { cause: { code: 'ECONNREFUSED' } }));

      const res = await run(buildApp(), 'POST', '/api/video/sessions/vs-1/end', {
        body: { documentId: 'doc-1', participantId: 'p-1' },
        headers: { authorization: 'Bearer user-jwt' },
      });

      expect(res.statusCode).toBe(200);
      expect(res.body).toMatchObject({ remaining: 1, ended: false, lvsDeferred: true });
      expect(mockEndSession).not.toHaveBeenCalled();
    } finally {
      __setLvsEndRetryDelaysForTests(null);
    }
  });

  it('a deferred LVS /end that finds the session gone (LVS restarted) ends the record', async () => {
    mockGetSession.mockResolvedValue({ sessionId: 'vs-1', channelArn: 'arn:lvs:1', status: 'active', participants: [{ participantId: 'p-2' }] });
    mockFetch.mockResolvedValueOnce(apiFail(404));
    deferLvsEnd({ channelArn: 'arn:lvs:1', participantId: 'p-1', authHeader: 'Bearer user-jwt', sessionId: 'vs-1', lobbyName: 'doc-1' }, [1]);
    await new Promise((r) => setTimeout(r, 50));
    expect(mockEndSession).toHaveBeenCalledWith('doc-1', 'vs-1', expect.any(String));
  });

  it('a deferred LVS /end retries until LVS answers', async () => {
    mockFetch.mockRejectedValueOnce(new TypeError('fetch failed'));
    mockFetch.mockResolvedValueOnce(apiOk({ participantsRemaining: 0 }));
    deferLvsEnd({ channelArn: 'arn:lvs:1', participantId: 'p-1', authHeader: 'Bearer user-jwt', sessionId: 'vs-1' }, [1, 1]);
    await new Promise((r) => setTimeout(r, 50));
    expect(mockFetch).toHaveBeenCalledTimes(2);
    const [url, init] = mockFetch.mock.calls[1];
    expect(url).toContain(`/api/sessions/${encodeURIComponent('arn:lvs:1')}/end`);
    expect(JSON.parse((init as RequestInit).body as string)).toEqual({ participantId: 'p-1' });
  });
});

// ---------------------------------------------------------------------------
// The participant leak — sessions that never end
// ---------------------------------------------------------------------------
//
// Measured on a real room: FOUR rows left `status: 'active'` in
// document-video-sessions for one room after the call had been hung up.
// Two mechanisms, and both are here:
//
//   1. A room entry joins the video session TWICE (the room bridge in
//      hangoutRooms.ts, then the call widget) and hangs up ONCE. The old
//      dedupe could not catch it, because it tested the participantId — which
//      LVS mints fresh on every join as `${userId}-${Date.now()}`, so the id
//      being tested for is by construction one we have never seen.
//   2. Nobody says goodbye at all: a crash, a closed lid, a killed headless
//      test. `status` has no other way to become 'ended' and the row carries
//      no TTL until it does, so it is immortal.

describe('POST /api/video/sessions — superseded lobby rows are closed out', () => {
  it('ends the previous active row when the lobby moves to a new channel', async () => {
    // The reuse branch only returns early when the arns MATCH. A second call
    // in the same lobby on a fresh channel therefore used to write a new
    // 'active' row beside the old one, forever.
    mockFetch.mockResolvedValue(lvsSessionCreated('arn:channel:new'));
    mockFindActiveByLobbyName.mockResolvedValue(null);
    mockGetSessionsByDocument.mockResolvedValue([
      { documentId: 'room:standup', sessionId: 'vs-old', channelArn: 'arn:channel:old', status: 'active' },
    ]);

    const res = await run(buildApp(), 'POST', '/api/video/sessions', {
      body: { lobbyName: 'room:standup' },
      headers: { authorization: 'Bearer user-jwt' },
    });

    expect(res.statusCode).toBe(201);
    expect(mockEndSession).toHaveBeenCalledWith('room:standup', 'vs-old', expect.any(String));
  });

  it('leaves a same-channel row alone — two clients racing to create one lobby', async () => {
    mockFetch.mockResolvedValue(lvsSessionCreated('arn:channel:same'));
    mockFindActiveByLobbyName.mockResolvedValue(null);
    mockGetSessionsByDocument.mockResolvedValue([
      { documentId: 'room:standup', sessionId: 'vs-peer', channelArn: 'arn:channel:same', status: 'active' },
    ]);

    await run(buildApp(), 'POST', '/api/video/sessions', {
      body: { lobbyName: 'room:standup' },
      headers: { authorization: 'Bearer user-jwt' },
    });

    expect(mockEndSession).not.toHaveBeenCalled();
  });
});

describe('POST /api/video/sessions/:sessionId/join — double-join idempotence', () => {
  const session = {
    documentId: 'room:standup', sessionId: 'vs-1', channelArn: 'arn:ch:1',
    streamKey: '', status: 'active',
    participants: [{ userId: 'alice', participantId: 'alice-bridge', displayName: 'Alice', joinedAt: '2026-09-07T00:00:00Z' }],
  };

  function lvsJoinOk(participantId: string) {
    return apiOk({ token: 'jwt', participantId, role: 'publisher', displayName: 'Alice' });
  }

  it('collapses the second join and releases the first participant in LVS', async () => {
    mockGetSession.mockResolvedValue(session);
    // The widget's join mints a DIFFERENT id for the same human.
    mockAddOrReplaceParticipant.mockResolvedValue({
      displaced: [{ userId: 'alice', participantId: 'alice-bridge' }],
      remaining: 1,
    });
    mockFetch
      .mockResolvedValueOnce(lvsJoinOk('alice-widget'))  // LVS /join
      .mockResolvedValue(apiOk({}));                     // release calls

    const res = await run(buildApp(), 'POST', '/api/video/sessions/vs-1/join', {
      body: { lobbyName: 'room:standup', displayName: 'Alice' },
      headers: { authorization: 'Bearer user-jwt' },
      userId: 'alice',
    });

    expect(res.statusCode).toBe(200);
    // One participant survives, and it is the one whose id the client holds
    // and will send back on hang-up.
    expect((res.body as { participantId: string }).participantId).toBe('alice-widget');

    const urls = mockFetch.mock.calls.map((c) => String(c[0]));
    // The displaced participant is released in BOTH of LVS's registries:
    // its sessionStore (POST /api/sessions/:arn/end — the count /end reads)
    // and its live transports (DELETE .../participants/:pid). A reap alone
    // would leave participantsRemaining above zero forever.
    expect(urls.some((u) => u.includes('/end'))).toBe(true);
    expect(urls.some((u) => u.includes('/participants/alice-bridge'))).toBe(true);
    // Never the surviving one.
    expect(urls.some((u) => u.includes('/participants/alice-widget'))).toBe(false);
  });

  it('threads the per-tab clientId into the stored participant', async () => {
    mockGetSession.mockResolvedValue(session);
    mockAddOrReplaceParticipant.mockResolvedValue({ displaced: [], remaining: 1 });
    mockFetch.mockResolvedValue(lvsJoinOk('alice-widget'));

    await run(buildApp(), 'POST', '/api/video/sessions/vs-1/join', {
      body: { lobbyName: 'room:standup', clientId: 'tab-7' },
      headers: { authorization: 'Bearer user-jwt' },
      userId: 'alice',
    });

    expect(mockAddOrReplaceParticipant).toHaveBeenCalledWith(
      'room:standup', 'vs-1',
      expect.objectContaining({ clientId: 'tab-7', participantId: 'alice-widget' }),
    );
  });

  it('a first join displaces nothing and touches no release path', async () => {
    mockGetSession.mockResolvedValue({ ...session, participants: [] });
    mockAddOrReplaceParticipant.mockResolvedValue({ displaced: [], remaining: 1 });
    mockFetch.mockResolvedValue(lvsJoinOk('alice-1'));

    const res = await run(buildApp(), 'POST', '/api/video/sessions/vs-1/join', {
      body: { lobbyName: 'room:standup' },
      headers: { authorization: 'Bearer user-jwt' },
      userId: 'alice',
    });

    expect(res.statusCode).toBe(200);
    // Exactly one upstream call: the join itself.
    expect(mockFetch).toHaveBeenCalledTimes(1);
  });
});

describe('POST /api/video/sessions/:sessionId/end — join/leave symmetry', () => {
  it('the participantId a join handed out is the one a leave removes', async () => {
    mockGetSession.mockResolvedValue({
      documentId: 'room:standup', sessionId: 'vs-1', channelArn: 'arn:lvs:1', status: 'active',
      participants: [{ userId: 'alice', participantId: 'alice-widget' }],
    });
    mockFetch
      .mockResolvedValueOnce(apiOk({}))                                   // reap
      .mockResolvedValueOnce(apiOk({ participantsRemaining: 0, endedAt: '2026-09-07T01:00:00Z' }));

    const res = await run(buildApp(), 'POST', '/api/video/sessions/vs-1/end', {
      body: { lobbyName: 'room:standup', participantId: 'alice-widget' },
      headers: { authorization: 'Bearer user-jwt' },
      userId: 'alice',
    });

    expect((res.body as { ended: boolean }).ended).toBe(true);
    expect(mockEndSession).toHaveBeenCalledWith('room:standup', 'vs-1', '2026-09-07T01:00:00Z');
  });

  it('a beacon that lost its participantId still names one entry via clientId', async () => {
    // The userId fallback below this one wipes EVERY tab the user has open;
    // the per-tab key resolves to exactly the row this tab created.
    mockGetSession.mockResolvedValue({
      documentId: 'room:standup', sessionId: 'vs-1', channelArn: 'arn:lvs:1', status: 'active',
      participants: [
        { userId: 'alice', participantId: 'alice-t1', clientId: 'tab-1' },
        { userId: 'alice', participantId: 'alice-t2', clientId: 'tab-2' },
      ],
    });
    mockRemoveParticipant.mockResolvedValueOnce({ remaining: 1 });
    mockFetch.mockResolvedValue(apiOk({}));

    const res = await run(buildApp(), 'POST', '/api/video/sessions/vs-1/end', {
      body: { lobbyName: 'room:standup', clientId: 'tab-2' },
      // No auth header — sendBeacon. NFR #175: the entry must be the caller's.
      userId: 'alice',
    });

    expect(res.statusCode).toBe(200);
    expect(mockRemoveParticipant).toHaveBeenCalledWith('room:standup', 'vs-1', 'alice-t2');
    expect(mockRemoveParticipantByUserId).not.toHaveBeenCalled();
  });
});

describe('stale-session reaper — sessions end even when nobody says goodbye', () => {
  const ONE_HOUR_AGO = new Date(Date.now() - 60 * 60 * 1000).toISOString();
  const JUST_NOW = new Date().toISOString();

  it('ends an active session whose channel the SFU says is not publishing', async () => {
    mockListActiveCandidates.mockResolvedValue([
      { documentId: 'room:ghost', sessionId: 'vs-ghost', channelArn: 'arn:ch:ghost', startedAt: ONE_HOUR_AGO },
    ]);
    mockFetch.mockResolvedValue(apiOk({ stream: { state: 'OFFLINE' } }));

    const ended = await reapStaleSessions();

    expect(ended).toEqual(['vs-ghost']);
    expect(mockEndSession).toHaveBeenCalledWith('room:ghost', 'vs-ghost', expect.any(String));
  });

  it('reaps EVERY leaked row for a lobby, not just the newest', async () => {
    // The bug as measured: one room, four rows, all still 'active'. Only the
    // newest is reachable through any read path, so a per-lobby collapse
    // would strand the other three forever.
    mockListActiveCandidates.mockImplementation(
      async (_limit?: number, opts?: { perLobby?: boolean }) =>
        opts?.perLobby === false
          ? [1, 2, 3, 4].map((n) => ({
              documentId: 'room:standup', sessionId: `vs-${n}`,
              channelArn: `arn:ch:${n}`, startedAt: ONE_HOUR_AGO,
            }))
          : [],
    );
    mockFetch.mockResolvedValue(apiOk({ stream: { state: 'OFFLINE' } }));

    const ended = await reapStaleSessions();

    expect(ended).toEqual(['vs-1', 'vs-2', 'vs-3', 'vs-4']);
    expect(mockEndSession).toHaveBeenCalledTimes(4);
  });

  it('spares a session that is still publishing', async () => {
    mockListActiveCandidates.mockResolvedValue([
      { documentId: 'room:live', sessionId: 'vs-live', channelArn: 'arn:ch:live', startedAt: ONE_HOUR_AGO },
    ]);
    mockFetch.mockResolvedValue(apiOk({ stream: { state: 'LIVE' } }));

    expect(await reapStaleSessions()).toEqual([]);
    expect(mockEndSession).not.toHaveBeenCalled();
  });

  it('spares a session too young to judge — a DM that is still ringing is OFFLINE', async () => {
    mockListActiveCandidates.mockResolvedValue([
      { documentId: 'dm:a:b', sessionId: 'vs-ringing', channelArn: 'arn:ch:ring', startedAt: JUST_NOW },
    ]);
    mockFetch.mockResolvedValue(apiOk({ stream: { state: 'OFFLINE' } }));

    expect(await reapStaleSessions()).toEqual([]);
    expect(mockEndSession).not.toHaveBeenCalled();
  });

  it('never ends a session on a probe it could not make', async () => {
    // `null` is "LVS could not be asked", not "nothing is on". Hanging up on
    // people who are still talking because a probe timed out is far worse
    // than leaving a row to the next sweep.
    mockListActiveCandidates.mockResolvedValue([
      { documentId: 'room:unknown', sessionId: 'vs-unknown', channelArn: 'arn:ch:u', startedAt: ONE_HOUR_AGO },
    ]);
    mockFetch.mockRejectedValue(new Error('SFU unreachable'));

    expect(await reapStaleSessions()).toEqual([]);
    expect(mockEndSession).not.toHaveBeenCalled();
  });

  it('GET /sessions/live runs the sweep, so a poll of the badge list tidies the table', async () => {
    mockListActiveCandidates.mockResolvedValue([
      { documentId: 'room:ghost', sessionId: 'vs-ghost', channelArn: 'arn:ch:ghost', startedAt: ONE_HOUR_AGO },
    ]);
    mockFetch.mockResolvedValue(apiOk({ stream: { state: 'OFFLINE' } }));

    const res = await run(buildApp(), 'GET', '/api/video/sessions/live', {
      headers: { authorization: 'Bearer user-jwt' },
    });

    expect((res.body as { lobbies: unknown[] }).lobbies).toEqual([]);
    expect(mockEndSession).toHaveBeenCalledWith('room:ghost', 'vs-ghost', expect.any(String));
  });

  it('GET /sessions/by-lobby closes out the dead row it just disbelieved', async () => {
    mockFindActiveByLobbyName.mockResolvedValueOnce({
      documentId: 'room:ghost', sessionId: 'vs-ghost', channelArn: 'arn:ch:ghost',
      startedAt: ONE_HOUR_AGO, participants: [{ participantId: 'leaked' }],
    });
    mockFetch.mockResolvedValue(apiOk({ stream: { state: 'OFFLINE' } }));

    const res = await run(buildApp(), 'GET', '/api/video/sessions/by-lobby/room%3Aghost', {
      headers: { authorization: 'Bearer user-jwt' },
    });

    expect((res.body as { live: boolean }).live).toBe(false);
    await new Promise((r) => setTimeout(r, 0));  // the write is fire-and-forget
    expect(mockEndSession).toHaveBeenCalledWith('room:ghost', 'vs-ghost', expect.any(String));
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

// join-knock: someone allowed on a private lobby brings a third person into
// its live call — the grant lib/lobbyAccess.ts honours while the call lasts.
describe('POST /api/video/sessions/guests', () => {
  beforeEach(() => {
    mockAddGuests.mockReset().mockImplementation(async (_l: string, _s: string, ids: string[]) => ({ sessionId: 'vs-1', guests: ids }));
    mockCanUseLobby.mockReset().mockImplementation(async () => true);
  });

  it('adds the guests to the lobby\'s ACTIVE session and answers them', async () => {
    mockFindActiveByLobbyName.mockResolvedValue({ documentId: 'dm:carol:hank', sessionId: 'vs-1', status: 'active' });
    const res = await run(buildApp(), 'POST', '/api/video/sessions/guests', {
      userId: 'carol', body: { lobbyName: 'dm:carol:hank', userIds: ['bob', 'bob', 7] },
    });
    expect(res.statusCode).toBe(200);
    expect(mockAddGuests).toHaveBeenCalledWith('dm:carol:hank', 'vs-1', ['bob']);
    expect(res.body).toEqual({ sessionId: 'vs-1', guests: ['bob'] });
  });

  it('refuses a caller who is not allowed on the lobby (404) and writes nothing', async () => {
    mockCanUseLobby.mockImplementation(async () => false);
    mockFindActiveByLobbyName.mockResolvedValue({ documentId: 'dm:carol:hank', sessionId: 'vs-1', status: 'active' });
    const res = await run(buildApp(), 'POST', '/api/video/sessions/guests', {
      userId: 'eve', body: { lobbyName: 'dm:carol:hank', userIds: ['eve'] },
    });
    expect(res.statusCode).toBe(404);
    expect(mockAddGuests).not.toHaveBeenCalled();
  });

  it('404 when the lobby has no call in progress — there is nothing to be let into', async () => {
    mockFindActiveByLobbyName.mockResolvedValue(null);
    const res = await run(buildApp(), 'POST', '/api/video/sessions/guests', {
      userId: 'carol', body: { lobbyName: 'dm:carol:hank', userIds: ['bob'] },
    });
    expect(res.statusCode).toBe(404);
    expect(mockAddGuests).not.toHaveBeenCalled();
  });

  it('400 without a lobby or without user ids', async () => {
    expect((await run(buildApp(), 'POST', '/api/video/sessions/guests', { body: { userIds: ['bob'] } })).statusCode).toBe(400);
    expect((await run(buildApp(), 'POST', '/api/video/sessions/guests', { body: { lobbyName: 'dm:a:b', userIds: [] } })).statusCode).toBe(400);
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

// Recording is a decision, made here and kept: on the row, in the response,
// and in what LVS is told. A DM records only with everyone's consent.
describe('POST /api/video/sessions — recording decision', () => {
  const lastLvsBody = () => JSON.parse((mockFetch.mock.calls[mockFetch.mock.calls.length - 1][1] as RequestInit).body as string);
  const profiles = (flags: Record<string, boolean | undefined>) =>
    mockGetProfile.mockImplementation(async (id: string) => (id in flags ? { userId: id, recordCalls: flags[id] } : null));

  it('records a DM when both people have opted in', async () => {
    profiles({ alice: true, bob: true });
    mockFetch.mockResolvedValue(lvsSessionCreated('arn:channel:dm-both'));
    const res = await run(buildApp(), 'POST', '/api/video/sessions', {
      body: { lobbyName: 'dm:alice:bob', findOrCreate: true },
      headers: { authorization: 'Bearer user-jwt' }, userId: 'alice',
    });
    expect(res.statusCode).toBe(201);
    expect(res.body).toMatchObject({ recording: { enabled: true, reason: 'dm-both-opted-in' } });
    expect(lastLvsBody().recordingConfiguration).toMatchObject({ enabled: true, startWhenParticipants: 2 });
    expect(mockCreateSession.mock.calls[0][0]).toMatchObject({ recording: { enabled: true, reason: 'dm-both-opted-in' } });
  });

  it('does not record a DM when one person has not opted in, and names them', async () => {
    profiles({ alice: true, bob: false });
    mockFetch.mockResolvedValue(lvsSessionCreated('arn:channel:dm-one'));
    const res = await run(buildApp(), 'POST', '/api/video/sessions', {
      body: { lobbyName: 'dm:alice:bob', findOrCreate: true },
      headers: { authorization: 'Bearer user-jwt' }, userId: 'alice',
    });
    expect(res.body).toMatchObject({ recording: { enabled: false, reason: 'dm-not-opted-in:bob' } });
    expect(lastLvsBody().recordingConfiguration.enabled).toBe(false);
  });

  // No profile row at all is the state every account starts in — that is "not opted in".
  it('treats a missing profile as not opted in', async () => {
    profiles({ alice: true });
    mockFetch.mockResolvedValue(lvsSessionCreated('arn:channel:dm-none'));
    const res = await run(buildApp(), 'POST', '/api/video/sessions', {
      body: { lobbyName: 'dm:alice:bob', findOrCreate: true },
      headers: { authorization: 'Bearer user-jwt' }, userId: 'alice',
    });
    expect(res.body).toMatchObject({ recording: { enabled: false, reason: 'dm-not-opted-in:bob' } });
  });

  // The transcribe checkbox sends enabled: true; it must not override the other person's choice.
  it('does not let an explicit enabled: true bypass DM consent', async () => {
    profiles({ alice: true });
    mockFetch.mockResolvedValue(lvsSessionCreated('arn:channel:dm-bypass'));
    await run(buildApp(), 'POST', '/api/video/sessions', {
      body: { lobbyName: 'dm:alice:bob', recordingConfiguration: { enabled: true, recordingProfile: 'hangout' } },
      headers: { authorization: 'Bearer user-jwt' }, userId: 'alice',
    });
    expect(lastLvsBody().recordingConfiguration).toMatchObject({ enabled: false, recordingProfile: 'hangout' });
  });

  // A hashed group cannot name its members from the lobby, so only the caller's own flag is checked.
  it('gates a hashed group on the caller alone', async () => {
    profiles({ alice: false });
    mockFetch.mockResolvedValue(lvsSessionCreated('arn:channel:dmg'));
    const res = await run(buildApp(), 'POST', '/api/video/sessions', {
      body: { lobbyName: 'dmg:9f2c1a4b55de7788aa11bb22', findOrCreate: true },
      headers: { authorization: 'Bearer user-jwt' }, userId: 'alice',
    });
    expect(res.body).toMatchObject({ recording: { enabled: false, reason: 'dm-not-opted-in:alice' } });
    expect(mockGetProfile).toHaveBeenCalledTimes(1);
  });

  it('keeps the reason a room sends with its setting', async () => {
    mockFetch.mockResolvedValue(lvsSessionCreated('arn:channel:room-off'));
    const res = await run(buildApp(), 'POST', '/api/video/sessions', {
      body: { lobbyName: 'room:design', findOrCreate: true, recordingConfiguration: { enabled: false, reason: 'room-setting-off', destination: 'local' } },
      headers: { authorization: 'Bearer user-jwt' }, userId: 'alice',
    });
    expect(res.body).toMatchObject({ recording: { enabled: false, reason: 'room-setting-off' } });
    expect(lastLvsBody().recordingConfiguration.enabled).toBe(false);
    expect(mockGetProfile).not.toHaveBeenCalled();
  });

  it('records a document call by default and says so', async () => {
    mockFetch.mockResolvedValue(lvsSessionCreated('arn:channel:doc'));
    const res = await run(buildApp(), 'POST', '/api/video/sessions', {
      body: { lobbyName: '6ea7c8fd-1234-4abc-8def-0123456789ab', findOrCreate: true },
      headers: { authorization: 'Bearer user-jwt' }, userId: 'alice',
    });
    expect(res.body).toMatchObject({ recording: { enabled: true, reason: 'default-on' } });
    expect(lastLvsBody().recordingConfiguration).toEqual({ enabled: true, destination: 'local', transcribe: false });
  });

  // A later joiner's own request does not change what LVS is already doing.
  it('returns the governing row\'s decision on a findOrCreate hit', async () => {
    profiles({ alice: true, bob: true });
    mockFetch.mockResolvedValue(lvsSessionReused('arn:channel:dm-reuse'));
    mockFindActiveByLobbyName.mockResolvedValueOnce({ sessionId: 'existing', channelArn: 'arn:channel:dm-reuse', recording: { enabled: false, reason: 'dm-not-opted-in:bob' } });
    const res = await run(buildApp(), 'POST', '/api/video/sessions', {
      body: { lobbyName: 'dm:alice:bob', findOrCreate: true },
      headers: { authorization: 'Bearer user-jwt' }, userId: 'bob',
    });
    expect(res.statusCode).toBe(200);
    expect(res.body).toEqual({ sessionId: 'existing', reused: true, recording: { enabled: false, reason: 'dm-not-opted-in:bob' } });
  });
});

describe('POST /api/video/captions — the toggle is a pipeline run', () => {
  beforeEach(() => { mockStartLiveCaptions.mockReset(); mockStopLiveCaptions.mockReset(); });

  it('enabled:true starts the live-captions run for the channel and returns its runId', async () => {
    mockStartLiveCaptions.mockResolvedValue({ runId: 'run-lc-1', pipelineId: 'live-captions', reused: false });
    const res = await run(buildApp(), 'POST', '/api/video/captions', { body: { channelArn: 'arn:lvs:channel/x', enabled: true }, userId: 'dev-grace' });
    expect(res.statusCode).toBe(200);
    expect(res.body).toEqual({ enabled: true, runId: 'run-lc-1', pipelineId: 'live-captions', reused: false });
    expect(mockStartLiveCaptions).toHaveBeenCalledWith({ channelArn: 'arn:lvs:channel/x', userId: 'dev-grace' });
  });

  it('enabled:false asks that run to stop and names it', async () => {
    mockStopLiveCaptions.mockResolvedValue({ runId: 'run-lc-1', stopped: true, removed: [] });
    const res = await run(buildApp(), 'POST', '/api/video/captions', { body: { channelArn: 'arn:lvs:channel/x', enabled: false } });
    expect(res.statusCode).toBe(200);
    expect(res.body).toEqual({ enabled: false, runId: 'run-lc-1', stopped: true });
    expect(mockStopLiveCaptions).toHaveBeenCalledWith({ channelArn: 'arn:lvs:channel/x' });
  });

  it('carries the direct tap ids when there was no engine to run under', async () => {
    mockStartLiveCaptions.mockResolvedValue({ runId: null, pipelineId: 'live-captions', reused: false, tapIds: ['tap-1'] });
    const res = await run(buildApp(), 'POST', '/api/video/captions', { body: { channelArn: 'arn:lvs:channel/x', enabled: true } });
    expect(res.body).toEqual({ enabled: true, runId: null, pipelineId: 'live-captions', reused: false, tapIds: ['tap-1'] });
  });

  it('400 without channelArn or a boolean enabled', async () => {
    const a = await run(buildApp(), 'POST', '/api/video/captions', { body: { enabled: true } });
    expect(a.statusCode).toBe(400);
    const b = await run(buildApp(), 'POST', '/api/video/captions', { body: { channelArn: 'arn:x', enabled: 'yes' } });
    expect(b.statusCode).toBe(400);
    expect(mockStartLiveCaptions).not.toHaveBeenCalled();
  });
});

// NFR #175 (realtime-examples Loop 29c): every lobby-addressed route asks the
// lobby check, and a body participantId must be the caller's own entry.
describe('NFR #175 — lobby access and participant binding', () => {
  const session = {
    documentId: 'dm:alice:bob', sessionId: 'vs-9', channelArn: 'arn:lvs:9', status: 'active', startedBy: 'alice',
    participants: [
      { userId: 'alice', participantId: 'p-alice' },
      { userId: 'bob', participantId: 'p-bob' },
    ],
  };
  beforeEach(() => {
    mockCanUseLobby.mockReset().mockImplementation(async (...a: unknown[]) => {
      const req = a[0] as { user?: { sub?: string } };
      return ['alice', 'bob'].includes(req.user?.sub ?? '');
    });
    mockGetSession.mockResolvedValue(session);
    mockFindByChannelArn.mockReset().mockResolvedValue([{ documentId: 'dm:alice:bob' }]);
  });
  afterAll(() => { mockCanUseLobby.mockReset().mockResolvedValue(true); });

  const auth = { authorization: 'Bearer t' };

  it('a stranger is 404 on create, join, end, refresh-token, reads and captions — nothing reaches LVS', async () => {
    const app = buildApp();
    const as = { userId: 'eve', headers: auth };
    const calls: Array<[string, string, unknown?]> = [
      ['POST', '/api/video/sessions', { lobbyName: 'dm:alice:bob' }],
      ['POST', '/api/video/sessions/vs-9/join', { lobbyName: 'dm:alice:bob' }],
      ['POST', '/api/video/sessions/vs-9/end', { lobbyName: 'dm:alice:bob', participantId: 'p-bob' }],
      ['POST', '/api/video/sessions/vs-9/refresh-token', { lobbyName: 'dm:alice:bob', participantId: 'p-bob' }],
      ['GET', '/api/video/sessions/document/dm:alice:bob'],
      ['GET', '/api/video/sessions/vs-9?lobbyName=dm:alice:bob'],
      ['GET', '/api/video/sessions/vs-9/recording?lobbyName=dm:alice:bob'],
      ['GET', `/api/video/sessions/by-lobby/${encodeURIComponent('dm:alice:bob')}`],
      ['POST', '/api/video/captions', { channelArn: 'arn:lvs:9', enabled: true }],
    ];
    for (const [method, url, body] of calls) {
      const res = await run(app, method, url, { ...as, body });
      expect([url, res.statusCode]).toEqual([url, 404]);
    }
    expect(mockFetch).not.toHaveBeenCalled();
    expect(mockStartLiveCaptions).not.toHaveBeenCalled();
  });

  it('a lobby member reads the session (200)', async () => {
    const res = await run(buildApp(), 'GET', '/api/video/sessions/vs-9?lobbyName=dm:alice:bob', { userId: 'bob', headers: auth });
    expect(res.statusCode).toBe(200);
  });

  it('join without a bearer is 401 even for a member', async () => {
    const res = await run(buildApp(), 'POST', '/api/video/sessions/vs-9/join', { userId: 'bob', body: { lobbyName: 'dm:alice:bob' } });
    expect(res.statusCode).toBe(401);
  });

  it('/end naming someone else\'s participant is 403 — and the session owner may clear it', async () => {
    const other = await run(buildApp(), 'POST', '/api/video/sessions/vs-9/end', { userId: 'bob', headers: auth, body: { lobbyName: 'dm:alice:bob', participantId: 'p-alice' } });
    expect(other.statusCode).toBe(403);
    expect(mockFetch).not.toHaveBeenCalled();

    mockFetch.mockResolvedValue(apiOk({ participantsRemaining: 1, endedAt: null }));
    mockRemoveParticipant.mockResolvedValue({ remaining: 1 });
    const own = await run(buildApp(), 'POST', '/api/video/sessions/vs-9/end', { userId: 'bob', headers: auth, body: { lobbyName: 'dm:alice:bob', participantId: 'p-bob' } });
    expect(own.statusCode).toBe(200);
    const owner = await run(buildApp(), 'POST', '/api/video/sessions/vs-9/end', { userId: 'alice', headers: auth, body: { lobbyName: 'dm:alice:bob', participantId: 'p-bob' } });
    expect(owner.statusCode).toBe(200);
  });

  it('refresh-token for someone else\'s participant is 403, even for the session owner', async () => {
    const res = await run(buildApp(), 'POST', '/api/video/sessions/vs-9/refresh-token', { userId: 'alice', headers: auth, body: { lobbyName: 'dm:alice:bob', participantId: 'p-bob' } });
    expect(res.statusCode).toBe(403);
    expect(mockFetch).not.toHaveBeenCalled();
  });

  it('/sessions/live lists only the lobbies the caller may use', async () => {
    mockListActiveCandidates.mockResolvedValue([
      { documentId: 'dm:alice:bob', sessionId: 's1', channelArn: 'arn:1', status: 'active', startedAt: new Date().toISOString(), startedBy: 'alice', participants: [{ userId: 'alice', participantId: 'p' }] },
    ]);
    mockFetch.mockResolvedValue(apiOk({ isLive: true, state: 'LIVE' }));
    const bob = await run(buildApp(), 'GET', '/api/video/sessions/live', { userId: 'bob', headers: auth });
    const eve = await run(buildApp(), 'GET', '/api/video/sessions/live', { userId: 'eve', headers: auth });
    const names = (r: MockRes) => ((r.body as { lobbies: Array<{ lobbyName: string }> }).lobbies ?? []).map((l) => l.lobbyName);
    expect(eve.statusCode).toBe(200);
    expect(names(eve)).toEqual([]);
    // Bob sees whatever the live computation produced — never less than eve.
    expect(names(bob).length).toBeGreaterThanOrEqual(names(eve).length);
  });
});

// OrgIQ (2026-09-29): with VIDEO_SERVICE_TOKEN set, platform-api calls LVS's
// session-lifecycle routes as a service and the caller's bearer stays here
// (lib/lvsServiceAuth.ts). Unset, the legacy forward above still applies.
describe('LVS lifecycle calls with VIDEO_SERVICE_TOKEN (no user bearer reaches the SFU)', () => {
  const USER = 'Bearer user-jwt';
  const SERVICE = 'svc-token-123';
  const prior = process.env.VIDEO_SERVICE_TOKEN;
  beforeEach(() => { process.env.VIDEO_SERVICE_TOKEN = SERVICE; });
  afterAll(() => {
    if (prior === undefined) delete process.env.VIDEO_SERVICE_TOKEN;
    else process.env.VIDEO_SERVICE_TOKEN = prior;
  });

  function authOfEveryFetch(): string[] {
    return mockFetch.mock.calls.map(([, init]) => String(((init as RequestInit).headers as Record<string, string>).Authorization));
  }

  it('create: LVS gets the service token, creatorId from req.user', async () => {
    mockFetch.mockResolvedValue(lvsSessionCreated('arn:channel:svc'));
    const res = await run(buildApp(), 'POST', '/api/video/sessions', {
      body: { lobbyName: 'orgiq:dm:u-nora:u-vera', findOrCreate: true },
      headers: { authorization: USER },
      userId: 'u-vera',
    });
    expect(res.statusCode).toBe(201);
    expect(authOfEveryFetch()).toEqual([`Bearer ${SERVICE}`]);
    const sent = JSON.parse((mockFetch.mock.calls[0][1] as RequestInit).body as string);
    expect(sent.creatorId).toBe('u-vera');
    // A tenant-prefixed DM is a conversation call: LVS waits for the second party.
    expect(sent.recordingConfiguration).toMatchObject({ startWhenParticipants: 2 });
  });

  it('create works with no caller bearer at all (the service token carries the call)', async () => {
    mockFetch.mockResolvedValue(lvsSessionCreated('arn:channel:svc2'));
    const res = await run(buildApp(), 'POST', '/api/video/sessions', { body: { lobbyName: 'room-x' } });
    expect(res.statusCode).toBe(201);
    expect(authOfEveryFetch()).toEqual([`Bearer ${SERVICE}`]);
  });

  it('join, refresh-token and end: every LVS call carries the service token, none the user bearer', async () => {
    mockGetSession.mockResolvedValue({
      sessionId: 'vs-1', channelArn: 'arn:ch:svc', streamKey: '', status: 'active',
      participants: [{ participantId: 'p-1', userId: 'tester' }, { participantId: 'p-2', userId: 'other' }],
    });
    mockFetch.mockResolvedValue(apiOk({ token: 't', participantId: 'p-1', participantsRemaining: 1, endedAt: null }));
    mockRemoveParticipant.mockResolvedValue({ remaining: 1 });
    const body = { lobbyName: 'orgiq:dm:other:tester', participantId: 'p-1', displayName: 'T' };
    for (const path of ['join', 'refresh-token', 'end']) {
      const res = await run(buildApp(), 'POST', `/api/video/sessions/vs-1/${path}`, { body, headers: { authorization: USER } });
      expect([200, 201]).toContain(res.statusCode);
    }
    const auths = authOfEveryFetch();
    expect(auths.length).toBeGreaterThanOrEqual(4); // join, refresh, reap DELETE, end
    expect(auths).not.toContain(USER);
    const lifecycle = mockFetch.mock.calls
      .filter(([url, init]) => (init as RequestInit).method === 'POST' && /\/api\/sessions\//.test(String(url)))
      .map(([, init]) => ((init as RequestInit).headers as Record<string, string>).Authorization);
    expect(lifecycle.length).toBe(3);
    expect(new Set(lifecycle)).toEqual(new Set([`Bearer ${SERVICE}`]));
  });
});

// The call's partition (lib/partition.ts): a tenant-prefixed lobby the caller
// may use — any tenant in its token's `tenants` — lives in that tenant.
describe('POST /api/video/sessions — the session row carries the lobby partition', () => {
  it('a social:dm lobby, for a token listing social, is stamped orgId social', async () => {
    mockFetch.mockResolvedValue(lvsSessionCreated('arn:channel:social'));
    const res = await run(buildApp(), 'POST', '/api/video/sessions', {
      body: { lobbyName: 'social:dm:bob:tester' },
      headers: { authorization: 'Bearer user-jwt' },
      user: { sub: 'tester', orgId: 'org-a', tenants: ['org-a', 'social'], authKind: 'orgiq-hs256' },
    });
    expect(res.statusCode).toBe(201);
    expect(mockCreateSession.mock.calls[0][0]).toMatchObject({ documentId: 'social:dm:bob:tester', orgId: 'social' });
  });

  it('a prefix the caller may not use never becomes the row org — the caller org stays', async () => {
    mockFetch.mockResolvedValue(lvsSessionCreated('arn:channel:adhoc'));
    await run(buildApp(), 'POST', '/api/video/sessions', {
      body: { lobbyName: 'org-b:standup' },
      headers: { authorization: 'Bearer user-jwt' },
      user: { sub: 'tester', orgId: 'org-a' },
    });
    expect(mockCreateSession.mock.calls[0][0]).toMatchObject({ orgId: 'org-a' });
  });
});

// A private room admits members only, whether the call is reached through the
// rooms route or straight at the video API: the real lobby rule, here.
describe('POST /api/video/sessions — a private room lobby admits members only', () => {
  const tok = (sub: string) => ({ sub, orgId: 'org-a', tenants: ['org-a', 'social'], authKind: 'orgiq-hs256' });
  beforeEach(() => {
    const { makeLobbyAccess } = jest.requireActual('../../lib/lobbyAccess') as typeof import('../../lib/lobbyAccess');
    mockHangoutBySlug.mockImplementation(async (orgId, slug) => ({ orgId, slug, roomId: 'r-' + slug, state: 'active', settings: { audioOnly: true, recordingEnabled: false, softCap: 25 } }));
    mockRealLobbyAccess = makeLobbyAccess({
      roomBySlug: async (orgId, slug) => (orgId === 'social' && slug === 'den'
        ? { roomId: 'r-den', orgId: 'social', visibility: 'private', createdBy: 'u-owner' }
        : orgId === 'social' && slug === 'lounge' ? { roomId: 'r-lounge', orgId: 'social', visibility: 'org' } : null),
      isRoomMember: async (roomId, userId) => roomId === 'r-den' && userId === 'u-member',
      isChannelMember: async () => false,
      documentExists: async () => false,
      assertCanReadDocument: async () => undefined,
    }) as never;
  });
  afterEach(() => { mockRealLobbyAccess = null; });

  it('a listed-tenant non-member is refused 403 before any LVS call', async () => {
    const res = await run(buildApp(), 'POST', '/api/video/sessions', {
      body: { lobbyName: 'social:room:den', findOrCreate: true },
      headers: { authorization: 'Bearer user-jwt' },
      user: tok('u-stranger'),
    });
    expect(res.statusCode).toBe(403);
    expect(mockFetch).not.toHaveBeenCalled();
    expect(mockCreateSession).not.toHaveBeenCalled();
  });

  it('recorded hangouts select transcription even when the client asks for a broadcast profile', async () => {
    mockHangoutBySlug.mockImplementation(async (orgId, slug) => ({ orgId, slug, roomId: 'r-' + slug, state: 'active', settings: { audioOnly: false, recordingEnabled: true, softCap: 25 } }));
    mockFetch.mockResolvedValue(lvsSessionCreated('arn:channel:lounge'));
    const response = await run(buildApp(), 'POST', '/api/video/sessions', {
      body: { lobbyName: 'social:room:lounge', findOrCreate: true, recordingConfiguration: { enabled: false, recordingProfile: 'broadcast' } },
      headers: { authorization: 'Bearer user-jwt' }, user: tok('u-member'),
    });
    expect(response.statusCode).toBe(201);
    expect(JSON.parse(mockFetch.mock.calls.at(-1)[1].body).recordingConfiguration).toMatchObject({ enabled: true, recordingProfile: 'hangout', transcribe: true });
  });

  it('a member opens the call; a public room opens for any listed tenant', async () => {
    mockFetch.mockResolvedValue(lvsSessionCreated('arn:channel:den'));
    const member = await run(buildApp(), 'POST', '/api/video/sessions', {
      body: { lobbyName: 'social:room:den', findOrCreate: true },
      headers: { authorization: 'Bearer user-jwt' },
      user: tok('u-member'),
    });
    expect(member.statusCode).toBe(201);
    expect(mockCreateSession.mock.calls.at(-1)[0]).toMatchObject({ recording: { enabled: false, reason: 'room-setting-off' }, hangout: { roomId: 'r-den', audioOnly: true } });
    const open = await run(buildApp(), 'POST', '/api/video/sessions', {
      body: { lobbyName: 'social:room:lounge', findOrCreate: true },
      headers: { authorization: 'Bearer user-jwt' },
      user: tok('u-stranger'),
    });
    expect(open.statusCode).toBe(201);
  });
});
