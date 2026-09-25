// In-document calls (realtime-examples docs/design/document-calls SPEC §5.2):
// the document-review fields on POST /sessions, the new PATCH, and the
// participantCount / review-document rows on the GETs. Scaffold copied from
// videoSessions.test.ts so that file stays untouched.

import express, { type NextFunction, type Request, type Response } from 'express';

const mockCreateSession = jest.fn();
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
const mockFindByChannelArn = jest.fn().mockResolvedValue([{ documentId: 'room:standup' }]);
// Profiles decide whether a DM records: both parties must have recordCalls on.
const mockGetProfile = jest.fn().mockResolvedValue(null);
const mockUpdateDocumentCallFields = jest.fn();
const mockFindActiveByReviewDocument = jest.fn().mockResolvedValue([]);

// NFR #175: lobby access is its own unit (lib/__tests__/lobbyAccess.test.ts);
// here it allows unless a test says otherwise.
const mockCanUseLobby = jest.fn(async (..._a: unknown[]) => true);
jest.mock('../../lib/lobbyAccess', () => ({
  lobbyAccess: () => ({
    canUseLobby: (...a: unknown[]) => mockCanUseLobby(...a),
    assertLobbyAccess: async (...a: unknown[]) => {
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
    updateDocumentCallFields: (...args: unknown[]) => mockUpdateDocumentCallFields(...args),
    findActiveByReviewDocument: (...args: unknown[]) => mockFindActiveByReviewDocument(...args),
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
  mockUpdateDocumentCallFields.mockReset();
  mockFindActiveByReviewDocument.mockReset().mockResolvedValue([]);
  mockCanUseLobby.mockReset().mockResolvedValue(true);
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



const AUTH = { authorization: 'Bearer t' };

function reviewRow(over: Record<string, unknown> = {}) {
  return {
    documentId: 'doc-auth', sessionId: 'sess-1', channelArn: 'arn:channel:1', streamKey: '',
    status: 'active', startedAt: '2026-09-24T10:00:00Z', startedBy: 'connor',
    kind: 'document-review', title: 'Auth architecture review', documentIds: ['doc-auth', 'doc-brief'],
    media: 'video', hostUserId: 'connor',
    participants: [
      { userId: 'connor', participantId: 'p1', displayName: 'Connor', joinedAt: 'x' },
      { userId: 'alice', participantId: 'p2', displayName: 'Alice', joinedAt: 'x' },
      { userId: 'alice', participantId: 'p3', clientId: 'tab-2', displayName: 'Alice', joinedAt: 'x' },
      { userId: 'frank', participantId: 'p4', displayName: 'Frank', joinedAt: 'x' },
    ],
    ...over,
  };
}

describe('POST /api/video/sessions — document review fields', () => {
  it('stores kind, title, documentIds (host first, deduped), media and host, and returns them', async () => {
    mockFetch.mockResolvedValueOnce(lvsSessionCreated());
    const res = await run(buildApp(), 'POST', '/api/video/sessions', {
      userId: 'connor', headers: AUTH,
      body: {
        lobbyName: 'doc-auth', findOrCreate: true, kind: 'document-review', title: '  Auth architecture review ',
        documentIds: ['doc-brief', 'doc-auth', 'doc-brief'], media: 'audio',
      },
    });
    expect(res.statusCode).toBe(201);
    const stored = mockCreateSession.mock.calls[0][0];
    expect(stored).toMatchObject({
      documentId: 'doc-auth', kind: 'document-review', title: 'Auth architecture review',
      documentIds: ['doc-auth', 'doc-brief'], media: 'audio', hostUserId: 'connor',
    });
    expect(res.body).toMatchObject({ kind: 'document-review', title: 'Auth architecture review', documentIds: ['doc-auth', 'doc-brief'], media: 'audio' });
  });

  it('leaves non-review calls exactly as before (no new keys on the row or the reply)', async () => {
    mockFetch.mockResolvedValueOnce(lvsSessionCreated());
    const res = await run(buildApp(), 'POST', '/api/video/sessions', { headers: AUTH, body: { lobbyName: 'dm:a-b' } });
    expect(res.statusCode).toBe(201);
    const stored = mockCreateSession.mock.calls[0][0];
    for (const k of ['kind', 'title', 'documentIds', 'media', 'hostUserId']) expect(stored).not.toHaveProperty(k);
    expect(Object.keys(res.body as object).sort()).toEqual(['recording', 'reused', 'sessionId']);
  });

  it('rejects an unknown kind and a bad media value with 400 before touching the SFU', async () => {
    let res = await run(buildApp(), 'POST', '/api/video/sessions', { headers: AUTH, body: { lobbyName: 'd', kind: 'party' } });
    expect(res.statusCode).toBe(400);
    res = await run(buildApp(), 'POST', '/api/video/sessions', { headers: AUTH, body: { lobbyName: 'd', kind: 'document-review', media: 'hologram' } });
    expect(res.statusCode).toBe(400);
    expect(mockFetch).not.toHaveBeenCalled();
  });

  it('findOrCreate hit on a plain document call adopts the review fields, host stays the starter', async () => {
    mockFetch.mockResolvedValueOnce(lvsSessionReused());
    const plain = { ...reviewRow(), kind: undefined, title: undefined, documentIds: undefined, media: undefined, hostUserId: undefined };
    mockFindActiveByLobbyName.mockResolvedValueOnce(plain);
    mockUpdateDocumentCallFields.mockImplementation(async (_d: string, _s: string, patch: Record<string, unknown>) => ({ ...plain, ...patch }));
    const res = await run(buildApp(), 'POST', '/api/video/sessions', {
      userId: 'alice', headers: AUTH,
      body: { lobbyName: 'doc-auth', findOrCreate: true, kind: 'document-review', title: 'Review' },
    });
    expect(res.statusCode).toBe(200);
    expect(mockUpdateDocumentCallFields).toHaveBeenCalledWith('doc-auth', 'sess-1', expect.objectContaining({ kind: 'document-review', title: 'Review', hostUserId: 'connor' }));
    expect(res.body).toMatchObject({ reused: true, kind: 'document-review', title: 'Review', hostUserId: 'connor' });
  });

  it('findOrCreate hit on an existing review keeps its own fields', async () => {
    mockFetch.mockResolvedValueOnce(lvsSessionReused());
    mockFindActiveByLobbyName.mockResolvedValueOnce(reviewRow());
    const res = await run(buildApp(), 'POST', '/api/video/sessions', {
      userId: 'alice', headers: AUTH,
      body: { lobbyName: 'doc-auth', findOrCreate: true, kind: 'document-review', title: 'Other title' },
    });
    expect(mockUpdateDocumentCallFields).not.toHaveBeenCalled();
    expect(res.body).toMatchObject({ title: 'Auth architecture review', hostUserId: 'connor' });
  });
});

describe('POST /api/video/sessions — document calls do not record by default', () => {
  function lvsBody() { return JSON.parse(mockFetch.mock.calls[0][1].body); }

  it('a document review with no recording request is created not recording, and LVS is told so', async () => {
    mockFetch.mockResolvedValueOnce(lvsSessionCreated());
    const res = await run(buildApp(), 'POST', '/api/video/sessions', {
      userId: 'connor', headers: AUTH, body: { lobbyName: 'doc-auth', findOrCreate: true, kind: 'document-review', title: 'Review' },
    });
    expect(res.statusCode).toBe(201);
    expect((res.body as any).recording).toEqual({ enabled: false, reason: 'document-call-not-recorded' });
    expect(lvsBody().recordingConfiguration.enabled).toBe(false);
    expect(mockCreateSession.mock.calls[0][0].recording).toEqual({ enabled: false, reason: 'document-call-not-recorded' });
    expect(mockGetProfile).not.toHaveBeenCalled();
  });

  it('a recording config without enabled:true still does not record', async () => {
    mockFetch.mockResolvedValueOnce(lvsSessionCreated());
    await run(buildApp(), 'POST', '/api/video/sessions', {
      headers: AUTH, body: { lobbyName: 'doc-auth', kind: 'document-review', recordingConfiguration: { recordingProfile: 'hangout' } },
    });
    expect(lvsBody().recordingConfiguration.enabled).toBe(false);
  });

  it('records when someone explicitly asks', async () => {
    mockFetch.mockResolvedValueOnce(lvsSessionCreated());
    const res = await run(buildApp(), 'POST', '/api/video/sessions', {
      headers: AUTH, body: { lobbyName: 'doc-auth', kind: 'document-review', recordingConfiguration: { enabled: true, recordingProfile: 'hangout' } },
    });
    expect((res.body as any).recording.enabled).toBe(true);
    expect(lvsBody().recordingConfiguration.enabled).toBe(true);
  });

  it('leaves every other call on its old default', async () => {
    mockFetch.mockResolvedValueOnce(lvsSessionCreated());
    const res = await run(buildApp(), 'POST', '/api/video/sessions', { headers: AUTH, body: { lobbyName: 'room:standup' } });
    expect((res.body as any).recording).toEqual({ enabled: true, reason: 'default-on' });
  });
});

describe('PATCH /api/video/sessions/:sessionId', () => {
  beforeEach(() => {
    mockGetSession.mockResolvedValue(reviewRow());
    mockUpdateDocumentCallFields.mockImplementation(async (_d: string, _s: string, patch: Record<string, unknown>) => ({ ...reviewRow(), ...patch }));
  });

  it('lets a participant change the review list; host document stays first', async () => {
    const res = await run(buildApp(), 'PATCH', '/api/video/sessions/sess-1', {
      userId: 'alice', body: { lobbyName: 'doc-auth', documentIds: ['doc-brief', 'doc-plan'] },
    });
    expect(res.statusCode).toBe(200);
    expect(mockUpdateDocumentCallFields).toHaveBeenCalledWith('doc-auth', 'sess-1', { documentIds: ['doc-auth', 'doc-brief', 'doc-plan'] });
    expect(res.body).toMatchObject({ documentIds: ['doc-auth', 'doc-brief', 'doc-plan'], participantCount: 3 });
  });

  it('403s someone who is not in the call', async () => {
    const res = await run(buildApp(), 'PATCH', '/api/video/sessions/sess-1', {
      userId: 'mallory', body: { lobbyName: 'doc-auth', documentIds: ['x'] },
    });
    expect(res.statusCode).toBe(403);
    expect(mockUpdateDocumentCallFields).not.toHaveBeenCalled();
  });

  it('lets only the host rename', async () => {
    let res = await run(buildApp(), 'PATCH', '/api/video/sessions/sess-1', { userId: 'alice', body: { lobbyName: 'doc-auth', title: 'Mine now' } });
    expect(res.statusCode).toBe(403);
    res = await run(buildApp(), 'PATCH', '/api/video/sessions/sess-1', { userId: 'connor', body: { lobbyName: 'doc-auth', title: 'Renamed' } });
    expect(res.statusCode).toBe(200);
    expect(res.body).toMatchObject({ title: 'Renamed' });
  });

  it('400s an empty patch or a missing lobby, 404s an unknown session or a lobby the caller cannot use, 409s an ended call', async () => {
    expect((await run(buildApp(), 'PATCH', '/api/video/sessions/sess-1', { userId: 'connor', body: { lobbyName: 'doc-auth' } })).statusCode).toBe(400);
    expect((await run(buildApp(), 'PATCH', '/api/video/sessions/sess-1', { userId: 'connor', body: { title: 'x' } })).statusCode).toBe(400);
    mockGetSession.mockResolvedValueOnce(null);
    expect((await run(buildApp(), 'PATCH', '/api/video/sessions/sess-1', { userId: 'connor', body: { lobbyName: 'doc-auth', title: 'x' } })).statusCode).toBe(404);
    mockCanUseLobby.mockResolvedValueOnce(false);
    expect((await run(buildApp(), 'PATCH', '/api/video/sessions/sess-1', { userId: 'connor', body: { lobbyName: 'doc-auth', title: 'x' } })).statusCode).toBe(404);
    mockGetSession.mockResolvedValueOnce(reviewRow({ status: 'ended' }));
    expect((await run(buildApp(), 'PATCH', '/api/video/sessions/sess-1', { userId: 'connor', body: { lobbyName: 'doc-auth', title: 'x' } })).statusCode).toBe(409);
  });

  it('turns a plain document call into a review on first PATCH', async () => {
    mockGetSession.mockResolvedValueOnce(reviewRow({ kind: undefined, hostUserId: undefined }));
    await run(buildApp(), 'PATCH', '/api/video/sessions/sess-1', { userId: 'alice', body: { lobbyName: 'doc-auth', documentIds: ['doc-brief'] } });
    expect(mockUpdateDocumentCallFields).toHaveBeenCalledWith('doc-auth', 'sess-1', {
      documentIds: ['doc-auth', 'doc-brief'], kind: 'document-review', hostUserId: 'connor',
    });
  });
});

describe('GET routes — participantCount and review-document rows', () => {
  it('GET /sessions/:id adds participantCount (distinct users, 0 once ended)', async () => {
    mockGetSession.mockResolvedValueOnce(reviewRow());
    let res = await run(buildApp(), 'GET', '/api/video/sessions/sess-1?lobbyName=doc-auth');
    expect(res.body).toMatchObject({ kind: 'document-review', title: 'Auth architecture review', participantCount: 3, hostUserId: 'connor' });
    mockGetSession.mockResolvedValueOnce(reviewRow({ status: 'ended' }));
    res = await run(buildApp(), 'GET', '/api/video/sessions/sess-1?lobbyName=doc-auth');
    expect(res.body).toMatchObject({ participantCount: 0 });
  });

  it('GET /sessions/document/:id also lists a review hosted elsewhere that includes this document', async () => {
    mockGetSessionsByDocument.mockResolvedValueOnce([]);
    mockFindActiveByReviewDocument.mockResolvedValueOnce([reviewRow()]);
    const res = await run(buildApp(), 'GET', '/api/video/sessions/document/doc-brief');
    const sessions = (res.body as { sessions: Array<Record<string, unknown>> }).sessions;
    expect(sessions).toHaveLength(1);
    expect(sessions[0]).toMatchObject({ documentId: 'doc-auth', title: 'Auth architecture review', participantCount: 3 });
    expect(mockFindActiveByReviewDocument).toHaveBeenCalledWith('doc-brief');
  });

  it('does not duplicate the host document own row, and hides reviews whose host the caller cannot use', async () => {
    mockGetSessionsByDocument.mockResolvedValueOnce([reviewRow()]);
    mockFindActiveByReviewDocument.mockResolvedValueOnce([reviewRow(), reviewRow({ documentId: 'doc-secret', sessionId: 's2' })]);
    mockCanUseLobby.mockImplementation(async (_req: unknown, lobby: unknown) => lobby !== 'doc-secret');
    const res = await run(buildApp(), 'GET', '/api/video/sessions/document/doc-auth');
    const sessions = (res.body as { sessions: Array<Record<string, unknown>> }).sessions;
    expect(sessions.map((s) => s.sessionId)).toEqual(['sess-1']);
  });

  it('still answers with the own rows when the review scan fails', async () => {
    mockGetSessionsByDocument.mockResolvedValueOnce([reviewRow()]);
    mockFindActiveByReviewDocument.mockRejectedValueOnce(new Error('scan down'));
    const res = await run(buildApp(), 'GET', '/api/video/sessions/document/doc-auth');
    expect(res.statusCode).toBe(200);
    expect((res.body as { sessions: unknown[] }).sessions).toHaveLength(1);
  });
});
