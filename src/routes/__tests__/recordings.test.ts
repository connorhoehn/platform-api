// platform-api/src/routes/__tests__/recordings.test.ts
//
// Route-level coverage for src/routes/recordings.ts. Audit (hub#1283)
// flagged this as one of two route files with no test — silent regressions
// in the breaker-error mapping or the playback-token mint flow would
// otherwise land with zero alarm.
//
// Mocks `callSfu` (not the underlying CircuitBreaker) to match the pattern
// used in videoSessions.test.ts.

import express, {
  type NextFunction,
  type Request,
  type Response,
} from 'express';

// --- mocks ------------------------------------------------------------------

const mockListByUser = jest.fn();
const mockGetRecording = jest.fn();
const mockFindByRecordingId = jest.fn();
const mockHasParticipantRow = jest.fn();
const mockUpdateComposeState = jest.fn();
const mockGetSessionsByDocument = jest.fn();
const mockDocClientSend = jest.fn();
const mockCallSfu = jest.fn();
const mockMapSfuError = jest.fn();

jest.mock('../../middleware/auth', () => ({
  requireAuth: (req: Request, _res: Response, next: NextFunction) => {
    (req as unknown as { user: { sub: string } }).user = { sub: 'u-1' };
    next();
  },
}));

jest.mock('../../repositories', () => {
  // `describeMedia` is a pure function over a row — mocking it would mean
  // the tests below assert against a fake notion of what is playable, which
  // is the one thing they exist to check. Take the real one.
  const { describeMedia } = jest.requireActual('../../repositories/CallRecordingRepository');
  return {
    describeMedia,
    callRecordingRepo: {
      listByUser: (...args: unknown[]) => mockListByUser(...args),
      getRecording: (...args: unknown[]) => mockGetRecording(...args),
      findByRecordingId: (...args: unknown[]) => mockFindByRecordingId(...args),
      hasParticipantRow: (...args: unknown[]) => mockHasParticipantRow(...args),
      updateComposeState: (...args: unknown[]) => mockUpdateComposeState(...args),
      // The real implementation, so a route that reaches for the point
      // lookup exercises the same two calls production would make.
      async resolveForUser(userId: string, recordingId: string) {
        const detail = await mockFindByRecordingId(recordingId);
        if (!detail) return null;
        const isParticipant = await mockHasParticipantRow(
          userId, detail.startedAt, recordingId,
        );
        return { detail, isParticipant };
      },
      listByLobbyName: jest.fn(),
      deleteRecording: jest.fn(),
    },
    videoSessionRepo: {
      getSessionsByDocument: (...args: unknown[]) => mockGetSessionsByDocument(...args),
    },
  };
});

jest.mock('../../lib/aws-clients', () => ({
  docClient: { send: (...args: unknown[]) => mockDocClientSend(...args) },
}));

jest.mock('../../lib/ddb-table-name', () => ({
  tableName: (n: string) => `test-${n}`,
}));

jest.mock('../../lib/video-auth', () => ({
  VIDEO_API_URL: 'https://video.test',
  VIDEO_PUBLIC_URL: 'https://public.video.test',
}));

jest.mock('../../lib/sfu-breaker', () => ({
  callSfu: (...args: unknown[]) => mockCallSfu(...args),
  mapSfuError: (e: unknown) => mockMapSfuError(e),
}));

// Lazy import after mocks.
import { recordingsRouter } from '../recordings';
import { errorHandler, AppError } from '../../middleware/error-handler';

// --- helpers ----------------------------------------------------------------

function buildApp(): express.Express {
  const app = express();
  app.use(express.json());
  app.use('/api', recordingsRouter);
  app.use(errorHandler);
  return app;
}

interface MockRes {
  statusCode: number;
  body: unknown;
  ended: boolean;
  finished: Promise<void>;
  /** Lower-cased header name → value, as express would send them. */
  headers: Record<string, string>;
  contentType: string | null;
  status(c: number): MockRes;
  json(b: unknown): MockRes;
  send(b?: unknown): MockRes;
  /** `res.type(...)` — the /summary route ends every path through it. */
  type(t: string): MockRes;
  setHeader(name: string, value: unknown): MockRes;
  getHeader(name: string): unknown;
  end(...args: unknown[]): MockRes;
}

function mockRes(): MockRes {
  let resolve!: () => void;
  const finished = new Promise<void>((r) => { resolve = r; });
  const r: MockRes = {
    statusCode: 200, body: undefined, ended: false, finished,
    // Headers used to be dropped on the floor here, which was fine while
    // nothing but the body carried meaning. `X-Summary-Status` is the one
    // place the summary's own state can live on a markdown response, so it
    // has to survive to be asserted on.
    headers: {}, contentType: null,
    status(c) { this.statusCode = c; return this; },
    json(b) { this.body = b; this.ended = true; resolve(); return this; },
    send(b) { this.body = b; this.ended = true; resolve(); return this; },
    type(t) { this.contentType = t; return this; },
    setHeader(name, value) { this.headers[String(name).toLowerCase()] = String(value); return this; },
    getHeader(name) { return this.headers[String(name).toLowerCase()]; },
    end() { this.ended = true; resolve(); return this; },
  };
  return r;
}

async function run(method: string, url: string, body?: unknown): Promise<MockRes> {
  const app = buildApp();
  const res = mockRes();
  const req = {
    method,
    url,
    originalUrl: url,
    path: url.split('?')[0],
    headers: { 'content-type': 'application/json' },
    query: {},
    body: body ?? {},
    params: {},
    ip: '127.0.0.1',
  } as unknown as Request;
  await new Promise<void>((resolve, reject) => {
    const finalHandler = (err?: unknown): void => {
      if (err) return reject(err);
      if (!res.ended) res.status(404).json({ error: 'not found' });
      resolve();
    };
    (app as unknown as (req: Request, res: Response, next: NextFunction) => void)(
      req, res as unknown as Response, finalHandler,
    );
    res.finished.then(() => resolve()).catch(reject);
  });
  return res;
}

function lvsOk(data: Record<string, unknown>) {
  return { ok: true, status: 200, json: async () => data } as unknown as globalThis.Response;
}

function lvsFail(status: number) {
  return { ok: false, status, json: async () => ({}) } as unknown as globalThis.Response;
}

// --- tests ------------------------------------------------------------------

/**
 * Point the route at one recording, with the caller in it (or not).
 *
 * Mirrors the two point-reads `resolveForUser` performs: the GSI query for
 * the recording, and the fan-out point-get for membership. Note there is no
 * page size anywhere in this helper — that is the fix.
 */
function givenRecording(detail: Record<string, unknown>, isParticipant = true): void {
  mockFindByRecordingId.mockImplementation(async (id: string) =>
    id === detail.recordingId ? detail : null);
  mockHasParticipantRow.mockResolvedValue(isParticipant);
}

beforeEach(() => {
  mockListByUser.mockReset();
  mockGetRecording.mockReset();
  mockFindByRecordingId.mockReset().mockResolvedValue(null);
  mockHasParticipantRow.mockReset().mockResolvedValue(false);
  mockUpdateComposeState.mockReset().mockResolvedValue(undefined);
  mockGetSessionsByDocument.mockReset();
  mockDocClientSend.mockReset();
  // Default: LVS is not answering. Every route must still produce a
  // coherent answer from the stored row rather than a guessed URL.
  mockCallSfu.mockReset().mockResolvedValue(lvsFail(503));
  mockMapSfuError.mockReset().mockImplementation((e) => {
    if (e?.name === 'BreakerOpenError') return new AppError(503, 'breaker open');
    if (e?.name === 'TimeoutError') return new AppError(504, 'timeout');
    return new AppError(502, 'unreachable');
  });
});

describe('GET /api/recordings/:recordingId', () => {
  const base = {
    recordingId: 'r-1',
    callId: 'c-1',
    channelArn: 'arn:1',
    startedAt: '2026-08-26T00:00:00Z',
    endedAt: '2026-08-26T00:01:00Z',
    composeStatus: 'skipped',
  };

  it('returns the recording when the caller participated', async () => {
    givenRecording(base);
    const res = await run('GET', '/api/recordings/r-1');
    expect(res.statusCode).toBe(200);
    expect((res.body as { recordingId: string }).recordingId).toBe('r-1');
  });

  it('404 when the caller has no participant row', async () => {
    givenRecording(base, false);
    const res = await run('GET', '/api/recordings/r-1');
    // 404 and not 403 — see the recording:read policy. Confirming that a
    // recording with this id exists is itself the disclosure.
    expect(res.statusCode).toBe(404);
  });

  it('404 when no such recording exists', async () => {
    const res = await run('GET', '/api/recordings/r-x');
    expect(res.statusCode).toBe(404);
  });

  // -------------------------------------------------------------------
  // THE DEEP-LINK REGRESSION GUARD.
  //
  // The bug was not "the wrong row came back" — it was that the lookup
  // only ever looked at a PAGE of the caller's recordings, so a recording
  // past their 100th 404'd while sitting in the table. Asserting on the
  // returned row cannot catch that; asserting that no bounded list is
  // consulted at all can. If someone reintroduces `listByUser({limit})`
  // here, this fails.
  // -------------------------------------------------------------------
  it('resolves by id alone and never consults a bounded list of the user\'s recordings', async () => {
    givenRecording(base);
    const res = await run('GET', '/api/recordings/r-1');
    expect(res.statusCode).toBe(200);
    expect(mockListByUser).not.toHaveBeenCalled();
    expect(mockFindByRecordingId).toHaveBeenCalledWith('r-1');
    // Membership is a point-get on the exact fan-out key, which is only
    // constructible once startedAt is known from the primary row.
    expect(mockHasParticipantRow).toHaveBeenCalledWith('u-1', base.startedAt, 'r-1');
  });
});

describe('POST /api/recordings/:recordingId/playback-token', () => {
  const matchDetail = {
    recordingId: 'r-1',
    callId: 'c-1',
    channelArn: 'arn:1',
    lobbyName: 'doc-1',
    startedAt: '2026-05-30T00:00:00Z',
    endedAt: '2026-05-30T00:30:00Z',
    // Settled compose with a real file — so `resolveMedia` short-circuits
    // and the only LVS call these cases make is the token mint.
    composeStatus: 'ok',
    composedSegment: { key: 'arn_1/r-1/composed.mp4' },
  };

  it('returns playlistUrl + token on 200 from LVS (happy path)', async () => {
    givenRecording(matchDetail);
    mockCallSfu.mockResolvedValue(lvsOk({ token: 'tok', expiresAt: '2026-05-30T01:00:00Z' }));

    const prevAdmin = process.env.VIDEO_ADMIN_TOKEN;
    process.env.VIDEO_ADMIN_TOKEN = 'admin-secret';
    let res;
    try {
      res = await run('POST', '/api/recordings/r-1/playback-token');
    } finally {
      if (prevAdmin === undefined) delete process.env.VIDEO_ADMIN_TOKEN;
      else process.env.VIDEO_ADMIN_TOKEN = prevAdmin;
    }
    expect(res.statusCode).toBe(200);
    const body = res.body as { playlistUrl: string; token: string; expiresAt: string };
    expect(body.token).toBe('tok');
    expect(body.expiresAt).toBe('2026-05-30T01:00:00Z');
    expect(body.playlistUrl).toContain('https://public.video.test/api/channels/');
    // Direct-MP4 playback (composed-url flow): playlistUrl points at the LVS
    // segments route with no token query param — the token rides separately
    // in body.token for private-channel callers.
    expect(body.playlistUrl).toContain('/recordings/r-1/segments/');
    expect((res.body as { composedUrl: string }).composedUrl).toBe(body.playlistUrl);

    // Post-Wave-7 the DDB streamKey is always '' (LVS no longer returns
    // one on POST /api/sessions), so this route uses VIDEO_ADMIN_TOKEN
    // as the LVS bearer. LVS bearerAuth treats it as a role=admin
    // wildcard and accepts it on /playback-tokens.
    const [, init] = mockCallSfu.mock.calls[0];
    expect((init as RequestInit).headers).toMatchObject({ Authorization: 'Bearer admin-secret' });
  });

  it('returns empty token when LVS responds 400 (public channel, playback open)', async () => {
    givenRecording(matchDetail);
    mockGetSessionsByDocument.mockResolvedValue([]);
    mockCallSfu.mockResolvedValue(lvsFail(400));

    const res = await run('POST', '/api/recordings/r-1/playback-token');
    expect(res.statusCode).toBe(200);
    const body = res.body as { token: string | null; expiresAt: string | null };
    expect(body.token).toBeNull();
    expect(body.expiresAt).toBeNull();
  });

  it('503 when the SFU breaker is open (BreakerOpenError → mapSfuError)', async () => {
    givenRecording(matchDetail);
    mockGetSessionsByDocument.mockResolvedValue([{ channelArn: 'arn:1', streamKey: 'sk-1' }]);
    const breakerOpen = new Error('breaker');
    breakerOpen.name = 'BreakerOpenError';
    mockCallSfu.mockRejectedValue(breakerOpen);

    const res = await run('POST', '/api/recordings/r-1/playback-token');
    expect(res.statusCode).toBe(503);
  });

  it('502 when LVS returns non-400 4xx/5xx (AppError thrown inside try)', async () => {
    givenRecording(matchDetail);
    mockGetSessionsByDocument.mockResolvedValue([{ channelArn: 'arn:1', streamKey: 'sk-1' }]);
    mockCallSfu.mockResolvedValue(lvsFail(500));

    const res = await run('POST', '/api/recordings/r-1/playback-token');
    expect(res.statusCode).toBe(502);
  });

  it('404 when caller did not participate', async () => {
    givenRecording(matchDetail, false);
    const res = await run('POST', '/api/recordings/r-1/playback-token');
    expect(res.statusCode).toBe(404);
    expect(mockCallSfu).not.toHaveBeenCalled();
  });
});

// ---------------------------------------------------------------------------
// "Playback error." was one message covering three unrelated situations,
// because the route ended its file-picking chain with `?? 'composed.mp4'`
// and let the browser 404 discover the truth. Only 11 of 941 recordings
// actually have composed media; 220 are still queued and 125 were skipped.
// These are the three cases, kept apart.
// ---------------------------------------------------------------------------
describe('media state — the thing "Playback error." was hiding', () => {
  const row = (over: Record<string, unknown>) => ({
    recordingId: 'r-1',
    callId: 'c-1',
    channelArn: 'arn:1',
    lobbyName: 'dm:a:b',
    startedAt: '2026-08-26T00:00:00Z',
    endedAt: '2026-08-26T00:01:00Z',
    ...over,
  });

  it('compose still queued → processing, no URL, and no token minted', async () => {
    givenRecording(row({ composeStatus: 'queued', tracks: [{ key: 'x/peer_leg-001.mp4' }, { key: 'x/peer_leg-002.mp4' }] }));
    const res = await run('POST', '/api/recordings/r-1/playback-token');
    expect(res.statusCode).toBe(200);
    const body = res.body as { mediaState: string; composeStatus: string; composedUrl: string | null };
    expect(body.mediaState).toBe('processing');
    expect(body.composeStatus).toBe('queued');
    expect(body.composedUrl).toBeNull();
    // The route still asks LVS what compose is doing — that is the only way
    // it can know — but there is nothing to sign, so nothing is signed.
    const urls = mockCallSfu.mock.calls.map((c) => c[0] as string);
    expect(urls.some((u) => u.includes('playback-tokens'))).toBe(false);
  });

  it('compose skipped with no media at all → unavailable, no URL', async () => {
    givenRecording(row({ composeStatus: 'skipped', segments: [], tracks: [] }));
    const res = await run('POST', '/api/recordings/r-1/playback-token');
    const body = res.body as { mediaState: string; composeStatus: string; composedUrl: string | null };
    expect(body.mediaState).toBe('unavailable');
    expect(body.composeStatus).toBe('skipped');
    expect(body.composedUrl).toBeNull();
  });

  // A solo call is `skipped` (there is no grid to build from one leg) and
  // has an EMPTY segments[] — but its single per-peer leg is real, servable
  // media. Writing skipped off as "nothing to play" would bury all 125 of
  // them behind a shrug.
  it('a skipped SOLO recording still plays, from its one per-peer leg', async () => {
    givenRecording(row({
      composeStatus: 'skipped',
      segments: [],
      tracks: [{ key: 'arn_1/r-1/peer_leg-001.mp4', size: 796 }],
    }));
    mockCallSfu.mockResolvedValue(lvsOk({ token: 'tok', expiresAt: 'later' }));
    const res = await run('POST', '/api/recordings/r-1/playback-token');
    const body = res.body as { mediaState: string; composedUrl: string };
    expect(body.mediaState).toBe('playable');
    expect(body.composedUrl).toContain('/segments/peer_leg-001.mp4');
  });

  it('reads live compose status from LVS when the row has none, and caches a settled answer', async () => {
    givenRecording(row({ tracks: [{ key: 'a/peer_leg-001.mp4' }, { key: 'a/peer_leg-002.mp4' }] }));
    mockCallSfu.mockImplementation(async (url: string) => {
      if (url.endsWith('/recordings')) {
        return lvsOk({ recordings: [{ recordingId: 'r-1', composeStatus: 'ok', composedSegment: { key: 'a/composed.mp4' } }] });
      }
      return lvsOk({ token: 'tok', expiresAt: 'later' });
    });
    const res = await run('POST', '/api/recordings/r-1/playback-token');
    const body = res.body as { mediaState: string; composeStatus: string; composedUrl: string };
    expect(body.composeStatus).toBe('ok');
    expect(body.mediaState).toBe('playable');
    expect(body.composedUrl).toContain('/segments/composed.mp4');
    // Cached, so the next read does not re-ask LVS about a job that is done.
    expect(mockUpdateComposeState).toHaveBeenCalledWith('c-1', 'r-1', expect.objectContaining({ composeStatus: 'ok' }));
  });

  it('an unreachable LVS reports what the row knows, never a guessed URL', async () => {
    givenRecording(row({ tracks: [{ key: 'a/peer_leg-001.mp4' }, { key: 'a/peer_leg-002.mp4' }] }));
    mockCallSfu.mockRejectedValue(new Error('ECONNREFUSED'));
    const res = await run('POST', '/api/recordings/r-1/playback-token');
    const body = res.body as { mediaState: string; composedUrl: string | null };
    // Two legs and no composed file we've been told about: compose is
    // presumed still queued, which is honest, and there is nothing to play.
    expect(body.mediaState).toBe('processing');
    expect(body.composedUrl).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// The transcript 202 used to carry no way of knowing whether to ask again,
// so the client asked forever — six identical requests were observed for
// one recording whose transcript does not exist.
// ---------------------------------------------------------------------------
describe('GET /api/recordings/:recordingId/transcript — bounded polling', () => {
  const transcriptRow = (over: Record<string, unknown>) => ({
    recordingId: 'r-1',
    callId: 'c-1',
    channelArn: 'arn:1',
    lobbyName: 'dm:a:b',
    startedAt: new Date(Date.now() - 120_000).toISOString(),
    endedAt: new Date(Date.now() - 60_000).toISOString(),
    composeStatus: 'skipped',
    ...over,
  });

  it('in flight → 202 with terminal:false and a retry delay', async () => {
    givenRecording(transcriptRow({ transcriptStatus: 'processing' }));
    const res = await run('GET', '/api/recordings/r-1/transcript');
    expect(res.statusCode).toBe(202);
    const body = res.body as { status: string; terminal: boolean; retryAfterMs: number };
    expect(body.status).toBe('processing');
    expect(body.terminal).toBe(false);
    expect(body.retryAfterMs).toBeGreaterThanOrEqual(5_000);
  });

  it('failed → terminal, with no retry delay to act on', async () => {
    givenRecording(transcriptRow({ transcriptStatus: 'failed', transcriptError: 'whisper OOM' }));
    const res = await run('GET', '/api/recordings/r-1/transcript');
    const body = res.body as { terminal: boolean; retryAfterMs: number | null; error: string };
    expect(body.terminal).toBe(true);
    expect(body.retryAfterMs).toBeNull();
    expect(body.error).toBe('whisper OOM');
  });

  it('pending long past the stall deadline → stalled and terminal', async () => {
    givenRecording(transcriptRow({
      transcriptStatus: 'pending',
      endedAt: new Date(Date.now() - 3 * 60 * 60 * 1000).toISOString(),
    }));
    const res = await run('GET', '/api/recordings/r-1/transcript');
    const body = res.body as { status: string; terminal: boolean; retryAfterMs: number | null };
    // 'stalled', not 'failed' — the job sat un-progressed past the
    // deadline, which is a different thing to go look at than a worker
    // that ran and gave up. This row has no `transcriptStatusAt`, so the
    // clock falls back to `endedAt` exactly as it did before that field
    // existed — the legacy path, still working.
    expect(body.status).toBe('stalled');
    expect(body.terminal).toBe(true);
    expect(body.retryAfterMs).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// The stall clock used to start at `endedAt` — when the CALL finished, not
// when the JOB was queued. Identical instants for a fresh recording, which
// is why it survived; wildly apart for a replayed or re-triggered one. A job
// enqueued thirty seconds earlier against an older recording reported
// `waitedMs: 32106080` and was declared stalled before a worker could
// plausibly have looked at it.
// ---------------------------------------------------------------------------
describe('GET /api/recordings/:recordingId/transcript — the stall clock', () => {
  const threeHoursAgo = () => new Date(Date.now() - 3 * 60 * 60 * 1000).toISOString();

  const requeuedRow = (over: Record<string, unknown> = {}) => ({
    recordingId: 'r-1',
    callId: 'c-1',
    channelArn: 'arn:1',
    lobbyName: 'dm:a:b',
    composeStatus: 'skipped',
    startedAt: threeHoursAgo(),
    // The call is old…
    endedAt: threeHoursAgo(),
    // …and the job was queued thirty seconds ago.
    transcriptStatus: 'pending',
    transcriptStatusAt: new Date(Date.now() - 30_000).toISOString(),
    ...over,
  });

  it('a job queued seconds ago on an old recording is not stalled', async () => {
    givenRecording(requeuedRow());
    const res = await run('GET', '/api/recordings/r-1/transcript');
    const body = res.body as {
      status: string; terminal: boolean; waitedMs: number; retryAfterMs: number | null;
    };
    expect(body.status).toBe('pending');
    expect(body.terminal).toBe(false);
    // Seconds, not the three hours since the call ended.
    expect(body.waitedMs).toBeLessThan(60_000);
    expect(body.retryAfterMs).toBeGreaterThanOrEqual(5_000);
  });

  it('reports which clock it used, so a large wait can be read', async () => {
    const row = requeuedRow();
    givenRecording(row);
    const res = await run('GET', '/api/recordings/r-1/transcript');
    const body = res.body as { waitedFrom: string };
    expect(body.waitedFrom).toBe(row.transcriptStatusAt);
    expect(body.waitedFrom).not.toBe(row.endedAt);
  });

  it('still stalls when the job itself has sat past the deadline', async () => {
    givenRecording(requeuedRow({
      // Queued three hours ago and never moved. Same verdict as before —
      // the fix narrows what counts as waiting, it does not remove the
      // deadline.
      transcriptStatusAt: threeHoursAgo(),
    }));
    const res = await run('GET', '/api/recordings/r-1/transcript');
    const body = res.body as { status: string; terminal: boolean; retryAfterMs: number | null };
    expect(body.status).toBe('stalled');
    expect(body.terminal).toBe(true);
    expect(body.retryAfterMs).toBeNull();
  });

  it('a worker that picked the job up restarts the clock', async () => {
    givenRecording(requeuedRow({
      transcriptStatus: 'processing',
      transcriptStatusAt: new Date(Date.now() - 10_000).toISOString(),
    }));
    const res = await run('GET', '/api/recordings/r-1/transcript');
    const body = res.body as { status: string; terminal: boolean };
    expect(body.status).toBe('processing');
    expect(body.terminal).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// Transcript state and summary state are two axes, not one.
//
// The summary is an LLM call over a transcript that already exists. When it
// failed, the consumer wrote `transcriptStatus: 'failed'` — on a row whose
// `transcriptKey` it had stamped moments earlier — and the UI reported the
// transcription as failed for a transcript that served 200 the instant the
// status was flipped by hand.
// ---------------------------------------------------------------------------
describe('a failed summary does not hide a ready transcript', () => {
  const readyRow = (over: Record<string, unknown> = {}) => ({
    recordingId: 'r-1',
    callId: 'c-1',
    channelArn: 'arn:1',
    lobbyName: 'dm:a:b',
    composeStatus: 'skipped',
    startedAt: new Date(Date.now() - 120_000).toISOString(),
    endedAt: new Date(Date.now() - 60_000).toISOString(),
    transcriptStatus: 'ready',
    transcriptKey: 'arn_1/r-1/transcript.json',
    transcriptGeneratedAt: new Date().toISOString(),
    ...over,
  });

  const transcriptDoc = {
    recordingId: 'r-1',
    segments: [{ startMs: 0, endMs: 1_000, speakerId: 's1', speakerLabel: 'Bob', text: 'hi' }],
  };

  it('GET /transcript serves the transcript with the summary failed', async () => {
    givenRecording(readyRow({ summaryStatus: 'failed', summaryError: 'no ANTHROPIC_API_KEY' }));
    mockCallSfu.mockResolvedValue({
      ok: true, status: 200, text: async () => JSON.stringify(transcriptDoc),
    } as unknown as globalThis.Response);

    const res = await run('GET', '/api/recordings/r-1/transcript');
    // 200 with the transcript — not a 202 claiming the transcription
    // failed, which is what the row used to say.
    expect(res.statusCode).toBe(200);
    expect(String(res.body)).toContain('"speakerLabel":"Bob"');
    // …and the other axis travels with it, so a client fetching only the
    // transcript still learns the summary is gone.
    expect(res.headers['x-summary-status']).toBe('failed');
  });

  it('GET /summary explains the failure and points at the transcript', async () => {
    givenRecording(readyRow({ summaryStatus: 'failed', summaryError: 'no ANTHROPIC_API_KEY' }));
    const res = await run('GET', '/api/recordings/r-1/summary');
    // 200, not 202 or 404: an old client turns both of those into `null`
    // and renders an empty panel with no explanation anywhere.
    expect(res.statusCode).toBe(200);
    expect(res.contentType).toBe('text/markdown');
    expect(res.headers['x-summary-status']).toBe('failed');
    const body = String(res.body);
    expect(body).toContain('Summary generation failed');
    expect(body).toContain('no ANTHROPIC_API_KEY');
    expect(body).toContain('transcript itself is complete');
  });

  it('a ready summary is unchanged, and says so in the header', async () => {
    givenRecording(readyRow({ summaryStatus: 'ready', summaryMarkdown: '## Summary\nWe shipped it.' }));
    const res = await run('GET', '/api/recordings/r-1/summary');
    expect(res.statusCode).toBe(200);
    expect(res.body).toBe('## Summary\nWe shipped it.');
    expect(res.headers['x-summary-status']).toBe('ready');
  });

  it('a row from before the split still serves its summary', async () => {
    // No `summaryStatus` at all — every row written before this existed.
    givenRecording(readyRow({ summaryMarkdown: '## Summary\nLegacy row.' }));
    const res = await run('GET', '/api/recordings/r-1/summary');
    expect(res.statusCode).toBe(200);
    expect(res.body).toBe('## Summary\nLegacy row.');
    expect(res.headers['x-summary-status']).toBe('ready');
  });

  it('the detail response carries both axes', async () => {
    givenRecording(readyRow({ summaryStatus: 'failed', summaryError: 'rate limited' }));
    const res = await run('GET', '/api/recordings/r-1');
    const body = res.body as Record<string, unknown>;
    expect(body.transcriptStatus).toBe('ready');
    expect(body.summaryStatus).toBe('failed');
    expect(body.summaryError).toBe('rate limited');
  });
});

// ---------------------------------------------------------------------------
// The detail response is the only place `tracks[]` is served, and LVS stamps
// each track with the recorder pod's absolute path — in local dev, literally
// `file:///Users/<developer>/Projects/...`. Unusable by a browser, and a
// filesystem-layout leak to every authenticated caller.
//
// Same response also re-derives people, so rows written before the webhook
// fix don't keep rendering rejoin counts on the detail card.
// ---------------------------------------------------------------------------
describe('GET /api/recordings/:recordingId — what leaves the process', () => {
  /** Trimmed from the real row 2026-08-26T02-35-09-652Z-546cf1e9. */
  const leakyDetail = {
    recordingId: 'r-1',
    callId: 'c-1',
    channelArn: 'arn:local:ivs:channel/fe74b6a1',
    lobbyName: 'dm:dev-bob:dev-hank',
    startedAt: '2026-08-26T02:35:09.768Z',
    endedAt: '2026-08-26T02:35:28.215Z',
    durationMs: 8_318,
    sessionDurationMs: 18_447,
    startedBy: 'dev-hank',
    participants: [
      { userId: 'dev-bob', displayName: 'Bob Martinez', joinedAt: '2026-08-26T02:34:49.825Z' },
      { userId: 'dev-bob', displayName: 'Bob Martinez', joinedAt: '2026-08-26T02:35:10.000Z' },
    ],
    tracks: [
      {
        legId: 'leg-001',
        participantId: 'a937dfa9-aeb8-5366-a90b-6d5599ba1239',
        file: '/Users/connorhoehn/Projects/realtime-examples/.tilt-lvs-recordings/x/peer_leg-001.mp4',
        key: 'arn_local_ivs_channel_fe74b6a1/2026-08-26T02-35-09-652Z-546cf1e9/peer_leg-001.mp4',
        url: 'file:///Users/connorhoehn/Projects/realtime-examples/.tilt-lvs-recordings/x/peer_leg-001.mp4',
        size: 796,
        kind: 'av',
      },
    ],
    // The field the original scrub missed. Measured live 2026-08-26: a
    // response with a perfectly clean `tracks[]` sitting next to
    // `composedSegment.url: file:///Users/<developer>/…`. Scrubbing one
    // field and not its neighbour is how a fix looks finished while the
    // leak is still on the wire.
    composedSegment: {
      key: 'arn_local_ivs_channel_fe74b6a1/2026-08-26T02-35-09-652Z-546cf1e9/composed.mp4',
      url: 'file:///Users/connorhoehn/Projects/realtime-examples/.tilt-lvs-recordings/x/composed.mp4',
      size: 41_000,
    },
  };

  it('no file:// URI and no developer home directory anywhere in the response', async () => {
    givenRecording(leakyDetail);

    const res = await run('GET', '/api/recordings/r-1');
    expect(res.statusCode).toBe(200);

    const serialized = JSON.stringify(res.body);
    expect(serialized).not.toContain('file://');
    expect(serialized).not.toContain('/Users/');
    expect(serialized).not.toContain('.tilt-lvs-recordings');
  });

  it('keeps composedSegment useful without its recorder-pod path', async () => {
    givenRecording(leakyDetail);
    const res = await run('GET', '/api/recordings/r-1');
    const seg = (res.body as { composedSegment: Record<string, unknown> }).composedSegment;
    expect(seg.size).toBe(41_000);
    expect(seg.key).toContain('composed.mp4');
    expect(seg).not.toHaveProperty('url');
  });

  it('tracks[] carry the same proxied /segments/ URL shape as the composed file', async () => {
    givenRecording(leakyDetail);

    const res = await run('GET', '/api/recordings/r-1');
    const track = (res.body as { tracks: Array<Record<string, unknown>> }).tracks[0]!;
    expect(track.url).toBe(
      'https://public.video.test/api/channels/'
      + encodeURIComponent('arn:local:ivs:channel/fe74b6a1')
      + '/recordings/r-1/segments/peer_leg-001.mp4',
    );
    expect(track).not.toHaveProperty('file');
    // Everything else the downloads list needs survives.
    expect(track.legId).toBe('leg-001');
    expect(track.size).toBe(796);
  });

  it('participants are deduped and include the initiator', async () => {
    givenRecording(leakyDetail);

    const res = await run('GET', '/api/recordings/r-1');
    const body = res.body as {
      participants: Array<{ userId: string }>;
      participantCount: number;
    };
    expect(body.participants.map((p) => p.userId).sort()).toEqual(['dev-bob', 'dev-hank']);
    expect(body.participantCount).toBe(2);
  });
});
