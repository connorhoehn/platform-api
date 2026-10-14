// P1 2026-10-14 (route-access-guard): the access guards on
//   POST /api/video/sessions/:sessionId/call-binding
//   GET  /api/recordings/:recordingId/status
// run the real requireAuth (HS256 OrgIQ realtime token) and the real lobby
// authority callback against a stub BFF (LOBBY_AUTHORITY_REQUIRED=true). Each
// refusal is proven to happen BEFORE any side effect: no seat callback or
// binding write for call-binding, and no event-bus subscription, timer or
// source lookup for the recording-status long poll.
import express from 'express';
import jwt from 'jsonwebtoken';
import type { Server } from 'node:http';
import { MemoryVideoSessionStore } from '../../repositories/MemoryVideoSessionStore';
import type { VideoSessionRecord } from '../../repositories/VideoSessionStore';
import type { CallRecording } from '../../repositories/CallRecordingRepository';

let mockStore: MemoryVideoSessionStore;
const mockGetSession = jest.fn(), mockBind = jest.fn(), mockSeat = jest.fn();
jest.mock('../../repositories', () => ({
  videoSessionRepo: {
    getSession: (...a: unknown[]) => mockGetSession(...a),
    bindCallParticipant: (...a: unknown[]) => mockBind(...a),
    findActiveByLobbyName: async () => null,
  },
  callReactionRepo: {}, profileRepo: {}, workSourceAccountRepo: {},
  hangoutRoomRepo: { getRoomBySlug: async () => null }, hangoutRoomMemberRepo: { isMember: async () => false },
}));
jest.mock('../../lib/callSeatAuthority', () => ({ askCallSeatAuthority: (...a: unknown[]) => mockSeat(...a) }));
jest.mock('../../recordings/liveCaptions', () => ({ startLiveCaptions: jest.fn(), stopLiveCaptions: jest.fn() }));
jest.mock('../../pipeline/runPersistence', () => ({ getRunPersistenceService: () => null }));

import { videoSessionsRouter } from '../videoSessions';
import { installRecordingStatusRoute } from '../recordingStatus';
import { recordingStatus, type RecordingEventSource } from '../../recordings/recordingStatus';
import { errorHandler, NotFoundError } from '../../middleware/error-handler';

const secret = 'isolated-recording-guard-test-secret-not-a-user-key';
const lobbyName = 'orgiq:initiative:p-guard';
const token = (sub: string, expiresIn = 60) => jwt.sign({ sub, org: 'orgiq', tenants: ['orgiq'], tokenKind: 'orgiq-hs256' }, secret,
  { algorithm: 'HS256', issuer: 'orgiq-middleware', audience: 'realtime', expiresIn });
const expired = (sub: string) => jwt.sign({ sub, org: 'orgiq', tenants: ['orgiq'], tokenKind: 'orgiq-hs256', exp: Math.floor(Date.now() / 1000) - 60 }, secret,
  { algorithm: 'HS256', issuer: 'orgiq-middleware', audience: 'realtime' });

// The stub BFF: 200 {allowed:true} unless the subject's verdict says otherwise
// (403 = not admitted to this lobby, 401 = session epoch revoked).
const bffVerdict = new Map<string, number>();
const bffCalls: string[] = [];
const bff = express().use(express.json()).post('/api/realtime/lobby-access', (req, res) => {
  const sub = (jwt.decode(String(req.headers.authorization).slice(7)) as { sub: string }).sub;
  bffCalls.push(sub);
  const status = bffVerdict.get(sub) ?? 200;
  res.status(status).json(status === 200 ? { allowed: true } : { allowed: false });
});

const participant = { participantId: 'pid-alice', userId: 'alice', clientId: 'client-alice', displayName: 'Alice', joinedAt: '2026-10-14T12:00:00Z' };
const session = (): VideoSessionRecord => ({ documentId: lobbyName, sessionId: 'sess-one', channelArn: 'arn-one', streamKey: '', orgId: 'orgiq', startedBy: 'alice',
  status: 'active', startedAt: participant.joinedAt, participants: [participant], participantHistory: { 'pid-alice': { ...participant } } });
const recording = { callId: 'call-one', recordingId: 'rec-one', lobbyName, composeStatus: 'ok', transcriptStatus: 'processing',
  summaryRunId: 'run-one', processingAttemptId: 'gen-one' } as CallRecording;

const subscribeAll = jest.fn(() => 'sub-1'), unsubscribe = jest.fn(), sourceLookups = jest.fn();
const authorizeRecording = jest.fn(async (req: express.Request) => {
  if (req.user?.sub !== 'alice') throw new NotFoundError('recording not found');
  return recording;
});
const app = express().use(express.json()).use('/api/video', videoSessionsRouter);
const recordingsRouter = express.Router();
installRecordingStatusRoute(recordingsRouter, { authorize: authorizeRecording, timeoutMs: 150,
  source: (): RecordingEventSource => { sourceLookups(); return { subscribeAll, unsubscribe }; } });
app.use('/api', recordingsRouter).use(errorHandler);

const servers: Server[] = [];
let base = '';
const listen = (a: express.Express) => new Promise<string>((resolve) => {
  const s = a.listen(0, '127.0.0.1', () => resolve(`http://127.0.0.1:${(s.address() as { port: number }).port}`)); servers.push(s);
});
const saved = { ...process.env };
beforeAll(async () => {
  const bffBase = await listen(bff);
  base = await listen(app);
  Object.assign(process.env, { REALTIME_JWT_SECRET: secret, LOBBY_AUTHORITY_REQUIRED: 'true', LOBBY_AUTHORITY_URL: `${bffBase}/api/realtime/lobby-access` });
  delete process.env.SKIP_AUTH;
});
afterAll(async () => {
  process.env = saved;
  for (const s of servers) { s.closeAllConnections(); await new Promise<void>((resolve) => s.close(() => resolve())); }
});
beforeEach(async () => {
  bffVerdict.clear(); bffCalls.length = 0;
  mockStore = new MemoryVideoSessionStore(); await mockStore.createSession(session());
  mockGetSession.mockReset().mockImplementation((...a: Parameters<MemoryVideoSessionStore['getSession']>) => mockStore.getSession(...a));
  mockBind.mockReset().mockImplementation((...a: Parameters<MemoryVideoSessionStore['bindCallParticipant']>) => mockStore.bindCallParticipant(...a));
  mockSeat.mockReset().mockImplementation(async (bearer: string) => ({ allowed: true, callId: 'call-one', lobbyName, clientId: 'client-alice',
    userId: (jwt.decode(bearer) as { sub: string }).sub, tenant: 'orgiq', epoch: 3 }));
  subscribeAll.mockClear(); unsubscribe.mockClear(); sourceLookups.mockClear(); authorizeRecording.mockClear();
});

describe('call-binding access guard', () => {
  const bind = async (bearer: string, body: Record<string, string> = { lobbyName, callId: 'call-one', participantId: 'pid-alice', clientId: 'client-alice' }) => {
    const res = await fetch(`${base}/api/video/sessions/sess-one/call-binding`, { method: 'POST',
      headers: { Authorization: `Bearer ${bearer}`, 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
    return { status: res.status, body: await res.json() as Record<string, unknown> };
  };
  const bound = async () => (await mockStore.getSession(lobbyName, 'sess-one'))!.participantHistory!['pid-alice']!.callBinding;
  const nothingHappened = async () => {
    expect(mockSeat).not.toHaveBeenCalled(); expect(mockBind).not.toHaveBeenCalled(); expect(await bound()).toBeUndefined();
  };

  it('admits the media participant: lobby authority asked, seat proven, binding written', async () => {
    const res = await bind(token('alice'));
    expect(res.status).toBe(200); expect(res.body).toMatchObject({ bound: true, participantId: 'pid-alice', channelArn: 'arn-one' });
    expect(bffCalls[0]).toBe('alice'); expect(mockSeat).toHaveBeenCalledTimes(1); expect(await bound()).toMatchObject({ callId: 'call-one', userId: 'alice' });
  });

  it('refuses an outsider the BFF does not admit before reading the session', async () => {
    bffVerdict.set('mallory', 403);
    expect((await bind(token('mallory'))).status).toBe(404);
    expect(bffCalls).toEqual(['mallory']); expect(mockGetSession).not.toHaveBeenCalled();
    await nothingHappened();
  });

  it('refuses a lobby member who is not that media participant (naming someone else\'s leg)', async () => {
    expect((await bind(token('bob'))).status).toBe(404);
    await nothingHappened();
  });

  it('refuses a token whose session epoch the BFF has revoked', async () => {
    bffVerdict.set('alice', 401);
    expect((await bind(token('alice'))).status).toBe(404);
    expect(mockGetSession).not.toHaveBeenCalled();
    await nothingHappened();
  });

  it('refuses an expired token with 401 before asking anyone', async () => {
    expect((await bind(expired('alice'))).status).toBe(401);
    expect(bffCalls).toEqual([]); expect(mockGetSession).not.toHaveBeenCalled();
    await nothingHappened();
  });
});

describe('recording status access guard', () => {
  const status = async (bearer: string, after?: string) => {
    const res = await fetch(`${base}/api/recordings/rec-one/status${after ? `?after=${after}` : ''}`, { headers: { Authorization: `Bearer ${bearer}` } });
    return { status: res.status, body: await res.json() as Record<string, unknown> };
  };
  const revision = recordingStatus(recording).revision;
  const allocatedNothing = () => { expect(sourceLookups).not.toHaveBeenCalled(); expect(subscribeAll).not.toHaveBeenCalled(); };

  it('admits the participant; the long poll subscribes once and releases it', async () => {
    const res = await status(token('alice'), revision);
    expect(res.status).toBe(200); expect(res.body).toEqual(recordingStatus(recording));
    expect(subscribeAll).toHaveBeenCalledTimes(1); expect(unsubscribe).toHaveBeenCalledTimes(1);
  });

  it('refuses an outsider on the long-poll path without subscribing', async () => {
    const res = await status(token('mallory'), revision);
    expect(res.status).toBe(404); expect(res.body.recordingId).toBeUndefined();
    allocatedNothing();
  });

  it('refuses an outsider even with a malformed revision (404, not a 400 that confirms the id)', async () => {
    expect((await status(token('mallory'), 'not-a-revision')).status).toBe(404);
    allocatedNothing();
  });

  it('refuses a participant whose session the BFF has revoked, without subscribing', async () => {
    bffVerdict.set('alice', 401);
    expect((await status(token('alice'), revision)).status).toBe(404);
    expect(bffCalls).toEqual(['alice']);
    allocatedNothing();
  });

  it('refuses an expired token with 401 before the recording is read', async () => {
    expect((await status(expired('alice'), revision)).status).toBe(401);
    expect(authorizeRecording).not.toHaveBeenCalled(); expect(bffCalls).toEqual([]);
    allocatedNothing();
  });
});
