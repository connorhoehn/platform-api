// Route authority is inert fixture data here; real token/callback proofs have
// their own boundary suites. Actual native MemoryVideoSessionStore is used.
import express from 'express';
import type { Server } from 'node:http';
import { MemoryVideoSessionStore } from '../../repositories/MemoryVideoSessionStore';
import { VideoCallBindingConflict, type VideoSessionRecord } from '../../repositories/VideoSessionStore';
let mockStore: MemoryVideoSessionStore;
const mockAuthority = jest.fn(), mockLobby = jest.fn();
jest.mock('../../middleware/auth', () => ({ requireAuth: (_r: any, _s: any, n: any) => n(), optionalAuth: (_r: any, _s: any, n: any) => n() }));
jest.mock('../../repositories', () => ({ videoSessionRepo: {
  getSession: (...a: Parameters<MemoryVideoSessionStore['getSession']>) => mockStore.getSession(...a),
  bindCallParticipant: (...a: Parameters<MemoryVideoSessionStore['bindCallParticipant']>) => mockStore.bindCallParticipant(...a),
}, callReactionRepo: {}, profileRepo: {}, workSourceAccountRepo: {} }));
jest.mock('../../lib/callSeatAuthority', () => ({ askCallSeatAuthority: (...a: unknown[]) => mockAuthority(...a) }));
jest.mock('../../lib/lobbyAccess', () => ({ tenantLobby: jest.requireActual('../../lib/lobbyAccess').tenantLobby, lobbyAccess: () => ({ assertLobbyAccess: (...a: unknown[]) => mockLobby(...a) }) }));
jest.mock('../../recordings/liveCaptions', () => ({ startLiveCaptions: jest.fn(), stopLiveCaptions: jest.fn() }));
import { videoSessionsRouter } from '../videoSessions';
import { errorHandler, AppError } from '../../middleware/error-handler';
const body = { lobbyName: 'orgiq:initiative:p-observability', callId: 'call-one', participantId: 'pid-one', clientId: 'client-one' };
const participant = { participantId: 'pid-one', userId: 'alice', clientId: 'client-one', displayName: 'Alice', joinedAt: '2026-10-10T12:00:00Z' };
const row = (): VideoSessionRecord => ({ documentId: body.lobbyName, sessionId: 'sess-one', channelArn: 'arn-one', streamKey: '', orgId: 'orgiq', startedBy: 'alice', status: 'active', startedAt: participant.joinedAt, participants: [participant], participantHistory: { 'pid-one': { ...participant } } });
const app = express().use(express.json()).use((req, _res, next) => {
  req.user = { sub: 'alice', orgId: req.headers['x-test-tenant'] === 'assessment' ? 'assessment' : 'orgiq', ...(req.headers['x-test-auth-kind'] !== 'dev' ? { authKind: 'orgiq-hs256' as const } : {}) }; next();
}).use('/api/video', videoSessionsRouter).use(errorHandler);
let server: Server, base: string;
beforeAll(async () => { await new Promise<void>(resolve => { server = app.listen(0, '127.0.0.1', resolve); }); base = `http://127.0.0.1:${(server.address() as { port: number }).port}`; });
afterAll(async () => { server.closeIdleConnections(); await new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve())); });
const post = async (input: object = body, dev = false, tenant = 'orgiq') => {
  const res = await fetch(`${base}/api/video/sessions/sess-one/call-binding`, { method: 'POST', headers: { Authorization: 'Bearer fixture', 'Content-Type': 'application/json', 'x-test-tenant': tenant, ...(dev ? { 'x-test-auth-kind': 'dev' } : {}) }, body: JSON.stringify(input) });
  return { status: res.status, body: await res.json(), headers: Object.fromEntries(res.headers.entries()) };
};
beforeEach(async () => {
  mockStore = new MemoryVideoSessionStore(); await mockStore.createSession(row());
  mockLobby.mockReset().mockResolvedValue(undefined);
  mockAuthority.mockReset().mockResolvedValue({ allowed: true, callId: body.callId, lobbyName: body.lobbyName, clientId: body.clientId, userId: 'alice', tenant: 'orgiq', epoch: 2 });
});
test('exact active media+accepted callback binds once and ACK echoes server-owned tuple', async () => {
  const res = await post(); expect(res.status).toBe(200);
  expect(res.body).toEqual({ bound: true, ...body, sessionId: 'sess-one', channelArn: 'arn-one' });
  expect(res.headers['cache-control']).toBe('private, no-store');
  expect(mockAuthority).toHaveBeenCalledWith('fixture', { callId: 'call-one', lobbyName: body.lobbyName, clientId: 'client-one' });
  const first = (await mockStore.getSession(body.lobbyName, 'sess-one'))!.participantHistory!['pid-one']!.callBinding;
  expect((await post()).status).toBe(200);
  expect((await mockStore.getSession(body.lobbyName, 'sess-one'))!.participantHistory!['pid-one']!.callBinding).toEqual(first);
});
test('cannot attach arbitrary caller/tenant/participant/tab/channel or inactive media', async () => {
  expect((await post({ ...body, participantId: 'pid-other' })).status).toBe(404);
  expect((await post({ ...body, clientId: 'client-other' })).status).toBe(404);
  const rec = row(); rec.status = 'ended'; await mockStore.createSession(rec); expect((await post()).status).toBe(404);
  expect(mockAuthority).not.toHaveBeenCalled();
});
test('dev fallback, forged body identity and callback denial/unavailability cannot bind', async () => {
  expect((await post({ ...body, userId: 'bob' })).status).toBe(400);
  expect((await post(body, true)).status).toBe(403);
  mockAuthority.mockRejectedValue(new AppError(403, 'Denied')); expect((await post()).status).toBe(403);
  mockAuthority.mockRejectedValue(new AppError(503, 'Unavailable')); expect((await post()).status).toBe(503);
  expect((await mockStore.getSession(body.lobbyName, 'sess-one'))!.participantHistory!['pid-one']!.callBinding).toBeUndefined();
});
test.each([{ userId: 'bob' }, { tenant: 'assessment' }])('callback identity mismatch %j cannot bind', async changed => {
  mockAuthority.mockResolvedValue({ allowed: true, ...body, userId: 'alice', tenant: 'orgiq', epoch: 2, ...changed });
  expect((await post()).status).toBe(403);
});
test('leave/rejoin during callback rejects stale participant before persistence', async () => {
  mockAuthority.mockImplementation(async () => {
    await mockStore.removeParticipant(body.lobbyName, 'sess-one', body.participantId);
    return { allowed: true, ...body, userId: 'alice', tenant: 'orgiq', epoch: 2 };
  });
  expect((await post()).status).toBe(404);
  expect((await mockStore.getSession(body.lobbyName, 'sess-one'))!.participantHistory!['pid-one']!.callBinding).toBeUndefined();
});
test('host authority revoked during callback and immutable conflict preserve prior state', async () => {
  mockLobby.mockResolvedValueOnce(undefined).mockRejectedValue(new AppError(404, 'Gone'));
  expect((await post()).status).toBe(404);
  mockLobby.mockReset().mockResolvedValue(undefined);
  jest.spyOn(mockStore, 'bindCallParticipant').mockRejectedValue(new VideoCallBindingConflict());
  expect((await post()).status).toBe(409);
});
test('verified document source survives genuine first binding/ACK and conflicting source cannot overwrite', async () => {
  const lobbyName = 'assessment:doc:h:abc', sourceId = 'ec:standards/ci-cd.md';
  const documentContext = { tenant: 'assessment', sourceId, lobbyName };
  const docBody = { ...body, lobbyName, documentSourceId: sourceId };
  await mockStore.createSession({ ...row(), documentId: lobbyName, orgId: 'assessment' });
  mockAuthority.mockResolvedValue({ allowed: true, callId: body.callId, clientId: body.clientId, lobbyName, userId: 'alice', tenant: 'assessment', epoch: 2, documentContext });
  const first = await post(docBody, false, 'assessment');
  expect(first.status).toBe(200); expect(first.body).toMatchObject({ documentContext });
  expect(mockAuthority).toHaveBeenCalledWith('fixture', { callId: body.callId, clientId: body.clientId, lobbyName, documentSourceId: sourceId });
  const accepted = (await mockStore.getSession(lobbyName, 'sess-one'))!.participantHistory!['pid-one']!.callBinding;
  expect(accepted?.documentContext).toEqual(documentContext);
  expect((await post(docBody, false, 'assessment')).status).toBe(200);
  mockAuthority.mockResolvedValue({ allowed: true, callId: body.callId, clientId: body.clientId, lobbyName, userId: 'alice', tenant: 'assessment', epoch: 2, documentContext: { ...documentContext, sourceId: 'other' } });
  expect((await post({ ...docBody, documentSourceId: 'other' }, false, 'assessment')).status).toBe(409);
  expect((await mockStore.getSession(lobbyName, 'sess-one'))!.participantHistory!['pid-one']!.callBinding).toEqual(accepted);
});
test('source hint is required only on existing Assessment document calls and cannot invent Work document family', async () => {
  expect((await post({ ...body, documentSourceId: 'source' })).status).toBe(400);
  expect((await post({ ...body, lobbyName: 'assessment:doc:h:abc' }, false, 'assessment')).status).toBe(400);
  expect((await post({ ...body, lobbyName: 'orgiq:doc:source', documentSourceId: 'source' })).status).toBe(400);
  expect((await post({ ...body, lobbyName: 'assessment:doc:h:abc', documentSourceId: 'a\u0000bad' }, false, 'assessment')).status).toBe(400);
  expect(mockAuthority).not.toHaveBeenCalled();
});
