import express from 'express';
import { EventBus } from 'distributed-core';
import type { Server } from 'http';
import jwt from 'jsonwebtoken';
import { installRecordingStatusRoute } from '../recordingStatus';
import { recordingStatus, type RecordingEvent, type RecordingEventSource } from '../../recordings/recordingStatus';
import { NotFoundError, errorHandler } from '../../middleware/error-handler';
import type { CallRecording } from '../../repositories/CallRecordingRepository';
const hostAccess = jest.fn().mockResolvedValue(true);
jest.mock('../../lib/lobbyAuthority', () => ({ askLobbyAuthority: (...args: unknown[]) => hostAccess(...args) }));
jest.mock('../../pipeline/runPersistence', () => ({ getRunPersistenceService: () => null }));
const secret = 'isolated-recording-status-test-secret-not-a-user-key';
const initial = { callId: 'call-one', recordingId: 'recording-one', lobbyName: 'orgiq:initiative:one', composeStatus: 'ok',
  transcriptStatus: 'processing', summaryRunId: 'run-one', processingAttemptId: 'generation-one' } as CallRecording;
function token(expiresIn = 60) { return jwt.sign({ sub: 'reader', org: 'orgiq', tenants: ['orgiq'], tokenKind: 'orgiq-hs256' }, secret,
  { algorithm: 'HS256', issuer: 'orgiq-middleware', audience: 'realtime', expiresIn }); }
function harness(nativeBus?: RecordingEventSource) {
  let row = { ...initial }, allowed = true;
  const listeners = new Map<string, (event: RecordingEvent) => Promise<void>>();
  const authorize = jest.fn(async () => { if (!allowed) throw new NotFoundError('recording not found'); return row; });
  const app = express();
  installRecordingStatusRoute(app, { authorize, timeoutMs: 2000,
    source: () => ({ subscribeAll: listener => { const id = nativeBus?.subscribeAll(listener) ?? 'one'; listeners.set(id, listener); return id; }, unsubscribe: id => { nativeBus?.unsubscribe(id); listeners.delete(id); } }) });
  app.use(errorHandler);
  const server = app.listen(0, '127.0.0.1'); servers.push(server);
  const url = new Promise<string>(resolve => server.once('listening', () => resolve(`http://127.0.0.1:${(server.address() as { port: number }).port}`)));
  return { app, url, listeners, authorize, deny: () => { allowed = false; }, change: () => { row = { ...row, transcriptStatus: 'ready', summaryStatus: 'processing' }; },
    emit: async () => { for (const listener of listeners.values()) await listener({ payload: { runId: 'run-one' } }); } };
}
const servers: Server[] = [];
afterEach(async () => { for (const server of servers.splice(0)) { server.closeAllConnections(); await new Promise<void>(resolve => server.close(() => resolve())); } });
async function get(h: ReturnType<typeof harness>, path: string, bearer: string, expected?: number) {
  const response = await fetch((await h.url) + path, { headers: { Authorization: `Bearer ${bearer}` } });
  if (expected !== undefined) expect(response.status).toBe(expected);
  return { status: response.status, body: await response.json() as Record<string, unknown>, headers: Object.fromEntries(response.headers.entries()) };
}
const oldSecret = process.env.REALTIME_JWT_SECRET;
beforeAll(() => { process.env.REALTIME_JWT_SECRET = secret; });
afterAll(() => { if (oldSecret === undefined) delete process.env.REALTIME_JWT_SECRET; else process.env.REALTIME_JWT_SECRET = oldSecret; });
beforeEach(() => hostAccess.mockReset().mockResolvedValue(true));
const waitFor = async (predicate: () => boolean) => { for (let i = 0; i < 100 && !predicate(); i++) await new Promise(resolve => setTimeout(resolve, 5)); expect(predicate()).toBe(true); };
it('returns a private status projection only to an authorized recording participant', async () => {
  const h = harness();
  const response = await get(h, '/recordings/recording-one/status', token(), 200);
  expect(response.body).toEqual(recordingStatus(initial)); expect(response.headers['cache-control']).toBe('private, no-store');
  h.deny(); await get(h, '/recordings/recording-one/status', token(), 404);
});
it('native events wake a waiting request immediately and return freshly read state', async () => {
  const h = harness();
  const pending = get(h, `/recordings/recording-one/status?after=${recordingStatus(initial).revision}`, token());
  await waitFor(() => h.listeners.size === 1); h.change(); await h.emit();
  const response = await pending;
  expect(response.status).toBe(200); expect(response.body.summaryStatus).toBe('processing'); expect(h.listeners.size).toBe(0);
});
it.each(['participant', 'host'])('withholds status when %s access is revoked while waiting', async kind => {
  const h = harness();
  const pending = get(h, `/recordings/recording-one/status?after=${recordingStatus(initial).revision}`, token());
  await waitFor(() => h.listeners.size === 1);
  if (kind === 'participant') h.deny(); else hostAccess.mockResolvedValue(false);
  await h.emit(); const response = await pending;
  expect(response.status).toBe(404); expect(response.body.recordingId).toBeUndefined(); expect(h.listeners.size).toBe(0);
});
it('revalidates token expiry after waiting instead of returning a final status', async () => {
  const h = harness();
  const pending = get(h, `/recordings/recording-one/status?after=${recordingStatus(initial).revision}`, token(1));
  await waitFor(() => h.listeners.size === 1); await new Promise(resolve => setTimeout(resolve, 1100)); await h.emit();
  const response = await pending; expect(response.status).toBe(401); expect(response.body.recordingId).toBeUndefined();
});
it('rejects a malformed revision before opening a subscription', async () => {
  const h = harness(); await get(h, '/recordings/recording-one/status?after=anything', token(), 400);
  expect(h.listeners.size).toBe(0);
});


it('an actual native EventBus publication wakes the authenticated HTTP status request', async () => {
  const handlers = new Map<string, (topic: string, payload: unknown, meta: Record<string, unknown>) => Promise<void>>();
  let sequence = 0;
  const transport = {
    subscribe: (_topic: string, listener: never) => { const id = String(++sequence); handlers.set(id, listener); return Promise.resolve(id); },
    unsubscribe: async (id: string) => { handlers.delete(id); },
    publish: async (topic: string, payload: unknown, meta: Record<string, unknown> = {}) => { for (const listener of handlers.values()) await listener(topic, payload, meta); },
  };
  const bus = new EventBus<Record<string, Record<string, unknown>>>(transport as never, 'isolated-recording-status', { topic: 'isolated.recording.status' });
  await bus.start();
  try {
    const h = harness(bus);
    const pending = get(h, `/recordings/recording-one/status?after=${recordingStatus(initial).revision}`, token());
    await waitFor(() => h.listeners.size === 1);
    h.change();
    await bus.publish('pipeline:step:completed', { runId: 'run-one', stepId: 'store', output: { callId: initial.callId, recordingId: initial.recordingId } });
    const response = await pending;
    expect(response.status).toBe(200); expect(response.body.summaryStatus).toBe('processing'); expect(h.listeners.size).toBe(0);
  } finally { await bus.stop(); }
});
