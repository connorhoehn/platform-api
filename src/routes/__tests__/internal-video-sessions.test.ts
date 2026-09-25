/**
 * Document calls: the gateway reports that a person is gone, or that nobody
 * is connected, after the rejoin grace (a tab closed without Leave). Without
 * it the row stayed `active` for the reaper's ~5 min and the document offered
 * "Join · 1" into an empty call.
 */
jest.mock('../../repositories', () => ({ videoSessionRepo: {} }));

import express from 'express';
import { SERVICE_AUTH_HEADER, signEnvelope } from '@connorhoehn/service-runtime';
import { createInternalVideoSessionsRouter } from '../internal-video-sessions';
import { MemoryVideoSessionStore } from '../../repositories/MemoryVideoSessionStore';
import { participantCountOf } from '../../lib/documentCall';

const SECRET = 'test-secret';

function review(over: Record<string, unknown> = {}) {
  return {
    documentId: 'doc-auth', sessionId: 'sess-1', channelArn: 'arn:1', streamKey: '', status: 'active' as const,
    startedAt: '2026-09-25T04:00:00Z', startedBy: 'connor', kind: 'document-review' as const,
    title: 'Review', documentIds: ['doc-auth'], media: 'video' as const, hostUserId: 'connor',
    participants: [
      { userId: 'connor', participantId: 'p1', displayName: 'C', joinedAt: 'x' },
      { userId: 'alice', participantId: 'p2', displayName: 'A', joinedAt: 'x' },
      { userId: 'alice', participantId: 'p3', clientId: 'tab-2', displayName: 'A', joinedAt: 'x' },
    ],
    ...over,
  };
}

describe('/internal/video-sessions', () => {
  const prev = process.env.SERVICE_AUTH_SECRET;
  beforeAll(() => { process.env.SERVICE_AUTH_SECRET = SECRET; });
  afterAll(() => { process.env.SERVICE_AUTH_SECRET = prev; });

  async function post(store: MemoryVideoSessionStore, path: string, body: Record<string, unknown>, signed = true) {
    const app = express();
    app.use(express.json());
    app.use('/internal/video-sessions', createInternalVideoSessionsRouter(store));
    const server = app.listen(0);
    try {
      const port = (server.address() as { port: number }).port;
      const headers: Record<string, string> = { 'content-type': 'application/json' };
      if (signed) headers[SERVICE_AUTH_HEADER] = signEnvelope(body, { secret: SECRET, serviceId: 'realtime-examples' });
      const res = await fetch(`http://127.0.0.1:${port}/internal/video-sessions/${path}`, { method: 'POST', headers, body: JSON.stringify(body) });
      return { status: res.status, json: await res.json() as any };
    } finally {
      server.close();
    }
  }

  it('leave drops every entry of that person, and the count follows', async () => {
    const store = new MemoryVideoSessionStore();
    await store.createSession(review());
    const res = await post(store, 'leave', { lobbyName: 'doc-auth', sessionId: 'sess-1', userId: 'alice' });
    expect(res).toEqual({ status: 200, json: { remaining: 1, ended: false } });
    expect(participantCountOf((await store.getSession('doc-auth', 'sess-1'))!)).toBe(1);
  });

  it('end closes an active document call once; a repeat is harmless', async () => {
    const store = new MemoryVideoSessionStore();
    await store.createSession(review());
    expect((await post(store, 'end', { lobbyName: 'doc-auth', sessionId: 'sess-1', reason: 'rejoin-grace-expired' })).json)
      .toEqual({ ended: true, already: false });
    const row = (await store.getSession('doc-auth', 'sess-1'))!;
    expect(row.status).toBe('ended');
    expect(participantCountOf(row)).toBe(0);
    expect((await store.findActiveByLobbyName('doc-auth'))).toBeNull();
    expect((await post(store, 'end', { lobbyName: 'doc-auth', sessionId: 'sess-1' })).json).toEqual({ ended: true, already: true });
  });

  it('leaves every other kind of call alone', async () => {
    const store = new MemoryVideoSessionStore();
    await store.createSession(review({ documentId: 'dm:a-b', kind: undefined }));
    expect((await post(store, 'end', { lobbyName: 'dm:a-b', sessionId: 'sess-1' })).json).toEqual({ skipped: 'not a document call' });
    expect((await store.getSession('dm:a-b', 'sess-1'))!.status).toBe('active');
  });

  it('refuses unsigned callers, bad ids and unknown sessions', async () => {
    const store = new MemoryVideoSessionStore();
    await store.createSession(review());
    expect((await post(store, 'end', { lobbyName: 'doc-auth', sessionId: 'sess-1' }, false)).status).toBe(401);
    expect((await store.getSession('doc-auth', 'sess-1'))!.status).toBe('active');
    expect((await post(store, 'leave', { lobbyName: 'doc-auth', sessionId: 'sess-1' })).status).toBe(400);
    expect((await post(store, 'end', { lobbyName: 'doc-auth', sessionId: 'nope' })).status).toBe(404);
  });
});
