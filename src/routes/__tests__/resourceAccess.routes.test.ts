// Loop 29c (NFR #172): the document sub-resource routes. Each group is
// mounted behind the gate routes/index.ts uses, in front of a stand-in router
// that answers 200 — so this proves the gate, and the guard test
// (route-access-guard.test.ts) proves index.ts mounts it. Conventions: owner /
// editor allowed, reader on a write 403 `{code:'forbidden'}`, stranger 404,
// no bearer 401.
import express, { Router, type Request } from 'express';

const mockLogger = { info: jest.fn(), warn: jest.fn(), error: jest.fn(), debug: jest.fn(), child: () => mockLogger };
jest.mock('../../lib/logger', () => ({ __esModule: true, default: mockLogger, withContext: () => mockLogger }));
let mockGate: import('../../documents/documentReadGate').DocumentReadGate;
jest.mock('../../documents/documentReadGate', () => {
  const actual = jest.requireActual('../../documents/documentReadGate');
  return { ...actual, documentReadGate: () => mockGate };
});
jest.mock('../../repositories', () => ({ callRecordingRepo: { resolveForUser: jest.fn() } }));
jest.mock('../../chat/chatCommandRegistry', () => ({
  ...jest.requireActual('../../chat/chatCommandRegistry'),
  getChatCommandPipeline: (t: string) => ({ trigger: t.startsWith('/') ? t : `/${t}` }),
}));

import { documentSubrouteGates } from '../documentSubrouteGates';
import { makeDocumentReadGate } from '../../documents/documentReadGate';
import { requireDocumentEdit, requireDocumentRead, bodyId } from '../../middleware/resource-access';
import { assertDeckSourcesReadable } from '../../deck/deckSourceAccess';
import { assertChatCommandAccess } from '../chatCommands';
import { errorHandler } from '../../middleware/error-handler';
import { call } from './helpers/accessHarness';

const ROWS: Record<string, Record<string, unknown>> = {
  'doc-1': { documentId: 'doc-1', createdBy: 'dev-owner', editors: ['dev-grace'], viewers: ['dev-reader'] },
  'doc-private': { documentId: 'doc-private', createdBy: 'dev-frank' },
};
beforeEach(() => {
  mockGate = makeDocumentReadGate({ getDocumentRow: async (id) => (ROWS[id] as never) ?? null, getLifecycleOwner: async () => undefined });
});

function appWith(mount: string, gate: express.RequestHandler): express.Express {
  const stub = Router({ mergeParams: true });
  stub.all(/.*/, (req, res) => { res.status(200).json({ ok: true, path: req.path }); });
  const app = express();
  app.use(express.json());
  app.use(mount, gate, stub);
  app.use(errorHandler);
  return app;
}

const cases: Array<{ name: string; mount: string; gate: express.RequestHandler; read: [string, string]; write?: [string, string]; readerWrite?: [string, string] }> = [
  { name: 'lifecycle', mount: '/documents/:documentId/lifecycle', gate: documentSubrouteGates.lifecycle, read: ['GET', '/'], write: ['POST', '/ownership'], readerWrite: ['POST', '/transitions'] },
  { name: 'review requests', mount: '/documents/:documentId/review-requests', gate: documentSubrouteGates.reviewRequests, read: ['GET', '/'], readerWrite: ['PATCH', '/rk-1'] },
  { name: 'section review requests', mount: '/documents/:documentId/sections/:sectionId/review-requests', gate: documentSubrouteGates.reviewRequests, read: ['GET', '/'], readerWrite: ['POST', '/'] },
  { name: 'section reviews', mount: '/documents/:documentId/sections/:sectionId/reviews', gate: documentSubrouteGates.reviews, read: ['GET', '/'], readerWrite: ['POST', '/'] },
  { name: 'document reviews', mount: '/documents/:documentId/reviews', gate: documentSubrouteGates.reviews, read: ['GET', '/'] },
  { name: 'section items', mount: '/documents/:documentId/sections/:sectionId/items', gate: documentSubrouteGates.sectionItems, read: ['GET', '/'], write: ['PATCH', '/it-1'], readerWrite: ['POST', '/it-1/answer'] },
  { name: 'work items', mount: '/documents/:documentId/work-items', gate: documentSubrouteGates.workItems, read: ['GET', '/verdicts'], write: ['POST', '/wi-1/propose'], readerWrite: ['POST', '/'] },
  { name: 'work', mount: '/documents/:documentId/work', gate: documentSubrouteGates.work, read: ['GET', '/'], write: ['PATCH', '/'] },
  { name: 'run drafts', mount: '/documents/:documentId/run-drafts', gate: documentSubrouteGates.runDrafts, read: ['GET', '/d-1'], write: ['POST', '/d-1/dispatch'] },
  { name: 'export / import', mount: '/documents/:documentId', gate: express.Router({ mergeParams: true }).get('/export', requireDocumentRead()).post('/import', requireDocumentEdit()), read: ['GET', '/export'], write: ['POST', '/import'] },
];

describe.each(cases)('$name', ({ mount, gate, read, write, readerWrite }) => {
  const app = appWith(mount, gate);
  const url = (id: string, sub: string) => mount.replace(':documentId', id).replace(':sectionId', 's-1') + (sub === '/' ? '' : sub);

  it('the owner and an editor may read and write', async () => {
    for (const userId of ['dev-owner', 'dev-grace']) {
      expect((await call(app, read[0], url('doc-1', read[1]), { userId })).statusCode).toBe(200);
      if (write) expect((await call(app, write[0], url('doc-1', write[1]), { userId })).statusCode).toBe(200);
    }
  });
  it('a reader may read (and do the reader writes) but a write is 403', async () => {
    expect((await call(app, read[0], url('doc-1', read[1]), { userId: 'dev-reader' })).statusCode).toBe(200);
    if (readerWrite) expect((await call(app, readerWrite[0], url('doc-1', readerWrite[1]), { userId: 'dev-reader' })).statusCode).toBe(200);
    if (write) {
      const res = await call(app, write[0], url('doc-1', write[1]), { userId: 'dev-reader' });
      expect(res.statusCode).toBe(403);
      expect(res.body).toMatchObject({ code: 'forbidden', documentId: 'doc-1' });
    }
  });
  it('a stranger is 404 and no bearer 401', async () => {
    expect((await call(app, read[0], url('doc-private', read[1]), { userId: 'dev-owner' })).statusCode).toBe(404);
    if (write) expect((await call(app, write[0], url('doc-private', write[1]), { userId: 'dev-owner' })).statusCode).toBe(404);
    expect((await call(app, read[0], url('doc-1', read[1]), { anonymous: true })).statusCode).toBe(401);
  });
});

describe('approvals: body / query documentId takes document:read', () => {
  const stub = express.Router();
  stub.post('/', requireDocumentRead(bodyId('documentId'), { optional: true }), (_q, r) => { r.status(201).json({}); });
  stub.get('/', requireDocumentRead((q: Request) => q.query.documentId, { optional: true }), (_q, r) => { r.status(200).json({}); });
  const app = express(); app.use(express.json()); app.use('/approvals', stub); app.use(errorHandler);
  it('reader 201/200; stranger 404; anonymous 401', async () => {
    expect((await call(app, 'POST', '/approvals', { userId: 'dev-reader', body: { documentId: 'doc-1' } })).statusCode).toBe(201);
    expect((await call(app, 'GET', '/approvals?documentId=doc-1', { userId: 'dev-reader' })).statusCode).toBe(200);
    expect((await call(app, 'POST', '/approvals', { userId: 'dev-owner', body: { documentId: 'doc-private' } })).statusCode).toBe(404);
    expect((await call(app, 'GET', '/approvals?documentId=doc-private', { userId: 'dev-owner' })).statusCode).toBe(404);
    expect((await call(app, 'GET', '/approvals?documentId=doc-1', { anonymous: true })).statusCode).toBe(401);
  });
});

function reqOf(userId: string | null, body: Record<string, unknown>, params: Record<string, string> = {}): Request {
  return { headers: userId ? { authorization: 'Bearer t' } : {}, user: userId ? { sub: userId } : undefined, body, params, path: '/' } as unknown as Request;
}

describe('deck sources: each document source takes document:read, a recording its participant', () => {
  const deps = {
    assertCanRead: (req: Request, id: string) => mockGate.assertCanRead(req, id),
    isParticipant: async (u: string, r: string) => u === 'dev-owner' && r === 'rec-1',
    recordingOfTranscript: async (t: string) => (t === 'transcript:rec-1' ? 'rec-1' : null),
  };
  it('readable sources pass; someone else\'s page, recording or transcript is 404; no bearer 401', async () => {
    await expect(assertDeckSourcesReadable(reqOf('dev-reader', { sources: [{ kind: 'document', id: 'doc-1' }] }), deps)).resolves.toBeUndefined();
    await expect(assertDeckSourcesReadable(reqOf('dev-owner', { sources: [{ kind: 'recording', id: 'rec-1' }, { kind: 'transcript', id: 'transcript:rec-1' }] }), deps)).resolves.toBeUndefined();
    await expect(assertDeckSourcesReadable(reqOf('dev-owner', { sources: [{ kind: 'document', id: 'doc-private' }] }), deps)).rejects.toMatchObject({ status: 404 });
    await expect(assertDeckSourcesReadable(reqOf('dev-reader', { from: { recordingId: 'rec-1' } }), deps)).rejects.toMatchObject({ status: 404 });
    await expect(assertDeckSourcesReadable(reqOf('dev-reader', { sources: [{ kind: 'transcript', id: 'transcript:rec-1' }] }), deps)).rejects.toMatchObject({ status: 404 });
    await expect(assertDeckSourcesReadable(reqOf('dev-owner', { from: { documentId: 'doc-private' } }), deps)).rejects.toMatchObject({ status: 404 });
    await expect(assertDeckSourcesReadable(reqOf(null, { sources: [{ kind: 'document', id: 'doc-1' }] }), deps)).rejects.toMatchObject({ status: 401 });
  });
});

describe('chat commands: documentId edit, sources read, /summarize channel member', () => {
  const deps = { gate: mockGate, isParticipant: async () => false, isMember: async (c: string, u: string) => c === 'chat:room:team' && u !== 'dev-frank' };
  const run = (userId: string | null, body: Record<string, unknown>, trigger = 'doc') => assertChatCommandAccess(reqOf(userId, body, { trigger }), { ...deps, gate: mockGate });
  it('an editor runs /doc; a reader is 403; a stranger 404; no bearer 401', async () => {
    await expect(run('dev-grace', { documentId: 'doc-1' })).resolves.toBeUndefined();
    await expect(run('dev-reader', { documentId: 'doc-1' })).rejects.toMatchObject({ status: 403 });
    await expect(run('dev-owner', { documentId: 'doc-private' })).rejects.toMatchObject({ status: 404 });
    await expect(run(null, { documentId: 'doc-1' })).rejects.toMatchObject({ status: 401 });
  });
  it('a new page with no row yet is allowed (the command creates it)', async () => {
    await expect(run('dev-owner', { documentId: 'doc-new' }, 'transform')).resolves.toBeUndefined();
  });
  it('/transform\'s source and /deck\'s documents take read; a recording its participant', async () => {
    await expect(run('dev-owner', { documentId: 'doc-new', extra: { sourceDocumentId: 'doc-private' } }, 'transform')).rejects.toMatchObject({ status: 404 });
    await expect(run('dev-reader', { documentId: 'doc-new', extra: { sourceDocumentId: 'doc-1' } }, 'transform')).resolves.toBeUndefined();
    await expect(run('dev-owner', { documentId: 'doc-new', extra: { documents: [{ id: 'doc-1' }, { documentId: 'doc-private' }] } }, 'deck')).rejects.toMatchObject({ status: 404 });
    await expect(run('dev-owner', { documentId: 'doc-new', extra: { recordingId: 'rec-9' } }, 'deck')).rejects.toMatchObject({ status: 404 });
  });
  it('/summarize reads the channel only for its members', async () => {
    await expect(run('dev-owner', { channel: 'chat:room:team', documentId: 'doc-new' }, 'summarize')).resolves.toBeUndefined();
    await expect(run('dev-owner', { channel: 'chat:dm:dev-frank:dev-x', documentId: 'doc-new' }, 'summarize')).rejects.toMatchObject({ status: 404 });
  });
});

describe('automations: creating or running a doc bot takes document:edit', () => {
  const r = express.Router();
  r.post('/:documentId/automations', requireDocumentEdit(), (_q, res) => { res.status(201).json({}); });
  r.post('/:documentId/automations/:automationId/run', requireDocumentEdit(), (_q, res) => { res.status(202).json({}); });
  const app = express(); app.use(express.json()); app.use('/documents', r); app.use(errorHandler);
  it('editor allowed; reader 403; stranger 404; anonymous 401', async () => {
    expect((await call(app, 'POST', '/documents/doc-1/automations', { userId: 'dev-grace' })).statusCode).toBe(201);
    expect((await call(app, 'POST', '/documents/doc-1/automations/a-1/run', { userId: 'dev-owner' })).statusCode).toBe(202);
    expect((await call(app, 'POST', '/documents/doc-1/automations', { userId: 'dev-reader' })).statusCode).toBe(403);
    expect((await call(app, 'POST', '/documents/doc-private/automations/a-1/run', { userId: 'dev-owner' })).statusCode).toBe(404);
    expect((await call(app, 'POST', '/documents/doc-1/automations', { anonymous: true })).statusCode).toBe(401);
  });
});
