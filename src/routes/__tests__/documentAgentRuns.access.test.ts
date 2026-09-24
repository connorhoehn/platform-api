// Loop 29c: the three "new page" runs on /documents/:id. transform-run takes
// document:edit on the target and document:read on `sourceDocumentId`;
// summarize-run takes edit + membership of `chatChannel`; diagram-run edit.
import express, { type NextFunction, type Request, type Response } from 'express';

jest.mock('../../middleware/auth', () => ({ requireAuth: (_q: Request, _s: Response, next: NextFunction) => next() }));
const mockLogger = { info: jest.fn(), warn: jest.fn(), error: jest.fn(), debug: jest.fn(), child: () => mockLogger };
jest.mock('../../lib/logger', () => ({ __esModule: true, default: mockLogger, withContext: () => mockLogger }));
const mockTrigger = jest.fn();
jest.mock('../../pipeline/bridge-types', () => ({ getPipelineBridge: () => ({ trigger: mockTrigger }) }));
jest.mock('../../pipeline/runPersistence', () => ({ getRunPersistenceService: () => null }));
jest.mock('../../repositories', () => ({ documentCommentRepo: {} }));
jest.mock('../../lib/event-catalog', () => ({ publishSignal: jest.fn(() => Promise.resolve()) }));
jest.mock('../../documents/commentAgentRequests', () => ({ commentScopedInstruction: (t: string) => t, linkRunToComment: jest.fn() }));
jest.mock('../../documents/commentAgentResolver', () => ({ getCommentAgentResolver: () => null }));
const mockMember = jest.fn();
jest.mock('../../deck/documentEditAccess', () => {
  const actual = jest.requireActual('../../deck/documentEditAccess');
  return { ...actual, isChannelMember: (c: string, u: string) => mockMember(c, u) };
});
let mockGate: import('../../documents/documentReadGate').DocumentReadGate;
jest.mock('../../documents/documentReadGate', () => {
  const actual = jest.requireActual('../../documents/documentReadGate');
  return { ...actual, documentReadGate: () => mockGate };
});

import { documentAgentEditRouter } from '../documentAgentEdit';
import { makeDocumentReadGate } from '../../documents/documentReadGate';
import { errorHandler } from '../../middleware/error-handler';
import { call } from './helpers/accessHarness';

const ROWS: Record<string, Record<string, unknown>> = {
  'new-1': { documentId: 'new-1', createdBy: 'dev-owner', editors: ['dev-grace'], viewers: ['dev-reader'] },
  'src-1': { documentId: 'src-1', createdBy: 'dev-owner', viewers: ['dev-reader'] },
  'src-private': { documentId: 'src-private', createdBy: 'dev-frank' },
};
const app = express();
app.use(express.json());
app.use('/documents', documentAgentEditRouter);
app.use(errorHandler);

beforeEach(() => {
  jest.clearAllMocks();
  mockGate = makeDocumentReadGate({ getDocumentRow: async (id) => (ROWS[id] as never) ?? null, getLifecycleOwner: async () => undefined });
  mockTrigger.mockResolvedValue({ runId: 'run-1' });
  mockMember.mockImplementation(async (c: string, u: string) => c === 'chat:room:team' && u !== 'dev-frank');
});

const transform = (sourceDocumentId: string) => ({ sourceDocumentId, target: { name: 'Bug Report', fields: [] } });

describe('POST /documents/:documentId/transform-run', () => {
  it('owner and editor of the target who can read the source start a run (202)', async () => {
    expect((await call(app, 'POST', '/documents/new-1/transform-run', { userId: 'dev-owner', body: transform('src-1') })).statusCode).toBe(202);
    ROWS['src-2'] = { documentId: 'src-2', createdBy: 'dev-grace' };
    expect((await call(app, 'POST', '/documents/new-1/transform-run', { userId: 'dev-grace', body: transform('src-2') })).statusCode).toBe(202);
    expect(mockTrigger).toHaveBeenCalledTimes(2);
  });
  it('a source the caller cannot read is 404 and no run starts (the hole)', async () => {
    const res = await call(app, 'POST', '/documents/new-1/transform-run', { userId: 'dev-owner', body: transform('src-private') });
    expect(res.statusCode).toBe(404);
    expect(mockTrigger).not.toHaveBeenCalled();
  });
  it('a reader of the target is 403, a stranger 404, no bearer 401', async () => {
    const reader = await call(app, 'POST', '/documents/new-1/transform-run', { userId: 'dev-reader', body: transform('src-1') });
    expect(reader.statusCode).toBe(403);
    expect(reader.body).toMatchObject({ code: 'forbidden', documentId: 'new-1' });
    expect((await call(app, 'POST', '/documents/new-1/transform-run', { userId: 'dev-frank', body: transform('src-private') })).statusCode).toBe(404);
    expect((await call(app, 'POST', '/documents/new-1/transform-run', { anonymous: true, body: transform('src-1') })).statusCode).toBe(401);
    expect(mockTrigger).not.toHaveBeenCalled();
  });
  it('a scoped grant for the target does not read another document as the source', async () => {
    const gate = makeDocumentReadGate({ getDocumentRow: async (id) => (ROWS[id] as never) ?? null, getLifecycleOwner: async () => undefined });
    const req = { headers: {}, path: '/', documentGrant: { documentId: 'new-1', operations: ['read', 'edit'] } } as never;
    await expect(gate.assertCanRead(req, 'new-1')).resolves.toEqual({});
    await expect(gate.assertCanRead(req, 'src-private')).rejects.toMatchObject({ status: 404 });
  });
});

describe('POST /documents/:documentId/summarize-run and diagram-run', () => {
  it('summarize: editor + channel member 202; non-member of the channel 404; reader 403; anonymous 401', async () => {
    const body = { chatChannel: 'chat:room:team' };
    expect((await call(app, 'POST', '/documents/new-1/summarize-run', { userId: 'dev-owner', body })).statusCode).toBe(202);
    expect((await call(app, 'POST', '/documents/new-1/summarize-run', { userId: 'dev-owner', body: { chatChannel: 'chat:room:secret' } })).statusCode).toBe(404);
    expect((await call(app, 'POST', '/documents/new-1/summarize-run', { userId: 'dev-reader', body })).statusCode).toBe(403);
    expect((await call(app, 'POST', '/documents/new-1/summarize-run', { userId: 'dev-frank', body })).statusCode).toBe(404);
    expect((await call(app, 'POST', '/documents/new-1/summarize-run', { anonymous: true, body })).statusCode).toBe(401);
    expect(mockTrigger).toHaveBeenCalledTimes(1);
  });
  it('diagram: editor 202; reader 403; stranger 404; anonymous 401', async () => {
    const body = { prompt: 'a login flow' };
    expect((await call(app, 'POST', '/documents/new-1/diagram-run', { userId: 'dev-grace', body })).statusCode).toBe(202);
    expect((await call(app, 'POST', '/documents/new-1/diagram-run', { userId: 'dev-reader', body })).statusCode).toBe(403);
    expect((await call(app, 'POST', '/documents/new-1/diagram-run', { userId: 'dev-frank', body })).statusCode).toBe(404);
    expect((await call(app, 'POST', '/documents/new-1/diagram-run', { anonymous: true, body })).statusCode).toBe(401);
    expect(mockTrigger).toHaveBeenCalledTimes(1);
  });
});
