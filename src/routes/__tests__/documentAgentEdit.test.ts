// POST /documents/:documentId/agent-edit — asking the agent for an edit writes
// into the document (suggestions, or a direct edit after approval), so it takes
// #41's document:edit (NFR #171): owner/editor 202, reader 403, stranger 404,
// no bearer 401.
import express, { type NextFunction, type Request, type Response } from 'express';

jest.mock('../../middleware/auth', () => ({
  requireAuth: (_req: Request, _res: Response, next: NextFunction) => next(),
}));
const mockLogger = { info: jest.fn(), warn: jest.fn(), error: jest.fn(), debug: jest.fn(), child: () => mockLogger };
jest.mock('../../lib/logger', () => ({ __esModule: true, default: mockLogger, withContext: () => mockLogger }));
const mockTrigger = jest.fn();
jest.mock('../../pipeline/bridge-types', () => ({ getPipelineBridge: () => ({ trigger: mockTrigger }) }));
jest.mock('../../pipeline/runPersistence', () => ({ getRunPersistenceService: () => null }));
const mockGetComment = jest.fn();
const mockSetAgentRequest = jest.fn();
jest.mock('../../repositories', () => ({ documentCommentRepo: { getComment: (...a: unknown[]) => mockGetComment(...a), setAgentRequest: (...a: unknown[]) => mockSetAgentRequest(...a) } }));
jest.mock('../../lib/event-catalog', () => ({ publishSignal: jest.fn(() => Promise.resolve()) }));
jest.mock('../../documents/commentAgentRequests', () => ({
  commentScopedInstruction: (t: string) => t,
  linkRunToComment: jest.fn(async () => undefined),
}));
jest.mock('../../documents/commentAgentResolver', () => ({ getCommentAgentResolver: () => null }));
const mockDocRow = jest.fn();
let mockGate: import('../../documents/documentReadGate').DocumentReadGate;
jest.mock('../../documents/documentReadGate', () => {
  const actual = jest.requireActual('../../documents/documentReadGate');
  return { ...actual, documentReadGate: () => mockGate };
});

import { documentAgentEditRouter } from '../documentAgentEdit';
import { makeDocumentReadGate } from '../../documents/documentReadGate';
import { errorHandler } from '../../middleware/error-handler';

interface MockRes { statusCode: number; body: unknown; ended: boolean; finished: Promise<void>; status(c: number): MockRes; json(b: unknown): MockRes; setHeader(): MockRes; getHeader(): unknown; end(): MockRes }
function mockRes(): MockRes {
  let resolve!: () => void;
  const finished = new Promise<void>((r) => { resolve = r; });
  return {
    statusCode: 200, body: undefined, ended: false, finished,
    status(c) { this.statusCode = c; return this; },
    json(b) { this.body = b; this.ended = true; resolve(); return this; },
    setHeader() { return this; }, getHeader() { return undefined; },
    end() { this.ended = true; resolve(); return this; },
  };
}
const app = express();
app.use(express.json());
app.use('/documents', documentAgentEditRouter);
app.use(errorHandler);

async function post(body: unknown, opts: { userId?: string; anonymous?: boolean } = {}): Promise<MockRes> {
  const url = '/documents/doc-1/agent-edit';
  const res = mockRes();
  const headers = opts.anonymous ? { 'content-type': 'application/json' } : { 'content-type': 'application/json', authorization: 'Bearer t' };
  const req = { method: 'POST', url, originalUrl: url, path: url, headers, query: {}, body, user: { sub: opts.userId ?? 'dev-grace' }, params: {} } as unknown as Request;
  await new Promise<void>((done, reject) => {
    (app as unknown as (req: Request, res: Response, next: NextFunction) => void)(req, res as unknown as Response, (err?: unknown) => {
      if (err) { reject(err); return; }
      if (!res.ended) res.status(404).json({ error: 'route not found' });
      done();
    });
    res.finished.then(() => done()).catch(reject);
  });
  return res;
}

beforeEach(() => {
  jest.clearAllMocks();
  mockDocRow.mockResolvedValue({ documentId: 'doc-1', createdBy: 'dev-owner', editors: ['dev-grace'], viewers: ['dev-reader'] });
  mockGate = makeDocumentReadGate({ getDocumentRow: mockDocRow, getLifecycleOwner: async () => undefined });
  mockTrigger.mockResolvedValue({ runId: 'run-1' });
  mockGetComment.mockResolvedValue({ documentId: 'doc-1', commentId: 'c-1', text: 'x' });
  mockSetAgentRequest.mockResolvedValue({ documentId: 'doc-1', commentId: 'c-1' });
});

describe('POST /documents/:documentId/agent-edit — document:edit', () => {
  const ask = { instruction: 'Tighten the intro' };

  it('the owner and an editor start a run (202)', async () => {
    expect((await post(ask, { userId: 'dev-owner' })).statusCode).toBe(202);
    expect((await post({ ...ask, commentId: 'c-1' }, { userId: 'dev-grace' })).statusCode).toBe(202);
    expect(mockTrigger).toHaveBeenCalledTimes(2);
  });

  it('a reader is refused 403 forbidden, with or without a comment, and no run starts', async () => {
    for (const body of [ask, { ...ask, commentId: 'c-1' }]) {
      const res = await post(body, { userId: 'dev-reader' });
      expect(res.statusCode).toBe(403);
      expect(res.body).toMatchObject({ code: 'forbidden', documentId: 'doc-1' });
    }
    expect(mockTrigger).not.toHaveBeenCalled();
    expect(mockSetAgentRequest).not.toHaveBeenCalled();
  });

  it('a stranger gets 404 and no run starts', async () => {
    expect((await post(ask, { userId: 'dev-frank' })).statusCode).toBe(404);
    expect(mockTrigger).not.toHaveBeenCalled();
  });

  it('no bearer is 401', async () => {
    expect((await post(ask, { anonymous: true })).statusCode).toBe(401);
    expect(mockTrigger).not.toHaveBeenCalled();
  });
});

describe('documentReadGate.assertCanEdit', () => {
  const req = (sub: string) => ({ user: { sub }, headers: { authorization: 'Bearer t' }, path: '/x' }) as never;

  it('caches an allow for 60 s and never a refusal', async () => {
    let t = 0;
    const gate = makeDocumentReadGate({ getDocumentRow: mockDocRow, getLifecycleOwner: async () => undefined, now: () => t });
    await gate.assertCanEdit(req('dev-grace'), 'doc-1');
    await gate.assertCanEdit(req('dev-grace'), 'doc-1');
    expect(mockDocRow).toHaveBeenCalledTimes(1);
    t = 60_001;
    await gate.assertCanEdit(req('dev-grace'), 'doc-1');
    expect(mockDocRow).toHaveBeenCalledTimes(2);
    await expect(gate.assertCanEdit(req('dev-reader'), 'doc-1')).rejects.toMatchObject({ status: 403 });
    mockDocRow.mockResolvedValue({ documentId: 'doc-1', createdBy: 'dev-owner', editors: ['dev-grace', 'dev-reader'] });
    await expect(gate.assertCanEdit(req('dev-reader'), 'doc-1')).resolves.toBeDefined();
  });

  it('a scoped grant must name this document and carry edit', async () => {
    const gate = makeDocumentReadGate({ getDocumentRow: mockDocRow, getLifecycleOwner: async () => undefined });
    const withGrant = (documentId: string, operations: string[]) => ({ user: { sub: 'u' }, headers: {}, path: '/x', documentGrant: { documentId, operations } }) as never;
    await expect(gate.assertCanEdit(withGrant('doc-1', ['read', 'edit']), 'doc-1')).resolves.toEqual({});
    await expect(gate.assertCanEdit(withGrant('doc-1', ['read', 'comment']), 'doc-1')).rejects.toMatchObject({ status: 403 });
    await expect(gate.assertCanEdit(withGrant('doc-2', ['edit']), 'doc-1')).rejects.toMatchObject({ status: 404 });
  });
});
