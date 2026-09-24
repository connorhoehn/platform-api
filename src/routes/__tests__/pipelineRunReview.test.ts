// POST /pipelines/:pipelineId/runs/:runId/review — records a person's verdict
// on a run's suggestions, optionally landing it through the review pipeline.
import express, { type NextFunction, type Request, type Response } from 'express';

const mockGet = jest.fn();
const mockSetReview = jest.fn();
jest.mock('../../lib/ddb-client', () => ({ getDynamoClient: jest.fn().mockReturnValue({}) }));
jest.mock('../../repositories/PipelineRunRepository', () => ({
  PipelineRunRepository: jest.fn().mockImplementation(() => ({ get: mockGet, setReview: mockSetReview })),
}));
const mockLogger = { info: jest.fn(), warn: jest.fn(), error: jest.fn(), child: () => mockLogger };
jest.mock('../../lib/logger', () => ({ __esModule: true, default: mockLogger, withContext: () => mockLogger }));
const mockTrigger = jest.fn();
jest.mock('../../pipeline/bridge-types', () => ({ getPipelineBridge: jest.fn() }));
const mockPublish = jest.fn().mockResolvedValue(undefined);
const mockRegisterTrigger = jest.fn();
const mockPersistenceSetReview = jest.fn();
jest.mock('../../pipeline/runPersistence', () => ({ getRunPersistenceService: jest.fn() }));
// NFR #171: the real gate over a mocked row — owner dev-owner, editor dev-grace, viewer dev-reader.
const mockDocRow = jest.fn();
let mockGate: import('../../documents/documentReadGate').DocumentReadGate;
jest.mock('../../documents/documentReadGate', () => {
  const actual = jest.requireActual('../../documents/documentReadGate');
  return { ...actual, documentReadGate: () => mockGate };
});
const mockCommentForRun = jest.fn();
jest.mock('../../documents/commentAgentRequests', () => ({ commentForRun: (...a: unknown[]) => mockCommentForRun(...a) }));

import { pipelineRunReviewRouter, RUN_REVIEWED_EVENT } from '../pipelineRunReview';
import { errorHandler } from '../../middleware/error-handler';
import { getPipelineBridge } from '../../pipeline/bridge-types';
import { getRunPersistenceService } from '../../pipeline/runPersistence';
import { DOCUMENT_AGENT_REVIEW_PIPELINE_ID } from '../../documents/documentAgentPipeline';
import { makeDocumentReadGate } from '../../documents/documentReadGate';

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
async function post(app: express.Express, url: string, body: unknown, opts: { userId?: string; anonymous?: boolean } = {}): Promise<MockRes> {
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
function buildApp(): express.Express {
  const app = express();
  app.use(express.json());
  // Mirrors pipelineTriggers.ts: the review router sits inside the runs router, mounted at /pipelines/:pipelineId/runs.
  const runs = express.Router({ mergeParams: true });
  runs.use(pipelineRunReviewRouter);
  app.use('/pipelines/:pipelineId/runs', runs);
  app.use(errorHandler);
  return app;
}

const URL = '/pipelines/document-agent-edit/runs/run-1/review';
const good = { decision: 'reject', suggestionKey: 'agent:haiku@1788000000000', documentId: 'doc-1', byName: 'Grace', comment: 'not needed' };

beforeEach(() => {
  jest.clearAllMocks();
  mockDocRow.mockResolvedValue({ documentId: 'doc-1', createdBy: 'dev-owner', editors: ['dev-grace'], viewers: ['dev-reader'] });
  mockGate = makeDocumentReadGate({ getDocumentRow: mockDocRow, getLifecycleOwner: async () => undefined });
  mockCommentForRun.mockResolvedValue(null);
  mockGet.mockResolvedValue({ pipelineId: 'document-agent-edit', runId: 'run-1', status: 'completed' });
  mockSetReview.mockResolvedValue(true);
  (getPipelineBridge as jest.Mock).mockReturnValue({ trigger: mockTrigger });
  mockTrigger.mockResolvedValue({ runId: 'review-run-9' });
  (getRunPersistenceService as jest.Mock).mockReturnValue({ getEventBus: () => ({ publish: mockPublish }), registerTrigger: mockRegisterTrigger, setReview: mockPersistenceSetReview });
});

describe('POST /pipelines/:pipelineId/runs/:runId/review', () => {
  const app = buildApp();

  it('records the verdict on the run and publishes pipeline:run:reviewed', async () => {
    const res = await post(app, URL, good);
    expect(res.statusCode).toBe(200);
    const body = res.body as { runId: string; pipelineId: string; review: Record<string, unknown>; applied?: unknown };
    expect(body.runId).toBe('run-1');
    expect(body.pipelineId).toBe('document-agent-edit');
    expect(body.review).toEqual({ decision: 'reject', suggestionKey: good.suggestionKey, documentId: 'doc-1', by: 'dev-grace', byName: 'Grace', at: expect.any(String), comment: 'not needed' });
    expect(body.applied).toBeUndefined();
    expect(mockSetReview).toHaveBeenCalledWith('document-agent-edit', 'run-1', body.review);
    expect(mockPersistenceSetReview).toHaveBeenCalledWith('run-1', body.review);
    expect(mockTrigger).not.toHaveBeenCalled();
    expect(mockPublish).toHaveBeenCalledWith(RUN_REVIEWED_EVENT, expect.objectContaining({ runId: 'run-1', pipelineId: 'document-agent-edit', decision: 'reject', by: 'dev-grace', byName: 'Grace', at: body.review.at }));
  });

  it('applyServerSide lands the decision through the document-agent-review pipeline and links the runs', async () => {
    const res = await post(app, URL, { ...good, decision: 'accept', applyServerSide: true });
    expect(res.statusCode).toBe(200);
    const body = res.body as { review: Record<string, unknown>; applied: unknown };
    expect(body.applied).toEqual({ runId: 'review-run-9', pipelineId: DOCUMENT_AGENT_REVIEW_PIPELINE_ID });
    expect(body.review.appliedByRunId).toBe('review-run-9');
    expect(mockTrigger).toHaveBeenCalledTimes(1);
    const args = mockTrigger.mock.calls[0]![0] as { pipelineId: string; definition: { nodes: Array<{ id: string; data: Record<string, unknown> }> }; triggerPayload: Record<string, unknown>; triggeredBy: { userId: string } };
    expect(args.pipelineId).toBe(DOCUMENT_AGENT_REVIEW_PIPELINE_ID);
    expect(args.triggeredBy).toEqual({ userId: 'dev-grace' });
    expect(args.triggerPayload).toMatchObject({ documentId: 'doc-1', reviewOps: [{ op: 'acceptSuggestion', key: good.suggestionKey }], reviewOf: 'run-1', requestedBy: 'dev-grace', requestedByName: 'Grace' });
    const apply = args.definition.nodes.find((n) => n.id === 'apply')!;
    expect(apply.data).toMatchObject({ type: 'document:apply', mode: 'direct', opsFrom: 'reviewOps' });
    expect(mockRegisterTrigger).toHaveBeenCalledWith(expect.objectContaining({ runId: 'review-run-9', pipelineId: DOCUMENT_AGENT_REVIEW_PIPELINE_ID, userId: 'dev-grace' }));
  });

  it('503 when applyServerSide is asked for and the engine is down; nothing is recorded', async () => {
    (getPipelineBridge as jest.Mock).mockReturnValue(null);
    const res = await post(app, URL, { ...good, applyServerSide: true });
    expect(res.statusCode).toBe(503);
    expect(mockSetReview).not.toHaveBeenCalled();
  });

  it('records without the bus when run persistence is off', async () => {
    (getRunPersistenceService as jest.Mock).mockReturnValue(null);
    const res = await post(app, URL, good);
    expect(res.statusCode).toBe(200);
    expect(mockSetReview).toHaveBeenCalledTimes(1);
    expect(mockPublish).not.toHaveBeenCalled();
  });

  // The margin and the card both reach this route, and they race: a person
  // resolves a suggestion in the gutter while the card beside it is still
  // offering both buttons. Whichever lands second must not be able to replace a
  // true record with a false one — and must not be thrown back at somebody who
  // pressed exactly what they were shown.
  describe('a run that has already been reviewed', () => {
    const held = { decision: 'reject', suggestionKey: good.suggestionKey, documentId: 'doc-1', by: 'dev-grace', byName: 'Grace', at: '2026-09-09T00:00:00.000Z' };

    it('refuses the OPPOSITE verdict, keeps the record, and starts no apply run', async () => {
      mockGet.mockResolvedValue({ pipelineId: 'document-agent-edit', runId: 'run-1', status: 'completed', review: held });
      const res = await post(app, URL, { ...good, decision: 'accept', applyServerSide: true });
      expect(res.statusCode).toBe(409);
      // The message says what actually happened to the suggestions, so the
      // client can show it instead of "run run-1 was already rejected".
      expect((res.body as { error: string }).error).toBe('These suggestions were already rejected by Grace, so they are no longer in the document.');
      expect(mockSetReview).not.toHaveBeenCalled();
      // The damage the old route did: a `document-agent-review` run whose
      // acceptSuggestion finds no marks, joins the document and leaves.
      expect(mockTrigger).not.toHaveBeenCalled();
    });

    it('answers the SAME verdict with the record that stands, rather than an error', async () => {
      mockGet.mockResolvedValue({ pipelineId: 'document-agent-edit', runId: 'run-1', status: 'completed', review: held });
      const res = await post(app, URL, { ...good, applyServerSide: true });
      expect(res.statusCode).toBe(200);
      expect((res.body as { review: unknown }).review).toEqual(held);
      // Nothing is rewritten and nothing is sent in to re-resolve marks that
      // the first reviewer already resolved.
      expect(mockSetReview).not.toHaveBeenCalled();
      expect(mockTrigger).not.toHaveBeenCalled();
      expect(mockPublish).not.toHaveBeenCalled();
    });
  });

  it('404 for an unknown run, 409 for a run already reviewed, 400 for a bad body', async () => {
    mockGet.mockResolvedValueOnce(null);
    expect((await post(app, URL, good)).statusCode).toBe(404);
    mockGet.mockResolvedValueOnce({ runId: 'run-1', review: { decision: 'accept', by: 'dev-bob', at: '2026-09-09T00:00:00.000Z' } });
    expect((await post(app, URL, good)).statusCode).toBe(409);
    expect((await post(app, URL, { ...good, decision: 'maybe' })).statusCode).toBe(400);
    expect((await post(app, URL, { ...good, suggestionKey: 'no-at-sign' })).statusCode).toBe(400);
    expect((await post(app, URL, { ...good, documentId: '' })).statusCode).toBe(400);
    expect(mockSetReview).not.toHaveBeenCalled();
  });

  // NFR #171: a verdict changes the document, so it takes document:edit.
  describe('who may give a verdict', () => {
    const accept = { ...good, decision: 'accept', applyServerSide: true };

    it('the owner and an editor may accept', async () => {
      expect((await post(app, URL, accept, { userId: 'dev-owner' })).statusCode).toBe(200);
      mockGet.mockResolvedValue({ pipelineId: 'document-agent-edit', runId: 'run-1', status: 'completed' });
      expect((await post(app, URL, accept, { userId: 'dev-grace' })).statusCode).toBe(200);
      expect(mockSetReview).toHaveBeenCalledTimes(2);
    });

    it('a reader is refused 403 forbidden: nothing recorded, no apply run, no event', async () => {
      const res = await post(app, URL, accept, { userId: 'dev-reader' });
      expect(res.statusCode).toBe(403);
      expect(res.body).toMatchObject({ code: 'forbidden', documentId: 'doc-1' });
      expect((res.body as { error: string }).error).toMatch(/view this document but not change it/);
      expect(mockGet).not.toHaveBeenCalled();
      expect(mockTrigger).not.toHaveBeenCalled();
      expect(mockSetReview).not.toHaveBeenCalled();
      expect(mockPublish).not.toHaveBeenCalled();
    });

    it('a stranger gets 404, the same as a missing document; a missing document is 404', async () => {
      expect((await post(app, URL, accept, { userId: 'dev-frank' })).statusCode).toBe(404);
      mockDocRow.mockResolvedValue(null);
      expect((await post(app, URL, accept, { userId: 'dev-owner' })).statusCode).toBe(404);
      expect(mockSetReview).not.toHaveBeenCalled();
      expect(mockTrigger).not.toHaveBeenCalled();
    });

    it('no bearer is 401', async () => {
      expect((await post(app, URL, accept, { anonymous: true })).statusCode).toBe(401);
      expect(mockSetReview).not.toHaveBeenCalled();
    });

    it('a comment run is bound to its comment\'s document: a verdict naming another document is 404', async () => {
      mockCommentForRun.mockResolvedValue({ documentId: 'doc-someone-elses', commentId: 'c-1', pipelineId: 'document-agent-edit' });
      const res = await post(app, URL, accept, { userId: 'dev-owner' });
      expect(res.statusCode).toBe(404);
      expect(mockSetReview).not.toHaveBeenCalled();
      expect(mockTrigger).not.toHaveBeenCalled();
    });

    it('an editor\'s accept on a comment run still records and publishes pipeline:run:reviewed (the resolve path)', async () => {
      mockCommentForRun.mockResolvedValue({ documentId: 'doc-1', commentId: 'c-1', pipelineId: 'document-agent-edit' });
      const res = await post(app, URL, { ...good, decision: 'accept' }, { userId: 'dev-grace' });
      expect(res.statusCode).toBe(200);
      expect(mockPublish).toHaveBeenCalledWith(RUN_REVIEWED_EVENT, expect.objectContaining({ runId: 'run-1', decision: 'accept', by: 'dev-grace' }));
    });
  });
});
