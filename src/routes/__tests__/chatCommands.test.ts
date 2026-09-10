// /api/chat/commands — discovery and the one run path, over the real registry
// (built-ins registered by the route module) with a stubbed bridge. Same
// hand-rolled Express runner as pipelineDefinitions.test.ts.

import express, { type NextFunction, type Request, type Response } from 'express';

const mockRegisterTrigger = jest.fn();
const mockPostCard = jest.fn().mockResolvedValue(true);
const mockTypeGet = jest.fn();
const mockTypeList = jest.fn();

jest.mock('../../pipeline/runPersistence', () => ({
  getRunPersistenceService: () => ({ registerTrigger: mockRegisterTrigger }),
}));
jest.mock('../../documents/chatRunCard', () => ({
  postRunCardToChannel: (...args: unknown[]) => mockPostCard(...args),
  str: (v: unknown) => (typeof v === 'string' && v ? v : undefined),
}));
jest.mock('../../repositories', () => ({
  ...jest.requireActual('../../repositories'),
  documentTypeRepo: { get: (...a: unknown[]) => mockTypeGet(...a), list: (...a: unknown[]) => mockTypeList(...a) },
}));
jest.mock('../../lib/logger', () => {
  const l = { info: jest.fn(), warn: jest.fn(), error: jest.fn(), debug: jest.fn(), child: () => l };
  return { __esModule: true, default: l, withContext: () => l };
});

import { chatCommandsRouter } from '../chatCommands';
import { errorHandler } from '../../middleware/error-handler';
import { setPipelineBridge, type PipelineBridge } from '../../pipeline/bridge-types';
import { registerChatCommandPipeline } from '../../chat/chatCommandRegistry';

interface MockRes {
  statusCode: number; body: unknown; ended: boolean; finished: Promise<void>;
  status(code: number): MockRes; json(body: unknown): MockRes; send(body?: unknown): MockRes;
  setHeader(_n: string, _v: unknown): MockRes; getHeader(_n: string): unknown; end(...args: unknown[]): MockRes;
}
function mockRes(): MockRes {
  let resolve!: () => void;
  const finished = new Promise<void>((r) => { resolve = r; });
  return {
    statusCode: 200, body: undefined, ended: false, finished,
    status(code) { this.statusCode = code; return this; },
    json(body) { this.body = body; this.ended = true; resolve(); return this; },
    send(body) { this.body = body; this.ended = true; resolve(); return this; },
    setHeader() { return this; }, getHeader() { return undefined; },
    end() { this.ended = true; resolve(); return this; },
  };
}
async function run(app: express.Express, method: string, url: string, body?: unknown): Promise<MockRes> {
  const res = mockRes();
  const req = {
    method: method.toUpperCase(), url, originalUrl: url, path: url,
    headers: { 'content-type': 'application/json' }, query: {}, body, params: {}, ip: '127.0.0.1',
    user: { sub: 'dev-grace' },
  } as unknown as Request;
  await new Promise<void>((resolveOuter, reject) => {
    const finalHandler = (err?: unknown): void => {
      if (err) { reject(err); return; }
      if (!res.ended) res.status(404).json({ error: 'route not found' });
      resolveOuter();
    };
    (app as unknown as (req: Request, res: Response, next: NextFunction) => void)(req, res as unknown as Response, finalHandler);
    res.finished.then(() => resolveOuter()).catch(reject);
  });
  return res;
}
function buildApp(): express.Express {
  const app = express();
  app.use(express.json());
  app.use('/api/chat/commands', chatCommandsRouter);
  app.use(errorHandler);
  return app;
}

let trigger: jest.Mock;
beforeEach(() => {
  mockRegisterTrigger.mockReset();
  mockPostCard.mockReset().mockResolvedValue(true);
  mockTypeGet.mockReset();
  mockTypeList.mockReset();
  trigger = jest.fn().mockResolvedValue({ runId: 'run-77' });
  setPipelineBridge({ trigger } as unknown as PipelineBridge);
});
afterEach(() => setPipelineBridge(null));

describe('GET /api/chat/commands', () => {
  it('lists the built-ins with what each one needs', async () => {
    const res = await run(buildApp(), 'GET', '/api/chat/commands');
    expect(res.statusCode).toBe(200);
    const commands = (res.body as { commands: Array<{ trigger: string; pipelineId: string; requires?: unknown }> }).commands;
    const byTrigger = Object.fromEntries(commands.map((c) => [c.trigger, c]));
    expect(byTrigger['/doc']).toMatchObject({ pipelineId: 'document-agent-edit', requires: { document: true, argumentText: true } });
    expect(byTrigger['/transform']).toMatchObject({ pipelineId: 'document-transform', requires: { document: true, argument: true, extra: ['sourceDocumentId'] } });
    expect(byTrigger['/summarize']).toMatchObject({ pipelineId: 'conversation-summarize', requires: { document: true } });
    expect(byTrigger['/diagram']).toMatchObject({ pipelineId: 'diagram-generate', requires: { document: true, argumentText: true } });
    for (const c of commands) expect(typeof (c as { description: string }).description).toBe('string');
  });

  it('lists a command an embedder registered', async () => {
    registerChatCommandPipeline({ trigger: '/triage', pipelineId: 'embedder-triage', description: 'Triage it.', buildDefinition: () => ({ id: 'embedder-triage', nodes: [], edges: [] }), triggerPayload: () => ({}) });
    const res = await run(buildApp(), 'GET', '/api/chat/commands');
    expect((res.body as { commands: Array<{ trigger: string }> }).commands.map((c) => c.trigger)).toContain('/triage');
  });
});

describe('POST /api/chat/commands/:trigger/run', () => {
  it('starts /summarize with the slash URL-encoded and answers 202 with the card', async () => {
    const res = await run(buildApp(), 'POST', '/api/chat/commands/%2Fsummarize/run', { channel: 'room:pipeline-live', text: '/summarize', documentId: 'doc-new', requestedByName: 'Grace' });
    expect(res.statusCode).toBe(202);
    expect(res.body).toEqual({
      runId: 'run-77', pipelineId: 'conversation-summarize',
      card: { kind: 'agent-run', command: '/summarize', instruction: 'Summarise room:pipeline-live', runId: 'run-77', pipelineId: 'conversation-summarize', documentId: 'doc-new', agent: { userId: 'agent:haiku', displayName: 'Haiku' } },
    });
    const args = trigger.mock.calls[0][0];
    expect(args.pipelineId).toBe('conversation-summarize');
    expect(args.triggerPayload).toMatchObject({ documentId: 'doc-new', chatChannel: 'room:pipeline-live', limit: 200, requestedBy: 'dev-grace', requestedByName: 'Grace' });
    expect(args.triggeredBy).toEqual({ userId: 'dev-grace' });
    expect(mockRegisterTrigger).toHaveBeenCalledWith(expect.objectContaining({ runId: 'run-77', pipelineId: 'conversation-summarize', userId: 'dev-grace' }));
    // The built-ins leave the card to the client / the pipeline's own last step.
    expect(mockPostCard).not.toHaveBeenCalled();
  });

  it('accepts the trigger without its slash and derives argumentText from the text', async () => {
    const res = await run(buildApp(), 'POST', '/api/chat/commands/doc/run', { channel: 'room:r', text: '/doc add a title', documentId: 'doc-1' });
    expect(res.statusCode).toBe(202);
    expect(trigger.mock.calls[0][0].triggerPayload).toMatchObject({ documentId: 'doc-1', instruction: 'add a title', chatChannel: 'room:r' });
    // The requester approves a big edit.
    const approve = trigger.mock.calls[0][0].definition.nodes.find((n: { id: string }) => n.id === 'approve');
    expect(approve.data.approvers).toEqual([{ type: 'user', value: 'dev-grace', userId: 'dev-grace' }]);
  });

  it('/transform looks the target type up by name and carries the source page', async () => {
    mockTypeList.mockResolvedValue([{ typeId: 't-rfc', name: 'RFC', icon: 'R', fields: [{ fieldId: 'f1', name: 'Context' }, { fieldId: 'f2', name: 'Decision', sectionType: 'decision' }] }]);
    const res = await run(buildApp(), 'POST', '/api/chat/commands/transform/run', {
      channel: 'room:r', text: '/transform rfc', documentId: 'doc-new', argument: { id: '', label: 'rfc' }, extra: { sourceDocumentId: 'doc-src' },
    });
    expect(res.statusCode).toBe(202);
    expect(trigger.mock.calls[0][0].triggerPayload).toMatchObject({
      documentId: 'doc-new', sourceDocumentId: 'doc-src', targetName: 'RFC', targetId: 't-rfc', targetIcon: 'R',
      targetFieldList: '- Context\n- Decision (decision)', instruction: 'Transform into RFC',
    });
    expect((res.body as { card: Record<string, unknown> }).card).toMatchObject({ command: '/transform', targetName: 'RFC', sourceDocumentId: 'doc-src' });
  });

  it('/transform with a type nobody has is a 404', async () => {
    mockTypeList.mockResolvedValue([]);
    const res = await run(buildApp(), 'POST', '/api/chat/commands/transform/run', {
      channel: 'room:r', text: '/transform nope', documentId: 'doc-new', argument: { id: '', label: 'nope' }, extra: { sourceDocumentId: 'doc-src' },
    });
    expect(res.statusCode).toBe(404);
    expect(trigger).not.toHaveBeenCalled();
  });

  it('404s an unknown trigger with the known ones', async () => {
    const res = await run(buildApp(), 'POST', '/api/chat/commands/nope/run', { channel: 'room:r', text: '/nope' });
    expect(res.statusCode).toBe(404);
    expect((res.body as { known: string[] }).known).toEqual(expect.arrayContaining(['/doc', '/transform', '/summarize', '/diagram']));
  });

  it('400s with the requirements when they are not met', async () => {
    const res = await run(buildApp(), 'POST', '/api/chat/commands/diagram/run', { channel: 'room:r', text: '/diagram' });
    expect(res.statusCode).toBe(400);
    expect(res.body).toEqual({ error: '/diagram needs documentId, argumentText', requires: { document: true, argumentText: true } });
    expect(trigger).not.toHaveBeenCalled();
  });

  it('400s without a channel', async () => {
    const res = await run(buildApp(), 'POST', '/api/chat/commands/summarize/run', { text: '/summarize', documentId: 'd' });
    expect(res.statusCode).toBe(400);
  });

  it('503s when the pipeline engine is not wired', async () => {
    setPipelineBridge(null);
    const res = await run(buildApp(), 'POST', '/api/chat/commands/summarize/run', { channel: 'room:r', text: '/summarize', documentId: 'd' });
    expect(res.statusCode).toBe(503);
  });
});
