// POST /api/deck/revise-run: a composer edit starts a `deck-revise` run and
// answers with its id at once; the same requestId never starts a second run.
import express from 'express';
import http from 'http';
import type { AddressInfo } from 'net';

jest.mock('../../lib/aws-clients', () => ({ docClient: { send: jest.fn() }, ddbClient: { send: jest.fn() } }));
jest.mock('../../lib/ddb-table-name', () => ({ tableName: (name: string) => `test-${name}` }));
jest.mock('../../lib/redis-client', () => ({ getRedisClient: async () => null }));
jest.mock('../../repositories', () => ({
  workSourceAccountRepo: { ensureVerifiedPrincipal: jest.fn(async () => undefined), ensureVerifiedSourcePrincipal: jest.fn(async () => undefined) },
}));
const registerTrigger = jest.fn();
jest.mock('../../pipeline/runPersistence', () => ({ getRunPersistenceService: () => ({ registerTrigger }) }));

import { errorHandler } from '../../middleware/error-handler';
import { deckRouter } from '../deck';
import { setPipelineBridge } from '../../pipeline/bridge-types';
import { setPresentationDocumentStore, type PresentationDocumentRow } from '../../deck/presentationDocuments';
import { setClaimStore } from '../../deck/deckReviseRequests';
import { sampleDeck } from '../../deck/__tests__/fixtures';

const deck = sampleDeck();
const rows = new Map<string, PresentationDocumentRow>();
const trigger = jest.fn(async (_args: Record<string, unknown>) => ({ runId: `run-${trigger.mock.calls.length}` }));

function post(body: unknown): Promise<{ status: number; body: Record<string, unknown> }> {
  const { port } = server.address() as AddressInfo;
  const payload = Buffer.from(JSON.stringify(body), 'utf8');
  return new Promise((resolve, reject) => {
    const req = http.request({ host: '127.0.0.1', port, method: 'POST', path: '/api/deck/revise-run', headers: { 'content-type': 'application/json', 'content-length': String(payload.length) } }, (res) => {
      const chunks: Buffer[] = [];
      res.on('data', (chunk: Buffer) => chunks.push(chunk));
      res.on('end', () => { const text = Buffer.concat(chunks).toString('utf8'); resolve({ status: res.statusCode ?? 0, body: text ? JSON.parse(text) : {} }); });
    });
    req.on('error', reject);
    req.write(payload);
    req.end();
  });
}

let server: http.Server;
const originalSkipAuth = process.env.SKIP_AUTH;

beforeAll(async () => {
  process.env.SKIP_AUTH = 'true';
  setPresentationDocumentStore({ get: async (id: string) => rows.get(id) ?? null, appendRevision: jest.fn() } as never);
  setPipelineBridge({ trigger } as never);
  const app = express();
  app.use(express.json({ limit: '4mb' }));
  app.use('/api/deck', deckRouter);
  app.use(errorHandler);
  server = http.createServer(app);
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
});

afterAll(async () => {
  if (originalSkipAuth === undefined) delete process.env.SKIP_AUTH; else process.env.SKIP_AUTH = originalSkipAuth;
  setPipelineBridge(null);
  setPresentationDocumentStore(null);
  await new Promise<void>((resolve) => server.close(() => resolve()));
});

beforeEach(() => {
  trigger.mockClear();
  registerTrigger.mockClear();
  setClaimStore(null);
  rows.clear();
  rows.set('audit-deck-1', { documentId: 'audit-deck-1', type: 'presentation', revisions: [{ id: 'v1', label: 'v1', spec: deck } as never] });
});

describe('POST /api/deck/revise-run', () => {
  it('starts a deck-revise run and answers 202 with its id and channels', async () => {
    const slideId = deck.slides[1]!.id;
    const res = await post({ documentId: 'audit-deck-1', instruction: 'Tighten', slideId, target: { field: 'title' }, requestId: 'r-1', baseRevisionId: 'v1' });
    expect(res.status).toBe(202);
    expect(res.body).toMatchObject({
      runId: 'run-1', pipelineId: 'deck-revise', documentId: 'audit-deck-1', requestId: 'r-1', replay: false,
      channels: { document: 'pipeline:run:deck-revise:audit-deck-1', run: 'pipeline:run:run-1' },
    });
    const args = trigger.mock.calls[0]![0] as { pipelineId: string; triggerPayload: Record<string, unknown>; definition: { nodes: Array<{ id: string }> } };
    expect(args.pipelineId).toBe('deck-revise');
    expect(args.definition.nodes.map((node) => node.id)).toEqual(['trigger', 'load', 'propose', 'write']);
    expect(args.triggerPayload).toMatchObject({ documentId: 'audit-deck-1', instruction: 'Tighten', requestId: 'r-1', slideId, target: { field: 'title' }, baseRevisionId: 'v1' });
    expect(args.triggerPayload.spec).toBeUndefined();
    // The run is projected into the work graph as its steps, named after the deck.
    expect(registerTrigger).toHaveBeenCalledWith(expect.objectContaining({
      runId: 'run-1', pipelineId: 'deck-revise',
      workGraph: expect.objectContaining({ title: 'Edit presentation', steps: expect.arrayContaining([expect.objectContaining({ stepId: 'write' })]) }),
    }));
  });

  it('is idempotent by requestId: a retry answers the same run and starts nothing', async () => {
    const first = await post({ documentId: 'audit-deck-1', instruction: 'Tighten', requestId: 'r-2' });
    const again = await post({ documentId: 'audit-deck-1', instruction: 'Tighten', requestId: 'r-2' });
    expect(first.body.runId).toBe('run-1');
    expect(again.status).toBe(202);
    expect(again.body).toMatchObject({ runId: 'run-1', replay: true });
    expect(trigger).toHaveBeenCalledTimes(1);
  });

  it('refuses before starting anything: unknown deck 404, bad slide 400, no spec anywhere 400', async () => {
    expect((await post({ documentId: 'audit-nope', instruction: 'x' })).status).toBe(404);
    expect((await post({ documentId: 'audit-deck-1', instruction: 'x', slideId: 'nope' })).status).toBe(400);
    rows.set('audit-empty', { documentId: 'audit-empty', type: 'presentation', revisions: [] });
    expect((await post({ documentId: 'audit-empty', instruction: 'x' })).status).toBe(400);
    expect((await post({ documentId: 'audit-empty', instruction: 'x', spec: deck, baseRevisionId: null })).status).toBe(202);
    expect(trigger).toHaveBeenCalledTimes(1);
  });

  it('releases the claim when the engine refuses, so a retry can start the run', async () => {
    trigger.mockImplementationOnce(async () => { throw new Error('engine down'); });
    expect((await post({ documentId: 'audit-deck-1', instruction: 'x', requestId: 'r-3' })).status).toBe(500);
    const retry = await post({ documentId: 'audit-deck-1', instruction: 'x', requestId: 'r-3' });
    expect(retry.status).toBe(202);
    expect(retry.body.replay).toBe(false);
  });
});
