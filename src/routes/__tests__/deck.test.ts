// /api/deck — preview and fixture. Both are stateless; the tests assert that,
// and that a spec too big to be a deck is refused as 413 rather than rendered.

import express from 'express';
import http from 'http';
import type { AddressInfo } from 'net';
import { errorHandler } from '../../middleware/error-handler';
import { MAX_SLIDES } from '../../deck/DeckSpec';
import { sampleDeck } from '../../deck/__tests__/fixtures';

const buildDeckFixture = jest.fn();
const readFixtureMaterial = jest.fn();

// Loop 29c: the route's access gate is covered in resourceAccess.routes.test.ts;
// here every caller may read and edit.
jest.mock('../../deck/deckSourceAccess', () => ({
  requireDeckSourcesReadable: () => (_req: unknown, _res: unknown, next: () => void) => next(),
}));
jest.mock('../../deck/deckFixture', () => {
  const actual = jest.requireActual('../../deck/deckFixture');
  return {
    ...actual,
    buildDeckFixture: (...args: unknown[]) => buildDeckFixture(...args),
    readFixtureMaterial: (...args: unknown[]) => readFixtureMaterial(...args),
  };
});

jest.mock('../../lib/aws-clients', () => ({ docClient: { send: jest.fn() }, ddbClient: { send: jest.fn() } }));
jest.mock('../../lib/ddb-table-name', () => ({ tableName: (name: string) => `test-${name}` }));

// The revise progress events go to Redis; the fake records them.
const published: Array<{ channel: string; event: { type: string; version: number; payload: Record<string, unknown> } }> = [];
jest.mock('../../lib/redis-client', () => ({
  getRedisClient: async () => ({
    publish: async (channel: string, message: string) => { published.push({ channel, event: JSON.parse(message) }); return 1; },
  }),
}));

const reviseDeck = jest.fn();

jest.mock('../../deck/deckRevise', () => {
  const actual = jest.requireActual('../../deck/deckRevise');
  return { ...actual, reviseDeck: (...args: unknown[]) => reviseDeck(...args) };
});

import { deckRouter } from '../deck';
import { _resetRateLimitMemFallback } from '../../middleware/rateLimit';
import { DeckReviseError } from '../../deck/deckRevise';
import { NoFixtureMaterialError } from '../../deck/deckFixture';
import { setPresentationDocumentStore, type PresentationDocumentRow } from '../../deck/presentationDocuments';

const deckRows = new Map<string, PresentationDocumentRow>();

function post(path: string, body: unknown): Promise<{ status: number; body: Record<string, unknown>; headers: http.IncomingHttpHeaders }> {
  const { port } = server.address() as AddressInfo;
  const payload = Buffer.from(JSON.stringify(body), 'utf8');
  return new Promise((resolve, reject) => {
    const req = http.request({
      host: '127.0.0.1', port, method: 'POST', path,
      headers: { 'content-type': 'application/json', 'content-length': String(payload.length) },
    }, (res) => {
      const chunks: Buffer[] = [];
      res.on('data', (chunk: Buffer) => chunks.push(chunk));
      res.on('end', () => {
        const text = Buffer.concat(chunks).toString('utf8');
        resolve({ status: res.statusCode ?? 0, body: text ? JSON.parse(text) : {}, headers: res.headers });
      });
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
  setPresentationDocumentStore({ get: async (id: string) => deckRows.get(id) ?? null, appendRevision: jest.fn() } as never);
  const app = express();
  app.use(express.json({ limit: '4mb' }));
  app.use('/api/deck', deckRouter);
  app.use(errorHandler);
  server = http.createServer(app);
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
});

afterAll(async () => {
  if (originalSkipAuth === undefined) delete process.env.SKIP_AUTH;
  else process.env.SKIP_AUTH = originalSkipAuth;
  setPresentationDocumentStore(null);
  await new Promise<void>((resolve) => server.close(() => resolve()));
});

beforeEach(() => { _resetRateLimitMemFallback(); buildDeckFixture.mockReset(); reviseDeck.mockReset(); published.length = 0; });

/** Lets the fire-and-forget publishes land. */
const settle = () => new Promise((resolve) => setImmediate(resolve));

describe('POST /api/deck/preview', () => {
  it('renders every slide plus Sources, with warnings, and stores nothing', async () => {
    const res = await post('/api/deck/preview', { spec: sampleDeck() });
    expect(res.status).toBe(200);
    expect(res.body.slideCount).toBe(9);
    expect((res.body.slides as Array<{ id: string; svg: string }>)[0]!.svg.startsWith('<svg')).toBe(true);
    expect(Array.isArray(res.body.warnings)).toBe(true);
  });

  it('accepts the spec at the top level too, the way an editor posts it', async () => {
    expect((await post('/api/deck/preview', sampleDeck())).status).toBe(200);
  });

  it('400s a deck with an uncited slide, listing every problem', async () => {
    const spec = sampleDeck();
    spec.slides[0]!.sources = [];
    const res = await post('/api/deck/preview', { spec });
    expect(res.status).toBe(400);
    expect(String(res.body.error)).toContain('must cite at least one source');
    expect((res.body.errors as string[]).length).toBeGreaterThan(0);
  });

  it('413s a deck past the slide cap', async () => {
    const slide = { id: 's', layout: 'bullets', title: 'T', bullets: ['b'], sources: [{ documentId: 'd', title: 'D' }] };
    const spec = { title: 'Deck', slides: Array.from({ length: MAX_SLIDES + 1 }, (_v, index) => ({ ...slide, id: `s${index}` })) };
    const res = await post('/api/deck/preview', { spec });
    expect(res.status).toBe(413);
    expect(String(res.body.error)).toContain(`the cap is ${MAX_SLIDES}`);
  });
});

describe('POST /api/deck/fixture', () => {
  it('answers with the spec and says where it came from', async () => {
    buildDeckFixture.mockResolvedValue({
      spec: sampleDeck(),
      source: 'model',
      warnings: ['Release plan was provided but not cited.'],
      transcriptIds: ['active-now-planning-transcript'],
      material: { document: { documentId: 'doc-release-plan', title: 'Release plan', text: 'xxx' } },
    });
    const res = await post('/api/deck/fixture', { from: { documentId: 'doc-release-plan' }, title: 'Sprint review' });
    expect(res.status).toBe(200);
    expect(res.body.source).toBe('model');
    expect((res.body.material as Record<string, unknown>).document).toMatchObject({ documentId: 'doc-release-plan', characters: 3 });
    expect(res.body.warnings).toEqual(['Release plan was provided but not cited.']);
    expect(res.body.transcriptIds).toEqual(['active-now-planning-transcript']);
    expect(buildDeckFixture).toHaveBeenCalledWith({ documentId: 'doc-release-plan' }, 'Sprint review', {});
  });

  it('answers only the material, with no draft, when asked for materialOnly (Loop 34 ppt)', async () => {
    readFixtureMaterial.mockResolvedValue({ document: { documentId: 'doc-release-plan', title: 'Release plan', text: 'Rollback owner: Sam.' } });
    const res = await post('/api/deck/fixture', { from: { documentId: 'doc-release-plan' }, materialOnly: true });
    expect(res.status).toBe(200);
    expect(Object.keys(res.body)).toEqual(['materialText']);
    expect(String(res.body.materialText)).toContain('Rollback owner: Sam.');
    expect(buildDeckFixture).not.toHaveBeenCalled();
  });

  it('passes the fallback reason through rather than hiding it', async () => {
    buildDeckFixture.mockResolvedValue({ spec: sampleDeck(), source: 'fixture', reason: 'no model is configured on this deployment', warnings: [], transcriptIds: [], material: {} });
    const res = await post('/api/deck/fixture', { from: { recordingId: 'rec-1' } });
    expect(res.status).toBe(200);
    expect(res.body.source).toBe('fixture');
    expect(res.body.reason).toBe('no model is configured on this deployment');
  });

  it('400s when nothing is named, and when what is named is not an id', async () => {
    expect((await post('/api/deck/fixture', { from: {} })).status).toBe(400);
    expect((await post('/api/deck/fixture', { from: { documentId: '../etc/passwd' } })).status).toBe(400);
    expect(buildDeckFixture).not.toHaveBeenCalled();
  });

  it('limits each person to 6 fixtures a minute: the 7th of 10 rapid calls is a 429 with Retry-After (NFR #9)', async () => {
    buildDeckFixture.mockResolvedValue({ spec: sampleDeck(), source: 'model', warnings: [], transcriptIds: [], material: {} });
    const answers = [];
    for (let i = 0; i < 10; i += 1) {
      answers.push(await post('/api/deck/fixture', { from: { documentId: 'doc-release-plan' } }));
    }
    expect(answers.map((a) => a.status)).toEqual([200, 200, 200, 200, 200, 200, 429, 429, 429, 429]);
    expect(Number(answers[6]!.headers['retry-after'])).toBeGreaterThanOrEqual(1);
    expect(answers[6]!.body).toMatchObject({ status: 429, title: 'Too Many Requests' });
    // Refused calls never reach the model.
    expect(buildDeckFixture).toHaveBeenCalledTimes(6);
  });

  it('400s when the named material cannot be read', async () => {
    buildDeckFixture.mockRejectedValue(new NoFixtureMaterialError('name a documentId or a recordingId that can be read'));
    const res = await post('/api/deck/fixture', { from: { recordingId: 'rec-missing' } });
    expect(res.status).toBe(400);
    expect(String(res.body.error)).toContain('can be read');
  });
});

describe('POST /api/deck/revise', () => {
  it('passes the instruction, the slide and the sources through and answers the edit', async () => {
    const edited = sampleDeck();
    edited.slides[1]!.title = 'Agenda, short';
    reviseDeck.mockResolvedValue({ spec: edited, summary: 'Shortened the title.', changedSlideIds: ['agenda'], warnings: [], source: 'model' });
    const res = await post('/api/deck/revise', {
      spec: sampleDeck(), instruction: '  make it shorter ', slideId: 'agenda',
      sources: [{ kind: 'transcript', id: 'active-now-planning-transcript' }],
    });
    expect(res.status).toBe(200);
    expect(res.body).toMatchObject({ summary: 'Shortened the title.', changedSlideIds: ['agenda'], source: 'model' });
    const [input] = reviseDeck.mock.calls[0] as [{ instruction: string; slideId?: string; sources: unknown[] }];
    expect(input.instruction).toBe('make it shorter');
    expect(input.slideId).toBe('agenda');
    expect(input.sources).toEqual([{ kind: 'transcript', id: 'active-now-planning-transcript' }]);
  });

  it('edits the whole deck when no slide is named', async () => {
    reviseDeck.mockResolvedValue({ spec: sampleDeck(), summary: 's', changedSlideIds: [], warnings: [], source: 'model' });
    expect((await post('/api/deck/revise', { spec: sampleDeck(), instruction: 'tighten it' })).status).toBe(200);
    expect((reviseDeck.mock.calls[0] as [{ slideId?: string }])[0].slideId).toBeUndefined();
  });

  it('400s a missing instruction, an unknown slide, a bad source and a spec that is not a deck', async () => {
    expect((await post('/api/deck/revise', { spec: sampleDeck(), instruction: ' ' })).status).toBe(400);
    expect((await post('/api/deck/revise', { spec: sampleDeck(), instruction: 'x', slideId: 'nope' })).status).toBe(400);
    expect((await post('/api/deck/revise', { spec: sampleDeck(), instruction: 'x', sources: [{ kind: 'web', id: 'x' }] })).status).toBe(400);
    const broken = { ...sampleDeck(), slides: [{ id: 'a', layout: 'bullets', title: 'A', sources: [] }] };
    const res = await post('/api/deck/revise', { spec: broken, instruction: 'x' });
    expect(res.status).toBe(400);
    expect(String(res.body.error)).toContain('needs at least one bullet');
    expect(reviseDeck).not.toHaveBeenCalled();
  });

  it('lets an uncited deck through — the agent can add the citation', async () => {
    reviseDeck.mockResolvedValue({ spec: sampleDeck(), summary: 's', changedSlideIds: [], warnings: [], source: 'model' });
    const spec = sampleDeck();
    spec.slides[0]!.sources = [];
    expect((await post('/api/deck/revise', { spec, instruction: 'cite the plan', sources: [{ kind: 'document', id: 'doc-plan' }] })).status).toBe(200);
  });

  it('answers the model failure honestly, with the reason and no spec', async () => {
    reviseDeck.mockRejectedValue(new DeckReviseError('The agent\'s edit broke the deck\'s rules, so nothing was changed.', 502, 'slides[1].sources: every slide must cite at least one source document', ['slides[1].sources: every slide must cite at least one source document']));
    const res = await post('/api/deck/revise', { spec: sampleDeck(), instruction: 'x' });
    expect(res.status).toBe(502);
    expect(res.body.spec).toBeUndefined();
    expect(String(res.body.reason)).toContain('must cite');
    expect(res.body.errors).toHaveLength(1);
  });
});

describe('POST /api/deck/revise — target', () => {
  it('passes a target that fits the slide through', async () => {
    reviseDeck.mockResolvedValue({ spec: sampleDeck(), summary: 's', changedSlideIds: ['agenda'], warnings: [], source: 'model' });
    const res = await post('/api/deck/revise', { spec: sampleDeck(), instruction: 'shorter', slideId: 'agenda', target: { field: 'bullets', index: 2 } });
    expect(res.status).toBe(200);
    expect((reviseDeck.mock.calls[0] as [{ target?: unknown }])[0].target).toEqual({ field: 'bullets', index: 2 });
  });

  it('400s a field the layout does not have, a missing bullet, an unknown field and a slide target with no slide', async () => {
    const bad = async (slideId: string | undefined, target: unknown) => {
      const res = await post('/api/deck/revise', { spec: sampleDeck(), instruction: 'x', ...(slideId ? { slideId } : {}), target });
      return [res.status, String(res.body.error)];
    };
    expect(await bad('agenda', { field: 'chart' })).toEqual([400, expect.stringContaining("only a 'chart' slide")]);
    expect(await bad('agenda', { field: 'bullets', index: 3 })).toEqual([400, expect.stringContaining('has 3 bullets')]);
    expect(await bad('agenda', { field: 'footer' })).toEqual([400, expect.stringContaining('target.field must be one of')]);
    expect(await bad(undefined, { field: 'notes' })).toEqual([400, expect.stringContaining('slideId')]);
    expect(reviseDeck).not.toHaveBeenCalled();
    expect(published).toHaveLength(0);
  });
});

describe('POST /api/deck/revise — live progress', () => {
  it('publishes started, each phase and completed on the document\'s channel, and returns the requestId', async () => {
    reviseDeck.mockImplementation(async (_input: unknown, opts: { onPhase?: (phase: string) => void }) => {
      opts.onPhase?.('reading-sources');
      opts.onPhase?.('asking-model');
      opts.onPhase?.('checking');
      return { spec: sampleDeck(), summary: 'Secret deck words.', changedSlideIds: ['agenda'], warnings: [], source: 'model' };
    });
    const res = await post('/api/deck/revise', {
      spec: sampleDeck(), instruction: 'confidential instruction', slideId: 'agenda', target: { field: 'notes' },
      documentId: 'doc-123', requestId: 'req-1',
    });
    await settle();
    expect(res.status).toBe(200);
    expect(res.body.requestId).toBe('req-1');
    expect(published.map((entry) => entry.channel)).toEqual([
      'dcore:topic:pipeline.events.deck.revise.started',
      'dcore:topic:pipeline.events.deck.revise.phase',
      'dcore:topic:pipeline.events.deck.revise.phase',
      'dcore:topic:pipeline.events.deck.revise.phase',
      'dcore:topic:pipeline.events.deck.revise.completed',
    ]);
    const events = published.map((entry) => entry.event);
    expect(events.map((event) => event.payload.phase)).toEqual(['started', 'reading-sources', 'asking-model', 'checking', 'completed']);
    expect(events.map((event) => event.version)).toEqual([1, 2, 3, 4, 5]);
    expect(events[0]).toMatchObject({
      type: 'pipeline:deck:revise:started',
      payload: { runId: 'deck-revise:doc-123', documentId: 'doc-123', requestId: 'req-1', userId: expect.any(String), scope: 'slide', slideId: 'agenda', target: { field: 'notes' } },
    });
    expect(events[4]!.payload.changedSlideIds).toEqual(['agenda']);
    // Nothing the deck or the instruction says rides the channel.
    const wire = JSON.stringify(events);
    expect(wire).not.toContain('confidential');
    expect(wire).not.toContain('Secret deck words');
  });

  it('publishes failed with the status and the plain sentence (not the model\'s reason)', async () => {
    reviseDeck.mockRejectedValue(new DeckReviseError('The agent could not answer, so nothing was changed.', 503, 'model said: "the deck text"'));
    const res = await post('/api/deck/revise', { spec: sampleDeck(), instruction: 'x', documentId: 'doc-123' });
    await settle();
    expect(res.status).toBe(503);
    expect(typeof res.body.requestId).toBe('string');
    const last = published[published.length - 1]!.event;
    expect(last.type).toBe('pipeline:deck:revise:failed');
    expect(last.payload).toMatchObject({ phase: 'failed', status: 503, reason: 'The agent could not answer, so nothing was changed.', scope: 'deck', requestId: res.body.requestId });
    expect(JSON.stringify(last)).not.toContain('the deck text');
  });

  it('403s an edit of someone else\'s document, before the model is asked or anything is published (NFR #41)', async () => {
    deckRows.set('doc-connor', { documentId: 'doc-connor', type: 'presentation', createdBy: 'dev-connor', viewers: ['dev-user'] } as never);
    try {
      const res = await post('/api/deck/revise', { spec: sampleDeck(), instruction: 'x', documentId: 'doc-connor' });
      expect(res.status).toBe(403);
      expect(res.body).toMatchObject({ code: 'forbidden', documentId: 'doc-connor' });
      expect(String(res.body.error)).toMatch(/view this document but not change it/);
      await settle();
      expect(reviseDeck).not.toHaveBeenCalled();
      expect(published).toHaveLength(0);
    } finally { deckRows.delete('doc-connor'); }
  });

  it('publishes nothing without a documentId, and 400s a bad documentId or requestId', async () => {
    reviseDeck.mockResolvedValue({ spec: sampleDeck(), summary: 's', changedSlideIds: [], warnings: [], source: 'model' });
    expect((await post('/api/deck/revise', { spec: sampleDeck(), instruction: 'x' })).status).toBe(200);
    await settle();
    expect(published).toHaveLength(0);
    expect((await post('/api/deck/revise', { spec: sampleDeck(), instruction: 'x', documentId: 'a b' })).status).toBe(400);
    expect((await post('/api/deck/revise', { spec: sampleDeck(), instruction: 'x', documentId: 'd', requestId: 'x'.repeat(65) })).status).toBe(400);
  });
});
