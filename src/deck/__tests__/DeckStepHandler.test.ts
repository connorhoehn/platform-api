// The `deck` step end to end, against a fake document store: the spec the
// drafting step produced becomes a .pptx, one SVG per slide, and a revision.
import type { PipelineNode, StepHandlerHelpers } from 'distributed-core/applications/pipeline';
import { createDeckStepHandler } from '../DeckStepHandler';
import type { AppendRevisionInput, AppendRevisionResult, PresentationDocumentStore } from '../presentationDocuments';
import { deckPreviewHandle } from '../presentationDocuments';
import { sampleDeck } from './fixtures';
import { hidePptxNodeDetection } from './pptxTestEnv';

class FakeStore implements PresentationDocumentStore {
  readonly calls: AppendRevisionInput[] = [];
  /** Rows the store already holds, by id — what `get` answers with. */
  readonly rows = new Map<string, { documentId: string; title?: string; type?: string; createdBy?: string; editors?: string[]; viewers?: string[] }>();
  failGet = false;

  async get(documentId: string) { if (this.failGet) throw new Error('ddb down'); return (this.rows.get(documentId) ?? null) as never; }

  async appendRevision(input: AppendRevisionInput): Promise<AppendRevisionResult> {
    this.calls.push(input);
    const revisionId = `v${this.calls.length}`;
    return {
      documentId: input.documentId,
      revision: {
        id: revisionId,
        label: revisionId,
        createdAt: '2026-09-21T10:00:00.000Z',
        createdBy: input.createdBy,
        slideCount: input.slides.length,
        asset: { key: `deck-${input.documentId}-${revisionId}.pptx`, contentType: 'application/vnd.openxmlformats-officedocument.presentationml.presentation', sizeBytes: input.pptx.byteLength },
        slides: input.slides.map((slide, index) => ({
          id: slide.id,
          index: index + 1,
          title: slide.title,
          previewHandle: deckPreviewHandle(input.documentId, revisionId, slide.id),
          asset: { key: `deck-${input.documentId}-${revisionId}-${slide.id}.svg`, contentType: 'image/svg+xml', sizeBytes: Buffer.byteLength(slide.svg) },
        })),
      },
    };
  }
}

const node = (data: Record<string, unknown>): PipelineNode => ({ id: 'render', type: 'deck', position: { x: 0, y: 0 }, data } as unknown as PipelineNode);

function helpers(): StepHandlerHelpers & { emitted: Array<{ event: string; payload: unknown }> } {
  const emitted: Array<{ event: string; payload: unknown }> = [];
  return {
    runId: 'run-1',
    stepId: 'render',
    emit: async (event: string, payload: unknown) => { emitted.push({ event, payload }); },
    emitted,
  } as unknown as StepHandlerHelpers & { emitted: Array<{ event: string; payload: unknown }> };
}

describe('the deck step', () => {
  let store: FakeStore;
  let restore: () => void;

  beforeAll(() => { restore = hidePptxNodeDetection(); });
  afterAll(() => restore());
  beforeEach(() => { store = new FakeStore(); });

  it('renders the drafted spec into a revision of the run’s document', async () => {
    const handler = createDeckStepHandler({ store });
    const context = {
      documentId: 'doc-new',
      requestedBy: 'user-frank',
      steps: { draft: { text: `Here is the deck:\n\`\`\`json\n${JSON.stringify(sampleDeck())}\n\`\`\`` } },
    };
    const outcome = await handler.execute(node({ type: 'deck', specFrom: 'steps.draft.text' }), context, helpers());

    expect(outcome.status).toBe('completed');
    const output = outcome.output as Record<string, unknown>;
    expect(output.documentId).toBe('doc-new');
    expect(output.revisionId).toBe('v1');
    // Eight slides plus the Sources slide that closes every deck.
    expect(output.slideCount).toBe(9);
    expect(output.previewHandles).toHaveLength(9);
    expect((output.previewHandles as string[])[0]).toBe('doc-new.v1.cover');
    expect((output.previewHandles as string[])[8]).toBe('doc-new.v1.sources');
    expect(output.result).toEqual({ kind: 'document', documentId: 'doc-new' });

    const stored = store.calls[0]!;
    expect(stored.createdBy).toBe('user-frank');
    expect(stored.title).toBe('Q4 platform review');
    expect(stored.pptx.subarray(0, 2).toString('utf8')).toBe('PK');
    expect(stored.slides).toHaveLength(9);
    // The spec travels with the bytes, so the revision can be reopened and edited.
    expect(stored.spec?.title).toBe('Q4 platform review');
    expect(stored.slides[8]).toMatchObject({ id: 'sources', title: 'Sources' });
    for (const slide of stored.slides) expect(slide.svg.startsWith('<svg')).toBe(true);
  }, 30_000);

  it('marks a cited source as a transcript only when the reading step declared it', async () => {
    const handler = createDeckStepHandler({ store });
    const spec = sampleDeck();
    spec.slides[0]!.sources = [{ documentId: 'active-now-planning-transcript', title: 'Planning sync' }];
    await handler.execute(
      node({ type: 'deck', spec }),
      { documentId: 'doc-new', requestedBy: 'user-frank', transcriptSource: { documentId: 'active-now-planning-transcript', title: 'Planning sync' } },
      helpers(),
    );
    expect(store.calls[0]!.transcriptIds).toEqual(['active-now-planning-transcript']);
  }, 30_000);

  it('announces what it produced', async () => {
    const handler = createDeckStepHandler({ store });
    const help = helpers();
    await handler.execute(
      node({ type: 'deck', spec: sampleDeck() }),
      { documentId: 'doc-new', requestedBy: 'user-frank' },
      help,
    );
    expect(help.emitted[0]?.event).toBe('pipeline:step:deck:produced');
    expect(help.emitted[0]?.payload).toMatchObject({ documentId: 'doc-new', revisionId: 'v1', slideCount: 9 });
  }, 30_000);

  it('makes a document when the run was not given one', async () => {
    const handler = createDeckStepHandler({ store, newDocumentId: () => 'doc-generated' });
    const outcome = await handler.execute(node({ type: 'deck', spec: sampleDeck() }), {}, helpers());
    expect(outcome.status).toBe('completed');
    expect((outcome.output as Record<string, unknown>).documentId).toBe('doc-generated');
    expect(store.calls[0]!.createdBy).toBe('agent:deck');
  }, 30_000);

  it('fails with the validator’s reasons when a slide cites nothing', async () => {
    const handler = createDeckStepHandler({ store });
    const spec = sampleDeck();
    spec.slides[0]!.sources = [];
    const outcome = await handler.execute(
      node({ type: 'deck', spec }),
      { documentId: 'doc-new' },
      helpers(),
    );
    expect(outcome.status).toBe('failed');
    expect(outcome.error).toContain('must cite at least one source');
    expect(store.calls).toHaveLength(0);
  });

  it('fails when there is no spec to render', async () => {
    const handler = createDeckStepHandler({ store });
    const outcome = await handler.execute(node({ type: 'deck', specFrom: 'steps.draft.text' }), { steps: {} }, helpers());
    expect(outcome.status).toBe('failed');
    expect(outcome.error).toContain('no DeckSpec found');
  });

  it('fails when the store cannot hold the bytes, and says so', async () => {
    const failing: PresentationDocumentStore = {
      get: async () => null,
      appendRevision: async () => { throw new Error('asset is over the DDB inline cap — run with IMAGE_BLOB_STORE=fs'); },
    };
    const handler = createDeckStepHandler({ store: failing });
    const outcome = await handler.execute(node({ type: 'deck', spec: sampleDeck() }), { documentId: 'd' }, helpers());
    expect(outcome.status).toBe('failed');
    expect(outcome.error).toContain('IMAGE_BLOB_STORE=fs');
  }, 30_000);
});


describe('a deck never writes into another kind of document', () => {
  let store: FakeStore;
  let restore: () => void;

  beforeAll(() => { restore = hidePptxNodeDetection(); });
  afterAll(() => restore());
  beforeEach(() => { store = new FakeStore(); });

  it('files a new presentation when the run’s documentId is the agenda page', async () => {
    // What a four-kind loop leaves in the context: the agenda step's own page.
    store.rows.set('doc-agenda-1', { documentId: 'doc-agenda-1', title: 'Fixture agenda — the loop', type: 'page' });
    const spec = sampleDeck();
    spec.slides[0]!.sources = [{ documentId: 'doc-agenda-1', title: 'Fixture agenda — the loop' }];

    const handler = createDeckStepHandler({ store, newDocumentId: () => 'doc-presentation-new' });
    const outcome = await handler.execute(
      node({ type: 'deck', spec }),
      { documentId: 'doc-agenda-1', requestedBy: 'dev-frank' },
      helpers(),
    );

    expect(outcome.status).toBe('completed');
    const output = outcome.output as Record<string, unknown>;
    expect(output.documentId).toBe('doc-presentation-new');
    expect(output.displacedDocument).toEqual({ documentId: 'doc-agenda-1', title: 'Fixture agenda — the loop', type: 'page' });
    expect(output.citesDisplacedDocument).toBe(true);
    expect(String((output.notices as string[])[0])).toContain('not a presentation');
    // The page itself is untouched: the only write names the new document.
    expect(store.calls).toHaveLength(1);
    expect(store.calls[0]!.documentId).toBe('doc-presentation-new');
  }, 30_000);

  it('adds a revision to a real presentation, as before', async () => {
    store.rows.set('doc-deck-1', { documentId: 'doc-deck-1', title: 'Sprint review', type: 'presentation', createdBy: 'user-connor' });
    const handler = createDeckStepHandler({ store, newDocumentId: () => 'unused' });
    const outcome = await handler.execute(node({ type: 'deck', spec: sampleDeck() }), { documentId: 'doc-deck-1', requestedBy: 'user-connor' }, helpers());
    expect((outcome.output as Record<string, unknown>).documentId).toBe('doc-deck-1');
    expect((outcome.output as Record<string, unknown>).displacedDocument).toBeUndefined();
    // The presentation keeps its owner's name; the spec's title does not rename it.
    expect((outcome.output as Record<string, unknown>).title).toBe('Sprint review');
  }, 30_000);

  it('uses the id it was given when no row exists yet — the client made the page', async () => {
    const handler = createDeckStepHandler({ store, newDocumentId: () => 'unused' });
    const outcome = await handler.execute(node({ type: 'deck', spec: sampleDeck() }), { documentId: 'doc-fresh' }, helpers());
    expect((outcome.output as Record<string, unknown>).documentId).toBe('doc-fresh');
  }, 30_000);

  // NFR #47: the revision is written as the requester, so #41's rule applies at write time.
  describe('who may add a revision to an existing presentation', () => {
    const deckRow = { documentId: 'doc-owned', title: 'Connor\'s deck', type: 'presentation', createdBy: 'user-connor', editors: ['user-erin'], viewers: ['user-vic'] };
    const run = (requestedBy: string) => {
      const handler = createDeckStepHandler({ store, newDocumentId: () => 'unused' });
      return handler.execute(node({ type: 'deck', spec: sampleDeck() }), { documentId: 'doc-owned', requestedBy }, helpers());
    };
    beforeEach(() => { store.rows.set('doc-owned', { ...deckRow }); });

    it.each([['owner', 'user-connor'], ['editor', 'user-erin']])('the %s revises it in place', async (_role, user) => {
      const outcome = await run(user);
      expect(outcome.status).toBe('completed');
      expect(store.calls).toHaveLength(1);
      expect(store.calls[0]!.documentId).toBe('doc-owned');
      expect(store.calls[0]!.createdBy).toBe(user);
    }, 30_000);

    it.each([['viewer', 'user-vic', /view this document but not change it/], ['stranger', 'user-frank', /owner or an editor/]])(
      'a %s is refused as forbidden and nothing is written', async (_role, user, message) => {
        const outcome = await run(user);
        expect(outcome.status).toBe('failed');
        expect(outcome.error).toMatch(/^deck forbidden: /);
        expect(outcome.error).toMatch(message);
        expect(store.calls).toHaveLength(0);
      }, 30_000);

    it('a run with no requester cannot revise someone\'s deck', async () => {
      const handler = createDeckStepHandler({ store, newDocumentId: () => 'unused' });
      const outcome = await handler.execute(node({ type: 'deck', spec: sampleDeck() }), { documentId: 'doc-owned' }, helpers());
      expect(outcome.status).toBe('failed');
      expect(store.calls).toHaveLength(0);
    }, 30_000);

    it('fails closed when the row cannot be read', async () => {
      store.failGet = true;
      const outcome = await run('user-connor');
      expect(outcome.status).toBe('failed');
      expect(outcome.error).toMatch(/could not read doc-owned/);
      expect(store.calls).toHaveLength(0);
    }, 30_000);
  });
});
