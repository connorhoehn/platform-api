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

  async get(): Promise<null> { return null; }

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
