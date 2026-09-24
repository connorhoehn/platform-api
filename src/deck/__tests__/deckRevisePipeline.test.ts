// The `deck-revise` pipeline end to end through distributed-core's real
// PipelineExecutor: a composer edit is a run whose steps read the deck, make
// the edit and write the revision — conditioned on the base, idempotent by
// requestId — and whose progress lands on the document's revise channel.
import { EventBus } from 'distributed-core';
import { PipelineExecutor, type PipelineDefinition, type PipelineEventMap, type StepHandler } from 'distributed-core/applications/pipeline';

jest.mock('../../lib/event-catalog', () => ({ publishDurable: jest.fn(async () => undefined) }));

import { sampleDeck } from './fixtures';
import { hidePptxNodeDetection } from './pptxTestEnv';
import {
  attachDeckReviseRunEvents, buildDeckReviseDefinition, buildDeckReviseStepHandlers, rebaseScopedEdit,
  resetDeckReviseRuns, setDeckReviseRunEventDeps, DECK_REVISE_PIPELINE_ID,
} from '../deckRevisePipeline';
import { DeckReviseError, type DeckReviseInput, type DeckReviseResult } from '../deckRevise';
import type { DeckSpec } from '../DeckSpec';
import {
  deckPreviewHandle, PresentationRevisionConflictError,
  type AppendRevisionInput, type AppendRevisionResult, type PresentationDocumentRow, type PresentationDocumentStore, type StoredRevision, type HeadRevision,
} from '../presentationDocuments';
import { lintSystemPipeline } from '../../pipeline/systemPipelineLint';
import { getSystemPipeline } from '../../pipeline/systemPipelines';

const DOC = 'audit-deck-test';

/** A store with the real conditional-append semantics (expectedHead → conflict). */
class FakeStore implements PresentationDocumentStore {
  readonly rows = new Map<string, PresentationDocumentRow>();
  appends = 0;
  /** Runs before an append checks its base: a concurrent save lands here. */
  beforeAppend?: () => void;

  async get(documentId: string) { return this.rows.get(documentId) ?? null; }

  seed(spec: DeckSpec, revisions = 1): void {
    const row: PresentationDocumentRow = { documentId: DOC, title: 'Audit deck', type: 'presentation', createdBy: 'frank', revisions: [] };
    this.rows.set(DOC, row);
    for (let i = 0; i < revisions; i += 1) this.push({ documentId: DOC, title: 'Audit deck', createdBy: 'frank', pptx: Buffer.from('x'), slides: [], spec });
  }

  push(input: AppendRevisionInput): StoredRevision {
    const row = this.rows.get(input.documentId)!;
    const id = `v${(row.revisions?.length ?? 0) + 1}`;
    const revision: StoredRevision = {
      id, label: id, createdAt: '2026-09-22T10:00:00.000Z', createdBy: input.createdBy, slideCount: input.slides.length,
      asset: { key: `k-${id}`, contentType: 'application/pptx', sizeBytes: input.pptx.byteLength } as never,
      slides: input.slides.map((slide, index) => ({ id: slide.id, index: index + 1, title: slide.title, previewHandle: deckPreviewHandle(input.documentId, id, slide.id), asset: {} as never })),
      ...(input.spec ? { spec: input.spec } : {}),
      ...(input.producedBy ? { producedBy: input.producedBy } : {}),
    };
    row.revisions = [...(row.revisions ?? []), revision];
    return revision;
  }

  async appendRevision(input: AppendRevisionInput): Promise<AppendRevisionResult> {
    const hook = this.beforeAppend;
    this.beforeAppend = undefined;
    hook?.();
    const row = this.rows.get(input.documentId)!;
    const head = row.revisions?.length ? row.revisions[row.revisions.length - 1]! : null;
    if (input.expectedHead !== undefined && (head?.id ?? null) !== input.expectedHead) {
      throw new PresentationRevisionConflictError(input.documentId, head, row.revisions?.length ?? 0);
    }
    this.appends += 1;
    return { documentId: input.documentId, revision: this.push(input) as HeadRevision };
  }
}

function makeBus(): EventBus<PipelineEventMap> {
  const handlers = new Map<string, (topic: string, payload: unknown, meta: Record<string, unknown>) => Promise<void>>();
  let n = 0;
  const pubsub = {
    subscribe: (_t: string, h: never) => { n += 1; handlers.set(`s${n}`, h); return Promise.resolve(`s${n}`); },
    unsubscribe: (id: string) => { handlers.delete(id); return Promise.resolve(); },
    publish: async (topic: string, payload: unknown, meta: Record<string, unknown> = {}) => { for (const h of handlers.values()) await h(topic, payload, meta); },
    subscribeAll: (h: never) => { n += 1; handlers.set(`a${n}`, h); return Promise.resolve(`a${n}`); },
  };
  return new EventBus<PipelineEventMap>(pubsub as never, 'test-node', { topic: 'pipeline.deck-revise' });
}

/** Retitles the target slide: a stand-in for the model. */
function retitle(title: string) {
  return jest.fn(async (input: DeckReviseInput, opts?: { onPhase?: (p: 'reading-sources' | 'asking-model' | 'checking') => void }): Promise<DeckReviseResult> => {
    opts?.onPhase?.('reading-sources');
    opts?.onPhase?.('asking-model');
    opts?.onPhase?.('checking');
    const id = input.slideId ?? input.spec.slides[0]!.id;
    return {
      spec: { ...input.spec, slides: input.spec.slides.map((slide) => (slide.id === id ? { ...slide, title } : slide)) },
      summary: `Retitled ${id}.`, changedSlideIds: [id], warnings: [], source: 'model',
    };
  });
}

const published: Array<{ type: string; payload: Record<string, unknown> }> = [];
const durable: Array<{ name: string; payload: Record<string, unknown> }> = [];
const produced: Array<Record<string, unknown>> = [];

async function run(store: FakeStore, revise: jest.Mock, trigger: Record<string, unknown>) {
  const handlers = new Map<string, StepHandler>();
  for (const handler of buildDeckReviseStepHandlers({ store, revise: revise as never })) handlers.set(handler.kind, handler);
  const bus = makeBus();
  await bus.start();
  attachDeckReviseRunEvents(bus as never);
  await (bus as unknown as { subscribe: (type: string, h: (event: { payload: Record<string, unknown> }) => void) => Promise<unknown> })
    .subscribe('pipeline:step:deck:produced', (event) => { produced.push(event.payload); });
  const executor = new PipelineExecutor({
    definition: buildDeckReviseDefinition() as unknown as PipelineDefinition,
    ownerNodeId: 'test-node',
    llmClient: { stream: () => ({ async *[Symbol.asyncIterator]() { /* none */ } }) } as never,
    eventBus: bus,
    speedMultiplier: 0.001,
    triggerPayload: { documentId: DOC, requestId: 'req-1', requestedBy: 'frank', instruction: 'Retitle this slide', sources: [], ...trigger },
    customStepHandlers: handlers,
  });
  const result = await executor.run();
  // Terminal events are published on the bus asynchronously.
  await new Promise((resolve) => setImmediate(resolve));
  return result;
}

const kinds = () => published.map((event) => (event.type.endsWith(':phase') ? `phase:${event.payload.phase}` : event.type.split(':').pop()));

describe('deck-revise pipeline', () => {
  let restore: () => void;
  let store: FakeStore;
  const deck = sampleDeck();
  const slideId = deck.slides[1]!.id;

  beforeAll(() => { restore = hidePptxNodeDetection(); });
  afterAll(() => restore());
  beforeEach(() => {
    published.length = 0;
    durable.length = 0;
    produced.length = 0;
    resetDeckReviseRuns();
    setDeckReviseRunEventDeps({
      publish: async (_channel, message) => { const event = JSON.parse(message); published.push({ type: event.type, payload: event.payload }); },
      durable: async (name, payload) => { durable.push({ name, payload }); },
    });
    store = new FakeStore();
    store.seed(deck);
  });

  it('is a registered system pipeline whose steps pass the retry lint', () => {
    const def = getSystemPipeline(DECK_REVISE_PIPELINE_ID) as { nodes: Array<{ id: string }> } | null;
    expect(def?.nodes.map((node) => node.id)).toEqual(['trigger', 'load', 'propose', 'check', 'write']);
    expect(lintSystemPipeline(buildDeckReviseDefinition())).toEqual([]);
  });

  it('runs load → propose → write and the run writes the revision, not the caller', async () => {
    const revise = retitle('Tighter');
    const result = await run(store, revise, { slideId, baseRevisionId: 'v1' });

    expect(result.status).toBe('completed');
    for (const step of ['load', 'propose', 'write']) expect(result.steps[step]?.status).toBe('completed');
    const head = store.rows.get(DOC)!.revisions!.at(-1)!;
    expect(head.id).toBe('v2');
    expect(head.spec!.slides[1]!.title).toBe('Tighter');
    expect(head.producedBy).toEqual({ pipelineId: 'deck-revise', runId: result.id, requestId: 'req-1' });
    expect(result.steps.write?.output).toMatchObject({ written: true, revisionId: 'v2', baseRevisionId: 'v1', changedSlideIds: [slideId], summary: `Retitled ${slideId}. Changed: slide 2 title.`, changes: ["slide 2 title"] });
    expect(revise.mock.calls[0]![0].slideId).toBe(slideId);
  });

  it('announces the revision it wrote, so the work graph draws it under the run that made it', async () => {
    const result = await run(store, retitle('Tighter'), { slideId, baseRevisionId: 'v1' });
    expect(produced).toEqual([expect.objectContaining({ runId: result.id, stepId: 'write', documentId: DOC, revisionId: 'v2' })]);
  });

  it('announces nothing when the edit failed or changed nothing', async () => {
    await run(store, retitle('Tighter'), { slideId, baseRevisionId: 'v0' });
    expect(produced).toEqual([]);
  });

  it('publishes started → phases → saving → completed on the document channel, and the outcome durably', async () => {
    const result = await run(store, retitle('Tighter'), { slideId });
    expect(kinds()).toEqual(['started', 'phase:reading-sources', 'phase:asking-model', 'phase:checking', 'phase:checking', 'phase:saving', 'completed']);
    for (const event of published) {
      expect(event.payload).toMatchObject({ runId: `deck-revise:${DOC}`, pipelineRunId: result.id, documentId: DOC, requestId: 'req-1', scope: 'slide', slideId });
    }
    expect(published.at(-1)!.payload).toMatchObject({ revisionId: 'v2', changedSlideIds: [slideId] });
    expect(durable.map((entry) => entry.name)).toEqual(['pipeline.deck.revise.completed']);
    expect(durable[0]!.payload).toMatchObject({ documentId: DOC, requestId: 'req-1', revisionId: 'v2', pipelineRunId: result.id });
    // Nothing the deck says is on the wire.
    expect(JSON.stringify(published)).not.toContain('Tighter');
  });

  it('edits the editor\'s unsaved spec when it sends one, on top of its base', async () => {
    const unsaved = { ...deck, title: 'Local edit' };
    await run(store, retitle('Tighter'), { slideId, baseRevisionId: 'v1', spec: unsaved });
    expect(store.rows.get(DOC)!.revisions!.at(-1)!.spec!.title).toBe('Local edit');
  });

  it('fails at load, before any model time, when the base is no longer the head', async () => {
    store.push({ documentId: DOC, title: 'Audit deck', createdBy: 'connor', pptx: Buffer.from('x'), slides: [], spec: deck });
    const revise = retitle('Tighter');
    const result = await run(store, revise, { slideId, baseRevisionId: 'v1' });
    expect(result.status).toBe('failed');
    expect(revise).not.toHaveBeenCalled();
    expect(store.appends).toBe(0);
    expect(published.at(-1)).toMatchObject({ type: 'pipeline:deck:revise:failed', payload: { status: 409, code: 'revision-conflict' } });
    expect(durable.map((entry) => entry.name)).toEqual(['pipeline.deck.revise.failed']);
  });

  it('rebases a slide-scoped edit when the concurrent save did not touch that slide', async () => {
    store.beforeAppend = () => {
      // Connor retitles a DIFFERENT slide while the model is thinking.
      store.push({ documentId: DOC, title: 'Audit deck', createdBy: 'connor', pptx: Buffer.from('x'), slides: [], spec: { ...deck, slides: deck.slides.map((slide, index) => (index === 2 ? { ...slide, title: 'Connor' } : slide)) } });
    };
    const result = await run(store, retitle('Tighter'), { slideId, baseRevisionId: 'v1' });
    expect(result.status).toBe('completed');
    const head = store.rows.get(DOC)!.revisions!.at(-1)!;
    expect(head.id).toBe('v3');
    expect(head.spec!.slides[1]!.title).toBe('Tighter');
    expect(head.spec!.slides[2]!.title).toBe('Connor');
    expect(result.steps.write?.output).toMatchObject({ rebasedOnto: 'v2', revisionId: 'v3' });
    expect(published.at(-1)!.payload).toMatchObject({ revisionId: 'v3', rebasedOnto: 'v2' });
  });

  it('fails honestly, writing nothing, when the concurrent save changed the same slide', async () => {
    store.beforeAppend = () => {
      store.push({ documentId: DOC, title: 'Audit deck', createdBy: 'connor', pptx: Buffer.from('x'), slides: [], spec: { ...deck, slides: deck.slides.map((slide) => (slide.id === slideId ? { ...slide, title: 'Connor' } : slide)) } });
    };
    const result = await run(store, retitle('Tighter'), { slideId, baseRevisionId: 'v1' });
    expect(result.status).toBe('failed');
    expect(result.steps.write?.status).toBe('failed');
    expect(store.appends).toBe(0);
    expect(store.rows.get(DOC)!.revisions!.at(-1)!.spec!.slides[1]!.title).toBe('Connor');
    expect(published.at(-1)).toMatchObject({ type: 'pipeline:deck:revise:failed', payload: { status: 409, code: 'revision-conflict' } });
  });

  it('rebases onto a head whose unchanged slide was stored in a different key order', () => {
    // The head as DynamoDB hands it back: same content, keys in another order.
    const reordered = (value: unknown): unknown => (Array.isArray(value)
      ? value.map(reordered)
      : value && typeof value === 'object'
        ? Object.fromEntries(Object.entries(value as Record<string, unknown>).reverse().map(([k, v]) => [k, reordered(v)]))
        : value);
    const otherSlide = deck.slides.find((slide) => slide.id !== slideId)!;
    const head = reordered({ ...deck, slides: deck.slides.map((slide) => (slide.id === otherSlide.id ? { ...slide, title: 'Theirs' } : slide)) }) as typeof deck;
    const edited = { ...deck, slides: deck.slides.map((slide) => (slide.id === slideId ? { ...slide, title: 'Mine' } : slide)) };
    const rebased = rebaseScopedEdit(deck, edited, head, slideId, undefined);
    expect(rebased).not.toBeNull();
    expect(rebased!.slides.find((slide) => slide.id === slideId)!.title).toBe('Mine');
    expect(rebased!.slides.find((slide) => slide.id === otherSlide.id)!.title).toBe('Theirs');
  });

  it('never rebases a deck-scoped edit', () => {
    const head = { ...deck, title: 'Someone else' };
    expect(rebaseScopedEdit(deck, { ...deck, title: 'Mine' }, head, undefined, undefined)).toBeNull();
  });

  it('is idempotent by requestId: a write that already landed is returned, not repeated', async () => {
    // Both runs edit the same input, as a retried step or a resumed run would.
    const first = await run(store, retitle('Tighter'), { slideId, spec: deck });
    expect(first.status).toBe('completed');
    resetDeckReviseRuns();
    published.length = 0;
    // The same request run again (a retried step / a resumed run).
    const again = await run(store, retitle('Tighter'), { slideId, spec: deck });
    expect(again.status).toBe('completed');
    expect(store.appends).toBe(1);
    expect(again.steps.write?.output).toMatchObject({ replayed: true, revisionId: 'v2' });
  });

  describe('check step (constraints measured, never the model\'s word)', () => {
    const split = deck.slides[2]!.id; // 'Shipped vs deferred': 3 words
    /** Answers each call with the next title. */
    const retitles = (...titles: string[]) => {
      const fn = jest.fn(async (input: DeckReviseInput, opts?: { onPhase?: (p: 'reading-sources' | 'asking-model' | 'checking') => void }): Promise<DeckReviseResult> => {
        opts?.onPhase?.('reading-sources');
        opts?.onPhase?.('asking-model');
        opts?.onPhase?.('checking');
        const title = titles[Math.min(fn.mock.calls.length - 1, titles.length - 1)]!;
        return {
          spec: { ...input.spec, slides: input.spec.slides.map((slide) => (slide.id === split ? { ...slide, title } : slide)) },
          summary: 'Made the title one word shorter.', changedSlideIds: [split], warnings: [], source: 'model',
        };
      });
      return fn;
    };

    it('records the measured facts on the run and appends them to the summary when the ask is met', async () => {
      const revise = retitles('Shipped, deferred');
      const result = await run(store, revise, { slideId: split, baseRevisionId: 'v1', instruction: "make this slide's title one word shorter" });
      expect(result.status).toBe('completed');
      expect(revise).toHaveBeenCalledTimes(1);
      expect(result.steps.check?.output).toMatchObject({ facts: ['Title: 3 → 2 words'], retried: false, constraints: [{ kind: 'delta', subject: 'title', metric: 'words', n: -1 }] });
      expect(result.steps.write?.output).toMatchObject({
        written: true, revisionId: 'v2', facts: ['Title: 3 → 2 words'], retried: false,
        summary: 'Made the title one word shorter. Measured: Title: 3 → 2 words.',
      });
    });

    it('asks the model once more with the numbers when the proposal misses, and writes the second answer', async () => {
      const revise = retitles('Deferred vs shipped', 'Shipped, deferred');
      const result = await run(store, revise, { slideId: split, baseRevisionId: 'v1', instruction: "make this slide's title one word shorter" });
      expect(result.status).toBe('completed');
      expect(revise).toHaveBeenCalledTimes(2);
      expect(revise.mock.calls[1]![0].instruction).toContain('Title has 3 words; before it had 3. Make it 2.');
      expect(revise.mock.calls[1]![0].spec).toEqual(deck); // against the deck as it was, not the missed proposal
      expect(store.rows.get(DOC)!.revisions!.at(-1)!.spec!.slides[2]!.title).toBe('Shipped, deferred');
      expect(result.steps.check?.output).toMatchObject({ retried: true, facts: ['Title: 3 → 2 words'] });
      expect(kinds()).toEqual(['started', 'phase:reading-sources', 'phase:asking-model', 'phase:checking', 'phase:checking', 'phase:reading-sources', 'phase:asking-model', 'phase:checking', 'phase:saving', 'completed']);
    });

    it('fails constraint-not-met, writing nothing, when the second answer still misses — the claim never stands', async () => {
      const revise = retitles('Deferred vs shipped', 'Release risk management');
      const result = await run(store, revise, { slideId: split, baseRevisionId: 'v1', instruction: "make this slide's title one word shorter" });
      expect(result.status).toBe('failed');
      expect(store.appends).toBe(0);
      expect(result.steps.write?.status ?? 'pending').not.toBe('completed');
      expect(result.steps.check?.error).toBe('deck-revise constraint-not-met: The edit did not do what was asked: Title has 3 words; before it had 3. Make it 2. Measured: Title: 3 → 3 words. Nothing was saved.');
      expect(published.at(-1)).toMatchObject({ type: 'pipeline:deck:revise:failed', payload: { status: 422, code: 'constraint-not-met' } });
    });

    it('an impossible ask fails honestly with the numbers', async () => {
      const revise = retitles('Shipped', 'Done');
      const result = await run(store, revise, { slideId: split, baseRevisionId: 'v1', instruction: 'make the title 0 words' });
      expect(result.status).toBe('failed');
      expect(store.appends).toBe(0);
      expect(result.steps.check?.error).toContain('Title has 1 word; it must have exactly 0.');
      expect(published.at(-1)).toMatchObject({ type: 'pipeline:deck:revise:failed', payload: { status: 422, code: 'constraint-not-met' } });
    });

    it('drops a claim the measurements contradict even when nothing was asked in numbers', async () => {
      const revise = retitles('Deferred vs shipped');
      const result = await run(store, revise, { slideId: split, baseRevisionId: 'v1', instruction: 'rename the slide' });
      expect(result.status).toBe('completed');
      expect(result.steps.write?.output).toMatchObject({
        written: true,
        summary: 'Changed 1 slide. Measured: Title: 3 → 3 words. The agent’s own note was left out where the numbers contradict it.',
        facts: ['Title: 3 → 3 words'],
      });
    });
  });

  it('writes nothing when the edit changed nothing', async () => {
    const same = jest.fn(async (input: DeckReviseInput): Promise<DeckReviseResult> => ({ spec: input.spec, summary: 'No change.', changedSlideIds: [], warnings: ['The agent returned the deck unchanged.'], source: 'model' }));
    const result = await run(store, same, { slideId });
    expect(result.status).toBe('completed');
    expect(store.appends).toBe(0);
    expect(published.at(-1)!.payload).toMatchObject({ phase: 'completed', revisionId: null });
  });

  it('fails the run with the model\'s refusal on the channel', async () => {
    const refuse = jest.fn(async () => { throw new DeckReviseError('The agent\'s edit broke the deck\'s rules, so nothing was changed.', 502, 'bad slide'); });
    const result = await run(store, refuse, { slideId });
    expect(result.status).toBe('failed');
    expect(refuse).toHaveBeenCalledTimes(1);
    expect(store.appends).toBe(0);
    expect(published.at(-1)).toMatchObject({ type: 'pipeline:deck:revise:failed', payload: { status: 502, code: 'model-failed' } });
  });

  it('fails at load, before any model time, when the requester may not edit the deck (NFR #41)', async () => {
    const row = store.rows.get(DOC)!;
    row.createdBy = 'connor';
    row.viewers = ['frank'];
    const revise = retitle('Tighter');
    const result = await run(store, revise, { slideId });
    expect(result.status).toBe('failed');
    expect(revise).not.toHaveBeenCalled();
    expect(store.appends).toBe(0);
    expect(published.at(-1)).toMatchObject({ type: 'pipeline:deck:revise:failed', payload: { status: 403, code: 'forbidden' } });
  });

  it('re-checks at write time: access revoked while the model was thinking writes nothing (NFR #41)', async () => {
    const row = store.rows.get(DOC)!;
    row.createdBy = 'connor';
    row.editors = ['frank'];
    const inner = retitle('Tighter');
    const revise = jest.fn(async (...args: Parameters<typeof inner>) => {
      row.editors = [];
      return inner(...args);
    });
    const result = await run(store, revise as never, { slideId });
    expect(result.status).toBe('failed');
    expect(revise).toHaveBeenCalledTimes(1);
    expect(store.appends).toBe(0);
    expect(row.revisions).toHaveLength(1);
    expect(published.at(-1)).toMatchObject({ type: 'pipeline:deck:revise:failed', payload: { status: 403, code: 'forbidden' } });
  });

  it('lets an editor the owner named write the revision', async () => {
    const row = store.rows.get(DOC)!;
    row.createdBy = 'connor';
    row.editors = ['frank'];
    const result = await run(store, retitle('Tighter'), { slideId });
    expect(result.status).toBe('completed');
    expect(store.appends).toBe(1);
  });

  it('fails at load on a document that is not a presentation', async () => {
    store.rows.set(DOC, { documentId: DOC, type: 'page' });
    const result = await run(store, retitle('x'), {});
    expect(result.status).toBe('failed');
    expect(published.at(-1)!.payload).toMatchObject({ status: 404, code: 'not-found' });
  });
});
