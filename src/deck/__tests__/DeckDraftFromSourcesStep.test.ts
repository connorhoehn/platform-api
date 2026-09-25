// presentation-deck's drafting step: every source is checked as the requester
// and read; one that fails refuses the whole run by id; the deck cites each.

jest.mock('../../lib/aws-clients', () => ({ docClient: { send: jest.fn(async () => ({})) }, ddbClient: { send: jest.fn() } }));
jest.mock('../../lib/ddb-table-name', () => ({ tableName: (name: string) => `test-${name}` }));

import type { PipelineNode } from 'distributed-core/applications/pipeline';
import type { LLMClient } from '../../pipeline/LLMClient';
import { createDeckDraftFromSourcesHandler, citeEverySource, sourceDocumentIds } from '../DeckDraftFromSourcesStep';
import { readFixtureMaterial } from '../deckFixture';
import { buildPresentationDeckDefinition } from '../presentationDeckDefinition';
import { getDraftableSystemPipeline, getSystemPipeline } from '../../pipeline/systemPipelines';
import type { DeckSpec } from '../DeckSpec';

const ROWS: Record<string, { createdBy: string; viewers?: string[] }> = {
  a: { createdBy: 'dev-connor' },
  b: { createdBy: 'dev-frank', viewers: ['dev-connor'] },
  c: { createdBy: 'dev-connor' },
  secret: { createdBy: 'dev-frank' },
};
const node = { id: 'draft', type: 'deck:draft-from-sources', data: {} } as unknown as PipelineNode;
const readMaterial: typeof readFixtureMaterial = (from, deps) => readFixtureMaterial(from, {
  ...deps,
  readDocument: async (id: string) => (id === 'c-broken' ? { error: 'no snapshot arrived' } : { text: `Facts from ${id}. Ship on Thursday.` }),
  readDocumentRow: async (id: string) => ({ title: `Doc ${id}` }),
});
function model(answer: string): LLMClient {
  return { provider: 'test', async *stream() { yield { done: true, response: answer, token: '' }; } } as unknown as LLMClient;
}
function handler(answer = '{}', extraRows: Record<string, { createdBy: string }> = {}) {
  return createDeckDraftFromSourcesHandler({
    access: { getDocumentRow: async (id) => ({ ...ROWS, ...extraRows }[id] ?? null), getLifecycleOwner: async () => undefined },
    readMaterial,
    fixtureOpts: { llm: model(answer) },
  });
}
const helpers = {} as never;

describe('deck:draft-from-sources', () => {
  it('parses run-draft and deck-ref sources, skipping other kinds, deduped', () => {
    expect(sourceDocumentIds([{ kind: 'document', id: 'a' }, { kind: 'transcript', id: 't' }, { documentId: 'b', title: 'B' }, { kind: 'document', id: 'a' }])).toEqual(['a', 'b']);
  });

  it('drafts one deck citing every readable source', async () => {
    const deck = { title: 'Review', slides: [
      { id: 'cover', layout: 'title', title: 'Review', sources: [{ documentId: 'a', title: 'Doc a' }] },
      { id: 'b', layout: 'bullets', title: 'B', bullets: ['Ship on Thursday'], sources: [{ documentId: 'b', title: 'Doc b' }] },
    ] };
    const out = await handler(JSON.stringify(deck)).execute(node, {
      sources: [{ kind: 'document', id: 'a' }, { kind: 'document', id: 'b' }, { kind: 'document', id: 'c' }],
      requestedBy: 'dev-connor', organizationId: 'dev-org', instruction: 'Sprint review',
    }, helpers);
    expect(out.status).toBe('completed');
    const output = out.output as { spec: DeckSpec; source: string; sources: Array<{ documentId: string }> };
    expect(output.source).toBe('model');
    expect(output.sources.map((s) => s.documentId)).toEqual(['a', 'b', 'c']);
    const cited = new Set(output.spec.slides.flatMap((s) => s.sources.map((x) => x.documentId)));
    expect([...cited].sort()).toEqual(['a', 'b', 'c']); // c was uncited by the model → added to the cover
  });

  it('one source the requester cannot read refuses the whole run, naming it', async () => {
    const out = await handler().execute(node, {
      sources: [{ kind: 'document', id: 'a' }, { kind: 'document', id: 'secret' }, { kind: 'document', id: 'nope' }],
      requestedBy: 'dev-connor', organizationId: 'dev-org',
    }, helpers);
    expect(out.status).toBe('failed');
    expect((out as { retry?: boolean }).retry).toBe(false);
    expect(out.error).toContain('secret, nope');
    expect(out.error).toContain('nothing was drafted');
  });

  it('a readable source whose text cannot be read refuses the run too', async () => {
    const out = await handler('{}', { 'c-broken': { createdBy: 'dev-connor' } }).execute(node, {
      sources: [{ kind: 'document', id: 'a' }, { kind: 'document', id: 'c-broken' }], requestedBy: 'dev-connor', organizationId: 'dev-org',
    }, helpers);
    expect(out.status).toBe('failed');
    expect(out.error).toContain('c-broken');
  });

  it('no sources or no requester refuses', async () => {
    expect((await handler().execute(node, { sources: [], requestedBy: 'dev-connor' }, helpers)).status).toBe('failed');
    expect((await handler().execute(node, { sources: [{ kind: 'document', id: 'a' }] }, helpers)).status).toBe('failed');
  });

  it('citeEverySource leaves a fully cited deck alone', () => {
    const spec = { title: 'T', slides: [{ id: 'c', layout: 'title', title: 'T', sources: [{ documentId: 'a', title: 'A' }] }] } as unknown as DeckSpec;
    expect(citeEverySource(spec, [{ documentId: 'a', title: 'A' }])).toBe(spec);
  });
});

describe('presentation-deck system pipeline', () => {
  it('is draftable; other system pipelines are not', () => {
    expect(getDraftableSystemPipeline('presentation-deck')).toMatchObject({ id: 'presentation-deck', kind: 'presentation', system: true });
    expect(getSystemPipeline('deck-revise')).not.toBeNull();
    expect(getDraftableSystemPipeline('deck-revise')).toBeNull();
    expect(getDraftableSystemPipeline('document-agent-edit')).toBeNull();
    expect(getDraftableSystemPipeline('nope')).toBeNull();
  });

  it('renders the draft step\'s spec', () => {
    const def = buildPresentationDeckDefinition() as { nodes: Array<{ id: string; type: string; data: { specFrom?: string } }> };
    expect(def.nodes.map((n) => n.type)).toEqual(['trigger', 'deck:draft-from-sources', 'deck']);
    expect(def.nodes[2]!.data.specFrom).toBe('steps.draft.spec');
  });

  describe('asked slide count and status words (Loop 38 ppt)', () => {
    function sequence(answers: string[]): { llm: LLMClient; prompts: string[] } {
      const prompts: string[] = [];
      let i = 0;
      const llm = {
        provider: 'test',
        async *stream(...args: unknown[]) {
          prompts.push(JSON.stringify(args));
          yield { done: true, response: answers[Math.min(i++, answers.length - 1)]!, token: '' };
        },
      } as unknown as LLMClient;
      return { llm, prompts };
    }
    const deckOf = (n: number, notes = 'Shipped on Thursday.') => JSON.stringify({ title: 'Sprint review', slides: Array.from({ length: n }, (_, k) => ({
      id: `s${k + 1}`, layout: k === 0 ? 'title' : 'bullets', title: `Part ${k + 1}`, notes,
      ...(k === 0 ? {} : { bullets: [`Facts from a, point ${k + 1}`] }), sources: [{ documentId: 'a', title: 'Doc a' }],
    })) });
    const withModel = (llm: LLMClient) => createDeckDraftFromSourcesHandler({
      access: { getDocumentRow: async (id) => ROWS[id] ?? null, getLifecycleOwner: async () => undefined },
      readMaterial: (from, deps) => readFixtureMaterial(from, { ...deps, readDocument: async () => ({ text: 'The login flow shipped on Thursday. Search is tracked. Billing needs an owner.' }), readDocumentRow: async (id) => ({ title: `Doc ${id}` }) }),
      fixtureOpts: { llm },
    });
    const ctx = (instruction: string) => ({ sources: [{ kind: 'document', id: 'a' }], requestedBy: 'dev-connor', instruction });

    it('a short first draft is asked again and measured 4 → 6', async () => {
      const { llm, prompts } = sequence([deckOf(4), deckOf(6)]);
      const out = await withModel(llm).execute(node, ctx('Create a 6-slide sprint review'), helpers);
      expect(out.status).toBe('completed');
      const output = out.output as { spec: DeckSpec; measured: string; slideCheck: unknown };
      expect(output.spec.slides).toHaveLength(6);
      expect(output.slideCheck).toEqual({ asked: 6, first: 4, final: 6 });
      expect(output.measured).toBe('Measured: slides 4 → 6 (asked 6)');
      expect(prompts).toHaveLength(2);
      expect(prompts[0]).toContain('EXACTLY 6 slides');
      expect(prompts[1]).toContain('It has 4 slides');
    });

    it('a draft that meets the count is asked once', async () => {
      const { llm, prompts } = sequence([deckOf(6)]);
      const out = await withModel(llm).execute(node, ctx('six slides on the sprint'), helpers);
      expect((out.output as { measured: string }).measured).toBe('Measured: slides 6 (asked 6)');
      expect(prompts).toHaveLength(1);
    });

    it('the drafter refusing to pad fails the run and says why', async () => {
      const { llm } = sequence([JSON.stringify({ refused: 'the notes cover three topics', supports: 3 })]);
      const out = await withModel(llm).execute(node, ctx('Create a 9-slide sprint review'), helpers);
      expect(out.status).toBe('failed');
      expect((out as { error: string }).error).toMatch(/asked for 9 slides; the sources support 3 distinct slides without inventing — the notes cover three topics/);
      expect((out as { error: string }).error).toMatch(/nothing was saved/);
    });

    it('still short after the second ask is refused, not padded', async () => {
      const { llm } = sequence([deckOf(4), deckOf(5)]);
      const out = await withModel(llm).execute(node, ctx('Create a 6-slide sprint review'), helpers);
      expect(out.status).toBe('failed');
      expect((out as { error: string }).error).toContain('Measured: slides 4 → 5 (asked 6)');
    });

    it('a substituted status verb is sent back once with the source\'s word', async () => {
      const { llm, prompts } = sequence([deckOf(3, 'Login was delivered on Thursday.'), deckOf(3, 'Login shipped on Thursday.')]);
      const out = await withModel(llm).execute(node, ctx('Sprint review'), helpers);
      expect(out.status).toBe('completed');
      expect(prompts[1]).toContain('“delivered” — the source says “shipped”');
      expect((out.output as { warnings: string[] }).warnings.join(' ')).not.toContain('delivered');
      const notes = (out.output as { spec: DeckSpec }).spec.slides.map((s) => s.notes).join(' ');
      expect(notes).not.toMatch(/delivered/);
    });

    it('a status verb still substituted after the second ask is a warning', async () => {
      const { llm } = sequence([deckOf(3, 'Login was delivered.'), deckOf(3, 'Login was delivered.')]);
      const out = await withModel(llm).execute(node, ctx('Sprint review'), helpers);
      expect((out.output as { warnings: string[] }).warnings.join(' ')).toMatch(/“delivered” — the source says “shipped”/);
    });

    it('over the slide cap is refused before drafting', async () => {
      const { llm, prompts } = sequence([deckOf(3)]);
      const out = await withModel(llm).execute(node, ctx('a 45-slide deck'), helpers);
      expect(out.status).toBe('failed');
      expect(prompts).toHaveLength(0);
    });
  });
});
