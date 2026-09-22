// The fixture path, driven with fake readers and a scripted model: real
// material in, a cited deck out — and an honest fallback when the model cannot
// answer or answers with something it was not given.

jest.mock('../../lib/aws-clients', () => ({ docClient: { send: jest.fn(async () => ({})) }, ddbClient: { send: jest.fn() } }));
jest.mock('../../lib/ddb-table-name', () => ({ tableName: (name: string) => `test-${name}` }));

import type { LLMClient } from '../../pipeline/LLMClient';
import { validateDeckSpec } from '../DeckSpec';
import {
  FIXTURE_TITLE_PREFIX, NoFixtureMaterialError,
  buildDeckFixture, citableSources, fixtureSpec, materialBlock, readFixtureMaterial,
  type FixtureMaterial,
} from '../deckFixture';

const RECORDING = {
  recordingId: 'rec-1',
  transcriptId: 'active-now-2026-09-21-planning-transcript',
  lobbyName: 'Planning sync',
  transcriptJson: JSON.stringify({
    segments: [
      { id: 'segment-1', at: '2026-09-21T08:42:00.000Z', speaker: 'Connor', text: 'Release risks before noon.' },
      { id: 'segment-2', at: '2026-09-21T08:47:00.000Z', speaker: 'Frank', text: 'Agreed. I will take the rollback owner for the gateway change.' },
      { id: 'segment-3', at: '2026-09-21T08:53:00.000Z', speaker: 'Connor', text: 'Then the deck needs an owner and a mitigation per risk.' },
    ],
    actionItems: ['Add an owner and mitigation for each release risk'],
  }),
};

const deps = {
  readDocument: async (documentId: string) => ({ text: `Release plan. Ship the gateway change on Thursday. Two risks remain open for ${documentId}.` }),
  readDocumentRow: async () => ({ title: 'Release plan' }),
  findRecording: async (recordingId: string) => (recordingId === 'rec-1' ? RECORDING : null),
};

function scriptedModel(answer: string | Error): LLMClient {
  return {
    provider: 'test',
    async *stream() {
      if (answer instanceof Error) throw answer;
      yield { done: true, response: answer, token: '', tokensIn: 10, tokensOut: 20 };
    },
  } as unknown as LLMClient;
}

describe('readFixtureMaterial', () => {
  it('reads the named document and the named recording’s transcript', async () => {
    const material = await readFixtureMaterial({ documentId: 'doc-release-plan', recordingId: 'rec-1' }, deps);
    expect(material.document).toMatchObject({ documentId: 'doc-release-plan', title: 'Release plan' });
    expect(material.document!.text).toContain('Ship the gateway change');
    expect(material.transcript).toMatchObject({ transcriptId: RECORDING.transcriptId, title: 'Planning sync' });
    expect(material.transcript!.segments).toHaveLength(3);
  });

  it('reports a document it could not read instead of pretending it is empty', async () => {
    const material = await readFixtureMaterial({ documentId: 'doc-x' }, {
      ...deps,
      readDocument: async () => ({ error: 'no snapshot arrived' }),
      readDocumentRow: async () => ({ title: 'Doc X' }),
    });
    expect(material.document!.unreadable).toBe('no snapshot arrived');
    expect(materialBlock(material)).toContain('could not be read: no snapshot arrived');
  });

  it('leaves the transcript out when the recording has none', async () => {
    const material = await readFixtureMaterial({ recordingId: 'rec-none' }, deps);
    expect(material.transcript).toBeUndefined();
    expect(citableSources(material)).toEqual([]);
  });
});

describe('fixtureSpec', () => {
  it('folds the material into a cited deck, titled as the fixture it is', async () => {
    const material = await readFixtureMaterial({ documentId: 'doc-release-plan', recordingId: 'rec-1' }, deps);
    const spec = fixtureSpec(material, 'Sprint review');
    expect(spec.title.startsWith(FIXTURE_TITLE_PREFIX)).toBe(true);
    expect(validateDeckSpec(spec).ok).toBe(true);
    for (const slide of spec.slides) expect(slide.sources.length).toBeGreaterThan(0);
    const quote = spec.slides.find((slide) => slide.layout === 'quote');
    // The quote is something a named person actually said in the transcript.
    expect(RECORDING.transcriptJson).toContain(quote!.quote!.text);
    const chart = spec.slides.find((slide) => slide.layout === 'chart');
    expect(chart!.chart).toMatchObject({ labels: ['Connor', 'Frank'], series: [{ name: 'Segments', values: [2, 1] }] });
  });

  it('is deterministic', async () => {
    const material = await readFixtureMaterial({ recordingId: 'rec-1' }, deps);
    expect(fixtureSpec(material, 'T')).toEqual(fixtureSpec(material, 'T'));
  });

  it('refuses to invent a deck out of nothing', () => {
    expect(() => fixtureSpec({} as FixtureMaterial, 'T')).toThrow(NoFixtureMaterialError);
  });
});

describe('buildDeckFixture', () => {
  const modelDeck = {
    title: 'Sprint review',
    slides: [
      { id: 'cover', layout: 'title', title: 'Sprint review', sources: [{ documentId: 'doc-release-plan', title: 'Release plan' }] },
      { id: 'risks', layout: 'bullets', title: 'Open risks', bullets: ['Two risks remain open'], sources: [{ documentId: 'doc-release-plan', title: 'Release plan' }] },
    ],
  };

  it('uses the model’s deck when it validates and cites only what it was given', async () => {
    const result = await buildDeckFixture({ documentId: 'doc-release-plan' }, 'Sprint review', {
      ...deps, llm: scriptedModel(`\`\`\`json\n${JSON.stringify(modelDeck)}\n\`\`\``),
    });
    expect(result.source).toBe('model');
    expect(result.reason).toBeUndefined();
    expect(result.spec.slides).toHaveLength(2);
  });

  it('falls back with a reason when the model invents a source', async () => {
    const invented = { ...modelDeck, slides: [{ ...modelDeck.slides[0], sources: [{ documentId: 'doc-imaginary', title: 'Nope' }] }] };
    const result = await buildDeckFixture({ documentId: 'doc-release-plan' }, 'Sprint review', {
      ...deps, llm: scriptedModel(JSON.stringify(invented)),
    });
    expect(result.source).toBe('fixture');
    expect(result.reason).toContain("cited 'doc-imaginary'");
    expect(result.spec.title.startsWith(FIXTURE_TITLE_PREFIX)).toBe(true);
  });

  it('falls back with a reason when the model is unavailable — never silently', async () => {
    const result = await buildDeckFixture({ recordingId: 'rec-1' }, 'Planning sync', {
      ...deps, llm: scriptedModel(new Error('connect ECONNREFUSED')),
    });
    expect(result.source).toBe('fixture');
    expect(result.reason).toContain('ECONNREFUSED');
  });

  it('falls back when no model is configured at all', async () => {
    const result = await buildDeckFixture({ recordingId: 'rec-1' }, undefined, { ...deps, llm: null });
    expect(result.source).toBe('fixture');
    expect(result.reason).toBe('no model is configured on this deployment');
    expect(result.material.transcript!.segments).toHaveLength(3);
  });

  it('refuses when nothing readable was named', async () => {
    await expect(buildDeckFixture({ recordingId: 'rec-none' }, undefined, { ...deps, llm: null }))
      .rejects.toBeInstanceOf(NoFixtureMaterialError);
  });
});
