// The agent composer's edit path, driven with fake readers and a scripted
// model: the model sees the real source text, a slide-scoped edit touches only
// that slide and keeps its id, and anything the model gets wrong is refused
// with the reason — the deck is never edited on its behalf.

jest.mock('../../lib/aws-clients', () => ({ docClient: { send: jest.fn(async () => ({})) }, ddbClient: { send: jest.fn() } }));
jest.mock('../../lib/ddb-table-name', () => ({ tableName: (name: string) => `test-${name}` }));

import type { LLMClient } from '../../pipeline/LLMClient';
import type { DeckSpec } from '../DeckSpec';
import { DeckReviseError, changedSlides, reviseDeck, validateReviseInputSpec } from '../deckRevise';

const TRANSCRIPT_ID = 'active-now-2026-09-22-planning-transcript';
const RECORDING = {
  recordingId: 'rec-1',
  transcriptId: TRANSCRIPT_ID,
  participants: [{ displayName: 'Connor Hoehn' }, { displayName: 'Frank Davis' }],
  transcriptJson: JSON.stringify({ segments: [
    { speaker: 'Connor', text: 'The rollback for the gateway change is the release risk nobody owns.' },
    { speaker: 'Frank', text: 'I will own the rollback plan.' },
  ] }),
};

const deps = {
  readDocument: async (documentId: string) => (documentId === 'doc-plan'
    ? { text: 'Release plan. Ship Thursday. Risk: gateway rollback is untested.' }
    : { text: '' }),
  readDocumentRow: async (documentId: string) => (documentId === 'doc-plan' ? { title: 'Release plan' } : null),
  findRecording: async (recordingId: string) => (recordingId === 'rec-1' ? RECORDING : null),
  findRecordingIdByTranscript: async (id: string) => (id === TRANSCRIPT_ID ? 'rec-1' : null),
};

function deck(): DeckSpec {
  return {
    title: 'Release review',
    theme: 'product',
    slides: [
      { id: 'cover', layout: 'title', title: 'Release review', sources: [{ documentId: 'doc-plan', title: 'Release plan' }] },
      { id: 'transcript', layout: 'bullets', title: 'What the planning call decided about the release', bullets: ['Ship Thursday'], sources: [{ documentId: TRANSCRIPT_ID, title: 'Call with Connor Hoehn & Frank Davis' }] },
    ],
  };
}

function scripted(answer: string | Error, seen: string[] = []): LLMClient {
  return {
    provider: 'test',
    async *stream(_model: string, _system: string, prompt: string) {
      seen.push(prompt);
      if (answer instanceof Error) throw answer;
      yield { done: true, response: answer, token: '', tokensIn: 1, tokensOut: 1 };
    },
  } as unknown as LLMClient;
}

describe('reviseDeck — one slide', () => {
  it('edits only the named slide, keeps its id, and grounds the prompt in the real transcript', async () => {
    const prompts: string[] = [];
    const answer = JSON.stringify({
      summary: 'Shortened the title and added a release-risk bullet.',
      slide: {
        id: 'renamed-by-model', layout: 'bullets', title: 'Call decisions',
        bullets: ['Ship Thursday', 'Risk: the gateway rollback has no owner yet'],
        sources: [{ documentId: TRANSCRIPT_ID, title: 'whatever the model said' }],
      },
    });
    const result = await reviseDeck(
      { spec: deck(), instruction: 'shorter title, add a bullet on release risks', slideId: 'transcript' },
      { ...deps, llm: scripted(answer, prompts) },
    );
    expect(result.source).toBe('model');
    expect(result.changedSlideIds).toEqual(['transcript']);
    expect(result.spec.slides.map((slide) => slide.id)).toEqual(['cover', 'transcript']);
    expect(result.spec.slides[0]).toEqual(deck().slides[0]);
    expect(result.spec.slides[1]!.title).toBe('Call decisions');
    // The citation keeps the title the deck already shows for it.
    expect(result.spec.slides[1]!.sources[0]!.title).toBe('Call with Connor Hoehn & Frank Davis');
    expect(result.summary).toMatch(/Shortened/);
    // A source saved as a "document" that is really a transcript is read as one.
    expect(prompts[0]).toContain('nobody owns');
    expect(prompts[0]).toContain('Ship Thursday. Risk: gateway rollback is untested.');
    expect(prompts[0]).toContain('"id": "transcript"');
  });

  it('refuses a slide that cites a source it was not given', async () => {
    const answer = JSON.stringify({ summary: 'x', slide: { id: 'transcript', layout: 'bullets', title: 'T', bullets: ['a'], sources: [{ documentId: 'doc-invented', title: 'Invented' }] } });
    await expect(reviseDeck({ spec: deck(), instruction: 'x', slideId: 'transcript' }, { ...deps, llm: scripted(answer) }))
      .rejects.toMatchObject({ status: 502, reason: expect.stringContaining('doc-invented') });
  });

  it('refuses an edit that breaks the DeckSpec rules (an uncited slide), listing why', async () => {
    const answer = JSON.stringify({ summary: 'x', slide: { id: 'transcript', layout: 'bullets', title: 'T', bullets: ['a'], sources: [] } });
    const error = await reviseDeck({ spec: deck(), instruction: 'x', slideId: 'transcript' }, { ...deps, llm: scripted(answer) }).catch((err: unknown) => err);
    expect(error).toBeInstanceOf(DeckReviseError);
    expect((error as DeckReviseError).status).toBe(502);
    expect((error as DeckReviseError).errors.join(' ')).toMatch(/every slide must cite/);
  });
});

describe('reviseDeck — the whole deck', () => {
  it('returns the new deck with changed and new slides named', async () => {
    const next = deck();
    next.slides[0] = { ...next.slides[0]!, title: 'Release review — Thursday' };
    next.slides.push({ id: 'risks', layout: 'bullets', title: 'Release risks', bullets: ['Gateway rollback is untested'], sources: [{ documentId: 'doc-plan', title: 'Release plan' }] });
    const result = await reviseDeck({ spec: deck(), instruction: 'add a risks slide' }, { ...deps, llm: scripted(JSON.stringify({ summary: 'Added a risks slide.', spec: next })) });
    expect(result.changedSlideIds).toEqual(['cover', 'risks']);
    expect(result.warnings).toEqual([]);
  });

  it('says so when the model removes slides', async () => {
    const next = deck();
    next.slides.pop();
    const result = await reviseDeck({ spec: deck(), instruction: 'cut it down' }, { ...deps, llm: scripted(JSON.stringify({ summary: 'Cut.', spec: next })) });
    expect(result.warnings.join(' ')).toMatch(/Removed slide/);
  });

  it('is an honest error — not a fake edit — when the model fails or is missing', async () => {
    await expect(reviseDeck({ spec: deck(), instruction: 'x' }, { ...deps, llm: scripted(new Error('credentials expired')) }))
      .rejects.toMatchObject({ status: 503, reason: 'credentials expired' });
    await expect(reviseDeck({ spec: deck(), instruction: 'x' }, { ...deps, llm: null }))
      .rejects.toMatchObject({ status: 503 });
    await expect(reviseDeck({ spec: deck(), instruction: 'x' }, { ...deps, llm: scripted('I would rather not.') }))
      .rejects.toMatchObject({ status: 502 });
  });

  it('refuses when there is nothing to ground an edit in', async () => {
    const spec = { title: 'Blank', slides: [{ id: 'cover', layout: 'title' as const, title: 'Blank', sources: [] }] };
    await expect(reviseDeck({ spec, instruction: 'x' }, { ...deps, llm: scripted('{}') })).rejects.toMatchObject({ status: 400 });
  });

  it('reads a source the editor added, and lets the model cite it on a blank deck', async () => {
    const spec = { title: 'Blank', slides: [{ id: 'cover', layout: 'title' as const, title: 'Blank', sources: [] }] };
    const next = { title: 'Release', slides: [{ id: 'cover', layout: 'title', title: 'Release', sources: [{ documentId: 'doc-plan', title: 'Release plan' }] }] };
    const result = await reviseDeck(
      { spec, instruction: 'build it from the plan', sources: [{ kind: 'document', id: 'doc-plan' }] },
      { ...deps, llm: scripted(JSON.stringify({ summary: 'Built.', spec: next })) },
    );
    expect(result.spec.slides[0]!.sources[0]).toEqual({ documentId: 'doc-plan', title: 'Release plan' });
  });
});

describe('helpers', () => {
  it('validateReviseInputSpec lets missing citations through and nothing else', () => {
    const ok = validateReviseInputSpec({ title: 'T', slides: [{ id: 'a', layout: 'title', title: 'A', sources: [] }] });
    expect(ok.ok && ok.spec.slides[0]!.sources).toEqual([]);
    const bad = validateReviseInputSpec({ title: 'T', slides: [{ id: 'a', layout: 'nope', title: 'A', sources: [] }] });
    expect(bad.ok).toBe(false);
  });

  it('changedSlides names edited and new slides only', () => {
    const next = deck();
    next.slides[1] = { ...next.slides[1]!, bullets: ['Ship Friday'] };
    expect(changedSlides(deck(), next)).toEqual(['transcript']);
  });
});
