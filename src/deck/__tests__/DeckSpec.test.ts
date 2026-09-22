import {
  MAX_BULLETS_PER_SLIDE, MAX_BULLET_CHARS, MAX_SLIDES,
  assertDeckSpec, deckSources, parseDeckSpecJson, validateDeckSpec,
} from '../DeckSpec';
import { SOURCE_A, sampleDeck } from './fixtures';

const slide = (over: Record<string, unknown> = {}) => ({
  id: 's1', layout: 'bullets', title: 'A slide', bullets: ['One'], sources: [SOURCE_A], ...over,
});

describe('validateDeckSpec', () => {
  it('accepts a deck that uses every layout', () => {
    const result = validateDeckSpec(sampleDeck());
    expect(result.ok).toBe(true);
  });

  it('rejects a slide that cites nothing — the generator is grounded', () => {
    const result = validateDeckSpec({ title: 'Deck', slides: [slide({ sources: [] })] });
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error('unreachable');
    expect(result.errors.join(' ')).toContain('must cite at least one source');
  });

  it('rejects a source with no documentId', () => {
    const result = validateDeckSpec({ title: 'Deck', slides: [slide({ sources: [{ title: 'Somewhere' }] })] });
    expect(result.ok).toBe(false);
  });

  it(`rejects more than ${MAX_SLIDES} slides`, () => {
    const slides = Array.from({ length: MAX_SLIDES + 1 }, (_v, index) => slide({ id: `s${index + 1}` }));
    const result = validateDeckSpec({ title: 'Deck', slides });
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error('unreachable');
    expect(result.errors.join(' ')).toContain(`the cap is ${MAX_SLIDES}`);
  });

  it('accepts exactly the slide cap', () => {
    const slides = Array.from({ length: MAX_SLIDES }, (_v, index) => slide({ id: `s${index + 1}` }));
    expect(validateDeckSpec({ title: 'Deck', slides }).ok).toBe(true);
  });

  it('rejects too many bullets and over-long ones', () => {
    const many = validateDeckSpec({ title: 'Deck', slides: [slide({ bullets: Array.from({ length: MAX_BULLETS_PER_SLIDE + 1 }, (_v, i) => `bullet ${i}`) })] });
    expect(many.ok).toBe(false);
    const long = validateDeckSpec({ title: 'Deck', slides: [slide({ bullets: ['x'.repeat(MAX_BULLET_CHARS + 1)] })] });
    expect(long.ok).toBe(false);
    if (long.ok) throw new Error('unreachable');
    expect(long.errors.join(' ')).toContain(`longer than ${MAX_BULLET_CHARS} characters`);
  });

  it('rejects unknown fields rather than dropping them', () => {
    const result = validateDeckSpec({ title: 'Deck', speakerMode: true, slides: [slide()] });
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error('unreachable');
    expect(result.errors.join(' ')).toContain("unknown field 'speakerMode'");
  });

  it('rejects duplicate and malformed slide ids', () => {
    expect(validateDeckSpec({ title: 'Deck', slides: [slide(), slide()] }).ok).toBe(false);
    expect(validateDeckSpec({ title: 'Deck', slides: [slide({ id: 'a/b' })] }).ok).toBe(false);
  });

  it('requires the payload each layout is named after', () => {
    expect(validateDeckSpec({ title: 'Deck', slides: [slide({ layout: 'chart', bullets: undefined })] }).ok).toBe(false);
    expect(validateDeckSpec({ title: 'Deck', slides: [slide({ layout: 'quote', bullets: undefined })] }).ok).toBe(false);
    expect(validateDeckSpec({ title: 'Deck', slides: [slide({ layout: 'two-column', bullets: undefined })] }).ok).toBe(false);
  });

  it('refuses a chart whose series does not line up with its labels', () => {
    const result = validateDeckSpec({
      title: 'Deck',
      slides: [slide({ layout: 'chart', bullets: undefined, chart: { kind: 'bar', labels: ['a', 'b'], series: [{ name: 'n', values: [1] }] } })],
    });
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error('unreachable');
    expect(result.errors.join(' ')).toContain('1 values for 2 labels');
  });

  it('refuses a chart payload on a bullets slide', () => {
    const result = validateDeckSpec({
      title: 'Deck',
      slides: [slide({ chart: { kind: 'bar', labels: ['a'], series: [{ name: 'n', values: [1] }] } })],
    });
    expect(result.ok).toBe(false);
  });

  it('reports every problem at once', () => {
    const result = validateDeckSpec({ slides: [slide({ sources: [] })] });
    if (result.ok) throw new Error('unreachable');
    expect(result.errors.length).toBeGreaterThan(1);
  });

  it('assertDeckSpec throws with the problems listed', () => {
    expect(() => assertDeckSpec({ title: 'Deck', slides: [slide({ sources: [] })] })).toThrow(/invalid DeckSpec/);
  });
});

describe('parseDeckSpecJson', () => {
  it('reads a deck out of a fenced block with prose around it', () => {
    const text = 'Here you go:\n```json\n{"title":"Deck","slides":[]}\n```\nHope that helps.';
    expect(parseDeckSpecJson(text)).toEqual({ title: 'Deck', slides: [] });
  });

  it('reads a bare object', () => {
    expect(parseDeckSpecJson('{"title":"Deck"}')).toEqual({ title: 'Deck' });
  });

  it('answers undefined when there is nothing parseable', () => {
    expect(parseDeckSpecJson('no json here')).toBeUndefined();
    expect(parseDeckSpecJson('{ not json }')).toBeUndefined();
  });
});

describe('deckSources', () => {
  it('lists each cited document once, first citation first', () => {
    const sources = deckSources(sampleDeck());
    expect(sources.map((source) => source.documentId)).toEqual(['doc-sprint-review', 'doc-roadmap']);
  });
});
