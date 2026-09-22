import { MAX_SLIDES } from '../DeckSpec';
import { deckWarnings, previewDeckSpec, renderDeckPreview } from '../preview';
import { sampleDeck } from './fixtures';

const slide = (over: Record<string, unknown> = {}) => ({
  id: 's1', layout: 'bullets', title: 'A slide', bullets: ['One'],
  sources: [{ documentId: 'doc-a', title: 'Doc A' }], ...over,
});

describe('renderDeckPreview', () => {
  it('renders every slide the .pptx would have, Sources last, without storing anything', () => {
    const preview = renderDeckPreview(sampleDeck());
    expect(preview.slides).toHaveLength(9);
    expect(preview.slides[0]).toMatchObject({ id: 'cover', index: 1, title: 'Q4 platform review' });
    expect(preview.slides[8]).toMatchObject({ id: 'sources', index: 9, title: 'Sources' });
    for (const item of preview.slides) expect(item.svg.startsWith('<svg')).toBe(true);
  });

  it('is deterministic, so a debounced editor redraw produces the same bytes', () => {
    expect(renderDeckPreview(sampleDeck())).toEqual(renderDeckPreview(sampleDeck()));
  });
});

describe('deckWarnings', () => {
  it('says when a slide is crowded, wraps, or has no notes', () => {
    const warnings = deckWarnings({
      title: 'Deck',
      slides: [slide({
        id: 'crowded',
        title: 'A title long enough that the renderer will wrap it onto a second line',
        bullets: Array.from({ length: 8 }, (_v, index) => `bullet number ${index}`),
      })],
    } as never);
    expect(warnings.join(' ')).toContain('at the bullet cap');
    expect(warnings.join(' ')).toContain('wraps onto a second line');
    expect(warnings.join(' ')).toContain('no speaker notes');
  });

  it('says when the whole deck leans on one source', () => {
    expect(deckWarnings({ title: 'Deck', slides: [slide()] } as never).join(' ')).toContain('Every slide cites the same source');
  });

  it('says nothing about a well-formed deck beyond its own observations', () => {
    const warnings = deckWarnings(sampleDeck());
    expect(warnings.some((warning) => warning.includes('bullet cap'))).toBe(false);
  });
});

describe('previewDeckSpec', () => {
  it('separates "too big to be a deck" (413) from "not a deck" (400)', () => {
    const big = previewDeckSpec({ title: 'Deck', slides: Array.from({ length: MAX_SLIDES + 1 }, (_v, i) => slide({ id: `s${i}` })) });
    expect(big.ok).toBe(false);
    if (!big.ok) expect(big.status).toBe(413);

    const bad = previewDeckSpec({ title: 'Deck', slides: [slide({ sources: [] })] });
    expect(bad.ok).toBe(false);
    if (!bad.ok) {
      expect(bad.status).toBe(400);
      expect(bad.errors.join(' ')).toContain('must cite at least one source');
    }
  });

  it('answers with the validated spec beside the preview', () => {
    const outcome = previewDeckSpec(sampleDeck());
    expect(outcome.ok).toBe(true);
    if (outcome.ok) {
      expect(outcome.spec.slides).toHaveLength(8);
      expect(outcome.preview.slides).toHaveLength(9);
    }
  });
});
