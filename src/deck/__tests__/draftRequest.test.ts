import { requestedSlideCount, slideCountLine, statusDrift } from '../draftRequest';
import type { DeckSpec } from '../DeckSpec';

describe('requestedSlideCount', () => {
  it.each([
    ['Create a 6-slide sprint review from these', 6],
    ['six slides on the release', 6],
    ['Make it exactly 8 slides', 8],
    ['a ten slide deck', 10],
    ['in 1 slide', 1],
  ])('%s → %s', (text, n) => expect(requestedSlideCount(text)).toBe(n));

  it.each([
    'Create a sprint review', 'add 2 slides', 'at most 6 slides', '2 more slides', 'a 6-slide or 8-slide deck', 'slide 3 title',
  ])('%s → none', (text) => expect(requestedSlideCount(text)).toBeUndefined());
});

describe('statusDrift', () => {
  const spec = (notes: string): DeckSpec => ({ title: 'Review', slides: [{ id: 'cover', layout: 'title', title: 'Review', notes, sources: [{ documentId: 'a', title: 'A' }] }] } as DeckSpec);
  it('flags a status verb the material does not use, naming the one it does', () => {
    expect(statusDrift(spec('Login was delivered.'), 'Login shipped Thursday.')).toEqual([{ slideId: 'cover', field: 'notes', word: 'delivered', sourceSays: ['shipped'] }]);
  });
  it('keeps the material\'s own word and a word the requester used', () => {
    expect(statusDrift(spec('Login shipped.'), 'Login shipped Thursday.')).toEqual([]);
    expect(statusDrift(spec('What was delivered'), 'Login shipped.', ['What was delivered this sprint'])).toEqual([]);
  });
  it('reads the measured line', () => {
    expect(slideCountLine(6, 4, 6)).toBe('Measured: slides 4 → 6 (asked 6)');
    expect(slideCountLine(6, 6, 6)).toBe('Measured: slides 6 (asked 6)');
  });
});
