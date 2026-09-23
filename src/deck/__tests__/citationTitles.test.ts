// Citations stored with a call's wire id are named from the recording when a
// spec is handed back to an editor; nothing else about the spec changes.

jest.mock('../../lib/aws-clients', () => ({ docClient: { send: jest.fn(async () => ({})) }, ddbClient: { send: jest.fn() } }));
jest.mock('../../lib/ddb-table-name', () => ({ tableName: (name: string) => `test-${name}` }));

import type { DeckSpec } from '../DeckSpec';
import { healCitationTitles, isWireTitle } from '../citationTitles';

const spec = (title: string): DeckSpec => ({
  title: 'Deck',
  theme: 'product',
  slides: [
    { id: 'a', layout: 'title', title: 'A', sources: [{ documentId: 'tr-1', title }] },
    { id: 'b', layout: 'bullets', title: 'B', bullets: ['x'], sources: [{ documentId: 'doc-1', title: 'Release plan' }, { documentId: 'tr-1', title }] },
  ],
});

describe('healCitationTitles', () => {
  it('names a wire-titled citation from its recording, in the spec and the revision source list', async () => {
    const lookup = jest.fn(async (id: string) => (id === 'tr-1' ? 'Call with Connor Hoehn & Frank Davis' : null));
    const healed = await healCitationTitles(
      spec('dm:dev-connor:dev-frank'),
      [{ documentId: 'tr-1', title: 'dm:dev-connor:dev-frank', kind: 'document' as const }],
      lookup,
    );
    expect(healed.spec.slides.flatMap((slide) => slide.sources.map((source) => source.title)))
      .toEqual(['Call with Connor Hoehn & Frank Davis', 'Release plan', 'Call with Connor Hoehn & Frank Davis']);
    expect(healed.sources).toEqual([{ documentId: 'tr-1', title: 'Call with Connor Hoehn & Frank Davis', kind: 'document' }]);
    expect(lookup).toHaveBeenCalledTimes(1);
  });

  it('does no lookup when nothing is wire-titled', async () => {
    const lookup = jest.fn(async () => 'never');
    const input = spec('Call with Connor Hoehn & Frank Davis');
    const healed = await healCitationTitles(input, [], lookup);
    expect(healed.spec).toBe(input);
    expect(lookup).not.toHaveBeenCalled();
  });

  it('keeps the stored title when the recording cannot be found or the lookup fails', async () => {
    expect((await healCitationTitles(spec('dm:a:b'), [], async () => null)).spec.slides[0]!.sources[0]!.title).toBe('dm:a:b');
    expect((await healCitationTitles(spec('dm:a:b'), [], async () => { throw new Error('ddb down'); })).spec.slides[0]!.sources[0]!.title).toBe('dm:a:b');
  });

  it('knows a wire id from a title a person wrote', () => {
    expect(isWireTitle('dm:dev-connor:dev-frank')).toBe(true);
    expect(isWireTitle('room:design')).toBe(true);
    expect(isWireTitle('Planning sync: Q3')).toBe(false);
    expect(isWireTitle('Call with Connor Hoehn & Frank Davis')).toBe(false);
  });
});
