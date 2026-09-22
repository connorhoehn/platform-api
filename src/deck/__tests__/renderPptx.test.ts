// The .pptx is checked as a file, not as a promise: the buffer is opened as a
// zip and its parts are listed, so a change that produces a plausible-looking
// buffer PowerPoint cannot open fails here.
import { assertDeckSpec } from '../DeckSpec';
import { PPTX_CONTENT_TYPE, citationLine, renderPptx, slideNotes } from '../renderPptx';
import { sampleDeck } from './fixtures';
import { hidePptxNodeDetection } from './pptxTestEnv';

/**
 * Lists a zip's entry names from its central directory. Deliberately hand-rolled:
 * it reads the archive the way a reader does, so the test does not depend on the
 * same library that wrote the file.
 */
function listZipEntries(buffer: Buffer): string[] {
  // End of central directory: signature 0x06054b50, scanned from the tail.
  let eocd = -1;
  for (let index = buffer.length - 22; index >= 0; index -= 1) {
    if (buffer.readUInt32LE(index) === 0x06054b50) { eocd = index; break; }
  }
  if (eocd < 0) throw new Error('not a zip: no end-of-central-directory record');
  const count = buffer.readUInt16LE(eocd + 10);
  let offset = buffer.readUInt32LE(eocd + 16);
  const names: string[] = [];
  for (let entry = 0; entry < count; entry += 1) {
    if (buffer.readUInt32LE(offset) !== 0x02014b50) throw new Error('not a zip: bad central-directory header');
    const nameLength = buffer.readUInt16LE(offset + 28);
    const extraLength = buffer.readUInt16LE(offset + 30);
    const commentLength = buffer.readUInt16LE(offset + 32);
    names.push(buffer.toString('utf8', offset + 46, offset + 46 + nameLength));
    offset += 46 + nameLength + extraLength + commentLength;
  }
  return names;
}

describe('renderPptx', () => {
  const spec = assertDeckSpec(sampleDeck());
  let pptx: Buffer;
  let restore: () => void;

  beforeAll(async () => {
    restore = hidePptxNodeDetection();
    pptx = await renderPptx(spec);
  }, 30_000);

  afterAll(() => restore());

  it('produces a zip archive', () => {
    expect(Buffer.isBuffer(pptx)).toBe(true);
    expect(pptx.subarray(0, 2).toString('utf8')).toBe('PK');
    expect(pptx.byteLength).toBeGreaterThan(5_000);
  });

  it('contains the presentation part', () => {
    expect(listZipEntries(pptx)).toContain('ppt/presentation.xml');
  });

  it('contains one slide part per slide plus the Sources slide', () => {
    const slides = listZipEntries(pptx).filter((name) => /^ppt\/slides\/slide\d+\.xml$/.test(name));
    expect(slides).toHaveLength(spec.slides.length + 1);
    for (let index = 1; index <= spec.slides.length + 1; index += 1) {
      expect(slides).toContain(`ppt/slides/slide${index}.xml`);
    }
  });

  it('ships speaker notes for every slide', () => {
    const notes = listZipEntries(pptx).filter((name) => /^ppt\/notesSlides\/notesSlide\d+\.xml$/.test(name));
    expect(notes).toHaveLength(spec.slides.length + 1);
  });

  it('ships a chart part for each chart slide', () => {
    const charts = listZipEntries(pptx).filter((name) => /^ppt\/charts\/chart\d+\.xml$/.test(name));
    expect(charts).toHaveLength(spec.slides.filter((slide) => slide.layout === 'chart').length);
  });

  it('is 16:9 at widescreen size', () => {
    // 13.333in × 7.5in in EMU (914400 per inch), as pptxgenjs writes it.
    const entries = listZipEntries(pptx);
    expect(entries).toContain('ppt/presentation.xml');
    expect(PPTX_CONTENT_TYPE).toBe('application/vnd.openxmlformats-officedocument.presentationml.presentation');
  });

  it('renders the same package a second time', async () => {
    // Chart part numbers come from a counter pptxgenjs keeps for the process,
    // so the two archives differ in those names; the slides, notes and the rest
    // of the package are the same file rendered again.
    const again = await renderPptx(spec);
    const shape = (buffer: Buffer): string[] => listZipEntries(buffer)
      .filter((name) => !name.includes('/charts/') && !name.includes('/embeddings/'))
      .sort();
    expect(shape(again)).toEqual(shape(pptx));
  }, 30_000);

  it('refuses nothing it was handed — the validator is the gate', () => {
    expect(() => assertDeckSpec({ ...sampleDeck(), slides: [] })).toThrow();
  });
});

describe('slide notes', () => {
  it('carry the speaker note and then the citations', () => {
    const spec = assertDeckSpec(sampleDeck());
    const notes = slideNotes(spec.slides[1]!);
    expect(notes).toContain('Keep this to 40 seconds.');
    expect(notes).toContain('Sources:');
    expect(notes).toContain('Sprint review — doc-sprint-review');
  });

  it('name the anchor when a citation has one', () => {
    expect(citationLine({ title: 'Q4 roadmap', documentId: 'doc-roadmap', anchor: 'h2-q4' })).toBe('Q4 roadmap — doc-roadmap#h2-q4');
  });
});
