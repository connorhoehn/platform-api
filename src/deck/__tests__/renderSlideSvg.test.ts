import { assertDeckSpec } from '../DeckSpec';
import { escapeXml, renderAllSlideSvgs, renderSlideSvg, wrapText } from '../renderSlideSvg';
import { SVG_HEIGHT, SVG_WIDTH } from '../theme';
import { sampleDeck } from './fixtures';

/** The same inertness rule the work-graph preview route applies before serving markup. */
function isInertSvg(markup: string): boolean {
  const lowered = markup.toLowerCase();
  if (lowered.includes('<script') || lowered.includes('<foreignobject') || lowered.includes('<use')) return false;
  if (/\son[a-z]+\s*=/.test(lowered)) return false;
  if (/(href|src)\s*=\s*["']?\s*(?!#)/.test(lowered)) return false;
  return lowered.includes('<svg');
}

describe('renderSlideSvg', () => {
  const spec = assertDeckSpec(sampleDeck());

  it('renders one SVG per slide plus the Sources slide', () => {
    const all = renderAllSlideSvgs(spec);
    expect(all).toHaveLength(spec.slides.length + 1);
    for (const svg of all) {
      expect(svg.startsWith('<svg')).toBe(true);
      expect(svg.endsWith('</svg>')).toBe(true);
      expect(svg).toContain(`viewBox="0 0 ${SVG_WIDTH} ${SVG_HEIGHT}"`);
    }
  });

  it('passes the preview route’s inertness check', () => {
    for (const svg of renderAllSlideSvgs(spec)) expect(isInertSvg(svg)).toBe(true);
  });

  it('is deterministic', () => {
    expect(renderSlideSvg(spec, 3)).toBe(renderSlideSvg(spec, 3));
    expect(renderAllSlideSvgs(spec)).toEqual(renderAllSlideSvgs(assertDeckSpec(sampleDeck())));
  });

  it('carries the slide title, the eyebrow and the footer position', () => {
    const svg = renderSlideSvg(spec, 1);
    expect(svg).toContain('Agenda');
    expect(svg).toContain('Q4 platform review · 2 / 9');
  });

  it('draws bars for a bar chart and a polyline for a line chart', () => {
    expect(renderSlideSvg(spec, 3)).toContain('<rect');
    expect(renderSlideSvg(spec, 4)).toContain('<polyline');
  });

  it('closes on the Sources slide, listing every cited document', () => {
    const sources = renderSlideSvg(spec, spec.slides.length);
    expect(sources).toContain('Sources');
    expect(sources).toContain('doc-sprint-review');
    expect(sources).toContain('doc-roadmap#h2-q4');
    expect(sources).toContain('Q4 platform review · 9 / 9');
  });

  it('refuses a slide index the deck does not have', () => {
    expect(() => renderSlideSvg(spec, -1)).toThrow(RangeError);
    expect(() => renderSlideSvg(spec, spec.slides.length + 1)).toThrow(RangeError);
  });

  it('escapes text rather than letting it become markup', () => {
    const risky = assertDeckSpec({
      title: '<b>Deck</b> & "friends"',
      slides: [{ id: 'a', layout: 'bullets', title: 'A & B', bullets: ['<img onerror="x">'], sources: [{ documentId: 'd1', title: 'Doc' }] }],
    });
    const svg = renderSlideSvg(risky, 0);
    expect(svg).toContain('&lt;img onerror&#61;&quot;x&quot;&gt;');
    expect(isInertSvg(svg)).toBe(true);
  });
});

describe('wrapText', () => {
  it('breaks on words and keeps every word', () => {
    const lines = wrapText('the quick brown fox jumps over the lazy dog', 18, 200);
    expect(lines.length).toBeGreaterThan(1);
    expect(lines.join(' ').split(/\s+/)).toHaveLength(9);
  });

  it('cuts a word that is longer than the line rather than hiding it', () => {
    const lines = wrapText('x'.repeat(120), 18, 200);
    expect(lines.length).toBeGreaterThan(1);
    expect(lines.join('').replace(/-/g, '')).toHaveLength(120);
  });
});

describe('escapeXml', () => {
  it('escapes the five XML characters', () => {
    expect(escapeXml(`<&>"'`)).toBe('&lt;&amp;&gt;&quot;&apos;');
  });

  it("escapes '=' too, so quoted markup cannot trip the route's inertness check", () => {
    expect(escapeXml('onerror="x"')).toBe('onerror&#61;&quot;x&quot;');
  });
});
