// The preview of a slide, as SVG, from the same geometry the .pptx is built
// from — so what the Artifact pane shows is where the slide actually is.
//
// Three properties this file is written to keep:
//   - INERT. No <script>, no <foreignObject>, no <use>, no event handlers, and
//     no href/src of any kind, so the markup passes the work-graph preview
//     route's own inertness check and can be served to an authorized viewer.
//   - OFFLINE. No font is fetched and no asset is embedded; text renders in a
//     stack of families every OS already has.
//   - DETERMINISTIC. Same spec, same index, same bytes: no clock, no random,
//     no id counter that depends on call order. Previews are cacheable and
//     diffable, and a test can assert on them.

import {
  deckSources,
  type DeckSlide,
  type DeckSpec,
} from './DeckSpec';
import {
  ACCENT_HEIGHT_IN, ACCENT_WIDTH_IN, ACCENT_Y_IN,
  BODY_HEIGHT_IN, BODY_Y_IN, CONTENT_WIDTH_IN,
  EYEBROW_Y_IN, FOOTER_Y_IN, MARGIN_IN,
  SOURCES_SLIDE_TITLE, SVG_HEIGHT, SVG_SCALE, SVG_WIDTH,
  TITLE_Y_IN, footerText, paletteFor, type DeckPalette,
} from './theme';

export const SVG_CONTENT_TYPE = 'image/svg+xml';

/** Inches → preview pixels. */
const px = (inches: number): number => Math.round(inches * SVG_SCALE * 100) / 100;

const MARGIN = px(MARGIN_IN);
const CONTENT_WIDTH = px(CONTENT_WIDTH_IN);

/**
 * Escapes text for SVG — and escapes '=' along with the five XML characters.
 *
 * The '=' is not an XML problem; it is an inertness one. The preview route
 * refuses any markup matching ` on<word>=` or `href=`, and it applies that test
 * to the whole document, so a bullet that merely QUOTES `<img onerror="…">`
 * would get the slide refused even though the angle brackets are escaped.
 * `&#61;` renders as '=' in every browser and matches neither pattern.
 */
export function escapeXml(value: string): string {
  return value
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&apos;')
    .replace(/=/g, '&#61;');
}

/**
 * Greedy wrap by estimated width. SVG has no layout engine to ask, and this
 * renderer refuses to load a font to measure with, so widths come from a
 * per-size average advance — good to a few percent for the Latin text a slide
 * carries, and identical on every machine.
 */
export function wrapText(text: string, fontSize: number, maxWidth: number): string[] {
  const perChar = fontSize * 0.52;
  const maxChars = Math.max(8, Math.floor(maxWidth / perChar));
  const lines: string[] = [];
  let line = '';
  for (const word of text.split(/\s+/).filter(Boolean)) {
    const candidate = line ? `${line} ${word}` : word;
    if (candidate.length <= maxChars) { line = candidate; continue; }
    if (line) lines.push(line);
    // A single word longer than the line is cut on the character, not hidden.
    if (word.length > maxChars) {
      let rest = word;
      while (rest.length > maxChars) { lines.push(rest.slice(0, maxChars - 1) + '-'); rest = rest.slice(maxChars - 1); }
      line = rest;
    } else {
      line = word;
    }
  }
  if (line) lines.push(line);
  return lines;
}

interface TextOpts {
  x: number; y: number; size: number; color: string; weight?: number; italic?: boolean;
  anchor?: 'start' | 'middle' | 'end'; letterSpacing?: number; family: string;
}

function textEl(content: string, opts: TextOpts): string {
  const attrs = [
    `x="${opts.x}"`, `y="${opts.y}"`,
    `font-family="${opts.family}"`,
    `font-size="${opts.size}"`,
    opts.weight ? `font-weight="${opts.weight}"` : '',
    opts.italic ? 'font-style="italic"' : '',
    `fill="${opts.color}"`,
    opts.anchor ? `text-anchor="${opts.anchor}"` : '',
    opts.letterSpacing ? `letter-spacing="${opts.letterSpacing}"` : '',
  ].filter(Boolean).join(' ');
  return `<text ${attrs}>${escapeXml(content)}</text>`;
}

function paragraph(lines: string[], opts: TextOpts & { lineHeight: number }): string {
  return lines
    .map((line, index) => textEl(line, { ...opts, y: opts.y + index * opts.lineHeight }))
    .join('');
}

function bulletsSvg(bullets: string[], palette: DeckPalette, box: { x: number; y: number; w: number }): string {
  const size = 18;
  const lineHeight = size * 1.45;
  let cursor = box.y;
  const parts: string[] = [];
  for (const bullet of bullets) {
    const lines = wrapText(bullet, size, box.w - 26);
    parts.push(`<circle cx="${box.x + 6}" cy="${cursor - 6}" r="4" fill="${palette.accent}"/>`);
    parts.push(paragraph(lines, {
      x: box.x + 26, y: cursor, size, color: palette.text, family: palette.svgFontStack, lineHeight,
    }));
    cursor += lines.length * lineHeight + 14;
  }
  return parts.join('');
}

function chartSvg(chart: NonNullable<DeckSlide['chart']>, palette: DeckPalette, box: { x: number; y: number; w: number; h: number }): string {
  const parts: string[] = [];
  const plot = { x: box.x + 46, y: box.y + 10, w: box.w - 60, h: box.h - 56 };
  const values = chart.series.flatMap((series) => series.values);
  const max = Math.max(...values, 0);
  const min = Math.min(...values, 0);
  const span = max - min || 1;
  const scaleY = (value: number): number => plot.y + plot.h - ((value - min) / span) * plot.h;

  // Baseline and the two gridlines that make a magnitude readable.
  for (const fraction of [0, 0.5, 1]) {
    const y = plot.y + plot.h * fraction;
    parts.push(`<line x1="${plot.x}" y1="${y}" x2="${plot.x + plot.w}" y2="${y}" stroke="${palette.muted}" stroke-opacity="0.25" stroke-width="1"/>`);
    const value = max - (max - min) * fraction;
    parts.push(textEl(String(Math.round(value * 100) / 100), {
      x: plot.x - 8, y: y + 4, size: 11, color: palette.muted, anchor: 'end', family: palette.svgFontStack,
    }));
  }

  const slot = plot.w / chart.labels.length;
  if (chart.kind === 'bar') {
    const barWidth = Math.max(4, (slot * 0.62) / chart.series.length);
    chart.series.forEach((series, seriesIndex) => {
      const color = palette.series[seriesIndex % palette.series.length];
      series.values.forEach((value, index) => {
        const zero = scaleY(Math.max(min, 0));
        const top = scaleY(value);
        const x = plot.x + slot * index + slot * 0.19 + seriesIndex * barWidth;
        const y = Math.min(top, zero);
        const height = Math.max(1, Math.abs(zero - top));
        parts.push(`<rect x="${Math.round(x * 100) / 100}" y="${Math.round(y * 100) / 100}" width="${Math.round(barWidth * 100) / 100}" height="${Math.round(height * 100) / 100}" fill="${color}" rx="2"/>`);
      });
    });
  } else {
    chart.series.forEach((series, seriesIndex) => {
      const color = palette.series[seriesIndex % palette.series.length];
      const points = series.values
        .map((value, index) => `${Math.round((plot.x + slot * index + slot / 2) * 100) / 100},${Math.round(scaleY(value) * 100) / 100}`)
        .join(' ');
      parts.push(`<polyline points="${points}" fill="none" stroke="${color}" stroke-width="3" stroke-linejoin="round" stroke-linecap="round"/>`);
      series.values.forEach((value, index) => {
        parts.push(`<circle cx="${Math.round((plot.x + slot * index + slot / 2) * 100) / 100}" cy="${Math.round(scaleY(value) * 100) / 100}" r="4" fill="${color}"/>`);
      });
    });
  }

  chart.labels.forEach((label, index) => {
    parts.push(textEl(label, {
      x: plot.x + slot * index + slot / 2, y: plot.y + plot.h + 22,
      size: 11, color: palette.muted, anchor: 'middle', family: palette.svgFontStack,
    }));
  });

  if (chart.series.length > 1) {
    let legendX = plot.x;
    const legendY = plot.y + plot.h + 44;
    chart.series.forEach((series, index) => {
      const color = palette.series[index % palette.series.length];
      parts.push(`<rect x="${legendX}" y="${legendY - 9}" width="10" height="10" rx="2" fill="${color}"/>`);
      parts.push(textEl(series.name, { x: legendX + 16, y: legendY, size: 11, color: palette.muted, family: palette.svgFontStack }));
      legendX += 26 + series.name.length * 6;
    });
  }
  return parts.join('');
}

function bodySvg(slide: DeckSlide, spec: DeckSpec, palette: DeckPalette): string {
  const box = { x: MARGIN, y: px(BODY_Y_IN) + 18, w: CONTENT_WIDTH, h: px(BODY_HEIGHT_IN) };
  switch (slide.layout) {
    case 'title':
    case 'section':
      return bulletsSvg(slide.bullets ?? [], palette, box);
    case 'bullets':
      return bulletsSvg(slide.bullets ?? [], palette, box);
    case 'two-column': {
      const [left, right] = slide.columns ?? [[], []];
      const columnWidth = (CONTENT_WIDTH - px(0.6)) / 2;
      return [
        bulletsSvg(left, palette, { x: box.x, y: box.y, w: columnWidth }),
        bulletsSvg(right, palette, { x: box.x + columnWidth + px(0.6), y: box.y, w: columnWidth }),
      ].join('');
    }
    case 'chart': {
      const captionHeight = slide.bullets?.length ? px(1.1) : 0;
      return [
        chartSvg(slide.chart!, palette, { x: box.x, y: px(BODY_Y_IN), w: box.w, h: box.h - captionHeight }),
        captionHeight ? bulletsSvg(slide.bullets ?? [], palette, { x: box.x, y: px(BODY_Y_IN) + box.h - captionHeight + 24, w: box.w }) : '',
      ].join('');
    }
    case 'quote': {
      const quote = slide.quote!;
      const lines = wrapText(`“${quote.text}”`, 24, box.w - px(1));
      return [
        `<rect x="${box.x}" y="${px(BODY_Y_IN)}" width="${box.w}" height="${box.h}" rx="8" fill="${palette.panel}"/>`,
        paragraph(lines, {
          x: box.x + px(0.5), y: px(BODY_Y_IN) + px(0.9), size: 24, color: palette.text,
          italic: true, family: palette.svgFontStack, lineHeight: 34,
        }),
        textEl(`— ${quote.by}`, {
          x: box.x + px(0.5), y: px(BODY_Y_IN) + box.h - 26, size: 14, color: palette.muted, family: palette.svgFontStack,
        }),
      ].join('');
    }
    case 'image': {
      const lines = wrapText(slide.image!.alt, 16, box.w - px(1));
      return [
        `<rect x="${box.x}" y="${px(BODY_Y_IN)}" width="${box.w}" height="${box.h}" rx="8" fill="${palette.panel}" stroke="${palette.muted}" stroke-width="1" stroke-dasharray="6 6"/>`,
        paragraph(lines, {
          x: box.x + box.w / 2, y: px(BODY_Y_IN) + box.h / 2, size: 16, color: palette.muted,
          anchor: 'middle', family: palette.svgFontStack, lineHeight: 24,
        }),
      ].join('');
    }
    default:
      return '';
  }
}

function frame(spec: DeckSpec, palette: DeckPalette, inner: string, opts: { eyebrow?: string; title: string; titleSize: number; footer: string; subtitle?: string }): string {
  const titleLines = wrapText(opts.title, opts.titleSize, CONTENT_WIDTH).slice(0, 2);
  return [
    `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${SVG_WIDTH} ${SVG_HEIGHT}" width="${SVG_WIDTH}" height="${SVG_HEIGHT}" role="img" aria-label="${escapeXml(opts.title)}">`,
    `<title>${escapeXml(`${spec.title} — ${opts.title}`)}</title>`,
    `<rect width="${SVG_WIDTH}" height="${SVG_HEIGHT}" fill="${palette.background}"/>`,
    opts.eyebrow
      ? textEl(opts.eyebrow.toUpperCase(), {
        x: MARGIN, y: px(EYEBROW_Y_IN) + 14, size: 11, color: palette.accent,
        weight: 700, letterSpacing: 1.6, family: palette.svgFontStack,
      })
      : '',
    paragraph(titleLines, {
      x: MARGIN, y: px(TITLE_Y_IN) + opts.titleSize, size: opts.titleSize, color: palette.text,
      weight: 700, family: palette.svgFontStack, lineHeight: opts.titleSize * 1.2,
    }),
    opts.subtitle
      ? textEl(opts.subtitle, { x: MARGIN, y: px(ACCENT_Y_IN) - 12, size: 18, color: palette.muted, family: palette.svgFontStack })
      : '',
    `<rect x="${MARGIN}" y="${px(ACCENT_Y_IN)}" width="${px(ACCENT_WIDTH_IN)}" height="${px(ACCENT_HEIGHT_IN)}" fill="${palette.accent}"/>`,
    inner,
    textEl(opts.footer, { x: MARGIN, y: px(FOOTER_Y_IN) + 14, size: 10, color: palette.muted, family: palette.svgFontStack }),
    '</svg>',
  ].join('');
}

/**
 * Renders one slide of the deck. `index` addresses the same slides the .pptx
 * has: `0…spec.slides.length - 1` are the deck's own, and `spec.slides.length`
 * is the Sources slide that closes every deck.
 */
export function renderSlideSvg(spec: DeckSpec, index: number): string {
  const palette = paletteFor(spec.theme);
  const total = spec.slides.length + 1;
  if (!Number.isInteger(index) || index < 0 || index >= total) {
    throw new RangeError(`renderSlideSvg: slide ${index} is outside 0..${total - 1}`);
  }

  if (index === spec.slides.length) {
    const sources = deckSources(spec);
    return frame(spec, palette, bulletsSvg(
      sources.map((source) => `${source.title} — ${source.documentId}${source.anchor ? `#${source.anchor}` : ''}`),
      palette,
      { x: MARGIN, y: px(BODY_Y_IN) + 18, w: CONTENT_WIDTH },
    ), {
      eyebrow: 'Grounded in',
      title: SOURCES_SLIDE_TITLE,
      titleSize: 28,
      footer: footerText(spec.title, total, total),
    });
  }

  const slide = spec.slides[index]!;
  return frame(spec, palette, bodySvg(slide, spec, palette), {
    ...(slide.eyebrow ? { eyebrow: slide.eyebrow } : {}),
    title: slide.title,
    titleSize: slide.layout === 'title' ? 40 : slide.layout === 'section' ? 34 : 28,
    footer: footerText(spec.title, index + 1, total),
    ...(slide.layout === 'title' && spec.subtitle ? { subtitle: spec.subtitle } : {}),
  });
}

/** Every slide of the deck, in order, Sources last. */
export function renderAllSlideSvgs(spec: DeckSpec): string[] {
  return Array.from({ length: spec.slides.length + 1 }, (_value, index) => renderSlideSvg(spec, index));
}
