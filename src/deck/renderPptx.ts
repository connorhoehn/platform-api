// The .pptx itself — a real OOXML package, not an export of a picture of one.
//
// pptxgenjs is pure JavaScript (it zips the OOXML parts itself), so this runs
// in the same container as every other step: no headless Office, no native
// module, no network. What comes back is the bytes of the file a person opens
// in PowerPoint, Keynote or Google Slides.
//
// Every slide is built from the shared geometry in theme.ts, which the SVG
// preview reads too, so the preview and the file agree on where things are.

import PptxGenJS from 'pptxgenjs';
import {
  deckSources,
  type DeckSlide,
  type DeckSpec,
} from './DeckSpec';
import {
  ACCENT_HEIGHT_IN, ACCENT_WIDTH_IN, ACCENT_Y_IN,
  BODY_HEIGHT_IN, BODY_Y_IN,
  CONTENT_WIDTH_IN, EYEBROW_Y_IN, FOOTER_HEIGHT_IN, FOOTER_Y_IN,
  MARGIN_IN, SLIDE_HEIGHT_IN, SLIDE_WIDTH_IN,
  SOURCES_SLIDE_TITLE, TITLE_HEIGHT_IN, TITLE_Y_IN,
  bare, footerText, paletteFor, type DeckPalette,
} from './theme';

export const PPTX_CONTENT_TYPE = 'application/vnd.openxmlformats-officedocument.presentationml.presentation';

const LAYOUT_NAME = 'DECK_16x9';
const MASTER_NAME = 'DECK_MASTER';

/** One line per citation, the way it reads in the speaker notes. */
export function citationLine(source: { title: string; documentId: string; anchor?: string }): string {
  return `${source.title} — ${source.documentId}${source.anchor ? `#${source.anchor}` : ''}`;
}

/** The speaker notes a slide ships with: what to say, then what it stands on. */
export function slideNotes(slide: DeckSlide): string {
  const parts: string[] = [];
  if (slide.notes) parts.push(slide.notes);
  parts.push(`Sources:\n${slide.sources.map((source) => `- ${citationLine(source)}`).join('\n')}`);
  return parts.join('\n\n');
}

function defineMaster(pptx: PptxGenJS, palette: DeckPalette): void {
  pptx.defineSlideMaster({
    title: MASTER_NAME,
    background: { color: bare(palette.background) },
    objects: [
      // The accent rule under the title — the one piece of furniture that is
      // the same on every slide, so it lives on the master.
      {
        rect: {
          x: MARGIN_IN, y: ACCENT_Y_IN, w: ACCENT_WIDTH_IN, h: ACCENT_HEIGHT_IN,
          fill: { color: bare(palette.accent) },
        },
      },
    ],
  });
}

function addFurniture(
  slide: PptxGenJS.Slide,
  palette: DeckPalette,
  opts: { eyebrow?: string; title: string; titleSize: number; footer: string },
): void {
  if (opts.eyebrow) {
    slide.addText(opts.eyebrow.toUpperCase(), {
      x: MARGIN_IN, y: EYEBROW_Y_IN, w: CONTENT_WIDTH_IN, h: 0.28,
      fontFace: palette.fontFace, fontSize: 11, bold: true, charSpacing: 1.6,
      color: bare(palette.accent), valign: 'middle',
    });
  }
  slide.addText(opts.title, {
    x: MARGIN_IN, y: TITLE_Y_IN, w: CONTENT_WIDTH_IN, h: TITLE_HEIGHT_IN,
    fontFace: palette.fontFace, fontSize: opts.titleSize, bold: true,
    color: bare(palette.text), valign: 'top', align: 'left', shrinkText: true,
  });
  slide.addText(opts.footer, {
    x: MARGIN_IN, y: FOOTER_Y_IN, w: CONTENT_WIDTH_IN, h: FOOTER_HEIGHT_IN,
    fontFace: palette.fontFace, fontSize: 10, color: bare(palette.muted), valign: 'middle',
  });
}

function addBullets(
  slide: PptxGenJS.Slide,
  palette: DeckPalette,
  bullets: string[],
  box: { x: number; y: number; w: number; h: number },
): void {
  if (!bullets.length) return;
  slide.addText(
    bullets.map((text) => ({ text, options: { bullet: { code: '2022' }, breakLine: true } })),
    {
      ...box,
      fontFace: palette.fontFace, fontSize: 18, color: bare(palette.text),
      lineSpacingMultiple: 1.4, paraSpaceAfter: 10, valign: 'top', shrinkText: true,
    },
  );
}

function addSlideBody(pptx: PptxGenJS, slide: PptxGenJS.Slide, palette: DeckPalette, spec: DeckSlide): void {
  const box = { x: MARGIN_IN, y: BODY_Y_IN, w: CONTENT_WIDTH_IN, h: BODY_HEIGHT_IN };
  switch (spec.layout) {
    case 'title':
    case 'section':
      // The furniture IS the slide; the bullets, when present, read as a standfirst.
      addBullets(slide, palette, spec.bullets ?? [], { ...box, h: 1.6 });
      return;
    case 'bullets':
      addBullets(slide, palette, spec.bullets ?? [], box);
      return;
    case 'two-column': {
      const [left, right] = spec.columns ?? [[], []];
      const columnWidth = (CONTENT_WIDTH_IN - 0.6) / 2;
      addBullets(slide, palette, left, { x: MARGIN_IN, y: BODY_Y_IN, w: columnWidth, h: BODY_HEIGHT_IN });
      addBullets(slide, palette, right, { x: MARGIN_IN + columnWidth + 0.6, y: BODY_Y_IN, w: columnWidth, h: BODY_HEIGHT_IN });
      return;
    }
    case 'chart': {
      const chart = spec.chart!;
      const captionHeight = spec.bullets?.length ? 1.1 : 0;
      slide.addChart(
        chart.kind === 'line' ? pptx.ChartType.line : pptx.ChartType.bar,
        chart.series.map((series) => ({ name: series.name, labels: chart.labels, values: series.values })),
        {
          x: MARGIN_IN, y: BODY_Y_IN, w: CONTENT_WIDTH_IN, h: BODY_HEIGHT_IN - captionHeight,
          chartColors: palette.series.map(bare),
          showLegend: chart.series.length > 1, legendPos: 'b', legendColor: bare(palette.muted),
          showValue: false,
          catAxisLabelColor: bare(palette.muted), valAxisLabelColor: bare(palette.muted),
          catAxisLabelFontSize: 11, valAxisLabelFontSize: 11,
          catAxisLineShow: false, valAxisLineShow: false,
          chartArea: { fill: { color: bare(palette.background) } },
          plotArea: { fill: { color: bare(palette.background) } },
          lineDataSymbol: 'circle', lineSize: 3,
          barGapWidthPct: 60,
        },
      );
      if (captionHeight) {
        addBullets(slide, palette, spec.bullets ?? [], {
          x: MARGIN_IN, y: BODY_Y_IN + BODY_HEIGHT_IN - captionHeight, w: CONTENT_WIDTH_IN, h: captionHeight,
        });
      }
      return;
    }
    case 'quote': {
      const quote = spec.quote!;
      slide.addShape('rect', {
        x: MARGIN_IN, y: BODY_Y_IN, w: CONTENT_WIDTH_IN, h: BODY_HEIGHT_IN,
        fill: { color: bare(palette.panel) },
      });
      slide.addText(`“${quote.text}”`, {
        x: MARGIN_IN + 0.5, y: BODY_Y_IN + 0.5, w: CONTENT_WIDTH_IN - 1, h: BODY_HEIGHT_IN - 1.4,
        fontFace: palette.fontFace, fontSize: 24, italic: true, color: bare(palette.text),
        valign: 'middle', shrinkText: true,
      });
      slide.addText(`— ${quote.by}`, {
        x: MARGIN_IN + 0.5, y: BODY_Y_IN + BODY_HEIGHT_IN - 0.85, w: CONTENT_WIDTH_IN - 1, h: 0.4,
        fontFace: palette.fontFace, fontSize: 14, color: bare(palette.muted), valign: 'middle',
      });
      return;
    }
    case 'image': {
      // No bytes are fetched at render time: the frame states what belongs here
      // rather than shipping a stock image nobody chose.
      slide.addShape('rect', {
        x: MARGIN_IN, y: BODY_Y_IN, w: CONTENT_WIDTH_IN, h: BODY_HEIGHT_IN,
        fill: { color: bare(palette.panel) },
        line: { color: bare(palette.muted), width: 1, dashType: 'dash' },
      });
      slide.addText(spec.image!.alt, {
        x: MARGIN_IN + 0.5, y: BODY_Y_IN + 0.5, w: CONTENT_WIDTH_IN - 1, h: BODY_HEIGHT_IN - 1,
        fontFace: palette.fontFace, fontSize: 16, color: bare(palette.muted),
        align: 'center', valign: 'middle', shrinkText: true,
      });
      return;
    }
  }
}

/**
 * Renders the whole deck, Sources slide included, and hands back the file's
 * bytes. `spec` must already have been through `validateDeckSpec`.
 */
export async function renderPptx(spec: DeckSpec): Promise<Buffer> {
  const palette = paletteFor(spec.theme);
  const pptx = new PptxGenJS();
  pptx.defineLayout({ name: LAYOUT_NAME, width: SLIDE_WIDTH_IN, height: SLIDE_HEIGHT_IN });
  pptx.layout = LAYOUT_NAME;
  pptx.title = spec.title;
  pptx.subject = spec.subtitle ?? spec.title;
  pptx.company = 'platform-api';
  defineMaster(pptx, palette);

  const total = spec.slides.length + 1;

  spec.slides.forEach((slideSpec, index) => {
    const slide = pptx.addSlide({ masterName: MASTER_NAME });
    addFurniture(slide, palette, {
      ...(slideSpec.eyebrow ? { eyebrow: slideSpec.eyebrow } : {}),
      title: slideSpec.title,
      titleSize: slideSpec.layout === 'title' ? 40 : slideSpec.layout === 'section' ? 34 : 28,
      footer: footerText(spec.title, index + 1, total),
    });
    if (slideSpec.layout === 'title' && spec.subtitle) {
      slide.addText(spec.subtitle, {
        x: MARGIN_IN, y: TITLE_Y_IN + TITLE_HEIGHT_IN + 0.15, w: CONTENT_WIDTH_IN, h: 0.5,
        fontFace: palette.fontFace, fontSize: 18, color: bare(palette.muted),
      });
    }
    addSlideBody(pptx, slide, palette, slideSpec);
    slide.addNotes(slideNotes(slideSpec));
  });

  // The Sources slide: the deck saying, on the record, what it stands on.
  const sources = deckSources(spec);
  const closing = pptx.addSlide({ masterName: MASTER_NAME });
  addFurniture(closing, palette, {
    eyebrow: 'Grounded in',
    title: SOURCES_SLIDE_TITLE,
    titleSize: 28,
    footer: footerText(spec.title, total, total),
  });
  closing.addText(
    sources.map((source) => ({
      text: citationLine(source),
      options: { bullet: { code: '2022' }, breakLine: true },
    })),
    {
      x: MARGIN_IN, y: BODY_Y_IN, w: CONTENT_WIDTH_IN, h: BODY_HEIGHT_IN,
      fontFace: palette.fontFace, fontSize: 14, color: bare(palette.text),
      lineSpacingMultiple: 1.3, valign: 'top', shrinkText: true,
    },
  );
  closing.addNotes(`Sources:\n${sources.map((source) => `- ${citationLine(source)}`).join('\n')}`);

  const out = await pptx.write({ outputType: 'nodebuffer' });
  return Buffer.isBuffer(out) ? out : Buffer.from(out as ArrayBuffer);
}
