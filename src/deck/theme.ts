// One geometry and one palette, shared by the .pptx and the SVG preview.
//
// The preview is not an approximation of the deck: both renderers read these
// numbers, so a slide in the browser sits where the same slide sits in
// PowerPoint. Everything is in inches on a 16:9 stage; the SVG multiplies by
// `SVG_SCALE` to get pixels.

import type { DeckTheme } from './DeckSpec';

/** 16:9 at PowerPoint's widescreen size. */
export const SLIDE_WIDTH_IN = 13.333;
export const SLIDE_HEIGHT_IN = 7.5;
/** 96 px per inch → a 1280×720 preview. */
export const SVG_SCALE = 96;
export const SVG_WIDTH = Math.round(SLIDE_WIDTH_IN * SVG_SCALE);
export const SVG_HEIGHT = Math.round(SLIDE_HEIGHT_IN * SVG_SCALE);

export const MARGIN_IN = 0.7;
export const CONTENT_WIDTH_IN = SLIDE_WIDTH_IN - MARGIN_IN * 2;

/** The master's fixed furniture: eyebrow, title, accent rule, footer. */
export const EYEBROW_Y_IN = 0.52;
export const TITLE_Y_IN = 0.82;
export const TITLE_HEIGHT_IN = 0.9;
export const ACCENT_Y_IN = 1.78;
export const ACCENT_WIDTH_IN = 1.4;
export const ACCENT_HEIGHT_IN = 0.06;
export const BODY_Y_IN = 2.1;
export const BODY_HEIGHT_IN = 4.55;
export const FOOTER_Y_IN = 6.92;
export const FOOTER_HEIGHT_IN = 0.3;

export interface DeckPalette {
  /** Slide background. */
  background: string;
  /** Headline and body text. */
  text: string;
  /** Secondary text: eyebrow context, footer, attributions. */
  muted: string;
  /** The accent rule, bullet marks and the first chart series. */
  accent: string;
  /** Panels: the two-column dividers, the quote block, the image frame. */
  panel: string;
  /** Chart series, accent first. */
  series: string[];
  /** Typeface stack. Only families PowerPoint and every browser already have. */
  fontFace: string;
  /** The SVG font stack, which needs fallbacks spelled out. */
  svgFontStack: string;
}

/** Hex without the '#', which is what pptxgenjs wants. */
export function bare(color: string): string {
  return color.replace('#', '').toUpperCase();
}

const PALETTES: Record<DeckTheme, DeckPalette> = {
  product: {
    background: '#FFFFFF',
    text: '#15161A',
    muted: '#6B7280',
    accent: '#6D4AFF',
    panel: '#F3F1FF',
    series: ['#6D4AFF', '#12B5A5', '#F5A524', '#E5484D', '#2F80ED', '#8E8E93'],
    fontFace: 'Calibri',
    svgFontStack: "'Segoe UI', 'Helvetica Neue', Arial, sans-serif",
  },
  engineering: {
    background: '#0F1117',
    text: '#F2F4F8',
    muted: '#9AA3B2',
    accent: '#32D3A6',
    panel: '#1A1F2B',
    series: ['#32D3A6', '#4C8DFF', '#F5A524', '#E5484D', '#B07CFF', '#9AA3B2'],
    fontFace: 'Consolas',
    svgFontStack: "'SF Mono', 'Cascadia Mono', Consolas, 'Liberation Mono', monospace",
  },
  plain: {
    background: '#FFFFFF',
    text: '#111111',
    muted: '#666666',
    accent: '#111111',
    panel: '#F1F1F1',
    series: ['#111111', '#666666', '#999999', '#444444', '#BBBBBB', '#2B2B2B'],
    fontFace: 'Arial',
    svgFontStack: "Arial, 'Helvetica Neue', Helvetica, sans-serif",
  },
};

export function paletteFor(theme: DeckTheme | undefined): DeckPalette {
  return PALETTES[theme ?? 'product'];
}

/** The footer every slide carries: `<deck title> · n / total`. */
export function footerText(deckTitle: string, position: number, total: number): string {
  return `${deckTitle} · ${position} / ${total}`;
}

/** The last slide of every deck, and the reason the deck is allowed to exist. */
export const SOURCES_SLIDE_ID = 'sources';
export const SOURCES_SLIDE_TITLE = 'Sources';
