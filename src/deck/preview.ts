// Rendering a deck WITHOUT keeping it.
//
// The editor redraws on every keystroke, so this path touches no store, no blob
// and no document: a spec in, the same SVGs the .pptx will be built from out.
// That is the point — the preview is the real renderer, not a second one that
// can drift from what a download would contain.
//
// It also answers with warnings. A warning is never a refusal: the deck renders
// and says what a reader would notice anyway — a slide crowded to its cap, a
// title that will wrap, a chart with more columns than a projector resolves,
// a deck standing on a single source.

import {
  MAX_BULLETS_PER_SLIDE, MAX_BULLET_CHARS, MAX_SLIDES,
  deckSources, validateDeckSpec, type DeckSpec,
} from './DeckSpec';
import { renderSlideSvg } from './renderSlideSvg';
import { SOURCES_SLIDE_ID, SOURCES_SLIDE_TITLE } from './theme';

export interface PreviewSlide {
  id: string;
  index: number;
  title: string;
  svg: string;
}

export interface DeckPreview {
  title: string;
  /** Every slide the .pptx would have, Sources last. */
  slides: PreviewSlide[];
  warnings: string[];
}

/** Title length past which the renderer wraps onto a second line. */
const TITLE_WRAP_CHARS = 46;
const CROWDED_CHART_LABELS = 12;

/** What a reader would notice. Advisory only — none of these stops a render. */
export function deckWarnings(spec: DeckSpec): string[] {
  const warnings: string[] = [];
  spec.slides.forEach((slide, index) => {
    const at = `Slide ${index + 1} (${slide.id})`;
    const bullets = slide.bullets ?? [];
    const columnBullets = (slide.columns ?? []).flat();
    if (bullets.length === MAX_BULLETS_PER_SLIDE || columnBullets.length >= MAX_BULLETS_PER_SLIDE * 2) {
      warnings.push(`${at} is at the bullet cap — split it and the audience keeps up.`);
    }
    for (const bullet of [...bullets, ...columnBullets]) {
      if (bullet.length > MAX_BULLET_CHARS * 0.75) {
        warnings.push(`${at} has a bullet of ${bullet.length} characters — it will read as a paragraph.`);
        break;
      }
    }
    if (slide.title.length > TITLE_WRAP_CHARS) warnings.push(`${at} has a title that wraps onto a second line.`);
    if (slide.chart && slide.chart.labels.length > CROWDED_CHART_LABELS) {
      warnings.push(`${at} charts ${slide.chart.labels.length} columns — past about ${CROWDED_CHART_LABELS} they stop being legible.`);
    }
    if (!slide.notes && slide.layout !== 'title' && slide.layout !== 'section') {
      warnings.push(`${at} has no speaker notes.`);
    }
  });
  const sources = deckSources(spec);
  if (sources.length === 1) warnings.push(`Every slide cites the same source (${sources[0]!.title}).`);
  return warnings;
}

/** One preview slide's identity — the deck's own slides, then the Sources slide. */
export function previewSlideIdentity(spec: DeckSpec, index: number): { id: string; title: string } {
  const slide = spec.slides[index];
  return slide ? { id: slide.id, title: slide.title } : { id: SOURCES_SLIDE_ID, title: SOURCES_SLIDE_TITLE };
}

/** Renders every slide of a validated spec. Nothing is written anywhere. */
export function renderDeckPreview(spec: DeckSpec): DeckPreview {
  const slides: PreviewSlide[] = Array.from({ length: spec.slides.length + 1 }, (_value, index) => ({
    ...previewSlideIdentity(spec, index),
    index: index + 1,
    svg: renderSlideSvg(spec, index),
  }));
  return { title: spec.title, slides, warnings: deckWarnings(spec) };
}

export type PreviewOutcome =
  | { ok: true; preview: DeckPreview; spec: DeckSpec }
  | { ok: false; status: 400 | 413; errors: string[] };

/**
 * Validates and previews in one step, separating "too big to be a deck" from
 * "not a deck": the first is a 413 the editor can act on by cutting slides, the
 * second is a 400 listing what is wrong.
 */
export function previewDeckSpec(input: unknown): PreviewOutcome {
  const slideCount = Array.isArray((input as { slides?: unknown[] } | null)?.slides)
    ? (input as { slides: unknown[] }).slides.length
    : 0;
  if (slideCount > MAX_SLIDES) {
    return { ok: false, status: 413, errors: [`deck.slides: ${slideCount} slides, the cap is ${MAX_SLIDES}`] };
  }
  const validation = validateDeckSpec(input);
  if (!validation.ok) return { ok: false, status: 400, errors: validation.errors };
  return { ok: true, preview: renderDeckPreview(validation.spec), spec: validation.spec };
}
