// The contract a model answers with when it drafts a presentation, and the
// validator that decides whether what it said may be rendered.
//
// The generator is GROUNDED, not decorative: every slide names at least one
// source from the run's own inputs, and a slide that cites nothing is rejected
// rather than rendered with an empty footer. The rest of the rules are caps —
// a deck is a fixed surface, and a 90-word bullet is a bug in the draft, not
// something to shrink the font for.
//
// Validation is strict on purpose. Unknown keys are refused instead of being
// dropped silently, because a key the model invented is a key it believed in:
// swallowing it would render a deck that is quietly missing what was asked for.

export type DeckTheme = 'product' | 'engineering' | 'plain';

export type SlideLayout = 'title' | 'section' | 'bullets' | 'two-column' | 'chart' | 'quote' | 'image';

/** A citation: the document the claim came from, and (optionally) where in it. */
export interface DeckSource {
  documentId: string;
  anchor?: string;
  title: string;
}

export interface DeckChart {
  kind: 'bar' | 'line';
  labels: string[];
  series: Array<{ name: string; values: number[] }>;
}

export interface DeckQuote {
  text: string;
  by: string;
}

/**
 * An image slide carries only its alt text. Nothing is fetched at render time —
 * neither the .pptx nor the SVG preview reaches the network — so the slide is a
 * framed caption until a real asset pipeline puts bytes behind it.
 */
export interface DeckImage {
  alt: string;
}

export interface DeckSlide {
  id: string;
  layout: SlideLayout;
  title: string;
  eyebrow?: string;
  bullets?: string[];
  columns?: [string[], string[]];
  chart?: DeckChart;
  quote?: DeckQuote;
  image?: DeckImage;
  notes?: string;
  sources: DeckSource[];
}

export interface DeckSpec {
  title: string;
  subtitle?: string;
  theme?: DeckTheme;
  slides: DeckSlide[];
}

// ---------------------------------------------------------------------------
// Caps — the limits a slide can hold and still be read from the back of a room
// ---------------------------------------------------------------------------

export const MAX_SLIDES = 40;
export const MAX_BULLETS_PER_SLIDE = 8;
export const MAX_BULLET_CHARS = 200;
export const MAX_TITLE_CHARS = 120;
export const MAX_SUBTITLE_CHARS = 200;
export const MAX_EYEBROW_CHARS = 60;
export const MAX_NOTES_CHARS = 2000;
export const MAX_CHART_LABELS = 24;
export const MAX_CHART_SERIES = 6;
export const MAX_QUOTE_CHARS = 400;
export const MAX_SOURCES_PER_SLIDE = 10;

/** Slide ids stay filename- and handle-safe: they end up in preview handles and blob keys. */
export const SLIDE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$/;

const THEMES: ReadonlySet<string> = new Set(['product', 'engineering', 'plain']);
const LAYOUTS: ReadonlySet<string> = new Set(['title', 'section', 'bullets', 'two-column', 'chart', 'quote', 'image']);

const SPEC_KEYS: ReadonlySet<string> = new Set(['title', 'subtitle', 'theme', 'slides']);
const SLIDE_KEYS: ReadonlySet<string> = new Set([
  'id', 'layout', 'title', 'eyebrow', 'bullets', 'columns', 'chart', 'quote', 'image', 'notes', 'sources',
]);
const SOURCE_KEYS: ReadonlySet<string> = new Set(['documentId', 'anchor', 'title']);
const CHART_KEYS: ReadonlySet<string> = new Set(['kind', 'labels', 'series']);
const SERIES_KEYS: ReadonlySet<string> = new Set(['name', 'values']);
const QUOTE_KEYS: ReadonlySet<string> = new Set(['text', 'by']);
const IMAGE_KEYS: ReadonlySet<string> = new Set(['alt']);

export type DeckSpecValidation =
  | { ok: true; spec: DeckSpec }
  | { ok: false; errors: string[] };

function isObject(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === 'object' && !Array.isArray(value);
}

/** Collapses whitespace so a cap counts characters a reader would see. */
function clean(value: unknown): string | undefined {
  if (typeof value !== 'string') return undefined;
  const trimmed = value.replace(/\s+/g, ' ').trim();
  return trimmed || undefined;
}

/** Notes keep their line breaks; only trailing space goes. */
function cleanMultiline(value: unknown): string | undefined {
  if (typeof value !== 'string') return undefined;
  const trimmed = value.replace(/[ \t]+$/gm, '').trim();
  return trimmed || undefined;
}

function unknownKeys(object: Record<string, unknown>, allowed: ReadonlySet<string>): string[] {
  return Object.keys(object).filter((key) => !allowed.has(key) && object[key] !== undefined);
}

/**
 * Validates one deck. Returns every problem it found rather than the first, so
 * a draft that has to be re-asked for can be re-asked for once.
 */
export function validateDeckSpec(input: unknown): DeckSpecValidation {
  const errors: string[] = [];
  if (!isObject(input)) return { ok: false, errors: ['deck: expected an object'] };

  for (const key of unknownKeys(input, SPEC_KEYS)) errors.push(`deck: unknown field '${key}'`);

  const title = clean(input.title);
  if (!title) errors.push('deck.title: required');
  else if (title.length > MAX_TITLE_CHARS) errors.push(`deck.title: longer than ${MAX_TITLE_CHARS} characters`);

  const subtitle = clean(input.subtitle);
  if (input.subtitle !== undefined && !subtitle) errors.push('deck.subtitle: must be a non-empty string when present');
  if (subtitle && subtitle.length > MAX_SUBTITLE_CHARS) errors.push(`deck.subtitle: longer than ${MAX_SUBTITLE_CHARS} characters`);

  let theme: DeckTheme | undefined;
  if (input.theme !== undefined) {
    if (typeof input.theme !== 'string' || !THEMES.has(input.theme)) errors.push("deck.theme: must be 'product', 'engineering' or 'plain'");
    else theme = input.theme as DeckTheme;
  }

  if (!Array.isArray(input.slides) || input.slides.length === 0) {
    errors.push('deck.slides: at least one slide is required');
    return { ok: false, errors };
  }
  if (input.slides.length > MAX_SLIDES) errors.push(`deck.slides: ${input.slides.length} slides, the cap is ${MAX_SLIDES}`);

  const slides: DeckSlide[] = [];
  const seenIds = new Set<string>();
  input.slides.slice(0, MAX_SLIDES).forEach((raw, index) => {
    const slide = validateSlide(raw, index, seenIds, errors);
    if (slide) slides.push(slide);
  });

  if (errors.length) return { ok: false, errors };
  return {
    ok: true,
    spec: {
      title: title as string,
      ...(subtitle ? { subtitle } : {}),
      ...(theme ? { theme } : {}),
      slides,
    },
  };
}

function validateSlide(raw: unknown, index: number, seenIds: Set<string>, errors: string[]): DeckSlide | null {
  const at = `slides[${index}]`;
  if (!isObject(raw)) { errors.push(`${at}: expected an object`); return null; }
  for (const key of unknownKeys(raw, SLIDE_KEYS)) errors.push(`${at}: unknown field '${key}'`);

  const id = typeof raw.id === 'string' ? raw.id.trim() : '';
  if (!SLIDE_ID_PATTERN.test(id)) {
    errors.push(`${at}.id: must match ${SLIDE_ID_PATTERN} (letters, digits, '_' and '-')`);
  } else if (seenIds.has(id)) {
    errors.push(`${at}.id: '${id}' is used twice`);
  } else {
    seenIds.add(id);
  }

  const layout = typeof raw.layout === 'string' ? raw.layout : '';
  if (!LAYOUTS.has(layout)) errors.push(`${at}.layout: must be one of ${[...LAYOUTS].join(', ')}`);

  const title = clean(raw.title);
  if (!title) errors.push(`${at}.title: required`);
  else if (title.length > MAX_TITLE_CHARS) errors.push(`${at}.title: longer than ${MAX_TITLE_CHARS} characters`);

  const eyebrow = clean(raw.eyebrow);
  if (raw.eyebrow !== undefined && !eyebrow) errors.push(`${at}.eyebrow: must be a non-empty string when present`);
  if (eyebrow && eyebrow.length > MAX_EYEBROW_CHARS) errors.push(`${at}.eyebrow: longer than ${MAX_EYEBROW_CHARS} characters`);

  const notes = cleanMultiline(raw.notes);
  if (notes && notes.length > MAX_NOTES_CHARS) errors.push(`${at}.notes: longer than ${MAX_NOTES_CHARS} characters`);

  const bullets = raw.bullets === undefined ? undefined : validateBullets(raw.bullets, `${at}.bullets`, errors);
  const columns = raw.columns === undefined ? undefined : validateColumns(raw.columns, at, errors);
  const chart = raw.chart === undefined ? undefined : validateChart(raw.chart, `${at}.chart`, errors);
  const quote = raw.quote === undefined ? undefined : validateQuote(raw.quote, `${at}.quote`, errors);
  const image = raw.image === undefined ? undefined : validateImage(raw.image, `${at}.image`, errors);

  // A layout's own payload belongs to it and nowhere else; bullets are the one
  // thing every layout may carry, as supporting text.
  if (layout === 'bullets' && !bullets?.length) errors.push(`${at}: a 'bullets' slide needs at least one bullet`);
  if (layout === 'two-column' && !columns) errors.push(`${at}: a 'two-column' slide needs columns`);
  if (layout === 'chart' && !chart) errors.push(`${at}: a 'chart' slide needs a chart`);
  if (layout === 'quote' && !quote) errors.push(`${at}: a 'quote' slide needs a quote`);
  if (layout === 'image' && !image) errors.push(`${at}: an 'image' slide needs image.alt`);
  if (columns && layout !== 'two-column') errors.push(`${at}.columns: only a 'two-column' slide may carry columns`);
  if (chart && layout !== 'chart') errors.push(`${at}.chart: only a 'chart' slide may carry a chart`);
  if (quote && layout !== 'quote') errors.push(`${at}.quote: only a 'quote' slide may carry a quote`);
  if (image && layout !== 'image') errors.push(`${at}.image: only an 'image' slide may carry an image`);

  const sources = validateSources(raw.sources, at, errors);

  if (!id || !title || !LAYOUTS.has(layout)) return null;
  return {
    id,
    layout: layout as SlideLayout,
    title,
    ...(eyebrow ? { eyebrow } : {}),
    ...(bullets?.length ? { bullets } : {}),
    ...(columns ? { columns } : {}),
    ...(chart ? { chart } : {}),
    ...(quote ? { quote } : {}),
    ...(image ? { image } : {}),
    ...(notes ? { notes } : {}),
    sources,
  };
}

function validateBullets(raw: unknown, at: string, errors: string[]): string[] {
  if (!Array.isArray(raw)) { errors.push(`${at}: expected an array of strings`); return []; }
  if (raw.length > MAX_BULLETS_PER_SLIDE) errors.push(`${at}: ${raw.length} bullets, the cap is ${MAX_BULLETS_PER_SLIDE}`);
  const out: string[] = [];
  raw.slice(0, MAX_BULLETS_PER_SLIDE).forEach((item, index) => {
    const text = clean(item);
    if (!text) { errors.push(`${at}[${index}]: must be a non-empty string`); return; }
    if (text.length > MAX_BULLET_CHARS) { errors.push(`${at}[${index}]: longer than ${MAX_BULLET_CHARS} characters`); return; }
    out.push(text);
  });
  return out;
}

function validateColumns(raw: unknown, at: string, errors: string[]): [string[], string[]] | undefined {
  if (!Array.isArray(raw) || raw.length !== 2) { errors.push(`${at}.columns: expected exactly two arrays of bullets`); return undefined; }
  const left = validateBullets(raw[0], `${at}.columns[0]`, errors);
  const right = validateBullets(raw[1], `${at}.columns[1]`, errors);
  if (!left.length && !right.length) { errors.push(`${at}.columns: both columns are empty`); return undefined; }
  return [left, right];
}

function validateChart(raw: unknown, at: string, errors: string[]): DeckChart | undefined {
  if (!isObject(raw)) { errors.push(`${at}: expected an object`); return undefined; }
  for (const key of unknownKeys(raw, CHART_KEYS)) errors.push(`${at}: unknown field '${key}'`);
  const kind = raw.kind === 'bar' || raw.kind === 'line' ? raw.kind : undefined;
  if (!kind) errors.push(`${at}.kind: must be 'bar' or 'line'`);

  const labels: string[] = [];
  if (!Array.isArray(raw.labels) || raw.labels.length === 0) {
    errors.push(`${at}.labels: at least one label is required`);
  } else {
    if (raw.labels.length > MAX_CHART_LABELS) errors.push(`${at}.labels: ${raw.labels.length} labels, the cap is ${MAX_CHART_LABELS}`);
    raw.labels.slice(0, MAX_CHART_LABELS).forEach((label, index) => {
      const text = clean(label);
      if (!text) { errors.push(`${at}.labels[${index}]: must be a non-empty string`); return; }
      labels.push(text.slice(0, 40));
    });
  }

  const series: DeckChart['series'] = [];
  if (!Array.isArray(raw.series) || raw.series.length === 0) {
    errors.push(`${at}.series: at least one series is required`);
  } else {
    if (raw.series.length > MAX_CHART_SERIES) errors.push(`${at}.series: ${raw.series.length} series, the cap is ${MAX_CHART_SERIES}`);
    raw.series.slice(0, MAX_CHART_SERIES).forEach((entry, index) => {
      const where = `${at}.series[${index}]`;
      if (!isObject(entry)) { errors.push(`${where}: expected an object`); return; }
      for (const key of unknownKeys(entry, SERIES_KEYS)) errors.push(`${where}: unknown field '${key}'`);
      const name = clean(entry.name);
      if (!name) { errors.push(`${where}.name: required`); return; }
      if (!Array.isArray(entry.values) || entry.values.some((value) => typeof value !== 'number' || !Number.isFinite(value))) {
        errors.push(`${where}.values: expected finite numbers`);
        return;
      }
      if (entry.values.length !== labels.length) {
        errors.push(`${where}.values: ${entry.values.length} values for ${labels.length} labels`);
        return;
      }
      series.push({ name: name.slice(0, 40), values: entry.values as number[] });
    });
  }

  if (!kind || !labels.length || !series.length) return undefined;
  return { kind, labels, series };
}

function validateQuote(raw: unknown, at: string, errors: string[]): DeckQuote | undefined {
  if (!isObject(raw)) { errors.push(`${at}: expected an object`); return undefined; }
  for (const key of unknownKeys(raw, QUOTE_KEYS)) errors.push(`${at}: unknown field '${key}'`);
  const text = clean(raw.text);
  const by = clean(raw.by);
  if (!text) errors.push(`${at}.text: required`);
  else if (text.length > MAX_QUOTE_CHARS) errors.push(`${at}.text: longer than ${MAX_QUOTE_CHARS} characters`);
  if (!by) errors.push(`${at}.by: required — an unattributed quote is not a source`);
  if (!text || !by) return undefined;
  return { text, by: by.slice(0, 80) };
}

function validateImage(raw: unknown, at: string, errors: string[]): DeckImage | undefined {
  if (!isObject(raw)) { errors.push(`${at}: expected an object`); return undefined; }
  for (const key of unknownKeys(raw, IMAGE_KEYS)) errors.push(`${at}: unknown field '${key}'`);
  const alt = clean(raw.alt);
  if (!alt) { errors.push(`${at}.alt: required`); return undefined; }
  if (alt.length > MAX_BULLET_CHARS) { errors.push(`${at}.alt: longer than ${MAX_BULLET_CHARS} characters`); return undefined; }
  return { alt };
}

/**
 * The rule that makes this a grounded generator: a slide with no citation is
 * refused. Rendering it would be the deck asserting something no input said.
 */
function validateSources(raw: unknown, at: string, errors: string[]): DeckSource[] {
  if (!Array.isArray(raw) || raw.length === 0) {
    errors.push(`${at}.sources: every slide must cite at least one source document`);
    return [];
  }
  if (raw.length > MAX_SOURCES_PER_SLIDE) errors.push(`${at}.sources: ${raw.length} sources, the cap is ${MAX_SOURCES_PER_SLIDE}`);
  const out: DeckSource[] = [];
  raw.slice(0, MAX_SOURCES_PER_SLIDE).forEach((entry, index) => {
    const where = `${at}.sources[${index}]`;
    if (!isObject(entry)) { errors.push(`${where}: expected an object`); return; }
    for (const key of unknownKeys(entry, SOURCE_KEYS)) errors.push(`${where}: unknown field '${key}'`);
    const documentId = clean(entry.documentId);
    const title = clean(entry.title);
    const anchor = clean(entry.anchor);
    if (!documentId) { errors.push(`${where}.documentId: required`); return; }
    if (!title) { errors.push(`${where}.title: required`); return; }
    out.push({
      documentId: documentId.slice(0, 200),
      ...(anchor ? { anchor: anchor.slice(0, 120) } : {}),
      title: title.slice(0, 160),
    });
  });
  if (!out.length) errors.push(`${at}.sources: no usable source survived validation`);
  return out;
}

/** Validate or throw. The message lists every problem, in order. */
export function assertDeckSpec(input: unknown): DeckSpec {
  const result = validateDeckSpec(input);
  if (result.ok) return result.spec;
  throw new Error(`invalid DeckSpec: ${result.errors.join('; ')}`);
}

/**
 * Pulls the deck out of whatever a model said: a bare object, a fenced block,
 * or prose with the JSON somewhere inside it. Returns `undefined` when there is
 * nothing parseable — the caller reports that, it does not improvise a deck.
 */
export function parseDeckSpecJson(text: string): unknown {
  if (typeof text !== 'string') return undefined;
  const fenced = /```(?:json)?\s*([\s\S]*?)```/i.exec(text);
  const candidates = [fenced?.[1], text].filter((value): value is string => typeof value === 'string');
  for (const candidate of candidates) {
    const start = candidate.indexOf('{');
    const end = candidate.lastIndexOf('}');
    if (start < 0 || end <= start) continue;
    try {
      return JSON.parse(candidate.slice(start, end + 1));
    } catch {
      continue;
    }
  }
  return undefined;
}

/** Every distinct document a deck cites, first-cited first — the Sources slide's content. */
export function deckSources(spec: DeckSpec): DeckSource[] {
  const seen = new Map<string, DeckSource>();
  for (const slide of spec.slides) {
    for (const source of slide.sources) {
      const key = `${source.documentId}#${source.anchor ?? ''}`;
      if (!seen.has(key)) seen.set(key, source);
    }
  }
  return [...seen.values()];
}

// ---------------------------------------------------------------------------
// Revise targets — "revise the third bullet" without element ids
// ---------------------------------------------------------------------------
//
// A slide's parts have no ids below the slide (bullets, columns and the quote
// are plain strings), and the layouts are typed. So an element is addressed by
// the FIELD it lives in plus, for a list, its position — never by an id the
// spec does not carry. `subtitle` is the deck's own field (the cover shows it);
// every other field belongs to the slide `slideId` names, and only to a layout
// that may carry it.

export type DeckReviseTargetField =
  | 'title' | 'eyebrow' | 'subtitle' | 'bullets' | 'columns' | 'chart' | 'quote' | 'image' | 'notes';

export interface DeckReviseTarget {
  field: DeckReviseTargetField;
  /**
   * `bullets`: the bullet (0-based) to change — it must exist. `columns`: the
   * column, 0 or 1. Omitted: the whole field. Not allowed on any other field.
   */
  index?: number;
}

export const REVISE_TARGET_FIELDS: readonly DeckReviseTargetField[] = [
  'title', 'eyebrow', 'subtitle', 'bullets', 'columns', 'chart', 'quote', 'image', 'notes',
];

/** Fields a slide may carry whatever its layout; the rest belong to one layout. */
const ANY_LAYOUT_FIELDS: ReadonlySet<DeckReviseTargetField> = new Set(['title', 'eyebrow', 'bullets', 'notes']);
const LAYOUT_OF_FIELD: Partial<Record<DeckReviseTargetField, SlideLayout>> = {
  columns: 'two-column', chart: 'chart', quote: 'quote', image: 'image',
};

export type DeckReviseTargetValidation =
  | { ok: true; target: DeckReviseTarget }
  | { ok: false; error: string };

/**
 * Checks a target against the deck it will be applied to. `slideId` must
 * already name a slide of `spec` (the caller checks that first) unless the
 * target is the deck's `subtitle`.
 */
export function validateReviseTarget(raw: unknown, spec: DeckSpec, slideId: string | undefined): DeckReviseTargetValidation {
  if (!isObject(raw)) return { ok: false, error: 'target must be an object: { field, index? }' };
  const extra = Object.keys(raw).filter((key) => key !== 'field' && key !== 'index' && raw[key] !== undefined);
  if (extra.length) return { ok: false, error: `target: unknown field '${extra[0]}'` };
  const field = raw.field;
  if (typeof field !== 'string' || !(REVISE_TARGET_FIELDS as readonly string[]).includes(field)) {
    return { ok: false, error: `target.field must be one of ${REVISE_TARGET_FIELDS.join(', ')}` };
  }
  const f = field as DeckReviseTargetField;
  let index: number | undefined;
  if (raw.index !== undefined && raw.index !== null) {
    if (typeof raw.index !== 'number' || !Number.isInteger(raw.index) || raw.index < 0) {
      return { ok: false, error: 'target.index must be a whole number, 0 or more' };
    }
    if (f !== 'bullets' && f !== 'columns') return { ok: false, error: `target.index applies only to bullets and columns, not ${f}` };
    index = raw.index;
  }

  if (f === 'subtitle') {
    // The deck's own field. A slideId alongside it is harmless (the editor
    // sends the selected slide); it does not narrow anything.
    return { ok: true, target: { field: f } };
  }
  if (!slideId) return { ok: false, error: `target.field '${f}' is a slide's field — name the slide with slideId` };
  const slide = spec.slides.find((candidate) => candidate.id === slideId);
  if (!slide) return { ok: false, error: 'slideId does not name a slide in this deck' };

  const layout = LAYOUT_OF_FIELD[f];
  if (!ANY_LAYOUT_FIELDS.has(f) && layout && slide.layout !== layout) {
    return { ok: false, error: `slide '${slide.id}' is a '${slide.layout}' slide; only a '${layout}' slide has ${f}` };
  }
  if (f === 'bullets' && index !== undefined) {
    const count = slide.bullets?.length ?? 0;
    if (index >= count) return { ok: false, error: `slide '${slide.id}' has ${count} bullet${count === 1 ? '' : 's'}; target.index ${index} is not one of them` };
  }
  if (f === 'columns' && index !== undefined && index > 1) {
    return { ok: false, error: 'target.index for columns is 0 (left) or 1 (right)' };
  }
  return { ok: true, target: { field: f, ...(index !== undefined ? { index } : {}) } };
}

/** How a target reads in a prompt, a log line or an event: "bullets[2]", "columns[1]", "notes". */
export function describeReviseTarget(target: DeckReviseTarget): string {
  return target.index === undefined ? target.field : `${target.field}[${target.index}]`;
}
