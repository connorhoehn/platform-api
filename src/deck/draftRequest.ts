// What a deck request asks for that can be measured, and what a draft must not
// change about its material (realtime-examples Loop 38 ppt, NFR #232/#234).
//
//   - The slide count: "Create a 6-slide sprint review" is 6 content slides.
//     The Sources slide the renderer appends is extra and never counted.
//     Live 2026-09-24 (run c26cebd4) the drafter chose 4 — nothing checked.
//   - Status verbs: a source that says "shipped" must not come back as
//     "delivered" (same run, the title slide's notes). The drafter is told,
//     and the draft is measured against the material's own words.

import { MAX_SLIDES, type DeckSpec } from './DeckSpec';

const NUMBER_WORDS: Record<string, number> = {
  one: 1, two: 2, three: 3, four: 4, five: 5, six: 6, seven: 7, eight: 8, nine: 9, ten: 10,
  eleven: 11, twelve: 12, thirteen: 13, fourteen: 14, fifteen: 15, sixteen: 16, twenty: 20,
};
const NUM = `(\\d{1,2}|${Object.keys(NUMBER_WORDS).join('|')})`;

/**
 * The number of content slides an instruction asks for, when it states one:
 * "a 6-slide review", "6 slides", "six slides", "exactly 8 slides", "in 5 slides".
 * A change to an existing deck ("add 2 slides", "2 more slides", "at most 6 slides")
 * is not a count for a new deck and is ignored. Undefined when none, or two
 * different counts are stated.
 */
export function requestedSlideCount(instruction: unknown): number | undefined {
  if (typeof instruction !== 'string') return undefined;
  const text = instruction.replace(/\s+/g, ' ');
  const counts = new Set<number>();
  for (const m of text.matchAll(new RegExp(`(?:^|[^\\w-])((?:add|append|insert|remove|drop|cut|delete|another|more than|fewer than|less than|at most|at least|up to|under|over|no more than)\\s+)?${NUM}[ -]slides?\\b(?!\\s+(?:more|fewer|less|longer|shorter))`, 'gi'))) {
    if (m[1]) continue;
    const raw = m[2]!.toLowerCase();
    const n = /^\d+$/.test(raw) ? Number(raw) : NUMBER_WORDS[raw];
    if (n && n > 0) counts.add(n);
  }
  return counts.size === 1 ? [...counts][0] : undefined;
}

/** The most content slides a request may ask for (the renderer adds Sources after them). */
export const MAX_REQUESTED_SLIDES = MAX_SLIDES - 1;

/**
 * Status verbs a slide might claim. Any of them in a draft must be the
 * material's own word: "shipped" stays "shipped", "tracked" is not "done".
 * Kept in step with `STATUS` in realtime-examples `scripts/deck-preview.mjs`.
 */
export const STATUS_WORD = /\b(confirmed|verified|completed?|done|approved|reviewed|signed[ -]off|shipped|finished|resolved|assigned|defined|decided|launched|delivered|released|deployed|landed|fixed|passed|achieved|secured|finali[sz]ed|in place)\b/gi;

export interface StatusDrift {
  slideId: string;
  field: string;
  /** The status word the draft used. */
  word: string;
  /** The status words the material does use — what it most likely said instead. */
  sourceSays: string[];
}

/** Every text of a spec, with where it sits. */
export function specTexts(spec: DeckSpec): Array<{ slideId: string; field: string; text: string }> {
  const out: Array<{ slideId: string; field: string; text: string }> = [];
  const add = (slideId: string, field: string, value: unknown) => {
    if (typeof value === 'string' && value.trim()) out.push({ slideId, field, text: value });
  };
  add('(deck)', 'title', spec.title);
  add('(deck)', 'subtitle', spec.subtitle);
  for (const slide of spec.slides) {
    add(slide.id, 'title', slide.title);
    add(slide.id, 'eyebrow', slide.eyebrow);
    add(slide.id, 'notes', slide.notes);
    (slide.bullets ?? []).forEach((bullet, i) => add(slide.id, `bullet ${i + 1}`, bullet));
    (slide.columns ?? []).forEach((column, c) => (column ?? []).forEach((bullet, i) => add(slide.id, `column ${c + 1}.${i + 1}`, bullet)));
    if (slide.quote) add(slide.id, 'quote', slide.quote.text);
  }
  return out;
}

/**
 * Status words in the draft that the material never uses. `allowed` is text
 * the requester wrote (the instruction, the deck title): a word the person
 * asked for is theirs, not a substitution.
 */
export function statusDrift(spec: DeckSpec, materialText: string, allowed: string[] = []): StatusDrift[] {
  const source = `${materialText}\n${allowed.join('\n')}`.toLowerCase();
  const sourceWords = [...new Set((materialText.match(STATUS_WORD) ?? []).map((w) => w.toLowerCase()))];
  const out: StatusDrift[] = [];
  for (const { slideId, field, text } of specTexts(spec)) {
    for (const word of text.match(STATUS_WORD) ?? []) {
      const lower = word.toLowerCase();
      if (new RegExp(`\\b${lower}\\b`).test(source)) continue;
      out.push({ slideId, field, word, sourceSays: sourceWords });
    }
  }
  return out;
}

/** "Slide cover notes: "delivered" — the source says "shipped"." */
export function statusDriftLine(drift: StatusDrift): string {
  const where = drift.slideId === '(deck)' ? `Deck ${drift.field}` : `Slide ${drift.slideId} ${drift.field}`;
  const says = drift.sourceSays.length ? ` — the source says ${drift.sourceSays.map((w) => `“${w}”`).join(' or ')}` : ' — the source uses no status word here';
  return `${where}: “${drift.word}”${says}.`;
}

/** "Measured: slides 4 → 6 (asked 6)" / "Measured: slides 6 (asked 6)". */
export function slideCountLine(asked: number, first: number, final: number): string {
  return first === final ? `Measured: slides ${final} (asked ${asked})` : `Measured: slides ${first} → ${final} (asked ${asked})`;
}
