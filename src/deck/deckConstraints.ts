// Measurable constraints in a composer instruction, checked against what the
// model actually did — the `check` step of `deck-revise`.
//
// The model's summary is a claim ("shortened the title by one word"). On
// 2026-09-23 a "one word shorter" edit renamed "Managing release risk" to
// "Release risk management" (three words to three) and the summary said "one
// word fewer"; nothing checked it. This module is the check: a small
// deterministic parser (not the model) pulls the countable parts out of the
// instruction — "shorter", "one word shorter", "10 words or fewer", "under 40
// characters", "fewer bullets", "three bullets", "add a bullet", "remove a
// bullet", "keep the title" — scoped to the slide, the field or the deck; the
// proposal is measured before and after; and a violation is a fact, not a
// judgement call. `reconcileSummary` then keeps the model's own claim from
// standing where the numbers contradict it.
//
// Nothing here reads the instruction for meaning beyond these phrases. An
// instruction with no measurable phrase yields no constraints, and the check
// passes on measurements alone (the facts still go on the run).

import type { DeckReviseTarget, DeckSlide, DeckSpec } from './DeckSpec';

export type ConstraintMetric = 'words' | 'chars' | 'bullets' | 'slides';

/** What is measured. `bullet` is one bullet (a field target with an index). */
export type ConstraintSubject = 'title' | 'subtitle' | 'notes' | 'bullets' | 'bullet' | 'slide' | 'deck';

export type ConstraintKind =
  | 'shorter'    // after < before
  | 'longer'     // after > before
  | 'delta'      // after == before + n (n signed)
  | 'max'        // after <= n
  | 'min'        // after >= n
  | 'exact'      // after == n
  | 'unchanged'  // the subject's text is what it was
  | 'removed';   // the subject is gone (empty)

export interface Constraint {
  kind: ConstraintKind;
  subject: ConstraintSubject;
  metric: ConstraintMetric;
  n?: number;
  /**
   * The slide the instruction names ("the title of slide 2" → 2), 1-based.
   * Measured on that slide when the run itself is not slide-scoped (#145).
   */
  slide?: number;
  /** The fragment of the instruction it came from, for the person reading the run. */
  phrase: string;
}

export interface ConstraintScope {
  slideId?: string | undefined;
  target?: DeckReviseTarget | undefined;
}

/** One measurement, before and after the proposal. */
export interface MeasuredFact {
  subject: ConstraintSubject;
  metric: ConstraintMetric | 'text';
  /** For a numeric metric. */
  before?: number;
  after?: number;
  /** For `text`: whether the subject's text changed. */
  changed?: boolean;
  label: string;
}

export interface ConstraintViolation {
  constraint: Constraint;
  fact: MeasuredFact;
  /** For the person, and for the model on its second try. */
  message: string;
}

export interface ConstraintCheck {
  constraints: Constraint[];
  facts: MeasuredFact[];
  violations: ConstraintViolation[];
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

const NUMBER_WORDS: Record<string, number> = {
  zero: 0, no: 0, a: 1, an: 1, one: 1, single: 1, two: 2, three: 3, four: 4, five: 5,
  six: 6, seven: 7, eight: 8, nine: 9, ten: 10, eleven: 11, twelve: 12,
};
const NUM = '(\\d+|zero|a|an|one|single|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve)';
const numberOf = (raw: string | undefined): number | undefined => {
  if (raw === undefined) return undefined;
  const lower = raw.toLowerCase();
  return /^\d+$/.test(lower) ? Number(lower) : NUMBER_WORDS[lower];
};

const UNIT = '(words?|characters?|chars?|letters?|bullets?|bullet points?|points?|lines?|slides?)';
const metricOfUnit = (unit: string): ConstraintMetric => {
  const u = unit.toLowerCase();
  if (u.startsWith('word')) return 'words';
  if (u.startsWith('char') || u.startsWith('letter')) return 'chars';
  if (u.startsWith('slide')) return 'slides';
  return 'bullets';
};

const NOUN = '(title|heading|headline|subtitle|sub-title|notes?|speaker notes|bullets?|bullet points?|points?|slide|deck|presentation)';
const subjectOfNoun = (noun: string | undefined, scope: ConstraintScope): ConstraintSubject | undefined => {
  if (!noun) return undefined;
  const n = noun.toLowerCase();
  if (n.startsWith('sub')) return 'subtitle';
  if (n.startsWith('title') || n.startsWith('head')) return 'title';
  if (n.includes('note')) return 'notes';
  if (n.startsWith('bullet') || n.startsWith('point')) return scope.target?.field === 'bullets' && scope.target.index !== undefined ? 'bullet' : 'bullets';
  if (n === 'slide') return 'slide';
  return 'deck';
};

/** Where a phrase applies when it names nothing: the field, else the slide, else the deck. */
export function defaultSubject(scope: ConstraintScope): ConstraintSubject {
  const field = scope.target?.field;
  if (field === 'title') return 'title';
  if (field === 'subtitle') return 'subtitle';
  if (field === 'notes') return 'notes';
  if (field === 'bullets') return scope.target?.index !== undefined ? 'bullet' : 'bullets';
  if (scope.target) return 'slide';
  return scope.slideId ? 'slide' : 'deck';
}

const SLIDE_NUM = '(\\d+|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve)';
const ORDINALS = ['first', 'second', 'third', 'fourth', 'fifth', 'sixth', 'seventh', 'eighth', 'ninth', 'tenth', 'eleventh', 'twelfth'];
const LOCATOR = '(?:(of|on|in|for|from|to) )';
/** "slide 2's" or "slide 2 title": the slide number is a possessive, a locator. */
const POSSESSIVE = "('s|\\s(?=(?:title|heading|headline|notes?|speaker notes|bullets?|bullet points?)\\b))";

interface SlideRef { slide: number; at: number }

/**
 * The slides an instruction names ("slide 2", "slide two", "the second
 * slide"), and the instruction with every LOCATOR ("of slide 2", "on the
 * second slide", "slide 2's") blanked out — same length, so match indices
 * still line up — so "the title of slide 2" names the title, not the slide
 * (#145). "Make slide 2 shorter" keeps its noun: there the slide is the subject.
 */
function slideRefs(text: string): { refs: SlideRef[]; nounText: string } {
  const refs: SlideRef[] = [];
  let nounText = text;
  const blank = (at: number, length: number) => {
    nounText = nounText.slice(0, at) + ' '.repeat(length) + nounText.slice(at + length);
  };
  for (const m of text.matchAll(new RegExp(`\\b${LOCATOR}?(?:the )?slide (?:#|no\\.? ?)?${SLIDE_NUM}${POSSESSIVE}?\\b`, 'gi'))) {
    const slide = numberOf(m[2]);
    if (!slide) continue;
    refs.push({ slide, at: m.index ?? 0 });
    if (m[1] || m[3]) blank(m.index ?? 0, m[0].length);
  }
  for (const m of text.matchAll(new RegExp(`\\b${LOCATOR}?the (${ORDINALS.join('|')}) slide${POSSESSIVE}?\\b`, 'gi'))) {
    const slide = ORDINALS.indexOf(m[2]!.toLowerCase()) + 1;
    refs.push({ slide, at: m.index ?? 0 });
    if (m[1] || m[3]) blank(m.index ?? 0, m[0].length);
  }
  return { refs: refs.sort((a, b) => a.at - b.at), nounText };
}

/** The slide named nearest before `at`, else the first one named. */
function slideNear(refs: SlideRef[], at: number): number | undefined {
  const before = refs.filter((r) => r.at <= at);
  return (before.length ? before[before.length - 1]! : refs[0])?.slide;
}

/** The noun the instruction names nearest BEFORE the phrase, else anywhere. */
function subjectNear(instruction: string, at: number, scope: ConstraintScope): ConstraintSubject {
  const before = instruction.slice(0, at);
  const nouns = [...before.matchAll(new RegExp(`\\b${NOUN}\\b`, 'gi'))];
  const last = nouns.length ? nouns[nouns.length - 1]![1] : undefined;
  const anywhere = last ?? new RegExp(`\\b${NOUN}\\b`, 'i').exec(instruction)?.[1];
  return subjectOfNoun(anywhere, scope) ?? defaultSubject(scope);
}

/** A count subject when the phrase counts bullets/slides, else the named subject. */
function subjectForMetric(subject: ConstraintSubject, metric: ConstraintMetric, scope: ConstraintScope): ConstraintSubject {
  if (metric === 'slides') return 'deck';
  if (metric === 'bullets' && subject !== 'bullet') return subject === 'deck' || subject === 'slide' ? subject : 'bullets';
  void scope;
  return subject;
}

/** The measurable constraints an instruction states, in the order they appear. */
export function parseConstraints(instruction: string, scope: ConstraintScope = {}): Constraint[] {
  const text = instruction.replace(/\s+/g, ' ').trim();
  const { refs, nounText } = slideRefs(text);
  const out: Constraint[] = [];
  const seen = new Set<string>();
  const push = (c: Constraint) => {
    const key = `${c.kind}:${c.subject}:${c.metric}:${c.n ?? ''}:${c.slide ?? ''}`;
    if (seen.has(key)) return;
    seen.add(key);
    out.push(c);
  };
  const each = (re: RegExp, make: (m: RegExpExecArray) => Constraint | undefined) => {
    for (const m of text.matchAll(new RegExp(re.source, re.flags.includes('g') ? re.flags : `${re.flags}g`))) {
      const c = make(m as RegExpExecArray);
      if (!c) continue;
      // A deck-wide count ("add a slide") is not about one slide.
      const slide = c.subject === 'deck' ? undefined : slideNear(refs, m.index ?? 0);
      push({ ...c, ...(slide ? { slide } : {}), phrase: m[0] });
    }
  };

  // keep the title / don't change the bullets / leave the notes as they are / without changing the title
  each(new RegExp(`\\b(?:keep|don'?t (?:change|touch|alter)|do not (?:change|touch|alter)|leave|without (?:changing|touching|altering)|preserve|retain)\\b(?: the| its| my)? ${NOUN}(?: (?:as (?:it is|they are|is)|alone|unchanged|the same|intact))?`, 'i'),
    (m) => {
      const subject = subjectOfNoun(m[1], scope);
      return subject && subject !== 'deck' && subject !== 'slide' ? { kind: 'unchanged', subject, metric: 'words', phrase: '' } : undefined;
    });

  // remove the notes / delete the subtitle (a part, not a count)
  each(new RegExp(`\\b(?:remove|delete|drop|clear)\\b(?: the| its)? (notes?|speaker notes|subtitle|sub-title)\\b`, 'i'),
    (m) => ({ kind: 'removed', subject: subjectOfNoun(m[1], scope)!, metric: 'words', phrase: '' }));

  // add a bullet / add two bullets / another bullet / add a slide
  each(new RegExp(`\\b(?:add|append|insert|include)\\b(?: in| to)?(?: ${NUM}| another| one more| a new| a further)? (bullets?|bullet points?|points?|slides?)\\b`, 'i'),
    (m) => {
      const metric = metricOfUnit(m[2]!);
      const n = numberOf(m[1]) ?? 1;
      return { kind: 'delta', subject: metric === 'slides' ? 'deck' : subjectForMetric(subjectNear(nounText, m.index ?? 0, scope), metric, scope), metric, n, phrase: '' };
    });
  each(/\b(?:another|one more) (bullets?|bullet points?|points?|slides?)\b/gi,
    (m) => {
      const metric = metricOfUnit(m[1]!);
      return { kind: 'delta', subject: metric === 'slides' ? 'deck' : subjectForMetric(subjectNear(nounText, m.index ?? 0, scope), metric, scope), metric, n: 1, phrase: '' };
    });

  // remove a bullet / drop the last bullet / delete two bullets / cut a slide
  each(new RegExp(`\\b(?:remove|delete|drop|cut|take out|take away)\\b(?: the)?(?: ${NUM}| the last| the first| the second| the third| last| first)? (bullets?|bullet points?|points?|slides?)\\b`, 'i'),
    (m) => {
      const metric = metricOfUnit(m[2]!);
      const n = numberOf(m[1]) ?? 1;
      return { kind: 'delta', subject: metric === 'slides' ? 'deck' : subjectForMetric(subjectNear(nounText, m.index ?? 0, scope), metric, scope), metric, n: -n, phrase: '' };
    });

  // one word shorter / 2 bullets fewer / one word longer / a word shorter
  each(new RegExp(`\\b${NUM} ${UNIT} (shorter|fewer|less|longer|more)\\b`, 'i'),
    (m) => {
      const metric = metricOfUnit(m[2]!);
      const n = numberOf(m[1]) ?? 1;
      const sign = /shorter|fewer|less/i.test(m[3]!) ? -1 : 1;
      return { kind: 'delta', subject: subjectForMetric(subjectNear(nounText, m.index ?? 0, scope), metric, scope), metric, n: sign * n, phrase: '' };
    });
  // shorter by one word / fewer by two bullets
  each(new RegExp(`\\b(shorter|fewer|longer|more) by ${NUM} ${UNIT}\\b`, 'i'),
    (m) => {
      const metric = metricOfUnit(m[3]!);
      const n = numberOf(m[2]) ?? 1;
      const sign = /shorter|fewer/i.test(m[1]!) ? -1 : 1;
      return { kind: 'delta', subject: subjectForMetric(subjectNear(nounText, m.index ?? 0, scope), metric, scope), metric, n: sign * n, phrase: '' };
    });

  // 10 words or fewer / at most 40 characters / no more than three bullets / under 40 characters / within 12 words / max 8 words
  each(new RegExp(`\\b(?:at most|no more than|not more than|under|below|less than|fewer than|within|max(?:imum)?(?: of)?|up to) ${NUM} ${UNIT}\\b`, 'i'),
    (m) => {
      const metric = metricOfUnit(m[2]!);
      const strict = /^(under|below|less than|fewer than)/i.test(m[0]);
      const n = (numberOf(m[1]) ?? 0) - (strict ? 1 : 0);
      return { kind: 'max', subject: subjectForMetric(subjectNear(nounText, m.index ?? 0, scope), metric, scope), metric, n: Math.max(0, n), phrase: '' };
    });
  each(new RegExp(`\\b${NUM} ${UNIT} (?:or fewer|or less|max(?:imum)?|tops|at most|or under)\\b`, 'i'),
    (m) => {
      const metric = metricOfUnit(m[2]!);
      return { kind: 'max', subject: subjectForMetric(subjectNear(nounText, m.index ?? 0, scope), metric, scope), metric, n: numberOf(m[1]) ?? 0, phrase: '' };
    });
  // at least 3 bullets / 3 bullets or more / more than 20 words
  each(new RegExp(`\\b(?:at least|no fewer than|not fewer than|minimum(?: of)?|min) ${NUM} ${UNIT}\\b`, 'i'),
    (m) => {
      const metric = metricOfUnit(m[2]!);
      return { kind: 'min', subject: subjectForMetric(subjectNear(nounText, m.index ?? 0, scope), metric, scope), metric, n: numberOf(m[1]) ?? 0, phrase: '' };
    });
  each(new RegExp(`\\b(?<!no |not )(?:more than|over|above) ${NUM} ${UNIT}\\b`, 'i'),
    (m) => {
      const metric = metricOfUnit(m[2]!);
      return { kind: 'min', subject: subjectForMetric(subjectNear(nounText, m.index ?? 0, scope), metric, scope), metric, n: (numberOf(m[1]) ?? 0) + 1, phrase: '' };
    });
  each(new RegExp(`\\b${NUM} ${UNIT} or more\\b`, 'i'),
    (m) => {
      const metric = metricOfUnit(m[2]!);
      return { kind: 'min', subject: subjectForMetric(subjectNear(nounText, m.index ?? 0, scope), metric, scope), metric, n: numberOf(m[1]) ?? 0, phrase: '' };
    });

  // exactly three bullets / to 3 bullets / down to two words / make the title 2 words / a two-word title / 0 words
  each(new RegExp(`\\b(?:(?:exactly|precisely|just|only|(?:down |up )?to|into|of|be) |(?:make|keep) (?:it|this|the ${NOUN}|the) )?${NUM}[ -]${UNIT}\\b(?! (?:or|shorter|fewer|less|longer|more|max|tops|at most|or under|or more|about))`, 'i'),
    (m) => {
      // A bare number before a unit is only exact when a lead word sets it ("to",
      // "exactly", "make the title 2 words"); "add two bullets" was taken above.
      const lead = /^(exactly|precisely|just|only|to|down to|up to|into|of|be|make|keep)\b/i.test(m[0]);
      if (!lead) return undefined;
      if (/^up to/i.test(m[0])) return undefined; // a max, taken above
      const metric = metricOfUnit(m[3]!);
      return { kind: 'exact', subject: subjectForMetric(subjectNear(nounText, m.index ?? 0, scope), metric, scope), metric, n: numberOf(m[2]) ?? 0, phrase: '' };
    });

  // fewer bullets / more bullets / fewer slides (no number)
  each(/\b(fewer|less|more) (bullets?|bullet points?|points?|slides?)\b/gi,
    (m) => {
      const metric = metricOfUnit(m[2]!);
      return { kind: /fewer|less/i.test(m[1]!) ? 'shorter' : 'longer', subject: metric === 'slides' ? 'deck' : subjectForMetric(subjectNear(nounText, m.index ?? 0, scope), metric, scope), metric, phrase: '' };
    });

  // shorter / briefer / more concise / tighter / trim / shorten / condense — and longer / expand
  each(/\b(shorter|briefer|more concise|concise|tighter|tighten|trim(?:med)?|shorten(?:ed)?|condense[d]?|cut down|pare down|terser|crisper|punchier|fewer words|less wordy)\b/gi,
    (m) => ({ kind: 'shorter', subject: subjectNear(nounText, m.index ?? 0, scope), metric: 'words', phrase: '' }));
  each(/\b(longer|expand(?:ed)?|elaborate|flesh out|more detail(?:ed)?|more words|lengthen)\b/gi,
    (m) => ({ kind: 'longer', subject: subjectNear(nounText, m.index ?? 0, scope), metric: 'words', phrase: '' }));

  // A delta/max/exact on a subject makes the bare "shorter" on the same
  // subject redundant, and a bullet COUNT phrase must not also read as words.
  const precise = new Set(out.filter((c) => c.kind === 'delta' || c.kind === 'max' || c.kind === 'exact' || c.kind === 'min').map((c) => `${c.slide ?? ''}:${c.subject}:${c.kind === 'delta' && c.n! < 0 ? 'shorter' : c.kind === 'delta' ? 'longer' : c.kind === 'max' ? 'shorter' : c.kind === 'min' ? 'longer' : 'exact'}`));
  return out.filter((c) => !((c.kind === 'shorter' || c.kind === 'longer') && (precise.has(`${c.slide ?? ''}:${c.subject}:${c.kind}`) || precise.has(`${c.slide ?? ''}:${c.subject}:exact`))));
}

// ---------------------------------------------------------------------------
// Measuring
// ---------------------------------------------------------------------------

export const countWords = (text: string | undefined): number => (text ?? '').trim().split(/\s+/).filter(Boolean).length;
const countChars = (text: string | undefined): number => (text ?? '').trim().length;

function slideText(slide: DeckSlide | undefined): string {
  if (!slide) return '';
  return [
    slide.eyebrow, slide.title, ...(slide.bullets ?? []), ...(slide.columns?.flat() ?? []),
    slide.quote?.text, slide.quote?.by, slide.notes,
  ].filter(Boolean).join('\n');
}

function slideBullets(slide: DeckSlide | undefined): string[] {
  return [...(slide?.bullets ?? []), ...(slide?.columns?.flat() ?? [])];
}

/** The subject's text and count under `metric`, in `spec` at `scope`. */
function measureSubject(spec: DeckSpec, scope: ConstraintScope, subject: ConstraintSubject, metric: ConstraintMetric): { text: string; count: number } {
  const slide = scope.slideId ? spec.slides.find((s) => s.id === scope.slideId) : undefined;
  let text = '';
  let items: string[] | undefined;
  switch (subject) {
    case 'title': text = slide ? slide.title : spec.title; break;
    case 'subtitle': text = spec.subtitle ?? ''; break;
    case 'notes': text = slide?.notes ?? ''; break;
    case 'bullet': {
      const index = scope.target?.index ?? 0;
      text = slideBullets(slide)[index] ?? '';
      break;
    }
    case 'bullets': items = slide ? slideBullets(slide) : spec.slides.flatMap(slideBullets); text = items.join('\n'); break;
    case 'slide': items = slideBullets(slide); text = slideText(slide); break;
    case 'deck': items = spec.slides.flatMap(slideBullets); text = spec.slides.map(slideText).join('\n'); break;
    default: text = '';
  }
  const count = metric === 'words' ? countWords(text)
    : metric === 'chars' ? countChars(text)
      : metric === 'slides' ? spec.slides.length
        : (items ?? (text ? [text] : [])).length;
  return { text, count };
}

const SUBJECT_LABEL: Record<ConstraintSubject, string> = {
  title: 'Title', subtitle: 'Subtitle', notes: 'Notes', bullets: 'Bullets', bullet: 'Bullet', slide: 'Slide', deck: 'Deck',
};
const UNIT_LABEL: Record<ConstraintMetric, string> = { words: 'words', chars: 'characters', bullets: 'bullets', slides: 'slides' };
/** "1 word", "2 words". */
const plural = (n: number, metric: ConstraintMetric): string => (n === 1 ? UNIT_LABEL[metric].replace(/s$/, '') : UNIT_LABEL[metric]);

/** "Title: 3 → 2 words" / "Bullet 2: 12 → 9 words" / "Notes: unchanged". */
export function factLabel(fact: MeasuredFact, scope: ConstraintScope = {}): string {
  const who = fact.subject === 'bullet' && scope.target?.index !== undefined
    ? `Bullet ${scope.target.index + 1}`
    : fact.subject === 'title' && !scope.slideId ? 'Deck title' : SUBJECT_LABEL[fact.subject];
  if (fact.metric === 'text') return `${who}: ${fact.changed ? 'changed' : 'unchanged'}`;
  return `${who}: ${fact.before} → ${fact.after} ${plural(fact.after ?? 0, fact.metric)}`;
}

function measureFact(before: DeckSpec, after: DeckSpec, scope: ConstraintScope, subject: ConstraintSubject, metric: ConstraintMetric | 'text'): MeasuredFact {
  if (metric === 'text') {
    const b = measureSubject(before, scope, subject, 'words').text;
    const a = measureSubject(after, scope, subject, 'words').text;
    const fact: MeasuredFact = { subject, metric, changed: a !== b, label: '' };
    return { ...fact, label: factLabel(fact, scope) };
  }
  const fact: MeasuredFact = {
    subject, metric,
    before: measureSubject(before, scope, subject, metric).count,
    after: measureSubject(after, scope, subject, metric).count,
    label: '',
  };
  return { ...fact, label: factLabel(fact, scope) };
}

/** What is measured even with no constraint: the field, else the slide's title and bullets, else the deck's slides. */
function defaultMeasures(scope: ConstraintScope): Array<[ConstraintSubject, ConstraintMetric]> {
  const subject = defaultSubject(scope);
  if (subject === 'bullets') return [['bullets', 'bullets'], ['bullets', 'words']];
  if (subject === 'slide') return [['title', 'words'], ['bullets', 'bullets']];
  if (subject === 'deck') return [['deck', 'slides'], ['title', 'words']];
  return [[subject, 'words']];
}

function violationMessage(c: Constraint, fact: MeasuredFact): string | undefined {
  const who = fact.label.split(':')[0]!;
  const b = fact.before ?? 0;
  const a = fact.after ?? 0;
  const has = `${who} has ${a} ${plural(a, c.metric)}`;
  switch (c.kind) {
    case 'shorter': return a < b ? undefined : `${has}; before it had ${b}. It must have fewer.`;
    case 'longer': return a > b ? undefined : `${has}; before it had ${b}. It must have more.`;
    case 'delta': {
      const want = Math.max(0, b + (c.n ?? 0));
      return a === want ? undefined : `${has}; before it had ${b}. Make it ${want}.`;
    }
    case 'max': return a <= (c.n ?? 0) ? undefined : `${has}; it must have at most ${c.n}.`;
    case 'min': return a >= (c.n ?? 0) ? undefined : `${has}; it must have at least ${c.n}.`;
    case 'exact': return a === (c.n ?? 0) ? undefined : `${has}; it must have exactly ${c.n}.`;
    case 'unchanged': return fact.changed ? `${who} was changed; the instruction said to keep it as it was.` : undefined;
    case 'removed': return (fact.after ?? 0) === 0 ? undefined : `${who} is still there (${a} ${plural(a, c.metric)}); the instruction said to remove it.`;
    default: return undefined;
  }
}

/**
 * Measures `after` against `before` for every constraint, every claim the
 * summary makes, and the scope's default subjects, and names each constraint
 * the proposal does not meet.
 */
export function checkConstraints(constraints: Constraint[], before: DeckSpec, after: DeckSpec, scope: ConstraintScope = {}, claims: Constraint[] = []): ConstraintCheck {
  const facts = new Map<string, MeasuredFact>();
  // The slide the instruction names, when the run is not already scoped to
  // one: a deck-wide "shorten the title of slide 2" measures slide 2's title,
  // not the deck title (#145). A slide-scoped run can only change its slide.
  const scopeFor = (slide: number | undefined): ConstraintScope => {
    if (!slide || scope.slideId) return scope;
    const named = before.slides[slide - 1];
    return named ? { slideId: named.id } : scope;
  };
  const factFor = (subject: ConstraintSubject, metric: ConstraintMetric | 'text', slide?: number): MeasuredFact => {
    const at = scopeFor(slide);
    const key = `${at.slideId ?? ''}:${subject}:${metric}`;
    let fact = facts.get(key);
    if (!fact) {
      fact = measureFact(before, after, at, subject, metric);
      facts.set(key, fact);
    }
    return fact;
  };
  const violations: ConstraintViolation[] = [];
  for (const c of constraints) {
    const fact = factFor(c.subject, c.kind === 'unchanged' ? 'text' : c.metric, c.slide);
    const message = violationMessage(c, fact);
    if (message) violations.push({ constraint: c, fact, message });
  }
  // What the model's own summary claims ("made the title one word shorter",
  // parsed like an instruction) is measured too, and always listed: that is
  // the number the claim is checked against.
  // A claim that names no slide ("renamed the slide title") is about the
  // slide the instruction named, and one that names a part no slide holds
  // (a slide-level part in a deck-wide run with no slide) measures nothing.
  const named = constraints.find((c) => c.slide !== undefined)?.slide;
  for (const claim of claims) {
    const at = claim.subject === 'deck' ? undefined : claim.slide ?? named;
    if (!scopeFor(at).slideId && (claim.subject === 'slide' || claim.subject === 'notes' || claim.subject === 'bullet')) continue;
    factFor(claim.subject, claim.kind === 'unchanged' ? 'text' : claim.metric, at);
  }
  // The default measures are facts too, but only worth a line when something moved.
  for (const [subject, metric] of defaultMeasures(scope)) {
    const key = `${scope.slideId ?? ''}:${subject}:${metric}`;
    if (facts.has(key)) continue;
    const fact = measureFact(before, after, scope, subject, metric);
    if (fact.before !== fact.after) facts.set(key, fact);
  }
  return { constraints, facts: [...facts.values()], violations };
}

// ---------------------------------------------------------------------------
// What moved, when nothing was counted
// ---------------------------------------------------------------------------

const SLIDE_PARTS: Array<[keyof DeckSlide, string]> = [
  ['layout', 'layout'], ['eyebrow', 'eyebrow'], ['title', 'title'], ['bullets', 'bullets'], ['columns', 'columns'],
  ['chart', 'chart'], ['quote', 'quote'], ['image', 'image'], ['notes', 'notes'], ['sources', 'sources'],
];
const sameValue = (a: unknown, b: unknown): boolean => JSON.stringify(a ?? null) === JSON.stringify(b ?? null);

/**
 * The parts an edit changed, as short factual phrases in slide order —
 * "slide 1 subtitle", "slide 3 bullets", "slide 4 added", "deck title". The
 * deck subtitle is drawn on the title slide, so it is named by that slide.
 * Deterministic, from the two specs; never the model's word.
 */
export function describeChanges(before: DeckSpec, after: DeckSpec): string[] {
  const out: string[] = [];
  if ((before.title ?? '') !== (after.title ?? '')) out.push('deck title');
  if ((before.subtitle ?? '') !== (after.subtitle ?? '')) {
    const at = after.slides.findIndex((s) => s.layout === 'title');
    out.push(at >= 0 ? `slide ${at + 1} subtitle` : 'deck subtitle');
  }
  if (!sameValue(before.theme, after.theme)) out.push('theme');
  const beforeById = new Map(before.slides.map((s, i) => [s.id, { slide: s, index: i }]));
  const afterIds = new Set(after.slides.map((s) => s.id));
  let reordered = false;
  after.slides.forEach((slide, i) => {
    const prev = beforeById.get(slide.id);
    if (!prev) { out.push(`slide ${i + 1} added`); return; }
    if (prev.index !== i) reordered = true;
    const parts = SLIDE_PARTS.filter(([key]) => !sameValue(prev.slide[key], slide[key])).map(([, label]) => label);
    if (parts.length) out.push(`slide ${i + 1} ${parts.join(' and ')}`);
  });
  before.slides.forEach((slide, i) => { if (!afterIds.has(slide.id)) out.push(`slide ${i + 1} removed`); });
  if (reordered && before.slides.length === after.slides.length) out.push('slide order');
  return out;
}

/** The lines a run shows for its measurements: "Title: 3 → 2 words". */
export const factLines = (facts: MeasuredFact[]): string[] => facts.map((fact) => fact.label);

// ---------------------------------------------------------------------------
// The model's own claim
// ---------------------------------------------------------------------------

const SHORTER_CLAIM = /\b(shorter|shortened|shortening|fewer|less wordy|trimmed|trim|condensed|tightened|concise|cut (?:down|out|it|the|one|two|a)\b|reduced|removed (?:a|one|two|\d+) words?|dropped (?:a|one|two|\d+) words?|one word fewer|briefer|pared)\b/i;
const LONGER_CLAIM = /\b(longer|lengthened|expanded|elaborated|added (?:a|one|two|three|\d+) words?|more detail)\b/i;
const CLAIM_SENTENCE = /(?<=[.!?])\s+(?=[A-Z"“])/;

/**
 * The summary the run records: the model's sentence(s) minus any that claim
 * a direction the measurements contradict, then the measured facts. A summary
 * that loses every sentence falls back to a deterministic line.
 */
export function reconcileSummary(summary: string, facts: MeasuredFact[], fallback: string, changes: string[] = []): string {
  const numeric = facts.filter((f): f is MeasuredFact & { before: number; after: number } => typeof f.before === 'number' && typeof f.after === 'number');
  const measuredShorter = numeric.some((f) => f.after < f.before);
  const measuredLonger = numeric.some((f) => f.after > f.before);
  const measuredAnything = numeric.length > 0;
  const sentences = summary.split(CLAIM_SENTENCE).map((s) => s.trim()).filter(Boolean);
  const kept = sentences.filter((sentence) => {
    if (measuredAnything && !measuredShorter && SHORTER_CLAIM.test(sentence)) return false;
    if (measuredAnything && !measuredLonger && LONGER_CLAIM.test(sentence)) return false;
    return true;
  });
  const claim = kept.length ? kept.join(' ') : fallback;
  const dropped = kept.length < sentences.length;
  // Nothing countable → say plainly what moved instead ("Changed: slide 1 subtitle").
  const measured = facts.length ? ` Measured: ${factLines(facts).join('; ')}.`
    : changes.length ? ` Changed: ${changes.join(', ')}.` : '';
  const corrected = dropped ? ' The agent’s own note was left out where the numbers contradict it.' : '';
  return `${claim}${measured}${corrected}`.trim();
}

/** What the model is told on its second try. */
export function retryInstruction(instruction: string, violations: ConstraintViolation[]): string {
  const lines = violations.map((v) => `- ${v.message}`).join('\n');
  return `${instruction}\n\nYour previous answer did not do this. Measured against the deck as it was:\n${lines}\nAnswer again so that each line above is satisfied; count the words yourself before answering.`;
}
