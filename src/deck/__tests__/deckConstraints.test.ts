// The deterministic parser behind the `check` step of `deck-revise`, and the
// measurement it makes. No model: an instruction's countable phrases in, the
// constraints out; a before/after pair in, facts and violations out.

import {
  checkConstraints, factLines, parseConstraints, reconcileSummary, retryInstruction, type Constraint,
} from '../deckConstraints';
import type { DeckSpec } from '../DeckSpec';
import { sampleDeck } from './fixtures';

const deck = sampleDeck();
const AGENDA = 'agenda';
const slide = { slideId: AGENDA };

const pick = (constraints: Constraint[]) => constraints.map(({ kind, subject, metric, n }) => ({ kind, subject, metric, ...(n !== undefined ? { n } : {}) }));

describe('parseConstraints', () => {
  it.each<[string, ReturnType<typeof pick>]>([
    ['make this slide\'s title one word shorter', [{ kind: 'delta', subject: 'title', metric: 'words', n: -1 }]],
    ['make the title shorter', [{ kind: 'shorter', subject: 'title', metric: 'words' }]],
    ['shorten the title', [{ kind: 'shorter', subject: 'title', metric: 'words' }]],
    ['tighten the bullets', [{ kind: 'shorter', subject: 'bullets', metric: 'words' }]],
    ['title: 10 words or fewer', [{ kind: 'max', subject: 'title', metric: 'words', n: 10 }]],
    ['keep the title under 40 characters', [{ kind: 'unchanged', subject: 'title', metric: 'words' }, { kind: 'max', subject: 'title', metric: 'chars', n: 39 }]],
    ['at most three bullets', [{ kind: 'max', subject: 'bullets', metric: 'bullets', n: 3 }]],
    ['no more than 2 bullets', [{ kind: 'max', subject: 'bullets', metric: 'bullets', n: 2 }]],
    ['fewer bullets', [{ kind: 'shorter', subject: 'bullets', metric: 'bullets' }]],
    ['cut it down to exactly two bullets', [{ kind: 'exact', subject: 'bullets', metric: 'bullets', n: 2 }]],
    ['add a bullet about the retry policy', [{ kind: 'delta', subject: 'bullets', metric: 'bullets', n: 1 }]],
    ['add two bullets from the transcript', [{ kind: 'delta', subject: 'bullets', metric: 'bullets', n: 2 }]],
    ['remove the last bullet', [{ kind: 'delta', subject: 'bullets', metric: 'bullets', n: -1 }]],
    ['drop one bullet and keep the title', [{ kind: 'unchanged', subject: 'title', metric: 'words' }, { kind: 'delta', subject: 'bullets', metric: 'bullets', n: -1 }]],
    ['make the title 0 words', [{ kind: 'exact', subject: 'title', metric: 'words', n: 0 }]],
    ['make the title 2 words', [{ kind: 'exact', subject: 'title', metric: 'words', n: 2 }]],
    ['rewrite the bullets, don\'t change the title', [{ kind: 'unchanged', subject: 'title', metric: 'words' }]],
    ['remove the notes', [{ kind: 'removed', subject: 'notes', metric: 'words' }]],
    ['make the notes two words longer', [{ kind: 'delta', subject: 'notes', metric: 'words', n: 2 }]],
    ['shorter by one word', [{ kind: 'delta', subject: 'slide', metric: 'words', n: -1 }]],
    ['at least 3 bullets', [{ kind: 'min', subject: 'bullets', metric: 'bullets', n: 3 }]],
    ['rename the slide to match the transcript', []],
  ])('%s', (instruction, expected) => {
    expect(pick(parseConstraints(instruction, slide))).toEqual(expected);
  });

  it('scopes a bare "shorter" to the field it was asked on, and to the deck when nothing is named', () => {
    expect(pick(parseConstraints('shorter', { slideId: AGENDA, target: { field: 'notes' } }))).toEqual([{ kind: 'shorter', subject: 'notes', metric: 'words' }]);
    expect(pick(parseConstraints('one word shorter', { slideId: AGENDA, target: { field: 'bullets', index: 1 } }))).toEqual([{ kind: 'delta', subject: 'bullet', metric: 'words', n: -1 }]);
    expect(pick(parseConstraints('two slides shorter', {}))).toEqual([{ kind: 'delta', subject: 'deck', metric: 'slides', n: -2 }]);
    expect(pick(parseConstraints('remove a slide', {}))).toEqual([{ kind: 'delta', subject: 'deck', metric: 'slides', n: -1 }]);
  });

  it('keeps the phrase each constraint came from', () => {
    expect(parseConstraints('make the title one word shorter', slide)[0]!.phrase).toBe('one word shorter');
  });
});

const retitled = (title: string): DeckSpec => ({ ...deck, slides: deck.slides.map((s) => (s.id === AGENDA ? { ...s, title } : s)) });
const rebulleted = (bullets: string[]): DeckSpec => ({ ...deck, slides: deck.slides.map((s) => (s.id === AGENDA ? { ...s, bullets } : s)) });

describe('checkConstraints', () => {
  it('passes a one-word-shorter title and records the fact', () => {
    const check = checkConstraints(parseConstraints('make the title one word shorter', slide), retitled('Agenda for today'), retitled('Today’s agenda'), slide);
    expect(check.violations).toEqual([]);
    expect(factLines(check.facts)).toEqual(['Title: 3 → 2 words']);
    // Nothing named: the whole slide is the subject.
    const whole = checkConstraints(parseConstraints('one word shorter', slide), retitled('Agenda for today'), retitled('Today’s agenda'), slide);
    expect(whole.violations).toEqual([]);
    expect(factLines(whole.facts)).toEqual(['Slide: 16 → 15 words', 'Title: 3 → 2 words']);
  });

  it('fails a same-length rename that claims to be shorter (the 2026-09-23 case)', () => {
    const before = retitled('Managing release risk');
    const after = retitled('Release risk management');
    const check = checkConstraints(parseConstraints('make this slide\'s title one word shorter', slide), before, after, slide);
    expect(check.violations.map((v) => v.message)).toEqual(['Title has 3 words; before it had 3. Make it 2.']);
    expect(factLines(check.facts)).toEqual(['Title: 3 → 3 words']);
  });

  it('an impossible constraint fails with the numbers', () => {
    const check = checkConstraints(parseConstraints('make the title 0 words', slide), deck, retitled('Plan'), slide);
    expect(check.violations[0]!.message).toBe('Title has 1 word; it must have exactly 0.');
  });

  it('counts bullets: max, add, remove, fewer', () => {
    const three = deck; // agenda has 3 bullets
    expect(checkConstraints(parseConstraints('at most two bullets', slide), three, rebulleted(['a', 'b']), slide).violations).toEqual([]);
    expect(checkConstraints(parseConstraints('at most two bullets', slide), three, three, slide).violations[0]!.message).toBe('Bullets has 3 bullets; it must have at most 2.');
    expect(checkConstraints(parseConstraints('add a bullet', slide), three, rebulleted([...three.slides[1]!.bullets!, 'd']), slide).violations).toEqual([]);
    expect(checkConstraints(parseConstraints('add a bullet', slide), three, three, slide).violations[0]!.message).toBe('Bullets has 3 bullets; before it had 3. Make it 4.');
    expect(checkConstraints(parseConstraints('remove a bullet', slide), three, rebulleted(['a', 'b']), slide).violations).toEqual([]);
    expect(factLines(checkConstraints(parseConstraints('fewer bullets', slide), three, rebulleted(['a']), slide).facts)).toEqual(['Bullets: 3 → 1 bullet']);
  });

  it('keep the title: a changed title is a violation even when the bullets were the ask', () => {
    const check = checkConstraints(parseConstraints('tighten the bullets, keep the title', slide), deck, { ...retitled('Plan'), slides: rebulleted(['a']).slides.map((s) => (s.id === AGENDA ? { ...s, title: 'Plan' } : s)) }, slide);
    expect(check.violations.map((v) => v.message)).toEqual(['Title was changed; the instruction said to keep it as it was.']);
    expect(factLines(check.facts)).toEqual(['Title: changed', 'Bullets: 8 → 1 word', 'Bullets: 3 → 1 bullet']);
  });

  it('with no constraint, measures the scope anyway and reports only what moved', () => {
    expect(factLines(checkConstraints([], deck, retitled('Agenda'), slide).facts)).toEqual([]);
    expect(factLines(checkConstraints([], deck, retitled('The agenda today'), slide).facts)).toEqual(['Title: 1 → 3 words']);
    expect(factLines(checkConstraints([], deck, { ...deck, slides: deck.slides.slice(1) }, {}).facts)).toEqual(['Deck: 8 → 7 slides']);
  });

  it('a field target measures that field', () => {
    const scope = { slideId: AGENDA, target: { field: 'bullets' as const, index: 0 } };
    const check = checkConstraints(parseConstraints('one word shorter', scope), deck, rebulleted(['Shipped', 'What it cost', 'What is next']), scope);
    expect(check.violations).toEqual([]);
    expect(factLines(check.facts)).toEqual(['Bullet 1: 2 → 1 word']);
  });
});

describe('reconcileSummary', () => {
  it('appends the measurements to the model\'s summary', () => {
    const check = checkConstraints(parseConstraints('make the title one word shorter', slide), retitled('Agenda for today'), retitled('Today’s agenda'), slide);
    expect(reconcileSummary('Shortened the title by one word.', check.facts, 'Changed 1 slide.')).toBe('Shortened the title by one word. Measured: Title: 3 → 2 words.');
  });

  it('drops a claim the numbers contradict and says so', () => {
    const check = checkConstraints([], retitled('Managing release risk'), retitled('Release risk management'), slide);
    // No length moved: the title fact is not even listed, but the claim still contradicts the (unchanged) measurement.
    const facts = [{ subject: 'title' as const, metric: 'words' as const, before: 3, after: 3, label: 'Title: 3 → 3 words' }, ...check.facts];
    expect(reconcileSummary('Renamed the slide. Made the title one word shorter.', facts, 'Changed 1 slide.'))
      .toBe('Renamed the slide. Measured: Title: 3 → 3 words. The agent’s own note was left out where the numbers contradict it.');
    expect(reconcileSummary('Shortened the title.', facts, 'Changed 1 slide.'))
      .toBe('Changed 1 slide. Measured: Title: 3 → 3 words. The agent’s own note was left out where the numbers contradict it.');
  });

  it('leaves a summary alone when nothing was measured', () => {
    expect(reconcileSummary('Renamed the slide.', [], 'Changed 1 slide.')).toBe('Renamed the slide.');
  });
});

describe('retryInstruction', () => {
  it('tells the model the numbers, under the original instruction', () => {
    const check = checkConstraints(parseConstraints('make the title one word shorter', slide), retitled('Managing release risk'), retitled('Release risk management'), slide);
    const again = retryInstruction('make the title one word shorter', check.violations);
    expect(again.startsWith('make the title one word shorter\n\n')).toBe(true);
    expect(again).toContain('- Title has 3 words; before it had 3. Make it 2.');
  });
});
