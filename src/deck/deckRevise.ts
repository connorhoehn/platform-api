// An instruction applied to a deck: "make this slide's title shorter", "add a
// slide on release risks". The editor's agent composer posts here.
//
// Same model path as the fixture (`askModel`, the light model), same contract
// (`validateDeckSpec`), and the same grounding rule: the model is handed the
// real text of the sources the deck cites (and any the editor adds), may cite
// only those, and every slide it returns must cite one of them.
//
// What this does NOT have is an offline fallback. The fixture can fold
// material into a deterministic deck and label it `[deck-fixture]`; there is no
// honest deterministic version of "make the title shorter". So when the model
// is unavailable, times out, or answers with something that does not validate,
// the caller gets an error with the reason and the deck is untouched — never an
// edit that looks like the model's and is not.
//
// Nothing is persisted. The editor applies the returned spec to its own state,
// the preview redraws, and Save is still the explicit act that writes a
// revision.

import { ScanCommand } from '@aws-sdk/lib-dynamodb';
import { docClient } from '../lib/aws-clients';
import log from '../lib/logger';
import { CALL_RECORDINGS_TABLE } from '../repositories/CallRecordingRepository';
import { createLLMClient } from '../pipeline/createLLMClient';
import type { LLMClient } from '../pipeline/LLMClient';
import {
  MAX_BULLETS_PER_SLIDE, MAX_BULLET_CHARS, MAX_NOTES_CHARS, MAX_SLIDES, MAX_SOURCES_PER_SLIDE, MAX_TITLE_CHARS,
  deckSources, describeReviseTarget, parseDeckSpecJson, validateDeckSpec,
  type DeckReviseTarget, type DeckSlide, type DeckSource, type DeckSpec,
} from './DeckSpec';
import {
  WIRE_LOBBY, askModel, materialBlock, readFixtureMaterial,
  type FixtureMaterial, type ReadMaterialDeps,
} from './deckFixture';

export const REVISE_TIMEOUT_MS = 45_000;
export const MAX_INSTRUCTION_CHARS = 2000;
export const MAX_REVISE_SOURCES = 6;
const MAX_SUMMARY_CHARS = 300;

/** How the editor names a source it wants the edit grounded in. */
export interface DeckReviseSourceRef {
  kind: 'document' | 'transcript' | 'recording';
  id: string;
}

export interface DeckReviseInput {
  spec: DeckSpec;
  instruction: string;
  /** Present: the edit is confined to this slide. Absent: the whole deck. */
  slideId?: string;
  /**
   * Present: the edit is confined to one field of `slideId`'s slide (or the
   * deck's subtitle). Validated by `validateReviseTarget` before it gets here.
   * Everything outside the field is copied from the input, not from the model.
   */
  target?: DeckReviseTarget;
  sources?: DeckReviseSourceRef[];
}

/**
 * Where a revise is, for a progress line: the sources are being read, the
 * model is thinking (the long part — up to REVISE_TIMEOUT_MS), the answer is
 * being checked against the deck's rules. Reported in this order, each once.
 */
export type DeckRevisePhase = 'reading-sources' | 'asking-model' | 'checking';

export interface DeckReviseResult {
  spec: DeckSpec;
  summary: string;
  changedSlideIds: string[];
  warnings: string[];
  /** Always 'model': there is no offline edit (see the file header). */
  source: 'model';
}

/**
 * The model could not produce an edit. `status` is what the route answers:
 * 503 when no model is reachable, 502 when it answered with something unusable,
 * 400 when there is nothing to ground an edit in.
 */
export class DeckReviseError extends Error {
  constructor(message: string, readonly status: 400 | 502 | 503, readonly reason: string, readonly errors: string[] = []) {
    super(message);
  }
}

/** One source, read, with the id the deck cites it by. */
interface ReadSource {
  documentId: string;
  title: string;
  material: FixtureMaterial;
}

export interface DeckReviseOpts extends ReadMaterialDeps {
  llm?: LLMClient | null;
  timeoutMs?: number;
  /** A transcript id → the recording that holds it. Injected in tests. */
  findRecordingIdByTranscript?: (transcriptId: string) => Promise<string | null>;
  /** Told as the revise moves between phases. Must not throw; a throw is ignored. */
  onPhase?: (phase: DeckRevisePhase) => void;
}

/**
 * The recording behind a transcript id. There is no index on `transcriptId`;
 * this scans, the same way the work graph resolves a transcript
 * (`activityReader.transcript`). A deck cites a handful of sources, and this
 * runs once per instruction, not per keystroke.
 */
export async function findRecordingIdByTranscript(transcriptId: string): Promise<string | null> {
  const id = transcriptId.replace(/^transcript:/, '');
  let ExclusiveStartKey: Record<string, unknown> | undefined;
  for (let page = 0; page < 20; page += 1) {
    const out = await docClient.send(new ScanCommand({
      TableName: CALL_RECORDINGS_TABLE,
      FilterExpression: 'transcriptId = :id OR recordingId = :id',
      ExpressionAttributeValues: { ':id': id },
      ProjectionExpression: 'recordingId',
      ...(ExclusiveStartKey ? { ExclusiveStartKey } : {}),
    }));
    const hit = (out.Items ?? []).find((item) => typeof item.recordingId === 'string');
    if (hit) return hit.recordingId as string;
    ExclusiveStartKey = out.LastEvaluatedKey as Record<string, unknown> | undefined;
    if (!ExclusiveStartKey) break;
  }
  return null;
}

const hasText = (material: FixtureMaterial): boolean =>
  !!material.transcript?.segments.length || !!material.document?.text.trim();

/**
 * Reads each source by what it is. A `document` whose body is empty is tried as
 * a transcript too: revisions saved before sources carried a kind recorded
 * transcripts as documents, and an empty read would ground nothing.
 */
async function readSource(ref: DeckReviseSourceRef, citedTitle: string | undefined, opts: DeckReviseOpts): Promise<ReadSource> {
  const findRecording = opts.findRecordingIdByTranscript ?? findRecordingIdByTranscript;
  const asTranscript = async (): Promise<ReadSource | null> => {
    const recordingId = ref.kind === 'recording' ? ref.id : await findRecording(ref.id);
    if (!recordingId) return null;
    const material = await readFixtureMaterial({ recordingId }, opts);
    if (!material.transcript) return null;
    // Cited by the id the deck already uses, so the citation stays the same.
    const documentId = ref.kind === 'recording' ? material.transcript.transcriptId : ref.id;
    material.transcript = { ...material.transcript, transcriptId: documentId };
    return { documentId, title: citedTitle ?? material.transcript.title, material };
  };

  if (ref.kind !== 'document') {
    const transcript = await asTranscript();
    if (transcript) return transcript;
    return { documentId: ref.id, title: citedTitle ?? ref.id, material: {} };
  }
  const material = await readFixtureMaterial({ documentId: ref.id }, opts);
  if (hasText(material)) {
    return { documentId: ref.id, title: citedTitle ?? material.document!.title, material };
  }
  const transcript = await asTranscript().catch(() => null);
  if (transcript) return transcript;
  return { documentId: ref.id, title: citedTitle ?? material.document?.title ?? ref.id, material };
}

/** The material block for every source, with the ones that could not be read said out loud. */
function sourcesMaterial(read: ReadSource[]): string {
  return read.map((source) => {
    if (hasText(source.material)) return materialBlock(source.material);
    const why = source.material.document?.unreadable;
    return [`### ${source.title}`, `documentId: ${source.documentId}`, why ? `(this source could not be read: ${why})` : '(this source has no readable text)'].join('\n');
  }).join('\n\n');
}

const SLIDE_SCHEMA = '{"id":"…","layout":"title|section|bullets|two-column|chart|quote|image","title":"…","eyebrow":"…","bullets":["…"],'
  + '"columns":[["…"],["…"]],"chart":{"kind":"bar|line","labels":["…"],"series":[{"name":"…","values":[1,2]}]},'
  + '"quote":{"text":"…","by":"…"},"image":{"alt":"…"},"notes":"…","sources":[{"documentId":"…","title":"…"}]}';

function rules(): string[] {
  return [
    `- At most ${MAX_BULLETS_PER_SLIDE} bullets a slide, each under ${MAX_BULLET_CHARS} characters. Titles under ${MAX_TITLE_CHARS} characters. Never more than ${MAX_SLIDES} slides.`,
    '- A slide carries only the payload its layout is named after; bullets may appear on any slide. Omit fields you do not use.',
    '- EVERY slide cites at least one source, using the exact documentId listed below and nothing else.',
    '- Only state what the material says. Anything you add must come from the material; if the material does not support the instruction, change as little as possible and say so in "summary".',
    '- Never attach a person\'s name to a line the material does not tie to them. If the instruction asks for owners the material does not name, leave those lines unowned and say which in "summary".',
    '- When the instruction asks you to add something, add information the deck does not already state: an owner, a date, a number, a decision, a risk and its mitigation — taken from the material. Never add a rewording of a bullet or quote the deck already has. If the material holds nothing new for it, add nothing and say so in "summary".',
    '- Change only what the instruction asks for. Leave everything else exactly as it is.',
    '- "summary" is one short sentence saying what you changed, written for the person who asked.',
  ];
}

/** The JSON shape of `value` for each target, as the prompt states it. */
function targetValueShape(target: DeckReviseTarget): string {
  switch (target.field) {
    case 'title': return '"…" (a non-empty string)';
    case 'eyebrow':
    case 'notes':
    case 'subtitle': return '"…" (a string), or null to remove it';
    case 'bullets': return target.index === undefined ? '["…", "…"] (the slide\'s whole bullet list)' : '"…" (the one bullet), or null to remove that bullet';
    case 'columns': return target.index === undefined ? '[["…"], ["…"]] (both columns)' : '["…"] (that column\'s bullets)';
    case 'chart': return '{"kind":"bar|line","labels":["…"],"series":[{"name":"…","values":[1,2]}]}';
    case 'quote': return '{"text":"…","by":"…"}';
    case 'image': return '{"alt":"…"}';
    default: return 'null';
  }
}

/** The part a target names, as the deck has it now. */
function currentTargetValue(spec: DeckSpec, slide: DeckSlide | undefined, target: DeckReviseTarget): unknown {
  if (target.field === 'subtitle') return spec.subtitle ?? null;
  if (!slide) return null;
  if (target.field === 'bullets' && target.index !== undefined) return slide.bullets?.[target.index] ?? null;
  if (target.field === 'columns' && target.index !== undefined) return slide.columns?.[target.index] ?? null;
  return (slide as unknown as Record<string, unknown>)[target.field] ?? null;
}

export function revisePrompt(input: DeckReviseInput, sources: ReadSource[]): string {
  const sourceList = sources.map((source) => `- ${source.title} (documentId: ${source.documentId})`).join('\n');
  const slide = input.slideId ? input.spec.slides.find((candidate) => candidate.id === input.slideId) : undefined;
  const target = input.target;
  const head = target
    ? [
      `You revise ONE PART of a presentation — ${target.field === 'subtitle' ? 'the deck\'s subtitle' : `the ${describeReviseTarget(target)} of slide "${slide?.id ?? ''}"`} — following the instruction. Answer with ONLY a JSON object, no prose, no code fence:`,
      '',
      `{"summary":"…","value":${targetValueShape(target)},"sources":[{"documentId":"…","title":"…"}]}`,
      '',
      'Rules:',
      '- "value" replaces that part and nothing else; every other part of the deck is kept exactly as it is.',
      '- "sources" is optional: list a source only when the new value uses material the slide does not already cite.',
      `- Bullets under ${MAX_BULLET_CHARS} characters, titles under ${MAX_TITLE_CHARS}, notes under ${MAX_NOTES_CHARS}.`,
      ...rules().slice(3),
    ]
    : slide
    ? [
      'You revise ONE slide of a presentation, following the instruction. Answer with ONLY a JSON object, no prose, no code fence:',
      '',
      `{"summary":"…","slide":${SLIDE_SCHEMA}}`,
      '',
      'Rules:',
      `- The slide keeps "id": "${slide.id}". Do not return any other slide.`,
      ...rules(),
    ]
    : [
      'You revise a presentation, following the instruction. Answer with ONLY a JSON object, no prose, no code fence:',
      '',
      `{"summary":"…","spec":{"title":"…","subtitle":"…","theme":"product|engineering|plain","slides":[${SLIDE_SCHEMA}]}}`,
      '',
      'Rules:',
      '- Return the WHOLE deck. A slide you keep keeps its "id" exactly; a new slide gets a new short kebab-case id not used by any other slide.',
      ...rules(),
    ];
  return [
    ...head,
    '',
    'Sources you may cite:',
    sourceList,
    '',
    'Material:',
    sourcesMaterial(sources),
    '',
    'The deck as it is now:',
    JSON.stringify(input.spec),
    ...(slide ? ['', target ? 'The slide:' : 'The slide to change:', JSON.stringify(slide)] : []),
    ...(target ? ['', `The part to change (${describeReviseTarget(target)}) as it is now:`, JSON.stringify(currentTargetValue(input.spec, slide, target))] : []),
    '',
    `Instruction: ${input.instruction}`,
  ].join('\n');
}

const STOPWORDS = new Set(['the', 'and', 'for', 'are', 'but', 'not', 'you', 'all', 'any', 'can', 'had', 'has', 'have', 'her', 'his', 'its', 'our', 'out', 'was', 'were', 'will', 'with', 'this', 'that', 'then', 'than', 'them', 'they', 'from', 'into', 'must', 'need', 'needs', 'should', 'would', 'could', 'been', 'being', 'what', 'when', 'which', 'who', 'about', 'there', 'their']);

/** The content words of a line, with a leading "Speaker:" label dropped. */
function contentWords(line: string): Set<string> {
  const body = line.replace(/^\s*[A-Z][\w .'-]{0,40}:\s+/, '');
  return new Set(body.toLowerCase().match(/[a-z0-9]+/g)?.filter((word) => word.length >= 3 && !STOPWORDS.has(word)) ?? []);
}

/**
 * Bullets the edit added that say again what the deck already said — the
 * words of the shorter line nearly all inside the longer one. The prompt asks
 * for new information; this is how a person finds out when it was not given.
 */
export function restatedBullets(before: DeckSpec, after: DeckSpec): string[] {
  const lines = (spec: DeckSpec) => spec.slides.flatMap((slide) => [
    ...(slide.bullets ?? []),
    ...(slide.columns ?? []).flat(),
    ...(slide.quote ? [slide.quote.text] : []),
  ]);
  const existing = lines(before);
  const had = new Set(existing);
  // Only lines the edit KEPT count: "tighten the bullets" replaces a line with
  // a shorter one, which is a rewrite, not a repeat.
  const kept = new Set(lines(after));
  const olds = existing.filter((line) => kept.has(line)).map(contentWords).filter((words) => words.size >= 2);
  const added = after.slides.flatMap((slide) => slide.bullets ?? []).filter((bullet) => !had.has(bullet));
  return added.filter((bullet) => {
    const words = contentWords(bullet);
    if (words.size < 2) return false;
    return olds.some((old) => {
      let shared = 0;
      for (const word of words) if (old.has(word)) shared += 1;
      return shared / Math.min(words.size, old.size) >= 0.8;
    });
  });
}

/** Slides whose content differs, plus slides that are new. */
export function changedSlides(before: DeckSpec, after: DeckSpec): string[] {
  // A citation's title is a label, not content: renaming a wire-titled
  // citation must not mark every slide as edited.
  const key = (slide: DeckSlide) => JSON.stringify({
    ...slide,
    sources: slide.sources.map((source) => `${source.documentId}#${source.anchor ?? ''}`),
  });
  const old = new Map(before.slides.map((slide) => [slide.id, key(slide)]));
  return after.slides.filter((slide) => old.get(slide.id) !== key(slide)).map((slide) => slide.id);
}

/**
 * The spec the editor posts may be unfinished — a blank deck's first slide
 * cites nothing yet — and that is exactly when an instruction ("build this from
 * the release plan") is most useful. So the input is held to every rule except
 * citations; the answer is held to all of them.
 */
export function validateReviseInputSpec(spec: unknown): { ok: true; spec: DeckSpec } | { ok: false; errors: string[] } {
  const strict = validateDeckSpec(spec);
  if (strict.ok) return strict;
  const blocking = strict.errors.filter((error) => !/\.sources(\[|:)/.test(error));
  if (blocking.length) return { ok: false, errors: blocking };
  // Only citations are missing: give each uncited slide a placeholder source
  // so the rest can be validated and normalized, then take it back off.
  const raw = spec as { slides: Array<Record<string, unknown>> };
  const patched = { ...raw, slides: raw.slides.map((slide) => (Array.isArray(slide.sources) && slide.sources.length ? slide : { ...slide, sources: [{ documentId: '__uncited__', title: '__uncited__' }] })) };
  const loose = validateDeckSpec(patched);
  if (!loose.ok) return { ok: false, errors: loose.errors };
  return {
    ok: true,
    spec: {
      ...loose.spec,
      slides: loose.spec.slides.map((slide) => ({ ...slide, sources: slide.sources.filter((source) => source.documentId !== '__uncited__') })),
    },
  };
}

const isStringList = (value: unknown): value is string[] => Array.isArray(value) && value.every((item) => typeof item === 'string');

/**
 * The deck with one part replaced by the model's `value`. Everything else is
 * copied from the input — the model never gets to touch what it was not
 * pointed at, whatever it answers. Shape errors are refused here; content
 * rules (lengths, a layout's payload, citations) by `validateDeckSpec` after.
 */
export function applyTargetValue(
  spec: DeckSpec, slideId: string | undefined, target: DeckReviseTarget, value: unknown, addedSources: DeckSource[] = [],
): DeckSpec {
  const wrong = (what: string) => new DeckReviseError('The agent answered with the wrong kind of value, so nothing was changed.', 502, `"value" for ${describeReviseTarget(target)} must be ${what}`);
  if (target.field === 'subtitle') {
    if (value !== null && typeof value !== 'string') throw wrong('a string or null');
    const { subtitle: _old, ...rest } = spec;
    return typeof value === 'string' && value.trim() ? { ...rest, subtitle: value } : rest;
  }
  const slides = spec.slides.map((slide): DeckSlide => {
    if (slide.id !== slideId) return slide;
    const next: Record<string, unknown> = { ...slide };
    switch (target.field) {
      case 'title':
        if (typeof value !== 'string') throw wrong('a string');
        next.title = value;
        break;
      case 'eyebrow':
      case 'notes':
        if (value !== null && typeof value !== 'string') throw wrong('a string or null');
        if (typeof value === 'string' && value.trim()) next[target.field] = value; else delete next[target.field];
        break;
      case 'bullets':
        if (target.index === undefined) {
          if (!isStringList(value)) throw wrong('an array of strings');
          if (value.length) next.bullets = value; else delete next.bullets;
        } else {
          if (value !== null && typeof value !== 'string') throw wrong('a string or null');
          const bullets = [...(slide.bullets ?? [])];
          if (typeof value === 'string' && value.trim()) bullets[target.index] = value; else bullets.splice(target.index, 1);
          if (bullets.length) next.bullets = bullets; else delete next.bullets;
        }
        break;
      case 'columns':
        if (target.index === undefined) {
          if (!Array.isArray(value) || value.length !== 2 || !value.every(isStringList)) throw wrong('two arrays of strings');
          next.columns = value;
        } else {
          if (!isStringList(value)) throw wrong('an array of strings');
          const columns: [string[], string[]] = [[...(slide.columns?.[0] ?? [])], [...(slide.columns?.[1] ?? [])]];
          columns[target.index as 0 | 1] = value;
          next.columns = columns;
        }
        break;
      case 'chart':
      case 'quote':
      case 'image':
        if (!value || typeof value !== 'object' || Array.isArray(value)) throw wrong('an object');
        next[target.field] = value;
        break;
      default:
        throw wrong('a known field');
    }
    if (addedSources.length) {
      const have = new Set(slide.sources.map((source) => source.documentId));
      next.sources = [...slide.sources, ...addedSources.filter((source) => !have.has(source.documentId))].slice(0, MAX_SOURCES_PER_SLIDE);
    }
    return next as unknown as DeckSlide;
  });
  return { ...spec, slides };
}

export async function reviseDeck(input: DeckReviseInput, opts: DeckReviseOpts = {}): Promise<DeckReviseResult> {
  const cited = deckSources(input.spec);
  // A citation still titled with a call's wire id (decks drafted before
  // f100e61) is renamed from the source as read, like a new draft would be.
  const titleOf = new Map(cited
    .filter((source) => !WIRE_LOBBY.test(source.title.trim()))
    .map((source) => [source.documentId, source.title]));
  const refs: DeckReviseSourceRef[] = [];
  const seen = new Set<string>();
  // The deck's own citations first: an edit is grounded in what the deck
  // already stands on, plus whatever the editor added to hand.
  for (const ref of [
    ...cited.map((source) => ({ kind: 'document' as const, id: source.documentId })),
    ...(input.sources ?? []),
  ]) {
    const known = input.sources?.find((candidate) => candidate.id === ref.id);
    const resolved = known ?? ref;
    if (seen.has(resolved.id)) continue;
    seen.add(resolved.id);
    refs.push(resolved);
    if (refs.length >= MAX_REVISE_SOURCES) break;
  }
  if (!refs.length) {
    throw new DeckReviseError('Add a source first: the agent only writes what a cited document or transcript says.', 400, 'the deck cites nothing and no source was named');
  }

  const phase = (next: DeckRevisePhase) => { try { opts.onPhase?.(next); } catch { /* a progress line is not the edit */ } };
  phase('reading-sources');
  const read = await Promise.all(refs.map((ref) => readSource(ref, titleOf.get(ref.id), opts)));
  const warnings = read
    .filter((source) => !hasText(source.material))
    .map((source) => `${source.title} could not be read, so nothing from it was used.`);

  let llm: LLMClient | null = null;
  if (opts.llm !== undefined) llm = opts.llm;
  else {
    try { llm = createLLMClient(); } catch (err) {
      log.warn({ err: err instanceof Error ? err.message : String(err) }, 'deck revise: no LLM client');
    }
  }
  if (!llm) throw new DeckReviseError('The agent is unavailable, so nothing was changed.', 503, 'no model is configured on this deployment');

  let answer: string;
  phase('asking-model');
  try {
    answer = await askModel(llm, revisePrompt(input, read), opts.timeoutMs ?? REVISE_TIMEOUT_MS);
  } catch (err) {
    const reason = err instanceof Error ? err.message : String(err);
    throw new DeckReviseError('The agent could not answer, so nothing was changed.', 503, reason);
  }

  phase('checking');
  const parsed = parseDeckSpecJson(answer) as { summary?: unknown; slide?: unknown; spec?: unknown; value?: unknown; sources?: unknown } | undefined;
  if (!parsed || typeof parsed !== 'object') {
    throw new DeckReviseError('The agent answered with something that is not an edit, so nothing was changed.', 502, 'the model answered with no JSON object');
  }

  let candidate: unknown;
  if (input.target) {
    if (!('value' in parsed)) {
      throw new DeckReviseError('The agent did not return the new text, so nothing was changed.', 502, 'the answer had no "value"');
    }
    // A citation the model adds must be one of the sources it was handed and
    // well-formed; anything else is refused below like any invented citation.
    const added: DeckSource[] = Array.isArray(parsed.sources)
      ? parsed.sources.flatMap((raw): DeckSource[] => {
        const entry = (raw ?? {}) as { documentId?: unknown; title?: unknown };
        return typeof entry.documentId === 'string' && entry.documentId.trim()
          ? [{ documentId: entry.documentId.trim(), title: typeof entry.title === 'string' && entry.title.trim() ? entry.title.trim() : entry.documentId.trim() }]
          : [];
      })
      : [];
    candidate = applyTargetValue(input.spec, input.slideId, input.target, parsed.value, added);
  } else if (input.slideId) {
    const slide = parsed.slide as Record<string, unknown> | undefined;
    if (!slide || typeof slide !== 'object') {
      throw new DeckReviseError('The agent did not return the slide, so nothing was changed.', 502, 'the answer had no "slide"');
    }
    // The id is the one thing a slide-scoped edit may not change: the rail,
    // the preview handles and the stored revision all key on it.
    candidate = { ...input.spec, slides: input.spec.slides.map((existing) => (existing.id === input.slideId ? { ...slide, id: input.slideId } : existing)) };
  } else {
    candidate = parsed.spec;
    if (!candidate || typeof candidate !== 'object') {
      throw new DeckReviseError('The agent did not return the deck, so nothing was changed.', 502, 'the answer had no "spec"');
    }
  }

  const validation = validateDeckSpec(candidate);
  if (!validation.ok) {
    throw new DeckReviseError('The agent\'s edit broke the deck\'s rules, so nothing was changed.', 502, validation.errors.slice(0, 4).join('; '), validation.errors);
  }
  const citable = new Set(read.map((source) => source.documentId));
  const invented = validation.spec.slides
    .flatMap((slide) => slide.sources.map((source) => source.documentId))
    .find((documentId) => !citable.has(documentId));
  if (invented) {
    throw new DeckReviseError('The agent cited something it was not given, so nothing was changed.', 502, `the model cited '${invented}', which is not one of the deck's sources`);
  }

  const spec: DeckSpec = {
    ...validation.spec,
    // A cited source keeps the title the deck already shows for it.
    slides: validation.spec.slides.map((slide): DeckSlide => ({
      ...slide,
      sources: slide.sources.map((source): DeckSource => {
        const title = titleOf.get(source.documentId) ?? read.find((entry) => entry.documentId === source.documentId)?.title;
        return title ? { ...source, title } : source;
      }),
    })),
  };
  const changedSlideIds = changedSlides(input.spec, spec);
  const kept = new Set(spec.slides.map((slide) => slide.id));
  const removed = input.spec.slides.filter((slide) => !kept.has(slide.id));
  if (removed.length) warnings.push(`Removed ${removed.length === 1 ? 'slide' : 'slides'}: ${removed.map((slide) => slide.title).join(', ')}.`);
  const titleChanged = spec.title !== input.spec.title || spec.subtitle !== input.spec.subtitle || spec.theme !== input.spec.theme;
  if (!changedSlideIds.length && !removed.length && !titleChanged) warnings.push('The agent returned the deck unchanged.');
  const restated = restatedBullets(input.spec, spec);
  if (restated.length) warnings.push(`${restated.map((bullet) => `"${bullet}"`).join(', ')} repeats what the deck already says.`);

  const summary = typeof parsed.summary === 'string' && parsed.summary.trim()
    ? parsed.summary.replace(/\s+/g, ' ').trim().slice(0, MAX_SUMMARY_CHARS)
    : changedSlideIds.length ? `Changed ${changedSlideIds.length} slide${changedSlideIds.length === 1 ? '' : 's'}.` : 'No slides changed.';

  return { spec, summary, changedSlideIds, warnings, source: 'model' };
}
