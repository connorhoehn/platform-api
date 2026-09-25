// A representative deck built from REAL local material.
//
// "Fixture" here does not mean invented: the material is a project document's
// own text and a recording's own transcript segments, read from the stores the
// rest of the platform reads. A light model turns that material into a DeckSpec
// whose every slide cites where it came from; when the model is unavailable the
// same material is folded into a deterministic deck instead, titled
// `[deck-fixture] …` and returned with `source: 'fixture'` and the reason — so
// nobody can mistake the offline path for the model's work.
//
// Nothing is persisted here. This produces a spec; the preview route renders it
// and the revisions route is what writes one down.

import { GetCommand } from '@aws-sdk/lib-dynamodb';
import { docClient } from '../lib/aws-clients';
import log from '../lib/logger';
import { callRecordingRepo } from '../repositories';
import { createLLMClient } from '../pipeline/createLLMClient';
import type { LLMClient } from '../pipeline/LLMClient';
import {
  MAX_BULLETS_PER_SLIDE, MAX_BULLET_CHARS, MAX_SLIDES,
  parseDeckSpecJson, validateDeckSpec, type DeckSlide, type DeckSpec, type DeckSource,
} from './DeckSpec';
import { readDocumentText } from './DeckSourcesStepHandler';
import { DOCUMENTS_TABLE } from './presentationDocuments';
import { getRedisClient } from '../lib/redis-client';
import { ModelSlotTimeoutError, withModelSlot, type ModelSlotRunner } from './modelSlots';
import { guardModelCall } from '../lib/modelHealth';
import { contentKey, createSingleFlight, type SingleFlightRole, type SingleFlightStore } from './fixtureSingleFlight';
import { slideCountLine, statusDrift, statusDriftLine } from './draftRequest';

export const FIXTURE_TITLE_PREFIX = '[deck-fixture]';
export const FIXTURE_MODEL = process.env.DOCUMENT_AGENT_MODEL || 'haiku';
export const FIXTURE_TIMEOUT_MS = 30_000;
const MAX_TRANSCRIPT_SEGMENTS = 120;
const MAX_DOCUMENT_CHARS = 6000;

export interface DeckFixtureFrom {
  documentId?: string;
  /**
   * Several documents drafted from together (a run draft's `sources`, the All
   * documents composer's chips). Read in order after `documentId`, deduped.
   */
  documentIds?: string[];
  recordingId?: string;
  personId?: string;
}

/** What was actually read, so the caller can see what the deck stands on. */
export interface FixtureDocument { documentId: string; title: string; text: string; unreadable?: string }

export interface FixtureMaterial {
  /** The first (or only) document — kept for the single-document callers. */
  document?: FixtureDocument;
  /** Every document read, in order, when more than one was named (`document` is `documents[0]`). */
  documents?: FixtureDocument[];
  transcript?: { transcriptId: string; title: string; segments: Array<{ speaker: string; text: string; at?: string }> };
}

export interface DeckFixtureResult {
  spec: DeckSpec;
  source: 'model' | 'fixture';
  /** Why the model's answer was not used. Present only when `source` is 'fixture'. */
  reason?: string;
  /** What the deck left out, said rather than refused. */
  warnings: string[];
  material: FixtureMaterial;
  /** Which of the cited ids are transcripts — known from what was read, not from the id. */
  transcriptIds: string[];
  /** Whether this caller asked the model or shared a concurrent identical draft (NFR #162). */
  flight?: SingleFlightRole;
  /** Set when the drafter refused the asked slide count rather than pad: the caller must not save `spec`. */
  refused?: string;
  /** With an asked slide count: what was asked, drafted first and finally. */
  slideCheck?: SlideCheck;
  /** "Measured: slides 4 → 6 (asked 6)". */
  measured?: string;
}

/** The part of a draft that is the model's answer — the rest is derived from the material. */
type ModelDraft = {
  spec: DeckSpec; source: 'model' | 'fixture'; reason?: string;
  /** The model said the sources cannot fill the asked slide count; nothing may be saved. */
  refused?: string;
  slideCheck?: SlideCheck;
  /** Status words still not the material's after the corrective ask. */
  driftWarnings?: string[];
};

/** The asked slide count, the first draft's and the final one's. */
export interface SlideCheck { asked: number; first: number; final: number }

/** `{"refused": "...", "supports": 4}` — the drafter's honest "these sources cannot fill N slides". */
function refusalOf(parsed: unknown): { refused: string; supports?: number } | undefined {
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return undefined;
  const r = parsed as { refused?: unknown; supports?: unknown; slides?: unknown };
  if (typeof r.refused !== 'string' || !r.refused.trim() || Array.isArray(r.slides)) return undefined;
  const supports = typeof r.supports === 'number' && Number.isInteger(r.supports) && r.supports >= 0 ? r.supports : undefined;
  return { refused: r.refused.trim().slice(0, 300), ...(supports !== undefined ? { supports } : {}) };
}

/**
 * Concurrent identical drafts share one model call, on this replica and
 * across replicas (main Redis: the lock is coordination state, never evicted).
 */
const fixtureFlight = createSingleFlight<ModelDraft>({
  store: async () => (await getRedisClient()) as unknown as SingleFlightStore | null,
  prefix: 'deck:fixture:flight',
  lockTtlMs: FIXTURE_TIMEOUT_MS + 10_000,
});

/**
 * Material that was handed over and never cited.
 *
 * Not a rejection: a deck that leans on the transcript and never opens the
 * release plan is still a deck, and the person who named both is the one who
 * should decide. Silence is the problem — the deck would look fully sourced.
 */
export function uncitedMaterialWarnings(spec: DeckSpec, material: FixtureMaterial): string[] {
  const cited = new Set(spec.slides.flatMap((slide) => slide.sources.map((source) => source.documentId)));
  return citableSources(material)
    .filter((source) => !cited.has(source.documentId))
    .map((source) => `${source.title} was provided but not cited.`);
}

/** Every document in the material, whichever field carries it. */
export function documentsOf(material: FixtureMaterial): FixtureDocument[] {
  if (material.documents?.length) return material.documents;
  return material.document ? [material.document] : [];
}

/** The transcript among the material, by the id the reader gave it. */
export function transcriptIdsOf(material: FixtureMaterial): string[] {
  return material.transcript ? [material.transcript.transcriptId] : [];
}

export class NoFixtureMaterialError extends Error {}

const OUTLINE_LINE = /^#\d+ ([A-Za-z]+)(?:\(h(\d)\))?(?:\([^)]*\))?: ?(.*)$/;

/**
 * A page's outline (`title: …` then `#<n> <kind>(h<level>): <text>` lines, the
 * document agent's EDIT format) as plain reading text: headings as `#`, list
 * items as `- `, one block per line, no block numbers. Loop 33 ppt: collapsed
 * onto one line with its markers, the model titled a deck "Release plan #1".
 * Text that is not an outline is only whitespace-tidied, as before.
 */
export function readableOutline(value: unknown, limit: number): string | undefined {
  if (typeof value !== 'string') return undefined;
  const lines = value.split('\n');
  if (!lines.some((line) => OUTLINE_LINE.test(line))) return clean(value, limit);
  const out: string[] = [];
  let list = false;
  for (const raw of lines) {
    if (/^title: /.test(raw)) continue; // the document's title heads the material block already
    const match = OUTLINE_LINE.exec(raw);
    if (match) {
      const [, kind, level, text] = match;
      list = kind === 'bulletList' || kind === 'orderedList';
      const body = (text ?? '').replace(/\s+/g, ' ').trim();
      if (!body || body === '(empty)') continue;
      out.push(kind === 'heading' ? `${'#'.repeat(Math.min(Number(level) || 1, 6))} ${body}` : list ? `- ${body}` : body);
    } else if (raw.trim()) {
      const body = raw.replace(/\s+/g, ' ').trim();
      out.push(list ? `- ${body}` : body); // a list's next item, or a table's next row
    }
  }
  const text = out.join('\n').trim();
  return text ? text.slice(0, limit) : undefined;
}

function clean(value: unknown, limit: number): string | undefined {
  if (typeof value !== 'string') return undefined;
  const trimmed = value.replace(/\s+/g, ' ').trim();
  if (!trimmed) return undefined;
  return trimmed.slice(0, limit);
}

/** Lobby names that are wire ids (`dm:dev-connor:dev-frank`, `room:design`), never titles. */
export const WIRE_LOBBY = /^(dm|dmg|room|doc|call|lobby|chat):/i;

/**
 * What a recording is called on a slide and in a citation. A `lobbyName` is
 * the call's wire id, not something a person wrote — it reached slide titles,
 * the deck subtitle and the Sources chip verbatim ("dm:dev-connor:dev-frank").
 * The participants' display names are on the row; this uses them, the same
 * rule the frontend's recording picker reads ("Call with Connor Hoehn & Frank
 * Davis"). A lobby name that is not wire-shaped is kept as written.
 */
export function recordingTitle(recording: {
  lobbyName?: unknown;
  participants?: unknown;
}): string {
  const lobby = clean(recording.lobbyName, 160);
  if (lobby && !WIRE_LOBBY.test(lobby)) return lobby;
  const names = (Array.isArray(recording.participants) ? recording.participants : [])
    .map((entry) => {
      const person = entry as { displayName?: unknown; userId?: unknown } | null;
      return clean(person?.displayName, 60) ?? clean(person?.userId, 60);
    })
    .filter((name): name is string => !!name);
  if (names.length > 3) return `Call with ${names.slice(0, 2).join(', ')} and ${names.length - 2} others`;
  if (names.length) return `Call with ${names.length === 1 ? names[0] : `${names.slice(0, -1).join(', ')} & ${names[names.length - 1]}`}`;
  if (lobby?.toLowerCase().startsWith('room:') && lobby.length > 5) return `Meeting in #${lobby.slice(5)}`;
  return 'Meeting transcript';
}

/** The document's own row: its title, and its outline when it has one. */
async function documentRow(documentId: string): Promise<{ title?: string; outline?: Array<{ title?: string }> } | null> {
  try {
    const result = await docClient.send(new GetCommand({ TableName: DOCUMENTS_TABLE, Key: { documentId } }));
    return (result.Item as { title?: string; outline?: Array<{ title?: string }> } | undefined) ?? null;
  } catch (err) {
    log.warn({ documentId, err: err instanceof Error ? err.message : String(err) }, 'deck fixture: document row unavailable');
    return null;
  }
}

export interface ReadMaterialDeps {
  readDocument?: typeof readDocumentText;
  findRecording?: (recordingId: string) => Promise<unknown>;
  readDocumentRow?: typeof documentRow;
}

/**
 * Reads whatever the caller named. A source that cannot be read is reported,
 * not skipped — the deck must never look better sourced than it is.
 */
export async function readFixtureMaterial(from: DeckFixtureFrom, deps: ReadMaterialDeps = {}): Promise<FixtureMaterial> {
  const readDocument = deps.readDocument ?? readDocumentText;
  const readRow = deps.readDocumentRow ?? documentRow;
  const findRecording = deps.findRecording ?? ((recordingId: string) => callRecordingRepo.findByRecordingId(recordingId));
  const material: FixtureMaterial = {};

  const readOne = async (documentId: string): Promise<FixtureDocument> => {
    const [row, body] = await Promise.all([readRow(documentId), readDocument(documentId)]);
    const outlineText = (row?.outline ?? [])
      .map((entry) => clean(entry?.title, 160))
      .filter((value): value is string => !!value)
      .join('\n');
    const text = readableOutline(body.text, MAX_DOCUMENT_CHARS) ?? outlineText;
    return {
      documentId,
      title: clean(row?.title, 160) ?? documentId,
      text: text ?? '',
      ...(body.error ? { unreadable: body.error } : {}),
    };
  };
  const ids = [...new Set([from.documentId, ...(from.documentIds ?? [])].filter((id): id is string => typeof id === 'string' && !!id))];
  if (ids.length === 1 && !from.documentIds?.length) {
    material.document = await readOne(ids[0]!);
  } else if (ids.length) {
    // One at a time: each read joins the document as a collaborator.
    const documents: FixtureDocument[] = [];
    for (const id of ids) documents.push(await readOne(id));
    material.documents = documents;
    material.document = documents[0];
  }

  if (from.recordingId) {
    const recording = await findRecording(from.recordingId) as {
      recordingId?: string; transcriptId?: string; lobbyName?: string; transcriptJson?: string;
      participants?: Array<{ userId?: string; displayName?: string }>;
    } | null;
    if (recording?.transcriptJson) {
      let parsed: unknown;
      try { parsed = JSON.parse(recording.transcriptJson); } catch { parsed = undefined; }
      const raw = Array.isArray(parsed)
        ? parsed
        : Array.isArray((parsed as { segments?: unknown[] } | undefined)?.segments)
          ? (parsed as { segments: unknown[] }).segments
          : [];
      const segments = raw
        .map((entry) => {
          const item = entry as { speaker?: unknown; text?: unknown; at?: unknown };
          const speaker = clean(item?.speaker, 60);
          const text = clean(item?.text, 600);
          if (!speaker || !text) return undefined;
          const at = clean(item?.at, 40);
          return { speaker, text, ...(at ? { at } : {}) };
        })
        .filter((value): value is { speaker: string; text: string; at?: string } => !!value)
        .slice(0, MAX_TRANSCRIPT_SEGMENTS);
      if (segments.length) {
        material.transcript = {
          transcriptId: clean(recording.transcriptId, 200) ?? `transcript:${from.recordingId}`,
          title: recordingTitle(recording),
          segments,
        };
      }
    }
  }

  return material;
}

/** The sources a deck built from this material may cite, and nothing else. */
export function citableSources(material: FixtureMaterial): DeckSource[] {
  const sources: DeckSource[] = documentsOf(material).map((document) => ({ documentId: document.documentId, title: document.title }));
  if (material.transcript) sources.push({ documentId: material.transcript.transcriptId, title: material.transcript.title });
  return sources;
}

/** The material as the model reads it. */
export function materialBlock(material: FixtureMaterial): string {
  const parts: string[] = [];
  for (const document of documentsOf(material)) {
    parts.push([
      `### ${document.title}`,
      `documentId: ${document.documentId}`,
      document.text || (document.unreadable
        ? `(this document could not be read: ${document.unreadable})`
        : '(this document is empty)'),
    ].join('\n'));
  }
  if (material.transcript) {
    parts.push([
      `### ${material.transcript.title}`,
      `documentId: ${material.transcript.transcriptId}`,
      ...material.transcript.segments.map((segment) => `${segment.speaker}: ${segment.text}`),
    ].join('\n'));
  }
  return parts.join('\n\n');
}

export function fixturePrompt(material: FixtureMaterial, title: string, requestedBy?: string, slideCount?: number): string {
  const countRule = slideCount
    ? `- EXACTLY ${slideCount} slides, the opening title slide included — the person asked for ${slideCount}. Do not write a Sources slide; one is added after yours and is not counted. Each slide must say something the others do not, drawn from the material. If the material cannot fill ${slideCount} distinct slides without repeating itself or inventing, do NOT pad: answer ONLY {"refused":"<what the material is missing, in one sentence>","supports":<how many distinct slides it can fill>}. At most ${MAX_BULLETS_PER_SLIDE} bullets a slide, each under ${MAX_BULLET_CHARS} characters.`
    : `- 6 to 10 slides when the material supports that many, never more than ${MAX_SLIDES}. A short source makes a short deck: never pad it with steps, procedures, dates, numbers or owners the material does not state. At most ${MAX_BULLETS_PER_SLIDE} bullets a slide, each under ${MAX_BULLET_CHARS} characters.`;
  return [
    'You draft a presentation from the material below. Answer with ONLY a JSON object, no prose, no code fence.',
    '',
    '{"title":"…","subtitle":"…","theme":"product|engineering|plain","slides":[{"id":"kebab-id",',
    '  "layout":"title|section|bullets|two-column|chart|quote|image","title":"…","eyebrow":"…","bullets":["…"],',
    '  "columns":[["…"],["…"]],"chart":{"kind":"bar|line","labels":["…"],"series":[{"name":"…","values":[1,2]}]},',
    '  "quote":{"text":"…","by":"…"},"image":{"alt":"…"},"notes":"…","sources":[{"documentId":"…","title":"…"}]}]}',
    '',
    'Rules:',
    countRule,
    '- A slide carries only the payload its layout is named after; bullets may appear on any slide.',
    '- EVERY slide cites at least one source, using the exact documentId given below.',
    '- EVERY source listed below must be cited by at least one slide. Material you were given and did not use is material you were asked for and ignored: if a document and a transcript are both listed, the deck draws on both.',
    '- Only state what the material says. A quote must be something a named person actually said in it. A chart\'s numbers must appear in it. Name a person as the owner of something only when the material ties them to it.',
    '- Keep the material\'s status and modality. Something the material says is tracked, needed, planned or under review is not "confirmed", "verified", "reviewed", "done", "assigned" or "defined" on a slide: "needs an owner" stays "needs an owner". The same goes for dates, owners and numbers: copy them, never round, extend or infer them.',
    '- Use the material\'s own status verbs, in titles, bullets AND speaker notes. If it says "shipped", write "shipped" — never swap in a synonym ("delivered", "launched", "released", "completed", "done", "landed"). A status word the material does not use is a claim it does not make.',
    '- Shape the deck to the material. A call transcript: slides for what was decided, the actions with the person who took each in their own words ("Frank takes the rollback", not "owner assigned"), and what is still open — titled that plainly (Decisions, Actions, Open items). A plan or project brief: the outcome it aims for, what is in and out of scope, owners and milestones as written, how success is measured, risks and open questions — whichever of these the material has.',
    '- No `section` divider slides unless the deck has more than 8 slides; a divider with nothing on it is padding.',
    '- An `eyebrow` is a short label (the part of the material the slide comes from), never a claim or a status.',
    '- Open with a `title` slide. Speaker notes in `notes`. `subtitle` belongs to the deck, never to a slide. Never write an empty string; leave a list shorter instead.',
    '',
    'Sources you may cite:',
    citableSources(material).map((source) => `- ${source.title} (documentId: ${source.documentId})`).join('\n'),
    '',
    'Material:',
    materialBlock(material),
    '',
    ...(requestedBy ? [`Requested by: ${requestedBy}`] : []),
    `Deck title: ${title}`,
  ].join('\n');
}

const sentences = (text: string, limit: number): string[] => text
  .split(/(?<=[.!?])\s+/)
  .map((sentence) => sentence.replace(/\s+/g, ' ').trim())
  .filter((sentence) => sentence.length > 8)
  .slice(0, limit)
  .map((sentence) => (sentence.length > MAX_BULLET_CHARS ? `${sentence.slice(0, MAX_BULLET_CHARS - 1)}…` : sentence));

/**
 * The offline deck: the same material, folded deterministically. It is titled
 * `[deck-fixture]` because that is what it is — a representative deck, not the
 * one a model would have written.
 */
export function fixtureSpec(material: FixtureMaterial, title: string): DeckSpec {
  const sources = citableSources(material);
  if (!sources.length) throw new NoFixtureMaterialError('no readable document or transcript was named');
  const transcriptSource = material.transcript ? [{ documentId: material.transcript.transcriptId, title: material.transcript.title }] : sources;
  const slides: DeckSlide[] = [{
    id: 'cover',
    layout: 'title',
    title: `${FIXTURE_TITLE_PREFIX} ${title}`.slice(0, 120),
    eyebrow: 'Built offline from local material',
    sources,
  }];

  documentsOf(material).forEach((document, index) => {
    const points = sentences(document.text, MAX_BULLETS_PER_SLIDE);
    slides.push({
      id: index === 0 ? 'document' : `document-${index + 1}`,
      layout: 'bullets',
      title: document.title.slice(0, 120),
      eyebrow: 'Project document',
      bullets: points.length ? points : ['(the document had no readable text)'],
      notes: 'Read straight from the document, unedited.',
      sources: [{ documentId: document.documentId, title: document.title }],
    });
  });

  if (material.transcript) {
    const segments = material.transcript.segments;
    slides.push({
      id: 'transcript',
      layout: 'bullets',
      title: material.transcript.title.slice(0, 120),
      eyebrow: 'Meeting transcript',
      bullets: segments.slice(0, MAX_BULLETS_PER_SLIDE).map((segment) => `${segment.speaker}: ${segment.text}`.slice(0, MAX_BULLET_CHARS)),
      notes: `${segments.length} segment${segments.length === 1 ? '' : 's'} were read.`,
      sources: transcriptSource,
    });
    const longest = [...segments].sort((left, right) => right.text.length - left.text.length)[0];
    if (longest) {
      slides.push({
        id: 'said',
        layout: 'quote',
        title: 'From the meeting',
        quote: { text: longest.text.slice(0, 400), by: longest.speaker },
        sources: transcriptSource,
      });
    }
    const speakers = [...new Set(segments.map((segment) => segment.speaker))];
    slides.push({
      id: 'who-spoke',
      layout: 'chart',
      title: 'Who spoke',
      chart: {
        kind: 'bar',
        labels: speakers.slice(0, 12),
        series: [{ name: 'Segments', values: speakers.slice(0, 12).map((speaker) => segments.filter((segment) => segment.speaker === speaker).length) }],
      },
      notes: 'Counted from the transcript segments.',
      sources: transcriptSource,
    });
  }

  slides.push({
    id: 'next',
    layout: 'section',
    title: 'What this is',
    bullets: ['A deck built offline from the named material, with no model in the loop.'],
    sources,
  });

  const spec: DeckSpec = {
    title: `${FIXTURE_TITLE_PREFIX} ${title}`.slice(0, 120),
    subtitle: sources.map((source) => source.title).join(' · ').slice(0, 200),
    theme: 'product',
    slides,
  };
  const validation = validateDeckSpec(spec);
  if (!validation.ok) throw new Error(`the offline fixture did not validate: ${validation.errors.join('; ')}`);
  return validation.spec;
}

/**
 * One model call. With `slot`, it runs holding a fleet-wide model slot
 * (modelSlots.ts) — waiting for the slot is not part of `timeoutMs`.
 */
export async function askModel(llm: LLMClient, prompt: string, timeoutMs: number, slot?: ModelSlotRunner | null): Promise<string> {
  if (slot) return slot(() => askModel(llm, prompt, timeoutMs));
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  let text = '';
  try {
    for await (const chunk of llm.stream(FIXTURE_MODEL, '', prompt, { temperature: 0, maxTokens: 4000, signal: controller.signal })) {
      if (chunk.done) text = chunk.response || text;
      else text += chunk.token;
    }
  } finally {
    clearTimeout(timer);
  }
  if (controller.signal.aborted) throw new Error(`the model did not answer within ${timeoutMs}ms`);
  return text;
}

/**
 * A real model call's slot, behind the model circuit breaker (NFR #225): an
 * open breaker fails at once with "Model unavailable: <reason>" — before
 * waiting for a slot — and the draft says so instead of waiting 30 s.
 */
export const guardedSlot: ModelSlotRunner = (run) =>
  guardModelCall(() => withModelSlot(run), (err) => err instanceof ModelSlotTimeoutError);

export interface BuildFixtureOpts extends ReadMaterialDeps {
  /** Material the caller already read (and checked) — the drafter does not read it again. */
  material?: FixtureMaterial;
  llm?: LLMClient | null;
  timeoutMs?: number;
  /** false: never share the model call with a concurrent identical draft. */
  singleFlight?: boolean;
  /** Content slides the request asked for (`requestedSlideCount`); the Sources slide is extra. */
  slideCount?: number;
  /** The requester's own words: a status word they used is not a substitution. */
  instruction?: string;
}

/**
 * The whole path: read the named material, ask the model for a deck, and fall
 * back to the offline deck — with the reason — when it cannot answer or answers
 * with something that does not validate.
 */
/**
 * The model's one recurring slip: a `subtitle` on the title slide. The deck
 * has a subtitle and a slide does not, and the strict validator (right for a
 * saved spec) rejected the whole draft over it, so the reader got the offline
 * deck (live 2026-09-23: "slides[0]: unknown field 'subtitle'"). The words are
 * the deck's subtitle when the model gave none; otherwise they repeat it and
 * are dropped. Nothing else is loosened — every other stray field still fails.
 */
export function liftSlideSubtitles(parsed: unknown): unknown {
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return parsed;
  const deck = parsed as Record<string, unknown>;
  if (!Array.isArray(deck.slides)) return parsed;
  // The other slip (live 2026-09-23, twice in a row): `"subtitle": ""` on the
  // deck itself. Spreading `deck` kept that empty string, the validator refused
  // it ("deck.subtitle: must be a non-empty string when present") and the
  // reader got the offline deck for a draft the model had actually written.
  // An empty or blank deck subtitle is dropped like a missing one.
  const { subtitle: given, ...deckRest } = deck;
  let subtitle = typeof given === 'string' && given.trim() ? given : undefined;
  // A blank bullet or column entry (live 2026-09-24, Loop 33: `columns[0][3]: ""`)
  // is a trailing slot the model left empty, not content — drop it rather than
  // throwing the whole draft away for the offline deck.
  const blank = (value: unknown): boolean => typeof value === 'string' && !value.trim();
  const slides = deck.slides.map((input) => {
    let slide = input;
    if (slide && typeof slide === 'object' && !Array.isArray(slide)) {
      const s = slide as Record<string, unknown>;
      const tidy: Record<string, unknown> = { ...s };
      if (Array.isArray(s.bullets)) tidy.bullets = s.bullets.filter((b) => !blank(b));
      if (Array.isArray(s.columns)) tidy.columns = s.columns.map((col) => (Array.isArray(col) ? col.filter((b) => !blank(b)) : col));
      // A blank eyebrow is a label left empty (live Loop 34: `slides[0].eyebrow`), dropped like a blank subtitle.
      if (blank(s.eyebrow)) delete tidy.eyebrow;
      slide = tidy;
    }
    if (!slide || typeof slide !== 'object' || !('subtitle' in slide)) return slide;
    const { subtitle: own, ...rest } = slide as Record<string, unknown>;
    if (!subtitle && typeof own === 'string' && own.trim()) subtitle = own.trim().slice(0, 200);
    return rest;
  });
  return { ...deckRest, ...(subtitle ? { subtitle } : {}), slides };
}

export async function buildDeckFixture(
  from: DeckFixtureFrom,
  title: string | undefined,
  opts: BuildFixtureOpts = {},
): Promise<DeckFixtureResult> {
  const material = opts.material ?? await readFixtureMaterial(from, opts);
  const sources = citableSources(material);
  if (!sources.length) {
    throw new NoFixtureMaterialError('name a documentId or a recordingId that can be read — a deck is only allowed to say what its inputs say');
  }
  const deckTitle = (title?.trim() || sources.map((source) => source.title).join(' · ')).slice(0, 100);

  let llm: LLMClient | null = null;
  if (opts.llm !== undefined) llm = opts.llm;
  else {
    try { llm = createLLMClient(); } catch (err) {
      log.warn({ err: err instanceof Error ? err.message : String(err) }, 'deck fixture: no LLM client');
    }
  }

  const prompt = fixturePrompt(material, deckTitle, from.personId, opts.slideCount);
  const materialText = materialBlock(material);
  const allowed = [deckTitle, ...(opts.instruction ? [opts.instruction] : [])];
  const draft = async (): Promise<ModelDraft> => {
    if (!llm) return { spec: fixtureSpec(material, deckTitle), source: 'fixture', reason: 'no model is configured on this deployment' };
    try {
      // Injected clients (tests) take no slot, as they take no single-flight.
      const slot = opts.llm === undefined ? guardedSlot : null;
      const timeoutMs = opts.timeoutMs ?? FIXTURE_TIMEOUT_MS;
      const ask = async (text: string): Promise<{ spec: DeckSpec } | { refused: string; supports?: number }> => {
        const parsed = parseDeckSpecJson(await askModel(llm!, text, timeoutMs, slot));
        if (parsed === undefined) throw new Error('the model answered with no JSON object');
        const refusal = refusalOf(parsed);
        if (refusal) return refusal;
        const validation = validateDeckSpec(liftSlideSubtitles(parsed));
        if (!validation.ok) throw new Error(validation.errors.slice(0, 4).join('; '));
        const citable = new Set(sources.map((source) => source.documentId));
        const invented = validation.spec.slides
          .flatMap((slide) => slide.sources.map((source) => source.documentId))
          .find((documentId) => !citable.has(documentId));
        if (invented) throw new Error(`the model cited '${invented}', which is not one of the sources it was given`);
        return { spec: validation.spec };
      };
      const asked = opts.slideCount;
      const refused = (answer: { refused: string; supports?: number }, first?: number): ModelDraft => ({
        spec: fixtureSpec(material, deckTitle),
        source: 'model',
        refused: `asked for ${asked} slides; the sources support ${answer.supports ?? 'fewer'} distinct slides without inventing — ${answer.refused}`,
        ...(asked ? { slideCheck: { asked, first: first ?? answer.supports ?? 0, final: answer.supports ?? 0 } } : {}),
      });
      // One draft, measured; one corrective second ask when it missed the
      // count or swapped a status verb. Never a third: what is still off is said.
      const first = await ask(prompt);
      if ('refused' in first) return asked ? refused(first) : (() => { throw new Error(`the model refused: ${first.refused}`); })();
      const problems = (spec: DeckSpec) => ({
        count: asked && spec.slides.length !== asked ? spec.slides.length : undefined,
        drift: statusDrift(spec, materialText, allowed),
      });
      let spec = first.spec;
      let found = problems(spec);
      if (found.count !== undefined || found.drift.length) {
        const fixes = [
          ...(found.count !== undefined ? [`- It has ${found.count} slides; the request is EXACTLY ${asked} (title slide included, no Sources slide). Write ${asked} distinct slides from the material, or refuse as the rules say — never pad.`] : []),
          ...found.drift.map((drift) => `- ${statusDriftLine(drift)} Use the material's word.`),
        ];
        const second = await ask(`${prompt}\n\nYour first answer was:\n${JSON.stringify(first.spec)}\n\nIt has these problems:\n${fixes.join('\n')}\nAnswer again with the whole corrected JSON object.`);
        if ('refused' in second) return asked ? refused(second, first.spec.slides.length) : { spec, source: 'model', driftWarnings: found.drift.map(statusDriftLine) };
        spec = second.spec;
        found = problems(spec);
      }
      if (asked && found.count !== undefined) {
        return refused({ refused: `the draft came back with ${found.count} after being asked twice`, supports: Math.min(found.count, asked) }, first.spec.slides.length);
      }
      return {
        spec,
        source: 'model',
        ...(asked ? { slideCheck: { asked, first: first.spec.slides.length, final: spec.slides.length } } : {}),
        ...(found.drift.length ? { driftWarnings: found.drift.map(statusDriftLine) } : {}),
      };
    } catch (err) {
      const reason = err instanceof Error ? err.message : String(err);
      log.warn({ reason }, 'deck fixture: falling back to the offline deck');
      return { spec: fixtureSpec(material, deckTitle), source: 'fixture', reason };
    }
  };

  // Only a real model call is shared: an injected client (tests) or no model runs alone.
  let flight: SingleFlightRole = 'ran';
  const answer = llm && opts.llm === undefined && opts.singleFlight !== false
    ? await fixtureFlight(contentKey(FIXTURE_MODEL, prompt), draft, (role) => { flight = role; })
    : await draft();
  const slideCheck = answer.slideCheck ?? (opts.slideCount && !answer.refused
    ? { asked: opts.slideCount, first: answer.spec.slides.length, final: answer.spec.slides.length } : undefined);
  const countWarning = answer.source === 'fixture' && slideCheck && slideCheck.final !== slideCheck.asked
    ? [`The offline deck does not follow the asked slide count (${slideCheck.final} of ${slideCheck.asked}).`] : [];
  return {
    spec: answer.spec,
    source: answer.source,
    ...(answer.reason ? { reason: answer.reason } : {}),
    ...(answer.refused ? { refused: answer.refused } : {}),
    ...(slideCheck ? { slideCheck, measured: slideCountLine(slideCheck.asked, slideCheck.first, slideCheck.final) } : {}),
    warnings: [...uncitedMaterialWarnings(answer.spec, material), ...(answer.driftWarnings ?? []), ...countWarning],
    material,
    transcriptIds: transcriptIdsOf(material),
    flight,
  };
}
