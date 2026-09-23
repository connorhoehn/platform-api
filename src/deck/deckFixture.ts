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

export const FIXTURE_TITLE_PREFIX = '[deck-fixture]';
export const FIXTURE_MODEL = process.env.DOCUMENT_AGENT_MODEL || 'haiku';
export const FIXTURE_TIMEOUT_MS = 30_000;
const MAX_TRANSCRIPT_SEGMENTS = 120;
const MAX_DOCUMENT_CHARS = 6000;

export interface DeckFixtureFrom {
  documentId?: string;
  recordingId?: string;
  personId?: string;
}

/** What was actually read, so the caller can see what the deck stands on. */
export interface FixtureMaterial {
  document?: { documentId: string; title: string; text: string; unreadable?: string };
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
}

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

/** The transcript among the material, by the id the reader gave it. */
export function transcriptIdsOf(material: FixtureMaterial): string[] {
  return material.transcript ? [material.transcript.transcriptId] : [];
}

export class NoFixtureMaterialError extends Error {}

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

  if (from.documentId) {
    const [row, body] = await Promise.all([readRow(from.documentId), readDocument(from.documentId)]);
    const outlineText = (row?.outline ?? [])
      .map((entry) => clean(entry?.title, 160))
      .filter((value): value is string => !!value)
      .join('\n');
    const text = clean(body.text, MAX_DOCUMENT_CHARS) ?? outlineText;
    material.document = {
      documentId: from.documentId,
      title: clean(row?.title, 160) ?? from.documentId,
      text: text ?? '',
      ...(body.error ? { unreadable: body.error } : {}),
    };
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
  const sources: DeckSource[] = [];
  if (material.document) sources.push({ documentId: material.document.documentId, title: material.document.title });
  if (material.transcript) sources.push({ documentId: material.transcript.transcriptId, title: material.transcript.title });
  return sources;
}

/** The material as the model reads it. */
export function materialBlock(material: FixtureMaterial): string {
  const parts: string[] = [];
  if (material.document) {
    parts.push([
      `### ${material.document.title}`,
      `documentId: ${material.document.documentId}`,
      material.document.text || (material.document.unreadable
        ? `(this document could not be read: ${material.document.unreadable})`
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

export function fixturePrompt(material: FixtureMaterial, title: string, requestedBy?: string): string {
  return [
    'You draft a presentation from the material below. Answer with ONLY a JSON object, no prose, no code fence.',
    '',
    '{"title":"…","subtitle":"…","theme":"product|engineering|plain","slides":[{"id":"kebab-id",',
    '  "layout":"title|section|bullets|two-column|chart|quote|image","title":"…","eyebrow":"…","bullets":["…"],',
    '  "columns":[["…"],["…"]],"chart":{"kind":"bar|line","labels":["…"],"series":[{"name":"…","values":[1,2]}]},',
    '  "quote":{"text":"…","by":"…"},"image":{"alt":"…"},"notes":"…","sources":[{"documentId":"…","title":"…"}]}]}',
    '',
    'Rules:',
    `- 6 to 10 slides when the material supports that many, never more than ${MAX_SLIDES}. A short source makes a short deck: never pad it with steps, procedures, dates, numbers or owners the material does not state. At most ${MAX_BULLETS_PER_SLIDE} bullets a slide, each under ${MAX_BULLET_CHARS} characters.`,
    '- A slide carries only the payload its layout is named after; bullets may appear on any slide.',
    '- EVERY slide cites at least one source, using the exact documentId given below.',
    '- EVERY source listed below must be cited by at least one slide. Material you were given and did not use is material you were asked for and ignored: if a document and a transcript are both listed, the deck draws on both.',
    '- Only state what the material says. A quote must be something a named person actually said in it. A chart\'s numbers must appear in it. Name a person as the owner of something only when the material ties them to it.',
    '- Open with a `title` slide. Speaker notes in `notes`. `subtitle` belongs to the deck, never to a slide.',
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
  const documentSource = material.document ? [{ documentId: material.document.documentId, title: material.document.title }] : sources;
  const transcriptSource = material.transcript ? [{ documentId: material.transcript.transcriptId, title: material.transcript.title }] : sources;
  const slides: DeckSlide[] = [{
    id: 'cover',
    layout: 'title',
    title: `${FIXTURE_TITLE_PREFIX} ${title}`.slice(0, 120),
    eyebrow: 'Built offline from local material',
    sources,
  }];

  if (material.document) {
    const points = sentences(material.document.text, MAX_BULLETS_PER_SLIDE);
    slides.push({
      id: 'document',
      layout: 'bullets',
      title: material.document.title.slice(0, 120),
      eyebrow: 'Project document',
      bullets: points.length ? points : ['(the document had no readable text)'],
      notes: 'Read straight from the document, unedited.',
      sources: documentSource,
    });
  }

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

export async function askModel(llm: LLMClient, prompt: string, timeoutMs: number): Promise<string> {
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

export interface BuildFixtureOpts extends ReadMaterialDeps {
  llm?: LLMClient | null;
  timeoutMs?: number;
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
  let subtitle = typeof deck.subtitle === 'string' && deck.subtitle.trim() ? deck.subtitle : undefined;
  const slides = deck.slides.map((slide) => {
    if (!slide || typeof slide !== 'object' || !('subtitle' in slide)) return slide;
    const { subtitle: own, ...rest } = slide as Record<string, unknown>;
    if (!subtitle && typeof own === 'string' && own.trim()) subtitle = own.trim().slice(0, 200);
    return rest;
  });
  return { ...deck, ...(subtitle ? { subtitle } : {}), slides };
}

export async function buildDeckFixture(
  from: DeckFixtureFrom,
  title: string | undefined,
  opts: BuildFixtureOpts = {},
): Promise<DeckFixtureResult> {
  const material = await readFixtureMaterial(from, opts);
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

  if (llm) {
    try {
      const answer = await askModel(llm, fixturePrompt(material, deckTitle, from.personId), opts.timeoutMs ?? FIXTURE_TIMEOUT_MS);
      const parsed = parseDeckSpecJson(answer);
      if (parsed === undefined) throw new Error('the model answered with no JSON object');
      const validation = validateDeckSpec(liftSlideSubtitles(parsed));
      if (!validation.ok) throw new Error(validation.errors.slice(0, 4).join('; '));
      const citable = new Set(sources.map((source) => source.documentId));
      const invented = validation.spec.slides
        .flatMap((slide) => slide.sources.map((source) => source.documentId))
        .find((documentId) => !citable.has(documentId));
      if (invented) throw new Error(`the model cited '${invented}', which is not one of the sources it was given`);
      return {
        spec: validation.spec,
        source: 'model',
        warnings: uncitedMaterialWarnings(validation.spec, material),
        material,
        transcriptIds: transcriptIdsOf(material),
      };
    } catch (err) {
      const reason = err instanceof Error ? err.message : String(err);
      log.warn({ reason }, 'deck fixture: falling back to the offline deck');
      const spec = fixtureSpec(material, deckTitle);
      return { spec, source: 'fixture', reason, warnings: uncitedMaterialWarnings(spec, material), material, transcriptIds: transcriptIdsOf(material) };
    }
  }

  const spec = fixtureSpec(material, deckTitle);
  return {
    spec, source: 'fixture', reason: 'no model is configured on this deployment',
    warnings: uncitedMaterialWarnings(spec, material), material, transcriptIds: transcriptIdsOf(material),
  };
}
