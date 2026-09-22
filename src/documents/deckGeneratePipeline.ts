// "/deck <what>" in a chat becomes a PIPELINE RUN that builds a real
// presentation: the conversation is read → the `#`-referenced documents are read
// → a light model drafts a DeckSpec that cites them → the `deck` step renders the
// .pptx and one SVG per slide and files them as a revision of the presentation
// document the client created.
//
//   deck-generate:  trigger → read-history → read-sources → draft → render
//
// The model never sees OOXML, and it is never the thing that decides what is
// true: it answers with a spec whose every slide names the document it came
// from, and the validator in src/deck refuses the slide that names none.

import type { StepHandler } from 'distributed-core/applications/pipeline';
import { CHAT_READ_HISTORY_KIND } from './conversationSummarizePipeline';
import { DECK_READ_SOURCES_KIND } from '../deck/DeckSourcesStepHandler';
import { DECK_STEP_KIND } from '../deck/DeckStepHandler';
import { buildDeckStepHandlers } from '../deck/registerDeckHandlers';
import { MAX_BULLETS_PER_SLIDE, MAX_BULLET_CHARS, MAX_SLIDES } from '../deck/DeckSpec';

export const DECK_GENERATE_PIPELINE_ID = 'deck-generate';
/** How much of the conversation the deck may draw on. */
export const DECK_HISTORY_LIMIT = 200;

const DRAFT_RETRY = { maxAttempts: 3, backoffMs: 2000, backoffMultiplier: 2, retryUnknown: true } as const;

export const DECK_DRAFT_PROMPT = [
  'You draft a presentation. Answer with ONLY a JSON object, no prose, no code fence.',
  '',
  'Shape:',
  '{"title":"…","subtitle":"…","theme":"product|engineering|plain","slides":[ … ]}',
  'Each slide:',
  '  {"id":"kebab-id","layout":"title|section|bullets|two-column|chart|quote|image","title":"…","eyebrow":"…",',
  '   "bullets":["…"], "columns":[["…"],["…"]],',
  '   "chart":{"kind":"bar|line","labels":["…"],"series":[{"name":"…","values":[1,2]}]},',
  '   "quote":{"text":"…","by":"…"}, "image":{"alt":"…"}, "notes":"what to say out loud",',
  '   "sources":[{"documentId":"…","title":"…","anchor":"optional heading"}]}',
  '',
  'Rules:',
  `- At most ${MAX_SLIDES} slides; aim for 8–14. At most ${MAX_BULLETS_PER_SLIDE} bullets a slide, each under ${MAX_BULLET_CHARS} characters.`,
  '- A slide carries only the payload its layout is named after: a chart slide a chart, a quote slide a quote, a two-column slide columns, an image slide image.alt. Bullets are allowed on any slide.',
  '- EVERY slide must cite at least one source, by the exact documentId listed below. A slide you cannot source is a slide you must not write.',
  '- Only state what the sources below actually say. No invented numbers: a chart\'s values must appear in the sources.',
  '- Open with a `title` slide and use `section` slides to divide the deck. Speaker notes in `notes`.',
  '',
  'Sources you may cite:',
  '{{ context.sourceList }}',
  '',
  'Source material:',
  '{{ context.sourcesBlock }}',
  '',
  'Requested by: {{ context.requestedByName }}',
  'Build a deck about: {{ context.prompt }}',
].join('\n');

export function buildDeckGenerateDefinition(model = process.env.DOCUMENT_AGENT_MODEL || 'haiku'): Record<string, unknown> {
  const pos = (x: number) => ({ x, y: 0 });
  const edge = (source: string, target: string) => ({ id: `${source}->${target}`, source, sourceHandle: 'out', target, targetHandle: 'in' });
  const retry = { maxAttempts: 3, backoffMs: 1500, backoffMultiplier: 2, retryUnknown: true } as const;
  return {
    id: DECK_GENERATE_PIPELINE_ID,
    name: 'Presentation from documents',
    description: 'A /deck command from a chat: read the conversation and the referenced documents, draft a grounded DeckSpec with a light model, render a real .pptx with per-slide previews into a presentation document.',
    version: 1, status: 'published', publishedVersion: 1,
    nodes: [
      { id: 'trigger', type: 'trigger', position: pos(0), data: { type: 'trigger', triggerType: 'platform', label: '/deck command' } },
      { id: 'read-history', type: CHAT_READ_HISTORY_KIND, position: pos(200), data: { type: CHAT_READ_HISTORY_KIND, label: 'Read the conversation', retryPolicy: retry } },
      { id: 'read-sources', type: DECK_READ_SOURCES_KIND, position: pos(400), data: { type: DECK_READ_SOURCES_KIND, label: 'Read the source documents', retryPolicy: retry } },
      { id: 'draft', type: 'agent', position: pos(600), data: { type: 'agent', label: 'Draft the slides (Haiku)', prompt: DECK_DRAFT_PROMPT, model, allowedTools: [], maxTurns: 1, retryPolicy: DRAFT_RETRY } },
      { id: 'render', type: DECK_STEP_KIND, position: pos(800), data: { type: DECK_STEP_KIND, label: 'Render the deck (.pptx)', specFrom: 'steps.draft.text', retryPolicy: retry } },
    ],
    edges: [edge('trigger', 'read-history'), edge('read-history', 'read-sources'), edge('read-sources', 'draft'), edge('draft', 'render')],
  };
}

/** The step kinds this pipeline needs, for the bootstrap's handler map. */
export function buildDeckPipelineHandlers(): Map<string, StepHandler> {
  return buildDeckStepHandlers();
}
