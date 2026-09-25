// `document-summary` and `document-action-list` — a NEW page drafted from one
// source document, started from a run draft (the Documents page ⋯ menu,
// "Create from this… › Summary… / Action list…"). The draft is ON the new,
// empty page (the run's `documentId`, the target); its `sources` name the
// document to read.
//
//   trigger → check (document:check-sources) → read → plan → review → approve → apply
//
// Draftable (systemPipelines.ts): the check step authorizes the requester —
// read on the source, edit on the target — before the agent reads or writes
// anything, and the target is never the source.
import {
  approvalNode,
  DOCUMENT_APPLY_KIND,
  DOCUMENT_IO_RETRY,
  DOCUMENT_PLAN_KIND,
  DOCUMENT_PLAN_MAX_TOKENS,
  DOCUMENT_PLAN_RETRY,
  DOCUMENT_PLAN_TIMEOUT_MS,
  DOCUMENT_READ_KIND,
  DOCUMENT_REVIEW_KIND,
  type DocumentPipelineOptions,
} from './documentAgentPipeline';
import { DOCUMENT_CHECK_SOURCES_KIND } from './DocumentCheckSourcesStep';

export const DOCUMENT_SUMMARY_PIPELINE_ID = 'document-summary';
export const DOCUMENT_ACTION_LIST_PIPELINE_ID = 'document-action-list';

const GROUNDING = 'Never invent facts, names, dates or numbers that are not in the source document. The TARGET page is empty: never use deleteBlock, replaceBlock, insertBlock or copyBlock.';
const SOURCE_AND_GUIDANCE = [
  '',
  'Source document (listed name, then numbered blocks; macros show their kind and YAML):',
  '{{ context.documentOutline }}',
  '',
  'Extra guidance from the person who asked (may be empty; it shapes emphasis and wording, never adds facts): {{ context.instruction }}',
];

export const DOCUMENT_SUMMARY_PROMPT = [
  'You write a short summary of a document onto a new, empty page. Answer with ONLY a JSON array of operations, no prose, no code fence.',
  'Operations available, in the order they should appear:',
  '  {"op":"setTitle","text":"<source title> — Summary"}              — always first, exactly this shape: the source title as listed, a spaced em dash, "Summary"',
  '  {"op":"appendBlock","kind":"heading","level":2,"text":"..."}    — a section heading',
  '  {"op":"appendBlock","kind":"paragraph","text":"..."}            — prose under the heading above it',
  '  {"op":"appendBlock","kind":"bulletList","text":"first\\nsecond"} — a bulleted list: ONE item per line, no bullet characters typed',
  '  {"op":"skip","reason":"..."}                                    — the ONLY thing to return when the source has nothing to summarise (empty, or a title alone)',
  'Sections, in this order: "Summary" (a heading, then 2–4 sentences on what the document is and says); "Key points" (a heading, then ONE bulletList of the main facts); "Decisions" and "Open questions" (a heading and a bulletList each) ONLY when the source states some — never an empty section.',
  GROUNDING,
  ...SOURCE_AND_GUIDANCE,
].join('\n');

export const DOCUMENT_ACTION_LIST_PROMPT = [
  'You pull the action items out of a document onto a new, empty page. Answer with ONLY a JSON array of operations, no prose, no code fence.',
  'Operations available, in the order they should appear:',
  '  {"op":"setTitle","text":"<source title> — Action items"}        — always first, exactly this shape: the source title as listed, a spaced em dash, "Action items"',
  '  {"op":"appendMacro","macroName":"action-items","yaml":"items:\\n  - assignee: <name>\\n    status: pending\\n    text: <what they will do>\\n  - status: pending\\n    text: <unowned next step>"}',
  '      — always second and last: ONE checklist of every concrete commitment or next step the source states (someone will do something, something must happen by a date, an open task). Keys per item, in this order: assignee (ONLY when the source names who owns it, written as the source writes it; omit otherwise), status (pending, or done when the source says it is done), text (the action in a short imperative sentence, with its due date when the source gives one).',
  '  {"op":"skip","reason":"..."}                                    — the ONLY thing to return when the source states no commitments or next steps; say so in one sentence',
  'Only actions the source states or plainly assigns: not background, not decisions already carried out, not your own suggestions. One item per action; never merge two owners into one item.',
  GROUNDING,
  ...SOURCE_AND_GUIDANCE,
].join('\n');

interface FromSourceSpec {
  id: string;
  name: string;
  description: string;
  what: string;
  prompt: string;
}

function buildFromSourceDefinition(spec: FromSourceSpec, model: string, opts: DocumentPipelineOptions): Record<string, unknown> {
  const pos = (x: number) => ({ x, y: 0 });
  const edge = (source: string, target: string, sourceHandle = 'out') => ({ id: `${source}->${target}`, source, sourceHandle, target, targetHandle: 'in' });
  return {
    id: spec.id,
    name: spec.name,
    workType: 'documents', kind: 'document', route: ['Run draft', 'Check', 'Read', 'Plan', 'Approval', 'Write'], origin: { kind: 'system' },
    description: spec.description,
    version: 1, status: 'published', publishedVersion: 1,
    nodes: [
      { id: 'trigger', type: 'trigger', position: pos(0), data: { type: 'trigger', triggerType: 'manual', label: 'Run draft' } },
      { id: 'check', type: DOCUMENT_CHECK_SOURCES_KIND, position: pos(200), data: { type: DOCUMENT_CHECK_SOURCES_KIND, label: 'Check the requester may read the source and edit the page', retryPolicy: DOCUMENT_IO_RETRY } },
      { id: 'read', type: DOCUMENT_READ_KIND, position: pos(400), data: { type: DOCUMENT_READ_KIND, label: 'Read the source', retryPolicy: DOCUMENT_IO_RETRY } },
      { id: 'plan', type: DOCUMENT_PLAN_KIND, position: pos(600), data: { type: DOCUMENT_PLAN_KIND, label: `Plan the ${spec.what} (Haiku)`, prompt: spec.prompt, model, temperature: 0, maxTokens: DOCUMENT_PLAN_MAX_TOKENS, timeoutMs: DOCUMENT_PLAN_TIMEOUT_MS, retryPolicy: DOCUMENT_PLAN_RETRY } },
      // A new, empty page: the op count is the page, not a risk. Only a deleteBlock would gate it.
      { id: 'review', type: DOCUMENT_REVIEW_KIND, position: pos(800), data: { type: DOCUMENT_REVIEW_KIND, label: 'Check the plan', maxOpsWithoutApproval: Number.MAX_SAFE_INTEGER, retryPolicy: DOCUMENT_IO_RETRY } },
      approvalNode(pos(1000), opts),
      { id: 'apply', type: DOCUMENT_APPLY_KIND, position: pos(1200), data: { type: DOCUMENT_APPLY_KIND, label: `Write the ${spec.what} into the new page`, mode: 'direct', retryPolicy: DOCUMENT_IO_RETRY } },
    ],
    edges: [edge('trigger', 'check'), edge('check', 'read'), edge('read', 'plan'), edge('plan', 'review'), edge('review', 'approve'), edge('approve', 'apply', 'approved')],
  };
}

export function buildDocumentSummaryDefinition(model = process.env.DOCUMENT_AGENT_MODEL || 'haiku', opts: DocumentPipelineOptions = {}): Record<string, unknown> {
  return buildFromSourceDefinition({
    id: DOCUMENT_SUMMARY_PIPELINE_ID,
    name: 'Summary from a document',
    description: 'A run draft on a new page with one source document: check the requester may read the source and edit the page, read the source, plan a short summary (key points, decisions, open questions) with a light model, write it into the new page.',
    what: 'summary',
    prompt: DOCUMENT_SUMMARY_PROMPT,
  }, model, opts);
}

export function buildDocumentActionListDefinition(model = process.env.DOCUMENT_AGENT_MODEL || 'haiku', opts: DocumentPipelineOptions = {}): Record<string, unknown> {
  return buildFromSourceDefinition({
    id: DOCUMENT_ACTION_LIST_PIPELINE_ID,
    name: 'Action list from a document',
    description: 'A run draft on a new page with one source document: check the requester may read the source and edit the page, read the source, pull its commitments and next steps into one action-items checklist with a light model, write it into the new page.',
    what: 'action list',
    prompt: DOCUMENT_ACTION_LIST_PROMPT,
  }, model, opts);
}
