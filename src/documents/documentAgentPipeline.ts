// "/doc change the title to X" in a chat becomes a PIPELINE RUN, not a request
// handler: read the document → a light model plans the edit → the plan is
// reviewed (a big or destructive one pauses for the person who asked) → the
// edit lands through the collaboration channel. Steps the run store persists,
// the pipeline UI shows, and the durability machinery (retry, resume, DLQ)
// covers — the same path any other pipeline takes, exercised by something
// people do all day.
import type { StepHandler, StepOutcome, PipelineNode } from 'distributed-core/applications/pipeline';
import { describeError } from '../gateway/GatewayServiceClient';
import type { LLMClient } from 'distributed-core/applications/pipeline';
import { createLLMClient } from '../pipeline/factories/LLMClientFactory';
import { interpolateContext } from '../pipeline/agent/AgentStepHandler';
import { AgentCollaborator, HAIKU_AGENT } from './AgentCollaborator';
import { applyChangeIndex, applyOps, createSuggestionIdentity, runSuggestionIdentity, ensureCanvasMeta, outlineOf, parseOps, type ApplyMode, type OutlineBlock } from './documentAgentOps';
import log from '../lib/logger';
import type { DescribedStepHandler } from '../pipeline/customStepHandlers';

export const DOCUMENT_AGENT_EDIT_PIPELINE_ID = 'document-agent-edit';
export const DOCUMENT_TRANSFORM_PIPELINE_ID = 'document-transform';
/** A person's Accept/Reject of an agent's suggestions, landing server-side through the same apply step. */
export const DOCUMENT_AGENT_REVIEW_PIPELINE_ID = 'document-agent-review';
/**
 * Who a suggestion is FROM: the person who asked, with the model second.
 *
 * "Eve (powered by Haiku)" rather than "Haiku". The agent's own name tells a
 * reader which engine ran and not who to talk to about the change — in a
 * document with several collaborators that is the least useful name on the
 * card. `requestedByName` has always been in the run context (the plan prompt
 * interpolates it); it simply never reached the suggestion mark.
 *
 * Falls back to the bare agent name when no person is known, so a run started
 * by an API caller or a schedule is attributed to something rather than to an
 * empty string.
 */
export function suggestionAuthorName(requestedByName: unknown, agentName: string): string {
  const person = typeof requestedByName === 'string' ? requestedByName.trim() : '';
  return person ? `${person} (powered by ${agentName})` : agentName;
}

export const DOCUMENT_READ_KIND = 'document:read';
/**
 * The plan: ONE model call, not an agent session. It used to be an `agent`
 * step — a `claude -p` process with a tool loop, an 8-turn budget and a
 * 120-second bound — for a job that is "read this outline, answer with a
 * JSON array". A person waiting on a /doc edit watched "Deciding what to
 * change" for two minutes when the model endpoint was wrong, and every
 * successful edit paid a CLI start-up for nothing. This is a direct
 * `LLMClient` call: temperature 0 (the same instruction should give the same
 * plan), a small output cap, and a 30-second bound.
 */
export const DOCUMENT_PLAN_KIND = 'document:plan';
export const DOCUMENT_PLAN_TIMEOUT_MS = 30_000;
export const DOCUMENT_PLAN_MAX_TOKENS = 1024;
export const DOCUMENT_REVIEW_KIND = 'document:review-plan';
export const DOCUMENT_APPLY_KIND = 'document:apply';

/**
 * Retries are the executor's (distributed-core ≥ 0.45.8 retries every node
 * kind from `data.retryPolicy`). Reading and writing a document cross the
 * collaboration channel, so they get three goes; the light model gets two.
 */
export const DOCUMENT_IO_RETRY = { maxAttempts: 3, backoffMs: 1500, backoffMultiplier: 2, retryUnknown: true } as const;
export const DOCUMENT_PLAN_RETRY = { maxAttempts: 2, backoffMs: 3000, backoffMultiplier: 2, retryUnknown: true } as const;

/** Plans with more operations than this pause for a person. */
export const DEFAULT_APPROVAL_OPS = 8;
export const APPROVAL_TIMEOUT_MS = 24 * 60 * 60 * 1000;

/** Options both document pipelines take when they are built. */
export interface DocumentPipelineOptions {
  /**
   * Who must approve a big edit. Set by the route at trigger time (the
   * requester); the registry build, which names no one, falls back to the
   * `requester` role so the definition still lints and still lists.
   */
  approverUserId?: string;
}

/** The approval gate shared by both pipelines: skipped unless the review step says a person is needed. */
function approvalNode(pos: { x: number; y: number }, opts: DocumentPipelineOptions): Record<string, unknown> {
  const approvers = opts.approverUserId
    ? [{ type: 'user', value: opts.approverUserId, userId: opts.approverUserId }]
    : [{ type: 'role', value: 'requester' }];
  return {
    id: 'approve', type: 'approval', position: pos,
    data: {
      type: 'approval', label: 'Approve a big edit',
      condition: 'context.needsApproval === true',
      approvers, requiredCount: 1,
      timeoutMs: APPROVAL_TIMEOUT_MS, timeoutAction: 'reject',
      // Not templated by the executor; the review step's `reason` rides the run context and the approvals list shows it.
      message: 'A big or destructive edit is waiting for your approval.',
    },
  };
}

/** What the light model is told. Interpolated by the agent step from the run context. */
export const DOCUMENT_EDIT_PROMPT = [
  'You edit a collaborative document on behalf of a person. Answer with ONLY a JSON array of operations, no prose, no code fence.',
  'Allowed operations:',
  '  {"op":"setTitle","text":"..."}                              — rename the document (also rewrites its first level-1 heading, or adds one if there is none)',
  '  {"op":"replaceBlock","index":N,"text":"..."}                — replace the text of block #N',
  '  {"op":"appendBlock","kind":"paragraph"|"heading"|"horizontalRule"|"bulletList"|"orderedList","level":2,"text":"..."} — add a block at the end (a horizontalRule is a divider line; give it text "")',
  '  {"op":"insertBlock","index":N,"kind":"paragraph"|"heading"|"horizontalRule"|"bulletList"|"orderedList","level":1|2|3,"text":"..."} — add a block before #N (e.g. a divider after the title: insertBlock at index 1, kind horizontalRule)',
  '  {"op":"appendBlock","kind":"bulletList","text":"first item\\nsecond item"} — a bulleted list (also works with insertBlock + index): ONE item per line, and do NOT type the bullet character yourself',
  '  {"op":"appendBlock","kind":"orderedList","text":"first step\\nsecond step"} — a numbered list, same shape: one item per line, no numbers typed in the text',
  'Bullets are ONLY ever a list block: never write a list as a paragraph containing • or - characters, which renders as one run-on line.',
  '  {"op":"appendBlock","kind":"table","rows":[["Name","Owner"],["Data access","Sarah Chen"]]} — add a table (also works with insertBlock + index): rows is an array of rows, the first row is the header, "" is an empty cell',
  '  {"op":"replaceBlock","index":N,"rows":[["Name","Owner"],["Data access","Sarah Chen"]]} — change a table: block #N is a table (its outline shows cells joined by " | "); give the WHOLE table back as rows, header first, with the cells you were asked to fill filled',
  'Tables are ONLY ever rows: never write a table as text with | characters, never put a table inside a cell, and never add filler such as "Placeholder text" — an empty cell is "".',
  '  {"op":"deleteBlock","index":N}                              — remove block #N',
  '  {"op":"skip","reason":"..."}                                — the ONLY thing to return when you will not change the document; say why in one sentence',
  'Titles: the "title" line is the document\'s NAME as listed; it is often a placeholder or an identifier (e.g. "Verify 0mlyxo", "Untitled", a ticket code). When asked for a title or heading, COMPOSE one from the document\'s content — what the text is about — in a few words; never repeat the listed name or any identifier-looking token, and never answer that a title already exists because the name line is filled. "Add a title" means setTitle with the composed title (it becomes the level-1 heading too).',
  'Make the smallest change that satisfies the instruction. Never rewrite blocks the instruction does not concern. Never invent facts that are not in the document or the instruction.',
  '',
  'Document (listed name, then numbered blocks):',
  '{{ context.documentOutline }}',
  '',
  'Instruction from {{ context.requestedByName }}: {{ context.instruction }}',
].join('\n');

/**
 * What the light model is told for a TRANSFORM: rebuild the source under the
 * target type's fields. The field list arrives as `- Name (sectionType)` lines
 * (see the transform-run route); the mark decides the SHAPE each field takes,
 * mirroring how the client seeds a typed canvas (documentTypeCanvasModel):
 * text/date/number/boolean → one `field` macro, long-form → a heading and the
 * blocks that answer it. Measured, not eyeballed: transformPlan.golden.test.ts
 * records Haiku's answer for nine source×target pairs and asserts the
 * invariants spelled out below; change the wording here and re-record.
 */
export const DOCUMENT_TRANSFORM_PROMPT = [
  'You restructure a document from one known type into another. Answer with ONLY a JSON array of operations, no prose, no code fence.',
  'The TARGET page is empty: never use deleteBlock, replaceBlock, insertBlock or skip. Operations available:',
  '  {"op":"setDocumentType","typeName":"{{ context.targetName }}","typeId":"{{ context.targetId }}","icon":"{{ context.targetIcon }}"}   — always first',
  '  {"op":"setTitle","text":"<source title> — {{ context.targetName }}"}   — always second, exactly this shape: the source title as listed, a spaced em dash, the target type name',
  '  {"op":"appendMacro","macroName":"field","yaml":"label: <field name>\\ntype: <text|date|number|boolean>\\nvalue: <value>"} — a SHORT target field, filled from the source; leave the value line out entirely when the source gives no answer',
  '  {"op":"appendBlock","kind":"heading","level":2,"text":"<field name>"} — a LONG-FORM target field, or the final "Notes" heading',
  '  {"op":"copyBlock","index":N}                                  — carry source block #N VERBATIM (the only way a macro crosses; never retype a macro)',
  '  {"op":"appendBlock","kind":"paragraph","text":"..."}          — source prose that answers a long-form field, reworded; prefer copyBlock, and a reworded paragraph keeps every fact, name and number of the original',
  '  {"op":"appendMacro","macroName":"action-items","yaml":"items: []"} — the empty list under a (checklist) or (tasks) field the source has nothing for',
  '  {"op":"appendMacro","macroName":"links","yaml":"links: []"}   — the empty list under a (link-block) field the source has nothing for',
  'Every target field appears EXACTLY ONCE, by its exact name, in the order given. The mark after a field name says which shape it takes:',
  '- (text), (date), (number) or (boolean): ONE field macro, never a heading. label is the field name exactly as given; type is the mark. The value comes from the source: a source field macro with the same or an equivalent label (its value MOVES here), or a fact the source states outright (its title, a date, a name, a number). Dates are YYYY-MM-DD, numbers bare digits, booleans true or false. When the source does not say, omit the value line — never guess, never fill a placeholder.',
  '- (rich-text), (checklist), (tasks), (link-block) or no mark: a level-2 heading, then IMMEDIATELY the source blocks that answer it — copyBlock for macros and for paragraphs used as they are, appendBlock paragraphs for prose that needs rewording — then the next field. Never emit all headings first. A field with nothing to say is its heading alone ((checklist)/(tasks): heading + empty action-items macro; (link-block): heading + empty links macro).',
  'Source blocks: the source\'s level-1 title heading is covered by setTitle and is not copied. A source heading that a target field replaces is not copied either; its paragraphs go under that field. Every other source block with text is placed exactly once: a source field macro whose value moved into a target field macro is done; every other source macro (field, action-items, decision, links, attachment, callout, hitl) is copied exactly once — never retyped, never split — under the field it belongs to; every source paragraph is copied or reworded under the field it answers.',
  'Leftovers: source blocks that answer no target field go under one final heading "Notes", in source order — a leftover field macro (a Severity, a Status, a Date with no counterpart) is a copyBlock of that macro, never a paragraph that spells it out as "Severity: High". Never invent content; never drop a source block that has text.',
  'Before answering, walk the source block numbers #1 to the last one: each must be either a copyBlock index, the origin of one field value, or fully reworded into one paragraph. Any number you cannot point to goes under Notes as a copyBlock.',
  '',
  'Source document (numbered blocks; macros show their kind and YAML):',
  '{{ context.documentOutline }}',
  '',
  'Target type: {{ context.targetName }}',
  'Target fields, in order:',
  '{{ context.targetFieldList }}',
].join('\n');

/** The transform pipeline: read the SOURCE, plan the target with a light model, write the target as a collaborator. */
export function buildDocumentTransformDefinition(model = process.env.DOCUMENT_AGENT_MODEL || 'haiku', opts: DocumentPipelineOptions = {}): Record<string, unknown> {
  const pos = (x: number) => ({ x, y: 0 });
  return {
    id: DOCUMENT_TRANSFORM_PIPELINE_ID,
    name: 'Document transform',
    workType: 'documents', kind: 'document', route: ['Transform', 'Read', 'Plan', 'Approval', 'Write'], origin: { kind: 'system' },
    description: 'Transform to <type>: read the source page, plan the target under its fields with a light model (macros carried verbatim), pause for approval when the plan is big, write it as a collaborator.',
    version: 1, status: 'published', publishedVersion: 1,
    nodes: [
      { id: 'trigger', type: 'trigger', position: pos(0), data: { type: 'trigger', triggerType: 'platform', label: 'Transform to <type>' } },
      { id: 'read', type: DOCUMENT_READ_KIND, position: pos(200), data: { type: DOCUMENT_READ_KIND, label: 'Read the source', retryPolicy: DOCUMENT_IO_RETRY } },
      { id: 'plan', type: DOCUMENT_PLAN_KIND, position: pos(400), data: { type: DOCUMENT_PLAN_KIND, label: 'Plan the target (Haiku)', prompt: DOCUMENT_TRANSFORM_PROMPT, model, temperature: 0, maxTokens: DOCUMENT_PLAN_MAX_TOKENS, timeoutMs: DOCUMENT_PLAN_TIMEOUT_MS, retryPolicy: DOCUMENT_PLAN_RETRY } },
      // A transform always stamps the new type — that is not a reason to stop and ask.
      // A transform writes a NEW, empty page: the op count is the page, not a
      // risk, and the type change is the point. Only a deleteBlock would gate it.
      { id: 'review', type: DOCUMENT_REVIEW_KIND, position: pos(600), data: { type: DOCUMENT_REVIEW_KIND, label: 'Check the plan', allowTypeChange: true, maxOpsWithoutApproval: Number.MAX_SAFE_INTEGER, retryPolicy: DOCUMENT_IO_RETRY } },
      approvalNode(pos(800), opts),
      { id: 'apply', type: DOCUMENT_APPLY_KIND, position: pos(1000), data: { type: DOCUMENT_APPLY_KIND, label: 'Write the target as a collaborator', mode: 'suggest', retryPolicy: DOCUMENT_IO_RETRY } },
    ],
    edges: documentEdges(),
  };
}

/**
 * trigger → read → plan → review → approve → apply. The approval's `rejected`
 * handle is deliberately unwired: the executor gives `rejected` no fallback,
 * so a reject ends the traversal and the run settles `rejected`.
 */
function documentEdges(): Array<Record<string, unknown>> {
  return [
    { id: 'e1', source: 'trigger', sourceHandle: 'out', target: 'read', targetHandle: 'in' },
    { id: 'e2', source: 'read', sourceHandle: 'out', target: 'plan', targetHandle: 'in' },
    { id: 'e3', source: 'plan', sourceHandle: 'out', target: 'review', targetHandle: 'in' },
    { id: 'e4', source: 'review', sourceHandle: 'out', target: 'approve', targetHandle: 'in' },
    { id: 'e5', source: 'approve', sourceHandle: 'approved', target: 'apply', targetHandle: 'in' },
  ];
}

/** The definition the route triggers inline — platform-owned, not a per-user record. */
export function buildDocumentAgentEditDefinition(model = process.env.DOCUMENT_AGENT_MODEL || 'haiku', opts: DocumentPipelineOptions = {}): Record<string, unknown> {
  const pos = (x: number) => ({ x, y: 0 });
  return {
    id: DOCUMENT_AGENT_EDIT_PIPELINE_ID,
    name: 'Document agent edit',
    workType: 'documents', kind: 'document', route: ['/doc', 'Read', 'Plan', 'Approval', 'Suggestions'], origin: { kind: 'system' },
    description: 'A /doc command from a chat: read the document, plan the edit with a light model, pause for approval when the plan is big or deletes, apply it as a collaborator.',
    version: 1,
    status: 'published',
    publishedVersion: 1,
    nodes: [
      { id: 'trigger', type: 'trigger', position: pos(0), data: { type: 'trigger', triggerType: 'platform', label: '/doc command' } },
      { id: 'read', type: DOCUMENT_READ_KIND, position: pos(200), data: { type: DOCUMENT_READ_KIND, label: 'Read the document', retryPolicy: DOCUMENT_IO_RETRY } },
      { id: 'plan', type: DOCUMENT_PLAN_KIND, position: pos(400), data: { type: DOCUMENT_PLAN_KIND, label: 'Plan the edit (Haiku)', prompt: DOCUMENT_EDIT_PROMPT, model, temperature: 0, maxTokens: DOCUMENT_PLAN_MAX_TOKENS, timeoutMs: DOCUMENT_PLAN_TIMEOUT_MS, retryPolicy: DOCUMENT_PLAN_RETRY } },
      { id: 'review', type: DOCUMENT_REVIEW_KIND, position: pos(600), data: { type: DOCUMENT_REVIEW_KIND, label: 'Check the plan', retryPolicy: DOCUMENT_IO_RETRY } },
      approvalNode(pos(800), opts),
      { id: 'apply', type: DOCUMENT_APPLY_KIND, position: pos(1000), data: { type: DOCUMENT_APPLY_KIND, label: 'Suggest as a collaborator', mode: 'suggest', retryPolicy: DOCUMENT_IO_RETRY } },
    ],
    edges: documentEdges(),
  };
}

/**
 * The review landing: trigger → apply, direct mode, operations from
 * `reviewOps` (`acceptSuggestion` / `rejectSuggestion` under one key). Used
 * when the document is not open in anyone's editor, so the person's decision
 * still goes through the one write path and shows up as a run.
 */
export function buildDocumentReviewApplyDefinition(): Record<string, unknown> {
  const pos = (x: number) => ({ x, y: 0 });
  return {
    id: DOCUMENT_AGENT_REVIEW_PIPELINE_ID,
    name: 'Document suggestion review',
    workType: 'documents', kind: 'document', route: ['Suggestion reviewed', 'Resolve'], origin: { kind: 'system' },
    description: 'A person accepted or rejected an agent\'s suggestions: resolve them in the document as a collaborator.',
    version: 1, status: 'published', publishedVersion: 1,
    nodes: [
      { id: 'trigger', type: 'trigger', position: pos(0), data: { type: 'trigger', triggerType: 'platform', label: 'Suggestion reviewed' } },
      { id: 'apply', type: DOCUMENT_APPLY_KIND, position: pos(200), data: { type: DOCUMENT_APPLY_KIND, label: 'Resolve the suggestions', mode: 'direct', opsFrom: 'reviewOps', agent: REVIEW_AGENT, retryPolicy: DOCUMENT_IO_RETRY } },
    ],
    edges: [{ id: 'e1', source: 'trigger', sourceHandle: 'out', target: 'apply', targetHandle: 'in' }],
  };
}

/** Presence identity for a review landing — the decision is a person's, so the collaborator is named for the action, not a model. */
export const REVIEW_AGENT = { userId: 'agent:review', displayName: 'Review', color: '#0ea5e9' } as const;

function documentIdOf(context: Record<string, unknown>): string | null {
  if (typeof context.documentId === 'string' && context.documentId) return context.documentId;
  // A run that did not start from its own trigger — a retry, a replay —
  // still has the trigger's output under steps.
  const fromTrigger = readPath(context, 'steps.trigger.output.documentId');
  return typeof fromTrigger === 'string' && fromTrigger ? fromTrigger : null;
}

/** Step 1: join the document and hand its outline to the run context. */
export function createDocumentReadHandler(): DescribedStepHandler {
  return {
    kind: DOCUMENT_READ_KIND,
    description:
      'Joins the document as the agent and hands its outline to the run, so later steps can refer to blocks by id.',
    reads: 'sourceDocumentId, else documentId (or the trigger step\'s documentId).',
    emits: 'The document outline: one entry per block, with its id and text.',
    async execute(_node: PipelineNode, context: Record<string, unknown>): Promise<StepOutcome> {
      // A transform reads the SOURCE and writes the target; an edit reads and writes the same page.
      const readId = (typeof context.sourceDocumentId === 'string' && context.sourceDocumentId) || documentIdOf(context);
      if (!readId) return { status: 'failed', error: 'documentId missing from the run context' };
      const agent = new AgentCollaborator({ documentId: readId });
      try {
        await agent.join();
        const outline = outlineOf(agent.doc);
        // Merged into context by the executor: `{{ context.documentOutline }}`;
        // `sourceBlocks` lets `copyBlock` carry a macro across without the model retyping it.
        return { status: 'completed', output: { documentOutline: outline.text, documentTitle: outline.title ?? '', documentBlocks: outline.blocks.length, sourceBlocks: outline.blocks } };
      } catch (err) {
        return { status: 'failed', error: describeError(err) };
      } finally {
        await agent.leave(0).catch(() => undefined);
      }
    },
  };
}

/** Node options the review step honours. */
export interface DocumentReviewNodeData {
  /** Where the plan lives (dotted path); defaults like the apply step. */
  opsFrom?: string;
  /** A transform stamps the target type on purpose; do not stop for it. */
  allowTypeChange?: boolean;
  /** Per-node override of DOCUMENT_AGENT_APPROVAL_OPS. */
  maxOpsWithoutApproval?: number;
}

/** What the review step decides. Merged into the run context; the approval gate reads `needsApproval`. */
export interface DocumentReviewOutput {
  needsApproval: boolean;
  reason: string;
  opCount: number;
}

/**
 * The size of a plan, judged: big edits, deletions and type changes pause the
 * run for the person who asked. Pure over the plan — the same `parseOps` the
 * apply step will run, so what is approved is what lands.
 */
export function reviewPlan(ops: ReturnType<typeof parseOps>, data: DocumentReviewNodeData = {}): DocumentReviewOutput {
  const real = ops.filter((o) => o.op !== 'skip');
  const limit = typeof data.maxOpsWithoutApproval === 'number' ? data.maxOpsWithoutApproval : (Number(process.env.DOCUMENT_AGENT_APPROVAL_OPS) || DEFAULT_APPROVAL_OPS);
  const deletes = real.filter((o) => o.op === 'deleteBlock').length;
  const typeChange = !data.allowTypeChange && real.some((o) => o.op === 'setDocumentType');
  const reasons: string[] = [];
  if (real.length > limit) reasons.push(`${real.length} changes (more than ${limit})`);
  if (deletes > 0) reasons.push(`deletes ${deletes} block${deletes === 1 ? '' : 's'}`);
  if (typeChange) reasons.push('changes the document type');
  const needsApproval = reasons.length > 0;
  return {
    needsApproval,
    reason: needsApproval ? `The plan ${reasons.join(', ')}.` : (real.length === 0 ? 'The plan changes nothing.' : `${real.length} small change${real.length === 1 ? '' : 's'}; no approval needed.`),
    opCount: real.length,
  };
}

/** Step 3: read the plan and decide whether a person has to see it before it lands. */
export function createDocumentReviewHandler(): DescribedStepHandler {
  return {
    kind: DOCUMENT_REVIEW_KIND,
    description:
      'Judges the size of a planned edit and decides whether a person has to see it first. Big edits, deletions and type changes pause the run for whoever asked.',
    reads: 'The planned operations, from the plan step or from the node\'s opsFrom path.',
    emits: 'needsApproval, which the approval gate downstream reads.',
    async execute(node: PipelineNode, context: Record<string, unknown>): Promise<StepOutcome> {
      const data = ((node as { data?: DocumentReviewNodeData }).data ?? {}) as DocumentReviewNodeData;
      const { ops } = opsFromContext(context, { opsFrom: data.opsFrom });
      const output = reviewPlan(ops, data);
      log.info({ documentId: documentIdOf(context), ...output }, 'document agent plan reviewed');
      return { status: 'completed', output: { ...output } };
    },
  };
}

/** Node options the shared apply step honours. */
export interface DocumentApplyNodeData {
  /**
   * Where the operations live in the run context — a dotted path (e.g.
   * `transcriptOps`, `steps.plan.text`). The value may be an ops array or the
   * model's JSON text. Default: the planning agent's text, then `context.text`.
   */
  opsFrom?: string;
  /** Who the edit is attributed to in presence. Default: Haiku. */
  agent?: { userId: string; displayName: string; color?: string };
  /** When the run has no document, complete (nothing to write) instead of failing. */
  optional?: boolean;
  /**
   * `direct` (default) writes text; `suggest` writes tracked changes attributed
   * to `agent`, which a person then accepts or rejects. The two document
   * pipelines suggest; a transcript or a summary landing on a fresh page writes.
   */
  mode?: ApplyMode;
}

function readPath(obj: unknown, path: string): unknown {
  return path.split('.').reduce<unknown>((cur, key) => (cur && typeof cur === 'object' ? (cur as Record<string, unknown>)[key] : undefined), obj);
}

/** The operations a run wants applied: from `opsFrom`, else the planning agent's answer. */
export function opsFromContext(context: Record<string, unknown>, data: DocumentApplyNodeData): { ops: ReturnType<typeof parseOps>; source: string; raw: string } {
  const candidates: Array<[string, unknown]> = data.opsFrom
    ? [[data.opsFrom, readPath(context, data.opsFrom)]]
    : [['steps.plan.text', readPath(context, 'steps.plan.text')], ['text', context.text]];
  for (const [source, value] of candidates) {
    if (Array.isArray(value)) return { ops: parseOps(JSON.stringify(value)), source, raw: '' };
    if (typeof value === 'string' && value.trim()) return { ops: parseOps(value), source, raw: value };
  }
  return { ops: [], source: candidates[0]![0], raw: '' };
}

/**
 * Step 3, and THE write path into a document for every pipeline: join as the
 * agent, show up in presence, apply the operations, leave. The /doc edit, the
 * transform and the call transcript all land through this one step.
 */
export function createDocumentApplyHandler(): DescribedStepHandler {
  return {
    kind: DOCUMENT_APPLY_KIND,
    description:
      'The write path into a document: joins as the agent, applies the operations through the collaboration channel, and leaves. The /doc edit, the transform and the call transcript all land through this one step.',
    reads: 'documentId and the operations to apply.',
    emits: 'What landed — the applied operations and the suggestion identity they were written under.',
    async execute(node: PipelineNode, context: Record<string, unknown>, helpers?: { runId?: string; stepId?: string }): Promise<StepOutcome> {
      const data = ((node as { data?: DocumentApplyNodeData }).data ?? {}) as DocumentApplyNodeData;
      const documentId = documentIdOf(context);
      if (!documentId) {
        if (data.optional) return { status: 'completed', output: { applied: 0, skipped: 'no document on this run' } };
        return { status: 'failed', error: 'documentId missing from the run context' };
      }
      const { ops: planned, raw: planText } = opsFromContext(context, data);
      const skips = planned.filter((o) => o.op === 'skip') as Array<{ op: 'skip'; reason: string }>;
      const ops = planned.filter((o) => o.op !== 'skip');
      if (ops.length === 0) {
        const reason = skips[0]?.reason || (data.opsFrom ? 'nothing to write' : 'the model proposed no operations');
        return { status: 'completed', output: { applied: 0, skipped: 0, reason, planText: planText.slice(0, 500) } };
      }
      const identity = data.agent ? { ...HAIKU_AGENT, ...data.agent } : HAIKU_AGENT;
      const mode: ApplyMode = data.mode === 'suggest' ? 'suggest' : 'direct';
      const agent = new AgentCollaborator({ documentId, identity });
      try {
        await agent.join();
        agent.announce();
        // A page nobody seeded (an API caller's, a smoke test's) is still section-shaped; make it a canvas or the write is invisible.
        const seeded = ensureCanvasMeta(agent.doc, {
          id: documentId,
          title: typeof context.documentTitle === 'string' && context.documentTitle ? context.documentTitle : 'Untitled',
          createdBy: typeof context.requestedBy === 'string' && context.requestedBy ? context.requestedBy : identity.userId,
        });
        const sourceBlocks = Array.isArray(context.sourceBlocks) ? (context.sourceBlocks as OutlineBlock[]) : [];
        const result = applyOps(agent.doc, ops, {
          origin: 'agent', sourceBlocks, mode,
          ...(mode === 'suggest'
            ? {
              // NFR #213: one identity per run (and per apply arm), never the
              // shared ten-minute bucket. A caller with no run id (a test, a
              // direct call) keeps the chassis identity.
              identity: helpers?.runId
                ? runSuggestionIdentity(
                  identity.userId,
                  suggestionAuthorName(context.requestedByName, identity.displayName),
                  helpers.runId,
                  { change: applyChangeIndex(helpers.stepId) },
                )
                : createSuggestionIdentity(
                  identity.userId,
                  suggestionAuthorName(context.requestedByName, identity.displayName),
                  Date.now(),
                ),
            }
            : {}),
        });
        log.info({ documentId, ...result, mode, seeded, ops: ops.length }, 'document agent edit applied');
        // The step is done the moment the edit is in the document — the
        // run's status must not lag what people already see. The agent then
        // lingers in presence on its own clock so collaborators can see who
        // made the change, and leaves like anyone else.
        void agent.leave(Number(process.env.DOCUMENT_AGENT_PRESENCE_HOLD_MS) || 4000).catch(() => undefined);
        return { status: 'completed', output: { ...result, mode, ops } };
      } catch (err) {
        await agent.leave(0).catch(() => undefined);
        return { status: 'failed', error: describeError(err) };
      }
    },
  };
}

export interface DocumentPlanNodeData {
  prompt?: string;
  model?: string;
  temperature?: number;
  maxTokens?: number;
  timeoutMs?: number;
}

export function createDocumentPlanHandler(llm: LLMClient): DescribedStepHandler {
  return {
    kind: DOCUMENT_PLAN_KIND,
    description:
      'Asks a light model, in one call, which edits the instruction implies. Temperature 0 so the same instruction gives the same plan; bounded at 30 seconds.',
    reads: 'The node\'s prompt, interpolated against the run context and the outline.',
    emits: 'The planned operations, as JSON, for the review and apply steps.',
    async execute(node: PipelineNode, context: Record<string, unknown>): Promise<StepOutcome> {
      const data = ((node as { data?: DocumentPlanNodeData }).data ?? {}) as DocumentPlanNodeData;
      const rawPrompt = typeof data.prompt === 'string' ? data.prompt : '';
      if (!rawPrompt.trim()) return { status: 'failed', error: 'plan step has no prompt' };
      const prompt = interpolateContext(rawPrompt, context);
      const model = data.model?.trim() || process.env.DOCUMENT_AGENT_MODEL || 'haiku';
      const temperature = typeof data.temperature === 'number' ? data.temperature : 0;
      const maxTokens = typeof data.maxTokens === 'number' && data.maxTokens > 0 ? data.maxTokens : DOCUMENT_PLAN_MAX_TOKENS;
      const timeoutMs = typeof data.timeoutMs === 'number' && data.timeoutMs > 0 ? data.timeoutMs : DOCUMENT_PLAN_TIMEOUT_MS;
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), timeoutMs);
      const startedAt = Date.now();
      let text = '';
      let tokensIn = 0;
      let tokensOut = 0;
      try {
        for await (const chunk of llm.stream(model, '', prompt, { temperature, maxTokens, signal: controller.signal })) {
          if (chunk.done) {
            text = chunk.response || text;
            tokensIn = chunk.tokensIn;
            tokensOut = chunk.tokensOut;
          } else {
            text += chunk.token;
          }
        }
      } catch (err) {
        if (controller.signal.aborted) return { status: 'failed', error: `plan step stopped: timeout after ${timeoutMs}ms (${llm.provider}, ${model})` };
        return { status: 'failed', error: `plan step failed (${llm.provider}, ${model}): ${describeError(err)}` };
      } finally {
        clearTimeout(timer);
      }
      if (controller.signal.aborted) return { status: 'failed', error: `plan step stopped: timeout after ${timeoutMs}ms (${llm.provider}, ${model})` };
      log.info({ documentId: documentIdOf(context), model, provider: llm.provider, durationMs: Date.now() - startedAt, tokensIn, tokensOut }, 'document agent plan');
      return { status: 'completed', output: { text, model, provider: llm.provider, temperature, durationMs: Date.now() - startedAt, tokensIn, tokensOut } };
    },
  };
}

export function buildDocumentAgentHandlers(opts: { llm?: LLMClient } = {}): Map<string, StepHandler> {
  const m = new Map<string, StepHandler>();
  m.set(DOCUMENT_READ_KIND, createDocumentReadHandler());
  m.set(DOCUMENT_PLAN_KIND, createDocumentPlanHandler(opts.llm ?? createLLMClient()));
  m.set(DOCUMENT_REVIEW_KIND, createDocumentReviewHandler());
  m.set(DOCUMENT_APPLY_KIND, createDocumentApplyHandler());
  return m;
}
