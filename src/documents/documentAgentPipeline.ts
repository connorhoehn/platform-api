// "/doc change the title to X" in a chat becomes a PIPELINE RUN, not a request
// handler: read the document → a light model plans the edit → the edit lands
// through the collaboration channel. Three steps the run store persists, the
// pipeline UI shows, and the durability machinery (retry, resume, DLQ) covers
// — the same path any other pipeline takes, exercised by something people do
// all day.
import type { StepHandler, StepOutcome, PipelineNode } from 'distributed-core/applications/pipeline';
import { AgentCollaborator, HAIKU_AGENT } from './AgentCollaborator';
import { applyOps, outlineOf, parseOps, type OutlineBlock } from './documentAgentOps';
import log from '../lib/logger';

export const DOCUMENT_AGENT_EDIT_PIPELINE_ID = 'document-agent-edit';
export const DOCUMENT_TRANSFORM_PIPELINE_ID = 'document-transform';
export const DOCUMENT_READ_KIND = 'document:read';
export const DOCUMENT_APPLY_KIND = 'document:apply';

/** What the light model is told. Interpolated by the agent step from the run context. */
export const DOCUMENT_EDIT_PROMPT = [
  'You edit a collaborative document on behalf of a person. Answer with ONLY a JSON array of operations, no prose, no code fence.',
  'Allowed operations:',
  '  {"op":"setTitle","text":"..."}                              — rename the document (also rewrites its first level-1 heading, or adds one if there is none)',
  '  {"op":"replaceBlock","index":N,"text":"..."}                — replace the text of block #N',
  '  {"op":"appendBlock","kind":"paragraph"|"heading","level":2,"text":"..."} — add a block at the end',
  '  {"op":"insertBlock","index":N,"kind":"paragraph"|"heading","level":1|2|3,"text":"..."} — add a block before #N',
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

/** What the light model is told for a TRANSFORM: rebuild the source under the target type's fields. */
export const DOCUMENT_TRANSFORM_PROMPT = [
  'You restructure a document from one known type into another. Answer with ONLY a JSON array of operations, no prose, no code fence.',
  'The TARGET page is empty. Operations available:',
  '  {"op":"setDocumentType","typeName":"{{ context.targetName }}","typeId":"{{ context.targetId }}","icon":"{{ context.targetIcon }}"}   — always first',
  '  {"op":"setTitle","text":"<source title> — {{ context.targetName }}"}   — always second',
  '  {"op":"appendBlock","kind":"heading","level":2,"text":"<field name>"} — a target field',
  '  {"op":"copyBlock","index":N}                                  — carry source block #N VERBATIM (the only way a macro crosses; never retype a macro)',
  '  {"op":"appendBlock","kind":"paragraph","text":"..."}          — source prose that answers a field, rewritten only as much as the field needs',
  '  {"op":"appendMacro","macroName":"field","yaml":"label: <field name>\\ntype: <text|date|checklist|link-block>"} — a typed field the target declares and the source lacks a value for',
  'Build the page FIELD BY FIELD, in the order the target fields are given: emit the field heading, then IMMEDIATELY the source blocks (copyBlock) and prose that answer THAT field, then move to the next field. Never emit all headings first.',
  'Source macro blocks (kind "macro"): a field macro whose label matches a target field is copied under that field; action-item, decision, callout, link and attachment macros go under the field they belong to. Every source macro block is copied exactly once.',
  'Source blocks that answer no target field — including field macros with no counterpart — are copied under a final heading "Notes", in source order. Never drop a source block that has text; never invent content; leave a field with nothing to say as its heading alone.',
  '',
  'Source document (numbered blocks; macros show their kind and YAML):',
  '{{ context.documentOutline }}',
  '',
  'Target type: {{ context.targetName }}',
  'Target fields, in order:',
  '{{ context.targetFieldList }}',
].join('\n');

/** The transform pipeline: read the SOURCE, plan the target with a light model, write the target as a collaborator. */
export function buildDocumentTransformDefinition(model = process.env.DOCUMENT_AGENT_MODEL || 'haiku'): Record<string, unknown> {
  const pos = (x: number) => ({ x, y: 0 });
  return {
    id: DOCUMENT_TRANSFORM_PIPELINE_ID,
    name: 'Document transform',
    description: 'Transform to <type>: read the source page, plan the target under its fields with a light model (macros carried verbatim), write it as a collaborator.',
    version: 1, status: 'published', publishedVersion: 1,
    nodes: [
      { id: 'trigger', type: 'trigger', position: pos(0), data: { type: 'trigger', triggerType: 'manual', label: 'Transform to <type>' } },
      { id: 'read', type: DOCUMENT_READ_KIND, position: pos(200), data: { type: DOCUMENT_READ_KIND, label: 'Read the source' } },
      { id: 'plan', type: 'agent', position: pos(400), data: { type: 'agent', label: 'Plan the target (Haiku)', prompt: DOCUMENT_TRANSFORM_PROMPT, model, allowedTools: [], maxTurns: 1 } },
      { id: 'apply', type: DOCUMENT_APPLY_KIND, position: pos(600), data: { type: DOCUMENT_APPLY_KIND, label: 'Write the target as a collaborator' } },
    ],
    edges: [
      { id: 'e1', source: 'trigger', sourceHandle: 'out', target: 'read', targetHandle: 'in' },
      { id: 'e2', source: 'read', sourceHandle: 'out', target: 'plan', targetHandle: 'in' },
      { id: 'e3', source: 'plan', sourceHandle: 'out', target: 'apply', targetHandle: 'in' },
    ],
  };
}

/** The definition the route triggers inline — platform-owned, not a per-user record. */
export function buildDocumentAgentEditDefinition(model = process.env.DOCUMENT_AGENT_MODEL || 'haiku'): Record<string, unknown> {
  const pos = (x: number) => ({ x, y: 0 });
  return {
    id: DOCUMENT_AGENT_EDIT_PIPELINE_ID,
    name: 'Document agent edit',
    description: 'A /doc command from a chat: read the document, plan the edit with a light model, apply it as a collaborator.',
    version: 1,
    status: 'published',
    publishedVersion: 1,
    nodes: [
      { id: 'trigger', type: 'trigger', position: pos(0), data: { type: 'trigger', triggerType: 'manual', label: '/doc command' } },
      { id: 'read', type: DOCUMENT_READ_KIND, position: pos(200), data: { type: DOCUMENT_READ_KIND, label: 'Read the document' } },
      { id: 'plan', type: 'agent', position: pos(400), data: { type: 'agent', label: 'Plan the edit (Haiku)', prompt: DOCUMENT_EDIT_PROMPT, model, allowedTools: [], maxTurns: 1 } },
      { id: 'apply', type: DOCUMENT_APPLY_KIND, position: pos(600), data: { type: DOCUMENT_APPLY_KIND, label: 'Apply as a collaborator' } },
    ],
    edges: [
      { id: 'e1', source: 'trigger', sourceHandle: 'out', target: 'read', targetHandle: 'in' },
      { id: 'e2', source: 'read', sourceHandle: 'out', target: 'plan', targetHandle: 'in' },
      { id: 'e3', source: 'plan', sourceHandle: 'out', target: 'apply', targetHandle: 'in' },
    ],
  };
}

function documentIdOf(context: Record<string, unknown>): string | null {
  return typeof context.documentId === 'string' && context.documentId ? context.documentId : null;
}

/** Step 1: join the document and hand its outline to the run context. */
export function createDocumentReadHandler(): StepHandler {
  return {
    kind: DOCUMENT_READ_KIND,
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
        return { status: 'failed', error: err instanceof Error ? err.message : String(err) };
      } finally {
        await agent.leave(0).catch(() => undefined);
      }
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
export function createDocumentApplyHandler(): StepHandler {
  return {
    kind: DOCUMENT_APPLY_KIND,
    async execute(node: PipelineNode, context: Record<string, unknown>): Promise<StepOutcome> {
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
      const agent = new AgentCollaborator({ documentId, identity: data.agent ? { ...HAIKU_AGENT, ...data.agent } : HAIKU_AGENT });
      try {
        await agent.join();
        agent.announce();
        const sourceBlocks = Array.isArray(context.sourceBlocks) ? (context.sourceBlocks as OutlineBlock[]) : [];
        const result = applyOps(agent.doc, ops, 'agent', sourceBlocks);
        log.info({ documentId, ...result, ops: ops.length }, 'document agent edit applied');
        // The step is done the moment the edit is in the document — the
        // run's status must not lag what people already see. The agent then
        // lingers in presence on its own clock so collaborators can see who
        // made the change, and leaves like anyone else.
        void agent.leave(Number(process.env.DOCUMENT_AGENT_PRESENCE_HOLD_MS) || 4000).catch(() => undefined);
        return { status: 'completed', output: { ...result, ops } };
      } catch (err) {
        await agent.leave(0).catch(() => undefined);
        return { status: 'failed', error: err instanceof Error ? err.message : String(err) };
      }
    },
  };
}

export function buildDocumentAgentHandlers(): Map<string, StepHandler> {
  const m = new Map<string, StepHandler>();
  m.set(DOCUMENT_READ_KIND, createDocumentReadHandler());
  m.set(DOCUMENT_APPLY_KIND, createDocumentApplyHandler());
  return m;
}
