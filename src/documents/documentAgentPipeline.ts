// "/doc change the title to X" in a chat becomes a PIPELINE RUN, not a request
// handler: read the document → a light model plans the edit → the edit lands
// through the collaboration channel. Three steps the run store persists, the
// pipeline UI shows, and the durability machinery (retry, resume, DLQ) covers
// — the same path any other pipeline takes, exercised by something people do
// all day.
import type { StepHandler, StepOutcome, PipelineNode } from 'distributed-core/applications/pipeline';
import { AgentCollaborator, HAIKU_AGENT } from './AgentCollaborator';
import { applyOps, outlineOf, parseOps } from './documentAgentOps';
import log from '../lib/logger';

export const DOCUMENT_AGENT_EDIT_PIPELINE_ID = 'document-agent-edit';
export const DOCUMENT_READ_KIND = 'document:read';
export const DOCUMENT_APPLY_KIND = 'document:apply';

/** What the light model is told. Interpolated by the agent step from the run context. */
export const DOCUMENT_EDIT_PROMPT = [
  'You edit a collaborative document on behalf of a person. Answer with ONLY a JSON array of operations, no prose, no code fence.',
  'Allowed operations:',
  '  {"op":"setTitle","text":"..."}                              — rename the document (also rewrites its first level-1 heading)',
  '  {"op":"replaceBlock","index":N,"text":"..."}                — replace the text of block #N',
  '  {"op":"appendBlock","kind":"paragraph"|"heading","level":2,"text":"..."} — add a block at the end',
  '  {"op":"insertBlock","index":N,"kind":"paragraph"|"heading","level":2,"text":"..."} — add a block before #N',
  '  {"op":"deleteBlock","index":N}                              — remove block #N',
  'Make the smallest change that satisfies the instruction. Never rewrite blocks the instruction does not concern. If the instruction is unclear, return [].',
  '',
  'Document (current title, then numbered blocks):',
  '{{ context.documentOutline }}',
  '',
  'Instruction from {{ context.requestedByName }}: {{ context.instruction }}',
].join('\n');

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
      { id: 'trigger', type: 'trigger', position: pos(0), data: { type: 'trigger', triggerType: 'manual' } },
      { id: 'read', type: DOCUMENT_READ_KIND, position: pos(200), data: { type: DOCUMENT_READ_KIND } },
      { id: 'plan', type: 'agent', position: pos(400), data: { type: 'agent', prompt: DOCUMENT_EDIT_PROMPT, model, allowedTools: [], maxTurns: 1 } },
      { id: 'apply', type: DOCUMENT_APPLY_KIND, position: pos(600), data: { type: DOCUMENT_APPLY_KIND } },
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
      const documentId = documentIdOf(context);
      if (!documentId) return { status: 'failed', error: 'documentId missing from the run context' };
      const agent = new AgentCollaborator({ documentId });
      try {
        await agent.join();
        const outline = outlineOf(agent.doc);
        // Merged into context by the executor: `{{ context.documentOutline }}`.
        return { status: 'completed', output: { documentOutline: outline.text, documentTitle: outline.title ?? '', documentBlocks: outline.blocks.length } };
      } catch (err) {
        return { status: 'failed', error: err instanceof Error ? err.message : String(err) };
      } finally {
        await agent.leave(0).catch(() => undefined);
      }
    },
  };
}

/** Step 3: join as the agent, show up in presence, apply the planned operations, leave. */
export function createDocumentApplyHandler(): StepHandler {
  return {
    kind: DOCUMENT_APPLY_KIND,
    async execute(_node: PipelineNode, context: Record<string, unknown>): Promise<StepOutcome> {
      const documentId = documentIdOf(context);
      if (!documentId) return { status: 'failed', error: 'documentId missing from the run context' };
      const steps = (context.steps ?? {}) as Record<string, unknown>;
      const plan = steps.plan as { text?: unknown } | undefined;
      const planText = typeof plan?.text === 'string' ? plan.text : typeof context.text === 'string' ? context.text : '';
      const ops = parseOps(planText);
      if (ops.length === 0) {
        return { status: 'completed', output: { applied: 0, skipped: 0, reason: 'the model proposed no operations', planText: planText.slice(0, 500) } };
      }
      const agent = new AgentCollaborator({ documentId, identity: HAIKU_AGENT });
      try {
        await agent.join();
        agent.announce();
        const result = applyOps(agent.doc, ops, 'agent');
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
