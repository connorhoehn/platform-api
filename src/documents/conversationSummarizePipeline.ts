// "/summarize" in a chat becomes a PIPELINE RUN that writes a page: read the
// conversation through the chat service → a light model plans a document
// (title, headings, prose, and an action-items checklist when people
// committed to things) → the plan lands in the empty page the client created,
// as a collaborator → a card in the thread names the page.
//
//   conversation-summarize:  trigger → read-history → plan → apply → publish
//
// The apply step is the shared `document:apply` — the same write path /doc,
// Transform and the call transcript use — so this pipeline owns exactly two
// things: how a conversation is read, and what the model is told about it.
import type { DescribedStepHandler } from '../pipeline/customStepHandlers';
import type { StepHandler, StepOutcome, PipelineNode } from 'distributed-core/applications/pipeline';
import { readChatHistory, type ChatHistoryMessage } from '../gateway/GatewayServiceClient';
import { DOCUMENT_APPLY_KIND } from './documentAgentPipeline';
import { postRunCard, runCardMetadata, runCardOutcomeOf, runCardOutputOf, str, type RunCardInput, type RunCardPostOutcome } from './chatRunCard';

export const CONVERSATION_SUMMARIZE_PIPELINE_ID = 'conversation-summarize';
export const CHAT_READ_HISTORY_KIND = 'chat:read-history';
/**
 * A read retries a connection failure or a timeout, never a refusal: the chat
 * service saying this person may not read the channel is the same answer the
 * second and third time. `retryUnknown: false` makes the executor retry only
 * what its classifier calls transient.
 */
export const READ_HISTORY_RETRY = { maxAttempts: 3, backoffMs: 1500, backoffMultiplier: 2, retryUnknown: false } as const;
export const SUMMARIZE_PUBLISH_KIND = 'summarize:publish';
export const DEFAULT_HISTORY_LIMIT = 200;

const PLAN_RETRY = { maxAttempts: 3, backoffMs: 2000, backoffMultiplier: 2, retryUnknown: true } as const;

/**
 * The conversation as the model reads it: one line per message, oldest first,
 * `[HH:MM] Name: text`. Run cards (the thread's own /doc, /summarize and
 * /diagram messages) and empty messages are not conversation and are skipped.
 */
export function conversationText(messages: readonly ChatHistoryMessage[]): { text: string; count: number } {
  const lines: string[] = [];
  const sorted = [...messages].sort((a, b) => String(a.timestamp ?? '').localeCompare(String(b.timestamp ?? '')));
  for (const m of sorted) {
    const text = typeof m.message === 'string' ? m.message.trim() : '';
    if (!text) continue;
    const md = (m.metadata ?? {}) as Record<string, unknown>;
    if (md.kind === 'agent-run') continue;
    const name = str(md.displayName) ?? str(m.userId) ?? 'Someone';
    const at = m.timestamp ? new Date(m.timestamp) : null;
    const hhmm = at && !Number.isNaN(at.getTime()) ? `${String(at.getHours()).padStart(2, '0')}:${String(at.getMinutes()).padStart(2, '0')}` : '--:--';
    lines.push(`[${hhmm}] ${name}: ${text}`);
  }
  return { text: lines.join('\n'), count: lines.length };
}

/** What the light model is told; the `plan` agent step interpolates it from the run context. */
export const CONVERSATION_SUMMARY_PROMPT = [
  'You turn a chat conversation into a short, well-structured page. Answer with ONLY a JSON array of operations, no prose, no code fence.',
  'Operations available, in the order they should appear:',
  '  {"op":"setTitle","text":"..."}                                — always first. COMPOSE a title from what the conversation is about, in a few words. Never use the channel name, a date, or the word "summary" alone.',
  '  {"op":"appendBlock","kind":"heading","level":2,"text":"..."}  — a section heading (Summary, Decisions, Open questions, …)',
  '  {"op":"appendBlock","kind":"paragraph","text":"..."}          — prose under the heading above it; one idea per paragraph',
  '  {"op":"appendMacro","macroName":"action-items","yaml":"items:\\n  - assignee: <name>\\n    status: pending\\n    text: <what they will do>\\n  - status: pending\\n    text: <unowned commitment>"}',
  '      — ONE checklist, under a heading "Action items", ONLY when the conversation contains commitments (someone said they will do something, or asked someone to). Keys per item, in this order: assignee (omit when nobody owns it), status (always pending), text. Names come from the speakers as written.',
  '  {"op":"skip","reason":"..."}                                  — the ONLY thing to return when the conversation is empty or has nothing worth a page',
  'Start with a "Summary" section of 2–4 sentences. Add "Decisions" and "Open questions" sections only when the conversation has some. Never invent facts, names or dates that are not in the conversation. Quote people by name when a point is attributable.',
  '',
  'Channel: {{ context.chatChannel }}',
  'Requested by: {{ context.requestedByName }}',
  'Conversation ({{ context.messageCount }} messages, oldest first):',
  '{{ context.conversationText }}',
].join('\n');

export function buildConversationSummarizeDefinition(model = process.env.DOCUMENT_AGENT_MODEL || 'haiku'): Record<string, unknown> {
  const pos = (x: number) => ({ x, y: 0 });
  const edge = (source: string, target: string) => ({ id: `${source}->${target}`, source, sourceHandle: 'out', target, targetHandle: 'in' });
  return {
    id: CONVERSATION_SUMMARIZE_PIPELINE_ID,
    name: 'Conversation summary',
    workType: 'conversations', kind: 'conversation', route: ['/summarize', 'Read', 'Plan', 'Write', 'Card'], origin: { kind: 'system' },
    description: 'A /summarize command from a chat: read the conversation, plan a page with a light model (action items as a checklist), write it as a collaborator, leave a card in the thread.',
    version: 1, status: 'published', publishedVersion: 1,
    nodes: [
      { id: 'trigger', type: 'trigger', position: pos(0), data: { type: 'trigger', triggerType: 'platform', label: '/summarize command' } },
      { id: 'read-history', type: CHAT_READ_HISTORY_KIND, position: pos(200), data: { type: CHAT_READ_HISTORY_KIND, label: 'Read the conversation', retryPolicy: READ_HISTORY_RETRY } },
      { id: 'plan', type: 'agent', position: pos(400), data: { type: 'agent', label: 'Plan the page (Haiku)', prompt: CONVERSATION_SUMMARY_PROMPT, model, allowedTools: [], maxTurns: 1, retryPolicy: PLAN_RETRY } },
      { id: 'apply', type: DOCUMENT_APPLY_KIND, position: pos(600), data: { type: DOCUMENT_APPLY_KIND, label: 'Write the page as a collaborator', opsFrom: 'steps.plan.text', agent: { userId: 'agent:haiku', displayName: 'Haiku' }, retryPolicy: { maxAttempts: 3, backoffMs: 1500, backoffMultiplier: 2, retryUnknown: true } } },
      { id: 'publish', type: SUMMARIZE_PUBLISH_KIND, position: pos(800), data: { type: SUMMARIZE_PUBLISH_KIND, label: 'Post the card to the conversation', retryPolicy: { maxAttempts: 3, backoffMs: 1500, backoffMultiplier: 2, retryUnknown: true } } },
    ],
    edges: [edge('trigger', 'read-history'), edge('read-history', 'plan'), edge('plan', 'apply'), edge('apply', 'publish')],
  };
}

/** Step 1: read the channel through the chat service and hand the model a transcript. */
export function createChatReadHistoryHandler(deps: { readHistory?: typeof readChatHistory } = {}): DescribedStepHandler {
  const read = deps.readHistory ?? readChatHistory;
  return {
    kind: CHAT_READ_HISTORY_KIND,
    description:
      'Reads recent messages from the conversation and flattens them into one block of text for a model to summarise. Capped at 1000 messages.',
    reads: 'chatChannel, and an optional limit.',
    emits:
      'conversationText and messageCount, interpolated by later steps as {{ context.conversationText }}.',
    async execute(_node: PipelineNode, ctx: Record<string, unknown>): Promise<StepOutcome> {
      const channel = str(ctx.chatChannel);
      if (!channel) return { status: 'failed', error: 'chatChannel missing from the run context' };
      const limit = typeof ctx.limit === 'number' && ctx.limit > 0 ? Math.min(Math.floor(ctx.limit), 1000) : DEFAULT_HISTORY_LIMIT;
      try {
        // Read AS the person who asked: the gateway then judges this read
        // exactly as it would their own browser's. As `agent:pipeline` every
        // direct message was refused — the pipeline is not one of its two
        // members — and a refusal read as a 4 s timeout, three times.
        const requester = str(ctx.requestedBy);
        const identity = requester && !requester.startsWith('agent:')
          ? { userId: requester, displayName: str(ctx.requestedByName) ?? requester, kind: 'person' as const }
          : undefined;
        const messages = await read({ channel, limit, ...(identity ? { identity } : {}) });
        const { text, count } = conversationText(messages);
        // Merged into the run context: `{{ context.conversationText }}` and `{{ context.messageCount }}`.
        return { status: 'completed', output: { conversationText: text || '(no messages)', messageCount: count, rawMessages: messages.length } };
      } catch (err) {
        return { status: 'failed', error: err instanceof Error ? err.message : String(err) };
      }
    },
  };
}

/** Last step: the card in the thread — `/summarize <title> — <n> messages` — carrying the page's id. */
export function createSummarizePublishHandler(deps: { postCard?: (input: RunCardInput) => Promise<boolean | RunCardPostOutcome> } = {}): DescribedStepHandler {
  const post = deps.postCard ?? postRunCard;
  return {
    kind: SUMMARIZE_PUBLISH_KIND,
    description:
      'Posts the finished conversation summary back to the channel as a run card, saying how many messages went into it — or that there was nothing to summarise.',
    reads: 'chatChannel, the summary title and the message count from the steps before it.',
    emits:
      'Whether a card was posted, the card metadata (returned either way, since the client posts it itself when clientPostsCard is set), the title and the message count.',
    async execute(_node: PipelineNode, ctx: Record<string, unknown>, helpers): Promise<StepOutcome> {
      const steps = (ctx.steps ?? {}) as Record<string, Record<string, unknown> | undefined>;
      const title = str(steps.apply?.title) ?? 'Conversation summary';
      const count = typeof ctx.messageCount === 'number' ? ctx.messageCount : 0;
      const applied = typeof steps.apply?.applied === 'number' ? steps.apply.applied : 0;
      const instruction = applied > 0 ? `${title} — ${count} message${count === 1 ? '' : 's'}` : `Nothing to summarise — ${count} message${count === 1 ? '' : 's'}`;
      // `context: ctx` lets the post step down when the client posted the card (clientPostsCard); the metadata is returned either way.
      const cardInput = {
        channel: str(ctx.chatChannel), runId: helpers.runId, pipelineId: CONVERSATION_SUMMARIZE_PIPELINE_ID,
        command: '/summarize', instruction,
        extra: { documentId: ctx.documentId, title, messageCount: count, applied },
        context: ctx,
      };
      // A refused post (a DM that would not admit the sender) is on the step's output as `cardError`, not swallowed.
      const cardOutcome = runCardOutcomeOf(await post(cardInput));
      // The card's 'Open' target — the run-result convention (src/pipeline/runResult.ts).
      const resultDocumentId = typeof ctx.documentId === 'string' && ctx.documentId ? ctx.documentId : undefined;
      const result = resultDocumentId ? { kind: 'document' as const, documentId: resultDocumentId } : undefined;
      return { status: 'completed', output: { ...runCardOutputOf(cardOutcome), card: runCardMetadata(cardInput), title, messageCount: count, applied, ...(result ? { result } : {}) } };
    },
  };
}

export function buildConversationSummarizeHandlers(): Map<string, StepHandler> {
  const m = new Map<string, StepHandler>();
  for (const h of [createChatReadHistoryHandler(), createSummarizePublishHandler()]) m.set(h.kind, h);
  return m;
}
