// /summarize: the conversation as the model reads it, and the run that writes the page.
import { conversationText, buildConversationSummarizeDefinition, createChatReadHistoryHandler, createSummarizePublishHandler, CHAT_READ_HISTORY_KIND, SUMMARIZE_PUBLISH_KIND, CONVERSATION_SUMMARIZE_PIPELINE_ID } from '../conversationSummarizePipeline';
import { DOCUMENT_APPLY_KIND, opsFromContext } from '../documentAgentPipeline';
import { isChatFrame } from '../../gateway/GatewayServiceClient';

const at = (h: number, m: number) => { const d = new Date(2026, 8, 9, h, m, 0); return d.toISOString(); };

describe('conversationText', () => {
  it('renders one [HH:MM] Name: text line per message, oldest first', () => {
    const { text, count } = conversationText([
      { id: '2', userId: 'u2', message: 'I will ship it Friday', metadata: { displayName: 'Ana' }, timestamp: at(9, 12) },
      { id: '1', userId: 'u1', message: 'Can someone own the rollout?', metadata: { displayName: 'Grace' }, timestamp: at(9, 5) },
    ]);
    expect(text).toBe('[09:05] Grace: Can someone own the rollout?\n[09:12] Ana: I will ship it Friday');
    expect(count).toBe(2);
  });
  it('skips agent-run cards and empty messages, and falls back to the user id for a name', () => {
    const { text, count } = conversationText([
      { id: '1', userId: 'u1', message: '/doc add a title', metadata: { kind: 'agent-run', runId: 'r1', displayName: 'Grace' }, timestamp: at(9, 0) },
      { id: '2', userId: 'u1', message: '   ', metadata: {}, timestamp: at(9, 1) },
      { id: '3', userId: 'dev-bob', message: 'hello', timestamp: at(9, 2) },
    ]);
    expect(text).toBe('[09:02] dev-bob: hello');
    expect(count).toBe(1);
  });
  it('is empty for an empty channel', () => {
    expect(conversationText([])).toEqual({ text: '', count: 0 });
  });
});

describe('the chat frame matcher', () => {
  it('accepts the nested frames the gateway emits and the flat spelling', () => {
    expect(isChatFrame({ type: 'chat', action: 'history', channel: 'room:x' }, 'history', 'room:x')).toBe(true);
    expect(isChatFrame({ type: 'chat:history', channel: 'room:x' }, 'history', 'room:x')).toBe(true);
    expect(isChatFrame({ type: 'chat', action: 'joined', channel: 'room:y' }, 'joined', 'room:x')).toBe(false);
    expect(isChatFrame({ type: 'chat', action: 'message', channel: 'room:x' }, 'history')).toBe(false);
  });
});

describe('the conversation-summarize definition', () => {
  const def = buildConversationSummarizeDefinition('haiku') as { id: string; nodes: Array<{ id: string; type: string; data: Record<string, unknown> }>; edges: Array<{ source: string; target: string }> };
  it('is read-history → plan → apply → publish, writing through the shared document:apply step as Haiku', () => {
    expect(def.id).toBe(CONVERSATION_SUMMARIZE_PIPELINE_ID);
    expect(def.edges.map((e) => `${e.source}->${e.target}`)).toEqual(['trigger->read-history', 'read-history->plan', 'plan->apply', 'apply->publish']);
    expect(def.nodes.find((n) => n.id === 'read-history')!.type).toBe(CHAT_READ_HISTORY_KIND);
    const apply = def.nodes.find((n) => n.id === 'apply')!;
    expect(apply.type).toBe(DOCUMENT_APPLY_KIND);
    expect(apply.data).toMatchObject({ opsFrom: 'steps.plan.text', agent: { userId: 'agent:haiku', displayName: 'Haiku' } });
    expect(def.nodes.find((n) => n.id === 'publish')!.type).toBe(SUMMARIZE_PUBLISH_KIND);
  });
  it('plans with a light agent step that retries and is told the conversation', () => {
    const plan = def.nodes.find((n) => n.id === 'plan')!;
    expect(plan.type).toBe('agent');
    expect(plan.data).toMatchObject({ model: 'haiku', allowedTools: [], maxTurns: 1, retryPolicy: { maxAttempts: 3, backoffMs: 2000, backoffMultiplier: 2 } });
    const prompt = String(plan.data.prompt);
    expect(prompt).toContain('{{ context.conversationText }}');
    expect(prompt).toContain('{{ context.messageCount }}');
    expect(prompt).toContain('"macroName":"action-items"');
  });
  it('the apply step resolves the plan text into ops', () => {
    const apply = def.nodes.find((n) => n.id === 'apply')!;
    const { ops } = opsFromContext({ steps: { plan: { text: '[{"op":"setTitle","text":"Rollout plan"},{"op":"appendMacro","macroName":"action-items","yaml":"items:\\n  - status: pending\\n    text: ship"}]' } } }, apply.data as never);
    expect(ops).toEqual([{ op: 'setTitle', text: 'Rollout plan' }, { op: 'appendMacro', macroName: 'action-items', yaml: 'items:\n  - status: pending\n    text: ship' }]);
  });
});

describe('the read-history step', () => {
  it('reads the channel with the run’s limit and hands the model the rendered conversation', async () => {
    const readHistory = jest.fn(async () => [
      { id: '1', userId: 'u1', message: 'hi', metadata: { displayName: 'Grace' }, timestamp: at(10, 0) },
      { id: '2', userId: 'u2', message: '/summarize', metadata: { kind: 'agent-run', runId: 'r' }, timestamp: at(10, 1) },
    ]);
    const h = createChatReadHistoryHandler({ readHistory });
    const out = await h.execute({ id: 'read-history', type: CHAT_READ_HISTORY_KIND, data: {} } as never, { chatChannel: 'room:x', limit: 50 }, { runId: 'run-1', emit: async () => undefined } as never);
    expect(readHistory).toHaveBeenCalledWith({ channel: 'room:x', limit: 50 });
    expect(out).toEqual({ status: 'completed', output: { conversationText: '[10:00] Grace: hi', messageCount: 1, rawMessages: 2 } });
  });
  it('reads as the person who asked, so a direct message they are in is readable', async () => {
    const readHistory = jest.fn(async () => []);
    const h = createChatReadHistoryHandler({ readHistory });
    await h.execute({ id: 'read-history', type: CHAT_READ_HISTORY_KIND, data: {} } as never,
      { chatChannel: 'chat:dm:dev-connor:dev-frank', requestedBy: 'dev-connor', requestedByName: 'Connor' }, { runId: 'r', emit: async () => undefined } as never);
    expect(readHistory).toHaveBeenCalledWith(expect.objectContaining({ identity: { userId: 'dev-connor', displayName: 'Connor', kind: 'person' } }));
  });
  it('never reads as an agent requester — the pipeline identity is the fallback', async () => {
    const readHistory = jest.fn(async () => []);
    await createChatReadHistoryHandler({ readHistory }).execute({ id: 'read-history', type: CHAT_READ_HISTORY_KIND, data: {} } as never,
      { chatChannel: 'room:x', requestedBy: 'agent:planner' }, { runId: 'r', emit: async () => undefined } as never);
    expect(readHistory).toHaveBeenCalledWith({ channel: 'room:x', limit: expect.any(Number) });
  });
  it('retries a timeout or a dropped connection, never a refusal', () => {
    const node = (buildConversationSummarizeDefinition().nodes as Array<{ id: string }>).find((n) => n.id === 'read-history') as unknown as { data: { retryPolicy: { retryUnknown: boolean; maxAttempts: number } } };
    expect(node.data.retryPolicy).toMatchObject({ retryUnknown: false, maxAttempts: 3 });
  });
  it('fails without a channel', async () => {
    const out = await createChatReadHistoryHandler({ readHistory: jest.fn() }).execute({ id: 'r', type: CHAT_READ_HISTORY_KIND, data: {} } as never, {}, { runId: 'x', emit: async () => undefined } as never);
    expect(out.status).toBe('failed');
  });
});

describe('the publish step', () => {
  it('posts "/summarize <title> — <n> messages" with the page id on the card', async () => {
    const postCard = jest.fn(async () => true);
    const h = createSummarizePublishHandler({ postCard });
    const out = await h.execute({ id: 'publish', type: SUMMARIZE_PUBLISH_KIND, data: {} } as never,
      { chatChannel: 'room:x', documentId: 'doc-1', messageCount: 12, steps: { apply: { title: 'Rollout plan', applied: 4 } } },
      { runId: 'run-9', emit: async () => undefined } as never);
    expect(postCard).toHaveBeenCalledWith(expect.objectContaining({ channel: 'room:x', runId: 'run-9', pipelineId: CONVERSATION_SUMMARIZE_PIPELINE_ID, command: '/summarize', instruction: 'Rollout plan — 12 messages', extra: expect.objectContaining({ documentId: 'doc-1' }) }));
    expect(out).toEqual({ status: 'completed', output: { posted: true, card: expect.objectContaining({ kind: 'agent-run', runId: 'run-9', command: '/summarize', documentId: 'doc-1' }), title: 'Rollout plan', messageCount: 12, applied: 4, result: { kind: 'document', documentId: 'doc-1' } } });
    // The run context rides along so the post can step down when the client posted the card.
    expect((postCard.mock.calls[0] as unknown[])[0]).toMatchObject({ context: { chatChannel: 'room:x' } });
  });
});
