// The /agent grammar, the planner's contract with itself, and the graph the
// plan becomes. All three are pure, so none of this needs a bridge or a model.
import {
  CHAT_AGENT_LOOP_PIPELINE_ID, DEFAULT_CONTEXT_BUDGET_TOKENS, LOOP_STEP_MAX_TURNS, LOOP_STEP_TIMEOUT_MS,
  MAX_CONTEXT_BUDGET_TOKENS, MIN_CONTEXT_BUDGET_TOKENS,
  agentLoopNodeId, buildAgentLoopDefinition, buildAgentLoopRequest, extractJsonObject,
  fallbackPlan, parseAgentLoopCommand, planAgentLoop, plannerPrompt, validateLoopPlan,
  type AgentLoopRequest, type LoopPlan, type LoopStep,
} from '../agentLoopPlanner';
import type { AgentClient, AgentEvent } from '../../pipeline/agent/types';

/** A planner that says exactly what the test tells it to. */
function scriptedPlanner(text: string | Error): AgentClient {
  return {
    provider: 'test',
    async *run(): AsyncIterable<AgentEvent> {
      if (text instanceof Error) throw text;
      yield { type: 'done', text, turns: 1, tokensIn: 0, tokensOut: 0, model: 'haiku', stopReason: 'complete', toolCalls: [] };
    },
  };
}

const request = (over: Partial<AgentLoopRequest> = {}): AgentLoopRequest => ({
  instruction: 'create a meeting with the X team',
  hints: [],
  contextBudgetTokens: DEFAULT_CONTEXT_BUDGET_TOKENS,
  model: 'sonnet',
  chatChannel: 'room:x',
  requestedBy: 'dev-frank',
  ...over,
});

const goodStep = (id: string, over: Partial<LoopStep> = {}): Record<string, unknown> => ({
  id, title: `Step ${id}`, kind: 'research', prompt: `do ${id}`, dependsOn: [], done: `${id} is written to notes.md`, ...over,
});

describe('parseAgentLoopCommand', () => {
  it('reads the example command: leading hints, trailing budget, the rest is the instruction', () => {
    expect(parseAgentLoopCommand('/meeting /agenda create a meeting with X team and create a ppt 200k')).toEqual({
      hints: ['meeting', 'agenda'],
      instruction: 'create a meeting with X team and create a ppt',
      contextBudgetTokens: 200_000,
      model: 'sonnet',
    });
  });

  it('takes --model and the budget from the end in either order', () => {
    expect(parseAgentLoopCommand('write the deck --model opus 50k')).toMatchObject({ model: 'opus', contextBudgetTokens: 50_000, instruction: 'write the deck' });
    expect(parseAgentLoopCommand('write the deck 50k --model opus')).toMatchObject({ model: 'opus', contextBudgetTokens: 50_000, instruction: 'write the deck' });
    expect(parseAgentLoopCommand('write the deck --model=haiku')).toMatchObject({ model: 'haiku', instruction: 'write the deck' });
  });

  it('defaults to 200k of sonnet with no hints', () => {
    expect(parseAgentLoopCommand('just do it')).toEqual({
      hints: [], instruction: 'just do it', contextBudgetTokens: DEFAULT_CONTEXT_BUDGET_TOKENS, model: 'sonnet',
    });
  });

  it('clamps a budget nobody meant', () => {
    expect(parseAgentLoopCommand('x 1k').contextBudgetTokens).toBe(MIN_CONTEXT_BUDGET_TOKENS);
    expect(parseAgentLoopCommand('x 9000k').contextBudgetTokens).toBe(MAX_CONTEXT_BUDGET_TOKENS);
  });

  it('only treats a slash as a hint at the START — a slash mid-sentence is prose', () => {
    expect(parseAgentLoopCommand('/research compare either/or with https://x.dev/docs')).toEqual({
      hints: ['research'],
      instruction: 'compare either/or with https://x.dev/docs',
      contextBudgetTokens: DEFAULT_CONTEXT_BUDGET_TOKENS,
      model: 'sonnet',
    });
  });

  it('lets the client fill only the gaps — whatever the text said wins', () => {
    // The text said 50k and nothing about the model.
    expect(parseAgentLoopCommand('do it 50k', { contextBudgetTokens: 999_000, model: 'opus', hints: ['deck'] }))
      .toEqual({ hints: ['deck'], instruction: 'do it', contextBudgetTokens: 50_000, model: 'opus' });
  });

  it('caps a very long instruction rather than passing it through', () => {
    expect(parseAgentLoopCommand('x'.repeat(9_000)).instruction).toHaveLength(4_000);
  });
});

describe('the planner prompt', () => {
  it('asks for JSON only, names the hints, and asks for an acceptance sentence', () => {
    const prompt = plannerPrompt(request({ hints: ['meeting', 'agenda'] }));
    expect(prompt).toContain('ONLY a JSON object');
    expect(prompt).toContain('"meeting", "agenda"');
    expect(prompt).toContain('"done" is ONE sentence naming the acceptance');
    expect(prompt).toContain('create a meeting with the X team');
  });

  it('says plainly when there were no hints, rather than leaving a dangling list', () => {
    expect(plannerPrompt(request())).toContain('no step hints');
  });
});

describe('validateLoopPlan', () => {
  it('accepts a well-formed plan and normalises an unknown kind to other', () => {
    const out = validateLoopPlan({ steps: [goodStep('s1'), goodStep('s2', { kind: 'nonsense', dependsOn: ['s1'] })] });
    expect(out).toMatchObject({ ok: true });
    expect((out as { steps: LoopStep[] }).steps.map((s) => s.kind)).toEqual(['research', 'other']);
  });

  it.each([
    ['not an object', 'nope'],
    ['no steps array', { steps: 'x' }],
    ['an empty plan', { steps: [] }],
    ['more steps than allowed', { steps: [1, 2, 3, 4, 5, 6, 7].map((i) => goodStep(`s${i}`)) }],
    ['ids out of order', { steps: [goodStep('s2')] }],
    ['a step with no title', { steps: [goodStep('s1', { title: '' })] }],
    ['a step with no acceptance sentence', { steps: [goodStep('s1', { done: '  ' })] }],
    ['a dependency on a LATER step', { steps: [goodStep('s1', { dependsOn: ['s2'] }), goodStep('s2')] }],
    ['a dependency on itself', { steps: [goodStep('s1', { dependsOn: ['s1'] })] }],
  ])('rejects %s', (_name, value) => {
    expect(validateLoopPlan(value)).toMatchObject({ ok: false });
  });
});

describe('extractJsonObject', () => {
  it('survives a code fence and a chatty preamble', () => {
    expect(extractJsonObject('Sure! ```json\n{"steps":[]}\n```')).toEqual({ steps: [] });
    expect(extractJsonObject('not json at all')).toBeNull();
  });
});

describe('planAgentLoop', () => {
  it('uses a valid plan and says it came from the planner', async () => {
    const plan = await planAgentLoop(request(), scriptedPlanner(JSON.stringify({ steps: [goodStep('s1'), goodStep('s2', { dependsOn: ['s1'] })] })));
    expect(plan.source).toBe('planner');
    expect(plan.steps.map((s) => s.id)).toEqual(['s1', 's2']);
    expect(plan.notice).toBeUndefined();
  });

  it.each([
    ['there is no planner client at all', null],
    ['the planner throws', scriptedPlanner(new Error('claude is not installed'))],
    ['the planner answers with nothing', scriptedPlanner('   ')],
    ['the planner answers with prose', scriptedPlanner('I think we should start by...')],
    ['the planner answers with an invalid plan', scriptedPlanner('{"steps":[{"id":"s9"}]}')],
  ])('falls back to one visible step when %s', async (_name, client) => {
    const plan = await planAgentLoop(request(), client as AgentClient | null);
    expect(plan.source).toBe('fallback');
    expect(plan.steps).toHaveLength(1);
    expect(plan.steps[0].id).toBe('s1');
    expect(plan.steps[0].prompt).toBe('create a meeting with the X team');
    // The reason is never swallowed — the card shows it.
    expect(plan.notice).toMatch(/single step/);
  });
});

describe('buildAgentLoopDefinition', () => {
  const plan: LoopPlan = {
    source: 'planner',
    steps: [
      { id: 's1', title: 'Book the meeting', kind: 'meeting', prompt: 'book it', dependsOn: [], done: 'the invite exists' },
      { id: 's2', title: 'Draft the agenda', kind: 'agenda', prompt: 'draft it', dependsOn: ['s1'], done: 'agenda.md is written' },
      { id: 's3', title: 'Build the deck', kind: 'deck', prompt: 'build it', dependsOn: ['s1'], done: 'deck.pptx exists' },
    ],
  };
  const def = buildAgentLoopDefinition(plan, request({ model: 'opus', contextBudgetTokens: 150_000 }));
  const nodes = def.nodes as Array<Record<string, unknown>>;
  const edges = def.edges as Array<Record<string, string>>;

  it('is the chat-agent-loop pipeline, so the run row and GET /:runId agree on the key', () => {
    expect(def.id).toBe(CHAT_AGENT_LOOP_PIPELINE_ID);
  });

  it('gives every step a node id of agent:<stepId> and the step title', () => {
    expect(nodes.map((n) => n.id)).toEqual(['trigger', 'agent:s1', 'agent:s2', 'agent:s3']);
    expect(nodes.slice(1).map((n) => (n.data as { label: string }).label))
      .toEqual(['Book the meeting', 'Draft the agenda', 'Build the deck']);
    expect(agentLoopNodeId('s4')).toBe('agent:s4');
  });

  it('carries the runtime, the budget and the bounds onto each agent node', () => {
    for (const node of nodes.slice(1)) {
      expect(node.type).toBe('agent');
      expect(node.data).toMatchObject({
        type: 'agent', runtime: 'devbox', model: 'opus', contextBudgetTokens: 150_000,
        maxTurns: LOOP_STEP_MAX_TURNS, timeoutMs: LOOP_STEP_TIMEOUT_MS,
        allowedTools: ['Read', 'Write', 'Edit', 'Bash', 'Glob', 'Grep'],
      });
      // Every system pipeline must declare one, and this definition is registered as one.
      expect((node.data as { retryPolicy: { maxAttempts: number } }).retryPolicy.maxAttempts).toBeGreaterThanOrEqual(2);
    }
  });

  it('appends the acceptance sentence to the prompt, which is what ends the loop', () => {
    expect((nodes[2].data as { prompt: string }).prompt).toBe('draft it\n\nAcceptance: agenda.md is written');
  });

  it('records the plan step on the node so the graph can name what it is doing', () => {
    expect((nodes[3].data as { loop: unknown }).loop).toEqual({ kind: 'deck', stepId: 's3', dependsOn: ['s1'] });
  });

  it('chains in topological order — s3 depends only on s1, but still runs after s2', () => {
    expect(edges.map((e) => `${e.source}->${e.target}`)).toEqual([
      'trigger->agent:s1', 'agent:s1->agent:s2', 'agent:s2->agent:s3',
    ]);
    expect(edges.every((e) => e.sourceHandle === 'out' && e.targetHandle === 'in')).toBe(true);
  });

  it('builds a one-node graph from a fallback plan without a dangling edge', () => {
    const fallback = buildAgentLoopDefinition(fallbackPlan(request(), 'Planner unavailable · single step'), request());
    expect((fallback.nodes as unknown[]).length).toBe(2);
    expect(fallback.edges).toEqual([{ id: 'e1', source: 'trigger', sourceHandle: 'out', target: 'agent:s1', targetHandle: 'in' }]);
  });
});

describe('buildAgentLoopRequest', () => {
  it('carries who asked and where, and omits what was not given', () => {
    const out = buildAgentLoopRequest(parseAgentLoopCommand('/deck do it 40k'), { chatChannel: 'room:y', requestedBy: 'u1' });
    expect(out).toEqual({ instruction: 'do it', hints: ['deck'], contextBudgetTokens: 40_000, model: 'sonnet', chatChannel: 'room:y', requestedBy: 'u1' });
  });
});
