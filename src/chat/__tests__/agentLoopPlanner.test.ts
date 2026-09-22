// The /agent grammar, the planner's contract with itself, and the graph the
// plan becomes. All three are pure, so none of this needs a bridge or a model.
import {
  CHAT_AGENT_LOOP_PIPELINE_ID, DEFAULT_CONTEXT_BUDGET_TOKENS, LOOP_STEP_MAX_TURNS, LOOP_STEP_TIMEOUT_MS,
  MAX_CONTEXT_BUDGET_TOKENS, MIN_CONTEXT_BUDGET_TOKENS,
  agentLoopNodeId, buildAgentLoopDefinition, buildAgentLoopRequest, extractJsonObject,
  PLANNER_MAX_TURNS, PLANNER_TIMEOUT_MS,
  fallbackPlan, parseAgentLoopCommand, planAgentLoop, plannerPrompt, plannerStopNotice, validateLoopPlan,
  type AgentLoopRequest, type LoopPlan, type LoopStep,
} from '../agentLoopPlanner';
import type { AgentClient, AgentEvent, AgentStopReason } from '../../pipeline/agent/types';

/** A planner that says exactly what the test tells it to. */
function scriptedPlanner(text: string | Error, stopReason: AgentStopReason = 'complete'): AgentClient {
  return {
    provider: 'test',
    async *run(): AsyncIterable<AgentEvent> {
      if (text instanceof Error) throw text;
      yield { type: 'done', text, turns: 1, tokensIn: 0, tokensOut: 0, model: 'haiku', stopReason, toolCalls: [] };
    },
  };
}

/** A planner that yields nothing at all — the process died before `done`. */
const silentPlanner: AgentClient = { provider: 'test', async *run(): AsyncIterable<AgentEvent> { /* no events */ } };

const request = (over: Partial<AgentLoopRequest> = {}): AgentLoopRequest => ({
  instruction: 'create a meeting with the X team',
  hints: [],
  contextBudgetTokens: DEFAULT_CONTEXT_BUDGET_TOKENS,
  model: 'sonnet',
  chatChannel: 'room:x',
  requestedBy: 'dev-frank',
  ...over,
});

// Deliberately loose: several cases feed the validator a step that is NOT a
// valid LoopStep (an unknown kind, a missing title), which is the point.
const goodStep = (id: string, over: Record<string, unknown> = {}): Record<string, unknown> => ({
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


describe('the planner\'s bounds, which live runs actually hit', () => {
  it('asks for more than one turn and more than 30s', async () => {
    // Measured in the deployed pod: the real prompt takes 21-24s wall clock,
    // dominated by CLI start-up. A 30s cap lost that race about half the time.
    expect(PLANNER_TIMEOUT_MS).toBeGreaterThanOrEqual(60_000);
    // maxTurns 1 killed the process when a thinking block and its text landed
    // under different message ids. Toolless means it cannot loop anyway.
    expect(PLANNER_MAX_TURNS).toBeGreaterThan(1);
  });

  it('passes those bounds to the client, toolless and on haiku', async () => {
    const seen: Array<Record<string, unknown>> = [];
    const spy: AgentClient = {
      provider: 'test',
      async *run(_p: string, opts): AsyncIterable<AgentEvent> {
        seen.push(opts as unknown as Record<string, unknown>);
        yield { type: 'done', text: '{"steps":[]}', turns: 1, tokensIn: 0, tokensOut: 0, model: 'haiku', stopReason: 'complete', toolCalls: [] };
      },
    };
    await planAgentLoop(request(), spy);
    expect(seen[0]).toMatchObject({ model: 'haiku', allowedTools: [], maxTurns: PLANNER_MAX_TURNS, timeoutMs: PLANNER_TIMEOUT_MS });
  });
});

describe('a planner that stops without answering', () => {
  // The defect this replaces: every bounded stop read as "answered with
  // nothing", so a timeout looked like a model that had no opinion.
  it.each([
    ['timeout', /timed out after \d+s/],
    ['max_turns', /turn ceiling/],
    ['cancelled', /cancelled/],
    ['error', /reported an error/],
  ])('names %s in the notice instead of "answered with nothing"', async (stopReason, expected) => {
    const plan = await planAgentLoop(request(), scriptedPlanner('', stopReason as AgentStopReason));
    expect(plan.source).toBe('fallback');
    expect(plan.notice).toMatch(expected);
    expect(plan.notice).not.toMatch(/answered with nothing/);
  });

  it('says the planner exited without answering when no done event arrives at all', async () => {
    const plan = await planAgentLoop(request(), silentPlanner);
    expect(plan.notice).toMatch(/exited without answering/);
  });

  it('still says "answered with nothing" only when it genuinely completed empty', async () => {
    expect((await planAgentLoop(request(), scriptedPlanner('   ', 'complete'))).notice).toMatch(/answered with nothing/);
    expect(plannerStopNotice('complete')).toMatch(/answered with nothing/);
  });
});

describe('the answer the real CLI gives', () => {
  // Captured from `claude -p --output-format stream-json --tools "" --model
  // haiku` in the deployed pod: the JSON arrives inside a ```json fence even
  // though the prompt says not to fence it.
  const FENCED = [
    '```json',
    '{',
    '  "steps": [',
    '    {"id": "s1", "title": "Schedule meeting with X team", "kind": "meeting", "prompt": "Schedule a meeting with the X team", "dependsOn": [], "done": "Meeting is scheduled with X team and calendar invites sent"},',
    '    {"id": "s2", "title": "Create PowerPoint presentation", "kind": "deck", "prompt": "Build the deck", "dependsOn": ["s1"], "done": "deck.pptx exists in the workspace"}',
    '  ]',
    '}',
    '```',
  ].join('\n');

  it('parses through the fence into a real two-step plan', async () => {
    const plan = await planAgentLoop(request(), scriptedPlanner(FENCED));
    expect(plan.source).toBe('planner');
    expect(plan.steps.map((s) => [s.id, s.kind, s.title])).toEqual([
      ['s1', 'meeting', 'Schedule meeting with X team'],
      ['s2', 'deck', 'Create PowerPoint presentation'],
    ]);
    expect(plan.steps[1].dependsOn).toEqual(['s1']);
  });

  it('turns that plan into two agent nodes the person can see', () => {
    const def = buildAgentLoopDefinition({ source: 'planner', steps: [
      { id: 's1', title: 'Schedule meeting with X team', kind: 'meeting', prompt: 'a', dependsOn: [], done: 'd' },
      { id: 's2', title: 'Create PowerPoint presentation', kind: 'deck', prompt: 'b', dependsOn: ['s1'], done: 'e' },
    ] }, request());
    expect((def.nodes as Array<{ id: string }>).map((n) => n.id)).toEqual(['trigger', 'agent:s1', 'agent:s2']);
  });
});
