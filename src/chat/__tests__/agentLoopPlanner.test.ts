// The /agent grammar, the planner's contract with itself, and the graph the
// plan becomes. All three are pure, so none of this needs a bridge or a model.
import {
  CHAT_AGENT_LOOP_PIPELINE_ID, DEFAULT_CONTEXT_BUDGET_TOKENS, LOOP_STEP_MAX_TURNS, LOOP_STEP_TIMEOUT_MS,
  MAX_CONTEXT_BUDGET_TOKENS, MIN_CONTEXT_BUDGET_TOKENS,
  agentLoopNodeId, buildAgentLoopDefinition, buildAgentLoopRequest, extractJsonObject,
  PLANNER_MAX_TURNS, PLANNER_TIMEOUT_MS,
  fallbackPlan, parseAgentLoopCommand, planAgentLoop, plannerPrompt, plannerStopNotice,
  isSilentCliFailure, validateLoopPlan, wantsDeckFixture,
  type AgentLoopRequest, type LoopPlan, type LoopStep,
} from '../agentLoopPlanner';
import type { AgentClient, AgentEvent, AgentStopReason } from '../../pipeline/agent/types';
import { validateDeckSpec } from '../../deck/DeckSpec';
import { lastStderrLines } from '../../pipeline/agent/ClaudeCodeAgentClient';
import { describeClaudeCliFailure } from '../../pipeline/claudeCliErrors';

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

  it('gives every step its own node, and a capability step the node that does the work', () => {
    // s1 is a meeting step (one real node), s2 an agenda step and s3 a deck
    // step (each a draft node plus the node that produces the thing).
    expect(nodes.map((n) => n.id)).toEqual([
      'trigger', 'meeting:create:s1', 'agent:s2', 'agenda:write:s2', 'agent:s3', 'deck:s3',
    ]);
    expect(nodes.slice(1).map((n) => (n.data as { label: string }).label)).toEqual([
      'Book the meeting',
      'Draft the agenda for Draft the agenda',
      'Write Draft the agenda as a page',
      'Draft slides for Build the deck',
      'Render Build the deck (.pptx)',
    ]);
    expect(agentLoopNodeId('s4')).toBe('agent:s4');
  });

  it('carries the runtime, the budget and the bounds onto each agent node', () => {
    for (const node of nodes.filter((n) => n.type === 'agent')) {
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
    // An ordinary step: the ones with a capability behind them carry that
    // capability's own prompt instead.
    const ordinary = buildAgentLoopDefinition(
      { source: 'planner', steps: [{ id: 's1', title: 'Send it round', kind: 'other', prompt: 'send it', dependsOn: [], done: 'everyone has it' }] } as LoopPlan,
      request(),
    ).nodes as Array<Record<string, unknown>>;
    expect((ordinary[1].data as { prompt: string }).prompt).toBe('send it\n\nAcceptance: everyone has it');
  });

  it('gives a deck step its own drafting prompt instead of the generic one', () => {
    expect((nodes[4].data as { prompt: string }).prompt).toContain('You draft a presentation');
  });

  it('gives an agenda step its own drafting prompt, and a node that writes the page', () => {
    expect((nodes[2].data as { prompt: string }).prompt).toContain('You draft a meeting agenda');
    expect(nodes[3].type).toBe('agenda:write');
    expect((nodes[3].data as { agendaFrom: string }).agendaFrom).toBe('$.steps.agent:s2.text');
  });

  it('gives a meeting step the node that creates the room', () => {
    expect(nodes[1].type).toBe('meeting:create');
    expect((nodes[1].data as { title: string }).title).toBe('Book the meeting');
  });

  it('records the plan step on the node so the graph can name what it is doing', () => {
    expect((nodes[4].data as { loop: unknown }).loop).toEqual({ kind: 'deck', stepId: 's3', role: 'draft', dependsOn: ['s1'] });
    expect((nodes[5].data as { loop: unknown }).loop).toEqual({ kind: 'deck', stepId: 's3', role: 'render' });
    expect((nodes[1].data as { loop: unknown }).loop).toEqual({ kind: 'meeting', stepId: 's1' });
    expect((nodes[3].data as { loop: unknown }).loop).toEqual({ kind: 'agenda', stepId: 's2', role: 'write' });
  });

  it('chains in topological order — s3 depends only on s1, but still runs after s2', () => {
    expect(edges.map((e) => `${e.source}->${e.target}`)).toEqual([
      'trigger->meeting:create:s1', 'meeting:create:s1->agent:s2', 'agent:s2->agenda:write:s2',
      'agenda:write:s2->agent:s3', 'agent:s3->deck:s3',
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

  it('turns that plan into the nodes the person can see', () => {
    const def = buildAgentLoopDefinition({ source: 'planner', steps: [
      { id: 's1', title: 'Schedule meeting with X team', kind: 'meeting', prompt: 'a', dependsOn: [], done: 'd' },
      { id: 's2', title: 'Create PowerPoint presentation', kind: 'deck', prompt: 'b', dependsOn: ['s1'], done: 'e' },
    ] }, request());
    // The meeting step is the node that creates the room; the deck step is a
    // draft node and a render node.
    expect((def.nodes as Array<{ id: string }>).map((n) => n.id)).toEqual(['trigger', 'meeting:create:s1', 'agent:s2', 'deck:s2']);
  });
});

describe('a deck step becomes two nodes', () => {
  const deckPlan: LoopPlan = {
    source: 'planner',
    steps: [
      { id: 's1', title: 'Gather board work', kind: 'research', prompt: 'gather', dependsOn: [], done: 'notes.md exists' },
      { id: 's2', title: 'Platform review deck', kind: 'deck', prompt: 'build the deck', dependsOn: ['s1'], done: 'deck.pptx exists' },
      { id: 's3', title: 'Send it round', kind: 'other', prompt: 'send', dependsOn: ['s2'], done: 'sent' },
    ],
  };
  const req = request({
    model: 'opus',
    sources: [{ documentId: 'doc-1', title: 'Board work' }, { documentId: 'rec-1', title: 'Team sync', kind: 'transcript' }],
  });
  const def = buildAgentLoopDefinition(deckPlan, req);
  const nodes = def.nodes as Array<Record<string, unknown>>;
  const edges = def.edges as Array<Record<string, string>>;
  const data = (id: string) => nodes.find((n) => n.id === id)!.data as Record<string, unknown>;

  it('adds a render node after the draft, leaving every other step alone', () => {
    expect(nodes.map((n) => n.id)).toEqual(['trigger', 'agent:s1', 'agent:s2', 'deck:s2', 'agent:s3']);
    expect(nodes.find((n) => n.id === 'deck:s2')!.type).toBe('deck');
  });

  it('titles them the way the brief names them', () => {
    expect(data('agent:s2').label).toBe('Draft slides for Platform review deck');
    expect(data('deck:s2').label).toBe('Render Platform review deck (.pptx)');
  });

  it('runs the draft on the devbox like every other step', () => {
    expect(data('agent:s2')).toMatchObject({ type: 'agent', runtime: 'devbox', model: 'opus' });
  });

  it('points the render at the draft step\'s text, and names no document so one is created', () => {
    expect(data('deck:s2')).toMatchObject({ specFrom: '$.steps.agent:s2.text', title: 'Platform review deck' });
    expect(data('deck:s2')).not.toHaveProperty('documentId');
  });

  it('chains draft → render → next step', () => {
    expect(edges.map((e) => `${e.source}->${e.target}`)).toEqual([
      'trigger->agent:s1', 'agent:s1->agent:s2', 'agent:s2->deck:s2', 'deck:s2->agent:s3',
    ]);
  });

  it('grounds the draft prompt in the named sources and demands a citation per slide', () => {
    const prompt = data('agent:s2').prompt as string;
    expect(prompt).toContain('doc-1 — Board work');
    expect(prompt).toContain('rec-1 — Team sync (transcript)');
    expect(prompt).toContain('EVERY slide must cite at least one source');
    expect(prompt).toContain('ONLY a JSON object');
    // The transcript and the earlier steps arrive from the run context.
    expect(prompt).toContain('{{ context.deckTranscript }}');
    expect(prompt).toContain('{{ context.deckPriorWork }}');
    expect(prompt).toContain('build the deck');
  });

  it('says plainly when the conversation named nothing to cite', () => {
    const bare = buildAgentLoopDefinition(deckPlan, request());
    expect(((bare.nodes as Array<Record<string, unknown>>).find((n) => n.id === 'agent:s2')!.data as { prompt: string }).prompt)
      .toContain('(none: the conversation named no documents)');
  });

  it('pins no spec without the fixture marker — the mock must not render a fake deck', () => {
    expect(data('deck:s2')).not.toHaveProperty('spec');
  });
});

describe('the [deck-fixture] dev override', () => {
  const deckPlan: LoopPlan = {
    source: 'planner',
    steps: [{ id: 's1', title: 'A deck', kind: 'deck', prompt: 'build', dependsOn: [], done: 'done' }],
  };

  it('fires only on the marker', () => {
    expect([wantsDeckFixture('build a deck [deck-fixture]'), wantsDeckFixture('build a deck')]).toEqual([true, false]);
  });

  it('pins a valid, self-citing spec onto the render node', () => {
    const def = buildAgentLoopDefinition(deckPlan, request({
      instruction: 'make the platform deck [deck-fixture]',
      sources: [{ documentId: 'doc-1', title: 'Board work' }],
    }));
    const spec = ((def.nodes as Array<Record<string, unknown>>).find((n) => n.id === 'deck:s1')!.data as { spec: unknown }).spec;
    const validated = validateDeckSpec(spec);
    expect(validated.ok).toBe(true);
    // Every slide cites the document the conversation actually named.
    expect((spec as { slides: Array<{ sources: Array<{ documentId: string }> }> }).slides
      .every((s) => s.sources.some((src) => src.documentId === 'doc-1'))).toBe(true);
  });

  it('still produces a valid spec when nothing was named to cite', () => {
    const def = buildAgentLoopDefinition(deckPlan, request({ instruction: 'deck [deck-fixture]' }));
    const spec = ((def.nodes as Array<Record<string, unknown>>).find((n) => n.id === 'deck:s1')!.data as { spec: unknown }).spec;
    expect(validateDeckSpec(spec).ok).toBe(true);
  });
});

describe('the planner surviving a cold pod', () => {
  /** A client whose successive runs behave differently. */
  function sequence(...attempts: Array<string | Error>): { client: AgentClient; calls: () => number } {
    let i = 0;
    return {
      calls: () => i,
      client: {
        provider: 'test',
        async *run(): AsyncIterable<AgentEvent> {
          const attempt = attempts[Math.min(i, attempts.length - 1)];
          i += 1;
          if (attempt instanceof Error) throw attempt;
          yield { type: 'done', text: attempt, turns: 1, tokensIn: 0, tokensOut: 0, model: 'haiku', stopReason: 'complete', toolCalls: [] };
        },
      },
    };
  }

  const PLAN_JSON = JSON.stringify({ steps: [goodStep('s1'), goodStep('s2', { dependsOn: ['s1'] })] });
  const noSleep = async (): Promise<void> => undefined;

  // Live: a loop 30s after a pod restart got `claude exited 1: (no stderr)`
  // after 3.3s and fell back to one step. The CLI was not ready, not wrong.
  it('retries once when the first spawn exits silently, and plans from the second', async () => {
    const { client, calls } = sequence(new Error('claude exited 1: (no stderr)'), PLAN_JSON);
    const plan = await planAgentLoop(request(), client, noSleep);

    expect(plan.source).toBe('planner');
    expect(plan.steps.map((s) => s.id)).toEqual(['s1', 's2']);
    expect(plan.notice).toBeUndefined();
    expect(calls()).toBe(2);
  });

  it('falls back honestly when the second attempt fails too', async () => {
    const { client, calls } = sequence(new Error('claude exited 1: (no stderr)'), new Error('claude exited 1: (no stderr)'));
    const plan = await planAgentLoop(request(), client, noSleep);

    expect(plan.source).toBe('fallback');
    expect(plan.notice).toMatch(/Planner unavailable/);
    expect(calls()).toBe(2);
  });

  // A failure that named a reason will name it again; retrying only makes a
  // person wait twice as long for the same fallback.
  it('does NOT retry a failure that said what was wrong', async () => {
    const { client, calls } = sequence(new Error('claude exited 1: unknown model "haiku-9"'), PLAN_JSON);
    const plan = await planAgentLoop(request(), client, noSleep);

    expect(plan.source).toBe('fallback');
    expect(plan.notice).toMatch(/unknown model/);
    expect(calls()).toBe(1);
  });

  // The live failure: the planner reported "(no stderr)" while the deck
  // fixture path, driving the same binary in the same minute, reported stale
  // credentials — because the CLI says so on stdout.
  it('does NOT retry stale credentials, and passes the actionable message through', async () => {
    const authError = new Error(describeClaudeCliFailure({ code: 1, resultError: 'Not logged in · Please run /login' }));
    const { client, calls } = sequence(authError, PLAN_JSON);
    const plan = await planAgentLoop(request(), client, noSleep);

    expect(plan.source).toBe('fallback');
    expect(plan.notice).toContain('claude is not authenticated in this environment');
    expect(plan.notice).toContain('tilt trigger claude-creds-extract');
    // One attempt: it would fail identically the second time.
    expect(calls()).toBe(1);
  });

  it('carries a stdout-only failure into the notice instead of "(no stderr)"', async () => {
    const stdoutOnly = new Error(describeClaudeCliFailure({ code: 1, stderr: '', resultError: 'Error: model overloaded' }));
    const { client } = sequence(stdoutOnly, stdoutOnly);
    const plan = await planAgentLoop(request(), client, noSleep);

    expect(plan.notice).toContain('model overloaded');
    expect(plan.notice).not.toContain('no stderr');
  });

  it('knows the cold-start signature from a real error', () => {
    expect([
      isSilentCliFailure('claude exited 1: (no stderr)'),
      isSilentCliFailure('claude exited 1: '),
      isSilentCliFailure('claude exited 1: Error: ENOENT'),
    ]).toEqual([true, true, false]);
  });
});

describe('lastStderrLines', () => {
  it('carries the tail, which is where the reason is', () => {
    expect(lastStderrLines('a\nb\nc')).toBe('a\nb\nc');
    expect(lastStderrLines(Array.from({ length: 30 }, (_, i) => `line ${i}`).join('\n')))
      .toBe(Array.from({ length: 8 }, (_, i) => `line ${22 + i}`).join('\n'));
  });

  it('is empty for empty stderr, so the caller can say "(no stderr)" honestly', () => {
    expect([lastStderrLines(''), lastStderrLines('  \n \n ')]).toEqual(['', '']);
  });
});
