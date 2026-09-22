// What happens to a MULTI-STEP loop when the run's context budget runs out.
//
// The live failure this pins: three steps share one devbox session, so the
// context each step sees is cumulative. Step 1 finished at 145.8k of a 200k
// budget; step 2's first turn crossed 200k and stopped with `budget`; the
// handler called that a step FAILURE, so the run failed, went to the
// dead-letter queue as an UnknownError, and the shared session was torn down
// with two steps still to go.
import { createAgentStepHandler } from '../AgentStepHandler';
import type { AgentClient, AgentEvent, AgentStepOutput } from '../types';
import type { PipelineNode, StepHandlerHelpers, StepOutcome } from 'distributed-core/applications/pipeline';

/** A devbox client that always reports it stopped on the budget. */
const budgetClient: AgentClient = {
  provider: 'test',
  async *run(): AsyncIterable<AgentEvent> {
    yield { type: 'session', sessionId: 'sess-1', boxId: 'box-1', created: false };
    yield { type: 'iteration', iteration: 1, contextTokens: 205_000, contextBudgetTokens: 200_000, model: 'sonnet' };
    yield {
      type: 'done', text: 'partial work', turns: 1, tokensIn: 100, tokensOut: 10,
      model: 'sonnet', stopReason: 'budget', iterations: 1, contextTokens: 205_000,
      sessionId: 'sess-1', boxId: 'box-1', toolCalls: [],
    };
  },
};

const neverCalled: AgentClient = {
  provider: 'test',
  async *run(): AsyncIterable<AgentEvent> { throw new Error('the step should not have started'); },
};

const node = (over: Record<string, unknown> = {}): PipelineNode => ({
  id: 'agent:s2', type: 'agent', position: { x: 0, y: 0 },
  data: { type: 'agent', prompt: 'do the thing', runtime: 'devbox', contextBudgetTokens: 200_000, ...over },
} as unknown as PipelineNode);

const helpers = (): StepHandlerHelpers => ({
  runId: 'run-1', stepId: 'agent:s2',
  emit: async () => undefined,
  isCancelled: () => false,
  abortSignal: new AbortController().signal,
} as unknown as StepHandlerHelpers);

describe('a step that spends the run\'s context budget', () => {
  it('COMPLETES with the reason rather than failing the run', async () => {
    const handler = createAgentStepHandler({ agentClient: neverCalled, devboxAgentClient: budgetClient });
    const outcome = await handler.execute(node(), {}, helpers()) as StepOutcome;

    // The whole point: not 'failed'. A failed step fails the run, dead-letters
    // it, and tears down a session the later steps still need.
    expect(outcome.status).toBe('completed');
    expect(outcome.error).toBeUndefined();

    const output = outcome.output as AgentStepOutput & { budgetExhausted?: boolean };
    expect(output.stopReason).toBe('budget');
    expect(output.contextTokens).toBe(205_000);
    // Merged into the run context, so the steps behind it can see it.
    expect(output.budgetExhausted).toBe(true);
    // The partial work is kept, not discarded.
    expect(output.text).toBe('partial work');
  });

  it('a later step stops BEFORE starting, spending no tokens and no turns', async () => {
    const handler = createAgentStepHandler({ agentClient: neverCalled, devboxAgentClient: neverCalled });
    const outcome = await handler.execute(node(), { budgetExhausted: true }, helpers()) as StepOutcome;

    expect(outcome.status).toBe('completed');
    const output = outcome.output as AgentStepOutput & { skipped?: boolean; skippedReason?: string };
    expect(output).toMatchObject({
      stopReason: 'budget', iterations: 0, tokensIn: 0, tokensOut: 0, skipped: true,
    });
    expect(output.skippedReason).toMatch(/context budget/);
  });

  it('does not skip a LOCAL step on an exhausted devbox budget — different runtime, different bound', async () => {
    const local: AgentClient = {
      provider: 'test',
      async *run(): AsyncIterable<AgentEvent> {
        yield { type: 'done', text: 'ran', turns: 1, tokensIn: 5, tokensOut: 5, model: 'sonnet', stopReason: 'complete', toolCalls: [] };
      },
    };
    const handler = createAgentStepHandler({ agentClient: local, devboxAgentClient: neverCalled });
    const outcome = await handler.execute(
      node({ runtime: 'local' }), { budgetExhausted: true }, helpers(),
    ) as StepOutcome;
    expect(outcome.status).toBe('completed');
    expect((outcome.output as AgentStepOutput).text).toBe('ran');
  });

  it('still FAILS on max_turns and timeout — those are cut off, not spent', async () => {
    for (const stopReason of ['max_turns', 'timeout'] as const) {
      const client: AgentClient = {
        provider: 'test',
        async *run(): AsyncIterable<AgentEvent> {
          yield { type: 'done', text: '', turns: 1, tokensIn: 0, tokensOut: 0, model: 'sonnet', stopReason, iterations: 1, contextTokens: 10, toolCalls: [] };
        },
      };
      const handler = createAgentStepHandler({ agentClient: neverCalled, devboxAgentClient: client });
      const outcome = await handler.execute(node(), {}, helpers()) as StepOutcome;
      expect({ stopReason, status: outcome.status }).toEqual({ stopReason, status: 'failed' });
      expect(outcome.error).toContain(stopReason);
    }
  });

  it('carries the shared session through to the output, so the next step resumes it', async () => {
    const handler = createAgentStepHandler({ agentClient: neverCalled, devboxAgentClient: budgetClient });
    const outcome = await handler.execute(node(), {}, helpers()) as StepOutcome;
    const output = outcome.output as AgentStepOutput & { lab?: { sessionId: string } };
    expect(output.sessionId).toBe('sess-1');
    expect(output.lab).toEqual({ sessionId: 'sess-1', boxId: 'box-1' });
  });

  it('hands an existing session from the run context to the client, rather than opening a second box', async () => {
    let seenLab: unknown;
    const spy: AgentClient = {
      provider: 'test',
      async *run(_p: string, opts): AsyncIterable<AgentEvent> {
        seenLab = opts?.lab;
        yield { type: 'done', text: 'ok', turns: 1, tokensIn: 0, tokensOut: 0, model: 'sonnet', stopReason: 'complete', toolCalls: [] };
      },
    };
    const handler = createAgentStepHandler({ agentClient: neverCalled, devboxAgentClient: spy });
    await handler.execute(node(), { lab: { sessionId: 'sess-1', boxId: 'box-1' } }, helpers());
    expect(seenLab).toEqual({ sessionId: 'sess-1', boxId: 'box-1' });
  });
});

describe('the numbers surviving persistence', () => {
  it('mirrors the step facts into the run context, where truncation cannot reach them', async () => {
    const handler = createAgentStepHandler({ agentClient: neverCalled, devboxAgentClient: budgetClient });
    const outcome = await handler.execute(node(), {}, helpers()) as StepOutcome;
    const output = outcome.output as { loopSteps: Record<string, Record<string, unknown>> };
    expect(output.loopSteps['agent:s2']).toMatchObject({
      iterations: 1, contextTokens: 205_000, contextBudgetTokens: 200_000,
      stopReason: 'budget', model: 'sonnet', runtime: 'devbox', sessionId: 'sess-1', boxId: 'box-1',
    });
  });

  it('keeps every earlier step\'s facts as the run walks forward', async () => {
    const handler = createAgentStepHandler({ agentClient: neverCalled, devboxAgentClient: budgetClient });
    const prior = { 'agent:s1': { iterations: 4, contextTokens: 145_800, stopReason: 'complete' } };
    const outcome = await handler.execute(node(), { loopSteps: prior }, helpers()) as StepOutcome;
    const output = outcome.output as { loopSteps: Record<string, unknown> };
    expect(Object.keys(output.loopSteps)).toEqual(['agent:s1', 'agent:s2']);
    expect(output.loopSteps['agent:s1']).toEqual(prior['agent:s1']);
  });

  it('a skipped step records its facts too, so the pane is not blank for it', async () => {
    const handler = createAgentStepHandler({ agentClient: neverCalled, devboxAgentClient: neverCalled });
    const outcome = await handler.execute(node(), { budgetExhausted: true }, helpers()) as StepOutcome;
    expect((outcome.output as { loopSteps: Record<string, unknown> }).loopSteps['agent:s2'])
      .toMatchObject({ iterations: 0, stopReason: 'budget', skipped: true });
  });

  // The whole output is replaced past 100KB, not trimmed — so the trace is
  // what gets sacrificed, never the numbers.
  it('drops tool payloads rather than letting the output be replaced wholesale', async () => {
    const big = 'x'.repeat(5_000);
    const chatty: AgentClient = {
      provider: 'test',
      async *run(): AsyncIterable<AgentEvent> {
        yield {
          type: 'done', text: 'done', turns: 1, tokensIn: 1, tokensOut: 1, model: 'sonnet',
          stopReason: 'complete', iterations: 1, contextTokens: 10,
          toolCalls: Array.from({ length: 30 }, (_, i) => ({
            name: `Read${i}`, input: { file: big }, output: big, ok: true, startedAt: 'now', endedAt: 'now',
          })),
        };
      },
    };
    const handler = createAgentStepHandler({ agentClient: neverCalled, devboxAgentClient: chatty });
    const outcome = await handler.execute(node(), {}, helpers()) as StepOutcome;
    const output = outcome.output as AgentStepOutput & { loopSteps: Record<string, unknown> };

    expect(JSON.stringify(output).length).toBeLessThan(100 * 1024);
    // The calls are still listed, with their names and outcomes.
    expect(output.toolCalls).toHaveLength(30);
    expect(output.toolCalls[0]).toMatchObject({ name: 'Read0', ok: true });
    expect(String(output.toolCalls[0].output)).toMatch(/too large to persist/);
    // And the numbers came through untouched.
    expect(output.loopSteps['agent:s2']).toMatchObject({ iterations: 1, stopReason: 'complete' });
  });
});
