// Trigger-level model / budget overrides for agent steps (runOverrides.ts):
// validated at the trigger, honoured by the step, capped, and absent = the
// step's own config.
jest.mock('../../deck/presentationDocuments', () => ({ getPresentationDocumentStore: () => ({ get: async () => null }) }));
jest.mock('../../work-graph/sources/platformPublisher', () => ({
  platformWorkGraphSourcePublisher: { pipeline: jest.fn(async () => ({ published: true })), document: jest.fn(async () => ({ published: true })), meeting: jest.fn(async () => ({ published: true })) },
}));

import { RunPersistenceService } from '../runPersistence';
import type { PipelineRunItem, PipelineRunRepository } from '../../repositories/PipelineRunRepository';
import { applyRunEvent } from '../run-rollup';
import { createAgentStepHandler } from '../agent/AgentStepHandler';
import type { AgentClient, AgentEvent, AgentRunOptions } from '../agent/types';
import type { PipelineNode, StepHandlerHelpers, StepOutcome } from 'distributed-core/applications/pipeline';
import { effectiveAgentSettings, resolveRunOverrides, stampRunOverrides } from '../runOverrides';
import { isPricedModel } from '../llm-pricing';
import { ValidationError } from '../../middleware/error-handler';

/** A client that records the options each run was started with. */
function recordingClient(runtime: 'local' | 'devbox'): { client: AgentClient; calls: AgentRunOptions[] } {
  const calls: AgentRunOptions[] = [];
  const client: AgentClient = {
    provider: `test-${runtime}`,
    async *run(_prompt: string, opts: AgentRunOptions): AsyncIterable<AgentEvent> {
      calls.push(opts);
      yield { type: 'done', text: 'ok', turns: 1, tokensIn: 10, tokensOut: 2, model: opts.model ?? 'unset', stopReason: 'complete', toolCalls: [] } as AgentEvent;
    },
  };
  return { client, calls };
}

const node = (data: Record<string, unknown>): PipelineNode => ({
  id: 'agent:a', type: 'agent', position: { x: 0, y: 0 },
  data: { type: 'agent', prompt: 'do it', ...data },
} as unknown as PipelineNode);

const helpers = (): StepHandlerHelpers => ({
  runId: 'run-1', stepId: 'agent:a', emit: async () => undefined, isCancelled: () => false,
  abortSignal: new AbortController().signal,
} as unknown as StepHandlerHelpers);

async function runStep(data: Record<string, unknown>, triggerPayload: Record<string, unknown>) {
  const devbox = recordingClient('devbox');
  const local = recordingClient('local');
  const handler = createAgentStepHandler({ agentClient: local.client, devboxAgentClient: devbox.client });
  // The run context IS the (stamped) trigger payload.
  const context = stampRunOverrides(triggerPayload, 2_000_000);
  const outcome = await handler.execute(node(data), context, helpers()) as StepOutcome;
  return { outcome, opts: (data.runtime === 'devbox' ? devbox : local).calls[0]! };
}

describe('run overrides — the trigger contract', () => {
  it('accepts only models the pricing table prices (the estimate endpoint\'s list)', () => {
    for (const ok of ['haiku', 'sonnet', 'opus', 'claude-3-haiku-20240307', 'us.anthropic.claude-opus-5']) expect(isPricedModel(ok)).toBe(true);
    for (const bad of ['gpt-9', 'llama', '', '  ']) expect(isPricedModel(bad)).toBe(false);
    expect(resolveRunOverrides({ model: 'sonnet' })).toEqual({ model: 'sonnet' });
    expect(() => resolveRunOverrides({ model: 'gpt-9' })).toThrow(ValidationError);
    expect(() => resolveRunOverrides({ model: 42 })).toThrow(/not a priced model/);
  });

  it('caps the budget at the platform max and says what was asked for; refuses a non-integer', () => {
    expect(resolveRunOverrides({ contextBudgetTokens: 5_000_000 }, 2_000_000))
      .toEqual({ contextBudgetTokens: 2_000_000, requestedContextBudgetTokens: 5_000_000 });
    expect(resolveRunOverrides({ contextBudgetTokens: 80_000 }, 2_000_000)).toEqual({ contextBudgetTokens: 80_000 });
    expect(() => resolveRunOverrides({ contextBudgetTokens: -1 })).toThrow(ValidationError);
    expect(() => resolveRunOverrides({ contextBudgetTokens: 1.5 })).toThrow(ValidationError);
  });

  it('stamps only validated values: a caller-sent runOverrides is discarded', () => {
    expect(stampRunOverrides({ instruction: 'x', runOverrides: { model: 'gpt-9', contextBudgetTokens: 9e9 } })).toEqual({ instruction: 'x' });
    expect(stampRunOverrides({ model: 'opus' }).runOverrides).toEqual({ model: 'opus' });
  });
});

describe('run overrides — what an agent step runs with', () => {
  it('applies the override: the trigger\'s model beats the step\'s', async () => {
    const { outcome, opts } = await runStep({ model: 'haiku', runtime: 'devbox', contextBudgetTokens: 200_000 }, { model: 'opus', contextBudgetTokens: 60_000 });
    expect(outcome.status).toBe('completed');
    expect(opts.model).toBe('opus');
    expect(opts.contextBudgetTokens).toBe(60_000);
    // The step output (→ run record step summary) names what actually ran.
    expect(outcome.output).toMatchObject({ model: 'opus', contextBudgetTokens: 60_000, runtime: 'devbox' });
  });

  it('caps the budget: never above the step\'s own budget, nor the platform max', async () => {
    const underStep = await runStep({ runtime: 'devbox', contextBudgetTokens: 100_000 }, { contextBudgetTokens: 500_000 });
    expect(underStep.opts.contextBudgetTokens).toBe(100_000);
    const underMax = await runStep({ runtime: 'devbox' }, { contextBudgetTokens: 9_000_000 });
    expect(underMax.opts.contextBudgetTokens).toBe(2_000_000);
  });

  it('no override = the step config, exactly as before', async () => {
    const { opts } = await runStep({ model: 'haiku', runtime: 'devbox', contextBudgetTokens: 150_000 }, { instruction: 'x' });
    expect(opts.model).toBe('haiku');
    expect(opts.contextBudgetTokens).toBe(150_000);
    expect(effectiveAgentSettings({}, {}, 'sonnet')).toEqual({ model: 'sonnet' });
  });

  it('ignores a bare `model` key a prior step merged into the context — only the stamped override counts', async () => {
    const { opts } = await runStep({ model: 'haiku' }, {});
    expect(opts.model).toBe('haiku');
    expect(effectiveAgentSettings({ model: 'haiku' }, { model: 'opus' }, 'sonnet').model).toBe('haiku');
  });
});

describe('run overrides — the run record and the rollup show what ran', () => {
  it('the run row carries the overrides; the agent step summary carries its effective model and budget', async () => {
    const rows = new Map<string, PipelineRunItem>();
    const write = async (item: PipelineRunItem): Promise<true> => { rows.set(item.runId, { ...item }); return true; };
    const repository = { put: jest.fn(write), putOwned: jest.fn(write) };
    type Handler = (event: { id?: string; payload: Record<string, unknown>; sourceNodeId?: string }) => Promise<void>;
    const handlers = new Map<string, Handler>();
    const eventBus = { subscribe: (name: string, handler: Handler) => { handlers.set(name, handler); return name; } };
    const service = new RunPersistenceService({ eventBus: eventBus as never, repository: repository as unknown as PipelineRunRepository });
    service.start();
    let n = 0;
    const emit = (name: string, payload: Record<string, unknown>) => handlers.get(name)!({ id: `e-${++n}`, payload, sourceNodeId: 'node-a' });

    const payload = stampRunOverrides({ instruction: 'x', model: 'opus', contextBudgetTokens: 60_000 });
    await emit('pipeline:run:started', { runId: 'r', pipelineId: 'p', triggeredBy: { triggerType: 'manual', userId: 'u', payload }, at: '2026-09-23T12:00:00.000Z' });
    await emit('pipeline:step:completed', { runId: 'r', stepId: 'agent:a', durationMs: 5, at: '2026-09-23T12:00:01.000Z',
      output: { text: 'ok', model: 'opus', contextBudgetTokens: 60_000, runtime: 'local', tokensIn: 1, tokensOut: 1 } });
    await emit('pipeline:run:completed', { runId: 'r', pipelineId: 'p', at: '2026-09-23T12:00:02.000Z', durationMs: 2000 });

    const row = rows.get('r')!;
    expect(row.runOverrides).toEqual({ model: 'opus', contextBudgetTokens: 60_000 });
    expect(row.stepsSummary!['agent:a'].agent).toMatchObject({ model: 'opus', contextBudgetTokens: 60_000, runtime: 'local' });
  });

  it('a started run\'s rollup entry names the override model', () => {
    const row = applyRunEvent(null, { pipelineId: 'p', runId: 'r', status: 'running', at: 't1', startedAt: 't1', model: 'opus' });
    expect(row.recent[0]).toMatchObject({ runId: 'r', model: 'opus' });
    const done = applyRunEvent(row, { pipelineId: 'p', runId: 'r', status: 'completed', at: 't2' });
    expect(done.recent[0]).toMatchObject({ status: 'completed', model: 'opus' });
  });
});

describe('run overrides — the run read', () => {
  it('runItemToSnapshot (list + persisted detail) carries runOverrides and each agent step\'s effective settings', () => {
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const { runItemToSnapshot } = require('../createBridge') as typeof import('../createBridge');
    const snap = runItemToSnapshot({
      pipelineId: 'p', runId: 'r', status: 'completed', triggeredBy: 'u', triggerType: 'manual', startedAt: 't',
      runOverrides: { model: 'opus', contextBudgetTokens: 60_000 },
      stepsSummary: { 'agent:a': { status: 'completed', agent: { model: 'opus', contextBudgetTokens: 60_000 } } },
    } as unknown as PipelineRunItem);
    expect(snap.runOverrides).toEqual({ model: 'opus', contextBudgetTokens: 60_000 });
    expect((snap.steps as Record<string, { agent?: unknown }>)['agent:a'].agent).toEqual({ model: 'opus', contextBudgetTokens: 60_000 });
  });
});
