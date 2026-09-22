/**
 * What run persistence tells the work graph, and when.
 *
 * The cases here are all ordering: the executor publishes `run:started` on an
 * in-process bus, so it can reach this service before the caller that knows
 * the run's organization, origin and steps has even been handed its run id.
 */
jest.mock('../../work-graph/sources/platformPublisher', () => ({
  platformWorkGraphSourcePublisher: { pipeline: jest.fn(async () => ({ published: true })) },
}));

import { describe, it, expect, beforeEach, jest } from '@jest/globals';
import { RunPersistenceService } from '../runPersistence';
import type { PipelineRunRepository } from '../../repositories/PipelineRunRepository';

const publisher = (jest.requireMock('../../work-graph/sources/platformPublisher') as {
  platformWorkGraphSourcePublisher: { pipeline: jest.Mock };
}).platformWorkGraphSourcePublisher;

type Handler = (event: { payload: Record<string, unknown> }) => Promise<void>;

const AT = '2026-09-21T18:00:00.000Z';
const RUN = 'executor-run-1';

function harness() {
  const handlers = new Map<string, Handler>();
  const eventBus = { subscribe: (name: string, handler: Handler) => { handlers.set(name, handler); return name; } };
  const repository = { put: jest.fn(async () => undefined) };
  const service = new RunPersistenceService({
    eventBus: eventBus as never,
    repository: repository as unknown as PipelineRunRepository,
  });
  service.start();
  const emit = async (name: string, payload: Record<string, unknown>): Promise<void> => {
    const handler = handlers.get(name);
    if (!handler) throw new Error(`nothing subscribed to ${name}`);
    await handler({ payload });
  };
  return { service, emit, repository };
}

/** Every record the service asked the publisher to send, in order. */
function published(): Array<Record<string, unknown>> {
  return publisher.pipeline.mock.calls.map((call) => call[0] as Record<string, unknown>);
}

const plan = {
  conversationId: 'chat:c:general',
  title: 'Agent loop',
  steps: [
    { stepId: 'agent:s1', title: 'Draft the agenda', dependsOn: [] },
    { stepId: 'agent:s2', title: 'Build the deck', dependsOn: ['agent:s1'] },
  ],
};

describe('the run reaches the graph even when its trigger is registered late', () => {
  beforeEach(() => { publisher.pipeline.mockClear(); });

  it('projects the run and every planned step when the trigger registers first', async () => {
    const { service, emit } = harness();
    service.registerTrigger({
      runId: RUN, pipelineId: 'chat-agent-loop', userId: 'dev-frank',
      triggerType: 'manual', at: AT, organizationId: 'dev-org', workGraph: plan,
    });
    await emit('pipeline:run:started', {
      runId: RUN, pipelineId: 'chat-agent-loop', at: AT,
      triggeredBy: { triggerType: 'manual', userId: 'dev-frank', payload: { instruction: 'create a meeting with the X team' } },
    });
    const records = published();
    expect(records.filter((record) => record.kind === 'pipeline-step')).toHaveLength(2);
    expect(records[0]).toMatchObject({
      kind: 'pipeline-run',
      safeLabel: 'Agent loop · create a meeting with the X team',
      inputRefs: [{ source: 'conversation', resourceId: 'chat:c:general' }],
    });
    expect(records.slice(1).map((record) => record.safeLabel)).toEqual(['Draft the agenda', 'Build the deck']);
  });

  it('projects them just the same when `run:started` arrives FIRST', async () => {
    const { service, emit } = harness();
    // This is the real order for a chat command: the executor emits on an
    // in-process bus before `bridge.trigger` has returned the run id.
    await emit('pipeline:run:started', {
      runId: RUN, pipelineId: 'chat-agent-loop', at: AT,
      triggeredBy: { triggerType: 'manual', userId: 'dev-frank', payload: { instruction: 'create a meeting with the X team' } },
    });
    expect(published()).toHaveLength(0);   // nothing to say yet: no organization
    service.registerTrigger({
      runId: RUN, pipelineId: 'chat-agent-loop', userId: 'dev-frank',
      triggerType: 'manual', at: AT, organizationId: 'dev-org', workGraph: plan,
    });
    await new Promise((resolve) => setImmediate(resolve));
    const records = published();
    expect(records[0]).toMatchObject({
      kind: 'pipeline-run',
      safeLabel: 'Agent loop · create a meeting with the X team',
      inputRefs: [{ source: 'conversation', resourceId: 'chat:c:general' }],
    });
    expect(records.filter((record) => record.kind === 'pipeline-step').map((record) => record.safeLabel))
      .toEqual(['Draft the agenda', 'Build the deck']);
  });

  it('never lets a late registration overwrite what the run already established', async () => {
    const { service, emit } = harness();
    service.registerTrigger({
      runId: RUN, pipelineId: 'chat-agent-loop', userId: 'dev-frank',
      triggerType: 'manual', at: AT, organizationId: 'dev-org', workGraph: plan,
    });
    await emit('pipeline:run:started', {
      runId: RUN, pipelineId: 'chat-agent-loop', at: AT,
      triggeredBy: { triggerType: 'manual', userId: 'dev-frank', payload: {} },
    });
    publisher.pipeline.mockClear();
    service.registerTrigger({
      runId: RUN, pipelineId: 'chat-agent-loop', userId: 'someone-else',
      triggerType: 'manual', at: AT, organizationId: 'other-org',
      workGraph: { ...plan, conversationId: 'chat:c:elsewhere' },
    });
    await new Promise((resolve) => setImmediate(resolve));
    expect(published()[0]).toMatchObject({
      attributedSubject: 'dev-frank',
      inputRefs: [{ source: 'conversation', resourceId: 'chat:c:general' }],
    });
  });

  it('says nothing at all for a run with no verified organization', async () => {
    const { service, emit } = harness();
    service.registerTrigger({
      runId: RUN, pipelineId: 'chat-agent-loop', userId: 'dev-frank',
      triggerType: 'manual', at: AT, workGraph: plan,
    });
    await emit('pipeline:run:started', {
      runId: RUN, pipelineId: 'chat-agent-loop', at: AT,
      triggeredBy: { triggerType: 'manual', userId: 'dev-frank', payload: {} },
    });
    expect(published()).toHaveLength(0);
  });
});

describe('a step says where it is running and what it wrote', () => {
  beforeEach(() => { publisher.pipeline.mockClear(); });

  async function startedRun() {
    const { service, emit, repository } = harness();
    service.registerTrigger({
      runId: RUN, pipelineId: 'chat-agent-loop', userId: 'dev-frank',
      triggerType: 'manual', at: AT, organizationId: 'dev-org', workGraph: plan,
    });
    await emit('pipeline:run:started', {
      runId: RUN, pipelineId: 'chat-agent-loop', at: AT,
      triggeredBy: { triggerType: 'manual', userId: 'dev-frank', payload: { instruction: 'do the thing' } },
    });
    publisher.pipeline.mockClear();
    return { service, emit, repository };
  }

  it('reports a step as running from its own start event', async () => {
    const { emit } = await startedRun();
    await emit('pipeline:step:started', { runId: RUN, stepId: 'agent:s1', nodeType: 'agent', at: AT });
    expect(published()).toMatchObject([{ kind: 'pipeline-step', step: { stepId: 'agent:s1', status: 'running' } }]);
  });

  it('carries the workspace the agent loop reported on its iteration', async () => {
    const { emit } = await startedRun();
    await emit('pipeline:step:agent:iteration', {
      runId: RUN, stepId: 'agent:s1', at: AT, iteration: 4,
      model: 'sonnet', contextTokens: 145_800, contextBudgetTokens: 200_000,
      sessionId: 'session-28a319a9', boxId: 'box-7',
    });
    expect(published()[0]).toMatchObject({
      step: { stepId: 'agent:s1', status: 'running' },
      workspace: { resourceId: 'session-28a319a9', safeLabel: 'Cloud workspace · box-7' },
    });
  });

  it('holds the running step to one announcement a minute', async () => {
    const { emit } = await startedRun();
    const iteration = (at: string, n: number) => emit('pipeline:step:agent:iteration', {
      runId: RUN, stepId: 'agent:s1', at, iteration: n, sessionId: 's-1',
    });
    await iteration('2026-09-21T18:00:00.000Z', 1);
    await iteration('2026-09-21T18:00:20.000Z', 2);
    await iteration('2026-09-21T18:00:40.000Z', 3);
    expect(published()).toHaveLength(1);
    await iteration('2026-09-21T18:01:30.000Z', 4);
    expect(published()).toHaveLength(2);
  });

  it('names the document the step produced, from the step\'s own result', async () => {
    const { emit } = await startedRun();
    await emit('pipeline:step:completed', {
      runId: RUN, stepId: 'agent:s1', durationMs: 10, at: AT,
      output: { result: { kind: 'document', documentId: 'doc-1' } },
    });
    expect(published()[0]).toMatchObject({
      step: { stepId: 'agent:s1', status: 'completed' },
      producesRefs: [{ source: 'document', resourceId: 'doc-1' }],
    });
  });

  it('closes out the steps a terminal run never reached', async () => {
    const { emit } = await startedRun();
    await emit('pipeline:step:completed', { runId: RUN, stepId: 'agent:s1', durationMs: 10, at: AT });
    publisher.pipeline.mockClear();
    await emit('pipeline:run:completed', { runId: RUN, at: '2026-09-21T18:10:00.000Z' });
    const steps = published().filter((record) => record.kind === 'pipeline-step');
    // s1 already finished; only s2 is closed out, and never re-closed.
    expect(steps).toMatchObject([{ step: { stepId: 'agent:s2', status: 'cancelled' } }]);
  });
});

describe('replaying a stored run', () => {
  beforeEach(() => { publisher.pipeline.mockClear(); });

  const service = () => harness().service;
  const row = (over: Record<string, unknown> = {}) => ({
    pipelineId: 'chat-agent-loop', runId: 'loop-1', status: 'failed',
    triggeredBy: 'dev-frank', triggerType: 'manual', organizationId: 'dev-org',
    startedAt: AT, lastEventAt: AT,
    checkpoint: { sequenceNumber: 0, at: AT, currentStepIds: [], context: {
      loopId: 'loop-1', instruction: 'create a meeting with the X team', chatChannel: 'chat:c:general',
      plan: { steps: [] },
    } },
    ...over,
  }) as never;

  it('says nothing for a row that only points at the run that really happened', async () => {
    // One loop, one run node: the accept-time row and the executor run were
    // projected as two, with two titles, and the steps framed under one.
    await service().reconcileWorkGraphRun(row({ agentLoopRunId: 'executor-run-1' }));
    expect(published()).toHaveLength(0);
  });

  it('still speaks for a loop whose planner never answered', async () => {
    await service().reconcileWorkGraphRun(row());
    expect(published()[0]).toMatchObject({
      kind: 'pipeline-run',
      safeLabel: 'create a meeting with the X team',
      inputRefs: [{ source: 'conversation', resourceId: 'chat:c:general' }],
    });
  });

  it('names it from the definition the row kept, when it kept one', async () => {
    await service().reconcileWorkGraphRun(row({ definitionSnapshot: { name: 'Agent loop' } }));
    expect(published()[0]).toMatchObject({ safeLabel: 'Agent loop · create a meeting with the X team' });
  });

  it('is a NEW observation, so it can repair a name and not merely re-assert one', async () => {
    await service().reconcileWorkGraphRun(row());
    expect(Date.parse((published()[0] as { occurredAt: string }).occurredAt)).toBeGreaterThan(Date.parse(AT));
  });

  it('replays an ordinary run exactly as it stands', async () => {
    await service().reconcileWorkGraphRun({
      pipelineId: 'nightly', runId: 'run-9', status: 'completed', triggeredBy: 'dev-frank',
      triggerType: 'manual', organizationId: 'dev-org', startedAt: AT, lastEventAt: AT,
    } as never);
    expect(published()[0]).toMatchObject({ kind: 'pipeline-run', occurredAt: AT });
    expect(published()[0]).not.toHaveProperty('safeLabel');
  });
});
