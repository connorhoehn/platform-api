/**
 * What run persistence tells the work graph, and when.
 *
 * The cases here are all ordering: the executor publishes `run:started` on an
 * in-process bus, so it can reach this service before the caller that knows
 * the run's organization, origin and steps has even been handed its run id.
 */
const mockPresentation = jest.fn(async (_documentId: string) => ({ documentId: 'deck-1' }) as unknown);
jest.mock('../../deck/presentationDocuments', () => ({
  getPresentationDocumentStore: () => ({ get: (id: string) => mockPresentation(id) }),
}));
jest.mock('../../work-graph/sources/platformPublisher', () => ({
  platformWorkGraphSourcePublisher: {
    pipeline: jest.fn(async () => ({ published: true })),
    document: jest.fn(async () => ({ published: true })),
    meeting: jest.fn(async () => ({ published: true })),
  },
}));

import { describe, it, expect, beforeEach, jest } from '@jest/globals';
import { RunPersistenceService } from '../runPersistence';
import type { PipelineRunRepository } from '../../repositories/PipelineRunRepository';

const publisher = (jest.requireMock('../../work-graph/sources/platformPublisher') as {
  platformWorkGraphSourcePublisher: { pipeline: jest.Mock; document: jest.Mock; meeting: jest.Mock };
}).platformWorkGraphSourcePublisher;

type Handler = (event: { payload: Record<string, unknown> }) => Promise<void>;

const AT = '2026-09-21T18:00:00.000Z';
const RUN = 'executor-run-1';

function harness() {
  const handlers = new Map<string, Handler>();
  const eventBus = { subscribe: (name: string, handler: Handler) => { handlers.set(name, handler); return name; } };
  const repository = { put: jest.fn(async (_item: unknown) => undefined) };
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

  it('names a run once when its instruction is the pipeline’s own name', async () => {
    // A probe triggered with instruction "Agent loop" on the "Agent loop"
    // pipeline was titled "Agent loop · Agent loop" on every card and pane.
    const { service, emit } = harness();
    service.registerTrigger({
      runId: RUN, pipelineId: 'chat-agent-loop', userId: 'dev-frank',
      triggerType: 'manual', at: AT, organizationId: 'dev-org', workGraph: plan,
    });
    await emit('pipeline:run:started', {
      runId: RUN, pipelineId: 'chat-agent-loop', at: AT,
      triggeredBy: { triggerType: 'manual', userId: 'dev-frank', payload: { instruction: ' agent loop ' } },
    });
    expect(published()[0]).toMatchObject({ kind: 'pipeline-run', safeLabel: 'Agent loop' });
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
      // Addressed within its run, so the pipeline ACL can authorize it.
      workspace: { resourceId: `${RUN}::session-28a319a9`, safeLabel: 'Cloud workspace · box-7' },
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

describe('a run that edits a document is joined to it from its first announcement (NFR #125)', () => {
  beforeEach(() => { publisher.pipeline.mockClear(); });

  it('carries the plan\'s `produces` on every run record, and never on a plan that names none', async () => {
    const { service, emit } = harness();
    service.registerTrigger({
      runId: RUN, pipelineId: 'deck-revise', userId: 'dev-connor', triggerType: 'manual', at: AT, organizationId: 'dev-org',
      workGraph: { ...plan, produces: [{ source: 'document', resourceId: 'deck-1' }] },
    });
    await emit('pipeline:run:started', { runId: RUN, pipelineId: 'deck-revise', at: AT, triggeredBy: { triggerType: 'manual', userId: 'dev-connor' } });
    await emit('pipeline:step:breakpoint', { runId: RUN, stepId: 'agent:s2', at: '2026-09-21T18:01:00.000Z' });
    const runs = published().filter((record) => record.kind === 'pipeline-run');
    expect(runs).toHaveLength(2);
    for (const record of runs) expect(record.producesRefs).toEqual([{ source: 'document', resourceId: 'deck-1' }]);
    publisher.pipeline.mockClear();
    const other = harness();
    other.service.registerTrigger({ runId: 'plain-run', pipelineId: 'chat-agent-loop', userId: 'dev-connor', triggerType: 'manual', at: AT, organizationId: 'dev-org', workGraph: plan });
    await other.emit('pipeline:run:started', { runId: 'plain-run', pipelineId: 'chat-agent-loop', at: AT, triggeredBy: { triggerType: 'manual', userId: 'dev-connor' } });
    expect(published().find((record) => record.kind === 'pipeline-run')).not.toHaveProperty('producesRefs');
  });
});

describe('a run that stops to wait is not running (NFR #126)', () => {
  beforeEach(() => { publisher.pipeline.mockClear(); });

  async function startedRun() {
    const { service, emit, repository } = harness();
    service.registerTrigger({
      runId: RUN, pipelineId: 'deck-revise', userId: 'dev-connor',
      triggerType: 'manual', at: AT, organizationId: 'dev-org', workGraph: plan,
    });
    await emit('pipeline:run:started', {
      runId: RUN, pipelineId: 'deck-revise', at: AT,
      triggeredBy: { triggerType: 'manual', userId: 'dev-connor', payload: { instruction: 'shorten the title' } },
    });
    publisher.pipeline.mockClear();
    repository.put.mockClear();
    return { emit, repository };
  }
  const runRecords = () => published().filter((record) => record.kind === 'pipeline-run')
    .map((record) => (record.run as { status: string }).status);

  it('projects a breakpoint as the run waiting, and the next step starting as it running again', async () => {
    const { emit, repository } = await startedRun();
    await emit('pipeline:step:breakpoint', { runId: RUN, stepId: 'agent:s2', at: '2026-09-21T18:01:00.000Z' });
    expect(runRecords()).toEqual(['paused_at_breakpoint']);
    // The row says the same, so a replica that reloads the run agrees.
    expect((repository.put.mock.calls.at(-1)?.[0] as { status: string }).status).toBe('paused_at_breakpoint');
    await emit('pipeline:step:started', { runId: RUN, stepId: 'agent:s2', at: '2026-09-21T18:02:00.000Z' });
    expect(runRecords()).toEqual(['paused_at_breakpoint', 'running']);
  });

  it('projects an approval gate as waiting until the approval is recorded; a rejection is not a resume', async () => {
    const { emit } = await startedRun();
    await emit('pipeline:approval:requested', { runId: RUN, stepId: 'agent:s1', approvers: [], at: '2026-09-21T18:01:00.000Z' });
    await emit('pipeline:approval:recorded', { runId: RUN, stepId: 'agent:s1', userId: 'dev-frank', decision: 'reject', at: '2026-09-21T18:01:30.000Z' });
    expect(runRecords()).toEqual(['awaiting_approval']);
    await emit('pipeline:approval:recorded', { runId: RUN, stepId: 'agent:s1', userId: 'dev-frank', decision: 'approve', at: '2026-09-21T18:02:00.000Z' });
    expect(runRecords()).toEqual(['awaiting_approval', 'running']);
  });

  it('ignores a resume older than the pause, a repeated pause, and a run it does not hold', async () => {
    const { emit } = await startedRun();
    await emit('pipeline:step:breakpoint', { runId: RUN, stepId: 'agent:s2', at: '2026-09-21T18:01:00.000Z' });
    await emit('pipeline:step:breakpoint', { runId: RUN, stepId: 'agent:s2', at: '2026-09-21T18:01:00.000Z' });
    await emit('pipeline:run:resumed', { runId: RUN, pipelineId: 'deck-revise', frontier: [], at: '2026-09-21T18:00:30.000Z' });
    await emit('pipeline:step:breakpoint', { runId: 'someone-elses-run', stepId: 'agent:s2', at: '2026-09-21T18:01:00.000Z' });
    expect(runRecords()).toEqual(['paused_at_breakpoint']);
    await emit('pipeline:run:resumed', { runId: RUN, pipelineId: 'deck-revise', frontier: [], at: '2026-09-21T18:03:00.000Z' });
    expect(runRecords()).toEqual(['paused_at_breakpoint', 'running']);
  });

  it('writes why the run waits on the row with the pause, and clears it when the run works again', async () => {
    const { emit, repository } = await startedRun();
    const lastRow = () => repository.put.mock.calls.at(-1)?.[0] as { status: string; pause?: unknown };
    await emit('pipeline:step:breakpoint', { runId: RUN, stepId: 'write', at: '2026-09-21T18:01:00.000Z' });
    expect(lastRow().pause).toEqual({ reason: 'paused_at_breakpoint', stepId: 'write' });
    await emit('pipeline:step:started', { runId: RUN, stepId: 'write', at: '2026-09-21T18:02:00.000Z' });
    expect(lastRow().status).toBe('running');
    expect(lastRow().pause).toBeUndefined();
    await emit('pipeline:approval:requested', { runId: RUN, stepId: 'approve', approvers: [], at: '2026-09-21T18:03:00.000Z' });
    expect(lastRow().pause).toEqual({ reason: 'awaiting_approval', stepId: 'approve' });
  });
});

describe('a run that fails before it has a plan or an instruction to be named after', () => {
  beforeEach(() => { publisher.pipeline.mockClear(); });

  it('names the run after its own pipeline definition rather than shipping no name at all', async () => {
    // No `registerTrigger` (so no `workGraph`) and no `instruction` in the
    // trigger payload — the state a run is in when it dies at the trigger,
    // before either was ever recorded. `runTitle` has nothing to say, and
    // the projection's own generic "Pipeline run" names nothing a person
    // reading the failed run card can act on.
    const { getSystemPipeline } = await import('../systemPipelines.js');
    const { CONVERSATION_SUMMARIZE_PIPELINE_ID } = await import('../../documents/conversationSummarizePipeline.js');
    const expectedName = getSystemPipeline(CONVERSATION_SUMMARIZE_PIPELINE_ID)!['name'];

    const { service, emit } = harness();
    service.registerTrigger({
      runId: RUN, pipelineId: CONVERSATION_SUMMARIZE_PIPELINE_ID, userId: 'dev-frank',
      triggerType: 'manual', at: AT, organizationId: 'dev-org',
    });
    await emit('pipeline:run:started', {
      runId: RUN, pipelineId: CONVERSATION_SUMMARIZE_PIPELINE_ID, at: AT,
      triggeredBy: { triggerType: 'manual', userId: 'dev-frank', payload: {} },
    });
    publisher.pipeline.mockClear();
    await emit('pipeline:run:failed', {
      runId: RUN, at: AT,
      error: { nodeId: RUN, message: 'claude exited 1: Error: Input must be provided either through stdin or as a prompt.' },
    });

    expect(published()[0]).toMatchObject({ kind: 'pipeline-run', safeLabel: expectedName });
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

describe('a deck the run rendered', () => {
  beforeEach(() => {
    publisher.pipeline.mockClear();
    publisher.document.mockClear();
    mockPresentation.mockClear();
    mockPresentation.mockResolvedValue({ documentId: 'deck-1' });
  });

  async function startedRun() {
    const { service, emit } = harness();
    service.registerTrigger({
      runId: RUN, pipelineId: 'chat-agent-loop', userId: 'dev-frank',
      triggerType: 'manual', at: AT, organizationId: 'dev-org', workGraph: plan,
    });
    await emit('pipeline:run:started', {
      runId: RUN, pipelineId: 'chat-agent-loop', at: AT,
      triggeredBy: { triggerType: 'manual', userId: 'dev-frank', payload: {} },
    });
    return { emit };
  }

  it('is published on the document source, with the run as its provenance', async () => {
    const { emit } = await startedRun();
    await emit('pipeline:step:deck:produced', {
      runId: RUN, stepId: 'deck:s1', documentId: 'deck-1', revisionId: 'v1',
      slideCount: 7, title: 'Sprint review', at: AT,
    });
    expect(publisher.document).toHaveBeenCalledTimes(1);
    expect(publisher.document.mock.calls[0][0]).toMatchObject({
      kind: 'document-revision', documentId: 'deck-1', revisionId: 'v1',
      safeLabel: 'Sprint review', producedByRunId: RUN,
    });
    expect(publisher.document.mock.calls[0][1]).toEqual({
      organizationId: 'dev-org', sourceSubject: 'dev-frank',
    });
  });

  it('says nothing for a deck whose run has no verified organization', async () => {
    const { service, emit } = harness();
    service.registerTrigger({
      runId: RUN, pipelineId: 'chat-agent-loop', userId: 'dev-frank', triggerType: 'manual', at: AT,
    });
    await emit('pipeline:run:started', {
      runId: RUN, pipelineId: 'chat-agent-loop', at: AT,
      triggeredBy: { triggerType: 'manual', userId: 'dev-frank', payload: {} },
    });
    await emit('pipeline:step:deck:produced', {
      runId: RUN, stepId: 'deck:s1', documentId: 'deck-1', revisionId: 'v1', at: AT,
    });
    expect(publisher.document).not.toHaveBeenCalled();
  });

  it('says nothing for a document the deck store does not hold', async () => {
    // The platform may only describe what it wrote. The consumer checks the
    // same thing; refusing here names the document, which a denial cannot.
    mockPresentation.mockResolvedValue(null);
    const { emit } = await startedRun();
    await emit('pipeline:step:deck:produced', {
      runId: RUN, stepId: 'deck:s1', documentId: 'someone-elses-page', revisionId: 'v1', at: AT,
    });
    expect(publisher.document).not.toHaveBeenCalled();
  });

  it('says nothing when the deck store cannot be read at all', async () => {
    mockPresentation.mockRejectedValue(new Error('store unavailable'));
    const { emit } = await startedRun();
    await emit('pipeline:step:deck:produced', {
      runId: RUN, stepId: 'deck:s1', documentId: 'deck-1', revisionId: 'v1', at: AT,
    });
    expect(publisher.document).not.toHaveBeenCalled();
  });

  it('ignores a produced event that names no revision', async () => {
    const { emit } = await startedRun();
    await emit('pipeline:step:deck:produced', { runId: RUN, documentId: 'deck-1', at: AT });
    expect(publisher.document).not.toHaveBeenCalled();
  });
});

describe('live telemetry is not a previous execution of the step', () => {
  beforeEach(() => {
    publisher.pipeline.mockClear();
    mockPresentation.mockResolvedValue({ documentId: 'deck-1' });
  });

  it('completes into the step\'s own key, not a fork-branch suffix', async () => {
    const { emit, repository } = harness();
    await emit('pipeline:run:started', {
      runId: RUN, pipelineId: 'chat-agent-loop', at: AT,
      triggeredBy: { triggerType: 'manual', userId: 'dev-frank', payload: {} },
    });
    // The loop reports a turn first: an in-flight placeholder, not a run.
    await emit('pipeline:step:agent:iteration', {
      runId: RUN, stepId: 'agent:s1', at: AT, iteration: 1, model: 'sonnet',
    });
    await emit('pipeline:step:completed', {
      runId: RUN, stepId: 'agent:s1', durationMs: 10, at: AT, output: { text: 'done' },
    });
    const written = repository.put.mock.calls
      .map((call) => (call as unknown[])[0] as { stepsSummary?: Record<string, unknown> });
    const steps = written[written.length - 1]?.stepsSummary ?? {};
    expect(Object.keys(steps)).toEqual(['agent:s1']);
    expect(steps['agent:s1']).toMatchObject({ status: 'completed', agent: { model: 'sonnet' } });
  });

  it('still separates a node that genuinely executed twice', async () => {
    const { emit, repository } = harness();
    await emit('pipeline:run:started', {
      runId: RUN, pipelineId: 'chat-agent-loop', at: AT,
      triggeredBy: { triggerType: 'manual', userId: 'dev-frank', payload: {} },
    });
    for (const at of [AT, '2026-09-21T18:01:00.000Z']) {
      await emit('pipeline:step:completed', { runId: RUN, stepId: 'fan:out', durationMs: 5, at, output: { branch: at } });
    }
    const written = repository.put.mock.calls
      .map((call) => (call as unknown[])[0] as { stepsSummary?: Record<string, unknown> });
    const steps = written[written.length - 1]?.stepsSummary ?? {};
    expect(Object.keys(steps).sort()).toEqual(['fan:out', 'fan:out:2']);
  });
});

describe('a room a step booked', () => {
  beforeEach(() => {
    publisher.pipeline.mockClear();
    publisher.meeting.mockClear();
    mockPresentation.mockResolvedValue({ documentId: 'deck-1' });
  });

  async function startedRun() {
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
    return { emit };
  }

  const meetingStep = (emit: (n: string, p: Record<string, unknown>) => Promise<void>) =>
    emit('pipeline:step:completed', {
      runId: RUN, stepId: 'agent:s1', durationMs: 10, at: AT,
      output: {
        meetingId: 'room-1',
        artifacts: [{ artifactId: 'room-1', kind: 'meeting', title: 'Platform team sync' }],
        result: { kind: 'room', roomId: 'room-1' },
      },
    });

  it('exists as a scheduled room before anybody has arrived', async () => {
    const { emit } = await startedRun();
    await meetingStep(emit);
    expect(publisher.meeting).toHaveBeenCalledTimes(1);
    expect(publisher.meeting.mock.calls[0][0]).toMatchObject({
      kind: 'scheduled', meetingId: 'room-1', safeLabel: 'Platform team sync',
      organizationId: 'dev-org', participantSubject: 'dev-frank',
    });
  });

  it('is declared as the step\'s own output, so the run reaches it', async () => {
    const { emit } = await startedRun();
    await meetingStep(emit);
    const step = publisher.pipeline.mock.calls
      .map((call) => call[0] as Record<string, unknown>)
      .find((record) => record.kind === 'pipeline-step');
    // The bare room id, which is how the meeting source projects the node.
    expect(step?.producesRefs).toEqual([{ source: 'meeting', resourceId: 'room-1' }]);
  });

  it('books nothing for a step that made no room', async () => {
    const { emit } = await startedRun();
    await emit('pipeline:step:completed', {
      runId: RUN, stepId: 'agent:s1', durationMs: 10, at: AT, output: { text: 'no room here' },
    });
    expect(publisher.meeting).not.toHaveBeenCalled();
  });

  it('declares a deck and a room from one step without confusing them', async () => {
    const { emit } = await startedRun();
    await emit('pipeline:step:completed', {
      runId: RUN, stepId: 'agent:s1', durationMs: 10, at: AT,
      output: {
        artifacts: [
          { artifactId: 'room-1', kind: 'meeting', title: 'Platform team sync' },
          { artifactId: 'deck-1', kind: 'presentation', title: 'Sprint review' },
        ],
      },
    });
    const step = publisher.pipeline.mock.calls
      .map((call) => call[0] as Record<string, unknown>)
      .find((record) => record.kind === 'pipeline-step');
    expect(step?.producesRefs).toEqual([
      { source: 'meeting', resourceId: 'room-1' },
      { source: 'document', resourceId: 'deck-1' },
    ]);
  });
});

describe('the trigger is not a step', () => {
  beforeEach(() => { publisher.pipeline.mockClear(); });

  it('projects nothing for it, and does not warn about it either', async () => {
    // It is how a run starts, never in the plan, and warning every run trains
    // people to ignore the warn that matters.
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
    await emit('pipeline:step:completed', { runId: RUN, stepId: 'trigger', durationMs: 1, at: AT });
    expect(publisher.pipeline).not.toHaveBeenCalled();
  });

  it('still says so for a step that really should have been planned', async () => {
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
    await emit('pipeline:step:completed', { runId: RUN, stepId: 'agent:s9', durationMs: 1, at: AT });
    expect(publisher.pipeline).not.toHaveBeenCalled();
  });
});

describe('NFR #34 — an at-least-once bus never regresses a finished step', () => {
  beforeEach(() => { publisher.pipeline.mockClear(); });

  function idHarness() {
    const handlers = new Map<string, (event: Record<string, unknown>) => Promise<void>>();
    const eventBus = { subscribe: (name: string, handler: (e: Record<string, unknown>) => Promise<void>) => { handlers.set(name, handler); return name; } };
    const repository = { put: jest.fn(async (_item: unknown) => undefined) };
    const service = new RunPersistenceService({
      eventBus: eventBus as never,
      repository: repository as unknown as PipelineRunRepository,
    });
    service.start();
    const deliver = async (name: string, id: string | undefined, payload: Record<string, unknown>) =>
      handlers.get(name)!({ ...(id ? { id } : {}), type: name, sourceNodeId: 'node-a', payload });
    return { service, deliver, repository };
  }

  it('the same events delivered twice ~250 ms apart leave step s1 completed', async () => {
    const { service, deliver, repository } = idHarness();
    service.registerTrigger({
      runId: RUN, pipelineId: 'chat-agent-loop', userId: 'dev-frank',
      triggerType: 'manual', at: AT, organizationId: 'dev-org', workGraph: plan,
    });
    const started = { runId: RUN, pipelineId: 'chat-agent-loop', at: AT, triggeredBy: { triggerType: 'manual', userId: 'dev-frank' } };
    const s1Start = { runId: RUN, stepId: 'agent:s1', at: '2026-09-21T18:00:01.000Z' };
    const s1Done = { runId: RUN, stepId: 'agent:s1', durationMs: 10, at: '2026-09-21T18:00:02.000Z' };

    // First delivery.
    await deliver('pipeline:run:started', 'e1', started);
    await deliver('pipeline:step:started', 'e2', s1Start);
    await deliver('pipeline:step:completed', 'e3', s1Done);
    const putsAfterFirst = repository.put.mock.calls.length;
    publisher.pipeline.mockClear();

    // Redelivery of the same ids: dropped outright.
    await deliver('pipeline:run:started', 'e1', started);
    await deliver('pipeline:step:started', 'e2', s1Start);
    await deliver('pipeline:step:completed', 'e3', s1Done);
    expect(published()).toHaveLength(0);
    expect(repository.put.mock.calls.length).toBe(putsAfterFirst);

    // A late `started` with NO id (cannot be deduped by id): still must not
    // rebuild the run or regress the finished step.
    await deliver('pipeline:run:started', undefined, started);
    await deliver('pipeline:step:started', undefined, s1Start);
    const steps = published().filter((r) => r.kind === 'pipeline-step');
    expect(steps.filter((r) => (r.step as { status: string }).status !== 'completed')).toHaveLength(0);
    const lastPut = repository.put.mock.calls.at(-1)?.[0] as { stepsSummary?: Record<string, { status: string }>; ownerNodeId?: string };
    expect(lastPut.stepsSummary?.['agent:s1']?.status).toBe('completed');
    expect(lastPut.ownerNodeId).toBe('node-a');
  });

  it('a genuinely newer start of the same step (a re-run) still shows running', async () => {
    const { service, deliver } = idHarness();
    service.registerTrigger({
      runId: RUN, pipelineId: 'chat-agent-loop', userId: 'dev-frank',
      triggerType: 'manual', at: AT, organizationId: 'dev-org', workGraph: plan,
    });
    await deliver('pipeline:run:started', 'r1', { runId: RUN, pipelineId: 'chat-agent-loop', at: AT, triggeredBy: { triggerType: 'manual', userId: 'dev-frank' } });
    await deliver('pipeline:step:completed', 'r2', { runId: RUN, stepId: 'agent:s1', durationMs: 10, at: '2026-09-21T18:00:02.000Z' });
    publisher.pipeline.mockClear();
    await deliver('pipeline:step:started', 'r3', { runId: RUN, stepId: 'agent:s1', at: '2026-09-21T18:00:05.000Z' });
    expect(published()).toMatchObject([{ step: { stepId: 'agent:s1', status: 'running' } }]);
  });
});
