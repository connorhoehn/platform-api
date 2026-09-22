// Starting a loop must not make the composer wait for the planner, and a loop
// the client has been told about must never vanish.
jest.mock('../../lib/logger', () => {
  const l = { info: jest.fn(), warn: jest.fn(), error: jest.fn(), debug: jest.fn(), child: () => l };
  return { __esModule: true, default: l, withContext: () => l };
});

import {
  PENDING_PLAN, failOrphanedLoops, pendingRow, planAndTrigger, requestFromInput, startAgentLoop,
  type AgentLoopRunnerDeps,
} from '../agentLoopRunner';
import { CHAT_AGENT_LOOP_PIPELINE_ID, type LoopPlan } from '../agentLoopPlanner';
import type { ChatCommandRunInput } from '../chatCommandRegistry';
import type { PipelineRunItem } from '../../repositories/PipelineRunRepository';

const INPUT: ChatCommandRunInput = {
  trigger: '/agent',
  channel: 'chat:c:general',
  text: '/agent /meeting book the X sync 200k',
  argumentText: '/meeting book the X sync 200k',
  requestedBy: 'dev-frank',
  requestedByName: 'Frank',
};

const REAL_PLAN: LoopPlan = {
  source: 'planner',
  steps: [{ id: 's1', title: 'Book the sync', kind: 'meeting', prompt: 'book', dependsOn: [], done: 'invite exists' }],
};

const AT = new Date('2026-09-21T12:00:00.000Z');

function harness(over: Partial<AgentLoopRunnerDeps> = {}): { deps: AgentLoopRunnerDeps; rows: Map<string, PipelineRunItem>; rerun: jest.Mock } {
  const rows = new Map<string, PipelineRunItem>();
  const rerun = jest.fn().mockResolvedValue('executor-run-1');
  const deps: AgentLoopRunnerDeps = {
    putRun: async (item) => { rows.set(item.runId, item); },
    getRun: async (runId) => rows.get(runId) ?? null,
    plan: async () => REAL_PLAN,
    rerun: (...a: unknown[]) => rerun(...a) as Promise<string>,
    now: () => AT,
    ...over,
  };
  return { deps, rows, rerun };
}

describe('startAgentLoop', () => {
  it('RESOLVES BEFORE THE PLANNER DOES — the whole point of the change', async () => {
    const order: string[] = [];
    let releasePlanner!: () => void;
    const plannerFinished = new Promise<void>((r) => { releasePlanner = r; });

    const { deps } = harness({
      plan: async () => {
        await plannerFinished;
        order.push('planner');
        return REAL_PLAN;
      },
    });

    let background!: Promise<void>;
    const started = await startAgentLoop(INPUT, deps, (p) => { background = p; });
    order.push('post-answered');

    // The POST has its runId while the planner is still blocked.
    expect(started.runId).toMatch(/^[0-9a-f-]{36}$/);
    expect(order).toEqual(['post-answered']);

    releasePlanner();
    await background;
    expect(order).toEqual(['post-answered', 'planner']);
  });

  it('persists the loop BEFORE answering, so the GET can never 404 on the id it handed out', async () => {
    const { deps, rows } = harness({ plan: () => new Promise(() => { /* never settles */ }) });
    const started = await startAgentLoop(INPUT, deps, () => undefined);

    const row = rows.get(started.runId)!;
    expect(row).toMatchObject({ pipelineId: CHAT_AGENT_LOOP_PIPELINE_ID, runId: started.runId, status: 'pending', triggeredBy: 'dev-frank' });
    // Everything the pane needs while the planner is still thinking.
    expect(row.checkpoint!.context).toMatchObject({
      instruction: 'book the X sync', hints: ['meeting'], contextBudgetTokens: 200_000,
      chatChannel: 'chat:c:general', requestedBy: 'dev-frank', plan: PENDING_PLAN,
    });
  });

  it('refuses a command with nothing to do, before writing anything', async () => {
    const { deps, rows } = harness();
    await expect(startAgentLoop({ ...INPUT, argumentText: '   ' }, deps, () => undefined)).rejects.toThrow(/needs something to do/);
    expect(rows.size).toBe(0);
  });
});

describe('planAndTrigger', () => {
  it('re-enters the ordinary command path with the finished plan, and records the executor run', async () => {
    const { deps, rows, rerun } = harness();
    const request = requestFromInput(INPUT);
    rows.set('loop-1', pendingRow('loop-1', request, AT));

    await planAndTrigger('loop-1', request, INPUT, deps);

    const passed = rerun.mock.calls[0][0] as ChatCommandRunInput;
    // The plan in `extra` is what makes the start hook decline on this pass.
    expect((passed.extra!.agentLoop as { plan: LoopPlan }).plan).toEqual(REAL_PLAN);
    expect(passed.extra!.agentLoopId).toBe('loop-1');
    expect(passed.clientPostsCard).toBe(true);

    const row = rows.get('loop-1')!;
    expect(row.agentLoopRunId).toBe('executor-run-1');
    expect(row.checkpoint!.context).toMatchObject({ plan: REAL_PLAN, executorRunId: 'executor-run-1' });
  });

  it('a trigger that fails leaves a FAILED loop with the reason, never a lost one', async () => {
    const { deps, rows } = harness({ rerun: async () => { throw new Error('pipeline engine is not available'); } });
    const request = requestFromInput(INPUT);
    rows.set('loop-1', pendingRow('loop-1', request, AT));

    await planAndTrigger('loop-1', request, INPUT, deps);

    expect(rows.get('loop-1')).toMatchObject({
      status: 'failed',
      error: 'the agent loop could not be started: pipeline engine is not available',
      completedAt: AT.toISOString(),
    });
  });

  it('a fallback plan still starts a run — the planner failing is not the loop failing', async () => {
    const fallback: LoopPlan = { source: 'fallback', notice: 'Planner timed out after 90s · single step', steps: REAL_PLAN.steps };
    const { deps, rows, rerun } = harness({ plan: async () => fallback });
    const request = requestFromInput(INPUT);
    rows.set('loop-1', pendingRow('loop-1', request, AT));

    await planAndTrigger('loop-1', request, INPUT, deps);
    expect(rerun).toHaveBeenCalled();
    expect(rows.get('loop-1')!.status).not.toBe('failed');
    expect((rows.get('loop-1')!.checkpoint!.context.plan as LoopPlan).source).toBe('fallback');
  });
});

describe('failOrphanedLoops — the restart path', () => {
  const loopRow = (runId: string, over: Partial<PipelineRunItem> = {}, context: Record<string, unknown> = {}): PipelineRunItem => ({
    ...pendingRow(runId, requestFromInput(INPUT), AT),
    ...over,
    checkpoint: {
      sequenceNumber: 0, at: AT.toISOString(), currentStepIds: [],
      context: { ...pendingRow(runId, requestFromInput(INPUT), AT).checkpoint!.context, ...context },
    },
  } as PipelineRunItem);

  it('fails a loop the restart caught mid-plan, and says so', async () => {
    const written: PipelineRunItem[] = [];
    const out = await failOrphanedLoops({
      listPending: async () => [loopRow('loop-1')],
      putRun: async (item) => { written.push(item); },
      now: () => AT,
    });
    expect(out.failed).toEqual(['loop-1']);
    expect(written[0]).toMatchObject({
      status: 'failed',
      error: 'the agent loop could not be started: platform restarted while it was being planned',
      completedAt: AT.toISOString(),
    });
  });

  it('stops the devbox session the orphan left behind', async () => {
    const stopSession = jest.fn().mockResolvedValue(true);
    const out = await failOrphanedLoops({
      listPending: async () => [loopRow('loop-1', {}, { lab: { sessionId: 'sess-9', boxId: 'box-9' } })],
      putRun: async () => undefined,
      stopSession,
      now: () => AT,
    });
    expect(stopSession).toHaveBeenCalledWith('sess-9');
    expect(out.sessionsStopped).toBe(1);
  });

  it('leaves a loop that already reached the executor alone — that is an ordinary run', async () => {
    const written: PipelineRunItem[] = [];
    const out = await failOrphanedLoops({
      listPending: async () => [loopRow('loop-1', { agentLoopRunId: 'executor-run-1' })],
      putRun: async (item) => { written.push(item); },
      now: () => AT,
    });
    expect(out.failed).toEqual([]);
    expect(written).toEqual([]);
  });

  it('leaves a pending row whose plan is real, and rows of other pipelines', async () => {
    const out = await failOrphanedLoops({
      listPending: async () => [
        loopRow('loop-1', {}, { plan: REAL_PLAN }),
        loopRow('loop-2', { pipelineId: 'document-agent-edit' }),
      ],
      putRun: async () => undefined,
      now: () => AT,
    });
    expect(out.failed).toEqual([]);
  });

  it('a session that will not stop does not stop the sweep', async () => {
    const out = await failOrphanedLoops({
      listPending: async () => [
        loopRow('loop-1', {}, { lab: { sessionId: 'sess-dead' } }),
        loopRow('loop-2'),
      ],
      putRun: async () => undefined,
      stopSession: async () => { throw new Error('box already gone'); },
      now: () => AT,
    });
    expect(out.failed).toEqual(['loop-1', 'loop-2']);
    expect(out.sessionsStopped).toBe(0);
  });
});
