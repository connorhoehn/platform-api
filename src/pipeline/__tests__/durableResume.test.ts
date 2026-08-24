// DC-25 — durable resume, platform-api half.
//
// Covers the three pieces this service adds on top of distributed-core's
// executor resume:
//   1. RunPersistenceService.handleCheckpoint — checkpoint → DDB run row,
//      with step-record slimming and the 100KB compaction ladder.
//   2. RunPersistenceService.seedResumedRun — resumed executors merge onto
//      the EXISTING step history (original timestamps preserved).
//   3. resumeInterruptedRuns — eligibility, clean skips (no checkpoint = no
//      guess), the conditional-claim double-resume guard, and the
//      resumeState handed to PipelineModule.resumeRun.
//   4. PipelineRunRepository.claimForResume — conditional-write semantics.

jest.mock('../../lib/logger', () => {
  const noop = () => undefined;
  const fakeLogger: Record<string, unknown> = {
    info: jest.fn(noop),
    warn: jest.fn(noop),
    error: jest.fn(noop),
    debug: jest.fn(noop),
    trace: jest.fn(noop),
    fatal: jest.fn(noop),
  };
  fakeLogger.child = jest.fn(() => fakeLogger);
  return {
    __esModule: true,
    default: fakeLogger,
    withContext: jest.fn(() => fakeLogger),
  };
});

import { describe, it, expect, beforeEach, jest } from '@jest/globals';
import { RunPersistenceService } from '../runPersistence';
import { resumeInterruptedRuns } from '../resumeInterruptedRuns';
import {
  PipelineRunRepository,
  type PipelineRunItem,
} from '../../repositories/PipelineRunRepository';

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

type Handler = (event: { payload: unknown }) => Promise<void>;

/** Minimal EventBus double: records subscriptions, lets tests fire events. */
function makeFakeBus() {
  const handlers = new Map<string, Handler[]>();
  return {
    subscribe: jest.fn((type: string, h: Handler) => {
      const list = handlers.get(type) ?? [];
      list.push(h);
      handlers.set(type, list);
      return `sub-${type}-${list.length}`;
    }),
    async fire(type: string, payload: unknown): Promise<void> {
      for (const h of handlers.get(type) ?? []) await h({ payload });
    },
  };
}

function makeService(maxCheckpointBytes?: number) {
  const bus = makeFakeBus();
  const puts: PipelineRunItem[] = [];
  const repository = {
    put: jest.fn(async (item: PipelineRunItem) => {
      puts.push(JSON.parse(JSON.stringify(item)) as PipelineRunItem);
    }),
  };
  const service = new RunPersistenceService({
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    eventBus: bus as any,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    repository: repository as any,
    ...(maxCheckpointBytes !== undefined ? { maxCheckpointBytes } : {}),
  });
  service.start();
  return { bus, service, repository, puts };
}

const NOW = '2026-08-24T10:00:00.000Z';

function checkpointPayload(overrides: Record<string, unknown> = {}) {
  return {
    runId: 'run-1',
    context: { steps: { t1: { big: 'x' } }, seed: 1 },
    currentStepIds: ['t1'],
    sequenceNumber: 2,
    at: NOW,
    steps: {
      trigger: {
        nodeId: 'trigger', status: 'completed', startedAt: NOW, completedAt: NOW,
        durationMs: 5, sourceHandle: 'out', input: { huge: 'should-be-slimmed' },
        output: { also: 'slimmed' }, llm: { prompt: 'p', response: 'r', tokensIn: 1, tokensOut: 1 },
      },
      t1: {
        nodeId: 't1', status: 'completed', startedAt: NOW, completedAt: NOW,
        durationMs: 7, sourceHandle: 'out', output: { big: 'x' },
      },
    },
    joinStates: { j1: [{ edgeId: 'e1', status: 'completed', sourceStepId: 'a' }] },
    firedJoins: [],
    completedStepCount: 2,
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// 1. handleCheckpoint
// ---------------------------------------------------------------------------

describe('RunPersistenceService checkpoint persistence', () => {
  let harness: ReturnType<typeof makeService>;

  beforeEach(() => {
    harness = makeService();
    harness.service.registerTrigger({
      runId: 'run-1', pipelineId: 'pipe-1', userId: 'u1', triggerType: 'manual', at: NOW,
    });
  });

  it('persists the checkpoint onto the run row with slimmed step records', async () => {
    await harness.bus.fire('pipeline:run:checkpoint', checkpointPayload());

    expect(harness.puts.length).toBeGreaterThan(0);
    const item = harness.puts[harness.puts.length - 1];
    expect(item.checkpoint).toBeDefined();
    expect(item.checkpoint!.sequenceNumber).toBe(2);
    expect(item.checkpoint!.currentStepIds).toEqual(['t1']);
    expect(item.checkpoint!.joinStates).toEqual({
      j1: [{ edgeId: 'e1', status: 'completed', sourceStepId: 'a' }],
    });

    // Step records are slimmed: resume-critical fields kept, payloads dropped.
    const trigger = item.checkpoint!.steps!['trigger'] as Record<string, unknown>;
    expect(trigger.status).toBe('completed');
    expect(trigger.sourceHandle).toBe('out');
    expect(trigger.completedAt).toBe(NOW);
    expect(trigger.input).toBeUndefined();
    expect(trigger.output).toBeUndefined();
    expect(trigger.llm).toBeUndefined();

    // Execution truth (context.steps) is preserved intact.
    expect((item.checkpoint!.context.steps as Record<string, unknown>).t1).toEqual({ big: 'x' });
  });

  it('captures the definition snapshot from the first checkpoint and keeps it', async () => {
    const definition = { id: 'pipe-1', nodes: [], edges: [] };
    await harness.bus.fire('pipeline:run:checkpoint', checkpointPayload({ sequenceNumber: 1, definition }));
    await harness.bus.fire('pipeline:run:checkpoint', checkpointPayload({ sequenceNumber: 2 }));

    const item = harness.puts[harness.puts.length - 1];
    expect(item.definitionSnapshot).toEqual(definition);
    expect(item.checkpoint!.sequenceNumber).toBe(2);
  });

  it('truncates the largest context.steps entries when over the size budget', async () => {
    const small = makeService(2_000); // 2KB budget for the test
    small.service.registerTrigger({
      runId: 'run-1', pipelineId: 'pipe-1', userId: 'u1', triggerType: 'manual', at: NOW,
    });
    const bigValue = 'y'.repeat(5_000);
    await small.bus.fire('pipeline:run:checkpoint', checkpointPayload({
      context: { steps: { fat: { blob: bigValue }, thin: { ok: 1 } }, seed: 1 },
    }));

    const item = small.puts[small.puts.length - 1];
    expect(item.checkpoint).toBeDefined();
    const ctxSteps = item.checkpoint!.context.steps as Record<string, unknown>;
    expect(ctxSteps.fat).toMatchObject({ truncated: true });
    expect(ctxSteps.thin).toEqual({ ok: 1 }); // small entries survive
    expect(item.checkpoint!.droppedContextStepKeys).toEqual(['fat']);
    expect(JSON.stringify(item.checkpoint).length).toBeLessThanOrEqual(2_000);
  });

  it('keeps the previous checkpoint when even compaction cannot fit the new one', async () => {
    const small = makeService(1_000);
    small.service.registerTrigger({
      runId: 'run-1', pipelineId: 'pipe-1', userId: 'u1', triggerType: 'manual', at: NOW,
    });
    await small.bus.fire('pipeline:run:checkpoint', checkpointPayload({ sequenceNumber: 1, context: { steps: {}, s: 1 }, steps: {} }));
    const putsAfterFirst = small.puts.length;

    // Oversized OUTSIDE context.steps — compaction can't touch it.
    await small.bus.fire('pipeline:run:checkpoint', checkpointPayload({
      sequenceNumber: 2,
      context: { steps: {}, giant: 'z'.repeat(5_000) },
      steps: {},
    }));

    // No new put for the dropped checkpoint; the row still carries seq 1.
    expect(small.puts.length).toBe(putsAfterFirst);
    expect(small.puts[small.puts.length - 1].checkpoint!.sequenceNumber).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// 2. seedResumedRun
// ---------------------------------------------------------------------------

describe('RunPersistenceService.seedResumedRun', () => {
  it('post-resume events merge onto the pre-crash history — original timestamps survive', async () => {
    const harness = makeService();
    const preCrashCompletedAt = '2026-08-24T09:00:00.000Z';

    harness.service.seedResumedRun({
      pipelineId: 'pipe-1',
      runId: 'run-1',
      status: 'running',
      triggeredBy: 'u1',
      triggerType: 'manual',
      startedAt: '2026-08-24T08:59:00.000Z',
      stepsSummary: {
        trigger: { status: 'completed', completedAt: preCrashCompletedAt, durationMs: 3 },
        t1: { status: 'completed', completedAt: preCrashCompletedAt, durationMs: 9 },
      },
      checkpoint: {
        sequenceNumber: 2, at: preCrashCompletedAt, context: { steps: {} }, currentStepIds: ['t1'],
      },
      resumedAt: NOW,
      resumeCount: 1,
      resumeClaimedBy: 'node-b',
    });

    // The resumed executor completes t2, then the run.
    await harness.bus.fire('pipeline:step:completed', {
      runId: 'run-1', stepId: 't2', durationMs: 11, output: { ok: true }, at: NOW,
    });
    await harness.bus.fire('pipeline:run:completed', { runId: 'run-1', durationMs: 999, at: NOW });

    const final = harness.puts[harness.puts.length - 1];
    expect(final.status).toBe('completed');
    // Pre-crash steps: present, UNTOUCHED timestamps (the no-re-execution proof).
    expect(final.stepsSummary!['trigger']!.completedAt).toBe(preCrashCompletedAt);
    expect(final.stepsSummary!['t1']!.completedAt).toBe(preCrashCompletedAt);
    // Post-resume step joined the same row.
    expect(final.stepsSummary!['t2']!.status).toBe('completed');
    // Resume bookkeeping survives full-row puts.
    expect(final.resumeCount).toBe(1);
    expect(final.resumeClaimedBy).toBe('node-b');
    expect(final.startedAt).toBe('2026-08-24T08:59:00.000Z');
  });
});

// ---------------------------------------------------------------------------
// 3. resumeInterruptedRuns
// ---------------------------------------------------------------------------

describe('resumeInterruptedRuns', () => {
  const baseItem = (over: Partial<PipelineRunItem> = {}): PipelineRunItem => ({
    pipelineId: 'pipe-1',
    runId: 'run-1',
    status: 'interrupted',
    triggeredBy: 'u1',
    triggerType: 'manual',
    startedAt: NOW,
    interruptedAt: NOW,
    definitionSnapshot: { id: 'pipe-1', nodes: [], edges: [] },
    checkpoint: {
      sequenceNumber: 3,
      at: NOW,
      context: { steps: { t1: { ok: 1 } } },
      currentStepIds: ['t1'],
      steps: { t1: { nodeId: 't1', status: 'completed', sourceHandle: 'out', completedAt: NOW } },
      completedStepCount: 2,
    },
    stepsSummary: { t1: { status: 'completed', completedAt: NOW, output: { fromSummary: true } } },
    ...over,
  });

  function makeDeps(items: PipelineRunItem[], opts: { claimWins?: boolean } = {}) {
    const claimWins = opts.claimWins ?? true;
    const repository = {
      listByStatus: jest.fn(async () => items),
      claimForResume: jest.fn(async (pipelineId: string, runId: string, nodeId: string, at: string) =>
        claimWins
          ? { ...items.find((i) => i.runId === runId)!, status: 'running', resumeClaimedBy: nodeId, resumedAt: at, resumeCount: 1 }
          : null,
      ),
    };
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const resumeRun = jest.fn(async (_args: any) => ({ runId: 'run-1' }));
    const module = { resumeRun };
    const seedResumedRun = jest.fn();
    const runPersistence = { seedResumedRun };
    const definitionsRepo = { get: jest.fn(async () => null) };
    return { repository, module, resumeRun, runPersistence, seedResumedRun, definitionsRepo };
  }

  const NOW_MS = Date.parse(NOW) + 60_000;

  it('claims, seeds, and resumes an eligible run under its original runId', async () => {
    const deps = makeDeps([baseItem()]);
    const result = await resumeInterruptedRuns({
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      repository: deps.repository as any, module: deps.module as any,
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      runPersistence: deps.runPersistence as any, definitionsRepo: deps.definitionsRepo,
      nodeId: 'node-b', now: () => NOW_MS,
    });

    expect(result).toMatchObject({ scanned: 1, resumed: 1, skippedNoCheckpoint: 0, claimLost: 0, failed: 0 });
    expect(deps.repository.claimForResume).toHaveBeenCalledWith('pipe-1', 'run-1', 'node-b', expect.any(String));
    expect(deps.seedResumedRun).toHaveBeenCalledTimes(1);

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const args = deps.resumeRun.mock.calls[0]![0] as any;
    expect(args.resumeState.runId).toBe('run-1'); // SAME runId
    expect(args.resumeState.currentStepIds).toEqual(['t1']);
    expect(args.resumeState.sequenceNumber).toBe(3);
    expect(args.definition).toEqual({ id: 'pipe-1', nodes: [], edges: [] });
    // Display outputs re-attached from stepsSummary onto slimmed steps.
    expect(args.resumeState.steps.t1.output).toEqual({ fromSummary: true });
  });

  it('leaves runs WITHOUT a checkpoint interrupted — a clean skip, not a guessed restart', async () => {
    const deps = makeDeps([baseItem({ checkpoint: undefined })]);
    const result = await resumeInterruptedRuns({
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      repository: deps.repository as any, module: deps.module as any,
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      runPersistence: deps.runPersistence as any, definitionsRepo: deps.definitionsRepo,
      nodeId: 'node-b', now: () => NOW_MS,
    });

    expect(result).toMatchObject({ scanned: 1, resumed: 0, skippedNoCheckpoint: 1 });
    expect(deps.repository.claimForResume).not.toHaveBeenCalled();
    expect(deps.resumeRun).not.toHaveBeenCalled();
  });

  it('a lost claim means another process owns the resume — do not double-resume', async () => {
    const deps = makeDeps([baseItem()], { claimWins: false });
    const result = await resumeInterruptedRuns({
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      repository: deps.repository as any, module: deps.module as any,
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      runPersistence: deps.runPersistence as any, definitionsRepo: deps.definitionsRepo,
      nodeId: 'node-b', now: () => NOW_MS,
    });

    expect(result).toMatchObject({ scanned: 1, resumed: 0, claimLost: 1 });
    expect(deps.resumeRun).not.toHaveBeenCalled();
    expect(deps.seedResumedRun).not.toHaveBeenCalled();
  });

  it('falls back to the definitions repo when the row lacks a snapshot; skips when both miss', async () => {
    const withRepoDef = makeDeps([baseItem({ definitionSnapshot: undefined })]);
    withRepoDef.definitionsRepo.get.mockResolvedValueOnce({ id: 'pipe-1', nodes: [], edges: [] } as never);
    const okResult = await resumeInterruptedRuns({
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      repository: withRepoDef.repository as any, module: withRepoDef.module as any,
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      runPersistence: withRepoDef.runPersistence as any, definitionsRepo: withRepoDef.definitionsRepo,
      nodeId: 'node-b', now: () => NOW_MS,
    });
    expect(okResult.resumed).toBe(1);
    expect(withRepoDef.definitionsRepo.get).toHaveBeenCalledWith('u1', 'pipe-1');

    const noDef = makeDeps([baseItem({ definitionSnapshot: undefined })]);
    const skipResult = await resumeInterruptedRuns({
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      repository: noDef.repository as any, module: noDef.module as any,
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      runPersistence: noDef.runPersistence as any, definitionsRepo: noDef.definitionsRepo,
      nodeId: 'node-b', now: () => NOW_MS,
    });
    expect(skipResult).toMatchObject({ resumed: 0, skippedNoDefinition: 1 });
  });

  it('runs interrupted outside the resume window are left alone', async () => {
    const stale = baseItem({ interruptedAt: '2026-08-24T00:00:00.000Z' });
    const deps = makeDeps([stale]);
    const result = await resumeInterruptedRuns({
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      repository: deps.repository as any, module: deps.module as any,
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      runPersistence: deps.runPersistence as any, definitionsRepo: deps.definitionsRepo,
      nodeId: 'node-b', resumeWindowMs: 60 * 60 * 1000, now: () => NOW_MS,
    });
    expect(result).toMatchObject({ scanned: 1, resumed: 0, skippedOutsideWindow: 1 });
  });
});

// ---------------------------------------------------------------------------
// 4. claimForResume conditional-write semantics
// ---------------------------------------------------------------------------

describe('PipelineRunRepository.claimForResume', () => {
  function repoWithSend(send: jest.Mock) {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return new PipelineRunRepository({ send } as any);
  }

  it('returns the post-claim item when the condition holds', async () => {
    const send = jest.fn(async () => ({
      Attributes: { pipelineId: 'p', runId: 'r', status: 'running', resumeClaimedBy: 'node-b', resumeCount: 1 },
    }));
    const repo = repoWithSend(send as jest.Mock);
    const claimed = await repo.claimForResume('p', 'r', 'node-b', NOW);
    expect(claimed).toMatchObject({ status: 'running', resumeClaimedBy: 'node-b', resumeCount: 1 });

    // The write IS conditional on status=interrupted — the guard, not decoration.
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const input = (send.mock.calls[0]![0] as any).input;
    expect(input.ConditionExpression).toBe('#status = :interrupted');
    expect(input.ExpressionAttributeValues[':interrupted']).toBe('interrupted');
    expect(input.UpdateExpression).toContain('resumeCount = if_not_exists(resumeCount, :zero) + :one');
  });

  it('returns null (claim lost) on ConditionalCheckFailedException', async () => {
    const err = Object.assign(new Error('conditional check failed'), {
      name: 'ConditionalCheckFailedException',
    });
    const send = jest.fn(async () => { throw err; });
    const repo = repoWithSend(send as jest.Mock);
    await expect(repo.claimForResume('p', 'r', 'node-b', NOW)).resolves.toBeNull();
  });

  it('re-throws non-conditional errors', async () => {
    const send = jest.fn(async () => { throw new Error('network down'); });
    const repo = repoWithSend(send as jest.Mock);
    await expect(repo.claimForResume('p', 'r', 'node-b', NOW)).rejects.toThrow('network down');
  });
});
