// social-api/src/pipeline/__tests__/createBridge.test.ts
//
// Tests the bridge that wraps distributed-core's PipelineModule behind the
// PipelineBridge contract consumed by social-api routes. Uses a minimal mock
// PipelineModule (just the methods createBridge calls) so the suite doesn't
// boot a real cluster — that's covered by bootstrap.test.ts.

import { createBridge } from '../createBridge';
import type { PipelineModule } from 'distributed-core/applications/pipeline';
import type { StepAttempt, StepSummary, PipelineRunItem, PipelineRunRepository } from '../../repositories/PipelineRunRepository';
import type { PipelineRunSnapshot, PendingApprovalRow } from '../../routes/pipelineTriggers';

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

/**
 * Local test-only extension: `summariesToStepExecutions` spreads `attempts[]`
 * onto StepExecution objects even though the DC type doesn't declare it.
 * This type captures what the bridge actually returns so tests can assert
 * on `attempts` without TypeScript errors.
 */
type StepWithAttempts = { attempts?: StepAttempt[]; [k: string]: unknown };
type StepsMap = Record<string, StepWithAttempts>;

interface MockModule {
  createResource: jest.Mock;
  getRun: jest.Mock;
  getHistory: jest.Mock;
  resolveApproval: jest.Mock;
  listActiveRuns: jest.Mock;
  cancelRun: jest.Mock;
  getPendingApprovals: jest.Mock;
  getMetrics: jest.Mock;
  getDashboardData: jest.Mock;
  resumeFromStep: jest.Mock;
  patchPausedContext: jest.Mock;
  revertContextPatch: jest.Mock;
}

function makeMockModule(): MockModule {
  return {
    createResource: jest.fn(),
    getRun: jest.fn(),
    getHistory: jest.fn(),
    resolveApproval: jest.fn(),
    listActiveRuns: jest.fn(),
    cancelRun: jest.fn(),
    getPendingApprovals: jest.fn(),
    getMetrics: jest.fn(),
    getDashboardData: jest.fn(),
    resumeFromStep: jest.fn(),
    patchPausedContext: jest.fn(),
    revertContextPatch: jest.fn(),
  };
}

function bridge(mod: MockModule) {
  // The bridge only depends on the eight methods we mock — cast through
  // unknown so we don't have to stub every PipelineModule field.
  return createBridge(mod as unknown as PipelineModule);
}

// ---------------------------------------------------------------------------
// trigger() — wraps PipelineModule.createResource
// ---------------------------------------------------------------------------

describe('createBridge — trigger()', () => {
  test('forwards args into createResource and returns the runId', async () => {
    const mod = makeMockModule();
    mod.createResource.mockResolvedValue({
      applicationData: { runId: 'run-abc' },
    });
    const b = bridge(mod);

    const result = await b.trigger!({
      pipelineId: 'p-1',
      definition: { id: 'p-1', nodes: [], edges: [] },
      triggerPayload: { hello: 'world' },
      triggeredBy: { userId: 'u-1' },
    });

    expect(result).toEqual({ runId: 'run-abc' });
    expect(mod.createResource).toHaveBeenCalledTimes(1);
    const call = mod.createResource.mock.calls[0][0];
    expect(call.applicationData.pipelineId).toBe('p-1');
    expect(call.applicationData.triggerPayload).toEqual({ hello: 'world' });
    expect(call.applicationData.triggeredBy).toEqual({ userId: 'u-1' });
  });

  test('throws when createResource omits runId', async () => {
    const mod = makeMockModule();
    mod.createResource.mockResolvedValue({ applicationData: {} });
    const b = bridge(mod);

    await expect(
      b.trigger!({ pipelineId: 'p-1', triggeredBy: { userId: 'u-1' } }),
    ).rejects.toThrow(/did not return a runId/);
  });

  test('throws when applicationData is missing entirely', async () => {
    const mod = makeMockModule();
    mod.createResource.mockResolvedValue({});
    const b = bridge(mod);

    await expect(
      b.trigger!({ pipelineId: 'p-1', triggeredBy: { userId: 'u-1' } }),
    ).rejects.toThrow(/did not return a runId/);
  });
});

// ---------------------------------------------------------------------------
// getRun() — narrows the PipelineModule shape into PipelineRunSnapshot
// ---------------------------------------------------------------------------

describe('createBridge — getRun()', () => {
  test('returns the run when the module has one with required fields', () => {
    const mod = makeMockModule();
    mod.getRun.mockReturnValue({
      runId: 'run-abc',
      pipelineId: 'p-1',
      status: 'running',
      startedAt: '2026-04-26T00:00:00Z',
    });
    const b = bridge(mod);

    const snap = b.getRun('run-abc');
    expect(snap).toMatchObject({ runId: 'run-abc', pipelineId: 'p-1', status: 'running' });
    expect(mod.getRun).toHaveBeenCalledWith('run-abc');
  });

  test('accepts the live PipelineRun shape (uses `id`, not `runId`) and projects to runId', () => {
    // PipelineExecutor.getCurrentRun() returns the internal PipelineRun shape,
    // which uses `id` for the run identifier (see distributed-core
    // `applications/pipeline/types.ts` PipelineRun.id). The bridge must accept
    // it and surface it as `runId` for downstream snapshot consumers.
    const mod = makeMockModule();
    mod.getRun.mockReturnValue({
      id: 'run-live',
      pipelineId: 'p-1',
      status: 'awaiting_approval',
      startedAt: '2026-05-01T00:00:00Z',
      steps: {
        'step-A': {
          nodeId: 'step-A',
          status: 'completed',
          output: { foo: 'bar' },
        },
      },
      context: { hello: 'world' },
    });
    const b = bridge(mod);

    const snap = b.getRun('run-live');
    expect(snap).not.toBeNull();
    expect(snap).toMatchObject({
      runId: 'run-live',
      pipelineId: 'p-1',
      status: 'awaiting_approval',
    });
    // Live steps must propagate through unchanged (with output) so the v2
    // run-detail endpoint and the pending-approvals enrichment surface the
    // mid-execution step state.
    const steps = (snap as unknown as { steps?: Record<string, { output?: unknown }> }).steps;
    expect(steps?.['step-A']?.output).toEqual({ foo: 'bar' });
    expect(
      (snap as unknown as { context?: Record<string, unknown> }).context,
    ).toEqual({ hello: 'world' });
  });

  test('returns null when the module returns null', () => {
    const mod = makeMockModule();
    mod.getRun.mockReturnValue(null);
    expect(bridge(mod).getRun('run-x')).toBeNull();
  });

  test('returns null when the module returns a malformed shape (missing both id and runId)', () => {
    const mod = makeMockModule();
    mod.getRun.mockReturnValue({ pipelineId: 'p-1' });
    expect(bridge(mod).getRun('run-x')).toBeNull();
  });

  test('returns null when the module returns a non-object', () => {
    const mod = makeMockModule();
    mod.getRun.mockReturnValue(undefined);
    expect(bridge(mod).getRun('run-x')).toBeNull();
  });

  test('does not mutate the executor-owned run object', () => {
    // The live PipelineRun reference is owned by PipelineExecutor — the
    // bridge must not write `errorCategory`/`retryability` onto it.
    const mod = makeMockModule();
    const liveRun = {
      id: 'run-live',
      pipelineId: 'p-1',
      status: 'failed',
      startedAt: '2026-05-01T00:00:00Z',
      error: { message: 'fetch failed', nodeId: 'step-A' },
    };
    mod.getRun.mockReturnValue(liveRun);

    const snap = bridge(mod).getRun('run-live') as unknown as {
      error?: { errorCategory?: string; retryability?: string };
    };
    expect(snap.error?.errorCategory).toBeDefined();
    // Original object stays clean.
    expect(
      (liveRun.error as Record<string, unknown>)['errorCategory'],
    ).toBeUndefined();
    expect(
      (liveRun.error as Record<string, unknown>)['retryability'],
    ).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// getHistory() — passes through to module.getHistory
// ---------------------------------------------------------------------------

describe('createBridge — getHistory()', () => {
  test('forwards (runId, fromVersion) and returns events', async () => {
    const events = [
      { id: 'e1', type: 'pipeline:run:started', payload: {}, timestamp: 1, sourceNodeId: 'n', version: 0 },
      { id: 'e2', type: 'pipeline:step:started', payload: { stepId: 's1' }, timestamp: 2, sourceNodeId: 'n', version: 1 },
    ];
    const mod = makeMockModule();
    mod.getHistory.mockResolvedValue(events);

    const out = await bridge(mod).getHistory('run-abc', 0);
    expect(out).toEqual(events);
    expect(mod.getHistory).toHaveBeenCalledWith('run-abc', 0);
  });

  test('returns [] when the underlying bus has no WAL (per cross-repo contract)', async () => {
    const mod = makeMockModule();
    mod.getHistory.mockResolvedValue([]);
    expect(await bridge(mod).getHistory('run-abc', 0)).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// listActiveRuns() — flattens PipelineRunResource[] into PipelineRunSnapshot[]
// ---------------------------------------------------------------------------

describe('createBridge — listActiveRuns()', () => {
  test('unwraps applicationData on each resource', () => {
    const mod = makeMockModule();
    mod.listActiveRuns.mockReturnValue([
      { applicationData: { runId: 'run-1', pipelineId: 'p-1', status: 'running' } },
      { applicationData: { runId: 'run-2', pipelineId: 'p-2', status: 'pending' } },
    ]);

    // The interface types listActiveRuns() as `T[] | Promise<T[]>` for
    // bridge implementations that prefer async; createBridge returns sync.
    // Cast to the array branch for indexing.
    const out = bridge(mod).listActiveRuns!() as PipelineRunSnapshot[];
    expect(out).toHaveLength(2);
    expect(out[0]).toMatchObject({ runId: 'run-1', pipelineId: 'p-1' });
    expect(out[1]).toMatchObject({ runId: 'run-2', pipelineId: 'p-2' });
  });

  test('skips resources without a valid run snapshot inside applicationData', () => {
    const mod = makeMockModule();
    mod.listActiveRuns.mockReturnValue([
      { applicationData: { runId: 'run-1', pipelineId: 'p-1', status: 'running' } },
      { applicationData: undefined },
      { applicationData: { pipelineId: 'p-x' } }, // missing runId
      {},
    ]);

    const out = bridge(mod).listActiveRuns!() as PipelineRunSnapshot[];
    expect(out).toHaveLength(1);
    expect(out[0].runId).toBe('run-1');
  });

  test('returns [] when no resources are in flight', () => {
    const mod = makeMockModule();
    mod.listActiveRuns.mockReturnValue([]);
    expect(bridge(mod).listActiveRuns!()).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// cancelRun() / resolveApproval() — fire-and-forget pass-through
// ---------------------------------------------------------------------------

describe('createBridge — cancelRun() / resolveApproval()', () => {
  test('cancelRun forwards the runId verbatim', () => {
    const mod = makeMockModule();
    bridge(mod).cancelRun!('run-abc', 'u-1');
    expect(mod.cancelRun).toHaveBeenCalledWith('run-abc');
  });

  test('resolveApproval forwards all five arguments', async () => {
    const mod = makeMockModule();
    await bridge(mod).resolveApproval('run-1', 'step-2', 'u-3', 'approve', 'looks good');
    expect(mod.resolveApproval).toHaveBeenCalledWith('run-1', 'step-2', 'u-3', 'approve', 'looks good');
  });

  test('resolveApproval works without an optional comment', async () => {
    const mod = makeMockModule();
    await bridge(mod).resolveApproval('run-1', 'step-2', 'u-3', 'reject');
    expect(mod.resolveApproval).toHaveBeenCalledWith('run-1', 'step-2', 'u-3', 'reject', undefined);
  });
});

// ---------------------------------------------------------------------------
// resumeFromStep() — hub#2223 pass-through
// ---------------------------------------------------------------------------

describe('createBridge — resumeFromStep()', () => {
  test('forwards (runId, fromNodeId, options) and resolves with the run snapshot', async () => {
    const mod = makeMockModule();
    const snapshot = { id: 'run-abc', status: 'completed', replayAttempts: 1 };
    mod.resumeFromStep.mockResolvedValue(snapshot);

    const out = await bridge(mod).resumeFromStep!('run-abc', 'node-7', {
      breakpoints: ['node-9'],
    });

    expect(out).toEqual(snapshot);
    expect(mod.resumeFromStep).toHaveBeenCalledWith('run-abc', 'node-7', {
      breakpoints: ['node-9'],
    });
  });

  test('forwards without options when undefined', async () => {
    const mod = makeMockModule();
    mod.resumeFromStep.mockResolvedValue({ id: 'run-abc', replayAttempts: 2 });

    await bridge(mod).resumeFromStep!('run-abc', 'node-7');

    expect(mod.resumeFromStep).toHaveBeenCalledWith('run-abc', 'node-7', undefined);
  });

  test('rejects when the underlying module rejects (e.g. InvalidRunStateError)', async () => {
    const mod = makeMockModule();
    mod.resumeFromStep.mockRejectedValue(new Error('run is not in a resumable state'));

    await expect(
      bridge(mod).resumeFromStep!('run-abc', 'node-7'),
    ).rejects.toThrow(/resumable/);
  });
});

// ---------------------------------------------------------------------------
// getPendingApprovals() — pass-through with shape cast
// ---------------------------------------------------------------------------

describe('createBridge — getPendingApprovals()', () => {
  test('returns the rows as PendingApprovalRow[]', () => {
    const mod = makeMockModule();
    const row = {
      runId: 'r1',
      stepId: 's1',
      pipelineId: 'p1',
      approvers: [{ userId: 'u1', role: 'reviewer' }],
      message: 'please review',
      requestedAt: '2026-04-26T00:00:00Z',
    };
    mod.getPendingApprovals.mockReturnValue([row]);

    const out = bridge(mod).getPendingApprovals!() as PendingApprovalRow[];
    expect(out).toHaveLength(1);
    expect(out[0]).toMatchObject(row);
  });

  test('returns [] when no approvals are pending', () => {
    const mod = makeMockModule();
    mod.getPendingApprovals.mockReturnValue([]);
    expect(bridge(mod).getPendingApprovals!()).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// getMetrics() — full pass-through of dashboard fields
// ---------------------------------------------------------------------------

describe('createBridge — getMetrics()', () => {
  test('forwards every dashboard field the module exposes', async () => {
    const mod = makeMockModule();
    mod.getMetrics.mockResolvedValue({
      moduleId: 'pipeline-x',
      runsStarted: 100,
      runsCompleted: 80,
      runsFailed: 5,
      runsActive: 12,
      runsAwaitingApproval: 7,
      avgDurationMs: 8200,
      llmTokensIn: 250_000,
      llmTokensOut: 90_000,
      avgFirstTokenLatencyMs: 420,
      asOf: '2026-04-28T12:00:00.000Z',
      activeRuns: 3, // unrelated extra — fine to ignore (route projects fields)
    });

    const out = await bridge(mod).getMetrics!();
    expect(out).toEqual({
      runsStarted: 100,
      runsCompleted: 80,
      runsFailed: 5,
      runsActive: 12,
      runsAwaitingApproval: 7,
      avgDurationMs: 8200,
      llmTokensIn: 250_000,
      llmTokensOut: 90_000,
      avgFirstTokenLatencyMs: 420,
      asOf: '2026-04-28T12:00:00.000Z',
    });
  });

  test('omits fields the module does not expose (older versions)', async () => {
    const mod = makeMockModule();
    mod.getMetrics.mockResolvedValue({ runsAwaitingApproval: 7 });

    const out = await bridge(mod).getMetrics!();
    // Legacy contract: a module that only exposes runsAwaitingApproval still
    // produces a usable bridge result. Other fields stay absent (route → null).
    expect(out).toEqual({ runsAwaitingApproval: 7 });
  });

  test('coerces missing runsAwaitingApproval to 0 (legacy callers depend on this)', async () => {
    const mod = makeMockModule();
    mod.getMetrics.mockResolvedValue({});

    const out = await bridge(mod).getMetrics!();
    expect(out).toEqual({ runsAwaitingApproval: 0 });
  });

  test('drops non-finite numerics rather than salvaging them', async () => {
    const mod = makeMockModule();
    mod.getMetrics.mockResolvedValue({
      runsAwaitingApproval: '5', // string — not silently parsed
      runsStarted: Number.NaN,
      runsCompleted: Number.POSITIVE_INFINITY,
      avgFirstTokenLatencyMs: 'fast', // garbage — dropped, not parsed
    });

    const out = await bridge(mod).getMetrics!();
    // String runsAwaitingApproval coerces to 0 (legacy), other garbage drops.
    expect(out).toEqual({ runsAwaitingApproval: 0 });
  });
});

// ---------------------------------------------------------------------------
// createBridge — listRuns() — DDB-backed run history mapping
// ---------------------------------------------------------------------------

function makeRunRepository(items: PipelineRunItem[] = []): PipelineRunRepository {
  return {
    listByPipeline: jest.fn(async () => items),
    get: jest.fn(async () => null),
    put: jest.fn(async () => {}),
    listInterrupted: jest.fn(async () => []),
    listActive: jest.fn(async () => []),
  } as unknown as PipelineRunRepository;
}

function bridgeWithRepo(mod: MockModule, repo: PipelineRunRepository) {
  return createBridge(
    mod as unknown as PipelineModule,
    null,
    repo,
  );
}

function minimalItem(overrides: Partial<PipelineRunItem> = {}): PipelineRunItem {
  return {
    pipelineId: 'p-1',
    runId: 'run-1',
    status: 'completed',
    triggeredBy: 'user-42',
    triggerType: 'manual',
    startedAt: '2026-05-01T10:00:00.000Z',
    completedAt: '2026-05-01T10:00:05.000Z',
    ...overrides,
  };
}

describe('createBridge — listRuns()', () => {
  test('returns empty list when no repository is wired', async () => {
    const mod = makeMockModule();
    const b = createBridge(mod as unknown as PipelineModule);
    const out = await b.listRuns!('p-1', { limit: 10 });
    expect(out).toEqual({ runs: [], nextCursor: undefined });
  });

  test('calls listByPipeline with pipelineId and limit', async () => {
    const mod = makeMockModule();
    const repo = makeRunRepository();
    const b = bridgeWithRepo(mod, repo);
    await b.listRuns!('p-42', { limit: 15 });
    expect(repo.listByPipeline).toHaveBeenCalledWith('p-42', { limit: 15 });
  });

  test('defaults limit to 50 when opts omitted', async () => {
    const mod = makeMockModule();
    const repo = makeRunRepository();
    const b = bridgeWithRepo(mod, repo);
    await (b.listRuns as (id: string, opts?: { limit?: number }) => Promise<unknown>)('p-1');
    expect(repo.listByPipeline).toHaveBeenCalledWith('p-1', { limit: 50 });
  });

  test('maps PipelineRunItem core fields to PipelineRunSnapshot', async () => {
    const mod = makeMockModule();
    const item = minimalItem();
    const repo = makeRunRepository([item]);
    const b = bridgeWithRepo(mod, repo);
    const { runs } = await b.listRuns!('p-1', { limit: 1 });
    expect(runs).toHaveLength(1);
    const snap = runs[0]!;
    expect(snap.runId).toBe('run-1');
    expect(snap.pipelineId).toBe('p-1');
    expect(snap.status).toBe('completed');
    expect(snap.startedAt).toBe('2026-05-01T10:00:00.000Z');
    expect(snap.completedAt).toBe('2026-05-01T10:00:05.000Z');
  });

  test('maps triggeredBy userId into snapshot.triggeredBy', async () => {
    const mod = makeMockModule();
    const item = minimalItem({ triggeredBy: 'user-77', triggerType: 'webhook' });
    const repo = makeRunRepository([item]);
    const b = bridgeWithRepo(mod, repo);
    const { runs } = await b.listRuns!('p-1', { limit: 1 });
    expect(runs[0]!.triggeredBy).toEqual({
      userId: 'user-77',
      triggerType: 'webhook',
      payload: {},
    });
  });

  test('sets userId=undefined when triggeredBy is "system"', async () => {
    const mod = makeMockModule();
    const item = minimalItem({ triggeredBy: 'system', triggerType: 'schedule' });
    const repo = makeRunRepository([item]);
    const b = bridgeWithRepo(mod, repo);
    const { runs } = await b.listRuns!('p-1', { limit: 1 });
    expect(runs[0]!.triggeredBy?.userId).toBeUndefined();
    expect(runs[0]!.triggeredBy?.triggerType).toBe('schedule');
  });

  test('includes interruptedAt only when present on item', async () => {
    const mod = makeMockModule();
    const noInterrupt = minimalItem();
    const withInterrupt = minimalItem({
      runId: 'run-2',
      interruptedAt: '2026-05-01T10:00:03.000Z',
      status: 'interrupted',
    });
    const repo = makeRunRepository([noInterrupt, withInterrupt]);
    const b = bridgeWithRepo(mod, repo);
    const { runs } = await b.listRuns!('p-1', { limit: 10 });
    const plain = runs.find((r) => r.runId === 'run-1')!;
    const interrupted = runs.find((r) => r.runId === 'run-2')!;
    expect('interruptedAt' in plain).toBe(false);
    expect(interrupted).toMatchObject({ interruptedAt: '2026-05-01T10:00:03.000Z' });
  });

  test('parses error string into structured error with classification', async () => {
    const mod = makeMockModule();
    const item = minimalItem({
      status: 'failed',
      error: 'action-node-1: Webhook timed out after 30s',
    });
    const repo = makeRunRepository([item]);
    const b = bridgeWithRepo(mod, repo);
    const { runs } = await b.listRuns!('p-1', { limit: 1 });
    const snap = runs[0]!;
    expect(snap.error).toMatchObject({
      nodeId: 'action-node-1',
      message: 'Webhook timed out after 30s',
      errorCategory: 'TimeoutError',
      retryability: 'retryable',
    });
  });

  test('error is undefined when item.error is absent', async () => {
    const mod = makeMockModule();
    const item = minimalItem();
    const repo = makeRunRepository([item]);
    const b = bridgeWithRepo(mod, repo);
    const { runs } = await b.listRuns!('p-1', { limit: 1 });
    expect(runs[0]!.error).toBeUndefined();
  });

  test('maps stepsSummary to StepExecution records', async () => {
    const mod = makeMockModule();
    const item = minimalItem({
      stepsSummary: {
        'node-llm': {
          status: 'completed',
          startedAt: '2026-05-01T10:00:01.000Z',
          completedAt: '2026-05-01T10:00:04.000Z',
          durationMs: 3000,
          output: { answer: 'yes' },
        },
        'node-action': {
          status: 'failed',
          error: 'Network unreachable',
        },
      },
    });
    const repo = makeRunRepository([item]);
    const b = bridgeWithRepo(mod, repo);
    const { runs } = await b.listRuns!('p-1', { limit: 1 });
    const steps = runs[0]!.steps as StepsMap | undefined;
    expect(steps!['node-llm']).toMatchObject({
      nodeId: 'node-llm',
      status: 'completed',
      durationMs: 3000,
      output: { answer: 'yes' },
    });
    expect(steps!['node-action']).toMatchObject({
      nodeId: 'node-action',
      status: 'failed',
      error: 'Network unreachable',
    });
  });

  test('steps is empty object when stepsSummary absent', async () => {
    const mod = makeMockModule();
    const repo = makeRunRepository([minimalItem()]);
    const b = bridgeWithRepo(mod, repo);
    const { runs } = await b.listRuns!('p-1', { limit: 1 });
    expect(runs[0]!.steps).toEqual({});
  });

  test('returns nextCursor undefined (pagination NYI)', async () => {
    const mod = makeMockModule();
    const repo = makeRunRepository([minimalItem()]);
    const b = bridgeWithRepo(mod, repo);
    const out = await b.listRuns!('p-1', { limit: 10 });
    expect(out.nextCursor).toBeUndefined();
  });

  // ---
  // attempts[] propagation through summariesToStepExecutions / readAttempts.
  // ---

  test('attempts[] from stepsSummary propagate into StepExecution', async () => {
    // Explicit attempts array (post-#1280 rows) — readAttempts returns them directly.
    const mod = makeMockModule();
    const item = minimalItem({
      stepsSummary: {
        'node-llm': {
          status: 'completed',
          startedAt: '2026-05-01T10:00:01.000Z',
          completedAt: '2026-05-01T10:00:04.000Z',
          durationMs: 3000,
          attempts: [
            {
              attemptNumber: 1,
              startedAt: '2026-05-01T10:00:01.000Z',
              completedAt: '2026-05-01T10:00:02.000Z',
              durationMs: 1000,
              error: 'temporary glitch',
              idempotencyKey: 'run-1:node-llm:1',
            },
            {
              attemptNumber: 2,
              startedAt: '2026-05-01T10:00:03.000Z',
              completedAt: '2026-05-01T10:00:04.000Z',
              durationMs: 1000,
              output: { answer: 'yes' },
              idempotencyKey: 'run-1:node-llm:2',
            },
          ],
        },
      },
    });
    const repo = makeRunRepository([item]);
    const b = bridgeWithRepo(mod, repo);
    const { runs } = await b.listRuns!('p-1', { limit: 1 });
    const step = (runs[0]!.steps as StepsMap | undefined)?.['node-llm'];
    expect(step?.attempts).toHaveLength(2);
    expect(step?.attempts![0]).toMatchObject({ attemptNumber: 1, error: 'temporary glitch' });
    expect(step?.attempts![1]).toMatchObject({ attemptNumber: 2, output: { answer: 'yes' } });
  });

  test('legacy step (no attempts[]) gets synthetic single attempt back-filled', async () => {
    // Pre-#1280 row: stepsSummary has startedAt/completedAt but no attempts[]
    // → readAttempts synthesises one attempt so the frontend's "attempt 1 of 1" renders.
    const mod = makeMockModule();
    const item = minimalItem({
      stepsSummary: {
        'node-action': {
          status: 'failed',
          startedAt: '2026-05-01T10:00:00.000Z',
          completedAt: '2026-05-01T10:00:01.000Z',
          durationMs: 1000,
          error: 'timed out',
        },
      },
    });
    const repo = makeRunRepository([item]);
    const b = bridgeWithRepo(mod, repo);
    const { runs } = await b.listRuns!('p-1', { limit: 1 });
    const step = (runs[0]!.steps as StepsMap | undefined)?.['node-action'];
    expect(step?.attempts).toHaveLength(1);
    expect(step?.attempts![0]).toMatchObject({
      attemptNumber: 1,
      startedAt: '2026-05-01T10:00:00.000Z',
      error: 'timed out',
      idempotencyKey: 'run-1:node-action:1',
    });
  });

  test('step with no timing or status has no attempts (empty-shell summary)', async () => {
    // Bare-minimum summary with nothing to back-fill — readAttempts returns [].
    const mod = makeMockModule();
    const item = minimalItem({
      stepsSummary: {
        'node-placeholder': {} as StepSummary,
      },
    });
    const repo = makeRunRepository([item]);
    const b = bridgeWithRepo(mod, repo);
    const { runs } = await b.listRuns!('p-1', { limit: 1 });
    const step = (runs[0]!.steps as StepsMap | undefined)?.['node-placeholder'];
    // attempts should be absent (not an empty array — we omit rather than inject)
    expect(step?.attempts).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// getDashboardData() — pass-through of PipelineModule.getDashboardData()
// ---------------------------------------------------------------------------

describe('createBridge — getDashboardData()', () => {
  test('forwards the module payload verbatim', async () => {
    const mod = makeMockModule();
    const payload = {
      moduleId: 'pipeline',
      moduleName: 'Pipeline',
      state: 'running',
      summary: {
        totalResources: 7,
        healthyResources: 5,
        activeConnections: 1,
        throughput: 2.3,
      },
      charts: [
        { title: 'Run Outcomes', type: 'bar', data: { completed: 5, failed: 1, active: 1 } },
      ],
      alerts: [],
    };
    mod.getDashboardData.mockResolvedValue(payload);

    const out = await bridge(mod).getDashboardData!();
    expect(out).toEqual(payload);
    expect(mod.getDashboardData).toHaveBeenCalledTimes(1);
  });

  test('propagates errors from the module', async () => {
    const mod = makeMockModule();
    mod.getDashboardData.mockRejectedValue(new Error('module is down'));
    await expect(bridge(mod).getDashboardData!()).rejects.toThrow('module is down');
  });
});

// ---------------------------------------------------------------------------
// patchPausedContext() / revertContextPatch() — hub#2226 pass-through
// ---------------------------------------------------------------------------

describe('createBridge — patchPausedContext()', () => {
  test('forwards (runId, stepId, patch, appliedBy) and returns the PatchResult', async () => {
    const mod = makeMockModule();
    const result = {
      patchId: 'patch-1',
      before: { foo: 1 },
      after: { foo: 2 },
      validation: { ok: true },
    };
    mod.patchPausedContext.mockResolvedValue(result);

    const out = await bridge(mod).patchPausedContext!(
      'run-abc',
      'step-1',
      { mode: 'merge', values: { foo: 2 } },
      'operator@example.com',
    );

    expect(out).toEqual(result);
    expect(mod.patchPausedContext).toHaveBeenCalledWith(
      'run-abc',
      'step-1',
      { mode: 'merge', values: { foo: 2 } },
      'operator@example.com',
    );
  });

  test('forwards a validator-rejected result verbatim (validation.ok === false)', async () => {
    const mod = makeMockModule();
    const result = {
      patchId: 'patch-2',
      before: { foo: 1 },
      after: { foo: 1 },
      validation: { ok: false, reason: 'foo must be positive' },
    };
    mod.patchPausedContext.mockResolvedValue(result);

    const out = await bridge(mod).patchPausedContext!(
      'run-abc',
      'step-1',
      { mode: 'merge', values: { foo: -1 } },
    );
    expect(out.validation).toEqual({ ok: false, reason: 'foo must be positive' });
  });

  test('rejects when the underlying module rejects (e.g. InvalidRunStateError)', async () => {
    const mod = makeMockModule();
    mod.patchPausedContext.mockRejectedValue(new Error('run is not paused at step-1'));
    await expect(
      bridge(mod).patchPausedContext!('run-abc', 'step-1', {
        mode: 'merge',
        values: {},
      }),
    ).rejects.toThrow(/not paused/);
  });
});

describe('createBridge — revertContextPatch()', () => {
  test('forwards (runId, stepId, patchId) and resolves on success', async () => {
    const mod = makeMockModule();
    mod.revertContextPatch.mockResolvedValue(undefined);

    await bridge(mod).revertContextPatch!('run-abc', 'step-1', 'patch-1');
    expect(mod.revertContextPatch).toHaveBeenCalledWith('run-abc', 'step-1', 'patch-1');
  });

  test('rejects when the underlying module rejects', async () => {
    const mod = makeMockModule();
    mod.revertContextPatch.mockRejectedValue(new Error('patch unknown'));
    await expect(
      bridge(mod).revertContextPatch!('run-abc', 'step-1', 'nope'),
    ).rejects.toThrow(/unknown/);
  });
});
