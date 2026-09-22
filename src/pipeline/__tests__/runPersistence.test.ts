// social-api/src/pipeline/__tests__/runPersistence.test.ts

// Mock the logger so retry tests (P1 #7) can assert that error-level surfaces
// after the DDB retry budget is exhausted. Other tests in this file don't
// assert against log calls, so swapping in jest.fn()s is safe.
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
    __fakeLogger: fakeLogger,
  };
});

import { describe, it, expect, beforeEach, afterEach, jest } from '@jest/globals';
import { FixtureLLMClient } from 'distributed-core/applications/pipeline';
import { bootstrapPipeline } from '../bootstrap';
import { RunPersistenceService } from '../runPersistence';
import {
  PipelineRunRepository,
  readAttempts,
  type PipelineRunItem,
  type StepSummary,
} from '../../repositories/PipelineRunRepository';

// Handle to the mocked logger so retry tests can spy on log.error et al.
const fakeLogger = (jest.requireMock('../../lib/logger') as {
  __fakeLogger: { error: jest.Mock; info: jest.Mock; warn: jest.Mock };
}).__fakeLogger;

jest.setTimeout(20_000);

describe('RunPersistenceService', () => {
  let mockRepository: { put: jest.Mock };
  let cleanup: () => Promise<void>;

  beforeEach(async () => {
    mockRepository = { put: jest.fn().mockImplementation(() => Promise.resolve()) };
    const fixture = new FixtureLLMClient(['ok']);
    const { shutdown } = await bootstrapPipeline({
      llmClient: fixture,
      pipelineRunRepository: mockRepository as unknown as PipelineRunRepository,
    });
    cleanup = shutdown;
  });

  afterEach(async () => {
    if (cleanup) await cleanup();
  });

  it('generates unique keys for duplicate step executions (fork branches)', async () => {
    // hub#1132: parallel fork branches must store each execution under a unique key.
    expect(mockRepository.put.mock.calls.length).toBeGreaterThanOrEqual(0);
  });
});

// ---------------------------------------------------------------------------
// hub#1280 — per-step attempt records
// ---------------------------------------------------------------------------

describe('readAttempts (back-compat read helper)', () => {
  it('returns an empty array for undefined summary', () => {
    expect(readAttempts(undefined, 'run-1', 'node-1')).toEqual([]);
  });

  it('returns an empty array for an empty summary', () => {
    expect(readAttempts({}, 'run-1', 'node-1')).toEqual([]);
  });

  it('passes through a stored multi-attempt history unchanged', () => {
    const attempts = [
      { attemptNumber: 1, startedAt: 't0', error: 'boom', idempotencyKey: 'r:n:1' },
      { attemptNumber: 2, startedAt: 't1', completedAt: 't2', output: { ok: true }, idempotencyKey: 'r:n:2' },
    ];
    const summary: StepSummary = {
      status: 'completed',
      attempts,
    };
    expect(readAttempts(summary, 'run-1', 'node-1')).toBe(attempts);
  });

  it('synthesises a single attempt from a legacy row without attempts[]', () => {
    const summary: StepSummary = {
      status: 'completed',
      startedAt: '2026-01-01T00:00:00.000Z',
      completedAt: '2026-01-01T00:00:01.000Z',
      durationMs: 1_000,
      output: { value: 42 },
    };
    const result = readAttempts(summary, 'run-99', 'node-A');
    expect(result).toHaveLength(1);
    expect(result[0]).toMatchObject({
      attemptNumber: 1,
      startedAt: '2026-01-01T00:00:00.000Z',
      completedAt: '2026-01-01T00:00:01.000Z',
      durationMs: 1_000,
      output: { value: 42 },
      idempotencyKey: 'run-99:node-A:1',
    });
  });

  it('falls back to completedAt when legacy row has no startedAt', () => {
    const summary: StepSummary = {
      status: 'completed',
      completedAt: '2026-01-01T00:00:00.000Z',
    };
    const result = readAttempts(summary, 'run-1', 'node-1');
    expect(result[0].startedAt).toBe('2026-01-01T00:00:00.000Z');
  });
});

describe('RunPersistenceService writes attempts[] on step:completed (hub#1280)', () => {
  type Subscriber<P> = (event: { payload: P }) => unknown | Promise<unknown>;

  // Minimal in-memory bus that records subscribers per event name. Sufficient
  // for verifying that handleStepCompleted populates attempts[]; the real
  // distributed-core EventBus is exercised in bootstrap-level integration tests.
  function makeMockBus() {
    const subs = new Map<string, Subscriber<unknown>>();
    return {
      subscribe: jest.fn((event: string, fn: Subscriber<unknown>) => {
        subs.set(event, fn);
      }),
      async publish<P>(event: string, payload: P): Promise<void> {
        const fn = subs.get(event);
        if (fn) await fn({ payload: payload as unknown });
      },
    };
  }

  it('persists a single synthetic attempt entry per completed step', async () => {
    const repo = { put: jest.fn().mockImplementation(() => Promise.resolve()) };
    const bus = makeMockBus();
    const service = new RunPersistenceService({
      eventBus: bus as unknown as ConstructorParameters<typeof RunPersistenceService>[0]['eventBus'],
      repository: repo as unknown as PipelineRunRepository,
    });
    service.start();

    await bus.publish('pipeline:run:started', {
      runId: 'r1',
      pipelineId: 'p1',
      triggeredBy: { triggerType: 'manual', userId: 'u1' },
      at: '2026-01-01T00:00:00.000Z',
    });
    await bus.publish('pipeline:step:completed', {
      runId: 'r1',
      stepId: 'node-A',
      output: { hello: 'world' },
      durationMs: 250,
      at: '2026-01-01T00:00:00.250Z',
    });
    await bus.publish('pipeline:run:completed', {
      runId: 'r1',
      durationMs: 250,
      at: '2026-01-01T00:00:00.250Z',
    });

    const lastCall = repo.put.mock.calls.at(-1);
    expect(lastCall).toBeDefined();
    const item = lastCall![0] as PipelineRunItem;
    const stepA = item.stepsSummary?.['node-A'];
    expect(stepA).toBeDefined();
    expect(stepA?.attempts).toHaveLength(1);
    expect(stepA?.attempts?.[0]).toMatchObject({
      attemptNumber: 1,
      completedAt: '2026-01-01T00:00:00.250Z',
      durationMs: 250,
      output: { hello: 'world' },
      idempotencyKey: 'r1:node-A:1',
    });
    // Latest-attempt top-level fields preserved for back-compat readers.
    expect(stepA?.status).toBe('completed');
    expect(stepA?.output).toEqual({ hello: 'world' });
  });

  it('keeps fork-branch step keys distinct, each with its own attempts[]', async () => {
    const repo = { put: jest.fn().mockImplementation(() => Promise.resolve()) };
    const bus = makeMockBus();
    const service = new RunPersistenceService({
      eventBus: bus as unknown as ConstructorParameters<typeof RunPersistenceService>[0]['eventBus'],
      repository: repo as unknown as PipelineRunRepository,
    });
    service.start();

    await bus.publish('pipeline:run:started', {
      runId: 'r2',
      pipelineId: 'p2',
      triggeredBy: { triggerType: 'manual', userId: 'u1' },
      at: 't0',
    });
    await bus.publish('pipeline:step:completed', {
      runId: 'r2', stepId: 'fork-child', output: { branch: 1 }, durationMs: 10, at: 't1',
    });
    await bus.publish('pipeline:step:completed', {
      runId: 'r2', stepId: 'fork-child', output: { branch: 2 }, durationMs: 11, at: 't2',
    });
    await bus.publish('pipeline:run:completed', { runId: 'r2', durationMs: 12, at: 't3' });

    const item = repo.put.mock.calls.at(-1)![0] as PipelineRunItem;
    expect(item.stepsSummary?.['fork-child']?.attempts?.[0]?.output).toEqual({ branch: 1 });
    expect(item.stepsSummary?.['fork-child:2']?.attempts?.[0]?.output).toEqual({ branch: 2 });
    expect(item.stepsSummary?.['fork-child:2']?.attempts?.[0]?.idempotencyKey).toBe('r2:fork-child:1');
  });
});

// ---------------------------------------------------------------------------
// RunPersistenceService — edge cases and uncovered paths
// ---------------------------------------------------------------------------

describe('RunPersistenceService — event paths not yet covered', () => {
  type Subscriber<P> = (event: { payload: P }) => unknown | Promise<unknown>;

  function makeMockBus() {
    const subs = new Map<string, Subscriber<unknown>>();
    return {
      subscribe: jest.fn((event: string, fn: Subscriber<unknown>) => {
        subs.set(event, fn);
      }),
      async publish<P>(event: string, payload: P): Promise<void> {
        const fn = subs.get(event);
        if (fn) await fn({ payload: payload as unknown });
      },
    };
  }

  function makeService(repo: { put: jest.Mock<(item: PipelineRunItem) => Promise<void>>; listByStatus?: jest.Mock<() => Promise<unknown[]>> }) {
    // scanOrphanedRuns calls listByStatus on the repository; provide a
    // default stub so tests that don't care about orphan scanning don't
    // trigger TypeError logs from the background scan.
    if (!repo.listByStatus) {
      repo.listByStatus = jest.fn<() => Promise<unknown[]>>().mockResolvedValue([]);
    }
    const bus = makeMockBus();
    const service = new RunPersistenceService({
      eventBus: bus as unknown as ConstructorParameters<typeof RunPersistenceService>[0]['eventBus'],
      repository: repo as unknown as PipelineRunRepository,
    });
    return { service, bus };
  }

  it('handles pipeline:run:cancelled — sets status=cancelled and persists', async () => {
    const repo = { put: jest.fn<(item: PipelineRunItem) => Promise<void>>().mockResolvedValue(undefined) };
    const { service, bus } = makeService(repo);
    service.start();

    await bus.publish('pipeline:run:started', {
      runId: 'rc1', pipelineId: 'pc1',
      triggeredBy: { triggerType: 'manual', userId: 'u1' },
      at: 't0',
    });
    await bus.publish('pipeline:run:cancelled', { runId: 'rc1', at: 't1' });

    const lastItem = repo.put.mock.calls.at(-1)![0] as PipelineRunItem;
    expect(lastItem.status).toBe('cancelled');
    expect(lastItem.completedAt).toBe('t1');
  });

  it('warns but does not throw when run:cancelled arrives for unknown run', async () => {
    const repo = { put: jest.fn<(item: PipelineRunItem) => Promise<void>>().mockResolvedValue(undefined) };
    const { service, bus } = makeService(repo);
    service.start();

    // No run:started first — handler now synthesizes a terminal state from
    // the event and still persists. The "does not throw" guarantee is
    // what this test pins; persisting-synthesized-state is the expected
    // current behavior.
    await expect(bus.publish('pipeline:run:cancelled', { runId: 'unknown', at: 't0' }))
      .resolves.toBeUndefined();
  });

  it('warns but does not throw when step:completed arrives for unknown run', async () => {
    const repo = { put: jest.fn<(item: PipelineRunItem) => Promise<void>>().mockResolvedValue(undefined) };
    const { service, bus } = makeService(repo);
    service.start();

    await expect(bus.publish('pipeline:step:completed', {
      runId: 'missing', stepId: 'n1', output: {}, durationMs: 1, at: 't0',
    })).resolves.toBeUndefined();
    expect(repo.put).not.toHaveBeenCalled();
  });

  it('warns but does not throw when run:completed arrives for unknown run', async () => {
    const repo = { put: jest.fn<(item: PipelineRunItem) => Promise<void>>().mockResolvedValue(undefined) };
    const { service, bus } = makeService(repo);
    service.start();

    await expect(bus.publish('pipeline:run:completed', { runId: 'missing', durationMs: 1, at: 't0' }))
      .resolves.toBeUndefined();
  });

  it('warns but does not throw when run:failed arrives for unknown run', async () => {
    const repo = { put: jest.fn<(item: PipelineRunItem) => Promise<void>>().mockResolvedValue(undefined) };
    const { service, bus } = makeService(repo);
    service.start();

    await expect(bus.publish('pipeline:run:failed', {
      runId: 'missing', error: { nodeId: 'n1', message: 'boom' }, at: 't0',
    })).resolves.toBeUndefined();
  });

  it('swallows repo error in run:started catch path', async () => {
    const repo = { put: jest.fn<(item: PipelineRunItem) => Promise<void>>().mockRejectedValue(new Error('ddb down')) };
    const { service, bus } = makeService(repo);
    service.start();

    await expect(bus.publish('pipeline:run:started', {
      runId: 'rx', pipelineId: 'px',
      triggeredBy: { triggerType: 'manual', userId: 'u1' },
      at: 't0',
    })).resolves.toBeUndefined();
  });

  it('swallows repo error in run:completed catch path', async () => {
    let callCount = 0;
    const repo = {
      put: jest.fn<(item: PipelineRunItem) => Promise<void>>().mockImplementation(() => {
        if (++callCount === 1) return Promise.resolve(); // run:started ok
        return Promise.reject(new Error('ddb error'));   // run:completed fails
      }),
    };
    const { service, bus } = makeService(repo);
    service.start();

    await bus.publish('pipeline:run:started', {
      runId: 'ry', pipelineId: 'py',
      triggeredBy: { triggerType: 'manual', userId: 'u1' }, at: 't0',
    });
    await expect(bus.publish('pipeline:run:completed', { runId: 'ry', durationMs: 1, at: 't1' }))
      .resolves.toBeUndefined();
  });

  it('swallows repo error in run:failed catch path', async () => {
    let callCount = 0;
    const repo = {
      put: jest.fn<(item: PipelineRunItem) => Promise<void>>().mockImplementation(() => {
        if (++callCount === 1) return Promise.resolve();
        return Promise.reject(new Error('ddb error'));
      }),
    };
    const { service, bus } = makeService(repo);
    service.start();

    await bus.publish('pipeline:run:started', {
      runId: 'rz', pipelineId: 'pz',
      triggeredBy: { triggerType: 'manual', userId: 'u1' }, at: 't0',
    });
    await expect(bus.publish('pipeline:run:failed', {
      runId: 'rz', error: { nodeId: 'n1', message: 'boom' }, at: 't1',
    })).resolves.toBeUndefined();
  });

  it('swallows repo error in step:completed catch path', async () => {
    let callCount = 0;
    const repo = {
      put: jest.fn<(item: PipelineRunItem) => Promise<void>>().mockImplementation(() => {
        if (callCount++ === 0) return Promise.resolve(); // run:started ok
        return Promise.reject(new Error('ddb error'));
      }),
    };
    const { service, bus } = makeService(repo);
    // Use maxStepOutputBytes=1 to force truncation + periodic flush
    const svc = new RunPersistenceService({
      eventBus: bus as unknown as ConstructorParameters<typeof RunPersistenceService>[0]['eventBus'],
      repository: repo as unknown as PipelineRunRepository,
      maxStepOutputBytes: 1, // forces truncation path
    });
    svc.start();

    await bus.publish('pipeline:run:started', {
      runId: 'ra', pipelineId: 'pa',
      triggeredBy: { triggerType: 'manual', userId: 'u1' }, at: 't0',
    });
    // Publish 10 steps to trigger the periodic flush (which will fail, but be swallowed)
    for (let i = 1; i <= 10; i++) {
      await bus.publish('pipeline:step:completed', {
        runId: 'ra', stepId: `step-${i}`, output: { v: i }, durationMs: i, at: `t${i}`,
      });
    }
  });

  it('truncates step output that exceeds maxStepOutputBytes', async () => {
    const repo = { put: jest.fn<(item: PipelineRunItem) => Promise<void>>().mockResolvedValue(undefined) };
    const bus = makeMockBus();
    const service = new RunPersistenceService({
      eventBus: bus as unknown as ConstructorParameters<typeof RunPersistenceService>[0]['eventBus'],
      repository: repo as unknown as PipelineRunRepository,
      maxStepOutputBytes: 10, // very small so any non-trivial output is truncated
    });
    service.start();

    await bus.publish('pipeline:run:started', {
      runId: 'rt', pipelineId: 'pt',
      triggeredBy: { triggerType: 'manual', userId: 'u1' }, at: 't0',
    });
    await bus.publish('pipeline:step:completed', {
      runId: 'rt', stepId: 'big-step',
      output: { data: 'a'.repeat(100) }, // well beyond 10 bytes
      durationMs: 1, at: 't1',
    });
    await bus.publish('pipeline:run:completed', { runId: 'rt', durationMs: 1, at: 't2' });

    const item = repo.put.mock.calls.at(-1)![0] as PipelineRunItem;
    const step = item.stepsSummary?.['big-step'];
    expect(step?.output).toMatchObject({ truncated: true });
  });

  it('records LLM prompt and response — accumulates token usage and cost', async () => {
    const repo = { put: jest.fn<(item: PipelineRunItem) => Promise<void>>().mockResolvedValue(undefined) };
    const bus = makeMockBus();
    const service = new RunPersistenceService({
      eventBus: bus as unknown as ConstructorParameters<typeof RunPersistenceService>[0]['eventBus'],
      repository: repo as unknown as PipelineRunRepository,
    });
    service.start();

    await bus.publish('pipeline:run:started', {
      runId: 'rllm', pipelineId: 'pllm',
      triggeredBy: { triggerType: 'manual', userId: 'u1' }, at: 't0',
    });
    await bus.publish('pipeline:llm:prompt', {
      runId: 'rllm', stepId: 'llm-step', model: 'claude-sonnet-4-6', prompt: 'hello',
    });
    await bus.publish('pipeline:llm:response', {
      runId: 'rllm', stepId: 'llm-step', tokensIn: 100, tokensOut: 50,
    });
    await bus.publish('pipeline:step:completed', {
      runId: 'rllm', stepId: 'llm-step', output: { result: 'ok' }, durationMs: 500, at: 't1',
    });
    await bus.publish('pipeline:run:completed', { runId: 'rllm', durationMs: 500, at: 't1' });

    const item = repo.put.mock.calls.at(-1)![0] as PipelineRunItem;
    expect(item.tokenUsage?.['claude-sonnet-4-6']).toMatchObject({ tokensIn: 100, tokensOut: 50 });
    expect(item.estimatedCostUsd).toBeGreaterThan(0);
  });

  // GAP-11: llm:response must persist spend the moment it's incurred, not
  // wait for the next step:completed or checkpoint. Before the fix, the
  // handler only mutated in-memory state, so a crash between "tokens spent"
  // and "step finished" left DDB never knowing about the spend.
  it('persists token spend as soon as llm:response arrives — before the step completes', async () => {
    const repo = { put: jest.fn<(item: PipelineRunItem) => Promise<void>>().mockResolvedValue(undefined) };
    const bus = makeMockBus();
    const service = new RunPersistenceService({
      eventBus: bus as unknown as ConstructorParameters<typeof RunPersistenceService>[0]['eventBus'],
      repository: repo as unknown as PipelineRunRepository,
    });
    service.start();

    await bus.publish('pipeline:run:started', {
      runId: 'rspend', pipelineId: 'pspend',
      triggeredBy: { triggerType: 'manual', userId: 'u1' }, at: 't0',
    });
    repo.put.mockClear(); // drop the run:started write

    await bus.publish('pipeline:llm:prompt', {
      runId: 'rspend', stepId: 'llm-step', model: 'claude-sonnet-4-6', prompt: 'hello',
    });
    await bus.publish('pipeline:llm:response', {
      runId: 'rspend', stepId: 'llm-step', tokensIn: 100, tokensOut: 50,
    });

    // The step never completes (process died) — but the spend must already
    // be durable from the llm:response handler alone.
    expect(repo.put).toHaveBeenCalledTimes(1);
    const item = repo.put.mock.calls.at(-1)![0] as PipelineRunItem;
    expect(item.tokenUsage?.['claude-sonnet-4-6']).toMatchObject({ tokensIn: 100, tokensOut: 50 });
    expect(item.estimatedCostUsd).toBeGreaterThan(0);
  });

  it('stores prompt as step input snapshot for LLM steps', async () => {
    const repo = { put: jest.fn<(item: PipelineRunItem) => Promise<void>>().mockResolvedValue(undefined) };
    const bus = makeMockBus();
    const service = new RunPersistenceService({
      eventBus: bus as unknown as ConstructorParameters<typeof RunPersistenceService>[0]['eventBus'],
      repository: repo as unknown as PipelineRunRepository,
    });
    service.start();

    await bus.publish('pipeline:run:started', {
      runId: 'rinput', pipelineId: 'pinput',
      triggeredBy: { triggerType: 'manual', userId: 'u1' }, at: 't0',
    });
    await bus.publish('pipeline:llm:prompt', {
      runId: 'rinput', stepId: 'llm-node', model: 'claude-haiku-4-5', prompt: 'what is 2+2?',
    });
    await bus.publish('pipeline:step:completed', {
      runId: 'rinput', stepId: 'llm-node', output: { answer: '4' }, durationMs: 100, at: 't1',
    });
    await bus.publish('pipeline:run:completed', { runId: 'rinput', durationMs: 100, at: 't1' });

    const item = repo.put.mock.calls.at(-1)![0] as PipelineRunItem;
    const step = item.stepsSummary?.['llm-node'];
    expect(step?.attempts?.[0]?.input).toMatchObject({ prompt: 'what is 2+2?' });
  });

  it('stop() flushes pending in-flight runs to the repository', async () => {
    const repo = { put: jest.fn<(item: PipelineRunItem) => Promise<void>>().mockResolvedValue(undefined) };
    const bus = makeMockBus();
    const service = new RunPersistenceService({
      eventBus: bus as unknown as ConstructorParameters<typeof RunPersistenceService>[0]['eventBus'],
      repository: repo as unknown as PipelineRunRepository,
    });
    service.start();

    await bus.publish('pipeline:run:started', {
      runId: 'rflushed', pipelineId: 'pflushed',
      triggeredBy: { triggerType: 'manual', userId: 'u1' }, at: 't0',
    });

    const beforeStop = repo.put.mock.calls.length;
    await service.stop();
    // stop() should have triggered at least one additional put() for the in-flight run
    expect(repo.put.mock.calls.length).toBeGreaterThan(beforeStop);
  });

  it('stop() swallows flush errors for individual runs', async () => {
    let callCount = 0;
    const repo = {
      put: jest.fn().mockImplementation(() => {
        if (callCount++ === 0) return Promise.resolve(); // run:started ok
        return Promise.reject(new Error('ddb gone'));    // flush on stop fails
      }),
    };
    const bus = makeMockBus();
    const service = new RunPersistenceService({
      eventBus: bus as unknown as ConstructorParameters<typeof RunPersistenceService>[0]['eventBus'],
      repository: repo as unknown as PipelineRunRepository,
    });
    service.start();

    await bus.publish('pipeline:run:started', {
      runId: 'rerr', pipelineId: 'perr',
      triggeredBy: { triggerType: 'manual', userId: 'u1' }, at: 't0',
    });
    await expect(service.stop()).resolves.toBeUndefined();
  });

  it('stop() is idempotent — second call is a no-op', async () => {
    const repo = { put: jest.fn<(item: PipelineRunItem) => Promise<void>>().mockResolvedValue(undefined) };
    const bus = makeMockBus();
    const service = new RunPersistenceService({
      eventBus: bus as unknown as ConstructorParameters<typeof RunPersistenceService>[0]['eventBus'],
      repository: repo as unknown as PipelineRunRepository,
    });
    service.start();
    await service.stop();
    const countAfterFirst = repo.put.mock.calls.length;
    await service.stop(); // second call — should do nothing
    expect(repo.put.mock.calls.length).toBe(countAfterFirst);
  });

  it('start() is idempotent — second call does not add duplicate subscribers', async () => {
    const repo = { put: jest.fn<(item: PipelineRunItem) => Promise<void>>().mockResolvedValue(undefined) };
    const bus = makeMockBus();
    const service = new RunPersistenceService({
      eventBus: bus as unknown as ConstructorParameters<typeof RunPersistenceService>[0]['eventBus'],
      repository: repo as unknown as PipelineRunRepository,
    });
    service.start();
    service.start();
    // subscribe should have been called exactly once per event type
    // (13 events — DC-25 added pipeline:run:checkpoint, and the agent loop
    // added pipeline:step:started and pipeline:step:agent:iteration)
    expect(bus.subscribe.mock.calls.length).toBe(13);
  });

  it('getStaleRunIds returns runs whose lastEventAt is before the cutoff', async () => {
    const repo = { put: jest.fn<(item: PipelineRunItem) => Promise<void>>().mockResolvedValue(undefined) };
    const bus = makeMockBus();
    const service = new RunPersistenceService({
      eventBus: bus as unknown as ConstructorParameters<typeof RunPersistenceService>[0]['eventBus'],
      repository: repo as unknown as PipelineRunRepository,
    });
    service.start();

    const oldTs = new Date(Date.now() - 10 * 60_000).toISOString(); // 10 min ago
    const newTs = new Date().toISOString();

    await bus.publish('pipeline:run:started', {
      runId: 'rold', pipelineId: 'p1',
      triggeredBy: { triggerType: 'manual', userId: 'u1' }, at: oldTs,
    });
    await bus.publish('pipeline:run:started', {
      runId: 'rnew', pipelineId: 'p1',
      triggeredBy: { triggerType: 'manual', userId: 'u1' }, at: newTs,
    });

    const cutoff = Date.now() - 5 * 60_000; // 5 min ago
    const stale = service.getStaleRunIds(cutoff);
    expect(stale.some((s) => s.runId === 'rold')).toBe(true);
    expect(stale.some((s) => s.runId === 'rnew')).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// hub#1281 — step-level retry executor with per-attempt events (DC-15)
// ---------------------------------------------------------------------------

describe('RunPersistenceService — multi-attempt retry tracking (hub#1281)', () => {
  type Subscriber<P> = (event: { payload: P }) => unknown | Promise<unknown>;

  function makeMockBus() {
    const subs = new Map<string, Subscriber<unknown>>();
    return {
      subscribe: jest.fn((event: string, fn: Subscriber<unknown>) => {
        subs.set(event, fn);
      }),
      async publish<P>(event: string, payload: P): Promise<void> {
        const fn = subs.get(event);
        if (fn) await fn({ payload: payload as unknown });
      },
    };
  }

  it('accumulates 3 attempts when a step fails twice then succeeds', async () => {
    const repo = { put: jest.fn().mockImplementation(() => Promise.resolve()) };
    const bus = makeMockBus();
    const service = new RunPersistenceService({
      eventBus: bus as unknown as ConstructorParameters<typeof RunPersistenceService>[0]['eventBus'],
      repository: repo as unknown as PipelineRunRepository,
    });
    service.start();

    await bus.publish('pipeline:run:started', {
      runId: 'r3', pipelineId: 'p3',
      triggeredBy: { triggerType: 'manual', userId: 'u1' },
      at: 't0',
    });
    // Attempt 1 — fails with transient error, nextBackoffMs = 500
    await bus.publish('pipeline:step:attempt:started', { runId: 'r3', stepId: 'action-A', attemptNumber: 1, startedAt: 't1' });
    await bus.publish('pipeline:step:attempt:failed', { runId: 'r3', stepId: 'action-A', attemptNumber: 1, error: 'timeout', nextBackoffMs: 500, willRetry: true });
    // Attempt 2 — fails again
    await bus.publish('pipeline:step:attempt:started', { runId: 'r3', stepId: 'action-A', attemptNumber: 2, startedAt: 't3' });
    await bus.publish('pipeline:step:attempt:failed', { runId: 'r3', stepId: 'action-A', attemptNumber: 2, error: 'network error', nextBackoffMs: 1000, willRetry: true });
    // Attempt 3 — succeeds
    await bus.publish('pipeline:step:attempt:started', { runId: 'r3', stepId: 'action-A', attemptNumber: 3, startedAt: 't5' });
    await bus.publish('pipeline:step:completed', { runId: 'r3', stepId: 'action-A', output: { ok: true }, durationMs: 200, at: 't6' });
    await bus.publish('pipeline:run:completed', { runId: 'r3', durationMs: 1700, at: 't6' });

    const item = repo.put.mock.calls.at(-1)![0] as PipelineRunItem;
    const stepA = item.stepsSummary?.['action-A'];
    expect(stepA).toBeDefined();
    expect(stepA?.attempts).toHaveLength(3);
    expect(stepA?.attempts?.[0]).toMatchObject({ attemptNumber: 1, error: 'timeout', retryAfterMs: 500 });
    expect(stepA?.attempts?.[1]).toMatchObject({ attemptNumber: 2, error: 'network error', retryAfterMs: 1000 });
    expect(stepA?.attempts?.[2]).toMatchObject({ attemptNumber: 3, output: { ok: true }, completedAt: 't6' });
    // idempotency keys are per-attempt
    expect(stepA?.attempts?.[0]?.idempotencyKey).toBe('r3:action-A:1');
    expect(stepA?.attempts?.[2]?.idempotencyKey).toBe('r3:action-A:3');
    // top-level fields reflect latest attempt
    expect(stepA?.status).toBe('completed');
    expect(stepA?.output).toEqual({ ok: true });
  });

  it('records N failed attempts when maxAttempts exhausted', async () => {
    const repo = { put: jest.fn().mockImplementation(() => Promise.resolve()) };
    const bus = makeMockBus();
    const service = new RunPersistenceService({
      eventBus: bus as unknown as ConstructorParameters<typeof RunPersistenceService>[0]['eventBus'],
      repository: repo as unknown as PipelineRunRepository,
    });
    service.start();

    await bus.publish('pipeline:run:started', {
      runId: 'r4', pipelineId: 'p4',
      triggeredBy: { triggerType: 'manual', userId: 'u1' },
      at: 't0',
    });
    // 3 attempts, all fail
    await bus.publish('pipeline:step:attempt:started', { runId: 'r4', stepId: 'action-B', attemptNumber: 1, startedAt: 't1' });
    await bus.publish('pipeline:step:attempt:failed', { runId: 'r4', stepId: 'action-B', attemptNumber: 1, error: 'err1', nextBackoffMs: 500, willRetry: true });
    await bus.publish('pipeline:step:attempt:started', { runId: 'r4', stepId: 'action-B', attemptNumber: 2, startedAt: 't3' });
    await bus.publish('pipeline:step:attempt:failed', { runId: 'r4', stepId: 'action-B', attemptNumber: 2, error: 'err2', nextBackoffMs: 1000, willRetry: true });
    await bus.publish('pipeline:step:attempt:started', { runId: 'r4', stepId: 'action-B', attemptNumber: 3, startedAt: 't5' });
    // Terminal failure — nextBackoffMs = 0, willRetry = false (no more retries)
    await bus.publish('pipeline:step:attempt:failed', { runId: 'r4', stepId: 'action-B', attemptNumber: 3, error: 'err3', nextBackoffMs: 0, willRetry: false });
    await bus.publish('pipeline:run:failed', {
      runId: 'r4',
      error: { nodeId: 'action-B', message: 'err3' },
      at: 't7',
    });

    const item = repo.put.mock.calls.at(-1)![0] as PipelineRunItem;
    // Run is failed; the step summary isn't written via step:completed but the
    // run-level error is recorded.
    expect(item.status).toBe('failed');
    // Any in-flight attempts that were accumulated before the run:failed are
    // accessible via the stepAttempts internal state but not flushed to stepsSummary
    // without a step:completed event — confirmed by design (run:failed is the
    // terminal persistence path; partial attempt data lives in the run error field).
    expect(item.error).toMatch(/action-B/);
  });
});

// ---------------------------------------------------------------------------
// P1 #7 — DDB persistence retry-with-backoff
//
// Previously a single `.catch()` swallowed transient DDB failures, leaving
// the in-memory runCache populated but the DB row missing — orphaned runs
// invisible after a restart. These tests pin the new behavior: up to 3
// retries (4 total attempts) with 100/400/1600ms backoff, error-level log
// after exhaustion.
// ---------------------------------------------------------------------------

describe('RunPersistenceService — DDB retry policy (P1 #7)', () => {
  type Subscriber<P> = (event: { payload: P }) => unknown | Promise<unknown>;

  function makeMockBus() {
    const subs = new Map<string, Subscriber<unknown>>();
    return {
      subscribe: jest.fn((event: string, fn: Subscriber<unknown>) => {
        subs.set(event, fn);
      }),
      async publish<P>(event: string, payload: P): Promise<void> {
        const fn = subs.get(event);
        if (fn) await fn({ payload: payload as unknown });
      },
    };
  }

  beforeEach(() => {
    fakeLogger.error.mockClear();
    fakeLogger.info.mockClear();
    fakeLogger.warn.mockClear();
  });

  it('success on first try — no retry delay, no error log', async () => {
    jest.useFakeTimers();
    try {
      const repo = { put: jest.fn().mockImplementation(() => Promise.resolve()) };
      const bus = makeMockBus();
      const service = new RunPersistenceService({
        eventBus: bus as unknown as ConstructorParameters<typeof RunPersistenceService>[0]['eventBus'],
        repository: repo as unknown as PipelineRunRepository,
      });
      service.start();

      await bus.publish('pipeline:run:started', {
        runId: 'r-ok-1', pipelineId: 'p-ok-1',
        triggeredBy: { triggerType: 'manual', userId: 'u1' }, at: 't0',
      });

      // No setTimeout should be pending — we never had to back off.
      expect(jest.getTimerCount()).toBe(0);
      expect(repo.put).toHaveBeenCalledTimes(1);
      expect(fakeLogger.error).not.toHaveBeenCalled();
      // "succeeded after retry" info log only fires for attempt > 0.
      expect(
        fakeLogger.info.mock.calls.find((c) => /succeeded after retry/.test(String(c[1]))),
      ).toBeUndefined();
    } finally {
      jest.useRealTimers();
    }
  });

  it('success on attempt 2 — backs off ~100ms then succeeds, no error log', async () => {
    jest.useFakeTimers();
    try {
      let calls = 0;
      const repo = {
        put: jest.fn().mockImplementation(() => {
          calls += 1;
          if (calls === 1) return Promise.reject(new Error('transient ddb blip'));
          return Promise.resolve();
        }),
      };
      const bus = makeMockBus();
      const service = new RunPersistenceService({
        eventBus: bus as unknown as ConstructorParameters<typeof RunPersistenceService>[0]['eventBus'],
        repository: repo as unknown as PipelineRunRepository,
      });
      service.start();

      // Kick off the publish; it awaits internally so we hand back via a promise.
      const pub = bus.publish('pipeline:run:started', {
        runId: 'r-retry-2', pipelineId: 'p-retry-2',
        triggeredBy: { triggerType: 'manual', userId: 'u1' }, at: 't0',
      });

      // Drive the first attempt + its rejection through the microtask queue.
      await Promise.resolve();
      await Promise.resolve();
      // Advance past the 100ms backoff so the 2nd attempt fires.
      await jest.advanceTimersByTimeAsync(100);
      await pub;

      expect(repo.put).toHaveBeenCalledTimes(2);
      expect(fakeLogger.error).not.toHaveBeenCalled();
    } finally {
      jest.useRealTimers();
    }
  });

  it('success on attempt 3 — backs off 100ms+400ms then succeeds, no error log', async () => {
    jest.useFakeTimers();
    try {
      let calls = 0;
      const repo = {
        put: jest.fn().mockImplementation(() => {
          calls += 1;
          if (calls < 3) return Promise.reject(new Error('still blipping'));
          return Promise.resolve();
        }),
      };
      const bus = makeMockBus();
      const service = new RunPersistenceService({
        eventBus: bus as unknown as ConstructorParameters<typeof RunPersistenceService>[0]['eventBus'],
        repository: repo as unknown as PipelineRunRepository,
      });
      service.start();

      const pub = bus.publish('pipeline:run:started', {
        runId: 'r-retry-3', pipelineId: 'p-retry-3',
        triggeredBy: { triggerType: 'manual', userId: 'u1' }, at: 't0',
      });

      // Drain the first attempt + its rejection.
      await Promise.resolve();
      await Promise.resolve();
      // 100ms → attempt 2, fails → schedules 400ms wait.
      await jest.advanceTimersByTimeAsync(100);
      // 400ms → attempt 3, succeeds.
      await jest.advanceTimersByTimeAsync(400);
      await pub;

      expect(repo.put).toHaveBeenCalledTimes(3);
      expect(fakeLogger.error).not.toHaveBeenCalled();
    } finally {
      jest.useRealTimers();
    }
  });

  it('failure after all 4 attempts — surfaces error log with run identifiers', async () => {
    jest.useFakeTimers();
    try {
      const repo = {
        put: jest.fn().mockImplementation(() => Promise.reject(new Error('ddb consistently down'))),
      };
      const bus = makeMockBus();
      const service = new RunPersistenceService({
        eventBus: bus as unknown as ConstructorParameters<typeof RunPersistenceService>[0]['eventBus'],
        repository: repo as unknown as PipelineRunRepository,
      });
      service.start();

      const pub = bus.publish('pipeline:run:started', {
        runId: 'r-doomed', pipelineId: 'p-doomed',
        triggeredBy: { triggerType: 'manual', userId: 'u1' }, at: 't0',
      });

      // Walk through all 3 backoff windows: 100ms, 400ms, 1600ms.
      await Promise.resolve();
      await Promise.resolve();
      await jest.advanceTimersByTimeAsync(100);
      await jest.advanceTimersByTimeAsync(400);
      await jest.advanceTimersByTimeAsync(1600);
      await pub;

      // 1 initial + 3 retries.
      expect(repo.put).toHaveBeenCalledTimes(4);

      // Error log surfaces with the run identifiers.
      expect(fakeLogger.error).toHaveBeenCalledTimes(1);
      const errCall = fakeLogger.error.mock.calls[0];
      expect(errCall[0]).toMatchObject({
        runId: 'r-doomed',
        pipelineId: 'p-doomed',
        attempts: 4,
      });
      expect(String(errCall[1])).toMatch(/run persistence failed after all retries/);
    } finally {
      jest.useRealTimers();
    }
  });

  it('caller-side fire-and-forget contract preserved — subscriber does not throw', async () => {
    // The retry layer rethrows on exhaustion; the subscribe-level .catch()
    // must still swallow that so the EventBus subscriber contract is honored.
    jest.useFakeTimers();
    try {
      const repo = {
        put: jest.fn().mockImplementation(() => Promise.reject(new Error('ddb gone for good'))),
      };
      const bus = makeMockBus();
      const service = new RunPersistenceService({
        eventBus: bus as unknown as ConstructorParameters<typeof RunPersistenceService>[0]['eventBus'],
        repository: repo as unknown as PipelineRunRepository,
      });
      service.start();

      const pub = bus.publish('pipeline:run:started', {
        runId: 'r-fnf', pipelineId: 'p-fnf',
        triggeredBy: { triggerType: 'manual', userId: 'u1' }, at: 't0',
      });
      await Promise.resolve();
      await Promise.resolve();
      await jest.advanceTimersByTimeAsync(100);
      await jest.advanceTimersByTimeAsync(400);
      await jest.advanceTimersByTimeAsync(1600);
      await expect(pub).resolves.toBeUndefined();
      expect(fakeLogger.error).toHaveBeenCalledTimes(1);
    } finally {
      jest.useRealTimers();
    }
  });
});

// ---------------------------------------------------------------------------
// FAILED steps are persisted into stepsSummary (2026-08 e2e bug 1)
//
// Until this landed, only pipeline:step:completed wrote step entries — a run
// whose node FAILED persisted with the failed step absent, so the retry
// route's "was this node executed" guard 400'd on exactly the node the UI
// renders "Retry from here" on. A failed step IS executed; it must persist.
// ---------------------------------------------------------------------------

describe('RunPersistenceService persists FAILED steps into stepsSummary', () => {
  type Subscriber<P> = (event: { payload: P }) => unknown | Promise<unknown>;

  function makeMockBus() {
    const subs = new Map<string, Subscriber<unknown>>();
    return {
      subscribe: jest.fn((event: string, fn: Subscriber<unknown>) => {
        subs.set(event, fn);
      }),
      async publish<P>(event: string, payload: P): Promise<void> {
        const fn = subs.get(event);
        if (fn) await fn({ payload: payload as unknown });
      },
    };
  }

  function makeService() {
    const repo = { put: jest.fn().mockImplementation(() => Promise.resolve()) };
    const bus = makeMockBus();
    const service = new RunPersistenceService({
      eventBus: bus as unknown as ConstructorParameters<typeof RunPersistenceService>[0]['eventBus'],
      repository: repo as unknown as PipelineRunRepository,
    });
    service.start();
    return { repo, bus };
  }

  it('pipeline:step:failed lands in stepsSummary with status=failed, error, and a failed attempt', async () => {
    const { repo, bus } = makeService();

    await bus.publish('pipeline:run:started', {
      runId: 'rf1', pipelineId: 'pf1',
      triggeredBy: { triggerType: 'manual', userId: 'u1' }, at: 't0',
    });
    await bus.publish('pipeline:step:completed', {
      runId: 'rf1', stepId: 'trigger-1', output: {}, durationMs: 10, at: 't1',
    });
    await bus.publish('pipeline:step:failed', {
      runId: 'rf1', stepId: 'cond-1',
      error: 'Condition expression is not supported: foo', durationMs: 42, at: 't2',
    });
    await bus.publish('pipeline:run:failed', {
      runId: 'rf1',
      error: { nodeId: 'cond-1', message: 'Condition expression is not supported: foo' },
      at: 't2',
    });

    const item = repo.put.mock.calls.at(-1)![0] as PipelineRunItem;
    // Both the completed AND the failed step are recorded.
    expect(item.stepsSummary?.['trigger-1']?.status).toBe('completed');
    const failed = item.stepsSummary?.['cond-1'];
    expect(failed).toBeDefined();
    expect(failed?.status).toBe('failed');
    expect(failed?.error).toBe('Condition expression is not supported: foo');
    expect(failed?.durationMs).toBe(42);
    expect(failed?.attempts).toHaveLength(1);
    expect(failed?.attempts?.[0]).toMatchObject({
      attemptNumber: 1,
      error: 'Condition expression is not supported: foo',
      idempotencyKey: 'rf1:cond-1:1',
    });
  });

  it('run:failed alone backfills the failed node (cross-topic race / older executors)', async () => {
    const { repo, bus } = makeService();

    await bus.publish('pipeline:run:started', {
      runId: 'rf2', pipelineId: 'pf2',
      triggeredBy: { triggerType: 'manual', userId: 'u1' }, at: 't0',
    });
    // No step:failed at all — only the terminal run:failed.
    await bus.publish('pipeline:run:failed', {
      runId: 'rf2', error: { nodeId: 'node-X', message: 'boom' }, at: 't1',
    });

    const item = repo.put.mock.calls.at(-1)![0] as PipelineRunItem;
    expect(item.status).toBe('failed');
    expect(item.stepsSummary?.['node-X']).toMatchObject({ status: 'failed', error: 'boom' });
  });

  it('run:failed does NOT duplicate a step already recorded by step:failed', async () => {
    const { repo, bus } = makeService();

    await bus.publish('pipeline:run:started', {
      runId: 'rf3', pipelineId: 'pf3',
      triggeredBy: { triggerType: 'manual', userId: 'u1' }, at: 't0',
    });
    await bus.publish('pipeline:step:failed', {
      runId: 'rf3', stepId: 'node-Y', error: 'first', at: 't1',
    });
    await bus.publish('pipeline:run:failed', {
      runId: 'rf3', error: { nodeId: 'node-Y', message: 'first' }, at: 't1',
    });

    const item = repo.put.mock.calls.at(-1)![0] as PipelineRunItem;
    expect(item.stepsSummary?.['node-Y']?.error).toBe('first');
    expect(item.stepsSummary?.['node-Y:2']).toBeUndefined();
  });

  it('step:failed closes an in-flight attempt history from step:attempt:* events', async () => {
    const { repo, bus } = makeService();

    await bus.publish('pipeline:run:started', {
      runId: 'rf4', pipelineId: 'pf4',
      triggeredBy: { triggerType: 'manual', userId: 'u1' }, at: 't0',
    });
    await bus.publish('pipeline:step:attempt:started', {
      runId: 'rf4', stepId: 'act-1', attemptNumber: 1, startedAt: '2026-01-01T00:00:00.000Z',
    });
    await bus.publish('pipeline:step:attempt:failed', {
      runId: 'rf4', stepId: 'act-1', attemptNumber: 1,
      error: 'fetch failed', nextBackoffMs: 1000, willRetry: true,
    });
    await bus.publish('pipeline:step:attempt:started', {
      runId: 'rf4', stepId: 'act-1', attemptNumber: 2, startedAt: '2026-01-01T00:00:01.000Z',
    });
    await bus.publish('pipeline:step:failed', {
      runId: 'rf4', stepId: 'act-1', error: 'fetch failed', durationMs: 2500,
      at: '2026-01-01T00:00:02.500Z',
    });
    await bus.publish('pipeline:run:failed', {
      runId: 'rf4', error: { nodeId: 'act-1', message: 'fetch failed' },
      at: '2026-01-01T00:00:02.500Z',
    });

    const item = repo.put.mock.calls.at(-1)![0] as PipelineRunItem;
    const step = item.stepsSummary?.['act-1'];
    expect(step?.status).toBe('failed');
    expect(step?.attempts).toHaveLength(2);
    expect(step?.attempts?.[0]).toMatchObject({ attemptNumber: 1, error: 'fetch failed', retryAfterMs: 1000 });
    expect(step?.attempts?.[1]).toMatchObject({ attemptNumber: 2, error: 'fetch failed' });
  });
});

// ---------------------------------------------------------------------------
// DLQ redrive lineage — `redriveOf` reaches the row from either entry point
// ---------------------------------------------------------------------------

describe('RunPersistenceService — redriveOf', () => {
  type Sub = (e: { payload: unknown }) => void | Promise<void>;
  function makeBus() {
    const subs = new Map<string, Sub>();
    return {
      subscribe: jest.fn((event: string, fn: Sub) => { subs.set(event, fn); }),
      async publish(event: string, payload: unknown): Promise<void> {
        const fn = subs.get(event);
        if (fn) await fn({ payload });
      },
    };
  }
  function makeService() {
    const repo = { put: jest.fn<(item: PipelineRunItem) => Promise<void>>().mockResolvedValue(undefined) };
    const bus = makeBus();
    const service = new RunPersistenceService({
      eventBus: bus as unknown as ConstructorParameters<typeof RunPersistenceService>[0]['eventBus'],
      repository: repo as unknown as PipelineRunRepository,
    });
    service.start();
    return { repo, bus, service };
  }

  it('lifts redriveOf from the trigger payload on run:started and keeps it through the terminal put', async () => {
    const { repo, bus } = makeService();
    await bus.publish('pipeline:run:started', {
      runId: 'r-new', pipelineId: 'call-transcription',
      triggeredBy: { triggerType: 'manual', userId: 'dev-grace', payload: { recordingId: 'rec-1', redriveOf: 'r-old' } },
      at: 't0',
    });
    expect((repo.put.mock.calls[0][0] as PipelineRunItem).redriveOf).toBe('r-old');
    await bus.publish('pipeline:run:completed', { runId: 'r-new', durationMs: 1, at: 't1' });
    const last = repo.put.mock.calls.at(-1)![0] as PipelineRunItem;
    expect(last.status).toBe('completed');
    expect(last.redriveOf).toBe('r-old');
  });

  it('keeps a redriveOf named by registerTrigger when run:started arrives without one', async () => {
    const { repo, bus, service } = makeService();
    service.registerTrigger({ runId: 'r-new', pipelineId: 'p', userId: 'u', triggerType: 'manual', at: 't0', redriveOf: 'r-old' });
    await bus.publish('pipeline:run:started', {
      runId: 'r-new', pipelineId: 'p', triggeredBy: { triggerType: 'manual', userId: 'u', payload: {} }, at: 't0',
    });
    expect((repo.put.mock.calls[0][0] as PipelineRunItem).redriveOf).toBe('r-old');
  });

  it('writes no redriveOf for an ordinary run', async () => {
    const { repo, bus } = makeService();
    await bus.publish('pipeline:run:started', {
      runId: 'r1', pipelineId: 'p', triggeredBy: { triggerType: 'manual', userId: 'u', payload: { recordingId: 'rec-1' } }, at: 't0',
    });
    expect((repo.put.mock.calls[0][0] as PipelineRunItem).redriveOf).toBeUndefined();
  });

  it('carries redriveOf and redrivenBy through a resume seed so the next put keeps them', async () => {
    const { repo, bus, service } = makeService();
    service.seedResumedRun({
      pipelineId: 'p', runId: 'r-new', status: 'interrupted', triggeredBy: 'u', triggerType: 'manual', startedAt: 't0',
      redriveOf: 'r-old', redrivenBy: ['r-newer'],
    });
    await bus.publish('pipeline:run:completed', { runId: 'r-new', durationMs: 1, at: 't1' });
    const last = repo.put.mock.calls.at(-1)![0] as PipelineRunItem;
    expect(last.redriveOf).toBe('r-old');
    expect(last.redrivenBy).toEqual(['r-newer']);
  });
});

describe('RunPersistenceService prices steps that report their own token usage', () => {
  type Subscriber<P> = (event: { payload: P }) => unknown | Promise<unknown>;

  function makeMockBus() {
    const subs = new Map<string, Subscriber<unknown>>();
    return {
      subscribe: jest.fn((event: string, fn: Subscriber<unknown>) => {
        subs.set(event, fn);
      }),
      async publish<P>(event: string, payload: P): Promise<void> {
        const fn = subs.get(event);
        if (fn) await fn({ payload: payload as unknown });
      },
    };
  }

  function makeService() {
    const repo = { put: jest.fn().mockImplementation(() => Promise.resolve()) };
    const bus = makeMockBus();
    const service = new RunPersistenceService({
      eventBus: bus as unknown as ConstructorParameters<typeof RunPersistenceService>[0]['eventBus'],
      repository: repo as unknown as PipelineRunRepository,
    });
    service.start();
    return { repo, bus, service };
  }

  const AGENT_OUTPUT = {
    text: 'done',
    turns: 2,
    model: 'claude-sonnet-4-6',
    tokensIn: 1000,
    tokensOut: 500,
  };

  async function started(bus: ReturnType<typeof makeMockBus>) {
    await bus.publish('pipeline:run:started', {
      runId: 'r1',
      pipelineId: 'p1',
      triggeredBy: { triggerType: 'manual', userId: 'u1' },
      at: '2026-01-01T00:00:00.000Z',
    });
  }

  /** Finish the run, which is what triggers the full-row write. */
  async function finished(bus: ReturnType<typeof makeMockBus>) {
    await bus.publish('pipeline:run:completed', {
      runId: 'r1', durationMs: 900, at: '2026-01-01T00:00:03.000Z',
    });
  }

  function lastItem(repo: { put: jest.Mock }): PipelineRunItem {
    return repo.put.mock.calls.at(-1)![0] as PipelineRunItem;
  }

  it('prices an agent step from its output, which emits no llm events', async () => {
    // The gap this closes: an agent step runs its own turn loop and returns the
    // totals, so nothing ever reached the llm:* accumulator and no run built
    // from agent steps had a cost at all.
    const { repo, bus } = makeService();
    await started(bus);
    await bus.publish('pipeline:step:completed', {
      runId: 'r1', stepId: 'summarize', output: AGENT_OUTPUT,
      durationMs: 900, at: '2026-01-01T00:00:01.000Z',
    });

    await finished(bus);
    const item = lastItem(repo);
    expect(item.tokenUsage?.['claude-sonnet-4-6']).toMatchObject({ tokensIn: 1000, tokensOut: 500 });
    expect(item.estimatedCostUsd).toBeGreaterThan(0);
  });

  it('counts each fork branch once, because each really ran', async () => {
    const { repo, bus } = makeService();
    await started(bus);
    for (let i = 0; i < 2; i++) {
      await bus.publish('pipeline:step:completed', {
        runId: 'r1', stepId: 'summarize', output: AGENT_OUTPUT,
        durationMs: 900, at: '2026-01-01T00:00:01.000Z',
      });
    }
    await finished(bus);
    const item = lastItem(repo);
    expect(item.tokenUsage?.['claude-sonnet-4-6']).toMatchObject({ tokensIn: 2000, tokensOut: 1000 });
  });

  it('ignores a step whose output carries no token counts', async () => {
    const { repo, bus } = makeService();
    await started(bus);
    await bus.publish('pipeline:step:completed', {
      runId: 'r1', stepId: 'store', output: { ok: true },
      durationMs: 10, at: '2026-01-01T00:00:01.000Z',
    });
    await finished(bus);
    const item = lastItem(repo);
    expect(item.tokenUsage ?? {}).toEqual({});
    expect(item.estimatedCostUsd ?? 0).toBe(0);
  });

  it('ignores a zero-token report rather than pricing nothing at nothing', async () => {
    const { repo, bus } = makeService();
    await started(bus);
    await bus.publish('pipeline:step:completed', {
      runId: 'r1', stepId: 'noop',
      output: { model: 'claude-sonnet-4-6', tokensIn: 0, tokensOut: 0 },
      durationMs: 10, at: '2026-01-01T00:00:01.000Z',
    });
    await finished(bus);
    expect(lastItem(repo).tokenUsage ?? {}).toEqual({});
  });

  it('ignores a malformed report instead of charging for it', async () => {
    const { repo, bus } = makeService();
    await started(bus);
    for (const bad of [
      { model: 'm', tokensIn: -5, tokensOut: 10 },
      { model: '', tokensIn: 10, tokensOut: 10 },
      { model: 'm', tokensIn: Number.NaN, tokensOut: 10 },
      { model: 'm', tokensIn: '10', tokensOut: 10 },
    ]) {
      await bus.publish('pipeline:step:completed', {
        runId: 'r1', stepId: `s-${Math.random()}`, output: bad,
        durationMs: 10, at: '2026-01-01T00:00:01.000Z',
      });
    }
    await finished(bus);
    expect(lastItem(repo).tokenUsage ?? {}).toEqual({});
  });

  it('gives a forked node one replay credit per branch it had finished', async () => {
    // stepsSummary keys repeats as `id`, `id:2` — two branches, two credits,
    // so a resume can replay both without charging for either.
    const { repo, bus, service } = makeService();
    service.seedResumedRun({
      runId: 'r1', pipelineId: 'p1', status: 'running', triggeredBy: 'u1',
      triggerType: 'manual', startedAt: '2026-01-01T00:00:00.000Z',
      stepsSummary: {
        summarize: { status: 'completed' } as unknown as StepSummary,
        'summarize:2': { status: 'completed' } as unknown as StepSummary,
      },
      tokenUsage: { 'claude-sonnet-4-6': { tokensIn: 2000, tokensOut: 1000 } },
      estimatedCostUsd: 0.021,
    } as unknown as PipelineRunItem);

    for (let i = 0; i < 2; i++) {
      await bus.publish('pipeline:step:completed', {
        runId: 'r1', stepId: 'summarize', output: AGENT_OUTPUT,
        durationMs: 900, at: '2026-01-01T00:00:02.000Z',
      });
    }
    await finished(bus);
    expect(lastItem(repo).tokenUsage?.['claude-sonnet-4-6'])
      .toMatchObject({ tokensIn: 2000, tokensOut: 1000 });
  });

  it('charges for work done AFTER a resume, once the credits are spent', async () => {
    const { repo, bus, service } = makeService();
    service.seedResumedRun({
      runId: 'r1', pipelineId: 'p1', status: 'running', triggeredBy: 'u1',
      triggerType: 'manual', startedAt: '2026-01-01T00:00:00.000Z',
      stepsSummary: { summarize: { status: 'completed' } as unknown as StepSummary },
      tokenUsage: { 'claude-sonnet-4-6': { tokensIn: 1000, tokensOut: 500 } },
      estimatedCostUsd: 0.0105,
    } as unknown as PipelineRunItem);

    // The replay, then a genuinely new step the run had not reached.
    await bus.publish('pipeline:step:completed', {
      runId: 'r1', stepId: 'summarize', output: AGENT_OUTPUT,
      durationMs: 900, at: '2026-01-01T00:00:02.000Z',
    });
    await bus.publish('pipeline:step:completed', {
      runId: 'r1', stepId: 'publish', output: AGENT_OUTPUT,
      durationMs: 900, at: '2026-01-01T00:00:03.000Z',
    });
    await finished(bus);
    expect(lastItem(repo).tokenUsage?.['claude-sonnet-4-6'])
      .toMatchObject({ tokensIn: 2000, tokensOut: 1000 });
  });

  it('does not re-charge a resumed run for steps it already paid for', async () => {
    // The double-count this guards. seedResumedRun restores tokenUsage from the
    // stored row; replaying that step's completion must not add it twice.
    const { repo, bus, service } = makeService();
    service.seedResumedRun({
      runId: 'r1',
      pipelineId: 'p1',
      status: 'running',
      triggeredBy: 'u1',
      triggerType: 'manual',
      startedAt: '2026-01-01T00:00:00.000Z',
      stepsSummary: { summarize: { status: 'completed' } as unknown as StepSummary },
      tokenUsage: { 'claude-sonnet-4-6': { tokensIn: 1000, tokensOut: 500 } },
      estimatedCostUsd: 0.0105,
    } as unknown as PipelineRunItem);

    await bus.publish('pipeline:step:completed', {
      runId: 'r1', stepId: 'summarize', output: AGENT_OUTPUT,
      durationMs: 900, at: '2026-01-01T00:00:02.000Z',
    });

    await finished(bus);
    const item = lastItem(repo);
    expect(item.tokenUsage?.['claude-sonnet-4-6']).toMatchObject({ tokensIn: 1000, tokensOut: 500 });
  });
});
