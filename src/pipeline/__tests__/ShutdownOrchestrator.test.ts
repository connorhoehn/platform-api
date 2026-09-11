// social-api/src/pipeline/__tests__/ShutdownOrchestrator.test.ts
//
// Unit tests for createShutdown (hub#1817).
// Verifies that stuckRunDetector + runPersistence are stopped, errors are
// swallowed (each step tries independently), and all registry/cluster steps run.

import { describe, it, expect, jest, beforeEach } from '@jest/globals';
import { createShutdown } from '../ShutdownOrchestrator';
import type { RunPersistenceService } from '../runPersistence';
import type { StuckRunDetector } from '../StuckRunDetector';
import type { ExecutorAuditSubscriber } from '../executor-audit-subscriber';
import type { JudgeAggregatorSubscriber } from '../judge/judge-aggregator-subscriber';
import type { PipelineEventRedisRelay } from '../event-redis-relay';

function makeLogger() {
  return {
    error: jest.fn(),
  };
}

// ShutdownDeps uses concrete service types. Cast stubs through unknown so
// TypeScript allows minimal objects that only implement the methods under test.
function stub<T>(obj: Partial<T>): T {
  return obj as unknown as T;
}

function makeDeps(overrides: Partial<Parameters<typeof createShutdown>[0]> = {}) {
  const keepAlive = setInterval(() => {}, 1 << 30);
  const unsubscribe = jest.fn();
  const runPersistence = stub<RunPersistenceService>({
    stop: jest.fn(async () => {}),
  });
  const stuckRunDetector = stub<StuckRunDetector>({
    stop: jest.fn(),
  });
  const executorAuditSubscriber = stub<ExecutorAuditSubscriber>({
    stop: jest.fn(async () => {}),
  });
  const judgeAggregatorSubscriber = stub<JudgeAggregatorSubscriber>({
    stop: jest.fn(async () => {}),
  });
  const auditSweeperStop = jest.fn();
  const pipelineEventRelay = stub<PipelineEventRedisRelay>({
    stop: jest.fn(async () => {}),
  });
  const moduleRegistry = { stop: jest.fn(async () => {}) };
  const resourceRegistry = { stop: jest.fn(async () => {}) };
  const hintedHandoff = { stop: jest.fn(async () => {}) };
  const fdBridge = { stop: jest.fn(async () => {}) };
  const cluster = { stop: jest.fn(async () => {}) };
  const logger = makeLogger();

  return {
    keepAlive,
    runQueueSubIds: ['sub-1', 'sub-2'],
    eventBus: { unsubscribe },
    runPersistence,
    stuckRunDetector,
    executorAuditSubscriber,
    judgeAggregatorSubscriber,
    auditSweeperStop,
    pipelineEventRelay,
    moduleRegistry,
    resourceRegistry,
    hintedHandoff,
    fdBridge,
    cluster,
    logger,
    ...overrides,
  };
}

describe('createShutdown()', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('calls stuckRunDetector.stop() when provided', async () => {
    const deps = makeDeps();
    const shutdown = createShutdown(deps);
    await shutdown();
    expect((deps.stuckRunDetector as unknown as { stop: ReturnType<typeof jest.fn> }).stop).toHaveBeenCalledTimes(1);
  });

  it('skips stuckRunDetector when null', async () => {
    const deps = makeDeps({ stuckRunDetector: null });
    const shutdown = createShutdown(deps);
    await expect(shutdown()).resolves.toBeUndefined();
    // No attempt to call stop — would throw if null were dereferenced.
  });

  it('continues after stuckRunDetector.stop() throws', async () => {
    const deps = makeDeps({
      stuckRunDetector: stub<StuckRunDetector>({ stop: jest.fn(() => { throw new Error('detector exploded'); }) }),
    });
    const shutdown = createShutdown(deps);
    await expect(shutdown()).resolves.toBeUndefined();
    expect(deps.logger.error).toHaveBeenCalledWith(
      expect.objectContaining({ err: expect.stringContaining('detector exploded') }),
      'stuckRunDetector.stop() failed',
    );
    // runPersistence must still have been stopped after the error.
    expect((deps.runPersistence as unknown as { stop: ReturnType<typeof jest.fn> }).stop).toHaveBeenCalledTimes(1);
  });

  it('calls runPersistence.stop() when provided', async () => {
    const deps = makeDeps();
    await createShutdown(deps)();
    expect((deps.runPersistence as unknown as { stop: ReturnType<typeof jest.fn> }).stop).toHaveBeenCalledTimes(1);
  });

  it('calls judgeAggregatorSubscriber.stop() when provided', async () => {
    const deps = makeDeps();
    await createShutdown(deps)();
    expect(
      (deps.judgeAggregatorSubscriber as unknown as { stop: ReturnType<typeof jest.fn> }).stop,
    ).toHaveBeenCalledTimes(1);
  });

  it('skips judgeAggregatorSubscriber when null', async () => {
    const deps = makeDeps({ judgeAggregatorSubscriber: null });
    await expect(createShutdown(deps)()).resolves.toBeUndefined();
  });

  it('skips runPersistence when null', async () => {
    const deps = makeDeps({ runPersistence: null });
    await expect(createShutdown(deps)()).resolves.toBeUndefined();
  });

  it('calls pipelineEventRelay.stop() after the registries', async () => {
    const order: string[] = [];
    const deps = makeDeps({
      resourceRegistry: { stop: jest.fn(async () => { order.push('resourceRegistry'); }) },
      pipelineEventRelay: stub<PipelineEventRedisRelay>({
        stop: jest.fn(async () => { order.push('relay'); }),
      }),
    });
    await createShutdown(deps)();
    // Events emitted while the module drains must still reach the canvas.
    expect(order).toEqual(['resourceRegistry', 'relay']);
  });

  it('skips pipelineEventRelay when null', async () => {
    const deps = makeDeps({ pipelineEventRelay: null });
    await expect(createShutdown(deps)()).resolves.toBeUndefined();
  });

  it('continues after pipelineEventRelay.stop() throws', async () => {
    const deps = makeDeps({
      pipelineEventRelay: stub<PipelineEventRedisRelay>({
        stop: jest.fn(async () => { throw new Error('relay exploded'); }),
      }),
    });
    await expect(createShutdown(deps)()).resolves.toBeUndefined();
    expect(deps.logger.error).toHaveBeenCalledWith(
      expect.objectContaining({ err: expect.stringContaining('relay exploded') }),
      'pipelineEventRelay.stop() failed',
    );
    expect(deps.cluster.stop).toHaveBeenCalledTimes(1);
  });

  it('unsubscribes all run-queue subscription ids', async () => {
    const deps = makeDeps();
    await createShutdown(deps)();
    expect(deps.eventBus.unsubscribe).toHaveBeenCalledTimes(2);
    expect(deps.eventBus.unsubscribe).toHaveBeenCalledWith('sub-1');
    expect(deps.eventBus.unsubscribe).toHaveBeenCalledWith('sub-2');
  });

  it('calls all registry + cluster stop methods', async () => {
    const deps = makeDeps();
    await createShutdown(deps)();
    expect(deps.moduleRegistry.stop).toHaveBeenCalledTimes(1);
    expect(deps.resourceRegistry.stop).toHaveBeenCalledTimes(1);
    expect(deps.hintedHandoff!.stop).toHaveBeenCalledTimes(1);
    expect(deps.fdBridge!.stop).toHaveBeenCalledTimes(1);
    expect(deps.cluster.stop).toHaveBeenCalledTimes(1);
  });

  it('continues through all steps even when moduleRegistry.stop() throws', async () => {
    const deps = makeDeps({
      moduleRegistry: { stop: jest.fn(async () => { throw new Error('registry error'); }) },
    });
    await expect(createShutdown(deps)()).resolves.toBeUndefined();
    expect(deps.logger.error).toHaveBeenCalledWith(
      expect.objectContaining({ err: expect.stringContaining('registry error') }),
      'moduleRegistry.stop() failed',
    );
    // cluster should still be stopped.
    expect(deps.cluster.stop).toHaveBeenCalledTimes(1);
  });
});
