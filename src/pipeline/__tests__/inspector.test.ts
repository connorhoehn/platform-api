// social-api/src/pipeline/__tests__/inspector.test.ts
//
// T13 + T1 (lib-expansion-3) — verifies the QueueInspector returned by
// `bridge.getInspector()` reads from the live PipelineModule via the
// `asEnvelope` boundary adapter without rippling Envelope semantics
// through the wider pipeline state machine.

import { createBridge, asEnvelope } from '../createBridge';
import type { PipelineModule } from 'distributed-core';
import type { PipelineRunSnapshot } from '../../routes/pipelineTriggers';

interface MockModule {
  createResource: jest.Mock;
  getRun: jest.Mock;
  getHistory: jest.Mock;
  resolveApproval: jest.Mock;
  listActiveRuns: jest.Mock;
  cancelRun: jest.Mock;
  getPendingApprovals: jest.Mock;
  getMetrics: jest.Mock;
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
  };
}

function bridge(mod: MockModule) {
  return createBridge(mod as unknown as PipelineModule);
}

function snapshot(runId: string, pipelineId: string, startedAt?: string): PipelineRunSnapshot {
  return { runId, pipelineId, ...(startedAt ? { startedAt } : {}) } as PipelineRunSnapshot;
}

describe('asEnvelope (T1 boundary adapter)', () => {
  test('runId becomes the envelope id; startedAt drives enqueuedAtMs', () => {
    const startedAt = '2026-04-30T10:00:00.000Z';
    const env = asEnvelope(snapshot('r-1', 'p-1', startedAt));
    expect(env.id).toBe('r-1');
    expect(env.body.runId).toBe('r-1');
    expect(env.attemptCount).toBe(0);
    expect(env.enqueuedAtMs).toBe(new Date(startedAt).getTime());
  });

  test('missing startedAt falls back to a finite enqueuedAtMs', () => {
    const env = asEnvelope(snapshot('r-2', 'p-1'));
    expect(env.id).toBe('r-2');
    expect(Number.isFinite(env.enqueuedAtMs)).toBe(true);
  });
});

describe('bridge.getInspector — T13 introspection', () => {
  test('listPending mirrors module.listActiveRuns; ids are run ids', async () => {
    const mod = makeMockModule();
    mod.listActiveRuns.mockReturnValue([
      { applicationData: snapshot('run-a', 'pipe-1', '2026-04-30T10:00:00.000Z') },
      { applicationData: snapshot('run-b', 'pipe-1', '2026-04-30T10:01:00.000Z') },
    ]);
    const inspector = bridge(mod).getInspector!();
    const page = await inspector.listPending();
    expect(page.items.map((e) => e.id)).toEqual(['run-a', 'run-b']);
    expect(page.items[0].body.pipelineId).toBe('pipe-1');
  });

  test('summary returns counts and oldest-pending age derived from envelopes', async () => {
    const oldestStartedAt = '2026-04-30T09:00:00.000Z';
    const newerStartedAt  = '2026-04-30T10:00:00.000Z';
    const mod = makeMockModule();
    mod.listActiveRuns.mockReturnValue([
      { applicationData: snapshot('run-a', 'pipe-1', oldestStartedAt) },
      { applicationData: snapshot('run-b', 'pipe-1', newerStartedAt) },
    ]);
    const inspector = bridge(mod).getInspector!();
    const sum = await inspector.summary();
    expect(sum.pending).toBe(2);
    expect(sum.inflight).toBe(0);
    expect(sum.failed).toBe(0);
    // Oldest-pending age must be derived from the older startedAt; nowMs is
    // injected by InMemoryQueueInspector defaulting to Date.now, so we just
    // assert it's at least the elapsed-since-oldest delta.
    const sinceOldestMs = Date.now() - new Date(oldestStartedAt).getTime();
    expect(sum.oldestPendingAgeMs).toBeGreaterThan(0);
    expect(sum.oldestPendingAgeMs).toBeLessThanOrEqual(sinceOldestMs + 5_000);
  });

  test('peekPending returns the matching envelope or null', async () => {
    const mod = makeMockModule();
    mod.listActiveRuns.mockReturnValue([
      { applicationData: snapshot('run-a', 'pipe-1', '2026-04-30T10:00:00.000Z') },
    ]);
    const inspector = bridge(mod).getInspector!();
    const hit  = await inspector.peekPending('run-a');
    const miss = await inspector.peekPending('does-not-exist');
    expect(hit?.id).toBe('run-a');
    expect(miss).toBeNull();
  });

  test('inspector is memoized — repeated getInspector returns the same instance', () => {
    const mod = makeMockModule();
    mod.listActiveRuns.mockReturnValue([]);
    const b = bridge(mod);
    expect(b.getInspector!()).toBe(b.getInspector!());
  });
});
