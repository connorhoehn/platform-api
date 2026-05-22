// social-api/src/pipeline/__tests__/bootstrap.test.ts
//
// Lifecycle smoke test for the Phase-4 bootstrap. Boots a real single-node
// in-memory cluster with FixtureLLMClient (no external dependencies, no
// Anthropic key needed), verifies the module is RUNNING, and confirms
// shutdown() cleanly stops cluster + module.

// `distributed-core/testing` is a real subpath export — Jest resolves
// it via package `exports`. tsc with our current `module: commonjs`
// (classic resolver) doesn't honor `exports`. Suppressing here rather
// than flipping the project to `moduleResolution: nodenext`, which
// would require explicit `.js` extensions across the codebase.
import { FixtureLLMClient } from 'distributed-core/applications/pipeline';
import { bootstrapPipeline } from '../bootstrap';

// Mock publishDurable so the durable-publish subscriptions wired in
// bootstrap.ts don't throw (no catalog server in test). We capture the
// calls via jest.fn() so tests can assert on call counts.
//
// catalog.getRegistry() is called by createValidatingPublisher in bootstrap.ts.
// Return a minimal Registry stub that treats every event as valid (safe in
// warn mode — publish proceeds regardless).
jest.mock('../../lib/event-catalog', () => ({
  publishDurable: jest.fn(async () => {}),
  // wave24-b: stub the catalog/connectCatalog surface used at module level.
  catalog: {
    declareAll: jest.fn(),
    connect: jest.fn(async () => {}),
    checkAllSchemaFreshness: jest.fn(async () => []),
    // Required by createValidatingPublisher in bootstrap.ts (warn mode — always passes).
    getRegistry: jest.fn(() => ({ validate: () => ({ valid: true, errors: [] }) })),
  },
  connectCatalog: jest.fn(async () => {}),
}));

// The cluster's gossip + transport timers are slow enough that 5s default
// timeouts are tight. Bump for this suite.
jest.setTimeout(20_000);

describe('bootstrapPipeline — lifecycle', () => {
  test('boots, exposes a live module + nodeId, then shuts down cleanly', async () => {
    const fixture = new FixtureLLMClient(['hello from fixture']);
    const { module, nodeId, shutdown } = await bootstrapPipeline({ llmClient: fixture });

    expect(typeof nodeId).toBe('string');
    expect(nodeId.length).toBeGreaterThan(0);

    // Module is up and the EventBus is constructed (this is the field that
    // would throw on initialize() if context.configuration.pubsub were
    // missing — getting back a usable bus is the proof bootstrap got it
    // right).
    const bus = module.getEventBus();
    expect(bus).toBeDefined();
    expect(typeof bus.subscribe).toBe('function');

    // Pre-shutdown: surfaces work.
    const activeRuns = module.listActiveRuns();
    expect(Array.isArray(activeRuns)).toBe(true);
    expect(activeRuns).toHaveLength(0); // nothing started yet

    const pending = module.getPendingApprovals();
    expect(Array.isArray(pending)).toBe(true);
    expect(pending).toHaveLength(0);

    const metrics = await module.getMetrics();
    expect(typeof metrics.runsAwaitingApproval).toBe('number');
    expect(metrics.runsAwaitingApproval).toBe(0);

    // Shutdown is idempotent + doesn't throw on a freshly-booted cluster.
    await expect(shutdown()).resolves.toBeUndefined();
  });

  test('two sequential bootstraps each get distinct nodeIds (no shared state)', async () => {
    const fixture = new FixtureLLMClient(['ok']);

    const a = await bootstrapPipeline({ llmClient: fixture });
    const b = await bootstrapPipeline({ llmClient: fixture });

    expect(a.nodeId).not.toBe(b.nodeId);

    await a.shutdown();
    await b.shutdown();
  });

  test('the bootstrapped module exposes the six bridge surfaces by name', async () => {
    const fixture = new FixtureLLMClient(['ok']);
    const { module, shutdown } = await bootstrapPipeline({ llmClient: fixture });

    // Methods we depend on in createBridge.ts — fail fast if any of these
    // surfaces gets renamed or removed in distributed-core.
    expect(typeof module.createResource).toBe('function');
    expect(typeof module.getRun).toBe('function');
    expect(typeof module.getHistory).toBe('function');
    expect(typeof module.listActiveRuns).toBe('function');
    expect(typeof module.cancelRun).toBe('function');
    expect(typeof module.resolveApproval).toBe('function');
    expect(typeof module.getPendingApprovals).toBe('function');
    expect(typeof module.getMetrics).toBe('function');
    expect(typeof module.getEventBus).toBe('function');

    await shutdown();
  });

  test('identityFile produces the SAME nodeId across two sequential bootstraps (DC-1.3)', async () => {
    // Closes gap DC-1.3: stable cluster identity across restarts.
    //
    // Use a per-test path under /tmp so concurrent test runs cannot collide,
    // and clean up after both bootstraps so we don't leave litter in /tmp.
    // Note: the bootstrap defaults to ephemeral id under jest (NODE_ENV=test
    // or JEST_WORKER_ID set) — explicitly passing `identityFile` opts back
    // into persistent identity, which is what we want to verify here.
    const fs = await import('fs');
    const identityFile = `/tmp/test-identity-${Date.now()}-${Math.random().toString(36).slice(2)}`;

    try {
      const fixture = new FixtureLLMClient(['ok']);

      // First boot: file does not exist; loadOrCreateNodeId mints + persists.
      const a = await bootstrapPipeline({ llmClient: fixture, identityFile });
      const firstId = a.nodeId;
      await a.shutdown();

      // Second boot: file exists; loadOrCreateNodeId reads + reuses.
      const b = await bootstrapPipeline({ llmClient: fixture, identityFile });
      const secondId = b.nodeId;
      await b.shutdown();

      expect(firstId).toBe(secondId);
      // Sanity: distributed-core's createId() is non-trivial — guard against a
      // false pass where both ids were the empty string.
      expect(firstId.length).toBeGreaterThan(0);
    } finally {
      try { fs.unlinkSync(identityFile); } catch { /* ok */ }
    }
  });

  // wave24-b — single-source emission regression test.
  //
  // Invariant: each of pipeline:run:{started,completed,failed} fires exactly
  // once per occurrence. bootstrap.ts wires MULTIPLE SUBSCRIPTIONS to the same
  // event (queue-metrics + durable-publish) but that is the subscriber count,
  // not the producer count. The single authoritative producer is DC's
  // PipelineExecutor; PipelineRaftStateMachine is a separate in-process
  // construct that emits only `pipeline:raft:run:*` events and is never
  // instantiated in this bootstrap path.
  //
  // We verify this by injecting a synthetic bus publication and asserting that
  // each external listener attached by bootstrap fires exactly once per event.
  test('pipeline:run:* bus events fire exactly once per occurrence (no dual-emission)', async () => {
    const { publishDurable } = await import('../../lib/event-catalog.js');
    const publishDurableMock = publishDurable as jest.MockedFunction<typeof publishDurable>;
    publishDurableMock.mockClear();

    const fixture = new FixtureLLMClient(['ok']);
    const { module, shutdown } = await bootstrapPipeline({ llmClient: fixture });

    const bus = module.getEventBus();

    // Counters for an external subscriber that we add AFTER bootstrap has
    // attached its own listeners. If the bootstrap were accidentally
    // re-emitting the event, our counter would exceed 1.
    const fired: Record<string, number> = {
      'pipeline:run:started': 0,
      'pipeline:run:completed': 0,
      'pipeline:run:failed': 0,
    };
    for (const event of Object.keys(fired)) {
      bus.subscribe(event as Parameters<typeof bus.subscribe>[0], async () => {
        fired[event]++;
      });
    }

    // Simulate a single DC PipelineExecutor emission for each event type.
    const now = new Date().toISOString();
    await bus.publish('pipeline:run:started', { runId: 'r1', pipelineId: 'p1', triggeredBy: { triggerType: 'manual', payload: {} }, at: now });
    await bus.publish('pipeline:run:completed', { runId: 'r1', durationMs: 42, at: now });
    await bus.publish('pipeline:run:failed', { runId: 'r1', error: { nodeId: 'n1', message: 'boom' }, at: now });

    // Our counter subscriber fired exactly once per event — no double-fire.
    expect(fired['pipeline:run:started']).toBe(1);
    expect(fired['pipeline:run:completed']).toBe(1);
    expect(fired['pipeline:run:failed']).toBe(1);

    // publishDurable was called exactly 3 times (once per event) — the
    // durable-publish subscribers in bootstrap are also single-fire.
    expect(publishDurableMock).toHaveBeenCalledTimes(3);
    const durableNames = publishDurableMock.mock.calls.map((c: [string, ...unknown[]]) => c[0]);
    expect(durableNames).toContain('pipeline.run.started');
    expect(durableNames).toContain('pipeline.run.completed');
    expect(durableNames).toContain('pipeline.run.failed');

    await shutdown();
  });
});
