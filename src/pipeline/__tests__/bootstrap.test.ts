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
import { FixtureLLMClient } from 'distributed-core/testing';
import { bootstrapPipeline } from '../bootstrap';

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
});
