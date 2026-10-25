// social-api/src/pipeline/__tests__/eventBusCompact.test.ts
//
// Stream 4 (BusCompact): smoke test that verifies EventBus auto-compaction is
// wired through bootstrap → PipelineModule → EventBus. We:
//
//   1. Bootstrap with a per-test WAL path + a small auto-compaction interval
//      (200ms) and a small keepLastNPerType (2).
//   2. Publish several events of the same type onto the module's EventBus.
//   3. Wait for at least one `compact:completed` event (or the interval to
//      fire ≥ once).
//   4. Assert the post-compaction WAL has been bounded — we use the result
//      payload (`entriesBefore` / `entriesKept`) which is the most reliable
//      observation across timer racing.
//
// We deliberately publish on the bus directly (rather than going through
// `module.createResource`) because the compaction surface is bus-level —
// keeping the test focused on the wiring rather than pipeline run semantics.
//
// The test skips itself when the WAL parent directory isn't writable. In our
// jest env `/tmp` IS writable (bootstrap.test.ts's identity-file test exercises
// the same path), but the guard makes this safe to run in restricted CI.

import * as fs from 'fs';
// `distributed-core/testing` is a real subpath export — Jest resolves it via
// package `exports`. tsc with our current `module: commonjs` (classic
// resolver) doesn't honor `exports`. Suppressing here matches bootstrap.test.ts.
import { FixtureLLMClient } from 'distributed-core/applications/pipeline';
import { bootstrapPipeline } from '../bootstrap';

jest.setTimeout(20_000);

function tmpWalPath(): string {
  return `/tmp/test-pipeline-wal-compact-${Date.now()}-${Math.random().toString(36).slice(2)}.log`;
}

function isTmpWritable(): boolean {
  try {
    fs.accessSync('/tmp', fs.constants.W_OK);
    return true;
  } catch {
    return false;
  }
}

describe('EventBus auto-compaction wiring (Stream 4)', () => {
  if (!isTmpWritable()) {
    test.skip('SKIPPED: /tmp is not writable in this environment', () => {});
    return;
  }

  test('bootstrap threads eventBusAutoCompactIntervalMs + keepLastN through to the EventBus, which compacts on the configured cadence', async () => {
    const walFilePath = tmpWalPath();
    const fixture = new FixtureLLMClient(['ok']);

    const { module, shutdown } = await bootstrapPipeline({
      llmClient: fixture,
      walFilePath,
      // Aggressive cadence so the test wraps up quickly.
      eventBusAutoCompactIntervalMs: 200,
      // Keep at most 2 events per type → publishing 6 same-type events
      // should compact down to 2.
      eventBusAutoCompactOptions: { keepLastNPerType: 2 },
    });

    try {
      const bus = module.getEventBus();

      // Track compaction completions. EventBus extends EventEmitter and emits
      // 'compact:completed' (or 'compact:error' on failure) after each tick.
      const completions: Array<{
        entriesBefore: number;
        entriesKept: number;
        entriesRemoved: number;
        typesVisited: number;
      }> = [];
      const errors: unknown[] = [];
      bus.on('compact:completed', (r: any) => completions.push(r));
      bus.on('compact:error', (e) => errors.push(e));

      // Publish more than keepLastNPerType events of the SAME type so the
      // bus has something meaningful to compact down. We use a synthetic
      // event type so we don't collide with the real PipelineEventMap.
      // The bus is generic-typed; cast to `any` for the synthetic type.
      const NUM_EVENTS = 6;
      for (let i = 0; i < NUM_EVENTS; i++) {
        await (bus as any).publish('test:compaction:tick', { i });
      }

      // An empty completion may arrive while the publishes are still flushing.
      // Wait for the same meaningful completion that the assertion uses.
      const deadline = Date.now() + 15_000;
      while (!completions.some(r => r.entriesBefore >= NUM_EVENTS) && errors.length === 0 && Date.now() < deadline) {
        await new Promise((r) => setTimeout(r, 50));
      }

      expect(errors).toEqual([]);
      expect(completions.length).toBeGreaterThanOrEqual(1);

      // The most recent compaction should have observed ≥ NUM_EVENTS entries
      // before, and kept exactly keepLastNPerType (2) — there's only one
      // event type. Empty WAL file → entriesBefore=0, entriesKept=0, which
      // is also a valid intermediate (e.g., compaction fired before our
      // publishes hit). We assert against the FIRST result that observed
      // entriesBefore ≥ NUM_EVENTS to avoid that race.
      const observed = completions.find((r) => r.entriesBefore >= NUM_EVENTS);
      expect(observed).toBeDefined();
      expect(observed!.entriesKept).toBe(2);
      expect(observed!.entriesRemoved).toBe(observed!.entriesBefore - 2);
      expect(observed!.typesVisited).toBe(1);
    } finally {
      await shutdown();
      try { fs.unlinkSync(walFilePath); } catch { /* ok */ }
    }
  });

  test('bootstrap does NOT wire auto-compaction when walFilePath is unset (PIPELINE_WAL_PATH=disabled)', async () => {
    // When the user sets the interval but the WAL is disabled, bootstrap
    // should NOT thread the interval through to the EventBus — the upstream
    // EventBus only installs its auto-compact timer when walFilePath is
    // truthy (see EventBus.js: `if (… && this.config.walFilePath) { … }`).
    //
    // We verify by waiting past the would-be interval and asserting no
    // compact:completed and no compact:error events fired. (Without our
    // skip path the bus would attempt compact() and emit `compact:error`
    // with WalNotConfiguredError on every tick.)
    const prevWalEnv = process.env.PIPELINE_WAL_PATH;
    process.env.PIPELINE_WAL_PATH = 'disabled';

    const fixture = new FixtureLLMClient(['ok']);
    let shutdownFn: (() => Promise<void>) | null = null;
    try {
      const { module, shutdown } = await bootstrapPipeline({
        llmClient: fixture,
        eventBusAutoCompactIntervalMs: 100,
      });
      shutdownFn = shutdown;

      const bus = module.getEventBus();
      const completions: unknown[] = [];
      const errors: unknown[] = [];
      bus.on('compact:completed', (r) => completions.push(r));
      bus.on('compact:error', (e) => errors.push(e));

      // Wait past several would-be intervals to give the timer plenty of
      // opportunity to fire if it had been installed.
      await new Promise((r) => setTimeout(r, 350));

      expect(errors).toEqual([]);
      expect(completions).toEqual([]);
    } finally {
      if (shutdownFn) await shutdownFn();
      if (prevWalEnv === undefined) {
        delete process.env.PIPELINE_WAL_PATH;
      } else {
        process.env.PIPELINE_WAL_PATH = prevWalEnv;
      }
    }
  });
});
