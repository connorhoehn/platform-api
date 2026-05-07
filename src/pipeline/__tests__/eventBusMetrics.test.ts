// social-api/src/pipeline/__tests__/eventBusMetrics.test.ts
//
// Stream 5 (BusMetrics) — verifies the gateway's MetricsRegistry singleton is
// threaded into the pipeline EventBus so bus-level activity surfaces on
// /internal/metrics. The asserts are deliberately structural rather than
// numeric: we want to fail-fast if distributed-core ever stops emitting
// counters into the supplied registry, but we don't want to be brittle to
// per-version label changes.
//
// distributed-core v0.7.2's EventBus emits the following bus-level metrics
// when an `EventBusConfig.metrics` MetricsRegistry is supplied (verified
// against EventBus.ts source):
//
//   - event.published.count           counter (label: type)
//   - event.received.count            counter (label: type)
//   - event.deadletter.count          counter (label: type)
//   - eventbus.wal.replay.entries     counter
//   - eventbus.wal.replay.dropped     counter (label: reason)
//
// We trigger a publish through `module.getEventBus().publish(...)` with the
// canonical `pipeline:run:started` event type — that path always increments
// `event.published.count{type=pipeline:run:started}` regardless of WAL state
// or subscriber count, so it's the cleanest end-to-end probe.

// `distributed-core/testing` is a real subpath export — Jest resolves it via
// package `exports`. tsc with `module: commonjs` (classic resolver) doesn't
// honor `exports`. Same suppression as bootstrap.test.ts.
import { FixtureLLMClient } from 'distributed-core/testing';
import { bootstrapPipeline } from '../bootstrap';
import { getRegistry } from '../../observability/metrics';

jest.setTimeout(20_000);

describe('EventBus metrics — bus-level counters land in the gateway registry', () => {
  test('publishing on the pipeline EventBus increments event.published.count in /internal/metrics', async () => {
    const fixture = new FixtureLLMClient(['ok']);
    const { module, shutdown } = await bootstrapPipeline({ llmClient: fixture });

    try {
      // Snapshot the registry BEFORE we publish. The bootstrap path itself
      // may or may not produce bus events depending on internal lifecycle
      // wiring, so we compare delta rather than absolute count.
      const registry = getRegistry();
      const before = registry.getSnapshot();
      const beforePublish = countOf(before, 'event.published.count');

      // Publish a canonical pipeline event directly to the bus. This is the
      // cheapest path to exercise the publish counter without spinning up a
      // full pipeline run (which depends on resourceRegistry + topology +
      // executor + LLM mock plumbing).
      const bus = module.getEventBus();
      await bus.publish('pipeline:run:started', {
        runId:        'metrics-test-run',
        pipelineId:   'metrics-test-pipeline',
        triggeredBy:  { triggerType: 'manual', payload: {} },
        at:           new Date().toISOString(),
      });

      const after = registry.getSnapshot();
      const afterPublish = countOf(after, 'event.published.count');

      // The publish counter MUST have advanced. If this fails, distributed-core
      // either (a) accepted but ignored `eventBusMetrics`, or (b) the gateway
      // failed to thread the registry singleton through. Either is a real bug.
      expect(afterPublish).toBeGreaterThan(beforePublish);

      // Structural assertion: at least one counter named like `event.*` or
      // `eventbus.*` should exist in the snapshot. This protects future
      // refactors that rename the publish counter — we'll still notice the
      // bus emitting *something* into the registry.
      const busLevelMetrics = after.metrics.filter((m) =>
        m.name.startsWith('event.') || m.name.startsWith('eventbus.'),
      );
      expect(busLevelMetrics.length).toBeGreaterThan(0);

      // Document the names we observe so test failures (and humans reading
      // the snapshot) get a quick "what's available" reference.
      // eslint-disable-next-line no-console
      console.log(
        '[eventBusMetrics] bus-level metrics observed:',
        busLevelMetrics.map((m) => `${m.name}(${m.type})`).sort(),
      );
    } finally {
      await shutdown();
    }
  });
});

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Sum the value of every counter sample with the given name (labels-agnostic).
 * Returns 0 when the metric isn't registered yet — counters are lazy-created
 * on first inc(), so a missing counter pre-publish is the expected baseline.
 */
function countOf(
  snapshot: ReturnType<ReturnType<typeof getRegistry>['getSnapshot']>,
  metricName: string,
): number {
  let total = 0;
  for (const m of snapshot.metrics) {
    if (m.name !== metricName) continue;
    if (m.type !== 'counter') continue;
    if (typeof m.value === 'number') total += m.value;
  }
  return total;
}
