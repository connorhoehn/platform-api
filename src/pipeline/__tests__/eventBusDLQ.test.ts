// social-api/src/pipeline/__tests__/eventBusDLQ.test.ts
//
// Stream 3 (BusDLQ) — verifies that subscriber-throws on the pipeline
// EventBus increment the `pipeline_event_bus_dead_letters_total` counter
// instead of being swallowed.
//
// Trigger path: subscribe a known-throwing handler to a custom event type on
// `module.getEventBus()`, then publish that type. distributed-core's
// EventBus._onMessage catches the throw and invokes
// `config.deadLetterHandler(event, err)` (see EventBus.js lines 446-452).
// PipelineModule.onInitialize() (v0.7.2) wires `eventBusDeadLetterHandler`
// from `PipelineModuleConfig` into that hook, and `bootstrap.ts` constructs
// a handler that calls `incrementBusDeadLetter(error.name)`.
//
// We chose direct EventBus.publish() over starting a real pipeline run
// because:
//   1. It's a single deterministic await — no waiting on executor state.
//   2. It exercises the same DLQ code path used by every pipeline event
//      (the bus doesn't distinguish between pipeline-internal types and
//      custom types — both go through the same _onMessage handler loop).
//   3. It avoids dragging in a full PipelineDefinition fixture just to
//      provoke a throw.

// `distributed-core/testing` is a real subpath export — Jest resolves
// it via package `exports`. tsc with our current `module: commonjs`
// (classic resolver) doesn't honor `exports`. Same suppression used in
// bootstrap.test.ts.
import { FixtureLLMClient } from 'distributed-core/testing';
import { bootstrapPipeline } from '../bootstrap';
import { getRegistry } from '../../observability/metrics';

jest.setTimeout(20_000);

// Resolve the current value of pipeline_event_bus_dead_letters_total for the
// given `reason` label out of the registry snapshot. Returns 0 when no
// sample matches (e.g., before any DLQ has fired for that reason).
function readDlqCounter(reason: string): number {
  const snapshot = getRegistry().getSnapshot();
  const sample = snapshot.metrics.find(
    (m) =>
      m.name === 'pipeline_event_bus_dead_letters_total'
      && m.type === 'counter'
      && m.labels.reason === reason,
  );
  return sample && typeof sample.value === 'number' ? sample.value : 0;
}

describe('EventBus dead-letter handler — Stream 3 (BusDLQ)', () => {
  // Suppress the [pipeline] event bus dead letter console.error chatter for
  // the duration of these tests — the handler itself is under test, not the
  // logging side effect. Restored in afterAll.
  let originalError: typeof console.error;
  beforeAll(() => {
    originalError = console.error;
    console.error = (..._args: unknown[]): void => { /* suppressed in DLQ test */ };
  });
  afterAll(() => {
    console.error = originalError;
  });

  test('subscriber throw increments pipeline_event_bus_dead_letters_total', async () => {
    const fixture = new FixtureLLMClient(['ok']);
    const { module, shutdown } = await bootstrapPipeline({ llmClient: fixture });

    try {
      // Custom error class so we can assert the precise `reason` label
      // (errors's `name` field) without colliding with any other source of
      // dead letters that might be running concurrently in the registry.
      class StreamThreeDlqProbeError extends Error {
        constructor(message: string) {
          super(message);
          this.name = 'StreamThreeDlqProbeError';
        }
      }

      const reason = 'StreamThreeDlqProbeError';
      const before = readDlqCounter(reason);

      // The bus is typed against PipelineEventMap, but EventBus.publish /
      // subscribe are happy to carry arbitrary string event types at
      // runtime — the type parameter is for ergonomics, not enforcement.
      // Cast through `unknown` so we can wire a probe handler without
      // dragging in the full pipeline event surface.
      const bus = module.getEventBus() as unknown as {
        subscribe(type: string, handler: (event: unknown) => Promise<void>): string;
        publish(type: string, payload: unknown): Promise<unknown>;
      };

      bus.subscribe('stream-three-dlq-probe', async () => {
        throw new StreamThreeDlqProbeError('probe-throw');
      });

      // Publish drives EventBus._onMessage → the throw → deadLetterHandler.
      // Returns the published BusEvent envelope; we don't need it.
      await bus.publish('stream-three-dlq-probe', { probe: true });

      // The dead-letter handler runs synchronously inside the
      // subscriber-handler catch block (not via setImmediate / microtask
      // queue), so by the time `publish()` resolves the counter has been
      // bumped. No additional `await` needed.
      const after = readDlqCounter(reason);
      expect(after).toBe(before + 1);

      // Registry snapshot includes the metric (covers the second assertion
      // in the task spec — the metric is discoverable via the Prometheus
      // scrape, not just via the helper).
      const snapshot = getRegistry().getSnapshot();
      const found = snapshot.metrics.find(
        (m) =>
          m.name === 'pipeline_event_bus_dead_letters_total'
          && m.labels.reason === reason,
      );
      expect(found).toBeDefined();
      expect(found?.type).toBe('counter');
    } finally {
      await shutdown();
    }
  });
});
