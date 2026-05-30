// hub#1140 — verifies the bootstrap subscriber wires pipeline:run:completed
// payloads to the pipeline_executor_busy_seconds_total counter so the M4
// SLO trigger gate (.agent/SLO-M4-TRIGGER.md) clause-2 PromQL is decidable.

import { FixtureLLMClient } from 'distributed-core/applications/pipeline';
import { bootstrapPipeline } from '../bootstrap';
import { getRegistry } from '../../observability/metrics';

jest.setTimeout(20_000);

function busyCounterValue(): number {
  const snapshot = getRegistry().getSnapshot();
  let total = 0;
  for (const m of snapshot.metrics) {
    if (m.name !== 'pipeline_executor_busy_seconds_total') continue;
    if (m.type !== 'counter') continue;
    if (typeof m.value === 'number') total += m.value;
  }
  return total;
}

describe('M4 SLO trigger gate — pipeline executor busy counter wiring', () => {
  test('pipeline:run:completed with durationMs advances pipeline_executor_busy_seconds_total by durationMs/1000', async () => {
    const fixture = new FixtureLLMClient(['ok']);
    const { module, shutdown } = await bootstrapPipeline({ llmClient: fixture });

    try {
      const before = busyCounterValue();

      const bus = module.getEventBus();
      await bus.publish('pipeline:run:completed', {
        runId: 'busy-metric-test-run-1',
        durationMs: 250,
        at: new Date().toISOString(),
      });
      await bus.publish('pipeline:run:completed', {
        runId: 'busy-metric-test-run-2',
        durationMs: 1_500,
        at: new Date().toISOString(),
      });

      const after = busyCounterValue();
      expect(after - before).toBeCloseTo(0.25 + 1.5, 5);
    } finally {
      await shutdown();
    }
  });

  test('malformed durationMs (missing, NaN, zero, negative) does NOT advance the counter', async () => {
    const fixture = new FixtureLLMClient(['ok']);
    const { module, shutdown } = await bootstrapPipeline({ llmClient: fixture });

    try {
      const before = busyCounterValue();

      const bus = module.getEventBus();
      await bus.publish('pipeline:run:completed', {
        runId: 'busy-metric-test-malformed-1',
        durationMs: undefined as unknown as number,
        at: new Date().toISOString(),
      });
      await bus.publish('pipeline:run:completed', {
        runId: 'busy-metric-test-malformed-2',
        durationMs: Number.NaN,
        at: new Date().toISOString(),
      });
      await bus.publish('pipeline:run:completed', {
        runId: 'busy-metric-test-malformed-3',
        durationMs: 0,
        at: new Date().toISOString(),
      });
      await bus.publish('pipeline:run:completed', {
        runId: 'busy-metric-test-malformed-4',
        durationMs: -42,
        at: new Date().toISOString(),
      });

      const after = busyCounterValue();
      expect(after - before).toBe(0);
    } finally {
      await shutdown();
    }
  });
});
