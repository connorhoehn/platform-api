// social-api/src/pipeline/__tests__/queueMetrics.test.ts
//
// T12 (lib-expansion-3) — verifies the bootstrap subscribes the run-queue
// QueueMetrics to the canonical pipeline:run:* events on the module's
// EventBus. Publishing each lifecycle event must advance the matching
// dc_queue_throughput_*_total{queue="run-queue"} counter exposed via the
// gateway's MetricsRegistry singleton.
//
// We exercise the metric path directly via EventBus.publish rather than
// running a full pipeline definition because the bridge subscribers are
// typed against the bus's PipelineEventMap and the assertion is on the
// counter, not the executor.

import { FixtureLLMClient } from 'distributed-core/testing';
import { bootstrapPipeline } from '../bootstrap';
import { getRegistry } from '../../observability/metrics';

jest.setTimeout(20_000);

function counterValue(metricName: string, queueLabel: string): number {
  const snapshot = getRegistry().getSnapshot();
  let total = 0;
  for (const m of snapshot.metrics) {
    if (m.name !== metricName) continue;
    if (m.type !== 'counter') continue;
    if (m.labels.queue !== queueLabel) continue;
    if (typeof m.value === 'number') total += m.value;
  }
  return total;
}

describe('T12 run-queue metrics wiring', () => {
  test('pipeline:run:* events advance dc_queue_throughput_*_total{queue="run-queue"}', async () => {
    const fixture = new FixtureLLMClient(['ok']);
    const { module, shutdown } = await bootstrapPipeline({ llmClient: fixture });

    try {
      const before = {
        enqueued:  counterValue('dc_queue_throughput_enqueued_total',  'run-queue'),
        completed: counterValue('dc_queue_throughput_completed_total', 'run-queue'),
        failed:    counterValue('dc_queue_throughput_failed_total',    'run-queue'),
      };

      const bus = module.getEventBus();
      await bus.publish('pipeline:run:started', {
        runId: 'qm-test-run-1',
        pipelineId: 'qm-test-pipeline',
        triggeredBy: { triggerType: 'manual', payload: {} },
        at: new Date().toISOString(),
      });
      await bus.publish('pipeline:run:completed', {
        runId: 'qm-test-run-1',
        durationMs: 42,
        at: new Date().toISOString(),
      });
      await bus.publish('pipeline:run:failed', {
        runId: 'qm-test-run-2',
        error: { message: 'boom', name: 'Error' } as never,
        at: new Date().toISOString(),
      });

      const after = {
        enqueued:  counterValue('dc_queue_throughput_enqueued_total',  'run-queue'),
        completed: counterValue('dc_queue_throughput_completed_total', 'run-queue'),
        failed:    counterValue('dc_queue_throughput_failed_total',    'run-queue'),
      };

      expect(after.enqueued  - before.enqueued ).toBeGreaterThanOrEqual(1);
      expect(after.completed - before.completed).toBeGreaterThanOrEqual(1);
      expect(after.failed    - before.failed   ).toBeGreaterThanOrEqual(1);
    } finally {
      await shutdown();
    }
  });
});
