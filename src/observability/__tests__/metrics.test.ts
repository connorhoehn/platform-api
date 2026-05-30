// social-api/src/observability/__tests__/metrics.test.ts
//
// Unit tests for metrics.ts exported functions.
// Strategy: use the real MetricsRegistry (no I/O, pure in-process) and
// call getRegistry().reset() between tests so counter values don't accumulate.
// Each test verifies that the right metric name / label / value change occurs.

import type { MetricSample } from 'distributed-core';
import {
  getRegistry,
  recordAuditWriteLatency,
  recordCacheAccess,
  recordCacheEviction,
  recordConnection,
  recordConnectionFailure,
  recordDocumentTypeCreated,
  recordDocumentTypeDeleted,
  recordDocumentTypeUpdated,
  getQueueMetrics,
  incrementBusDeadLetter,
  ALLOWED_QUEUE_NAMES,
  recordIngestDlqDepthDelta,
  recordIngestItem,
  recordIngestMappingConfidence,
  recordIngestQueueDepthDelta,
  recordIngestSyncDuration,
  recordLLMTokens,
  recordMessage,
  recordOutboxLeaseAcquired,
  recordOutboxOldestPendingSeconds,
  recordOutboxPublishFailure,
  recordOutboxPublished,
  recordPipelineDefinitionsRefresh,
  recordPipelineApproval,
  recordPipelineApprovalPendingDelta,
  recordPipelineBridgeLatency,
  recordPipelineCancel,
  recordPipelineError,
  recordPipelineOperationDuration,
  recordPipelineRunInflightDelta,
  recordPipelineStepDuration,
  recordPipelineTrigger,
  recordReconnectionAttempt,
  recordReconnectionFailure,
  recordReconnectionSuccess,
  recordTypedDocumentBulkImported,
  recordTypedDocumentCreated,
  recordTypedDocumentValidationError,
  renderPrometheusText,
} from '../metrics';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function findMetric(
  samples: MetricSample[],
  name: string,
  labelPartial: Record<string, string> = {},
): MetricSample | undefined {
  return samples.find(
    (m) =>
      m.name === name &&
      Object.entries(labelPartial).every(([k, v]) => m.labels[k] === v),
  );
}

function counterValue(
  samples: MetricSample[],
  name: string,
  labelPartial: Record<string, string> = {},
): number {
  const m = findMetric(samples, name, labelPartial);
  if (!m) throw new Error(`metric "${name}" not found`);
  return m.value as number;
}

function gaugeValue(
  samples: MetricSample[],
  name: string,
  labelPartial: Record<string, string> = {},
): number {
  return counterValue(samples, name, labelPartial);
}

beforeEach(() => {
  getRegistry().reset();
});

// ---------------------------------------------------------------------------
// Connection / message counters
// ---------------------------------------------------------------------------

describe('connection metrics', () => {
  it('recordConnection +1 increments wsg_active_connections', () => {
    recordConnection(1);
    const { metrics } = getRegistry().getSnapshot();
    expect(gaugeValue(metrics, 'wsg_active_connections')).toBe(1);
  });

  it('recordConnection -1 decrements wsg_active_connections', () => {
    recordConnection(2);
    recordConnection(-1);
    const { metrics } = getRegistry().getSnapshot();
    expect(gaugeValue(metrics, 'wsg_active_connections')).toBe(1);
  });

  it('recordConnection 0 is a no-op', () => {
    recordConnection(0);
    const { metrics } = getRegistry().getSnapshot();
    expect(gaugeValue(metrics, 'wsg_active_connections')).toBe(0);
  });

  it('recordMessage increments wsg_messages_total', () => {
    recordMessage();
    recordMessage();
    const { metrics } = getRegistry().getSnapshot();
    expect(counterValue(metrics, 'wsg_messages_total')).toBe(2);
  });

  it('recordReconnectionAttempt increments wsg_reconnect_attempts_total', () => {
    recordReconnectionAttempt();
    expect(counterValue(getRegistry().getSnapshot().metrics, 'wsg_reconnect_attempts_total')).toBe(1);
  });

  it('recordReconnectionSuccess increments wsg_reconnect_successes_total', () => {
    recordReconnectionSuccess();
    expect(counterValue(getRegistry().getSnapshot().metrics, 'wsg_reconnect_successes_total')).toBe(1);
  });

  it('recordReconnectionFailure increments wsg_reconnect_failures_total', () => {
    recordReconnectionFailure();
    expect(counterValue(getRegistry().getSnapshot().metrics, 'wsg_reconnect_failures_total')).toBe(1);
  });

  it('recordConnectionFailure increments wsg_connection_failures_total', () => {
    recordConnectionFailure();
    expect(counterValue(getRegistry().getSnapshot().metrics, 'wsg_connection_failures_total')).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// Pipeline lifecycle counters
// ---------------------------------------------------------------------------

describe('pipeline lifecycle counters', () => {
  it('recordPipelineTrigger increments pipeline_triggers_total', () => {
    recordPipelineTrigger();
    expect(counterValue(getRegistry().getSnapshot().metrics, 'pipeline_triggers_total')).toBe(1);
  });

  it('recordPipelineApproval increments pipeline_approvals_total', () => {
    recordPipelineApproval();
    expect(counterValue(getRegistry().getSnapshot().metrics, 'pipeline_approvals_total')).toBe(1);
  });

  it('recordPipelineCancel increments pipeline_cancels_total', () => {
    recordPipelineCancel();
    expect(counterValue(getRegistry().getSnapshot().metrics, 'pipeline_cancels_total')).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// Pipeline error counter (labeled)
// ---------------------------------------------------------------------------

describe('recordPipelineError', () => {
  it('uses provided error category as label', () => {
    recordPipelineError('TimeoutError');
    const { metrics } = getRegistry().getSnapshot();
    expect(counterValue(metrics, 'pipeline_errors_total', { error_category: 'TimeoutError' })).toBe(1);
  });

  it('defaults to UnknownError category', () => {
    recordPipelineError();
    const { metrics } = getRegistry().getSnapshot();
    expect(counterValue(metrics, 'pipeline_errors_total', { error_category: 'UnknownError' })).toBe(1);
  });

  it('different categories are tracked independently', () => {
    recordPipelineError('NetworkError');
    recordPipelineError('NetworkError');
    recordPipelineError('ValidationError');
    const { metrics } = getRegistry().getSnapshot();
    expect(counterValue(metrics, 'pipeline_errors_total', { error_category: 'NetworkError' })).toBe(2);
    expect(counterValue(metrics, 'pipeline_errors_total', { error_category: 'ValidationError' })).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// Pipeline step duration histogram
// ---------------------------------------------------------------------------

describe('recordPipelineStepDuration', () => {
  it('records observation under the correct node_type label', () => {
    recordPipelineStepDuration('llm', 250);
    const { metrics } = getRegistry().getSnapshot();
    const m = findMetric(metrics, 'pipeline_step_duration_ms', { node_type: 'llm' });
    expect(m).toBeDefined();
    // histogram value is a HistogramSnapshot
    const snap = m!.value as { count: number; sum: number };
    expect(snap.count).toBe(1);
    expect(snap.sum).toBe(250);
  });

  it('accumulates multiple observations', () => {
    recordPipelineStepDuration('transform', 100);
    recordPipelineStepDuration('transform', 200);
    const { metrics } = getRegistry().getSnapshot();
    const m = findMetric(metrics, 'pipeline_step_duration_ms', { node_type: 'transform' });
    const snap = m!.value as { count: number; sum: number };
    expect(snap.count).toBe(2);
    expect(snap.sum).toBe(300);
  });
});

// ---------------------------------------------------------------------------
// Pipeline inflight + approval pending gauges
// ---------------------------------------------------------------------------

describe('pipeline gauge deltas', () => {
  it('recordPipelineRunInflightDelta +1 then -1 nets to 0', () => {
    recordPipelineRunInflightDelta(1);
    recordPipelineRunInflightDelta(-1);
    expect(gaugeValue(getRegistry().getSnapshot().metrics, 'pipeline_runs_inflight')).toBe(0);
  });

  it('recordPipelineApprovalPendingDelta increments then decrements', () => {
    recordPipelineApprovalPendingDelta(2);
    recordPipelineApprovalPendingDelta(-1);
    expect(gaugeValue(getRegistry().getSnapshot().metrics, 'pipeline_approvals_pending')).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// LLM token counter (model + direction labels)
// ---------------------------------------------------------------------------

describe('recordLLMTokens', () => {
  it('records tokens-in with correct model label', () => {
    recordLLMTokens('claude-sonnet-4-6', 'in', 150);
    const { metrics } = getRegistry().getSnapshot();
    expect(
      counterValue(metrics, 'pipeline_llm_tokens_total', { model: 'claude-sonnet-4-6', direction: 'in' }),
    ).toBe(150);
  });

  it('records tokens-out separately from tokens-in', () => {
    recordLLMTokens('claude-sonnet-4-6', 'in', 50);
    recordLLMTokens('claude-sonnet-4-6', 'out', 200);
    const { metrics } = getRegistry().getSnapshot();
    expect(counterValue(metrics, 'pipeline_llm_tokens_total', { model: 'claude-sonnet-4-6', direction: 'in' })).toBe(50);
    expect(counterValue(metrics, 'pipeline_llm_tokens_total', { model: 'claude-sonnet-4-6', direction: 'out' })).toBe(200);
  });
});

// ---------------------------------------------------------------------------
// EventBus dead-letter counter
// ---------------------------------------------------------------------------

describe('incrementBusDeadLetter', () => {
  it('mints a new counter labeled by reason', () => {
    incrementBusDeadLetter('TypeError');
    const { metrics } = getRegistry().getSnapshot();
    expect(
      counterValue(metrics, 'pipeline_event_bus_dead_letters_total', { reason: 'TypeError' }),
    ).toBe(1);
  });

  it('different error names are tracked independently', () => {
    incrementBusDeadLetter('Error');
    incrementBusDeadLetter('Error');
    incrementBusDeadLetter('CustomError');
    const { metrics } = getRegistry().getSnapshot();
    expect(counterValue(metrics, 'pipeline_event_bus_dead_letters_total', { reason: 'Error' })).toBe(2);
    expect(counterValue(metrics, 'pipeline_event_bus_dead_letters_total', { reason: 'CustomError' })).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// Document type / typed document counters
// ---------------------------------------------------------------------------

describe('document type counters', () => {
  it('recordDocumentTypeCreated increments document_types_created_total', () => {
    recordDocumentTypeCreated();
    expect(counterValue(getRegistry().getSnapshot().metrics, 'document_types_created_total')).toBe(1);
  });

  it('recordDocumentTypeUpdated increments document_types_updated_total', () => {
    recordDocumentTypeUpdated();
    expect(counterValue(getRegistry().getSnapshot().metrics, 'document_types_updated_total')).toBe(1);
  });

  it('recordDocumentTypeDeleted increments document_types_deleted_total', () => {
    recordDocumentTypeDeleted();
    expect(counterValue(getRegistry().getSnapshot().metrics, 'document_types_deleted_total')).toBe(1);
  });

  it('recordTypedDocumentCreated increments typed_documents_created_total', () => {
    recordTypedDocumentCreated();
    expect(counterValue(getRegistry().getSnapshot().metrics, 'typed_documents_created_total')).toBe(1);
  });

  it('recordTypedDocumentBulkImported increments typed_documents_bulk_imported_total', () => {
    recordTypedDocumentBulkImported();
    expect(counterValue(getRegistry().getSnapshot().metrics, 'typed_documents_bulk_imported_total')).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// TypedDocument validation error counter
// ---------------------------------------------------------------------------

describe('recordTypedDocumentValidationError', () => {
  it('labels counter by error_type', () => {
    recordTypedDocumentValidationError('required_field');
    const { metrics } = getRegistry().getSnapshot();
    expect(
      counterValue(metrics, 'typed_documents_validation_errors_total', { error_type: 'required_field' }),
    ).toBe(1);
  });

  it('accumulates across multiple calls with same type', () => {
    recordTypedDocumentValidationError('type_mismatch');
    recordTypedDocumentValidationError('type_mismatch');
    expect(
      counterValue(
        getRegistry().getSnapshot().metrics,
        'typed_documents_validation_errors_total',
        { error_type: 'type_mismatch' },
      ),
    ).toBe(2);
  });
});

// ---------------------------------------------------------------------------
// Bridge latency + operation duration histograms
// ---------------------------------------------------------------------------

describe('bridge / operation latency histograms', () => {
  it('recordPipelineBridgeLatency records under correct operation label', () => {
    recordPipelineBridgeLatency('trigger', 42);
    const { metrics } = getRegistry().getSnapshot();
    const m = findMetric(metrics, 'pipeline_bridge_latency_ms', { operation: 'trigger' });
    expect(m).toBeDefined();
    const snap = m!.value as { count: number; sum: number };
    expect(snap.count).toBe(1);
    expect(snap.sum).toBe(42);
  });

  it('recordPipelineOperationDuration records under correct operation label', () => {
    recordPipelineOperationDuration('listRuns', 75);
    const { metrics } = getRegistry().getSnapshot();
    const m = findMetric(metrics, 'pipeline_operation_duration_ms', { operation: 'listRuns' });
    const snap = m!.value as { count: number };
    expect(snap.count).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// Cache metrics
// ---------------------------------------------------------------------------

describe('recordCacheAccess', () => {
  it('hit increments cache_hits_total with cache_name label', () => {
    recordCacheAccess('definitions', true);
    const { metrics } = getRegistry().getSnapshot();
    expect(counterValue(metrics, 'cache_hits_total', { cache_name: 'definitions' })).toBe(1);
  });

  it('miss increments cache_misses_total', () => {
    recordCacheAccess('idempotency', false);
    const { metrics } = getRegistry().getSnapshot();
    expect(counterValue(metrics, 'cache_misses_total', { cache_name: 'idempotency' })).toBe(1);
  });

  it('hits and misses for the same cache are tracked separately', () => {
    recordCacheAccess('definitions', true);
    recordCacheAccess('definitions', true);
    recordCacheAccess('definitions', false);
    const { metrics } = getRegistry().getSnapshot();
    expect(counterValue(metrics, 'cache_hits_total', { cache_name: 'definitions' })).toBe(2);
    expect(counterValue(metrics, 'cache_misses_total', { cache_name: 'definitions' })).toBe(1);
  });
});

describe('recordCacheEviction', () => {
  it('increments cache_evictions_total with cache_name label', () => {
    recordCacheEviction('definitions');
    const { metrics } = getRegistry().getSnapshot();
    expect(counterValue(metrics, 'cache_evictions_total', { cache_name: 'definitions' })).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// getQueueMetrics
// ---------------------------------------------------------------------------

describe('getQueueMetrics', () => {
  it('returns a QueueMetrics instance for each allowed queue name', () => {
    for (const name of ALLOWED_QUEUE_NAMES) {
      expect(() => getQueueMetrics(name)).not.toThrow();
    }
  });

  it('returns the same instance on repeated calls (singleton per queue)', () => {
    const a = getQueueMetrics('run-queue');
    const b = getQueueMetrics('run-queue');
    expect(a).toBe(b);
  });

  it('throws for an unknown queue name', () => {
    expect(() => getQueueMetrics('unknown-queue' as never)).toThrow('unknown queueName');
  });
});

// ---------------------------------------------------------------------------
// recordAuditWriteLatency
// ---------------------------------------------------------------------------

describe('recordAuditWriteLatency', () => {
  it('records successful write under status=success', () => {
    recordAuditWriteLatency(12, true);
    const { metrics } = getRegistry().getSnapshot();
    const m = metrics.find((s) => s.name === 'audit_write_latency_ms' && s.labels['status'] === 'success');
    expect(m).toBeDefined();
    const snap = m!.value as { count: number; sum: number };
    expect(snap.count).toBe(1);
    expect(snap.sum).toBe(12);
  });

  it('records failed write under status=failure', () => {
    recordAuditWriteLatency(50, false);
    const { metrics } = getRegistry().getSnapshot();
    const m = metrics.find((s) => s.name === 'audit_write_latency_ms' && s.labels['status'] === 'failure');
    expect(m).toBeDefined();
    const snap = m!.value as { count: number; sum: number };
    expect(snap.count).toBe(1);
    expect(snap.sum).toBe(50);
  });
});

// ---------------------------------------------------------------------------
// Ingest subsystem metrics
// ---------------------------------------------------------------------------

describe('recordIngestItem', () => {
  it('increments ingest_items_total with the given status label', () => {
    recordIngestItem('done');
    const { metrics } = getRegistry().getSnapshot();
    expect(counterValue(metrics, 'ingest_items_total', { status: 'done' })).toBe(1);
  });

  it('different statuses produce independent counters', () => {
    recordIngestItem('done');
    recordIngestItem('failed');
    recordIngestItem('failed');
    const { metrics } = getRegistry().getSnapshot();
    expect(counterValue(metrics, 'ingest_items_total', { status: 'done' })).toBe(1);
    expect(counterValue(metrics, 'ingest_items_total', { status: 'failed' })).toBe(2);
  });
});

describe('recordIngestSyncDuration', () => {
  it('records observation in ingest_sync_duration_seconds', () => {
    recordIngestSyncDuration(2.5);
    const { metrics } = getRegistry().getSnapshot();
    const m = metrics.find((s) => s.name === 'ingest_sync_duration_seconds');
    expect(m).toBeDefined();
    const snap = m!.value as { count: number; sum: number };
    expect(snap.count).toBe(1);
    expect(snap.sum).toBeCloseTo(2.5);
  });
});

describe('recordIngestQueueDepthDelta', () => {
  it('+delta increments ingest_queue_depth', () => {
    recordIngestQueueDepthDelta(3);
    expect(gaugeValue(getRegistry().getSnapshot().metrics, 'ingest_queue_depth')).toBe(3);
  });

  it('-delta decrements ingest_queue_depth', () => {
    recordIngestQueueDepthDelta(3);
    recordIngestQueueDepthDelta(-1);
    expect(gaugeValue(getRegistry().getSnapshot().metrics, 'ingest_queue_depth')).toBe(2);
  });

  it('0 delta is a no-op', () => {
    recordIngestQueueDepthDelta(0);
    expect(gaugeValue(getRegistry().getSnapshot().metrics, 'ingest_queue_depth')).toBe(0);
  });
});

describe('recordIngestMappingConfidence', () => {
  it('records confidence observation in ingest_mapping_confidence', () => {
    recordIngestMappingConfidence(0.85);
    const { metrics } = getRegistry().getSnapshot();
    const m = metrics.find((s) => s.name === 'ingest_mapping_confidence');
    expect(m).toBeDefined();
    const snap = m!.value as { count: number; sum: number };
    expect(snap.count).toBe(1);
    expect(snap.sum).toBeCloseTo(0.85);
  });
});

describe('recordIngestDlqDepthDelta', () => {
  it('+delta increments ingest_dlq_depth', () => {
    recordIngestDlqDepthDelta(2);
    expect(gaugeValue(getRegistry().getSnapshot().metrics, 'ingest_dlq_depth')).toBe(2);
  });

  it('-delta decrements ingest_dlq_depth', () => {
    recordIngestDlqDepthDelta(2);
    recordIngestDlqDepthDelta(-2);
    expect(gaugeValue(getRegistry().getSnapshot().metrics, 'ingest_dlq_depth')).toBe(0);
  });

  it('0 delta is a no-op', () => {
    recordIngestDlqDepthDelta(0);
    expect(gaugeValue(getRegistry().getSnapshot().metrics, 'ingest_dlq_depth')).toBe(0);
  });
});

// ---------------------------------------------------------------------------
// renderPrometheusText
// ---------------------------------------------------------------------------

describe('renderPrometheusText', () => {
  it('returns a non-empty string containing known metric names', () => {
    const text = renderPrometheusText();
    expect(typeof text).toBe('string');
    expect(text.length).toBeGreaterThan(0);
    expect(text).toContain('wsg_active_connections');
    expect(text).toContain('pipeline_triggers_total');
    expect(text).toContain('ingest_items_total');
  });
});

// ---------------------------------------------------------------------------
// Outbox drainer metrics (hub#1002)
// ---------------------------------------------------------------------------

describe('outbox drainer metrics', () => {
  it('recordOutboxPublished mints a per-event-type series', () => {
    recordOutboxPublished('social.activity.created');
    recordOutboxPublished('social.activity.created');
    recordOutboxPublished('pipeline.run.completed');
    const { metrics } = getRegistry().getSnapshot();
    expect(
      counterValue(metrics, 'outbox_published_total', { event_type: 'social.activity.created' }),
    ).toBe(2);
    expect(
      counterValue(metrics, 'outbox_published_total', { event_type: 'pipeline.run.completed' }),
    ).toBe(1);
  });

  it('recordOutboxPublishFailure mints a per-event-type series', () => {
    recordOutboxPublishFailure('social.activity.created');
    const { metrics } = getRegistry().getSnapshot();
    expect(
      counterValue(metrics, 'outbox_publish_failures_total', { event_type: 'social.activity.created' }),
    ).toBe(1);
  });

  it('recordOutboxLeaseAcquired increments outbox_lease_acquired_total', () => {
    recordOutboxLeaseAcquired();
    recordOutboxLeaseAcquired();
    expect(
      counterValue(getRegistry().getSnapshot().metrics, 'outbox_lease_acquired_total'),
    ).toBe(2);
  });

  it('recordOutboxOldestPendingSeconds sets outbox_oldest_pending_seconds', () => {
    recordOutboxOldestPendingSeconds(42.5);
    expect(
      gaugeValue(getRegistry().getSnapshot().metrics, 'outbox_oldest_pending_seconds'),
    ).toBe(42.5);
    // Replaces, not adds — gauge.set() semantics.
    recordOutboxOldestPendingSeconds(0);
    expect(
      gaugeValue(getRegistry().getSnapshot().metrics, 'outbox_oldest_pending_seconds'),
    ).toBe(0);
  });

  it('renderPrometheusText includes outbox_* series', () => {
    recordOutboxPublished('social.activity.created');
    recordOutboxLeaseAcquired();
    recordOutboxOldestPendingSeconds(10);
    const text = renderPrometheusText();
    expect(text).toContain('outbox_published_total');
    expect(text).toContain('outbox_lease_acquired_total');
    expect(text).toContain('outbox_oldest_pending_seconds');
  });
});

// ---------------------------------------------------------------------------
// Pipeline definitions cache freshness (hub#1008)
// ---------------------------------------------------------------------------

describe('recordPipelineDefinitionsRefresh', () => {
  it('sets pipeline_definitions_last_refresh_timestamp to (now / 1000)', () => {
    const fixedMs = 1_700_000_000_000;
    recordPipelineDefinitionsRefresh(fixedMs);
    expect(
      gaugeValue(
        getRegistry().getSnapshot().metrics,
        'pipeline_definitions_last_refresh_timestamp',
      ),
    ).toBe(fixedMs / 1000);
  });

  it('subsequent calls move the gauge forward', () => {
    recordPipelineDefinitionsRefresh(1_000_000_000_000);
    recordPipelineDefinitionsRefresh(2_000_000_000_000);
    expect(
      gaugeValue(
        getRegistry().getSnapshot().metrics,
        'pipeline_definitions_last_refresh_timestamp',
      ),
    ).toBe(2_000_000_000);
  });
});
