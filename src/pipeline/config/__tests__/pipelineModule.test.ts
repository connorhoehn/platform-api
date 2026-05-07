// social-api/src/pipeline/config/__tests__/pipelineModule.test.ts
//
// Tests for buildPipelineModuleConfig: required field assembly, optional
// field inclusion (walFilePath, eventBus* fields), and the onBeforeAction
// hook passthrough.

import { describe, it, expect, jest } from '@jest/globals';
import { buildPipelineModuleConfig } from '../pipelineModule';
import type { LLMClient, MetricsRegistry } from 'distributed-core';

// ---------------------------------------------------------------------------
// Stubs
// ---------------------------------------------------------------------------

const mockLLMClient = {} as LLMClient;
const mockMetricsRegistry = {} as MetricsRegistry;

const BASE_ARGS = {
  nodeId: 'node-abc',
  walFilePath: undefined,
  llmClient: mockLLMClient,
  metricsRegistry: mockMetricsRegistry,
};

// ---------------------------------------------------------------------------
// Required fields
// ---------------------------------------------------------------------------

describe('buildPipelineModuleConfig — required fields', () => {
  it('sets moduleId to "pipeline-{nodeId}"', () => {
    const config = buildPipelineModuleConfig(BASE_ARGS);
    expect(config.moduleId).toBe('pipeline-node-abc');
  });

  it('sets moduleName to "Pipeline"', () => {
    const config = buildPipelineModuleConfig(BASE_ARGS);
    expect(config.moduleName).toBe('Pipeline');
  });

  it('sets version to "1.0.0"', () => {
    const config = buildPipelineModuleConfig(BASE_ARGS);
    expect(config.version).toBe('1.0.0');
  });

  it('sets resourceTypes to ["pipeline-run"]', () => {
    const config = buildPipelineModuleConfig(BASE_ARGS);
    expect(config.resourceTypes).toEqual(['pipeline-run']);
  });

  it('passes llmClient through', () => {
    const config = buildPipelineModuleConfig(BASE_ARGS);
    expect(config.llmClient).toBe(mockLLMClient);
  });

  it('uses metricsRegistry as run-level metrics', () => {
    const config = buildPipelineModuleConfig(BASE_ARGS);
    expect(config.metrics).toBe(mockMetricsRegistry);
  });

  it('attaches onBeforeAction hook', () => {
    const config = buildPipelineModuleConfig(BASE_ARGS);
    expect(typeof config.onBeforeAction).toBe('function');
  });
});

// ---------------------------------------------------------------------------
// walFilePath — only set when provided
// ---------------------------------------------------------------------------

describe('buildPipelineModuleConfig — walFilePath', () => {
  it('omits walFilePath when undefined', () => {
    const config = buildPipelineModuleConfig({ ...BASE_ARGS, walFilePath: undefined });
    expect('walFilePath' in config).toBe(false);
  });

  it('includes walFilePath when provided', () => {
    const config = buildPipelineModuleConfig({ ...BASE_ARGS, walFilePath: '/tmp/pipeline.wal' });
    expect(config.walFilePath).toBe('/tmp/pipeline.wal');
  });
});

// ---------------------------------------------------------------------------
// eventBus optional fields
// ---------------------------------------------------------------------------

describe('buildPipelineModuleConfig — eventBus optional fields', () => {
  it('omits eventBusDeadLetterHandler when not provided', () => {
    const config = buildPipelineModuleConfig(BASE_ARGS);
    expect('eventBusDeadLetterHandler' in config).toBe(false);
  });

  it('includes eventBusDeadLetterHandler when provided', () => {
    const handler = jest.fn();
    const config = buildPipelineModuleConfig({ ...BASE_ARGS, eventBusDeadLetterHandler: handler });
    expect(config.eventBusDeadLetterHandler).toBe(handler);
  });

  it('omits eventBusAutoCompactIntervalMs when not provided', () => {
    const config = buildPipelineModuleConfig(BASE_ARGS);
    expect('eventBusAutoCompactIntervalMs' in config).toBe(false);
  });

  it('includes eventBusAutoCompactIntervalMs when provided', () => {
    const config = buildPipelineModuleConfig({ ...BASE_ARGS, eventBusAutoCompactIntervalMs: 60_000 });
    expect(config.eventBusAutoCompactIntervalMs).toBe(60_000);
  });

  it('omits eventBusAutoCompactOptions when not provided', () => {
    const config = buildPipelineModuleConfig(BASE_ARGS);
    expect('eventBusAutoCompactOptions' in config).toBe(false);
  });

  it('includes eventBusAutoCompactOptions when provided', () => {
    const opts = { keepLastNPerType: 50 };
    const config = buildPipelineModuleConfig({ ...BASE_ARGS, eventBusAutoCompactOptions: opts });
    expect(config.eventBusAutoCompactOptions).toBe(opts);
  });

  it('omits eventBusMetrics when not provided', () => {
    const config = buildPipelineModuleConfig(BASE_ARGS);
    expect('eventBusMetrics' in config).toBe(false);
  });

  it('includes eventBusMetrics when provided', () => {
    const reg = {} as MetricsRegistry;
    const config = buildPipelineModuleConfig({ ...BASE_ARGS, eventBusMetrics: reg });
    expect(config.eventBusMetrics).toBe(reg);
  });
});

// ---------------------------------------------------------------------------
// Determinism — different nodeIds produce different moduleIds
// ---------------------------------------------------------------------------

describe('buildPipelineModuleConfig — nodeId suffix determinism', () => {
  it('uses different nodeId suffixes for distinct nodes', () => {
    const c1 = buildPipelineModuleConfig({ ...BASE_ARGS, nodeId: 'node-1' });
    const c2 = buildPipelineModuleConfig({ ...BASE_ARGS, nodeId: 'node-2' });
    expect(c1.moduleId).not.toBe(c2.moduleId);
    expect(c1.moduleId).toBe('pipeline-node-1');
    expect(c2.moduleId).toBe('pipeline-node-2');
  });
});
