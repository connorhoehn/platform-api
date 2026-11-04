// platform-api/src/pipeline/config/__tests__/pipelineModule-idempotency.test.ts
//
// Action idempotency (hub#1286) runs through distributed-core's
// `actionDedupStore` (0.50). platform-api hands every executor one
// process-wide store, so a run continued on a fresh executor (boot resume, a
// re-run from a step after its executor was evicted) does not fire a
// `per-step` action a second time. The old `onBeforeAction` hook was never
// read upstream; these tests drive real executors instead of the hook.

import { describe, it, expect } from '@jest/globals';
import { randomUUID } from 'crypto';
import { EventBus } from 'distributed-core';
import {
  PipelineExecutor,
  type PipelineDefinition,
  type PipelineEventMap,
  type PipelineResumeState,
} from 'distributed-core/applications/pipeline';
import { buildPipelineModuleConfig, getActionDedupStore } from '../pipelineModule';
import type { LLMClient } from 'distributed-core/applications/pipeline';
import type { MetricsRegistry } from 'distributed-core';

function makeBus(): EventBus<PipelineEventMap> {
  const handlers = new Map<string, (topic: string, payload: unknown, meta: Record<string, unknown>) => Promise<void>>();
  let n = 0;
  const pubsub = {
    subscribe: (_t: string, h: never) => { n += 1; handlers.set(`s${n}`, h); return Promise.resolve(`s${n}`); },
    unsubscribe: (id: string) => { handlers.delete(id); return Promise.resolve(); },
    publish: async (topic: string, payload: unknown, meta: Record<string, unknown> = {}) => { for (const h of handlers.values()) await h(topic, payload, meta); },
    subscribeAll: (h: never) => { n += 1; handlers.set(`a${n}`, h); return Promise.resolve(`a${n}`); },
  };
  return new EventBus<PipelineEventMap>(pubsub as never, 'test-node', { topic: 'pipeline.idempotency-test' });
}

const definition = (mode: 'per-step' | 'per-attempt'): PipelineDefinition => ({
  id: 'idem-pipeline',
  name: 'idempotency',
  version: 1,
  status: 'published',
  nodes: [
    { id: 'trigger', type: 'trigger', position: { x: 0, y: 0 }, data: { type: 'trigger', triggerType: 'manual' } },
    {
      id: 'notify',
      type: 'action',
      position: { x: 0, y: 1 },
      data: { type: 'action', actionType: 'notify', config: {}, idempotencyMode: mode },
    },
  ],
  edges: [{ id: 'e1', source: 'trigger', target: 'notify' }],
  createdAt: '2026-11-04T00:00:00.000Z',
  updatedAt: '2026-11-04T00:00:00.000Z',
  createdBy: 'test',
} as unknown as PipelineDefinition);

const llmClient = { stream: () => ({ async *[Symbol.asyncIterator]() { /* none */ } }) } as never;

/** A finished run's frontier rewound to the action step, as a resume would hand it over. */
function resumeAtAction(runId: string): PipelineResumeState {
  return {
    runId,
    context: {},
    currentStepIds: ['notify'],
    steps: { trigger: { nodeId: 'trigger', status: 'completed' } },
    joinStates: {},
    firedJoins: [],
    completedStepCount: 1,
    sequenceNumber: 1,
    startedAt: new Date().toISOString(),
    triggeredBy: { triggerType: 'manual', payload: {} },
  } as unknown as PipelineResumeState;
}

async function runOnce(def: PipelineDefinition, opts: { runId?: string; resumeFrom?: PipelineResumeState }) {
  const bus = makeBus();
  await bus.start();
  const executor = new PipelineExecutor({
    definition: def,
    ownerNodeId: 'test-node',
    llmClient,
    eventBus: bus,
    speedMultiplier: 0.001,
    actionDedupStore: getActionDedupStore(),
    ...(opts.runId ? { runId: opts.runId } : {}),
    ...(opts.resumeFrom ? { resumeFrom: opts.resumeFrom } : {}),
  });
  const run = opts.resumeFrom ? await executor.resume() : await executor.run();
  return run.steps['notify']?.output as Record<string, unknown> | undefined;
}

describe('buildPipelineModuleConfig — action idempotency', () => {
  const args = { nodeId: 'n', walFilePath: undefined, llmClient: {} as LLMClient, metricsRegistry: {} as MetricsRegistry };

  it('hands every module the same process-wide actionDedupStore', () => {
    const a = buildPipelineModuleConfig(args);
    const b = buildPipelineModuleConfig({ ...args, nodeId: 'm' });
    expect(a.actionDedupStore).toBe(getActionDedupStore());
    expect(b.actionDedupStore).toBe(a.actionDedupStore);
  });

  it('no longer sets the dead onBeforeAction hook', () => {
    expect('onBeforeAction' in buildPipelineModuleConfig(args)).toBe(false);
  });
});

describe('actionDedupStore across executors', () => {
  it('a per-step action resumed on a fresh executor is not dispatched again', async () => {
    const runId = randomUUID();
    const first = await runOnce(definition('per-step'), { runId });
    expect(first?.['deduped']).toBeUndefined();

    const second = await runOnce(definition('per-step'), { resumeFrom: resumeAtAction(runId) });
    expect(second).toMatchObject({ deduped: true, originalRunId: runId, dedupKey: `${runId}:notify` });
  });

  it('keys are run-scoped: another run dispatches its own action', async () => {
    await runOnce(definition('per-step'), { runId: randomUUID() });
    const other = await runOnce(definition('per-step'), { runId: randomUUID() });
    expect(other?.['deduped']).toBeUndefined();
  });
});
