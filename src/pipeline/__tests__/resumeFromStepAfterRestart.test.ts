// platform-api/src/pipeline/__tests__/resumeFromStepAfterRestart.test.ts
//
// POST …/resume?fromNodeId after a restart: the run's executor is gone, so
// the stored row (definition snapshot + checkpoint, written by
// RunPersistenceService) goes to distributed-core 0.50's resumeFromStep
// fallback, which rewinds it and continues on a fresh executor. One real
// PipelineModule runs and persists the run; a second, fresh one — the
// process after the restart — re-runs it from the row alone.

jest.mock('../../lib/logger', () => {
  const noop = () => undefined;
  const fakeLogger: Record<string, unknown> = {
    info: jest.fn(noop), warn: jest.fn(noop), error: jest.fn(noop), debug: jest.fn(noop), trace: jest.fn(noop), fatal: jest.fn(noop),
  };
  fakeLogger.child = jest.fn(() => fakeLogger);
  return { __esModule: true, default: fakeLogger, withContext: jest.fn(() => fakeLogger) };
});

import { describe, it, expect, jest } from '@jest/globals';
import { randomUUID } from 'crypto';
import { EventEmitter } from 'events';
import { EntityNotFoundError, InvalidRunStateError, type ApplicationModuleContext } from 'distributed-core';
import {
  FixtureLLMClient,
  PipelineModule,
  type PipelineDefinition,
  type PipelineRun,
} from 'distributed-core/applications/pipeline';
import { RunPersistenceService } from '../runPersistence';
import { rerunStateForRow } from '../runRecordStore';
import { createBridge } from '../createBridge';
import type { ResumeFromStepOptions } from '../bridge-types';
import type { PipelineRunItem, RunRecordRepository } from '../../repositories/PipelineRunRepository';

function makeLocalPubSub() {
  const handlers = new Map<string, (topic: string, payload: unknown, meta: Record<string, unknown>) => Promise<void>>();
  let n = 0;
  return {
    subscribe(_topic: string, h: (topic: string, payload: unknown, meta: Record<string, unknown>) => Promise<void>) {
      const id = `sub-${++n}`;
      handlers.set(id, h);
      return id;
    },
    unsubscribe(id: string) { handlers.delete(id); },
    async publish(topic: string, payload: unknown) {
      const meta = { publisherNodeId: 'test-node', messageId: String(Date.now()), timestamp: Date.now(), topic };
      await Promise.all(Array.from(handlers.values()).map((h) => h(topic, payload, meta)));
    },
  };
}

async function makeModule(): Promise<PipelineModule> {
  const nodeId = `node-${randomUUID()}`;
  const events = new EventEmitter();
  const context = {
    clusterManager: {
      localNodeId: nodeId,
      on: events.on.bind(events), off: events.off.bind(events), emit: events.emit.bind(events),
      getAliveMembers: () => [{ id: nodeId, status: 'ALIVE' }],
    },
    resourceRegistry: { registerResourceType: () => undefined, getResourcesByType: () => [], getResourcesByNode: () => [] },
    topologyManager: {},
    moduleRegistry: {
      registerModule: async () => undefined, unregisterModule: async () => undefined,
      getModule: () => undefined, getAllModules: () => [], getModulesByResourceType: () => [],
    },
    configuration: { pubsub: makeLocalPubSub() },
    logger: { info: () => undefined, warn: () => undefined, error: () => undefined, debug: () => undefined },
  } as unknown as ApplicationModuleContext;
  const module = new PipelineModule({
    moduleId: 'pipeline', moduleName: 'Pipeline', version: '1.0.0', resourceTypes: ['pipeline-run'],
    configuration: {}, llmClient: new FixtureLLMClient(['ok']),
  });
  await module.initialize(context);
  await module.start();
  return module;
}

/** trigger → step-1 → step-2 → step-3, simulated notify actions. */
function linearPipeline(): PipelineDefinition {
  const now = new Date().toISOString();
  const action = (id: string) => ({ id, type: 'action', position: { x: 0, y: 0 }, data: { type: 'action', actionType: 'notify', config: { message: id } } });
  const edge = (id: string, source: string, target: string) => ({ id, source, sourceHandle: 'out', target, targetHandle: 'in' });
  return {
    id: `pipe-${randomUUID()}`, name: 'restart', version: 1, status: 'published', publishedVersion: 1,
    nodes: [
      { id: 'trigger', type: 'trigger', position: { x: 0, y: 0 }, data: { type: 'trigger', triggerType: 'manual' } },
      action('step-1'), action('step-2'), action('step-3'),
    ],
    edges: [edge('e0', 'trigger', 'step-1'), edge('e1', 'step-1', 'step-2'), edge('e2', 'step-2', 'step-3')],
    createdAt: now, updatedAt: now, createdBy: 'test',
  } as unknown as PipelineDefinition;
}

/** The run table, in memory. */
function memoryRepository(): RunRecordRepository & { rows: Map<string, PipelineRunItem> } {
  const rows = new Map<string, PipelineRunItem>();
  const put = async (item: PipelineRunItem) => { rows.set(item.runId, JSON.parse(JSON.stringify(item)) as PipelineRunItem); };
  return {
    rows,
    put,
    putOwned: async (item) => { await put(item); return true; },
    get: async (_p, runId) => rows.get(runId) ?? null,
    listByStatus: async (status) => [...rows.values()].filter((r) => r.status === status),
    claimForResume: async () => null,
  };
}

/** Run a pipeline on a module whose events RunPersistenceService persists; return the stored row. */
async function persistedRun(definition: PipelineDefinition): Promise<{ row: PipelineRunItem; run: PipelineRun }> {
  const module = await makeModule();
  const repository = memoryRepository();
  const persistence = new RunPersistenceService({ eventBus: module.getEventBus() as never, repository });
  persistence.start();
  try {
    const resource = await module.createResource({ applicationData: { definition, speedMultiplier: 0.01, triggeredBy: { userId: 'user-1', triggerType: 'manual', payload: {} } } });
    const run = JSON.parse(JSON.stringify(await module.awaitRun(resource.resourceId))) as PipelineRun;
    for (let i = 0; i < 50 && repository.rows.get(run.id)?.status !== 'completed'; i++) await new Promise((r) => setTimeout(r, 10));
    const row = repository.rows.get(run.id);
    expect(row).toBeDefined();
    return { row: row!, run };
  } finally {
    await persistence.stop();
    await module.stop();
  }
}

describe('resume from a step after a restart (no live executor)', () => {
  it('re-runs a stored completed run from a middle step on a fresh module, keeping upstream outputs', async () => {
    const definition = linearPipeline();
    const { row, run } = await persistedRun(definition);
    expect(row.status).toBe('completed');
    expect(row.checkpoint).toBeDefined();
    expect(row.definitionSnapshot).toBeDefined();

    const restarted = await makeModule();
    try {
      const snapshot = row.definitionSnapshot as PipelineDefinition;
      const options: ResumeFromStepOptions = {
        allowCompleted: true,
        definition: snapshot,
        resumeState: rerunStateForRow(row, snapshot),
        runStatus: 'completed',
        allowStatuses: ['failed', 'completed'],
      };
      // Through the in-process bridge, as the route calls it.
      const rerun = (await createBridge(restarted).resumeFromStep!(run.id, 'step-2', options)) as PipelineRun;

      expect(rerun.id).toBe(run.id);
      expect(rerun.status).toBe('completed');
      expect(rerun.replayAttempts).toBe(1);
      // Upstream kept: the executor's own output, rebuilt from the checkpoint's context.
      expect(rerun.steps['step-1']!.status).toBe('completed');
      expect(rerun.steps['step-1']!.output).toEqual(run.steps['step-1']!.output);
      expect(rerun.steps['step-1']!.startedAt).toBe(run.steps['step-1']!.startedAt);
      expect((rerun.context['steps'] as Record<string, unknown>)['step-1']).toEqual(run.steps['step-1']!.output);
      // From step-2 down: run again.
      expect(rerun.steps['step-2']!.startedAt).not.toBe(run.steps['step-2']!.startedAt);
      expect(rerun.steps['step-3']!.status).toBe('completed');
    } finally {
      await restarted.stop();
    }
  });

  it('a completed run is refused unless the route allows completed', async () => {
    const definition = linearPipeline();
    const { row, run } = await persistedRun(definition);
    const restarted = await makeModule();
    try {
      const snapshot = row.definitionSnapshot as PipelineDefinition;
      await expect(restarted.resumeFromStep(run.id, 'step-2', {
        definition: snapshot, resumeState: rerunStateForRow(row, snapshot), runStatus: 'completed', allowStatuses: ['failed'],
      })).rejects.toBeInstanceOf(InvalidRunStateError);
      // Without the stored state a fresh module still does not know the run.
      await expect(restarted.resumeFromStep(run.id, 'step-2', { allowCompleted: true })).rejects.toBeInstanceOf(EntityNotFoundError);
    } finally {
      await restarted.stop();
    }
  });
});
