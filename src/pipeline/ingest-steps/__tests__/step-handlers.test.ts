// social-api/src/pipeline/ingest-steps/__tests__/step-handlers.test.ts
//
// Unit tests for the four ingest pipeline step types. Each test isolates a
// single handler against narrow stubs so the contract is exercised without
// pulling in the executor or repositories.

import {
  createFetchStep,
  createMapStep,
  createMaterializeStep,
  createWritebackStep,
  createDefaultPipelineResolver,
  registerIngestSteps,
  StepHandlerRegistry,
  isIngestStepType,
  INGEST_STEP_TYPES,
} from '..';
import type {
  IMappingEngine,
  IMaterializer,
  ISourceFetcher,
  IWriteBackDispatcher,
  StepContext,
  MappingDecision,
} from '../types';
import type { FileContent, FileDescriptor } from '../../../ingest/types';

// ─── helpers ──────────────────────────────────────────────────────────────

function makeFile(overrides: Partial<FileDescriptor> = {}): FileDescriptor {
  return {
    path: '/docs/report.md',
    name: 'report.md',
    hash: 'abc',
    size: 100,
    mimeType: 'text/markdown',
    lastModified: '2026-05-01T00:00:00Z',
    ...overrides,
  };
}

function makeContent(text = 'hello'): FileContent {
  return { data: Buffer.from(text, 'utf-8'), encoding: 'utf-8' };
}

function makeMapping(overrides: Partial<MappingDecision> = {}): MappingDecision {
  return {
    targetTypeId: 'doc-type-1',
    fieldMapping: { name: 'title' },
    method: 'rule',
    confidence: 1.0,
    ruleId: 'r1',
    ...overrides,
  };
}

function makeCtx(overrides: Partial<StepContext> = {}): StepContext {
  return {
    runId: 'run-1',
    nodeId: 'node-1',
    context: {},
    triggerPayload: {},
    ...overrides,
  };
}

// ─── ingest:fetch ─────────────────────────────────────────────────────────

describe('ingest:fetch', () => {
  test('happy path — calls fetcher and returns file + content', async () => {
    const fetcher: ISourceFetcher = {
      fetch: jest.fn(async (_sid, _path) => ({
        file: makeFile(),
        content: makeContent('body'),
      })),
    };
    const step = createFetchStep({ sourceFetcher: fetcher });

    const outcome = await step({ sourceId: 'src-1', path: '/docs/report.md' }, makeCtx());

    expect(outcome.status).toBe('completed');
    if (outcome.status !== 'completed') return;
    expect(outcome.output['sourceId']).toBe('src-1');
    expect(outcome.output['file']).toMatchObject({ path: '/docs/report.md' });
    expect(fetcher.fetch).toHaveBeenCalledWith('src-1', '/docs/report.md');
  });

  test('falls back to triggerPayload when config is empty', async () => {
    const fetcher: ISourceFetcher = {
      fetch: jest.fn(async () => ({ file: makeFile(), content: makeContent() })),
    };
    const step = createFetchStep({ sourceFetcher: fetcher });
    const ctx = makeCtx({
      triggerPayload: { sourceId: 'src-trigger', event: { filePath: '/x.md' } },
    });

    const outcome = await step({}, ctx);
    expect(outcome.status).toBe('completed');
    expect(fetcher.fetch).toHaveBeenCalledWith('src-trigger', '/x.md');
  });

  test('fails when sourceId cannot be resolved', async () => {
    const step = createFetchStep({ sourceFetcher: { fetch: jest.fn() } });
    const outcome = await step({ path: '/x' }, makeCtx());
    expect(outcome.status).toBe('failed');
    if (outcome.status === 'failed') {
      expect(outcome.error).toMatch(/sourceId is required/);
      expect(outcome.sourceHandle).toBe('error');
    }
  });

  test('wraps fetcher exceptions into failed outcomes', async () => {
    const step = createFetchStep({
      sourceFetcher: { fetch: jest.fn(async () => { throw new Error('boom'); }) },
    });
    const outcome = await step({ sourceId: 's', path: 'p' }, makeCtx());
    expect(outcome.status).toBe('failed');
    if (outcome.status === 'failed') {
      expect(outcome.error).toMatch(/ingest:fetch:.*boom/);
    }
  });
});

// ─── ingest:map ───────────────────────────────────────────────────────────

describe('ingest:map', () => {
  test('happy path — calls mapping engine with file from prior fetch step', async () => {
    const engine: IMappingEngine = {
      map: jest.fn(async () => makeMapping({ confidence: 0.8 })),
    };
    const step = createMapStep({ mappingEngine: engine });
    const ctx = makeCtx({
      context: {
        steps: {
          'fetch-1': { file: makeFile(), content: makeContent() },
        },
      },
    });

    const outcome = await step({ sourceId: 'src-1' }, ctx);
    expect(outcome.status).toBe('completed');
    if (outcome.status !== 'completed') return;
    expect(outcome.output['mappedTypeId']).toBe('doc-type-1');
    expect(outcome.output['mappingConfidence']).toBe(0.8);
    expect(engine.map).toHaveBeenCalledWith(
      expect.objectContaining({ sourceId: 'src-1', file: expect.any(Object) }),
    );
  });

  test('fails when no file descriptor is on context', async () => {
    const engine: IMappingEngine = { map: jest.fn() };
    const step = createMapStep({ mappingEngine: engine });
    const outcome = await step({ sourceId: 's' }, makeCtx());
    expect(outcome.status).toBe('failed');
    expect(engine.map).not.toHaveBeenCalled();
  });

  test('wraps engine exceptions', async () => {
    const step = createMapStep({
      mappingEngine: { map: jest.fn(async () => { throw new Error('llm-fail'); }) },
    });
    const ctx = makeCtx({ context: { file: makeFile() } });
    const outcome = await step({ sourceId: 's' }, ctx);
    expect(outcome.status).toBe('failed');
    if (outcome.status === 'failed') {
      expect(outcome.error).toMatch(/llm-fail/);
    }
  });
});

// ─── ingest:materialize ───────────────────────────────────────────────────

describe('ingest:materialize', () => {
  test('happy path — materializes and resolves default pipeline (doctype layer)', async () => {
    const materializer: IMaterializer = {
      materialize: jest.fn(async () => ({ documentId: 'doc-1', created: true, warnings: [] })),
    };
    const resolveDefaultPipeline = createDefaultPipelineResolver({
      docTypeLookup: { getDefaultPipelineId: jest.fn(async () => 'pipe-from-type') },
      sourceLookup: { getById: jest.fn(async () => null) },
    });
    const step = createMaterializeStep({ materializer, resolveDefaultPipeline });

    const ctx = makeCtx({
      context: {
        file: makeFile(),
        content: makeContent('hello'),
        mapping: makeMapping(),
      },
    });

    const outcome = await step({ sourceId: 'src-1' }, ctx);
    expect(outcome.status).toBe('completed');
    if (outcome.status !== 'completed') return;
    expect(outcome.output['documentId']).toBe('doc-1');
    expect(outcome.output['defaultPipeline']).toEqual({
      pipelineId: 'pipe-from-type',
      via: 'doctype',
    });
  });

  test('fails when prerequisite context is missing', async () => {
    const step = createMaterializeStep({
      materializer: { materialize: jest.fn() },
      resolveDefaultPipeline: createDefaultPipelineResolver({
        docTypeLookup: { getDefaultPipelineId: jest.fn() },
        sourceLookup: { getById: jest.fn() },
      }),
    });
    const outcome = await step({ sourceId: 's' }, makeCtx());
    expect(outcome.status).toBe('failed');
    if (outcome.status === 'failed') {
      expect(outcome.error).toMatch(/no file descriptor/);
    }
  });

  test('still completes when default-pipeline lookup throws', async () => {
    const materializer: IMaterializer = {
      materialize: jest.fn(async () => ({ documentId: 'doc-1', created: true, warnings: [] })),
    };
    const step = createMaterializeStep({
      materializer,
      resolveDefaultPipeline: () => Promise.reject(new Error('ddb down')),
    });
    const ctx = makeCtx({
      context: { file: makeFile(), content: makeContent(), mapping: makeMapping() },
    });
    const outcome = await step({ sourceId: 's' }, ctx);
    expect(outcome.status).toBe('completed');
    if (outcome.status !== 'completed') return;
    expect((outcome.output['warnings'] as string[]).some((w) => /ddb down/.test(w))).toBe(true);
    expect(outcome.output['defaultPipeline']).toEqual({ pipelineId: null, via: 'none' });
  });
});

// ─── ingest:writeback ─────────────────────────────────────────────────────

describe('ingest:writeback', () => {
  test('happy path — enqueues a WriteBackChange', async () => {
    const dispatcher: IWriteBackDispatcher = {
      enqueue: jest.fn(async () => ({ ackId: 'ack-1', enqueuedAt: '2026-05-06T00:00:00Z' })),
    };
    const step = createWritebackStep({ writebackDispatcher: dispatcher });
    const outcome = await step(
      { documentId: 'doc-1', sourceRef: 'drive:abc', change: { op: 'replace' } },
      makeCtx(),
    );
    expect(outcome.status).toBe('completed');
    expect(dispatcher.enqueue).toHaveBeenCalledWith(
      expect.objectContaining({ documentId: 'doc-1', sourceRef: 'drive:abc' }),
    );
  });

  test('falls back to context.documentId from a prior step', async () => {
    const dispatcher: IWriteBackDispatcher = {
      enqueue: jest.fn(async () => ({ ackId: 'a', enqueuedAt: 't' })),
    };
    const step = createWritebackStep({ writebackDispatcher: dispatcher });
    const ctx = makeCtx({
      context: {
        steps: {
          'mat-1': { documentId: 'doc-from-step' },
        },
      },
    });
    const outcome = await step({ change: { x: 1 } }, ctx);
    expect(outcome.status).toBe('completed');
    expect(dispatcher.enqueue).toHaveBeenCalledWith(
      expect.objectContaining({ documentId: 'doc-from-step' }),
    );
  });

  test('fails when change is missing', async () => {
    const step = createWritebackStep({
      writebackDispatcher: { enqueue: jest.fn() },
    });
    const outcome = await step({ documentId: 'd' }, makeCtx());
    expect(outcome.status).toBe('failed');
    if (outcome.status === 'failed') {
      expect(outcome.error).toMatch(/change payload is required/);
    }
  });
});

// ─── registry + identification ────────────────────────────────────────────

describe('StepHandlerRegistry / isIngestStepType', () => {
  test('registerIngestSteps wires all four handlers', () => {
    const registry = registerIngestSteps({
      sourceFetcher: { fetch: jest.fn() },
      mappingEngine: { map: jest.fn() },
      materializer: { materialize: jest.fn() },
      writebackDispatcher: { enqueue: jest.fn() },
      resolveDefaultPipeline: () => Promise.resolve({ pipelineId: null, via: 'none' as const }),
    });
    for (const t of INGEST_STEP_TYPES) {
      expect(registry.has(t)).toBe(true);
    }
    expect(registry.list().sort()).toEqual([...INGEST_STEP_TYPES].sort());
  });

  test('dispatch returns failed for unknown step types', async () => {
    const registry = new StepHandlerRegistry();
    const outcome = await registry.dispatch('ingest:unknown', {}, makeCtx());
    expect(outcome.status).toBe('failed');
  });

  test('isIngestStepType', () => {
    expect(isIngestStepType('ingest:fetch')).toBe(true);
    expect(isIngestStepType('ingest:writeback')).toBe(true);
    expect(isIngestStepType('action')).toBe(false);
    expect(isIngestStepType('ingest:nope')).toBe(false);
  });
});
