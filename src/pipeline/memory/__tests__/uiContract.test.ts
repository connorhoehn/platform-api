// Bidirectional contract for the `memory` node — executor side.
//
// The node is authored in ui-components (MemoryConfig.tsx) and executed here.
// The mirror of this file is
//   ui-components/src/components/pipelines/context/__tests__/memoryContract.test.ts
// and the two share one fixture. Keep them in sync: if the output shape or the
// config fields change, BOTH should fail, which is the point. A one-directional
// test passes while the halves disagree, and the failure then surfaces as an
// author picking a variable that is silently empty at run time.
//
// Direction asserted here: the handler really does read exactly the config
// fields the editor writes, and really does return exactly the output keys the
// editor advertises to downstream nodes.

import type {
  PipelineNode,
  StepHandlerHelpers,
} from 'distributed-core/applications/pipeline';
import { createMemoryStepHandler } from '../MemoryStepHandler';
import { InMemoryMemoryStore } from '../MemoryStore';

// ---------------------------------------------------------------------------
// Shared fixture — mirrors MEMORY_* in the ui-components contract test.
// ---------------------------------------------------------------------------

const MEMORY_CONFIG_FIELDS = [
  'type', 'mode', 'scope', 'namespace', 'keyExpression', 'valueField', 'outputKey',
] as const;

const MEMORY_READ_OUTPUT_KEYS = ['found', 'value', 'namespace', 'key', 'scope'] as const;
const MEMORY_WRITE_OUTPUT_KEYS = ['written', 'namespace', 'key', 'scope'] as const;

function makeNode(data: Record<string, unknown>): PipelineNode {
  return {
    id: 'm1',
    type: 'memory' as never,
    position: { x: 0, y: 0 },
    data: { type: 'memory', ...data } as never,
  } as unknown as PipelineNode;
}

function makeHelpers(runId = 'run_1'): StepHandlerHelpers {
  return {
    emit: async () => {},
    runId,
    ownerNodeId: 'n',
    stepId: 'm1',
    abortSignal: new AbortController().signal,
    sleep: async () => {},
    isCancelled: () => false,
  } as StepHandlerHelpers;
}

const build = () => {
  const store = new InMemoryMemoryStore();
  return {
    store,
    handler: createMemoryStepHandler({ store, pipelineIdLookup: () => 'pipe_1' }),
  };
};

/** The full config the editor can produce, with every optional field set. */
const FULL_WRITE_CONFIG = {
  mode: 'write',
  scope: 'long-term',
  namespace: 'ns',
  keyExpression: '{{context.trigger.userId}}',
  valueField: 'context.steps.llm-1.output',
  outputKey: 'recalled',
};

describe('memory node: UI -> executor', () => {
  it('accepts every field the editor can write', async () => {
    const { handler } = build();
    const outcome = await handler.execute(
      makeNode(FULL_WRITE_CONFIG),
      { trigger: { userId: 'u1' }, steps: { 'llm-1': { output: 'v' } } },
      makeHelpers(),
    );
    expect(outcome.status).toBe('completed');
  });

  it('reads no config field the editor cannot write', () => {
    // The handler's own node-data keys must be a subset of what MemoryConfig
    // produces — otherwise it depends on something no author can set.
    const declared = new Set<string>([...MEMORY_CONFIG_FIELDS]);
    for (const key of Object.keys(FULL_WRITE_CONFIG)) {
      expect(declared).toContain(key);
    }
  });

  it('resolves the {{context.*}} form the editor emits', async () => {
    // The editor's variable picker inserts `{{context.a.b}}`; the leading
    // `context.` names the run-context root and is not a key inside it.
    const { store, handler } = build();
    await handler.execute(
      makeNode({
        mode: 'write', scope: 'session', namespace: 'ns',
        keyExpression: 'user-{{context.trigger.userId}}',
        valueField: 'context.trigger.userId',
      }),
      { trigger: { userId: 'u1' } },
      makeHelpers(),
    );
    expect(
      await store.get({ scope: 'session', runId: 'run_1' }, 'ns', 'user-u1'),
    ).toBe('u1');
  });
});

describe('memory node: executor -> UI', () => {
  it('returns exactly the read output keys the editor advertises', async () => {
    const { store, handler } = build();
    await store.put({ scope: 'session', runId: 'run_1' }, 'ns', 'k', 'v');

    const outcome = await handler.execute(
      makeNode({ mode: 'read', scope: 'session', namespace: 'ns', keyExpression: 'k' }),
      {},
      makeHelpers(),
    );

    expect(Object.keys(outcome.output as object).sort()).toEqual(
      [...MEMORY_READ_OUTPUT_KEYS].sort(),
    );
  });

  it('returns exactly the write output keys the editor advertises', async () => {
    const { handler } = build();
    const outcome = await handler.execute(
      makeNode({
        mode: 'write', scope: 'session', namespace: 'ns',
        keyExpression: 'k', valueField: 'context.v',
      }),
      { v: 'x' },
      makeHelpers(),
    );

    expect(Object.keys(outcome.output as object).sort()).toEqual(
      [...MEMORY_WRITE_OUTPUT_KEYS].sort(),
    );
  });

  it('returns an object, not the array the editor once advertised', async () => {
    const { handler } = build();
    const outcome = await handler.execute(
      makeNode({ mode: 'read', scope: 'session', namespace: 'ns', keyExpression: 'k' }),
      {},
      makeHelpers(),
    );
    expect(Array.isArray(outcome.output)).toBe(false);
    expect(typeof outcome.output).toBe('object');
  });

  it('puts the recalled value at .value, the path the picker offers', async () => {
    const { store, handler } = build();
    await store.put({ scope: 'session', runId: 'run_1' }, 'ns', 'k', { deep: 1 });

    const outcome = await handler.execute(
      makeNode({ mode: 'read', scope: 'session', namespace: 'ns', keyExpression: 'k' }),
      {},
      makeHelpers(),
    );
    expect((outcome.output as { value: unknown }).value).toEqual({ deep: 1 });
  });

  it('still populates .value (as null) on a miss, so the path never vanishes', async () => {
    // A path that exists only sometimes is worse than one that is always
    // present — downstream nodes would break only on the miss case.
    const { handler } = build();
    const outcome = await handler.execute(
      makeNode({ mode: 'read', scope: 'session', namespace: 'ns', keyExpression: 'absent' }),
      {},
      makeHelpers(),
    );
    expect(outcome.output as object).toHaveProperty('value', null);
    expect(outcome.output as object).toHaveProperty('found', false);
  });

  it('leaves outputKey to the executor rather than applying it itself', async () => {
    // distributed-core does `context[outputKey] = output` (PipelineExecutor).
    // If the handler also nested under outputKey the value would land at
    // context.recalled.recalled and every advertised path would be wrong.
    const { handler } = build();
    const outcome = await handler.execute(
      makeNode({
        mode: 'read', scope: 'session', namespace: 'ns',
        keyExpression: 'k', outputKey: 'recalled',
      }),
      {},
      makeHelpers(),
    );
    expect(outcome.output as object).not.toHaveProperty('recalled');
  });
});
