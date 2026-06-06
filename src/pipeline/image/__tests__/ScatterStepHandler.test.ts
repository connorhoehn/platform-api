// platform-api/src/pipeline/image/__tests__/ScatterStepHandler.test.ts

import type {
  PipelineNode,
  StepHandlerHelpers,
} from 'distributed-core/applications/pipeline';
import {
  createScatterStepHandler,
  readPath,
} from '../ScatterStepHandler';

function makeNode(data: Record<string, unknown>): PipelineNode {
  return {
    id: 'step_scatter',
    type: 'scatter' as never,
    position: { x: 0, y: 0 },
    data: { type: 'scatter', ...data } as never,
  } as unknown as PipelineNode;
}

function makeHelpers(): StepHandlerHelpers {
  return {
    emit: async () => {},
    runId: 'run_1',
    ownerNodeId: 'node_test',
    stepId: 'step_scatter',
    abortSignal: new AbortController().signal,
    sleep: async () => {},
    isCancelled: () => false,
  } as StepHandlerHelpers;
}

describe('ScatterStepHandler.execute', () => {
  const handler = createScatterStepHandler();

  it('returns fanOut with the items resolved by itemsPath', async () => {
    const items = [{ tag: 'A' }, { tag: 'B' }, { tag: 'C' }];
    const outcome = await handler.execute(
      makeNode({ itemsPath: '$.steps.vision.output.regions' }),
      {
        steps: {
          vision: { output: { regions: items } },
        },
      },
      makeHelpers(),
    );
    expect(outcome.status).toBe('completed');
    expect(outcome.fanOut?.items).toEqual(items);
    expect(outcome.fanOut?.itemAlias).toBe('item');
    expect(outcome.fanOut?.maxConcurrency).toBe(4);
  });

  it('honors custom itemAlias and maxConcurrency', async () => {
    const outcome = await handler.execute(
      makeNode({
        itemsPath: '$.regions',
        itemAlias: 'crop',
        maxConcurrency: 2,
      }),
      { regions: [1, 2] },
      makeHelpers(),
    );
    expect(outcome.fanOut?.itemAlias).toBe('crop');
    expect(outcome.fanOut?.maxConcurrency).toBe(2);
  });

  it('JSON-parses a string array (upstream LLM JSON-mode case)', async () => {
    const outcome = await handler.execute(
      makeNode({ itemsPath: '$.json' }),
      { json: JSON.stringify([{ a: 1 }, { a: 2 }]) },
      makeHelpers(),
    );
    expect(outcome.status).toBe('completed');
    expect(outcome.fanOut?.items).toEqual([{ a: 1 }, { a: 2 }]);
  });

  it('fails when itemsPath is missing', async () => {
    const outcome = await handler.execute(
      makeNode({}),
      {},
      makeHelpers(),
    );
    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/itemsPath.*required/);
  });

  it('fails when path resolves to null', async () => {
    const outcome = await handler.execute(
      makeNode({ itemsPath: '$.missing' }),
      {},
      makeHelpers(),
    );
    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/null|undefined/);
  });

  it('fails when path resolves to non-array shape', async () => {
    const outcome = await handler.execute(
      makeNode({ itemsPath: '$.scalar' }),
      { scalar: 42 },
      makeHelpers(),
    );
    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/expected array/);
  });

  it('zero-item array still succeeds (executor handles empty fan-out)', async () => {
    const outcome = await handler.execute(
      makeNode({ itemsPath: '$.empty' }),
      { empty: [] },
      makeHelpers(),
    );
    expect(outcome.status).toBe('completed');
    expect(outcome.fanOut?.items).toEqual([]);
  });
});

describe('readPath (JSONPath-lite)', () => {
  it('strips leading $.', () => {
    expect(readPath({ a: 1 }, '$.a')).toBe(1);
    expect(readPath({ a: 1 }, 'a')).toBe(1);
  });

  it('walks dotted keys', () => {
    expect(readPath({ a: { b: { c: 42 } } }, 'a.b.c')).toBe(42);
  });

  it('walks bracket notation with quoted keys', () => {
    expect(readPath({ a: { b: 7 } }, "a['b']")).toBe(7);
    expect(readPath({ a: { b: 7 } }, 'a["b"]')).toBe(7);
  });

  it('walks numeric indices', () => {
    expect(readPath({ a: [10, 20, 30] }, 'a[1]')).toBe(20);
    expect(readPath({ a: { 0: 'x' } }, 'a.0')).toBe('x');
  });

  it('returns undefined on missing intermediate keys', () => {
    expect(readPath({ a: {} }, 'a.b.c')).toBeUndefined();
  });

  it('throws on unclosed bracket', () => {
    expect(() => readPath({ a: [] }, 'a[0')).toThrow();
  });
});
