import type {
  PipelineNode,
  StepHandlerHelpers,
} from 'distributed-core/applications/pipeline';
import { createMemoryStepHandler, resolveKeyExpression, readContextPath } from '../MemoryStepHandler';
import { InMemoryMemoryStore, memoryPartitionKey, memorySortKey } from '../MemoryStore';

function makeNode(data: Record<string, unknown>): PipelineNode {
  return {
    id: 'step_memory',
    type: 'memory' as never,
    position: { x: 0, y: 0 },
    data: { type: 'memory', ...data } as never,
  } as unknown as PipelineNode;
}

function makeHelpers(runId = 'run_1'): StepHandlerHelpers {
  return {
    emit: async () => {},
    runId,
    ownerNodeId: 'node_test',
    stepId: 'step_memory',
    abortSignal: new AbortController().signal,
    sleep: async () => {},
    isCancelled: () => false,
  } as StepHandlerHelpers;
}

// `null` means "lookup fails". Using `undefined` here would silently fall back
// to the default parameter and test the wrong thing.
const withStore = (pipelineId: string | null = 'pipe_1') => {
  const store = new InMemoryMemoryStore();
  const handler = createMemoryStepHandler({
    store,
    pipelineIdLookup: () => pipelineId ?? undefined,
  });
  return { store, handler };
};

describe('readContextPath', () => {
  it('strips the leading context. segment authors write in the editor', () => {
    expect(readContextPath({ trigger: { userId: 'u1' } }, 'context.trigger.userId')).toBe('u1');
  });

  it('returns undefined for a missing segment rather than throwing', () => {
    expect(readContextPath({ trigger: {} }, 'context.trigger.userId')).toBeUndefined();
  });

  it('does not walk into non-objects', () => {
    expect(readContextPath({ a: 'str' }, 'context.a.b')).toBeUndefined();
  });

  it('preserves the value type instead of stringifying', () => {
    const obj = { nested: true };
    expect(readContextPath({ a: obj }, 'context.a')).toBe(obj);
  });
});

describe('resolveKeyExpression', () => {
  it('substitutes resolved tokens', () => {
    const r = resolveKeyExpression('user-{{context.trigger.userId}}', {
      trigger: { userId: 'u1' },
    });
    expect(r.key).toBe('user-u1');
    expect(r.unresolved).toEqual([]);
  });

  it('reports unresolved paths instead of collapsing them to empty string', () => {
    // The executor's own interpolate() renders a missing path as '' — correct
    // for an LLM prompt, catastrophic for a key: every run with a missing
    // path would land on the SAME key and read back each other's values.
    const r = resolveKeyExpression('user-{{context.trigger.userId}}', { trigger: {} });
    expect(r.key).toBeUndefined();
    expect(r.unresolved).toEqual(['context.trigger.userId']);
  });

  it('treats a null value as unresolved', () => {
    const r = resolveKeyExpression('{{context.a}}', { a: null });
    expect(r.key).toBeUndefined();
  });

  it('passes through a literal key with no tokens', () => {
    expect(resolveKeyExpression('fixed-key', {}).key).toBe('fixed-key');
  });
});

describe('MemoryStepHandler', () => {
  const handlerKind = () => withStore().handler.kind;

  it('registers under the "memory" kind', () => {
    expect(handlerKind()).toBe('memory');
  });

  it('writes a value read from a context path', async () => {
    const { store, handler } = withStore();
    const node = makeNode({
      mode: 'write',
      scope: 'session',
      namespace: 'support',
      keyExpression: '{{context.trigger.userId}}',
      valueField: 'context.steps.llm-1.output',
    });
    const context = {
      trigger: { userId: 'u1' },
      steps: { 'llm-1': { output: 'the summary' } },
    };

    const outcome = await handler.execute(node, context, makeHelpers());

    expect(outcome.status).toBe('completed');
    expect(await store.get({ scope: 'session', runId: 'run_1' }, 'support', 'u1')).toBe(
      'the summary',
    );
  });

  it('stores an object without stringifying it', async () => {
    // A template-interpolated write would persist "[object Object]".
    const { store, handler } = withStore();
    const value = { score: 0.9, tags: ['a'] };
    const outcome = await handler.execute(
      makeNode({
        mode: 'write',
        scope: 'session',
        namespace: 'ns',
        keyExpression: 'k',
        valueField: 'context.result',
      }),
      { result: value },
      makeHelpers(),
    );

    expect(outcome.status).toBe('completed');
    expect(await store.get({ scope: 'session', runId: 'run_1' }, 'ns', 'k')).toEqual(value);
  });

  it('reads a value back, reporting found', async () => {
    const { store, handler } = withStore();
    await store.put({ scope: 'session', runId: 'run_1' }, 'ns', 'k', 'remembered');

    const outcome = await handler.execute(
      makeNode({ mode: 'read', scope: 'session', namespace: 'ns', keyExpression: 'k' }),
      {},
      makeHelpers(),
    );

    expect(outcome.status).toBe('completed');
    expect(outcome.output).toMatchObject({ found: true, value: 'remembered' });
  });

  it('treats a miss as a completed read, not a failure', async () => {
    // A first-run lookup of a long-term key legitimately finds nothing.
    const { handler } = withStore();
    const outcome = await handler.execute(
      makeNode({ mode: 'read', scope: 'session', namespace: 'ns', keyExpression: 'absent' }),
      {},
      makeHelpers(),
    );

    expect(outcome.status).toBe('completed');
    expect(outcome.output).toMatchObject({ found: false, value: null });
  });

  it('keeps session memory private to its own run', async () => {
    // Two concurrent runs of the same pipeline must not see each other.
    const { store, handler } = withStore();
    await handler.execute(
      makeNode({
        mode: 'write', scope: 'session', namespace: 'ns',
        keyExpression: 'k', valueField: 'context.v',
      }),
      { v: 'from run 1' },
      makeHelpers('run_1'),
    );

    const outcome = await handler.execute(
      makeNode({ mode: 'read', scope: 'session', namespace: 'ns', keyExpression: 'k' }),
      {},
      makeHelpers('run_2'),
    );

    expect(outcome.output).toMatchObject({ found: false });
    expect(store.size).toBe(1);
  });

  it('shares long-term memory across runs of the same pipeline', async () => {
    const { handler } = withStore('pipe_1');
    await handler.execute(
      makeNode({
        mode: 'write', scope: 'long-term', namespace: 'ns',
        keyExpression: 'k', valueField: 'context.v',
      }),
      { v: 'persisted' },
      makeHelpers('run_1'),
    );

    const outcome = await handler.execute(
      makeNode({ mode: 'read', scope: 'long-term', namespace: 'ns', keyExpression: 'k' }),
      {},
      makeHelpers('run_2'),
    );

    expect(outcome.output).toMatchObject({ found: true, value: 'persisted' });
  });

  it('keeps namespaces from colliding on the same key name', async () => {
    const { handler, store } = withStore();
    const ref = { scope: 'session' as const, runId: 'run_1' };
    await store.put(ref, 'nsA', 'k', 'a');
    await store.put(ref, 'nsB', 'k', 'b');

    const outcome = await handler.execute(
      makeNode({ mode: 'read', scope: 'session', namespace: 'nsB', keyExpression: 'k' }),
      {},
      makeHelpers(),
    );
    expect(outcome.output).toMatchObject({ value: 'b' });
  });

  it('fails rather than writing to a key with an unresolved path', async () => {
    const { store, handler } = withStore();
    const outcome = await handler.execute(
      makeNode({
        mode: 'write', scope: 'session', namespace: 'ns',
        keyExpression: 'user-{{context.trigger.userId}}', valueField: 'context.v',
      }),
      { v: 'x' },
      makeHelpers(),
    );

    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/unresolved/);
    expect(store.size).toBe(0);
  });

  it('fails a long-term step when the pipelineId cannot be resolved', async () => {
    // Falling back to session scope would write something no later run could
    // ever read, which looks like silent data loss to the author.
    const { store, handler } = withStore(null);
    const outcome = await handler.execute(
      makeNode({
        mode: 'write', scope: 'long-term', namespace: 'ns',
        keyExpression: 'k', valueField: 'context.v',
      }),
      { v: 'x' },
      makeHelpers(),
    );

    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/pipelineId/);
    expect(store.size).toBe(0);
  });

  it('requires a namespace', async () => {
    const { handler } = withStore();
    const outcome = await handler.execute(
      makeNode({ mode: 'read', scope: 'session', namespace: '  ', keyExpression: 'k' }),
      {},
      makeHelpers(),
    );
    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/namespace/);
  });

  it('requires a key expression', async () => {
    const { handler } = withStore();
    const outcome = await handler.execute(
      makeNode({ mode: 'read', scope: 'session', namespace: 'ns', keyExpression: '' }),
      {},
      makeHelpers(),
    );
    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/keyExpression/);
  });

  it('requires a valueField when writing', async () => {
    const { handler } = withStore();
    const outcome = await handler.execute(
      makeNode({ mode: 'write', scope: 'session', namespace: 'ns', keyExpression: 'k' }),
      {},
      makeHelpers(),
    );
    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/valueField/);
  });

  it('fails when the valueField does not resolve', async () => {
    const { handler } = withStore();
    const outcome = await handler.execute(
      makeNode({
        mode: 'write', scope: 'session', namespace: 'ns',
        keyExpression: 'k', valueField: 'context.missing',
      }),
      {},
      makeHelpers(),
    );
    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/valueField/);
  });

  it('returns a failed outcome instead of throwing when the store errors', async () => {
    const handler = createMemoryStepHandler({
      store: {
        get: async () => { throw new Error('ddb down'); },
        put: async () => {},
        delete: async () => {},
      },
      pipelineIdLookup: () => 'pipe_1',
    });

    const outcome = await handler.execute(
      makeNode({ mode: 'read', scope: 'session', namespace: 'ns', keyExpression: 'k' }),
      {},
      makeHelpers(),
    );
    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/ddb down/);
  });
});

describe('memory key shape', () => {
  it('puts the scope owner in the partition key so scopes cannot leak', () => {
    expect(memoryPartitionKey({ scope: 'session', runId: 'r1' })).toBe('run/r1');
    expect(memoryPartitionKey({ scope: 'long-term', pipelineId: 'p1' })).toBe('pipeline/p1');
  });

  it('refuses a scope reference missing its owner', () => {
    expect(() => memoryPartitionKey({ scope: 'session' })).toThrow(/runId/);
    expect(() => memoryPartitionKey({ scope: 'long-term' })).toThrow(/pipelineId/);
  });

  it('rejects a namespace containing the separator, which could forge a key', () => {
    expect(() => memorySortKey('a#b', 'c')).toThrow(/must not contain/);
    expect(memorySortKey('a', 'b')).toBe('a#b');
  });
});
