// Registration contract for the memory handler.
//
// distributed-core validates at boot that every map key equals its handler's
// `kind`, and that no kind collides with a canonical NodeType. A mismatch
// throws during bootstrap, which is a hard startup failure — cheap to assert
// here, expensive to discover in a deploy.

import { buildMemoryStepHandlers } from '../registerMemoryHandlers';
import { resetMemoryStoreForTests, resolveMemoryMode } from '../memoryServices';

const CANONICAL_NODE_TYPES = [
  'trigger', 'llm', 'transform', 'condition',
  'action', 'fork', 'join', 'approval',
];

describe('buildMemoryStepHandlers', () => {
  beforeEach(() => {
    resetMemoryStoreForTests();
    delete process.env.PIPELINE_MEMORY_MODE;
  });

  it('registers exactly the memory handler', () => {
    const handlers = buildMemoryStepHandlers({ moduleRef: { current: null } });
    expect([...handlers.keys()]).toEqual(['memory']);
  });

  it('keys every handler by its own kind', () => {
    const handlers = buildMemoryStepHandlers({ moduleRef: { current: null } });
    for (const [key, handler] of handlers) {
      expect(handler.kind).toBe(key);
    }
  });

  it('does not shadow a canonical node type', () => {
    const handlers = buildMemoryStepHandlers({ moduleRef: { current: null } });
    for (const key of handlers.keys()) {
      expect(CANONICAL_NODE_TYPES).not.toContain(key);
    }
  });

  it('tolerates a module ref that is not ready yet', async () => {
    // bootstrap builds the handlers BEFORE constructing the module and patches
    // the ref afterwards, so `current` is null during construction.
    const handlers = buildMemoryStepHandlers({ moduleRef: { current: null } });
    const handler = handlers.get('memory')!;

    const outcome = await handler.execute(
      {
        id: 's1', type: 'memory' as never, position: { x: 0, y: 0 },
        data: {
          type: 'memory', mode: 'read', scope: 'long-term',
          namespace: 'ns', keyExpression: 'k',
        } as never,
      } as never,
      {},
      {
        emit: async () => {}, runId: 'run_1', ownerNodeId: 'n',
        stepId: 's1', abortSignal: new AbortController().signal,
        sleep: async () => {}, isCancelled: () => false,
      } as never,
    );

    // Unresolvable pipelineId is a clean step failure, not a crash.
    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/pipelineId/);
  });
});

describe('resolveMemoryMode', () => {
  beforeEach(() => {
    resetMemoryStoreForTests();
    delete process.env.PIPELINE_MEMORY_MODE;
  });

  it('defaults to in-process, not ddb', () => {
    // Defaulting to ddb would fail every memory node on a deployment that has
    // not provisioned the table yet.
    expect(resolveMemoryMode()).toBe('memory');
  });

  it('selects ddb when asked', () => {
    process.env.PIPELINE_MEMORY_MODE = 'DDB';
    expect(resolveMemoryMode()).toBe('ddb');
  });

  it('treats an unrecognised value as in-process rather than throwing at boot', () => {
    process.env.PIPELINE_MEMORY_MODE = 'postgres';
    expect(resolveMemoryMode()).toBe('memory');
  });
});
