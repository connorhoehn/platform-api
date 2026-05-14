// src/lib/__tests__/dc-pipeline-declarations.test.ts
//
// Phase P3.3 — verifies platform-api declares the distributed-core
// in-process signal-transport pipeline events on DC's behalf:
//   - pipeline:run:started
//   - pipeline:run:completed
//   - pipeline:run:failed
//
// Each is a SEPARATE declaration from its durable counterpart
// (pipeline.run.started/completed/failed) which platform-api re-publishes
// after subscribing to the DC bus event.

// Capture catalog.declare calls without spinning up redis/message-queue.
const declareCalls: Array<{ name: string; transport: string; producer: string | string[]; description?: string; tags?: string[] }> = [];

jest.mock('@connorhoehn/event-catalog', () => ({
  ...jest.requireActual('@connorhoehn/event-catalog'),
  EventCatalog: jest.fn().mockImplementation(() => ({
    connect: jest.fn().mockResolvedValue(undefined),
    disconnect: jest.fn().mockResolvedValue(undefined),
    declare: jest.fn((decl: { name: string; transport: string; producer: string | string[]; description?: string; tags?: string[] }) => {
      declareCalls.push(decl);
    }),
    publish: jest.fn().mockResolvedValue(undefined),
    getSignalTransport: jest.fn(),
  })),
}));

// Import the module under test — declarations fire at import time.
import '../event-catalog';

describe('Phase P3.3 — distributed-core pipeline event declarations', () => {
  it('declares pipeline:run:started as a signal-transport event from distributed-core', () => {
    const decl = declareCalls.find((d) => d.name === 'pipeline:run:started');
    expect(decl).toBeDefined();
    expect(decl!.transport).toBe('signal');
    expect(decl!.producer).toBe('distributed-core');
    expect(decl!.tags).toContain('producer:distributed-core');
  });

  it('declares pipeline:run:completed as a signal-transport event from distributed-core', () => {
    const decl = declareCalls.find((d) => d.name === 'pipeline:run:completed');
    expect(decl).toBeDefined();
    expect(decl!.transport).toBe('signal');
    expect(decl!.producer).toBe('distributed-core');
  });

  it('declares pipeline:run:failed as a signal-transport event from distributed-core', () => {
    const decl = declareCalls.find((d) => d.name === 'pipeline:run:failed');
    expect(decl).toBeDefined();
    expect(decl!.transport).toBe('signal');
    expect(decl!.producer).toBe('distributed-core');
  });

  it('keeps the durable pipeline.run.completed declaration separate (platform-api producer)', () => {
    const durableDecl = declareCalls.find((d) => d.name === 'pipeline.run.completed');
    expect(durableDecl).toBeDefined();
    expect(durableDecl!.transport).toBe('durable');
    expect(durableDecl!.producer).toBe('platform-api');
  });

  it('signal and durable forms have distinct names (no name collision)', () => {
    const colon = declareCalls.find((d) => d.name === 'pipeline:run:completed');
    const dot = declareCalls.find((d) => d.name === 'pipeline.run.completed');
    expect(colon).toBeDefined();
    expect(dot).toBeDefined();
    expect(colon!.transport).not.toBe(dot!.transport);
    expect(colon!.producer).not.toBe(dot!.producer);
  });
});

// ---------------------------------------------------------------------------
// wave2-pa-1 — platform-api Raft state-machine event declarations
//
// PipelineRaftStateMachine emits in-process signal events to local listeners.
// The Raft variants live under the `pipeline:raft:run:*` namespace to
// disambiguate from DC's executor `pipeline:run:*` events (different
// producer + payload).
// ---------------------------------------------------------------------------

describe('wave2-pa-1 — Raft state-machine event declarations', () => {
  it('declares pipeline:raft:run:started as a signal-transport event from platform-api', () => {
    const decl = declareCalls.find((d) => d.name === 'pipeline:raft:run:started');
    expect(decl).toBeDefined();
    expect(decl!.transport).toBe('signal');
    expect(decl!.producer).toBe('platform-api');
    expect(decl!.tags).toEqual(expect.arrayContaining(['producer:platform-api', 'pipeline', 'raft']));
  });

  it('declares pipeline:raft:run:updated as a signal-transport event from platform-api', () => {
    const decl = declareCalls.find((d) => d.name === 'pipeline:raft:run:updated');
    expect(decl).toBeDefined();
    expect(decl!.transport).toBe('signal');
    expect(decl!.producer).toBe('platform-api');
  });

  it('declares pipeline:raft:run:synthesized as a signal-transport event from platform-api', () => {
    const decl = declareCalls.find((d) => d.name === 'pipeline:raft:run:synthesized');
    expect(decl).toBeDefined();
    expect(decl!.transport).toBe('signal');
    expect(decl!.producer).toBe('platform-api');
  });

  it('declares pipeline:raft:run:checkpointed as a signal-transport event from platform-api', () => {
    const decl = declareCalls.find((d) => d.name === 'pipeline:raft:run:checkpointed');
    expect(decl).toBeDefined();
    expect(decl!.transport).toBe('signal');
    expect(decl!.producer).toBe('platform-api');
  });

  it('declares pipeline:raft:run:terminated as a signal-transport event from platform-api', () => {
    const decl = declareCalls.find((d) => d.name === 'pipeline:raft:run:terminated');
    expect(decl).toBeDefined();
    expect(decl!.transport).toBe('signal');
    expect(decl!.producer).toBe('platform-api');
  });

  it('Raft `pipeline:raft:run:started` is distinct from DC `pipeline:run:started` (different producer)', () => {
    const dcStarted = declareCalls.find((d) => d.name === 'pipeline:run:started');
    const raftStarted = declareCalls.find((d) => d.name === 'pipeline:raft:run:started');
    expect(dcStarted).toBeDefined();
    expect(raftStarted).toBeDefined();
    expect(dcStarted!.name).not.toBe(raftStarted!.name);
    expect(dcStarted!.producer).toBe('distributed-core');
    expect(raftStarted!.producer).toBe('platform-api');
  });
});
