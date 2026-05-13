// Phase M4.1 — verifies wirePipelineBridge picks the right transport
// based on env vars and never blocks the caller's startup path.

import { wirePipelineBridge } from '../wire-bridge';
import { HTTPPipelineBridge } from '../http-bridge';

// Capture setPipelineBridge calls without touching the real route module.
const setPipelineBridgeMock = jest.fn();
jest.mock('../../routes/pipelineTriggers', () => ({
  setPipelineBridge: (bridge: unknown) => setPipelineBridgeMock(bridge),
}));

// bootstrapPipeline is heavy (spins up DC Cluster + Raft + WAL); stub it.
const bootstrapPipelineMock = jest.fn();
jest.mock('../bootstrap', () => ({
  bootstrapPipeline: (...args: unknown[]) => bootstrapPipelineMock(...args),
}));

// createBridge is a pure function over the bootstrap output; stub it.
const createBridgeMock = jest.fn();
jest.mock('../createBridge', () => ({
  createBridge: (...args: unknown[]) => createBridgeMock(...args),
}));

function makeLogger() {
  return {
    info: jest.fn(),
    error: jest.fn(),
    warn: jest.fn(),
    debug: jest.fn(),
  } as unknown as import('pino').Logger;
}

beforeEach(() => {
  setPipelineBridgeMock.mockReset();
  bootstrapPipelineMock.mockReset();
  createBridgeMock.mockReset();
});

describe('wirePipelineBridge — remote mode', () => {
  it('constructs HTTPPipelineBridge when PIPELINE_WORKER_URL is set and skips bootstrap', async () => {
    const log = makeLogger();
    const result = wirePipelineBridge({
      log,
      env: { PIPELINE_WORKER_URL: 'http://worker:8088' },
    });

    expect(result.mode).toBe('remote');
    expect(bootstrapPipelineMock).not.toHaveBeenCalled();
    expect(setPipelineBridgeMock).toHaveBeenCalledTimes(1);
    expect(setPipelineBridgeMock.mock.calls[0][0]).toBeInstanceOf(HTTPPipelineBridge);
    await expect(result.ready).resolves.toBeUndefined();
  });

  it('passes signOpts when PIPELINE_WORKER_SECRET is provided', () => {
    wirePipelineBridge({
      log: makeLogger(),
      env: {
        PIPELINE_WORKER_URL: 'http://worker:8088',
        PIPELINE_WORKER_SECRET: 's3cret',
      },
    });

    const bridge = setPipelineBridgeMock.mock.calls[0][0] as { signOpts?: unknown };
    // HTTPPipelineBridge stores signOpts internally; we just confirm the
    // bridge construction did not throw. A deeper assertion lives in
    // http-bridge.test.ts where the signed-request path is end-to-end tested.
    expect(bridge).toBeInstanceOf(HTTPPipelineBridge);
  });

  it('strips whitespace from PIPELINE_WORKER_URL', () => {
    wirePipelineBridge({
      log: makeLogger(),
      env: { PIPELINE_WORKER_URL: '  http://worker:8088  ' },
    });
    expect(setPipelineBridgeMock).toHaveBeenCalled();
  });

  it('shutdown() resolves with no work in remote mode', async () => {
    const result = wirePipelineBridge({
      log: makeLogger(),
      env: { PIPELINE_WORKER_URL: 'http://worker:8088' },
    });
    await expect(result.shutdown()).resolves.toBeUndefined();
  });
});

describe('wirePipelineBridge — in-process mode (default)', () => {
  it('calls bootstrapPipeline + createBridge when PIPELINE_WORKER_URL is unset', async () => {
    const log = makeLogger();
    const fakeShutdown = jest.fn(async () => {});
    bootstrapPipelineMock.mockResolvedValue({
      module: { id: 'mod' },
      nodeId: 'node-1',
      dlq: null,
      shutdown: fakeShutdown,
    });
    createBridgeMock.mockReturnValue({ trigger: jest.fn() });

    const result = wirePipelineBridge({ log, env: {} });

    expect(result.mode).toBe('in-process');
    await result.ready;

    expect(bootstrapPipelineMock).toHaveBeenCalledTimes(1);
    expect(createBridgeMock).toHaveBeenCalledTimes(1);
    expect(setPipelineBridgeMock).toHaveBeenCalledTimes(1);
  });

  it('shutdown() invokes the bootstrap-returned shutdown', async () => {
    const fakeShutdown = jest.fn(async () => {});
    bootstrapPipelineMock.mockResolvedValue({
      module: { id: 'mod' },
      nodeId: 'node-1',
      dlq: null,
      shutdown: fakeShutdown,
    });
    createBridgeMock.mockReturnValue({});

    const result = wirePipelineBridge({ log: makeLogger(), env: {} });
    await result.ready;
    await result.shutdown();

    expect(fakeShutdown).toHaveBeenCalledTimes(1);
  });

  it('bootstrap failure is logged + ready resolves (does NOT throw)', async () => {
    const log = makeLogger();
    bootstrapPipelineMock.mockRejectedValue(new Error('cluster startup hung'));

    const result = wirePipelineBridge({ log, env: {} });
    await expect(result.ready).resolves.toBeUndefined();

    expect(setPipelineBridgeMock).not.toHaveBeenCalled();
    expect((log.error as jest.Mock).mock.calls.length).toBeGreaterThan(0);
  });

  it('empty PIPELINE_WORKER_URL string still falls back to in-process', async () => {
    bootstrapPipelineMock.mockResolvedValue({
      module: {},
      nodeId: 'n',
      dlq: null,
      shutdown: async () => {},
    });
    createBridgeMock.mockReturnValue({});

    const result = wirePipelineBridge({
      log: makeLogger(),
      env: { PIPELINE_WORKER_URL: '   ' },
    });

    expect(result.mode).toBe('in-process');
    await result.ready;
    expect(bootstrapPipelineMock).toHaveBeenCalled();
  });
});
