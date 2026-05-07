// social-api/src/pipeline/config/__tests__/hintedHandoffQueue.test.ts
//
// Tests for setupHintedHandoffQueue: disabled path (returns null), enabled path,
// env-var-driven enabling, verifyWalParentWritable call, maxQueueDepth validation,
// and the returned handle's stop() hook.

const mockQueueStart = jest.fn<Promise<void>, []>().mockResolvedValue(undefined);
const mockQueueStop = jest.fn<Promise<void>, []>().mockResolvedValue(undefined);
const mockHintedHandoffQueueCtor = jest.fn();

jest.mock('distributed-core', () => ({
  ...jest.requireActual<typeof import('distributed-core')>('distributed-core'),
  HintedHandoffQueue: jest.fn().mockImplementation((opts: unknown) => {
    mockHintedHandoffQueueCtor(opts);
    return { start: mockQueueStart, stop: mockQueueStop };
  }),
}));

const mockVerifyWalParentWritable = jest.fn();
jest.mock('../walPreflight', () => ({
  verifyWalParentWritable: (...args: unknown[]) => mockVerifyWalParentWritable(...args),
}));

import { setupHintedHandoffQueue } from '../hintedHandoffQueue';
import type { Cluster } from 'distributed-core';

const MOCK_CLUSTER = {} as Cluster;
const origEnv = process.env;

beforeEach(() => {
  process.env = { ...origEnv };
  delete process.env.PIPELINE_HINTED_HANDOFF_ENABLED;
  delete process.env.PIPELINE_HINTED_HANDOFF_PATH;
  delete process.env.PIPELINE_HINTED_HANDOFF_MAX_BYTES;
  delete process.env.NODE_ENV;
  mockHintedHandoffQueueCtor.mockClear();
  mockQueueStart.mockClear();
  mockQueueStop.mockClear();
  mockVerifyWalParentWritable.mockClear();
});

afterAll(() => {
  process.env = origEnv;
});

// ---------------------------------------------------------------------------
// Disabled path
// ---------------------------------------------------------------------------

describe('setupHintedHandoffQueue — disabled', () => {
  it('returns null when enabled is not set (default false)', async () => {
    const result = await setupHintedHandoffQueue(MOCK_CLUSTER);
    expect(result).toBeNull();
    expect(mockHintedHandoffQueueCtor).not.toHaveBeenCalled();
  });

  it('returns null when opts.enabled is false', async () => {
    const result = await setupHintedHandoffQueue(MOCK_CLUSTER, { enabled: false });
    expect(result).toBeNull();
  });

  it('returns null when PIPELINE_HINTED_HANDOFF_ENABLED is empty string', async () => {
    process.env.PIPELINE_HINTED_HANDOFF_ENABLED = '';
    const result = await setupHintedHandoffQueue(MOCK_CLUSTER);
    expect(result).toBeNull();
  });

  it('returns null when PIPELINE_HINTED_HANDOFF_ENABLED is "false"', async () => {
    process.env.PIPELINE_HINTED_HANDOFF_ENABLED = 'false';
    const result = await setupHintedHandoffQueue(MOCK_CLUSTER);
    expect(result).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// Enabled path
// ---------------------------------------------------------------------------

describe('setupHintedHandoffQueue — enabled', () => {
  it('calls verifyWalParentWritable with the resolved dataDir', async () => {
    await setupHintedHandoffQueue(MOCK_CLUSTER, { enabled: true, dataDir: '/tmp/handoff' });
    expect(mockVerifyWalParentWritable).toHaveBeenCalledWith(
      '/tmp/handoff',
      'PIPELINE_HINTED_HANDOFF_PATH',
      false,
    );
  });

  it('starts the HintedHandoffQueue', async () => {
    await setupHintedHandoffQueue(MOCK_CLUSTER, { enabled: true, dataDir: '/tmp/handoff' });
    expect(mockQueueStart).toHaveBeenCalledTimes(1);
  });

  it('returns a handle with queue and stop properties', async () => {
    const handle = await setupHintedHandoffQueue(MOCK_CLUSTER, { enabled: true, dataDir: '/tmp/handoff' });
    expect(handle).not.toBeNull();
    expect(handle!.queue).toBeDefined();
    expect(typeof handle!.stop).toBe('function');
  });

  it('handle.stop() calls queue.stop()', async () => {
    const handle = await setupHintedHandoffQueue(MOCK_CLUSTER, { enabled: true, dataDir: '/tmp/handoff' });
    await handle!.stop();
    expect(mockQueueStop).toHaveBeenCalledTimes(1);
  });

  it('activates via PIPELINE_HINTED_HANDOFF_ENABLED=true env var', async () => {
    process.env.PIPELINE_HINTED_HANDOFF_ENABLED = 'true';
    process.env.PIPELINE_HINTED_HANDOFF_PATH = '/tmp/handoff-env';
    const result = await setupHintedHandoffQueue(MOCK_CLUSTER);
    expect(result).not.toBeNull();
  });

  it('passes maxQueueDepth to HintedHandoffQueue when provided', async () => {
    await setupHintedHandoffQueue(MOCK_CLUSTER, { enabled: true, dataDir: '/tmp/h', maxQueueDepth: 500 });
    expect(mockHintedHandoffQueueCtor).toHaveBeenCalledWith(
      expect.objectContaining({ maxQueueDepth: 500 }),
    );
  });

  it('omits maxQueueDepth from opts when not provided', async () => {
    await setupHintedHandoffQueue(MOCK_CLUSTER, { enabled: true, dataDir: '/tmp/h' });
    const opts = mockHintedHandoffQueueCtor.mock.calls[0][0] as Record<string, unknown>;
    expect(opts).not.toHaveProperty('maxQueueDepth');
  });
});

// ---------------------------------------------------------------------------
// maxQueueDepth env resolution
// ---------------------------------------------------------------------------

describe('setupHintedHandoffQueue — PIPELINE_HINTED_HANDOFF_MAX_BYTES validation', () => {
  it('throws for a non-numeric value', async () => {
    process.env.PIPELINE_HINTED_HANDOFF_MAX_BYTES = 'not-a-number';
    await expect(
      setupHintedHandoffQueue(MOCK_CLUSTER, { enabled: true, dataDir: '/tmp/h' }),
    ).rejects.toThrow('PIPELINE_HINTED_HANDOFF_MAX_BYTES');
  });

  it('throws for zero value', async () => {
    process.env.PIPELINE_HINTED_HANDOFF_MAX_BYTES = '0';
    await expect(
      setupHintedHandoffQueue(MOCK_CLUSTER, { enabled: true, dataDir: '/tmp/h' }),
    ).rejects.toThrow();
  });

  it('accepts a valid positive integer from env', async () => {
    process.env.PIPELINE_HINTED_HANDOFF_MAX_BYTES = '200';
    await setupHintedHandoffQueue(MOCK_CLUSTER, { enabled: true, dataDir: '/tmp/h' });
    expect(mockHintedHandoffQueueCtor).toHaveBeenCalledWith(
      expect.objectContaining({ maxQueueDepth: 200 }),
    );
  });
});

// ---------------------------------------------------------------------------
// dataDir resolution
// ---------------------------------------------------------------------------

describe('setupHintedHandoffQueue — dataDir resolution', () => {
  it('defaults to /tmp/pipeline-handoff in non-production', async () => {
    process.env.NODE_ENV = 'development';
    await setupHintedHandoffQueue(MOCK_CLUSTER, { enabled: true });
    expect(mockHintedHandoffQueueCtor).toHaveBeenCalledWith(
      expect.objectContaining({ dataDir: '/tmp/pipeline-handoff' }),
    );
  });

  it('uses PIPELINE_HINTED_HANDOFF_PATH env when explicit dataDir not set', async () => {
    process.env.PIPELINE_HINTED_HANDOFF_PATH = '/custom/handoff';
    await setupHintedHandoffQueue(MOCK_CLUSTER, { enabled: true });
    expect(mockHintedHandoffQueueCtor).toHaveBeenCalledWith(
      expect.objectContaining({ dataDir: '/custom/handoff' }),
    );
  });
});
