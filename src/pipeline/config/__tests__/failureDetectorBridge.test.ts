// social-api/src/pipeline/config/__tests__/failureDetectorBridge.test.ts
//
// Tests for setupFailureDetectorBridge: disabled path (returns null), enabled path,
// stop() reverse-order lifecycle, topic resolution, and heartbeatIntervalMs resolution.

const mockHeartbeatSourceStart = jest.fn<Promise<void>, []>().mockResolvedValue(undefined);
const mockHeartbeatSourceStop = jest.fn<Promise<void>, []>().mockResolvedValue(undefined);
const mockBridgeStart = jest.fn<Promise<void>, []>().mockResolvedValue(undefined);
const mockBridgeStop = jest.fn<Promise<void>, []>().mockResolvedValue(undefined);
const mockHeartbeatSourceCtor = jest.fn();
const mockBridgeCtor = jest.fn();

jest.mock('distributed-core', () => ({
  ...jest.requireActual<typeof import('distributed-core')>('distributed-core'),
  PubSubHeartbeatSource: jest.fn().mockImplementation((...args: unknown[]) => {
    mockHeartbeatSourceCtor(...args);
    return { start: mockHeartbeatSourceStart, stop: mockHeartbeatSourceStop };
  }),
  FailureDetectorBridge: jest.fn().mockImplementation((...args: unknown[]) => {
    mockBridgeCtor(...args);
    return { start: mockBridgeStart, stop: mockBridgeStop };
  }),
}));

import { setupFailureDetectorBridge } from '../failureDetectorBridge';
import { DistributedLock } from 'distributed-core';
import type { Cluster } from 'distributed-core';

const MOCK_CLUSTER = {
  pubsub: { _id: 'pubsub' },
  failureDetector: { _id: 'fd' },
  clusterManager: { localNodeId: 'node-test' },
  router: { _id: 'router' },
  // setupFailureDetectorBridge runtime-narrows cluster.lock with
  // `instanceof DistributedLock` (non-instances are dropped from the bridge
  // targets), so the mock must sit on the real prototype to be wired through.
  lock: Object.assign(Object.create(DistributedLock.prototype), { _id: 'lock' }),
} as unknown as Cluster;

const origEnv = process.env;

beforeEach(() => {
  process.env = { ...origEnv };
  delete process.env.PIPELINE_FD_BRIDGE_ENABLED;
  delete process.env.PIPELINE_FD_BRIDGE_TOPIC;
  delete process.env.PIPELINE_FD_BRIDGE_HEARTBEAT_MS;
  mockHeartbeatSourceCtor.mockClear();
  mockHeartbeatSourceStart.mockClear();
  mockHeartbeatSourceStop.mockClear();
  mockBridgeCtor.mockClear();
  mockBridgeStart.mockClear();
  mockBridgeStop.mockClear();
});

afterAll(() => {
  process.env = origEnv;
});

// ---------------------------------------------------------------------------
// Disabled path
// ---------------------------------------------------------------------------

describe('setupFailureDetectorBridge — disabled', () => {
  it('returns null when enabled is not set (default false)', async () => {
    const result = await setupFailureDetectorBridge(MOCK_CLUSTER);
    expect(result).toBeNull();
    expect(mockBridgeCtor).not.toHaveBeenCalled();
  });

  it('returns null when opts.enabled is false', async () => {
    const result = await setupFailureDetectorBridge(MOCK_CLUSTER, { enabled: false });
    expect(result).toBeNull();
  });

  it('returns null when PIPELINE_FD_BRIDGE_ENABLED is empty string', async () => {
    process.env.PIPELINE_FD_BRIDGE_ENABLED = '';
    const result = await setupFailureDetectorBridge(MOCK_CLUSTER);
    expect(result).toBeNull();
  });

  it('returns null when PIPELINE_FD_BRIDGE_ENABLED is "false"', async () => {
    process.env.PIPELINE_FD_BRIDGE_ENABLED = 'false';
    const result = await setupFailureDetectorBridge(MOCK_CLUSTER);
    expect(result).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// Enabled path
// ---------------------------------------------------------------------------

describe('setupFailureDetectorBridge — enabled', () => {
  it('starts PubSubHeartbeatSource', async () => {
    await setupFailureDetectorBridge(MOCK_CLUSTER, { enabled: true });
    expect(mockHeartbeatSourceStart).toHaveBeenCalledTimes(1);
  });

  it('starts FailureDetectorBridge', async () => {
    await setupFailureDetectorBridge(MOCK_CLUSTER, { enabled: true });
    expect(mockBridgeStart).toHaveBeenCalledTimes(1);
  });

  it('returns handle with stop function', async () => {
    const handle = await setupFailureDetectorBridge(MOCK_CLUSTER, { enabled: true });
    expect(handle).not.toBeNull();
    expect(typeof handle!.stop).toBe('function');
  });

  it('passes cluster.pubsub, cluster.failureDetector, and localNodeId to PubSubHeartbeatSource', async () => {
    await setupFailureDetectorBridge(MOCK_CLUSTER, { enabled: true });
    expect(mockHeartbeatSourceCtor).toHaveBeenCalledWith(
      MOCK_CLUSTER.pubsub,
      MOCK_CLUSTER.failureDetector,
      'node-test',
      expect.any(Object),
    );
  });

  it('passes { handleSuspected: true } and router+lock targets to FailureDetectorBridge', async () => {
    await setupFailureDetectorBridge(MOCK_CLUSTER, { enabled: true });
    expect(mockBridgeCtor).toHaveBeenCalledWith(
      MOCK_CLUSTER.failureDetector,
      expect.objectContaining({ router: MOCK_CLUSTER.router, lock: MOCK_CLUSTER.lock }),
      { handleSuspected: true },
    );
  });

  it('activates via PIPELINE_FD_BRIDGE_ENABLED=true env var', async () => {
    process.env.PIPELINE_FD_BRIDGE_ENABLED = 'true';
    const result = await setupFailureDetectorBridge(MOCK_CLUSTER);
    expect(result).not.toBeNull();
  });
});

// ---------------------------------------------------------------------------
// stop() lifecycle — reverse construction order
// ---------------------------------------------------------------------------

describe('setupFailureDetectorBridge — stop() lifecycle', () => {
  it('stop() calls both bridge.stop() and heartbeatSource.stop()', async () => {
    const handle = await setupFailureDetectorBridge(MOCK_CLUSTER, { enabled: true });
    await handle!.stop();
    expect(mockBridgeStop).toHaveBeenCalledTimes(1);
    expect(mockHeartbeatSourceStop).toHaveBeenCalledTimes(1);
  });

  it('stop() calls bridge.stop() before heartbeatSource.stop()', async () => {
    const callOrder: string[] = [];
    mockBridgeStop.mockImplementation(async () => { callOrder.push('bridge'); });
    mockHeartbeatSourceStop.mockImplementation(async () => { callOrder.push('heartbeat'); });

    const handle = await setupFailureDetectorBridge(MOCK_CLUSTER, { enabled: true });
    await handle!.stop();
    expect(callOrder).toEqual(['bridge', 'heartbeat']);
  });
});

// ---------------------------------------------------------------------------
// Topic resolution
// ---------------------------------------------------------------------------

describe('setupFailureDetectorBridge — topic resolution', () => {
  it('defaults to "pipeline.heartbeat" when no opts.topic or env', async () => {
    await setupFailureDetectorBridge(MOCK_CLUSTER, { enabled: true });
    expect(mockHeartbeatSourceCtor).toHaveBeenCalledWith(
      expect.anything(),
      expect.anything(),
      expect.anything(),
      expect.objectContaining({ topic: 'pipeline.heartbeat' }),
    );
  });

  it('uses explicit opts.topic when provided', async () => {
    await setupFailureDetectorBridge(MOCK_CLUSTER, { enabled: true, topic: 'my.custom.topic' });
    expect(mockHeartbeatSourceCtor).toHaveBeenCalledWith(
      expect.anything(),
      expect.anything(),
      expect.anything(),
      expect.objectContaining({ topic: 'my.custom.topic' }),
    );
  });

  it('uses PIPELINE_FD_BRIDGE_TOPIC env when no explicit topic', async () => {
    process.env.PIPELINE_FD_BRIDGE_TOPIC = 'env.custom.topic';
    await setupFailureDetectorBridge(MOCK_CLUSTER, { enabled: true });
    expect(mockHeartbeatSourceCtor).toHaveBeenCalledWith(
      expect.anything(),
      expect.anything(),
      expect.anything(),
      expect.objectContaining({ topic: 'env.custom.topic' }),
    );
  });
});

// ---------------------------------------------------------------------------
// HeartbeatIntervalMs resolution
// ---------------------------------------------------------------------------

describe('setupFailureDetectorBridge — heartbeatIntervalMs resolution', () => {
  it('defaults to 1000ms when no opts or env', async () => {
    await setupFailureDetectorBridge(MOCK_CLUSTER, { enabled: true });
    expect(mockHeartbeatSourceCtor).toHaveBeenCalledWith(
      expect.anything(),
      expect.anything(),
      expect.anything(),
      expect.objectContaining({ heartbeatIntervalMs: 1000 }),
    );
  });

  it('uses explicit opts.heartbeatIntervalMs when provided', async () => {
    await setupFailureDetectorBridge(MOCK_CLUSTER, { enabled: true, heartbeatIntervalMs: 500 });
    expect(mockHeartbeatSourceCtor).toHaveBeenCalledWith(
      expect.anything(),
      expect.anything(),
      expect.anything(),
      expect.objectContaining({ heartbeatIntervalMs: 500 }),
    );
  });

  it('uses PIPELINE_FD_BRIDGE_HEARTBEAT_MS env when no explicit value', async () => {
    process.env.PIPELINE_FD_BRIDGE_HEARTBEAT_MS = '2000';
    await setupFailureDetectorBridge(MOCK_CLUSTER, { enabled: true });
    expect(mockHeartbeatSourceCtor).toHaveBeenCalledWith(
      expect.anything(),
      expect.anything(),
      expect.anything(),
      expect.objectContaining({ heartbeatIntervalMs: 2000 }),
    );
  });

  it('falls back to 1000ms when PIPELINE_FD_BRIDGE_HEARTBEAT_MS is non-numeric', async () => {
    process.env.PIPELINE_FD_BRIDGE_HEARTBEAT_MS = 'not-a-number';
    await setupFailureDetectorBridge(MOCK_CLUSTER, { enabled: true });
    expect(mockHeartbeatSourceCtor).toHaveBeenCalledWith(
      expect.anything(),
      expect.anything(),
      expect.anything(),
      expect.objectContaining({ heartbeatIntervalMs: 1000 }),
    );
  });

  it('falls back to 1000ms when PIPELINE_FD_BRIDGE_HEARTBEAT_MS is zero', async () => {
    process.env.PIPELINE_FD_BRIDGE_HEARTBEAT_MS = '0';
    await setupFailureDetectorBridge(MOCK_CLUSTER, { enabled: true });
    expect(mockHeartbeatSourceCtor).toHaveBeenCalledWith(
      expect.anything(),
      expect.anything(),
      expect.anything(),
      expect.objectContaining({ heartbeatIntervalMs: 1000 }),
    );
  });
});
