// social-api/src/pipeline/config/__tests__/registries.test.ts
//
// Tests for buildAppLayerRegistries: mode switching (memory/wal/raft),
// validation errors for missing required args, and start() lifecycle calls.

const mockResourceRegistryStart = jest.fn<Promise<void>, []>().mockResolvedValue(undefined);
const mockModuleRegistryStart = jest.fn<Promise<void>, []>().mockResolvedValue(undefined);
const mockResourceRegistryCtor = jest.fn();
const mockResourceTypeRegistryCtor = jest.fn();
const mockStateAggregatorCtor = jest.fn();
const mockMetricsTrackerCtor = jest.fn();
const mockTopologyManagerCtor = jest.fn();
const mockApplicationRegistryCtor = jest.fn();

jest.mock('distributed-core', () => ({
  ...jest.requireActual<typeof import('distributed-core')>('distributed-core'),
  ResourceRegistry: jest.fn().mockImplementation((opts: unknown) => {
    mockResourceRegistryCtor(opts);
    return { start: mockResourceRegistryStart, stop: jest.fn() };
  }),
  ResourceTypeRegistry: jest.fn().mockImplementation(() => {
    mockResourceTypeRegistryCtor();
    return {};
  }),
  StateAggregator: jest.fn().mockImplementation((mgr: unknown) => {
    mockStateAggregatorCtor(mgr);
    return {};
  }),
  MetricsTracker: jest.fn().mockImplementation((opts: unknown) => {
    mockMetricsTrackerCtor(opts);
    return {};
  }),
  ResourceTopologyManager: jest.fn().mockImplementation((...args: unknown[]) => {
    mockTopologyManagerCtor(...args);
    return {};
  }),
  ApplicationRegistry: jest.fn().mockImplementation((...args: unknown[]) => {
    mockApplicationRegistryCtor(...args);
    return { start: mockModuleRegistryStart, stop: jest.fn() };
  }),
}));

import { buildAppLayerRegistries, type BuildAppLayerRegistriesArgs } from '../registries';
import type { ClusterManager, EntityRegistry } from 'distributed-core';

const MOCK_CLUSTER_MGR = {} as ClusterManager;
const MOCK_ENTITY_REGISTRY = {} as EntityRegistry;

function args(overrides: Partial<BuildAppLayerRegistriesArgs> = {}): BuildAppLayerRegistriesArgs {
  return {
    nodeId: 'node-1',
    clusterMgr: MOCK_CLUSTER_MGR,
    mode: 'memory',
    registryWalFilePath: undefined,
    ...overrides,
  };
}

beforeEach(() => {
  mockResourceRegistryCtor.mockClear();
  mockModuleRegistryStart.mockClear();
  mockResourceRegistryStart.mockClear();
  mockApplicationRegistryCtor.mockClear();
});

// ---------------------------------------------------------------------------
// memory mode
// ---------------------------------------------------------------------------

describe('buildAppLayerRegistries — memory mode', () => {
  it('constructs ResourceRegistry with entityRegistryType=memory', async () => {
    await buildAppLayerRegistries(args({ mode: 'memory' }));
    expect(mockResourceRegistryCtor).toHaveBeenCalledWith(
      expect.objectContaining({ entityRegistryType: 'memory' }),
    );
  });

  it('returns empty warnings array', async () => {
    const result = await buildAppLayerRegistries(args({ mode: 'memory' }));
    expect(result.warnings).toEqual([]);
  });

  it('calls resourceRegistry.start()', async () => {
    await buildAppLayerRegistries(args({ mode: 'memory' }));
    expect(mockResourceRegistryStart).toHaveBeenCalledTimes(1);
  });

  it('calls moduleRegistry.start()', async () => {
    await buildAppLayerRegistries(args({ mode: 'memory' }));
    expect(mockModuleRegistryStart).toHaveBeenCalledTimes(1);
  });

  it('returns all expected registry fields', async () => {
    const result = await buildAppLayerRegistries(args({ mode: 'memory' }));
    expect(result.resourceRegistry).toBeDefined();
    expect(result.resourceTypeRegistry).toBeDefined();
    expect(result.topologyManager).toBeDefined();
    expect(result.moduleRegistry).toBeDefined();
  });
});

// ---------------------------------------------------------------------------
// wal mode
// ---------------------------------------------------------------------------

describe('buildAppLayerRegistries — wal mode', () => {
  it('throws when registryWalFilePath is missing', async () => {
    await expect(
      buildAppLayerRegistries(args({ mode: 'wal', registryWalFilePath: undefined })),
    ).rejects.toThrow('PIPELINE_REGISTRY_WAL_PATH');
  });

  it('constructs ResourceRegistry with entityRegistryType=wal and walConfig', async () => {
    await buildAppLayerRegistries(args({ mode: 'wal', registryWalFilePath: '/data/registry.wal' }));
    expect(mockResourceRegistryCtor).toHaveBeenCalledWith(
      expect.objectContaining({
        entityRegistryType: 'wal',
        entityRegistryConfig: { walConfig: { filePath: '/data/registry.wal' } },
      }),
    );
  });
});

// ---------------------------------------------------------------------------
// raft mode
// ---------------------------------------------------------------------------

describe('buildAppLayerRegistries — raft mode', () => {
  it('throws when clusterEntityRegistry is missing', async () => {
    await expect(
      buildAppLayerRegistries(args({ mode: 'raft', clusterEntityRegistry: undefined })),
    ).rejects.toThrow('clusterEntityRegistry');
  });

  it('injects clusterEntityRegistry into ResourceRegistry', async () => {
    await buildAppLayerRegistries(args({ mode: 'raft', clusterEntityRegistry: MOCK_ENTITY_REGISTRY }));
    expect(mockResourceRegistryCtor).toHaveBeenCalledWith(
      expect.objectContaining({ entityRegistry: MOCK_ENTITY_REGISTRY }),
    );
  });

  it('does NOT pass entityRegistryType in raft mode', async () => {
    await buildAppLayerRegistries(args({ mode: 'raft', clusterEntityRegistry: MOCK_ENTITY_REGISTRY }));
    const callArg = mockResourceRegistryCtor.mock.calls[0][0] as Record<string, unknown>;
    expect(callArg).not.toHaveProperty('entityRegistryType');
  });
});

// ---------------------------------------------------------------------------
// unknown mode (exhaustive check)
// ---------------------------------------------------------------------------

describe('buildAppLayerRegistries — unknown mode', () => {
  it('throws for an unrecognized mode string', async () => {
    await expect(
      buildAppLayerRegistries(args({ mode: 'unknown-mode' as never })),
    ).rejects.toThrow('unhandled mode');
  });
});
