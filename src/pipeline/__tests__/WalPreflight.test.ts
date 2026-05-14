// social-api/src/pipeline/__tests__/WalPreflight.test.ts
//
// Tests for runWalPreflight: walFilePath branch, registryWalFilePath branch,
// raft+raftDataDir branch (mkdirSync + accessSync), raft error wrapping,
// and combinations of multiple flags set.

import { describe, it, expect, jest, beforeEach } from '@jest/globals';

const mockVerifyWalParentWritable = jest.fn();
jest.mock('../config/walPreflight', () => ({
  verifyWalParentWritable: mockVerifyWalParentWritable,
}));

const mockMkdirSync = jest.fn();
const mockAccessSync = jest.fn();
jest.mock('fs', () => ({
  // preserve constants so fs.constants.W_OK resolves
  constants: { W_OK: 2 },
  mkdirSync: mockMkdirSync,
  accessSync: mockAccessSync,
}));

import { runWalPreflight } from '../WalPreflight';

const BASE: Parameters<typeof runWalPreflight>[0] = {
  walFilePath: undefined,
  registryWalFilePath: undefined,
  registryMode: 'memory',
  raftDataDir: undefined,
};

beforeEach(() => {
  jest.resetAllMocks();
});

// ---------------------------------------------------------------------------
// walFilePath branch
// ---------------------------------------------------------------------------

describe('runWalPreflight — walFilePath branch', () => {
  it('calls verifyWalParentWritable with PIPELINE_WAL_PATH and disableHint=true', async () => {
    await runWalPreflight({ ...BASE, walFilePath: '/var/log/pipeline.wal' });
    expect(mockVerifyWalParentWritable).toHaveBeenCalledWith(
      '/var/log/pipeline.wal',
      'PIPELINE_WAL_PATH',
      true,
    );
  });

  it('does not call verifyWalParentWritable when walFilePath is undefined', async () => {
    await runWalPreflight({ ...BASE });
    expect(mockVerifyWalParentWritable).not.toHaveBeenCalled();
  });

  it('propagates error thrown by verifyWalParentWritable', async () => {
    mockVerifyWalParentWritable.mockImplementation(() => {
      throw new Error('not writable');
    });
    await expect(
      runWalPreflight({ ...BASE, walFilePath: '/bad/path.wal' }),
    ).rejects.toThrow('not writable');
  });
});

// ---------------------------------------------------------------------------
// registryWalFilePath branch
// ---------------------------------------------------------------------------

describe('runWalPreflight — registryWalFilePath branch', () => {
  it('calls verifyWalParentWritable with PIPELINE_REGISTRY_WAL_PATH and disableHint=false', async () => {
    await runWalPreflight({ ...BASE, registryWalFilePath: '/var/log/registry.wal' });
    expect(mockVerifyWalParentWritable).toHaveBeenCalledWith(
      '/var/log/registry.wal',
      'PIPELINE_REGISTRY_WAL_PATH',
      false,
    );
  });

  it('does not call verifyWalParentWritable when registryWalFilePath is undefined', async () => {
    await runWalPreflight({ ...BASE });
    expect(mockVerifyWalParentWritable).not.toHaveBeenCalled();
  });
});

// ---------------------------------------------------------------------------
// raft + raftDataDir branch
// ---------------------------------------------------------------------------

describe('runWalPreflight — raft path', () => {
  it('calls mkdirSync with recursive:true and accessSync with W_OK', async () => {
    await runWalPreflight({ ...BASE, registryMode: 'raft', raftDataDir: '/data/raft' });
    expect(mockMkdirSync).toHaveBeenCalledWith('/data/raft', { recursive: true });
    expect(mockAccessSync).toHaveBeenCalledWith('/data/raft', 2 /* W_OK */);
  });

  it('does not call mkdirSync when registryMode is not raft', async () => {
    await runWalPreflight({ ...BASE, registryMode: 'memory', raftDataDir: '/data/raft' });
    expect(mockMkdirSync).not.toHaveBeenCalled();
  });

  it('does not call mkdirSync when raftDataDir is undefined', async () => {
    await runWalPreflight({ ...BASE, registryMode: 'raft', raftDataDir: undefined });
    expect(mockMkdirSync).not.toHaveBeenCalled();
  });

  it('wraps mkdirSync Error in a PIPELINE_RAFT_DATA_DIR message', async () => {
    mockMkdirSync.mockImplementation(() => { throw new Error('Permission denied'); });
    await expect(
      runWalPreflight({ ...BASE, registryMode: 'raft', raftDataDir: '/data/raft' }),
    ).rejects.toThrow(/PIPELINE_RAFT_DATA_DIR.*\/data\/raft.*Permission denied/);
  });

  it('wraps accessSync Error in a PIPELINE_RAFT_DATA_DIR message', async () => {
    mockAccessSync.mockImplementation(() => { throw new Error('Access denied'); });
    await expect(
      runWalPreflight({ ...BASE, registryMode: 'raft', raftDataDir: '/data/raft' }),
    ).rejects.toThrow(/PIPELINE_RAFT_DATA_DIR.*\/data\/raft.*Access denied/);
  });

  it('wraps non-Error thrown value as string', async () => {
    mockMkdirSync.mockImplementation(() => { throw 'EACCES'; }); // eslint-disable-line @typescript-eslint/only-throw-error
    await expect(
      runWalPreflight({ ...BASE, registryMode: 'raft', raftDataDir: '/data/raft' }),
    ).rejects.toThrow(/PIPELINE_RAFT_DATA_DIR.*EACCES/);
  });

  it('error message instructs operator to fix permission or set PIPELINE_RAFT_DATA_DIR', async () => {
    mockMkdirSync.mockImplementation(() => { throw new Error('EPERM'); });
    await expect(
      runWalPreflight({ ...BASE, registryMode: 'raft', raftDataDir: '/data/raft' }),
    ).rejects.toThrow(/Fix the filesystem permission or set PIPELINE_RAFT_DATA_DIR/);
  });
});

// ---------------------------------------------------------------------------
// Combinations
// ---------------------------------------------------------------------------

describe('runWalPreflight — multiple flags set', () => {
  it('calls both verifyWalParentWritable branches when both paths are set', async () => {
    await runWalPreflight({
      ...BASE,
      walFilePath: '/a.wal',
      registryWalFilePath: '/b.wal',
    });
    expect(mockVerifyWalParentWritable).toHaveBeenCalledTimes(2);
    expect(mockVerifyWalParentWritable).toHaveBeenCalledWith('/a.wal', 'PIPELINE_WAL_PATH', true);
    expect(mockVerifyWalParentWritable).toHaveBeenCalledWith('/b.wal', 'PIPELINE_REGISTRY_WAL_PATH', false);
  });

  it('runs all three branches when walFilePath + registryWalFilePath + raft are all set', async () => {
    await runWalPreflight({
      walFilePath: '/a.wal',
      registryWalFilePath: '/b.wal',
      registryMode: 'raft',
      raftDataDir: '/data/raft',
    });
    expect(mockVerifyWalParentWritable).toHaveBeenCalledTimes(2);
    expect(mockMkdirSync).toHaveBeenCalledWith('/data/raft', { recursive: true });
    expect(mockAccessSync).toHaveBeenCalledWith('/data/raft', 2);
  });
});
