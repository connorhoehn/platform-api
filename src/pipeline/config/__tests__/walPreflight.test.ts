// social-api/src/pipeline/config/__tests__/walPreflight.test.ts
//
// Tests for verifyWalParentWritable: the error message is operator-facing
// and must name the env var + include the opt-out hint only when appropriate.

import * as path from 'path';

const mockAccessSync = jest.fn();

jest.mock('fs', () => ({
  ...jest.requireActual<typeof import('fs')>('fs'),
  accessSync: (...args: unknown[]) => mockAccessSync(...args),
  constants: jest.requireActual<typeof import('fs')>('fs').constants,
}));

// Import after mock so it picks up the mocked fs.
import { verifyWalParentWritable } from '../walPreflight';
import * as fs from 'fs';

afterEach(() => {
  mockAccessSync.mockReset();
});

describe('verifyWalParentWritable', () => {
  it('does not throw when the parent directory is writable', () => {
    mockAccessSync.mockReturnValue(undefined);
    expect(() =>
      verifyWalParentWritable('/var/lib/app/wal.db', 'PIPELINE_WAL_PATH', true),
    ).not.toThrow();
    expect(mockAccessSync).toHaveBeenCalledWith('/var/lib/app', fs.constants.W_OK);
  });

  it('throws with env var name in message when dir is not writable', () => {
    mockAccessSync.mockImplementation(() => {
      throw new Error('EACCES: permission denied');
    });
    expect(() =>
      verifyWalParentWritable('/data/wal.db', 'PIPELINE_WAL_PATH', false),
    ).toThrow('PIPELINE_WAL_PATH');
  });

  it('includes the walFilePath in the error message', () => {
    mockAccessSync.mockImplementation(() => {
      throw new Error('ENOENT: no such file');
    });
    expect(() =>
      verifyWalParentWritable('/missing/dir/wal.db', 'PIPELINE_WAL_PATH', false),
    ).toThrow('/missing/dir/wal.db');
  });

  it('includes "disabled" opt-out hint when disableHint is true', () => {
    mockAccessSync.mockImplementation(() => {
      throw new Error('EACCES: permission denied');
    });
    let err!: Error;
    try {
      verifyWalParentWritable('/data/wal.db', 'PIPELINE_WAL_PATH', true);
    } catch (e) {
      err = e as Error;
    }
    expect(err.message).toContain('PIPELINE_WAL_PATH=disabled');
  });

  it('omits "disabled" opt-out hint when disableHint is false', () => {
    mockAccessSync.mockImplementation(() => {
      throw new Error('EACCES: permission denied');
    });
    let err!: Error;
    try {
      verifyWalParentWritable('/data/wal.db', 'PIPELINE_REGISTRY_WAL_PATH', false);
    } catch (e) {
      err = e as Error;
    }
    expect(err.message).not.toContain('disabled');
    expect(err.message).toContain('PIPELINE_REGISTRY_WAL_PATH');
  });

  it('checks the parent directory, not the file itself', () => {
    mockAccessSync.mockReturnValue(undefined);
    verifyWalParentWritable('/some/deep/path/wal.db', 'PIPELINE_WAL_PATH', false);
    expect(mockAccessSync).toHaveBeenCalledWith(
      path.dirname('/some/deep/path/wal.db'),
      fs.constants.W_OK,
    );
  });
});
