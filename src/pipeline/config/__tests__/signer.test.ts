// social-api/src/pipeline/config/__tests__/signer.test.ts
//
// Tests for buildRaftSigner: undefined when no dir, error on unreadable dir,
// auto-creates dir, reads existing PEMs, mints + persists fresh keys.

import * as path from 'path';

const mockExistsSync = jest.fn<boolean, [string]>();
const mockAccessSync = jest.fn();
const mockMkdirSync = jest.fn();
const mockReadFileSync = jest.fn<string, [string, BufferEncoding]>();
const mockWriteFileSync = jest.fn();

jest.mock('fs', () => ({
  ...jest.requireActual<typeof import('fs')>('fs'),
  existsSync: (p: string) => mockExistsSync(p),
  accessSync: (...args: unknown[]) => mockAccessSync(...args),
  mkdirSync: (...args: unknown[]) => mockMkdirSync(...args),
  readFileSync: (p: string, enc: BufferEncoding) => mockReadFileSync(p, enc),
  writeFileSync: (...args: unknown[]) => mockWriteFileSync(...args),
  constants: jest.requireActual<typeof import('fs')>('fs').constants,
}));

const mockGetPrivateKey = jest.fn<string, []>(() => '---PRIVATE---');
const mockGetPublicKey = jest.fn<string, []>(() => '---PUBLIC---');
const mockKeyManagerConstructor = jest.fn();

jest.mock('distributed-core', () => ({
  ...jest.requireActual<typeof import('distributed-core')>('distributed-core'),
  KeyManager: jest.fn().mockImplementation((opts: unknown) => {
    mockKeyManagerConstructor(opts);
    return {
      getPrivateKey: mockGetPrivateKey,
      getPublicKey: mockGetPublicKey,
      signClusterPayload: jest.fn(),
      verifyClusterPayload: jest.fn(),
    };
  }),
}));

import { buildRaftSigner } from '../signer';

const NODE_ID = 'node-test';
const SIGNER_DIR = '/var/lib/signer';
const PRIVATE_PATH = path.join(SIGNER_DIR, `${NODE_ID}.private.pem`);
const PUBLIC_PATH = path.join(SIGNER_DIR, `${NODE_ID}.public.pem`);

const origEnv = process.env;

beforeEach(() => {
  process.env = { ...origEnv };
  delete process.env.PIPELINE_RAFT_SIGNER_DIR;
  mockExistsSync.mockReset();
  mockAccessSync.mockReset();
  mockMkdirSync.mockReset();
  mockReadFileSync.mockReset();
  mockWriteFileSync.mockReset();
  mockKeyManagerConstructor.mockReset();
});

afterAll(() => {
  process.env = origEnv;
});

// ---------------------------------------------------------------------------
// No dir configured — return undefined
// ---------------------------------------------------------------------------

describe('buildRaftSigner — no directory configured', () => {
  it('returns undefined when keyManagerSecretsDir is not set and env var is absent', () => {
    const result = buildRaftSigner({ nodeId: NODE_ID });
    expect(result).toBeUndefined();
  });

  it('returns undefined when keyManagerSecretsDir is empty string', () => {
    const result = buildRaftSigner({ nodeId: NODE_ID, keyManagerSecretsDir: '' });
    expect(result).toBeUndefined();
  });

  it('returns undefined when keyManagerSecretsDir is whitespace', () => {
    const result = buildRaftSigner({ nodeId: NODE_ID, keyManagerSecretsDir: '   ' });
    expect(result).toBeUndefined();
  });

  it('uses PIPELINE_RAFT_SIGNER_DIR env var when no explicit opts', () => {
    process.env.PIPELINE_RAFT_SIGNER_DIR = SIGNER_DIR;
    // Dir doesn't exist → auto-created; no PEMs → mint fresh
    mockExistsSync.mockImplementation((p: string) => p === SIGNER_DIR ? false : false);
    const result = buildRaftSigner({ nodeId: NODE_ID });
    expect(result).not.toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// Directory does not exist — auto-creates
// ---------------------------------------------------------------------------

describe('buildRaftSigner — auto-create directory', () => {
  it('calls mkdirSync when the directory does not exist', () => {
    mockExistsSync.mockReturnValue(false);
    buildRaftSigner({ nodeId: NODE_ID, keyManagerSecretsDir: SIGNER_DIR });
    expect(mockMkdirSync).toHaveBeenCalledWith(
      path.resolve(SIGNER_DIR),
      expect.objectContaining({ recursive: true }),
    );
  });
});

// ---------------------------------------------------------------------------
// Directory exists but is not readable/writable — throw
// ---------------------------------------------------------------------------

describe('buildRaftSigner — unreadable directory', () => {
  it('throws with PIPELINE_RAFT_SIGNER_DIR in message when dir is not accessible', () => {
    mockExistsSync.mockReturnValue(true);
    mockAccessSync.mockImplementation(() => { throw new Error('EACCES: permission denied'); });

    expect(() =>
      buildRaftSigner({ nodeId: NODE_ID, keyManagerSecretsDir: SIGNER_DIR }),
    ).toThrow('PIPELINE_RAFT_SIGNER_DIR');
  });

  it('error message includes the resolved path', () => {
    mockExistsSync.mockReturnValue(true);
    mockAccessSync.mockImplementation(() => { throw new Error('EACCES'); });

    expect(() =>
      buildRaftSigner({ nodeId: NODE_ID, keyManagerSecretsDir: SIGNER_DIR }),
    ).toThrow(SIGNER_DIR);
  });
});

// ---------------------------------------------------------------------------
// PEM files exist — reads them and constructs KeyManager with them
// ---------------------------------------------------------------------------

describe('buildRaftSigner — existing PEM files', () => {
  beforeEach(() => {
    // Dir exists and is accessible
    mockExistsSync.mockImplementation((p: string) => {
      return p === path.resolve(SIGNER_DIR) || p === PRIVATE_PATH || p === PUBLIC_PATH;
    });
    mockAccessSync.mockReturnValue(undefined);
    mockReadFileSync.mockImplementation((p: string) => {
      if (p === PRIVATE_PATH) return '---PRIVATE PEM---';
      if (p === PUBLIC_PATH) return '---PUBLIC PEM---';
      return '';
    });
  });

  it('reads both PEM files', () => {
    buildRaftSigner({ nodeId: NODE_ID, keyManagerSecretsDir: SIGNER_DIR });
    expect(mockReadFileSync).toHaveBeenCalledWith(PRIVATE_PATH, 'utf8');
    expect(mockReadFileSync).toHaveBeenCalledWith(PUBLIC_PATH, 'utf8');
  });

  it('passes PEM contents to KeyManager constructor', () => {
    buildRaftSigner({ nodeId: NODE_ID, keyManagerSecretsDir: SIGNER_DIR });
    expect(mockKeyManagerConstructor).toHaveBeenCalledWith(
      expect.objectContaining({
        privateKeyPem: '---PRIVATE PEM---',
        publicKeyPem: '---PUBLIC PEM---',
      }),
    );
  });

  it('does NOT write new key files when PEMs already exist', () => {
    buildRaftSigner({ nodeId: NODE_ID, keyManagerSecretsDir: SIGNER_DIR });
    expect(mockWriteFileSync).not.toHaveBeenCalled();
  });

  it('returns a signer object (not undefined)', () => {
    const result = buildRaftSigner({ nodeId: NODE_ID, keyManagerSecretsDir: SIGNER_DIR });
    expect(result).not.toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// PEM files absent — mints fresh key pair + persists
// ---------------------------------------------------------------------------

describe('buildRaftSigner — fresh key pair', () => {
  beforeEach(() => {
    // Dir exists and is accessible; PEM files do NOT exist
    mockExistsSync.mockImplementation((p: string) => p === path.resolve(SIGNER_DIR));
    mockAccessSync.mockReturnValue(undefined);
  });

  it('constructs KeyManager with empty options (fresh pair)', () => {
    buildRaftSigner({ nodeId: NODE_ID, keyManagerSecretsDir: SIGNER_DIR });
    expect(mockKeyManagerConstructor).toHaveBeenCalledWith({});
  });

  it('writes private key to <nodeId>.private.pem', () => {
    buildRaftSigner({ nodeId: NODE_ID, keyManagerSecretsDir: SIGNER_DIR });
    expect(mockWriteFileSync).toHaveBeenCalledWith(
      PRIVATE_PATH,
      '---PRIVATE---',
      expect.objectContaining({ mode: 0o600 }),
    );
  });

  it('writes public key to <nodeId>.public.pem', () => {
    buildRaftSigner({ nodeId: NODE_ID, keyManagerSecretsDir: SIGNER_DIR });
    expect(mockWriteFileSync).toHaveBeenCalledWith(
      PUBLIC_PATH,
      '---PUBLIC---',
      expect.objectContaining({ mode: 0o644 }),
    );
  });

  it('throws if persisting keys fails', () => {
    mockWriteFileSync.mockImplementation(() => { throw new Error('ENOSPC: no space left'); });
    expect(() =>
      buildRaftSigner({ nodeId: NODE_ID, keyManagerSecretsDir: SIGNER_DIR }),
    ).toThrow('ENOSPC');
  });
});
