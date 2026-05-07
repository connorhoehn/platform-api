// social-api/src/ingest/adapters/__tests__/google-drive-writeback.test.ts
//
// Integration-style tests for GoogleDriveWriteBackDriver — mocks the
// googleapis Drive client so no real API calls happen. Covers:
//   - happy push (revision matches → files.update succeeds)
//   - version conflict (local baseRevision != remote headRevisionId)
//   - missing-scope error (403 with insufficientFilePermissions reason)
//   - workspace-file refusal (no headRevisionId)

import { GoogleDriveWriteBackDriver } from '../google-drive-writeback';
import type { WriteBackChange } from '../writeback-types';
import type { SourceConfig } from '../../types';

const mockFilesGet = jest.fn();
const mockFilesUpdate = jest.fn();

jest.mock('googleapis', () => ({
  google: {
    auth: {
      OAuth2: jest.fn().mockImplementation(() => ({
        setCredentials: jest.fn(),
      })),
    },
    drive: jest.fn().mockImplementation(() => ({
      files: {
        get: mockFilesGet,
        update: mockFilesUpdate,
      },
    })),
  },
}));

function makeConfig(overrides: Partial<SourceConfig> = {}): SourceConfig {
  return {
    clientId: 'client-id',
    clientSecret: 'client-secret',
    refreshToken: 'refresh-token',
    ...overrides,
  };
}

function makeChange(overrides: Partial<WriteBackChange> = {}): WriteBackChange {
  return {
    documentId: 'doc-canonical-1',
    sourcePath: 'drive-file-id-1',
    content: Buffer.from('hello world'),
    mimeType: 'text/plain',
    baseRevision: 'rev-1',
    ...overrides,
  };
}

beforeEach(() => {
  mockFilesGet.mockReset();
  mockFilesUpdate.mockReset();
});

describe('GoogleDriveWriteBackDriver.push — happy path', () => {
  test('updates the file when local baseRevision matches remote headRevisionId', async () => {
    mockFilesGet.mockResolvedValueOnce({
      data: { headRevisionId: 'rev-1', mimeType: 'text/plain' },
    });
    mockFilesUpdate.mockResolvedValueOnce({
      data: { id: 'drive-file-id-1', headRevisionId: 'rev-2' },
    });

    const driver = new GoogleDriveWriteBackDriver();
    const result = await driver.push(makeConfig(), makeChange());

    expect(result.status).toBe('ok');
    if (result.status === 'ok') {
      expect(result.newRevision).toBe('rev-2');
      expect(result.sourcePath).toBe('drive-file-id-1');
    }

    // Verify files.update was called with the content stream + mimeType.
    expect(mockFilesUpdate).toHaveBeenCalledTimes(1);
    const updateArgs = mockFilesUpdate.mock.calls[0][0];
    expect(updateArgs.fileId).toBe('drive-file-id-1');
    expect(updateArgs.media.mimeType).toBe('text/plain');
    expect(updateArgs.fields).toContain('headRevisionId');
  });

  test('skips conflict check when baseRevision is null (first write)', async () => {
    mockFilesGet.mockResolvedValueOnce({
      data: { headRevisionId: 'rev-anything', mimeType: 'text/plain' },
    });
    mockFilesUpdate.mockResolvedValueOnce({
      data: { id: 'drive-file-id-1', headRevisionId: 'rev-new' },
    });

    const driver = new GoogleDriveWriteBackDriver();
    const result = await driver.push(makeConfig(), makeChange({ baseRevision: null }));

    expect(result.status).toBe('ok');
  });
});

describe('GoogleDriveWriteBackDriver.push — version conflict', () => {
  test('returns conflict when remote headRevisionId has moved past baseRevision', async () => {
    mockFilesGet
      .mockResolvedValueOnce({
        data: { headRevisionId: 'rev-7', mimeType: 'text/plain' },
      })
      .mockResolvedValueOnce({
        data: Buffer.from('newer remote content'),
      });

    const driver = new GoogleDriveWriteBackDriver();
    const result = await driver.push(
      makeConfig(),
      makeChange({ baseRevision: 'rev-3' }),
    );

    expect(result.status).toBe('conflict');
    if (result.status === 'conflict') {
      expect(result.expectedRevision).toBe('rev-3');
      expect(result.remoteRevision).toBe('rev-7');
      expect(result.remoteContent?.toString('utf-8')).toBe('newer remote content');
    }

    // files.update must NOT be called when there's a conflict.
    expect(mockFilesUpdate).not.toHaveBeenCalled();
  });

  test('still returns conflict even if remote content fetch fails', async () => {
    mockFilesGet
      .mockResolvedValueOnce({
        data: { headRevisionId: 'rev-9', mimeType: 'text/plain' },
      })
      .mockRejectedValueOnce(new Error('network blip'));

    const driver = new GoogleDriveWriteBackDriver();
    const result = await driver.push(
      makeConfig(),
      makeChange({ baseRevision: 'rev-3' }),
    );

    expect(result.status).toBe('conflict');
    if (result.status === 'conflict') {
      expect(result.remoteRevision).toBe('rev-9');
      expect(result.remoteContent).toBeUndefined();
    }
  });
});

describe('GoogleDriveWriteBackDriver.push — missing scope', () => {
  test('classifies 403 insufficientFilePermissions as missing_scope error', async () => {
    mockFilesGet.mockResolvedValueOnce({
      data: { headRevisionId: 'rev-1', mimeType: 'text/plain' },
    });
    const apiErr: any = new Error('Insufficient Permission');
    apiErr.code = 403;
    apiErr.errors = [{ reason: 'insufficientFilePermissions', message: 'no write' }];
    mockFilesUpdate.mockRejectedValueOnce(apiErr);

    const driver = new GoogleDriveWriteBackDriver();
    const result = await driver.push(makeConfig(), makeChange());

    expect(result.status).toBe('error');
    if (result.status === 'error') {
      expect(result.code).toBe('missing_scope');
      expect(result.retriable).toBe(false);
      expect(result.message).toContain('drive');
    }
  });

  test('classifies 403 insufficientPermissions reason via response.data.error', async () => {
    mockFilesGet.mockResolvedValueOnce({
      data: { headRevisionId: 'rev-1', mimeType: 'text/plain' },
    });
    const apiErr: any = new Error('Forbidden');
    apiErr.response = {
      status: 403,
      data: {
        error: {
          errors: [{ reason: 'insufficientPermissions' }],
          message: 'forbidden',
        },
      },
    };
    mockFilesUpdate.mockRejectedValueOnce(apiErr);

    const driver = new GoogleDriveWriteBackDriver();
    const result = await driver.push(makeConfig(), makeChange());

    expect(result.status).toBe('error');
    if (result.status === 'error') {
      expect(result.code).toBe('missing_scope');
    }
  });

  test('classifies 401 as auth_failed (not missing_scope)', async () => {
    const apiErr: any = new Error('Unauthorized');
    apiErr.code = 401;
    mockFilesGet.mockRejectedValueOnce(apiErr);

    const driver = new GoogleDriveWriteBackDriver();
    const result = await driver.push(makeConfig(), makeChange());

    expect(result.status).toBe('error');
    if (result.status === 'error') {
      expect(result.code).toBe('auth_failed');
    }
  });

  test('classifies 429 as rate_limited and retriable', async () => {
    const apiErr: any = new Error('Too Many Requests');
    apiErr.code = 429;
    mockFilesGet.mockRejectedValueOnce(apiErr);

    const driver = new GoogleDriveWriteBackDriver();
    const result = await driver.push(makeConfig(), makeChange());

    expect(result.status).toBe('error');
    if (result.status === 'error') {
      expect(result.code).toBe('rate_limited');
      expect(result.retriable).toBe(true);
    }
  });
});

describe('GoogleDriveWriteBackDriver.push — workspace file refusal', () => {
  test('refuses files without a headRevisionId (Google Docs etc.)', async () => {
    mockFilesGet.mockResolvedValueOnce({
      data: {
        headRevisionId: null,
        mimeType: 'application/vnd.google-apps.document',
      },
    });

    const driver = new GoogleDriveWriteBackDriver();
    const result = await driver.push(makeConfig(), makeChange());

    expect(result.status).toBe('error');
    if (result.status === 'error') {
      expect(result.code).toBe('unknown');
      expect(result.message).toMatch(/Workspace|headRevisionId/i);
    }
    expect(mockFilesUpdate).not.toHaveBeenCalled();
  });
});

describe('GoogleDriveWriteBackDriver.resolveConflict', () => {
  test('default v1 behavior is keep-local with rebased baseRevision', async () => {
    const driver = new GoogleDriveWriteBackDriver();
    const local = makeChange({ baseRevision: 'rev-3' });
    const remote = {
      revision: 'rev-7',
      content: Buffer.from('remote content'),
      mimeType: 'text/plain',
    };

    const out = await driver.resolveConflict(local, remote);

    expect(out.decision).toBe('keep-local');
    expect(out.resolved.baseRevision).toBe('rev-7');
    expect(out.resolved.content.toString('utf-8')).toBe('hello world');
  });
});
