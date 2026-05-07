// social-api/src/ingest/adapters/__tests__/google-drive.test.ts
//
// Unit tests for GoogleDriveAdapter — mocks googleapis so no real OAuth calls.

import { GoogleDriveAdapter } from '../google-drive';
import type { SourceConfig, SyncCursor } from '../../types';

// Mock the entire googleapis module
const mockFilesList = jest.fn();
const mockFilesGet = jest.fn();
const mockFilesExport = jest.fn();

jest.mock('googleapis', () => ({
  google: {
    auth: {
      OAuth2: jest.fn().mockImplementation(() => ({
        setCredentials: jest.fn(),
      })),
    },
    drive: jest.fn().mockImplementation(() => ({
      files: {
        list: mockFilesList,
        get: mockFilesGet,
        export: mockFilesExport,
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

function makeCursor(overrides: Partial<SyncCursor> = {}): SyncCursor {
  return { lastSyncAt: null, ...overrides };
}

beforeEach(() => {
  mockFilesList.mockReset();
  mockFilesGet.mockReset();
  mockFilesExport.mockReset();
});

describe('GoogleDriveAdapter.list', () => {
  test('yields file descriptors from a single page', async () => {
    mockFilesList.mockResolvedValueOnce({
      data: {
        files: [
          {
            id: 'file-id-1',
            name: 'report.pdf',
            mimeType: 'application/pdf',
            size: '2048',
            md5Checksum: 'abc123',
            modifiedTime: '2026-05-01T10:00:00Z',
          },
          {
            id: 'file-id-2',
            name: 'notes.txt',
            mimeType: 'text/plain',
            size: '512',
            md5Checksum: 'def456',
            modifiedTime: '2026-05-02T10:00:00Z',
          },
        ],
        nextPageToken: null,
      },
    });

    const adapter = new GoogleDriveAdapter();
    const results = [];
    for await (const file of adapter.list(makeConfig(), makeCursor())) {
      results.push(file);
    }

    expect(results).toHaveLength(2);
    expect(results[0].path).toBe('file-id-1');
    expect(results[0].name).toBe('report.pdf');
    expect(results[0].hash).toBe('abc123');
    expect(results[0].size).toBe(2048);
    expect(results[0].mimeType).toBe('application/pdf');

    expect(results[1].path).toBe('file-id-2');
    expect(results[1].hash).toBe('def456');
  });

  test('paginates via nextPageToken', async () => {
    mockFilesList
      .mockResolvedValueOnce({
        data: {
          files: [{ id: 'f1', name: 'a.txt', mimeType: 'text/plain', size: '10', md5Checksum: 'h1', modifiedTime: '2026-05-01T00:00:00Z' }],
          nextPageToken: 'page-2',
        },
      })
      .mockResolvedValueOnce({
        data: {
          files: [{ id: 'f2', name: 'b.txt', mimeType: 'text/plain', size: '20', md5Checksum: 'h2', modifiedTime: '2026-05-02T00:00:00Z' }],
          nextPageToken: null,
        },
      });

    const adapter = new GoogleDriveAdapter();
    const results = [];
    for await (const file of adapter.list(makeConfig(), makeCursor())) {
      results.push(file);
    }

    expect(results).toHaveLength(2);
    expect(mockFilesList).toHaveBeenCalledTimes(2);
    // Second call should have pageToken
    expect(mockFilesList.mock.calls[1][0]).toMatchObject({ pageToken: 'page-2' });
  });

  test('exports Google Docs as plain text mime', async () => {
    mockFilesList.mockResolvedValueOnce({
      data: {
        files: [
          {
            id: 'doc-id',
            name: 'Meeting Notes',
            mimeType: 'application/vnd.google-apps.document',
            size: '0',
            md5Checksum: null,
            modifiedTime: '2026-05-01T00:00:00Z',
          },
        ],
        nextPageToken: null,
      },
    });

    const adapter = new GoogleDriveAdapter();
    const results = [];
    for await (const file of adapter.list(makeConfig(), makeCursor())) {
      results.push(file);
    }

    expect(results[0].mimeType).toBe('text/plain');
    expect(results[0].metadata?.driveMimeType).toBe('application/vnd.google-apps.document');
    // No md5Checksum — hash is derived from modifiedTime
    expect(results[0].hash).toContain('modtime:');
  });

  test('exports Google Sheets as CSV mime', async () => {
    mockFilesList.mockResolvedValueOnce({
      data: {
        files: [
          {
            id: 'sheet-id',
            name: 'Budget',
            mimeType: 'application/vnd.google-apps.spreadsheet',
            size: '0',
            md5Checksum: null,
            modifiedTime: '2026-05-01T00:00:00Z',
          },
        ],
        nextPageToken: null,
      },
    });

    const adapter = new GoogleDriveAdapter();
    const results = [];
    for await (const file of adapter.list(makeConfig(), makeCursor())) {
      results.push(file);
    }

    expect(results[0].mimeType).toBe('text/csv');
    expect(results[0].metadata?.driveMimeType).toBe('application/vnd.google-apps.spreadsheet');
  });

  test('appends modifiedTime filter when cursor.lastSyncAt is set', async () => {
    mockFilesList.mockResolvedValueOnce({ data: { files: [], nextPageToken: null } });

    const adapter = new GoogleDriveAdapter();
    for await (const _ of adapter.list(makeConfig(), makeCursor({ lastSyncAt: '2026-05-01T00:00:00Z' }))) { /* drain */ }

    const q = mockFilesList.mock.calls[0][0].q as string;
    expect(q).toContain("modifiedTime > '2026-05-01T00:00:00Z'");
  });

  test('appends folderId constraint when config.folderId is set', async () => {
    mockFilesList.mockResolvedValueOnce({ data: { files: [], nextPageToken: null } });

    const adapter = new GoogleDriveAdapter();
    for await (const _ of adapter.list(makeConfig({ folderId: 'folder-abc' }), makeCursor())) { /* drain */ }

    const q = mockFilesList.mock.calls[0][0].q as string;
    expect(q).toContain("'folder-abc' in parents");
  });

  test('resumes from cursor.checkpoint pageToken', async () => {
    mockFilesList.mockResolvedValueOnce({ data: { files: [], nextPageToken: null } });

    const adapter = new GoogleDriveAdapter();
    for await (const _ of adapter.list(makeConfig(), makeCursor({ checkpoint: 'resume-page' }))) { /* drain */ }

    expect(mockFilesList.mock.calls[0][0].pageToken).toBe('resume-page');
  });
});

describe('GoogleDriveAdapter.fetch', () => {
  test('exports Google Docs file via files.export', async () => {
    const docContent = 'Meeting transcript text';
    mockFilesExport.mockResolvedValueOnce({ data: Buffer.from(docContent) });

    const adapter = new GoogleDriveAdapter();
    const content = await adapter.fetch(makeConfig(), {
      path: 'doc-id',
      name: 'Meeting Notes',
      hash: 'modtime:...',
      size: 0,
      mimeType: 'text/plain',
      lastModified: '2026-05-01T00:00:00Z',
      metadata: { driveId: 'doc-id', driveMimeType: 'application/vnd.google-apps.document' },
    });

    expect(mockFilesExport).toHaveBeenCalledWith(
      { fileId: 'doc-id', mimeType: 'text/plain' },
      { responseType: 'arraybuffer' },
    );
    expect(content.encoding).toBe('utf-8');
    expect(content.data.toString('utf-8')).toBe(docContent);
  });

  test('downloads binary files via files.get', async () => {
    const pdfBytes = Buffer.from('%PDF-1.4');
    mockFilesGet.mockResolvedValueOnce({ data: pdfBytes });

    const adapter = new GoogleDriveAdapter();
    const content = await adapter.fetch(makeConfig(), {
      path: 'pdf-id',
      name: 'report.pdf',
      hash: 'abc123',
      size: 8,
      mimeType: 'application/pdf',
      lastModified: '2026-05-01T00:00:00Z',
      metadata: { driveId: 'pdf-id', driveMimeType: 'application/pdf' },
    });

    expect(mockFilesGet).toHaveBeenCalledWith(
      { fileId: 'pdf-id', alt: 'media' },
      { responseType: 'arraybuffer' },
    );
    expect(content.encoding).toBe('binary');
  });

  test('parses JSON content into content.parsed', async () => {
    const json = JSON.stringify({ answer: 42 });
    mockFilesGet.mockResolvedValueOnce({ data: Buffer.from(json) });

    const adapter = new GoogleDriveAdapter();
    const content = await adapter.fetch(makeConfig(), {
      path: 'json-id',
      name: 'data.json',
      hash: 'abc',
      size: json.length,
      mimeType: 'application/json',
      lastModified: '2026-05-01T00:00:00Z',
      metadata: { driveId: 'json-id', driveMimeType: 'application/json' },
    });

    expect(content.parsed).toEqual({ answer: 42 });
  });

  test('falls back to path as driveId when metadata is absent', async () => {
    mockFilesGet.mockResolvedValueOnce({ data: Buffer.from('text') });

    const adapter = new GoogleDriveAdapter();
    await adapter.fetch(makeConfig(), {
      path: 'fallback-id',
      name: 'file.txt',
      hash: 'h',
      size: 4,
      mimeType: 'text/plain',
      lastModified: '2026-05-01T00:00:00Z',
    });

    expect(mockFilesGet).toHaveBeenCalledWith(
      { fileId: 'fallback-id', alt: 'media' },
      { responseType: 'arraybuffer' },
    );
  });
});
