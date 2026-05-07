// social-api/src/ingest/adapters/__tests__/s3.test.ts
//
// Unit tests for S3Adapter — mocks @aws-sdk/client-s3 so no real AWS calls.

import { S3Adapter } from '../s3';
import { S3Client, ListObjectsV2Command, GetObjectCommand } from '@aws-sdk/client-s3';
import { Readable } from 'stream';
import type { SourceConfig, SyncCursor } from '../../types';

jest.mock('@aws-sdk/client-s3', () => {
  const actual = jest.requireActual('@aws-sdk/client-s3');
  return {
    ...actual,
    S3Client: jest.fn(),
  };
});

const mockSend = jest.fn();
const MockS3Client = S3Client as jest.MockedClass<typeof S3Client>;

beforeEach(() => {
  mockSend.mockReset();
  MockS3Client.mockImplementation(() => ({ send: mockSend } as unknown as S3Client));
});

function makeConfig(overrides: Partial<SourceConfig> = {}): SourceConfig {
  return { bucket: 'my-bucket', region: 'us-east-1', ...overrides };
}

function makeCursor(overrides: Partial<SyncCursor> = {}): SyncCursor {
  return { lastSyncAt: null, ...overrides };
}

function makeReadable(content: string): Readable {
  return Readable.from([Buffer.from(content)]);
}

describe('S3Adapter.list', () => {
  test('yields objects from a single page', async () => {
    mockSend.mockResolvedValueOnce({
      Contents: [
        {
          Key: 'reports/q1.csv',
          ETag: '"abc123"',
          Size: 1024,
          LastModified: new Date('2026-05-01T00:00:00Z'),
        },
        {
          Key: 'data/notes.txt',
          ETag: '"def456"',
          Size: 512,
          LastModified: new Date('2026-05-02T00:00:00Z'),
        },
      ],
      IsTruncated: false,
    });

    const adapter = new S3Adapter();
    const results = [];
    for await (const file of adapter.list(makeConfig(), makeCursor())) {
      results.push(file);
    }

    expect(results).toHaveLength(2);
    expect(results[0].path).toBe('reports/q1.csv');
    expect(results[0].name).toBe('q1.csv');
    expect(results[0].mimeType).toBe('text/csv');
    expect(results[0].hash).toBe('abc123'); // ETag with quotes stripped
    expect(results[0].size).toBe(1024);

    expect(results[1].path).toBe('data/notes.txt');
    expect(results[1].hash).toBe('def456');
  });

  test('paginates using continuation token', async () => {
    mockSend
      .mockResolvedValueOnce({
        Contents: [{ Key: 'a.json', ETag: '"h1"', Size: 10, LastModified: new Date() }],
        IsTruncated: true,
        NextContinuationToken: 'token-1',
      })
      .mockResolvedValueOnce({
        Contents: [{ Key: 'b.json', ETag: '"h2"', Size: 20, LastModified: new Date() }],
        IsTruncated: false,
      });

    const adapter = new S3Adapter();
    const results = [];
    for await (const file of adapter.list(makeConfig(), makeCursor())) {
      results.push(file);
    }

    expect(results).toHaveLength(2);
    expect(mockSend).toHaveBeenCalledTimes(2);
    // Second call should include the continuation token
    const secondCall = mockSend.mock.calls[1][0];
    expect(secondCall.input.ContinuationToken).toBe('token-1');
  });

  test('skips folder markers (keys ending with /)', async () => {
    mockSend.mockResolvedValueOnce({
      Contents: [
        { Key: 'prefix/', ETag: '"h0"', Size: 0, LastModified: new Date() },
        { Key: 'prefix/file.txt', ETag: '"h1"', Size: 42, LastModified: new Date() },
      ],
      IsTruncated: false,
    });

    const adapter = new S3Adapter();
    const results = [];
    for await (const file of adapter.list(makeConfig(), makeCursor())) {
      results.push(file);
    }

    expect(results).toHaveLength(1);
    expect(results[0].path).toBe('prefix/file.txt');
  });

  test('filters out objects not modified since lastSyncAt', async () => {
    const now = '2026-05-05T00:00:00Z';
    mockSend.mockResolvedValueOnce({
      Contents: [
        { Key: 'old.txt', ETag: '"h1"', Size: 10, LastModified: new Date('2026-05-01T00:00:00Z') },
        { Key: 'new.txt', ETag: '"h2"', Size: 20, LastModified: new Date('2026-05-06T00:00:00Z') },
      ],
      IsTruncated: false,
    });

    const adapter = new S3Adapter();
    const results = [];
    for await (const file of adapter.list(makeConfig(), makeCursor({ lastSyncAt: now }))) {
      results.push(file);
    }

    expect(results).toHaveLength(1);
    expect(results[0].path).toBe('new.txt');
  });

  test('passes prefix to ListObjectsV2', async () => {
    mockSend.mockResolvedValueOnce({ Contents: [], IsTruncated: false });

    const adapter = new S3Adapter();
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    for await (const _ of adapter.list(makeConfig({ prefix: 'data/' }), makeCursor())) { /* drain */ }

    const call = mockSend.mock.calls[0][0];
    expect(call.input.Prefix).toBe('data/');
  });

  test('resumes listing from cursor.checkpoint', async () => {
    mockSend.mockResolvedValueOnce({ Contents: [], IsTruncated: false });

    const adapter = new S3Adapter();
    for await (const _ of adapter.list(makeConfig(), makeCursor({ checkpoint: 'resume-token' }))) { /* drain */ }

    const call = mockSend.mock.calls[0][0];
    expect(call.input.ContinuationToken).toBe('resume-token');
  });
});

describe('S3Adapter.fetch', () => {
  test('returns buffer and utf-8 encoding for text files', async () => {
    mockSend.mockResolvedValueOnce({ Body: makeReadable('hello world') });

    const adapter = new S3Adapter();
    const content = await adapter.fetch(makeConfig(), {
      path: 'notes.txt',
      name: 'notes.txt',
      hash: 'h1',
      size: 11,
      mimeType: 'text/plain',
      lastModified: new Date().toISOString(),
    });

    expect(content.encoding).toBe('utf-8');
    expect(content.data.toString('utf-8')).toBe('hello world');
  });

  test('parses JSON files into content.parsed', async () => {
    const payload = JSON.stringify({ key: 'value' });
    mockSend.mockResolvedValueOnce({ Body: makeReadable(payload) });

    const adapter = new S3Adapter();
    const content = await adapter.fetch(makeConfig(), {
      path: 'data.json',
      name: 'data.json',
      hash: 'h2',
      size: payload.length,
      mimeType: 'application/json',
      lastModified: new Date().toISOString(),
    });

    expect(content.parsed).toEqual({ key: 'value' });
  });

  test('returns binary encoding for non-text files', async () => {
    mockSend.mockResolvedValueOnce({ Body: makeReadable('\x00\x01\x02') });

    const adapter = new S3Adapter();
    const content = await adapter.fetch(makeConfig(), {
      path: 'image.png',
      name: 'image.png',
      hash: 'h3',
      size: 3,
      mimeType: 'image/png',
      lastModified: new Date().toISOString(),
    });

    expect(content.encoding).toBe('binary');
  });

  test('throws when S3 returns empty body', async () => {
    mockSend.mockResolvedValueOnce({ Body: null });

    const adapter = new S3Adapter();
    await expect(
      adapter.fetch(makeConfig(), {
        path: 'missing.txt',
        name: 'missing.txt',
        hash: '',
        size: 0,
        mimeType: 'text/plain',
        lastModified: new Date().toISOString(),
      }),
    ).rejects.toThrow('empty body for key missing.txt');
  });

  test('uses custom endpoint for localstack config', async () => {
    mockSend.mockResolvedValueOnce({ Contents: [], IsTruncated: false });

    const adapter = new S3Adapter();
    for await (const _ of adapter.list(
      makeConfig({ endpoint: 'http://localhost:4566', forcePathStyle: true }),
      makeCursor(),
    )) { /* drain */ }

    expect(MockS3Client).toHaveBeenCalledWith(
      expect.objectContaining({
        endpoint: 'http://localhost:4566',
        forcePathStyle: true,
      }),
    );
  });
});

describe('S3Adapter — GetObjectCommand is called with correct bucket+key', () => {
  test('sends GetObjectCommand with Bucket and Key', async () => {
    let capturedInput: { Bucket?: string; Key?: string } = {};
    mockSend.mockImplementation((cmd: GetObjectCommand) => {
      capturedInput = cmd.input as { Bucket?: string; Key?: string };
      return Promise.resolve({ Body: makeReadable('hi') });
    });

    const adapter = new S3Adapter();
    await adapter.fetch(makeConfig({ bucket: 'test-bucket' }), {
      path: 'folder/file.txt',
      name: 'file.txt',
      hash: 'h1',
      size: 2,
      mimeType: 'text/plain',
      lastModified: new Date().toISOString(),
    });

    expect(capturedInput.Bucket).toBe('test-bucket');
    expect(capturedInput.Key).toBe('folder/file.txt');
  });
});
