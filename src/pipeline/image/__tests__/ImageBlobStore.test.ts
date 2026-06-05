// platform-api/src/pipeline/image/__tests__/ImageBlobStore.test.ts
//
// Unit tests for DdbImageBlobStore + InMemoryImageBlobStore. Same
// shape as RawTextStore.test.ts.

import {
  DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
} from '@aws-sdk/lib-dynamodb';

const mockSend = jest.fn();
const mockDocClient = { send: mockSend } as unknown as DynamoDBDocumentClient;

jest.mock('../../../lib/ddb-table-name', () => ({
  tableName: (name: string) => `test-${name}`,
}));
jest.mock('../../../lib/aws-clients', () => ({
  docClient: { send: jest.fn() },
}));

import {
  DdbImageBlobStore,
  InMemoryImageBlobStore,
  IMAGE_BLOB_SIZE_CAP_BYTES,
} from '../ImageBlobStore';
import type { ImageMeta } from '../types';

const META: ImageMeta = {
  mimeType: 'image/jpeg',
  sizeBytes: 1000,
  width: 100,
  height: 80,
  uploadedAt: '2026-06-04T10:00:00.000Z',
  uploadedBy: 'user-1',
};

const BODY = Buffer.from('fake jpeg bytes');

beforeEach(() => {
  mockSend.mockReset();
  mockSend.mockResolvedValue({});
});

// ---------------------------------------------------------------------------
// InMemoryImageBlobStore
// ---------------------------------------------------------------------------

describe('InMemoryImageBlobStore', () => {
  it('round-trips put then get', async () => {
    const store = new InMemoryImageBlobStore();
    await store.put('img_1', BODY, META);
    const row = await store.get('img_1');
    expect(row?.body.toString()).toBe('fake jpeg bytes');
    expect(row?.meta.mimeType).toBe('image/jpeg');
  });

  it('returns null when the imageId was never put', async () => {
    const store = new InMemoryImageBlobStore();
    expect(await store.get('img_missing')).toBeNull();
  });

  it('getMetadata returns just the meta, not the body', async () => {
    const store = new InMemoryImageBlobStore();
    await store.put('img_1', BODY, META);
    const meta = await store.getMetadata('img_1');
    expect(meta?.mimeType).toBe('image/jpeg');
    expect(meta?.width).toBe(100);
  });

  it('throws when body exceeds the size cap', async () => {
    const store = new InMemoryImageBlobStore();
    const huge = Buffer.alloc(IMAGE_BLOB_SIZE_CAP_BYTES + 1);
    await expect(store.put('img_big', huge, META)).rejects.toThrow(/exceeds inline cap/);
  });

  it('overwrites on repeat put for the same imageId', async () => {
    const store = new InMemoryImageBlobStore();
    await store.put('img_1', BODY, META);
    await store.put('img_1', Buffer.from('newer'), { ...META, sizeBytes: 5 });
    expect((await store.get('img_1'))?.body.toString()).toBe('newer');
  });

  it('reset() drops all stored rows', async () => {
    const store = new InMemoryImageBlobStore();
    await store.put('img_1', BODY, META);
    store.reset();
    expect(await store.get('img_1')).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// DdbImageBlobStore
// ---------------------------------------------------------------------------

describe('DdbImageBlobStore', () => {
  it('put issues a PutCommand to the pipeline-images table', async () => {
    const store = new DdbImageBlobStore({ docClient: mockDocClient, now: () => 1_700_000_000_000 });
    await store.put('img_1', BODY, META);

    const cmd = mockSend.mock.calls[0][0] as PutCommand;
    expect(cmd).toBeInstanceOf(PutCommand);
    expect(cmd.input.TableName).toBe('test-pipeline-images');
    const item = cmd.input.Item as Record<string, unknown>;
    expect(item.imageId).toBe('img_1');
    expect(item.mimeType).toBe('image/jpeg');
    expect(item.width).toBe(100);
    expect(Buffer.isBuffer(item.body)).toBe(true);
  });

  it('put sets ttlEpoch derived from now + ttlDays', async () => {
    const fixedNow = 1_700_000_000_000;
    const store = new DdbImageBlobStore({
      docClient: mockDocClient,
      now: () => fixedNow,
      ttlDays: 7,
    });
    await store.put('img_1', BODY, META);
    const item = (mockSend.mock.calls[0][0] as PutCommand).input.Item as { ttlEpoch: number };
    expect(item.ttlEpoch).toBe(Math.floor(fixedNow / 1000) + 7 * 24 * 60 * 60);
  });

  it('put throws when body exceeds the size cap', async () => {
    const store = new DdbImageBlobStore({ docClient: mockDocClient });
    const huge = Buffer.alloc(IMAGE_BLOB_SIZE_CAP_BYTES + 1);
    await expect(store.put('img_big', huge, META)).rejects.toThrow(/exceeds DDB inline cap/);
    expect(mockSend).not.toHaveBeenCalled();
  });

  it('get issues a GetCommand keyed by imageId + returns body+meta', async () => {
    mockSend.mockResolvedValueOnce({
      Item: {
        imageId: 'img_1',
        body: BODY,
        mimeType: 'image/png',
        sizeBytes: BODY.byteLength,
        width: 50,
        height: 50,
        uploadedAt: '2026-06-04T10:00:00.000Z',
      },
    });
    const store = new DdbImageBlobStore({ docClient: mockDocClient });
    const row = await store.get('img_1');

    expect(row?.body.toString()).toBe('fake jpeg bytes');
    expect(row?.meta.mimeType).toBe('image/png');
    expect(row?.meta.width).toBe(50);
    const cmd = mockSend.mock.calls[0][0] as GetCommand;
    expect(cmd).toBeInstanceOf(GetCommand);
    expect(cmd.input.Key).toEqual({ imageId: 'img_1' });
  });

  it('get normalizes Uint8Array body to Buffer', async () => {
    const uint = new Uint8Array([1, 2, 3, 4]);
    mockSend.mockResolvedValueOnce({
      Item: { imageId: 'img_1', body: uint, mimeType: 'image/jpeg', sizeBytes: 4, width: 1, height: 1, uploadedAt: 't' },
    });
    const store = new DdbImageBlobStore({ docClient: mockDocClient });
    const row = await store.get('img_1');
    expect(Buffer.isBuffer(row?.body)).toBe(true);
    expect(row?.body[0]).toBe(1);
  });

  it('get returns null when the item is missing', async () => {
    mockSend.mockResolvedValueOnce({});
    const store = new DdbImageBlobStore({ docClient: mockDocClient });
    expect(await store.get('img_missing')).toBeNull();
  });

  it('get returns null when DDB throws (table missing, network)', async () => {
    mockSend.mockRejectedValueOnce(new Error('ResourceNotFoundException'));
    const store = new DdbImageBlobStore({ docClient: mockDocClient });
    expect(await store.get('img_1')).toBeNull();
  });

  it('getMetadata issues a projected GetCommand that does NOT request the body attribute', async () => {
    mockSend.mockResolvedValueOnce({
      Item: {
        imageId: 'img_1',
        mimeType: 'image/jpeg',
        sizeBytes: 1000,
        width: 100,
        height: 80,
        uploadedAt: '2026-06-04T10:00:00.000Z',
      },
    });
    const store = new DdbImageBlobStore({ docClient: mockDocClient });
    const meta = await store.getMetadata('img_1');

    expect(meta?.mimeType).toBe('image/jpeg');
    expect(meta?.width).toBe(100);
    const cmd = mockSend.mock.calls[0][0] as GetCommand;
    expect(cmd).toBeInstanceOf(GetCommand);
    // ProjectionExpression must be set and must NOT include `body`
    expect(cmd.input.ProjectionExpression).toBeDefined();
    expect(cmd.input.ProjectionExpression).not.toContain('body');
    expect(cmd.input.ProjectionExpression).toContain('mimeType');
  });

  it('getMetadata returns null when DDB returns no Item', async () => {
    mockSend.mockResolvedValueOnce({});
    const store = new DdbImageBlobStore({ docClient: mockDocClient });
    expect(await store.getMetadata('img_missing')).toBeNull();
  });

  it('getMetadata returns null when DDB throws', async () => {
    mockSend.mockRejectedValueOnce(new Error('ConnectionError'));
    const store = new DdbImageBlobStore({ docClient: mockDocClient });
    expect(await store.getMetadata('img_1')).toBeNull();
  });
});
