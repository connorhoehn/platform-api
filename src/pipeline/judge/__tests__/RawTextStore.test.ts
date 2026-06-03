// platform-api/src/pipeline/judge/__tests__/RawTextStore.test.ts
//
// Tests for InMemoryRawTextStore (round-trip, key shape) and
// S3RawTextStore (PutObjectCommand shape, GetObjectCommand shape,
// graceful return-null on get errors).

import {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
} from '@aws-sdk/client-s3';

import {
  InMemoryRawTextStore,
  S3RawTextStore,
  rawTextKeyFor,
} from '../RawTextStore';

describe('rawTextKeyFor', () => {
  it('namespaces under judgments/ with .txt extension', () => {
    expect(rawTextKeyFor('jdg_01H1234ABCDE')).toBe('judgments/jdg_01H1234ABCDE.txt');
  });
});

describe('InMemoryRawTextStore', () => {
  it('round-trips put then get under the canonical key', async () => {
    const store = new InMemoryRawTextStore();
    const key = await store.put('jdg_1', 'hello world');
    expect(key).toBe('judgments/jdg_1.txt');
    expect(await store.get(key)).toBe('hello world');
  });

  it('returns null when the key was never put', async () => {
    const store = new InMemoryRawTextStore();
    expect(await store.get('judgments/missing.txt')).toBeNull();
  });

  it('overwrites on repeat put for the same judgmentId', async () => {
    const store = new InMemoryRawTextStore();
    await store.put('jdg_1', 'v1');
    await store.put('jdg_1', 'v2');
    expect(await store.get(rawTextKeyFor('jdg_1'))).toBe('v2');
  });

  it('reset() drops all stored bodies', async () => {
    const store = new InMemoryRawTextStore();
    await store.put('jdg_1', 'x');
    store.reset();
    expect(await store.get(rawTextKeyFor('jdg_1'))).toBeNull();
  });
});

describe('S3RawTextStore', () => {
  it('put sends a PutObjectCommand with the canonical key + UTF-8 content-type', async () => {
    const send = jest.fn().mockResolvedValue({});
    const client = { send } as unknown as S3Client;
    const store = new S3RawTextStore({ bucket: 'my-bucket', client });
    const key = await store.put('jdg_xyz', 'the body');

    expect(key).toBe('judgments/jdg_xyz.txt');
    expect(send).toHaveBeenCalledTimes(1);
    const cmd = send.mock.calls[0][0] as PutObjectCommand;
    expect(cmd).toBeInstanceOf(PutObjectCommand);
    expect(cmd.input.Bucket).toBe('my-bucket');
    expect(cmd.input.Key).toBe('judgments/jdg_xyz.txt');
    expect(cmd.input.Body).toBe('the body');
    expect(cmd.input.ContentType).toMatch(/utf-8/);
  });

  it('get returns the body via transformToString when available', async () => {
    const send = jest.fn().mockResolvedValue({
      Body: { transformToString: async () => 'recovered body' },
    });
    const client = { send } as unknown as S3Client;
    const store = new S3RawTextStore({ bucket: 'b', client });

    const out = await store.get('judgments/jdg_1.txt');
    expect(out).toBe('recovered body');
    const cmd = send.mock.calls[0][0] as GetObjectCommand;
    expect(cmd).toBeInstanceOf(GetObjectCommand);
    expect(cmd.input.Key).toBe('judgments/jdg_1.txt');
  });

  it('get returns null when GetObject throws (missing key, network)', async () => {
    const send = jest.fn().mockRejectedValue(new Error('NoSuchKey'));
    const client = { send } as unknown as S3Client;
    const store = new S3RawTextStore({ bucket: 'b', client });

    expect(await store.get('judgments/missing.txt')).toBeNull();
  });

  it('get returns null when Body is missing entirely', async () => {
    const send = jest.fn().mockResolvedValue({});
    const client = { send } as unknown as S3Client;
    const store = new S3RawTextStore({ bucket: 'b', client });

    expect(await store.get('judgments/empty.txt')).toBeNull();
  });
});
