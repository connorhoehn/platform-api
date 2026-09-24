// NFR #166: slide requests in flight together share one row read and one read
// decision; nothing is kept after, so a later request is decided fresh.

import { SlideReadCache } from '../slideReadCache';

const deferred = <T>() => {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((r) => { resolve = r; });
  return { promise, resolve };
};

describe('SlideReadCache', () => {
  it('joins concurrent row reads and decisions, and reads again once they settled', async () => {
    const cache = new SlideReadCache();
    const row = deferred<any>();
    const get = jest.fn(() => row.promise);
    const store = { get } as any;
    const reads = Array.from({ length: 20 }, () => cache.row(store, 'doc-1'));
    expect(get).toHaveBeenCalledTimes(1);
    row.resolve({ documentId: 'doc-1' });
    await Promise.all(reads);
    await new Promise((r) => setImmediate(r));
    get.mockResolvedValueOnce({ documentId: 'doc-1', later: true } as never);
    expect(await cache.row(store, 'doc-1')).toEqual({ documentId: 'doc-1', later: true });
    expect(get).toHaveBeenCalledTimes(2);

    const decide = jest.fn(async () => true);
    await Promise.all(Array.from({ length: 20 }, () => cache.canRead('u:doc-1', decide)));
    expect(decide).toHaveBeenCalledTimes(1);
    const revoked = jest.fn(async () => false);
    expect(await cache.canRead('u:doc-1', revoked)).toBe(false);
    expect(revoked).toHaveBeenCalledTimes(1);
  });

  it('keeps a revision slide list and slide bytes once read', async () => {
    const cache = new SlideReadCache();
    const readAsset = jest.fn(async () => Buffer.from(JSON.stringify([{ id: 's1', index: 0 }])));
    const store = { readAsset } as any;
    const revision = { id: 'v1', slidesRef: { key: 'manifest-v1' } } as any;
    await cache.slides(store, 'doc-1', revision);
    expect(await cache.slides(store, 'doc-1', revision)).toEqual([{ id: 's1', index: 0 }]);
    expect(readAsset).toHaveBeenCalledTimes(1);

    const read = jest.fn(async () => Buffer.from('<svg/>'));
    await cache.slideBytes('doc-1:v1:s1', read);
    await cache.slideBytes('doc-1:v1:s1', read);
    expect(read).toHaveBeenCalledTimes(1);
    const missing = jest.fn(async () => null);
    await cache.slideBytes('doc-1:v1:s2', missing);
    await cache.slideBytes('doc-1:v1:s2', missing);
    expect(missing).toHaveBeenCalledTimes(2);
  });
});
