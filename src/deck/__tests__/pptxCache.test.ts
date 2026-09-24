import { describe, expect, it, jest, beforeEach } from '@jest/globals';

jest.mock('../../lib/redis-client', () => ({ getRedisClient: async () => null }));
jest.mock('../renderPptx', () => ({
  PPTX_CONTENT_TYPE: 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
  renderPptx: async () => { await new Promise((r) => setTimeout(r, 20)); return Buffer.from('PK-fake'); },
}));

import { PPTX_CONTENT_KEY, buildPptxOnce, pptxBuildCount, pptxContentKey, recalledPptx, resetPptxCache } from '../pptxCache';

const spec = { title: 'Release plan', slides: [{ id: 's1', layout: 'title' as const, title: 'Release plan', sources: [] }] };

describe('pptxCache (NFR #171)', () => {
  beforeEach(() => resetPptxCache());

  it('keys by content: same spec (any key order) → same key; a changed spec → a new key; sweepable name', () => {
    const a = pptxContentKey(spec);
    expect(a).toMatch(PPTX_CONTENT_KEY);
    expect(a).toMatch(/^deck-[A-Za-z0-9._-]+\.pptx$/); // the blob sweeper's pattern
    expect(pptxContentKey({ slides: spec.slides, title: 'Release plan' })).toBe(a);
    expect(pptxContentKey({ ...spec, title: 'Other' })).not.toBe(a);
  });

  it('ten concurrent cold reads are one build; the rest share it', async () => {
    const blobs = new Map<string, Buffer>();
    const io = { read: async (k: string) => blobs.get(k) ?? null, write: async (k: string, b: Buffer) => { blobs.set(k, b); } };
    const key = pptxContentKey(spec);
    const out = await Promise.all(Array.from({ length: 10 }, () => buildPptxOnce(key, spec, io)));
    expect(pptxBuildCount()).toBe(1);
    expect(out.filter((o) => o.source === 'built')).toHaveLength(1);
    expect(out.filter((o) => o.source === 'joined-local')).toHaveLength(9);
    expect(blobs.get(key)?.toString()).toBe('PK-fake');
    expect(recalledPptx(key)?.toString()).toBe('PK-fake');
  });

  it('a stored blob is read, not rebuilt', async () => {
    const key = pptxContentKey(spec);
    const io = { read: async () => Buffer.from('PK-stored'), write: async () => { throw new Error('no write'); } };
    const out = await buildPptxOnce(key, spec, io);
    expect(out.source).toBe('stored');
    expect(pptxBuildCount()).toBe(0);
  });
});
