// NFR #16: a person's conversation list is kept in the cache Redis and
// dropped by the gateway's membership signal, with a generation guard for the
// fill that raced the signal.

jest.mock('../../lib/redis-client', () => ({ getRedisCacheClient: async () => null, sharedRedisUrl: () => 'redis://x' }));

import { MembershipProjection, membershipChangeViewer, withFreshMembership } from '../membershipProjection';
import { memoOnce, withBuildMemo } from '../buildMemo';

function fakeCache() {
  const data = new Map<string, string>();
  return {
    data,
    mGet: async (keys: string[]) => keys.map((key) => data.get(key) ?? null),
    get: async (key: string) => data.get(key) ?? null,
    set: async (key: string, value: string) => { data.set(key, value); return 'OK'; },
    incr: async (key: string) => { const next = Number(data.get(key) ?? '0') + 1; data.set(key, String(next)); return next; },
    expire: async () => true,
    del: async (key: string) => (data.delete(key) ? 1 : 0),
  };
}

describe('MembershipProjection', () => {
  it('scans once, then answers from the cache until a membership change', async () => {
    const cache = fakeCache();
    const projection = new MembershipProjection(async () => cache as never);
    let scans = 0;
    const load = async () => { scans += 1; return scans === 1 ? ['general'] : ['general', 'ops']; };
    expect(await projection.conversationIds('ada', load)).toEqual(['general']);
    expect(await projection.conversationIds('ada', load)).toEqual(['general']);
    expect(scans).toBe(1);
    await projection.invalidate('ada');
    expect(await projection.conversationIds('ada', load)).toEqual(['general', 'ops']);
    expect(scans).toBe(2);
  });

  it('never believes a fill that started before the signal landed', async () => {
    const cache = fakeCache();
    const projection = new MembershipProjection(async () => cache as never);
    let release!: () => void;
    const slowLoad = () => new Promise<string[]>((resolve) => { release = () => resolve(['general']); });
    const filling = projection.conversationIds('ada', slowLoad);
    await new Promise((resolve) => setImmediate(resolve));
    await projection.invalidate('ada'); // the removal commits and signals mid-Scan
    release();
    await filling;
    let scans = 0;
    expect(await projection.conversationIds('ada', async () => { scans += 1; return []; })).toEqual([]);
    expect(scans).toBe(1);
  });

  it('reads DynamoDB on the access re-check path and refreshes the cache', async () => {
    const cache = fakeCache();
    const projection = new MembershipProjection(async () => cache as never);
    await projection.conversationIds('ada', async () => ['general']);
    const fresh = await withFreshMembership(() => projection.conversationIds('ada', async () => []));
    expect(fresh).toEqual([]);
    expect(await projection.conversationIds('ada', async () => ['stale'])).toEqual([]);
  });

  it('falls back to DynamoDB when there is no cache Redis', async () => {
    const projection = new MembershipProjection(async () => null);
    expect(await projection.conversationIds('ada', async () => ['general'])).toEqual(['general']);
  });

  it('reads the viewer from the gateway’s membership signal only', () => {
    expect(membershipChangeViewer(JSON.stringify({ payload: { viewerId: 'ada', reason: 'membership' } }))).toBe('ada');
    expect(membershipChangeViewer(JSON.stringify({ payload: { personId: 'ada', reason: 'grant' } }))).toBeNull();
    expect(membershipChangeViewer('not json')).toBeNull();
  });
});

describe('build memo', () => {
  it('reads a key once per build and fresh in the next build', async () => {
    let reads = 0;
    const read = () => memoOnce('k', async () => { reads += 1; return reads; });
    await withBuildMemo(async () => { expect(await read()).toBe(1); expect(await read()).toBe(1); });
    await withBuildMemo(async () => { expect(await read()).toBe(2); });
    expect(await read()).toBe(3); // outside a build, nothing is remembered
  });
});
