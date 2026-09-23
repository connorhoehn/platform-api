import { applyWorkGraphViewPatchV2, type WorkGraphStreamViewV2 } from '@connorhoehn/realtime-modules/work-graph';
import { FeedViewCache, type FeedViewScope, type FeedViewStore } from '../feedView';

const scope: FeedViewScope = { organizationId: 'org', viewerId: 'frank', personId: 'connor', day: '2026-09-23', timezone: 'America/New_York' };

function view(titles: string[]): WorkGraphStreamViewV2 {
  return {
    query: { schemaVersion: 2, personId: 'connor', day: '2026-09-23', timezone: 'America/New_York', windowStart: '2026-09-23T04:00:00.000Z', windowEnd: '2026-09-24T04:00:00.000Z', mode: 'live' },
    temporal: { mode: 'live', observedAt: '2026-09-23T12:00:00.000Z', coverage: { from: '2026-09-23T04:00:00.000Z', through: '2026-09-23T12:00:00.000Z', complete: true } },
    efforts: titles.map((title, i) => ({ id: `e${i}`, title })) as unknown as WorkGraphStreamViewV2['efforts'],
    details: [], operations: [], eventBuckets: [],
  };
}

function clock() {
  const t = { now: 0 };
  return t;
}

function cache(t = clock(), store?: FeedViewStore): FeedViewCache {
  return new FeedViewCache({ now: () => t.now, maxEntries: 3, ttlMs: 1_000, ...(store ? { store: async () => store } : {}) });
}

/** One Redis shared by two platform replicas. */
function sharedStore(): FeedViewStore & { data: Map<string, string> } {
  const data = new Map<string, string>();
  return { data, get: async (k) => data.get(k) ?? null, set: async (k, v) => { data.set(k, v); } };
}

describe('FeedViewCache', () => {
  it('answers the first call whole and the next as a patch that rebuilds the view', async () => {
    const c = cache();
    const first = await c.encode(scope, view(['a', 'b']));
    expect(first.view).toEqual(view(['a', 'b']));
    const next = view(['a', 'b2', 'c']);
    const second = await c.encode(scope, next, first.viewId);
    expect(second.viewPatch).toBeDefined();
    expect(second.baseViewId).toBe(first.viewId);
    expect(applyWorkGraphViewPatchV2(view(['a', 'b']), second.viewPatch!)).toEqual(next);
    expect(JSON.stringify(second.viewPatch).length).toBeLessThan(JSON.stringify(next).length);
  });

  it('names a view by scope and content, so identical views share an id and scopes never do', async () => {
    const c = cache();
    const a = await c.encode(scope, view(['a']));
    expect(a.viewId).toMatch(/^[0-9a-f]{64}$/); // the replay route's baseViewId shape
    expect((await c.encode(scope, view(['a']))).viewId).toBe(a.viewId);
    expect((await c.encode({ ...scope, viewerId: 'mallory' }, view(['a']))).viewId).not.toBe(a.viewId);
    expect((await c.encode({ ...scope, mode: 'past' }, view(['a']))).viewId).not.toBe(a.viewId);
  });

  it('never patches across scopes, after expiry, or against an unknown or superseded base', async () => {
    const t = clock();
    const c = cache(t);
    const a = await c.encode(scope, view(['a']));
    expect((await c.encode({ ...scope, viewerId: 'mallory' }, view(['a']), a.viewId)).view).toBeDefined();
    const b = await c.encode(scope, view(['b']), a.viewId);
    expect(b.viewPatch).toBeDefined();
    expect((await c.encode(scope, view(['c']), a.viewId)).view).toBeDefined(); // a was superseded by b
    expect((await c.encode(scope, view(['a']), 'nope')).view).toBeDefined();
    const last = await c.encode(scope, view(['d']));
    t.now += 1_001;
    expect((await c.encode(scope, view(['e']), last.viewId)).view).toBeDefined();
  });

  it('holds a bounded number of views', async () => {
    const c = cache();
    for (let i = 0; i < 10; i++) await c.encode(scope, view([String(i)]));
    expect(c.size).toBe(3);
  });

  it('patches on another replica from the shared store, re-hashing what it reads', async () => {
    const redis = sharedStore();
    const one = cache(clock(), redis);
    const two = cache(clock(), redis);
    const first = await one.encode(scope, view(['a', 'b']));
    expect(redis.data.get(`wg:viewbase:${first.viewId}`)).toBe(JSON.stringify(view(['a', 'b'])));
    const next = view(['a', 'b', 'c']);
    const second = await two.encode(scope, next, first.viewId);
    expect(second.baseViewId).toBe(first.viewId);
    expect(applyWorkGraphViewPatchV2(view(['a', 'b']), second.viewPatch!)).toEqual(next);
    expect(two.stats).toMatchObject({ patchShared: 1, whole: 0 });

    // Another scope naming the id, or a tampered stored value, is a miss.
    expect((await two.encode({ ...scope, viewerId: 'mallory' }, next, second.viewId)).view).toBeDefined();
    redis.data.set(`wg:viewbase:${second.viewId}`, JSON.stringify(view(['forged'])));
    const three = cache(clock(), redis);
    expect((await three.encode(scope, view(['z']), second.viewId)).view).toBeDefined();
    expect(three.stats.sharedMiss).toBe(1);
  });

  it('answers whole when the shared store is slow or failing', async () => {
    const slow: FeedViewStore = { get: () => new Promise(() => {}), set: () => new Promise(() => {}) };
    const c = new FeedViewCache({ store: async () => slow, sharedTimeoutMs: 20 });
    const r = await c.encode(scope, view(['a']), 'unknown');
    expect(r.view).toBeDefined();
    const broken: FeedViewStore = { get: async () => { throw new Error('down'); }, set: async () => { throw new Error('down'); } };
    const d = new FeedViewCache({ store: async () => broken });
    expect((await d.encode(scope, view(['a']), 'unknown')).view).toBeDefined();
  });
});
