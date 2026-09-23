import { applyWorkGraphViewPatchV2, type WorkGraphStreamViewV2 } from '@connorhoehn/realtime-modules/work-graph';
import { FeedViewCache, type FeedViewScope } from '../feedView';

const scope: FeedViewScope = { organizationId: 'org', viewerId: 'frank', personId: 'connor', day: '2026-09-23', timezone: 'America/New_York' };

function view(titles: string[]): WorkGraphStreamViewV2 {
  return {
    query: { schemaVersion: 2, personId: 'connor', day: '2026-09-23', timezone: 'America/New_York', windowStart: '2026-09-23T04:00:00.000Z', windowEnd: '2026-09-24T04:00:00.000Z', mode: 'live' },
    temporal: { mode: 'live', observedAt: '2026-09-23T12:00:00.000Z', coverage: { from: '2026-09-23T04:00:00.000Z', through: '2026-09-23T12:00:00.000Z', complete: true } },
    efforts: titles.map((title, i) => ({ id: `e${i}`, title })) as unknown as WorkGraphStreamViewV2['efforts'],
    details: [], operations: [], eventBuckets: [],
  };
}

function cache(): FeedViewCache {
  let n = 0;
  let now = 0;
  const instance = new FeedViewCache({ id: () => `v${++n}`, now: () => now, maxEntries: 3, ttlMs: 1_000 });
  (instance as unknown as { tick: (ms: number) => void }).tick = (ms) => { now += ms; };
  return instance;
}

describe('FeedViewCache', () => {
  it('answers the first call whole and the next as a patch that rebuilds the view', () => {
    const c = cache();
    const first = c.encode(scope, view(['a', 'b']));
    expect(first).toEqual({ viewId: 'v1', view: view(['a', 'b']) });
    const next = view(['a', 'b2', 'c']);
    const second = c.encode(scope, next, 'v1');
    expect(second.viewPatch).toBeDefined();
    expect(second.baseViewId).toBe('v1');
    expect(applyWorkGraphViewPatchV2(view(['a', 'b']), second.viewPatch!)).toEqual(next);
    expect(JSON.stringify(second.viewPatch).length).toBeLessThan(JSON.stringify(next).length);
  });

  it('never patches across scopes, after expiry, or against an unknown or superseded base', () => {
    const c = cache();
    c.encode(scope, view(['a']));
    expect(c.encode({ ...scope, viewerId: 'mallory' }, view(['a']), 'v1').view).toBeDefined();
    const again = c.encode(scope, view(['a']));
    expect(again.viewId).toBe('v3');
    expect(c.encode(scope, view(['a']), 'v3').viewPatch).toBeDefined();
    expect(c.encode(scope, view(['a']), 'v3').view).toBeDefined(); // v3 was superseded by v4
    expect(c.encode(scope, view(['a']), 'nope').view).toBeDefined();
    const last = c.encode(scope, view(['a']));
    (c as unknown as { tick: (ms: number) => void }).tick(1_001);
    expect(c.encode(scope, view(['a']), last.viewId).view).toBeDefined();
  });

  it('holds a bounded number of views', () => {
    const c = cache();
    for (let i = 0; i < 10; i++) c.encode(scope, view([String(i)]));
    expect(c.size).toBe(3);
  });
});
