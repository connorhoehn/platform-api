import { contentKey, createSingleFlight, type SingleFlightStore } from '../fixtureSingleFlight';

/** One Redis shared by every "replica" (each replica is its own createSingleFlight). */
function fakeRedis(): SingleFlightStore & { data: Map<string, string> } {
  const data = new Map<string, string>();
  const expiry = new Map<string, number>();
  const live = (k: string) => {
    const at = expiry.get(k);
    if (at !== undefined && at <= Date.now()) { data.delete(k); expiry.delete(k); }
    return data.get(k) ?? null;
  };
  return {
    data,
    async set(k, v, opts) {
      if (opts.NX && live(k) !== null) return null;
      data.set(k, v);
      if (opts.PX) expiry.set(k, Date.now() + opts.PX); else expiry.delete(k);
      return 'OK';
    },
    async get(k) { return live(k); },
    async del(k) { const had = data.delete(k); expiry.delete(k); return had ? 1 : 0; },
  };
}

const tick = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

describe('fixture single-flight (NFR #162)', () => {
  it('30 concurrent identical asks over 2 replicas run once; everyone gets the answer', async () => {
    const redis = fakeRedis();
    const replicas = [0, 1].map(() => createSingleFlight<{ n: number }>({ store: async () => redis, prefix: 't', lockTtlMs: 5_000, pollMs: 5 }));
    let runs = 0;
    const run = async () => { runs += 1; await tick(40); return { n: 42 }; };
    const roles: string[] = [];
    const key = contentKey('haiku', 'the same prompt');
    const answers = await Promise.all(Array.from({ length: 30 }, (_, i) => replicas[i % 2](key, run, (role) => roles.push(role))));
    expect(runs).toBe(1);
    expect(answers.every((a) => a.n === 42)).toBe(true);
    expect(roles.filter((r) => r === 'ran')).toHaveLength(1);
    expect(roles.filter((r) => r === 'joined-remote').length).toBeGreaterThanOrEqual(1);
    // The lock is gone; the result stays only for its grace window.
    expect(redis.data.has(`t:${key}:lock`)).toBe(false);
    expect(redis.data.has(`t:${key}:result`)).toBe(true);
  });

  it('different prompts do not share', async () => {
    const flight = createSingleFlight<string>({ store: async () => fakeRedis(), prefix: 't', lockTtlMs: 5_000, pollMs: 5 });
    let runs = 0;
    const run = (v: string) => async () => { runs += 1; await tick(10); return v; };
    const [a, b] = await Promise.all([flight(contentKey('a'), run('a')), flight(contentKey('b'), run('b'))]);
    expect([a, b, runs]).toEqual(['a', 'b', 2]);
  });

  it('a runner that fails lets a waiter on the other replica run it', async () => {
    const redis = fakeRedis();
    const [r1, r2] = [0, 1].map(() => createSingleFlight<string>({ store: async () => redis, prefix: 't', lockTtlMs: 5_000, pollMs: 5 }));
    const key = contentKey('x');
    const failing = r1(key, async () => { await tick(20); throw new Error('model down'); });
    await tick(2);
    const waiter = r2(key, async () => 'second try');
    await expect(failing).rejects.toThrow('model down');
    await expect(waiter).resolves.toBe('second try');
  });

  it('without a store it still shares on this replica', async () => {
    const flight = createSingleFlight<number>({ store: async () => null, prefix: 't', lockTtlMs: 1_000 });
    let runs = 0;
    const out = await Promise.all([1, 2, 3].map(() => flight('k', async () => { runs += 1; await tick(10); return 7; })));
    expect(out).toEqual([7, 7, 7]);
    expect(runs).toBe(1);
  });
});
