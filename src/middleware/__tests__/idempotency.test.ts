// Tests for the Idempotency-Key middleware + the legacy in-memory cache helper.
//
// Covers:
//   - cache miss → request runs, response captured + persisted
//   - cache hit  → cached response replayed verbatim with X-Idempotent-Replay
//   - same key + different body → 409 Conflict
//   - 4xx/5xx responses NOT cached (errors should be retryable)
//   - TTL: expired entries return null
//   - hashBody is order-insensitive (stable canonical form)
//   - back-compat in-memory cache (`createIdempotencyCache`) honors TTL
//
// These tests deliberately avoid supertest (not in the social-api dep tree).
// We exercise the middleware as a plain function with mock req/res objects,
// then thread captured response state through a fake handler that mirrors
// what an Express route would do.

import type { NextFunction, Request, Response } from 'express';

// Silence the redis-client's connection noise: tests always inject an explicit
// store override, but the 'no header' / passthrough path still calls
// resolveStore() which would touch Redis (and log warnings for unavailable
// hosts). Stub the module so it never tries.
jest.mock('../../lib/redis-client', () => ({
  getRedisClient: async () => null,
}));

import {
  _resetIdempotencyMemFallback,
  createIdempotencyCache,
  hashBody,
  idempotency,
  type CachedResponse,
  type IdempotencyStore,
} from '../idempotency';

// ---------------------------------------------------------------------------
// Test rig — minimal Express-shaped req/res that captures status/body/headers
// and a real `next()` implementation that defers to a "handler".
// ---------------------------------------------------------------------------

interface MockRes {
  statusCode: number;
  headers: Record<string, string | number | readonly string[]>;
  body: unknown;
  ended: boolean;
  status(code: number): MockRes;
  json(body: unknown): MockRes;
  setHeader(name: string, value: number | string | readonly string[]): MockRes;
  getHeader(name: string): unknown;
  end(...args: unknown[]): MockRes;
}

function mockRes(): MockRes {
  const r: MockRes = {
    statusCode: 200,
    headers: {},
    body: undefined,
    ended: false,
    status(code) {
      this.statusCode = code;
      return this;
    },
    json(body) {
      this.body = body;
      this.ended = true;
      return this;
    },
    setHeader(name, value) {
      this.headers[String(name)] = value;
      return this;
    },
    getHeader(name) {
      return this.headers[String(name)];
    },
    end(..._args) {
      this.ended = true;
      return this;
    },
  };
  return r;
}

function mockReq(opts: {
  body?: unknown;
  idempotencyKey?: string;
  userId?: string;
}): Request {
  const headers: Record<string, string> = {};
  if (opts.idempotencyKey !== undefined) headers['idempotency-key'] = opts.idempotencyKey;
  return {
    headers,
    body: opts.body,
    user: opts.userId === null ? undefined : { sub: opts.userId ?? 'user-1' },
  } as unknown as Request;
}

/**
 * Run the middleware against a mock req/res, then run `handler` only if the
 * middleware called next() (i.e. cache miss / passthrough). Returns the
 * captured response state so tests can assert on status/body/headers.
 */
async function runMiddleware(
  mw: ReturnType<typeof idempotency>,
  req: Request,
  // Handlers may return the Response object (chained .json()) or nothing —
  // we only care about completion, so widen the return type.
  handler: (req: Request, res: Response) => unknown | Promise<unknown>,
): Promise<MockRes> {
  const res = mockRes();
  await new Promise<void>((resolve, reject) => {
    const next: NextFunction = (err?: unknown) => {
      if (err) {
        reject(err);
        return;
      }
      // Simulate the wrapped route handler.
      Promise.resolve(handler(req, res as unknown as Response))
        .then(() => resolve())
        .catch(reject);
    };
    // If the middleware writes the response itself (replay or 409), it never
    // calls next(). We resolve by polling res.ended after a microtask.
    Promise.resolve(mw(req, res as unknown as Response, next))
      .then(() => {
        if (res.ended && !next.length) {
          // No-op: next() may have already been invoked synchronously in
          // the passthrough path.
        }
        if (res.ended) resolve();
      })
      .catch(reject);
  });
  return res;
}

// ---------------------------------------------------------------------------
// Fake store — full control over Redis-side timing for deterministic tests.
// ---------------------------------------------------------------------------

function fakeStore(): IdempotencyStore & { dump(): Map<string, string>; clear(): void } {
  const map = new Map<string, string>();
  return {
    async get(key) {
      return map.get(key) ?? null;
    },
    async set(key, value, _opts) {
      map.set(key, value);
    },
    dump() {
      return new Map(map);
    },
    clear() {
      map.clear();
    },
  };
}

beforeEach(() => {
  _resetIdempotencyMemFallback();
});

// ---------------------------------------------------------------------------
// hashBody
// ---------------------------------------------------------------------------

describe('hashBody', () => {
  test('is stable regardless of key order', () => {
    expect(hashBody({ a: 1, b: 2 })).toBe(hashBody({ b: 2, a: 1 }));
  });
  test('distinguishes different bodies', () => {
    expect(hashBody({ a: 1 })).not.toBe(hashBody({ a: 2 }));
  });
  test('handles arrays + nested', () => {
    expect(hashBody({ x: [1, { y: 2 }] })).toBe(hashBody({ x: [1, { y: 2 }] }));
    expect(hashBody({ x: [1, { y: 2 }] })).not.toBe(hashBody({ x: [{ y: 2 }, 1] }));
  });
  test('null and undefined collapse', () => {
    expect(hashBody(null)).toBe(hashBody(undefined));
  });
});

// ---------------------------------------------------------------------------
// idempotency middleware
// ---------------------------------------------------------------------------

describe('idempotency middleware — cache hit/miss', () => {
  test('no header → passthrough (handler runs, nothing cached)', async () => {
    const store = fakeStore();
    let calls = 0;
    const mw = idempotency({ scope: 'test', store });

    const res = await runMiddleware(mw, mockReq({ body: { x: 1 } }), (_req, r) => {
      calls += 1;
      r.status(200).json({ ok: true });
    });

    expect(res.statusCode).toBe(200);
    expect(res.body).toEqual({ ok: true });
    expect(calls).toBe(1);
    expect(store.dump().size).toBe(0);
  });

  test('first request stores response; replay returns it with X-Idempotent-Replay', async () => {
    const store = fakeStore();
    let calls = 0;
    const mw = idempotency({ scope: 'test', store });
    const handler = (_req: Request, r: Response): void => {
      calls += 1;
      r.status(202).json({ runId: 'abc', n: calls });
    };

    const r1 = await runMiddleware(mw, mockReq({ body: { x: 1 }, idempotencyKey: 'k1' }), handler);
    expect(r1.statusCode).toBe(202);
    expect(r1.body).toEqual({ runId: 'abc', n: 1 });
    expect(r1.headers['X-Idempotent-Replay']).toBeUndefined();
    // Allow microtask flush for fire-and-forget persist.
    await new Promise((r) => setImmediate(r));
    expect(store.dump().size).toBe(1);

    // Replay: handler must NOT run again.
    const r2 = await runMiddleware(mw, mockReq({ body: { x: 1 }, idempotencyKey: 'k1' }), handler);
    expect(r2.statusCode).toBe(202);
    expect(r2.body).toEqual({ runId: 'abc', n: 1 });
    expect(r2.headers['X-Idempotent-Replay']).toBe('true');
    expect(calls).toBe(1);
  });

  test('same key, different body → 409 Conflict', async () => {
    const store = fakeStore();
    const mw = idempotency({ scope: 'test', store });
    const handler = (_req: Request, r: Response): void => {
      r.status(202).json({ runId: 'abc' });
    };

    const r1 = await runMiddleware(mw, mockReq({ body: { x: 1 }, idempotencyKey: 'k2' }), handler);
    expect(r1.statusCode).toBe(202);
    await new Promise((r) => setImmediate(r));

    const r2 = await runMiddleware(
      mw,
      mockReq({ body: { x: 2 }, idempotencyKey: 'k2' }),
      handler,
    );
    expect(r2.statusCode).toBe(409);
    expect((r2.body as { error: string }).error).toBe('idempotency_key_conflict');
  });

  test('204 (no body) responses are still replayed', async () => {
    const store = fakeStore();
    let calls = 0;
    const mw = idempotency({ scope: 'test', store });
    const handler = (_req: Request, r: Response): void => {
      calls += 1;
      r.status(204).end();
    };

    const r1 = await runMiddleware(mw, mockReq({ body: { x: 1 }, idempotencyKey: 'k3' }), handler);
    expect(r1.statusCode).toBe(204);
    await new Promise((r) => setImmediate(r));

    const r2 = await runMiddleware(mw, mockReq({ body: { x: 1 }, idempotencyKey: 'k3' }), handler);
    expect(r2.statusCode).toBe(204);
    expect(r2.headers['X-Idempotent-Replay']).toBe('true');
    expect(calls).toBe(1);
  });

  test('error responses (5xx) are NOT cached — retryable', async () => {
    const store = fakeStore();
    let calls = 0;
    const mw = idempotency({ scope: 'test', store });
    const handler = (_req: Request, r: Response): void => {
      calls += 1;
      r.status(500).json({ error: 'boom' });
    };

    const r1 = await runMiddleware(mw, mockReq({ body: { x: 1 }, idempotencyKey: 'k4' }), handler);
    expect(r1.statusCode).toBe(500);
    await new Promise((r) => setImmediate(r));

    const r2 = await runMiddleware(mw, mockReq({ body: { x: 1 }, idempotencyKey: 'k4' }), handler);
    expect(r2.statusCode).toBe(500);
    expect(calls).toBe(2);
    expect(store.dump().size).toBe(0);
  });

  test('different scopes do not collide on the same key', async () => {
    const store = fakeStore();
    const mwA = idempotency({ scope: 'A', store });
    const mwB = idempotency({ scope: 'B', store });

    const a = await runMiddleware(
      mwA,
      mockReq({ body: { x: 1 }, idempotencyKey: 'shared' }),
      (_req, r) => r.json({ scope: 'A' }),
    );
    const b = await runMiddleware(
      mwB,
      mockReq({ body: { x: 1 }, idempotencyKey: 'shared' }),
      (_req, r) => r.json({ scope: 'B' }),
    );
    expect(a.body).toEqual({ scope: 'A' });
    expect(b.body).toEqual({ scope: 'B' });
    await new Promise((r) => setImmediate(r));
    expect(store.dump().size).toBe(2);
  });

  test('different users do not collide on the same key', async () => {
    const store = fakeStore();
    const mw = idempotency({ scope: 'test', store });
    const handler = (req: Request, r: Response): void => {
      r.status(202).json({ user: req.user!.sub });
    };

    const r1 = await runMiddleware(
      mw,
      mockReq({ body: { x: 1 }, idempotencyKey: 'k7', userId: 'alice' }),
      handler,
    );
    await new Promise((r) => setImmediate(r));
    const r2 = await runMiddleware(
      mw,
      mockReq({ body: { x: 1 }, idempotencyKey: 'k7', userId: 'bob' }),
      handler,
    );
    expect(r1.body).toEqual({ user: 'alice' });
    expect(r2.body).toEqual({ user: 'bob' });
  });

  test('ttlSeconds is forwarded as EX option (24h default)', async () => {
    const seen: Array<{ key: string; opts: { EX: number } }> = [];
    const store: IdempotencyStore = {
      async get() {
        return null;
      },
      async set(key, _value, opts) {
        seen.push({ key, opts });
      },
    };
    const mw = idempotency({ scope: 'test', store });
    await runMiddleware(mw, mockReq({ body: { x: 1 }, idempotencyKey: 'k5' }), (_req, r) =>
      r.json({ ok: true }),
    );
    await new Promise((r) => setImmediate(r));
    expect(seen).toHaveLength(1);
    expect(seen[0].opts.EX).toBe(60 * 60 * 24);
  });

  test('explicit ttlSeconds overrides default', async () => {
    const seen: Array<{ key: string; opts: { EX: number } }> = [];
    const store: IdempotencyStore = {
      async get() {
        return null;
      },
      async set(key, _value, opts) {
        seen.push({ key, opts });
      },
    };
    const mw = idempotency({ scope: 'test', store, ttlSeconds: 1234 });
    await runMiddleware(mw, mockReq({ body: { x: 1 }, idempotencyKey: 'k8' }), (_req, r) =>
      r.json({ ok: true }),
    );
    await new Promise((r) => setImmediate(r));
    expect(seen[0].opts.EX).toBe(1234);
  });

  test('cached payload contains body hash + status + headers', async () => {
    const store = fakeStore();
    const mw = idempotency({ scope: 'test', store });
    await runMiddleware(mw, mockReq({ body: { x: 1 }, idempotencyKey: 'k6' }), (_req, r) => {
      r.setHeader('X-Custom', 'y');
      r.status(202).json({ ok: true });
    });
    await new Promise((r) => setImmediate(r));
    const raw = Array.from(store.dump().values())[0];
    const parsed = JSON.parse(raw) as CachedResponse;
    expect(parsed.status).toBe(202);
    expect(parsed.body).toEqual({ ok: true });
    expect(parsed.headers['X-Custom']).toBe('y');
    expect(parsed.bodyHash).toBe(hashBody({ x: 1 }));
  });
});

// ---------------------------------------------------------------------------
// Legacy in-memory cache helper.
// ---------------------------------------------------------------------------

describe('createIdempotencyCache (legacy in-memory)', () => {
  test('roundtrip set/get within TTL', () => {
    const cache = createIdempotencyCache<{ runId: string }>(1000);
    expect(cache.get('k')).toBeNull();
    cache.set('k', { runId: 'r1' });
    expect(cache.get('k')?.response).toEqual({ runId: 'r1' });
  });

  test('TTL: stale entries return null and are evicted', () => {
    jest.useFakeTimers();
    try {
      const cache = createIdempotencyCache<number>(100);
      cache.set('k', 42);
      expect(cache.get('k')?.response).toBe(42);
      jest.advanceTimersByTime(150);
      expect(cache.get('k')).toBeNull();
    } finally {
      jest.useRealTimers();
    }
  });

  test('sweep drops expired entries en masse', () => {
    jest.useFakeTimers();
    try {
      const cache = createIdempotencyCache<number>(50);
      cache.set('a', 1);
      cache.set('b', 2);
      jest.advanceTimersByTime(100);
      cache.sweep();
      expect(cache.get('a')).toBeNull();
      expect(cache.get('b')).toBeNull();
    } finally {
      jest.useRealTimers();
    }
  });
});
