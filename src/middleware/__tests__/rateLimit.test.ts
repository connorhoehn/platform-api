// Tests for the per-key token-bucket rate limiter middleware.
//
// Mirrors the test rig used in `idempotency.test.ts` — minimal Express-shaped
// req/res mocks, no supertest dependency. Tests assert observable behaviour
// (passes / 429s, headers, body shape, refill timing under fake timers)
// rather than poking the underlying bucket state, which is now opaque
// behind DC's `RateLimitStore.take()` interface.
//
// Coverage:
//   - under-limit request returns 200 and consumes a token
//   - over-limit request returns 429 with `Retry-After` header + RFC-7807 body
//   - refill restores availability after the configured interval (fake timers)
//   - two different users (different keys) have separate buckets
//   - in-memory fallback works when no store is injected and Redis is null

import type { NextFunction, Request, Response } from 'express';

// Force `getRedisClient()` to return null so the Redis-less fallback path
// is exercised. Tests that need a custom store inject `opts.store` directly.
jest.mock('../../lib/redis-client', () => ({
  getRedisClient: async () => null,
}));

import {
  _resetRateLimitMemFallback,
  createRateLimiter,
  type RateLimitedProblem,
} from '../rateLimit';

// ---------------------------------------------------------------------------
// Mock req/res helpers — copied from idempotency.test.ts for parity.
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

function mockReq(opts: { userId?: string; ip?: string } = {}): Request {
  return {
    headers: {},
    user: opts.userId === undefined ? { sub: 'user-1' } : { sub: opts.userId },
    ip: opts.ip,
  } as unknown as Request;
}

/**
 * Drive the middleware once and report whether `next()` was called (request
 * passes) vs the response was written directly (429). The `handler` is only
 * invoked on the pass path so we can assert "handler ran" cleanly.
 */
async function runOnce(
  mw: ReturnType<typeof createRateLimiter>,
  req: Request,
  handler: (req: Request, res: Response) => void = (_q, r) => {
    r.status(200).json({ ok: true });
  },
): Promise<{ res: MockRes; passed: boolean }> {
  const res = mockRes();
  let passed = false;
  await new Promise<void>((resolve, reject) => {
    const next: NextFunction = (err?: unknown) => {
      if (err) {
        reject(err);
        return;
      }
      passed = true;
      try {
        handler(req, res as unknown as Response);
        resolve();
      } catch (e) {
        reject(e);
      }
    };
    Promise.resolve(mw(req, res as unknown as Response, next))
      .then(() => {
        if (res.ended) resolve();
      })
      .catch(reject);
  });
  return { res, passed };
}

beforeEach(() => {
  _resetRateLimitMemFallback();
});

// ---------------------------------------------------------------------------
// Construction validation
// ---------------------------------------------------------------------------

describe('createRateLimiter — option validation', () => {
  test('rejects non-positive capacity', () => {
    expect(() =>
      createRateLimiter({
        capacity: 0,
        refillRate: 1,
        refillIntervalMs: 1000,
        key: () => 'k',
      }),
    ).toThrow(/capacity/);
  });
  test('rejects non-positive refillRate', () => {
    expect(() =>
      createRateLimiter({
        capacity: 1,
        refillRate: 0,
        refillIntervalMs: 1000,
        key: () => 'k',
      }),
    ).toThrow(/refillRate/);
  });
  test('rejects non-positive refillIntervalMs', () => {
    expect(() =>
      createRateLimiter({
        capacity: 1,
        refillRate: 1,
        refillIntervalMs: 0,
        key: () => 'k',
      }),
    ).toThrow(/refillIntervalMs/);
  });
});

// ---------------------------------------------------------------------------
// Under-limit / over-limit
// ---------------------------------------------------------------------------

describe('rate limiter — under/over limit', () => {
  test('under-limit request passes through with 200', async () => {
    const mw = createRateLimiter({
      capacity: 3,
      refillRate: 1,
      refillIntervalMs: 1000,
      key: (req) => req.user!.sub,
      scope: 'underlimit',
    });
    const { res, passed } = await runOnce(mw, mockReq({ userId: 'alice' }));
    expect(passed).toBe(true);
    expect(res.statusCode).toBe(200);
    expect(res.body).toEqual({ ok: true });
  });

  test('successive requests up to capacity all pass; the next one 429s', async () => {
    const mw = createRateLimiter({
      capacity: 3,
      refillRate: 1,
      refillIntervalMs: 60_000, // slow refill so no drift mid-test
      key: (req) => req.user!.sub,
      scope: 'capacity-drain',
    });

    // Three should pass (the bucket starts at capacity=3).
    for (let i = 0; i < 3; i++) {
      // eslint-disable-next-line no-await-in-loop
      const r = await runOnce(mw, mockReq({ userId: 'drain' }));
      expect(r.passed).toBe(true);
      expect(r.res.statusCode).toBe(200);
    }
    // The fourth should be blocked.
    const blocked = await runOnce(mw, mockReq({ userId: 'drain' }));
    expect(blocked.passed).toBe(false);
    expect(blocked.res.statusCode).toBe(429);
  });

  test('over-limit request returns 429 with Retry-After + RFC-7807 body', async () => {
    const mw = createRateLimiter({
      capacity: 2,
      refillRate: 1,
      refillIntervalMs: 60_000, // slow refill so we don't drift mid-test
      key: (req) => req.user!.sub,
      scope: 'pipeline-trigger',
    });

    // First two consume the burst.
    const a = await runOnce(mw, mockReq({ userId: 'bob' }));
    const b = await runOnce(mw, mockReq({ userId: 'bob' }));
    expect(a.passed).toBe(true);
    expect(b.passed).toBe(true);

    // Third should be rate-limited.
    const c = await runOnce(mw, mockReq({ userId: 'bob' }));
    expect(c.passed).toBe(false);
    expect(c.res.statusCode).toBe(429);
    const body = c.res.body as RateLimitedProblem;
    expect(body.status).toBe(429);
    expect(body.title).toBe('Too Many Requests');
    expect(typeof body.type).toBe('string');
    expect(typeof body.detail).toBe('string');
    expect(body.detail).toMatch(/pipeline-trigger/);
    expect(body.retryAfterSec).toBeGreaterThan(0);
    // Retry-After header must reflect the same value (in seconds).
    expect(c.res.headers['Retry-After']).toBe(String(body.retryAfterSec));
  });

  test('429 body type/title match RFC-7807 problem-details shape', async () => {
    const mw = createRateLimiter({
      capacity: 1,
      refillRate: 1,
      refillIntervalMs: 60_000,
      key: () => 'singleton',
      scope: 'shape-check',
    });
    await runOnce(mw, mockReq()); // consume
    const blocked = await runOnce(mw, mockReq());
    expect(blocked.passed).toBe(false);
    const body = blocked.res.body as RateLimitedProblem;
    expect(body).toEqual(
      expect.objectContaining({
        type: expect.any(String),
        title: expect.any(String),
        status: 429,
        detail: expect.any(String),
        retryAfterSec: expect.any(Number),
      }),
    );
  });
});

// ---------------------------------------------------------------------------
// Refill behavior — uses jest fake timers to advance Date.now.
// ---------------------------------------------------------------------------

describe('rate limiter — refill restores availability', () => {
  test('after refill interval, a fresh request passes', async () => {
    jest.useFakeTimers();
    try {
      jest.setSystemTime(new Date('2026-01-01T00:00:00Z'));
      const mw = createRateLimiter({
        capacity: 1,
        refillRate: 1,
        refillIntervalMs: 1000, // 1 token / second
        key: () => 'refill-user',
        scope: 'refill',
      });

      // Burn the only token.
      const first = await runOnce(mw, mockReq());
      expect(first.passed).toBe(true);

      // Immediate second attempt → 429.
      const blocked = await runOnce(mw, mockReq());
      expect(blocked.passed).toBe(false);
      expect(blocked.res.statusCode).toBe(429);

      // Advance past the refill interval so the bucket has a token again.
      jest.advanceTimersByTime(1500);

      const allowed = await runOnce(mw, mockReq());
      expect(allowed.passed).toBe(true);
      expect(allowed.res.statusCode).toBe(200);
    } finally {
      jest.useRealTimers();
    }
  });

  test('partial refill — fractional tokens still block until ≥1', async () => {
    jest.useFakeTimers();
    try {
      jest.setSystemTime(new Date('2026-01-01T00:00:00Z'));
      const mw = createRateLimiter({
        capacity: 1,
        refillRate: 1,
        refillIntervalMs: 1000,
        key: () => 'frac',
        scope: 'partial-refill',
      });

      await runOnce(mw, mockReq()); // consume
      // Only 400ms of a 1s refill interval — bucket holds ~0.4 tokens.
      jest.advanceTimersByTime(400);
      const stillBlocked = await runOnce(mw, mockReq());
      expect(stillBlocked.passed).toBe(false);
      expect(stillBlocked.res.statusCode).toBe(429);

      // Wait the rest of the interval; should now allow.
      jest.advanceTimersByTime(700);
      const allowed = await runOnce(mw, mockReq());
      expect(allowed.passed).toBe(true);
    } finally {
      jest.useRealTimers();
    }
  });

  test('refill caps at capacity — long idle periods do not over-credit', async () => {
    jest.useFakeTimers();
    try {
      jest.setSystemTime(new Date('2026-01-01T00:00:00Z'));
      const mw = createRateLimiter({
        capacity: 3,
        refillRate: 1,
        refillIntervalMs: 1000,
        key: () => 'cap',
        scope: 'cap',
      });

      // Drain to zero.
      for (let i = 0; i < 3; i++) {
        // eslint-disable-next-line no-await-in-loop
        await runOnce(mw, mockReq());
      }
      // Idle for an hour — way more than 3 seconds of refill time.
      jest.advanceTimersByTime(60 * 60 * 1000);
      // Bucket should be capped at 3 again — three more should pass, fourth blocks.
      for (let i = 0; i < 3; i++) {
        // eslint-disable-next-line no-await-in-loop
        const r = await runOnce(mw, mockReq());
        expect(r.passed).toBe(true);
      }
      const blocked = await runOnce(mw, mockReq());
      expect(blocked.passed).toBe(false);
      expect(blocked.res.statusCode).toBe(429);
    } finally {
      jest.useRealTimers();
    }
  });
});

// ---------------------------------------------------------------------------
// Per-identity isolation
// ---------------------------------------------------------------------------

describe('rate limiter — separate buckets per identity', () => {
  test('two different users do not share a bucket', async () => {
    const mw = createRateLimiter({
      capacity: 1,
      refillRate: 1,
      refillIntervalMs: 60_000,
      key: (req) => req.user!.sub,
      scope: 'isolation',
    });

    // Alice burns her token.
    const a1 = await runOnce(mw, mockReq({ userId: 'alice' }));
    expect(a1.passed).toBe(true);
    const a2 = await runOnce(mw, mockReq({ userId: 'alice' }));
    expect(a2.passed).toBe(false);
    expect(a2.res.statusCode).toBe(429);

    // Bob's bucket is untouched — should still pass.
    const b1 = await runOnce(mw, mockReq({ userId: 'bob' }));
    expect(b1.passed).toBe(true);
    expect(b1.res.statusCode).toBe(200);
  });

  test('userId fallback to req.ip when no user is authed', async () => {
    const mw = createRateLimiter({
      capacity: 1,
      refillRate: 1,
      refillIntervalMs: 60_000,
      // Mirror the real wiring: prefer userId, fall back to ip.
      key: (req) => req.user?.sub ?? req.ip ?? 'anon',
      scope: 'ip-fallback',
    });

    // Two different "unauthed" callers, distinguished only by IP.
    const r1 = await runOnce(
      mw,
      { headers: {}, user: undefined, ip: '10.0.0.1' } as unknown as Request,
    );
    const r2 = await runOnce(
      mw,
      { headers: {}, user: undefined, ip: '10.0.0.2' } as unknown as Request,
    );
    expect(r1.passed).toBe(true);
    expect(r2.passed).toBe(true);
    // Same IP again → blocked (capacity=1).
    const r1b = await runOnce(
      mw,
      { headers: {}, user: undefined, ip: '10.0.0.1' } as unknown as Request,
    );
    expect(r1b.passed).toBe(false);
    expect(r1b.res.statusCode).toBe(429);
  });
});

// ---------------------------------------------------------------------------
// In-memory fallback when no store override + Redis is null.
// ---------------------------------------------------------------------------

describe('rate limiter — in-memory fallback', () => {
  test('works without a store override or Redis (mocked null)', async () => {
    const mw = createRateLimiter({
      capacity: 2,
      refillRate: 1,
      refillIntervalMs: 60_000,
      key: () => 'fallback-user',
      scope: 'fallback',
      // No store override — exercises the InMemoryRateLimitStore path.
    });

    const r1 = await runOnce(mw, mockReq());
    const r2 = await runOnce(mw, mockReq());
    const r3 = await runOnce(mw, mockReq());
    expect(r1.passed).toBe(true);
    expect(r2.passed).toBe(true);
    expect(r3.passed).toBe(false);
    expect(r3.res.statusCode).toBe(429);
    expect(r3.res.headers['Retry-After']).toBeDefined();
  });

  test('_resetRateLimitMemFallback() clears state across cases', async () => {
    const mw1 = createRateLimiter({
      capacity: 1,
      refillRate: 1,
      refillIntervalMs: 60_000,
      key: () => 'reset-test',
      scope: 'reset',
    });
    const r1 = await runOnce(mw1, mockReq());
    expect(r1.passed).toBe(true);
    const r2 = await runOnce(mw1, mockReq());
    expect(r2.passed).toBe(false);

    // Reset clears the policy-cache so a new middleware on the same policy
    // gets a fresh store.
    _resetRateLimitMemFallback();

    const mw2 = createRateLimiter({
      capacity: 1,
      refillRate: 1,
      refillIntervalMs: 60_000,
      key: () => 'reset-test',
      scope: 'reset',
    });
    const r3 = await runOnce(mw2, mockReq());
    expect(r3.passed).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// DC RateLimitStore contract — sanity check on the underlying take() shape
// (matches the acceptance criterion: "a single take() decrements capacity
// by 1 and that take() at empty returns allowed:false with a retryAfterMs").
// ---------------------------------------------------------------------------

describe('rate limiter — DC RateLimitStore.take() contract', () => {
  test('single take() decrements remaining; empty take() returns allowed=false with retryAfterMs', async () => {
    const { InMemoryRateLimitStore } = await import('distributed-core');
    const store = new InMemoryRateLimitStore({
      capacity: 2,
      refillRate: 1,
      maxBurstSeconds: 2,
    });

    const first = await store.take('k', 1);
    expect(first.allowed).toBe(true);
    // The bucket refills continuously in wall-clock time, so a few ms
    // between take() calls adds a sliver of tokens — precision 2 keeps the
    // contract meaningful without being flaky on contended machines.
    expect(first.remaining).toBeCloseTo(1, 2);
    expect(first.retryAfterMs).toBe(0);

    const second = await store.take('k', 1);
    expect(second.allowed).toBe(true);
    expect(second.remaining).toBeCloseTo(0, 2);

    const third = await store.take('k', 1);
    expect(third.allowed).toBe(false);
    expect(third.retryAfterMs).toBeGreaterThan(0);
  });
});
