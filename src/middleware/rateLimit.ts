// social-api/src/middleware/rateLimit.ts
//
// Per-key token-bucket rate limiter, structured to match
// `middleware/idempotency.ts`:
//
//   * Redis-backed when `getRedisClient()` returns a live connection — the
//     bucket state (`tokens`, `lastRefill`) is stored under a single key per
//     identity and refilled lazily on each request. Multi-replica safe ONLY
//     in Redis mode (replicas share the bucket state through the same key
//     space).
//
//   * Falls back to a per-process `Map<string, { tokens, lastRefill }>` when
//     Redis is unavailable so unit tests and dev work without infra. NOTE:
//     this fallback is per-process; in a multi-replica deployment each
//     replica would maintain its own bucket and the effective burst rate
//     scales with replica count. Operate with Redis in production.
//
// Algorithm — token bucket:
//   - Each identity starts with `capacity` tokens.
//   - `refillRate` tokens are added every `refillIntervalMs` (computed
//     fractionally — a request that arrives halfway through the interval
//     gets credit for half the refill). Tokens cap at `capacity`.
//   - A request consumes 1 token. If `tokens < 1`, return 429 with a
//     `Retry-After` header set to ceil(seconds until next whole token) and
//     an RFC-7807 problem-details JSON body matching the shape used by
//     `routes/pipelineWebhooks.ts` for 401s.
//
// The middleware does NOT throw on Redis hiccups — a transient Redis error
// degrades to "allow" rather than "deny" so a Redis outage doesn't take down
// the API. Failures are logged via structured logging.

import type { NextFunction, Request, RequestHandler, Response } from 'express';
import { getRedisClient } from '../lib/redis-client';
import log from '../lib/logger';

// ---------------------------------------------------------------------------
// Storage abstraction — same shape as the idempotency middleware so the two
// stay easy to compare. Tests can inject a fake without booting Redis.
// ---------------------------------------------------------------------------

/** Persisted bucket state. Both fields are required. */
export interface BucketState {
  tokens: number;
  lastRefill: number; // ms epoch
}

/** Pluggable store for bucket state. Tests inject a fake; prod uses Redis. */
export interface RateLimitStore {
  get(key: string): Promise<BucketState | null>;
  /** TTL is informational — Redis sets EXPIRE so abandoned buckets reap. */
  set(key: string, state: BucketState, ttlSeconds: number): Promise<void>;
}

// Per-process fallback used when Redis is unavailable.
const memBuckets = new Map<string, BucketState>();

function memStore(): RateLimitStore {
  return {
    async get(key) {
      return memBuckets.get(key) ?? null;
    },
    async set(key, state, _ttlSeconds) {
      memBuckets.set(key, { ...state });
    },
  };
}

/**
 * Test-only: clear the in-memory fallback so suites don't leak state across
 * tests. Has no effect on Redis (which has its own TTL).
 */
export function _resetRateLimitMemFallback(): void {
  memBuckets.clear();
}

async function resolveStore(override?: RateLimitStore | null): Promise<RateLimitStore> {
  if (override) return override;
  const client = await getRedisClient();
  if (!client) return memStore();
  return {
    async get(key) {
      const raw = await client.get(key);
      if (typeof raw !== 'string' || raw.length === 0) return null;
      try {
        const parsed = JSON.parse(raw) as Partial<BucketState>;
        if (
          typeof parsed.tokens !== 'number' ||
          typeof parsed.lastRefill !== 'number' ||
          !Number.isFinite(parsed.tokens) ||
          !Number.isFinite(parsed.lastRefill)
        ) {
          return null;
        }
        return { tokens: parsed.tokens, lastRefill: parsed.lastRefill };
      } catch {
        return null;
      }
    },
    async set(key, state, ttlSeconds) {
      await client.set(key, JSON.stringify(state), { EX: ttlSeconds });
    },
  };
}

// ---------------------------------------------------------------------------
// Public API.
// ---------------------------------------------------------------------------

export interface RateLimitOptions {
  /** Maximum tokens the bucket holds (= burst size). */
  capacity: number;
  /** Tokens added per `refillIntervalMs`. */
  refillRate: number;
  /** Refill cadence in ms — refill is computed fractionally. */
  refillIntervalMs: number;
  /** Identity extractor. Returns the bucket key (typically userId or IP). */
  key: (req: Request) => string;
  /**
   * Optional logical scope to namespace the Redis key — lets multiple
   * limiters share the same store without colliding (e.g. `pipeline-trigger`
   * vs `pipeline-cancel`). Defaults to `'default'`.
   */
  scope?: string;
  /**
   * Test-only hook to inject a fake store. When omitted, the middleware
   * lazily resolves Redis (and falls back to the in-memory map).
   */
  store?: RateLimitStore | null;
  /**
   * Optional clock injection for deterministic tests. Defaults to
   * `Date.now()`. Note: jest fake timers also move `Date.now`, so most tests
   * don't need this.
   */
  now?: () => number;
}

/**
 * RFC-7807 problem-details body returned on 429. Shape matches the 401 body
 * used by `routes/pipelineWebhooks.ts` so callers can parse both uniformly.
 */
export interface RateLimitedProblem {
  type: string;
  title: string;
  status: 429;
  detail: string;
  retryAfterSec: number;
}

/**
 * Build a token-bucket rate limiter middleware. See file header for the
 * full algorithm + storage/multi-replica notes.
 */
export function createRateLimiter(opts: RateLimitOptions): RequestHandler {
  if (!Number.isFinite(opts.capacity) || opts.capacity <= 0) {
    throw new Error('createRateLimiter: capacity must be a positive number');
  }
  if (!Number.isFinite(opts.refillRate) || opts.refillRate <= 0) {
    throw new Error('createRateLimiter: refillRate must be a positive number');
  }
  if (!Number.isFinite(opts.refillIntervalMs) || opts.refillIntervalMs <= 0) {
    throw new Error('createRateLimiter: refillIntervalMs must be > 0');
  }

  const scope = opts.scope ?? 'default';
  const now = opts.now ?? ((): number => Date.now());

  // Tokens-per-millisecond rate. Used for both lazy refill and the
  // Retry-After computation.
  const tokensPerMs = opts.refillRate / opts.refillIntervalMs;

  // TTL for abandoned buckets — set generously so we don't churn Redis
  // writes for every tick, but short enough that idle keys reap. Two refill
  // intervals beyond a full bucket is plenty: after that, a new bucket would
  // start at full capacity anyway.
  const ttlSeconds = Math.max(
    60,
    Math.ceil((opts.refillIntervalMs * (opts.capacity / opts.refillRate) * 2) / 1000),
  );

  return async function rateLimitMiddleware(
    req: Request,
    res: Response,
    next: NextFunction,
  ): Promise<void> {
    const identity = opts.key(req);
    if (!identity) {
      // No identity — fail open. Misconfigured key fns shouldn't block traffic.
      next();
      return;
    }
    const cacheKey = `ratelimit:${scope}:${identity}`;

    let store: RateLimitStore;
    try {
      store = await resolveStore(opts.store);
    } catch (err) {
      log.warn({ err: (err as Error).message, scope: opts.scope, identity }, 'rateLimit failed to resolve store; failing open');
      next();
      return;
    }

    let state: BucketState | null;
    try {
      state = await store.get(cacheKey);
    } catch (err) {
      log.warn({ err: (err as Error).message, scope: opts.scope, identity, cacheKey }, 'rateLimit store.get failed; failing open');
      next();
      return;
    }

    const t = now();
    if (!state) {
      state = { tokens: opts.capacity, lastRefill: t };
    } else {
      // Lazy refill — credit the bucket for time elapsed since lastRefill.
      const elapsedMs = Math.max(0, t - state.lastRefill);
      const refilled = elapsedMs * tokensPerMs;
      state = {
        tokens: Math.min(opts.capacity, state.tokens + refilled),
        lastRefill: t,
      };
    }

    if (state.tokens < 1) {
      // Compute time until the bucket holds one whole token. `tokensPerMs`
      // is positive (validated at construction), so this is finite.
      const msUntilNextToken = (1 - state.tokens) / tokensPerMs;
      const retryAfterSec = Math.max(1, Math.ceil(msUntilNextToken / 1000));

      // Persist the (refilled) state so concurrent requests see the same
      // negative-ish budget and don't all race past it.
      try {
        await store.set(cacheKey, state, ttlSeconds);
      } catch (err) {
        log.warn({ err: (err as Error).message, scope: opts.scope, identity, cacheKey }, 'rateLimit store.set failed (continuing)');
      }

      const problem: RateLimitedProblem = {
        type: 'about:blank',
        title: 'Too Many Requests',
        status: 429,
        detail: `Rate limit exceeded for scope '${scope}'. Retry after ${retryAfterSec}s.`,
        retryAfterSec,
      };
      res.setHeader('Retry-After', String(retryAfterSec));
      res.status(429).json(problem);
      return;
    }

    // Consume one token + persist.
    state.tokens -= 1;
    try {
      await store.set(cacheKey, state, ttlSeconds);
    } catch (err) {
      log.warn({ err: (err as Error).message, scope: opts.scope, identity, cacheKey }, 'rateLimit store.set failed (allowing request)');
    }
    next();
  };
}

// ---------------------------------------------------------------------------
// Preconfigured limiter for pipeline READ endpoints (GET /api/pipelines/*).
//
// Policy: 60 req / min / user, bursting up to 60. Keyed by Cognito sub with
// IP fallback for the unauthed edge cases (shouldn't happen behind
// `requireAuth`, but defensive). Scope `pipeline:read` namespaces the bucket
// independently from the trigger / cancel / approval limiters so a heavy
// dashboard doesn't starve write traffic.
//
// Wired in `app.ts` as a method-aware mount on `/api/pipelines` so it ONLY
// rate-limits GETs — POST/PUT/DELETE/PATCH bypass it. The webhook path
// `/hooks/pipeline/*` is mounted before `requireAuth` and is NOT covered by
// this limiter (it has its own auth + budget concerns).
// ---------------------------------------------------------------------------

/**
 * Build a rate limiter middleware tuned for pipeline READ endpoints.
 *
 * @param storeOverride Optional store injection — tests pass a fake to avoid
 *                      cross-test bleed via the in-memory fallback.
 */
export function pipelineReadRateLimit(
  storeOverride?: RateLimitStore | null,
): RequestHandler {
  return createRateLimiter({
    // 200 / min — high enough for 500ms polling during active runs.
    capacity: 200,
    refillRate: 200,
    refillIntervalMs: 60_000,
    scope: 'pipeline:read',
    key: (req) => req.user?.sub ?? req.ip ?? 'anon',
    store: storeOverride ?? null,
  });
}

// ---------------------------------------------------------------------------
// Preconfigured limiter for pipeline destructive WRITE endpoints — currently
// `POST /api/pipelines/dlq/redrive` and `POST /api/pipelines/dlq/purge`.
//
// Tighter budget than the read limiter: 10 writes / min / user, no burst
// beyond the per-minute budget. The intent is operator-grade safety — a
// fat-finger script that POSTs `{ids: [...]}` in a tight loop will 429
// well before re-fanning hundreds of failed events across the bus. Trigger,
// cancel, and approval routes have their own per-route limiters with their
// own budgets; this one is namespaced (`pipeline:write`) so it does not
// share a bucket with them.
//
// Mounted at the router level on the DLQ POST routes (not via app.ts mount)
// so it ships co-located with the destructive endpoints.
// ---------------------------------------------------------------------------

/**
 * Build a rate limiter middleware tuned for pipeline destructive writes
 * (DLQ redrive / purge).
 *
 * @param storeOverride Optional store injection — tests pass a fake to avoid
 *                      cross-test bleed via the in-memory fallback.
 */
export function pipelineWriteRateLimit(
  storeOverride?: RateLimitStore | null,
): RequestHandler {
  return createRateLimiter({
    capacity: 10,
    refillRate: 10,
    refillIntervalMs: 60_000,
    scope: 'pipeline:write',
    key: (req) => req.user?.sub ?? req.ip ?? 'anon',
    store: storeOverride ?? null,
  });
}
