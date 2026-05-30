// platform-api/src/middleware/rateLimit.ts
//
// Per-key token-bucket rate limiter, backed by distributed-core's
// `RateLimitStore` family (DC v0.41+):
//
//   * `InMemoryRateLimitStore` (default) — per-process buckets. Same
//     posture as before: in a multi-replica deployment each replica
//     enforces the cap independently. Adequate for single-process
//     platform-api and for unit tests.
//
//   * `RedisRateLimitStore` (opt-in via `REDIS_RATE_LIMIT=true`) —
//     atomic single-`EVAL` per take through Redis. Multi-replica safe:
//     replicas share bucket state through the same key namespace.
//     Falls back to in-memory if `getRedisClient()` returns null
//     (transient Redis outage → degrade to per-process enforcement
//     rather than dropping the limiter).
//
// Algorithm — token bucket (implemented inside DC):
//   - Each identity starts with `capacity` tokens.
//   - `refillRate` tokens are added per `refillIntervalMs` (computed
//     fractionally — a request that arrives halfway through the
//     interval gets credit for half the refill). Tokens cap at
//     `capacity` (via the matching `maxBurstSeconds` derived below).
//   - A request consumes 1 token. If `take()` returns `allowed=false`,
//     respond 429 with a `Retry-After` header (seconds) and an
//     RFC-7807 problem-details JSON body matching the shape used by
//     `routes/pipelineWebhooks.ts` for 401s.
//
// The middleware does NOT throw on store hiccups — a transient
// failure degrades to "allow" rather than "deny" so a Redis outage
// doesn't take down the API.

import type { NextFunction, Request, RequestHandler, Response } from 'express';
import {
  InMemoryRateLimitStore,
  RedisRateLimitStore,
  type RateLimitStore,
  type RateLimitRedisClient,
} from 'distributed-core';
import { getRedisClient } from '../lib/redis-client';
import log from '../lib/logger';

// Re-export DC's store type so consumers / tests can `import type { RateLimitStore }`
// from the same module they already use.
export type { RateLimitStore } from 'distributed-core';

// ---------------------------------------------------------------------------
// Backing-store resolution. Stores are cached per (scope, policy) tuple so
// repeat `createRateLimiter` calls with the same effective policy share one
// bucket map (matches the legacy module-level `memBuckets` singleton).
// ---------------------------------------------------------------------------

interface PolicyKey {
  scope: string;
  capacity: number;
  refillRate: number;
  refillIntervalMs: number;
}

function policyCacheKey(k: PolicyKey): string {
  return `${k.scope}|${k.capacity}|${k.refillRate}|${k.refillIntervalMs}`;
}

const storeCache = new Map<string, Promise<RateLimitStore>>();

/**
 * Test-only: clear all cached backing stores so suites don't leak state
 * across tests. Kept under the legacy name so the dozen existing call sites
 * (pipelineDLQ.test.ts, pipelineTriggers.integration.test.ts, …) don't need
 * to be touched.
 */
export function _resetRateLimitMemFallback(): void {
  storeCache.clear();
}

/**
 * Adapt the platform-api `redis` (node-redis v5) client to DC's positional
 * `eval(script, numKeys, ...args)` + `del(key)` shape. Node-redis v5 uses
 * `eval(script, { keys, arguments })`; DC's `RateLimitStore.d.ts` calls out
 * the wrap-or-adapt step explicitly. The adapter is the smallest possible
 * surface — only the two methods DC actually invokes.
 */
function redisAdapter(client: Awaited<ReturnType<typeof getRedisClient>>): RateLimitRedisClient | null {
  if (!client) return null;
  return {
    async eval(script: string, numKeys: number, ...args: Array<string | number>): Promise<unknown> {
      const keys = args.slice(0, numKeys).map(String);
      const argv = args.slice(numKeys).map(String);
      return (client as unknown as {
        eval: (s: string, opts: { keys: string[]; arguments: string[] }) => Promise<unknown>;
      }).eval(script, { keys, arguments: argv });
    },
    async del(key: string): Promise<unknown> {
      return (client as unknown as { del: (k: string) => Promise<unknown> }).del(key);
    },
  };
}

function redisRateLimitOn(): boolean {
  const v = process.env['REDIS_RATE_LIMIT'];
  return v === 'true' || v === '1';
}

async function buildBackingStore(k: PolicyKey): Promise<RateLimitStore> {
  // DC's RateLimitStore takes tokens/sec; we expose tokens-per-interval.
  const tokensPerSecond = (k.refillRate * 1000) / k.refillIntervalMs;
  // Tune `maxBurstSeconds` so that the effective max tokens equals
  // `capacity` (DC takes the min of `capacity` and `refillRate * maxBurstSeconds`).
  const maxBurstSeconds = Math.max(1, Math.ceil(k.capacity / tokensPerSecond));

  if (redisRateLimitOn()) {
    try {
      const adapter = redisAdapter(await getRedisClient());
      if (adapter) {
        return new RedisRateLimitStore({
          client: adapter,
          capacity: k.capacity,
          refillRate: tokensPerSecond,
          maxBurstSeconds,
          keyPrefix: 'ratelimit',
        });
      }
      log.warn({ scope: k.scope }, 'REDIS_RATE_LIMIT=true but redis client unavailable; using in-memory store');
    } catch (err) {
      log.warn({ err: (err as Error).message, scope: k.scope }, 'redis store init failed; falling back to in-memory');
    }
  }

  return new InMemoryRateLimitStore({
    capacity: k.capacity,
    refillRate: tokensPerSecond,
    maxBurstSeconds,
  });
}

async function resolveStore(k: PolicyKey, override?: RateLimitStore | null): Promise<RateLimitStore> {
  if (override) return override;
  const cacheKey = policyCacheKey(k);
  let pending = storeCache.get(cacheKey);
  if (!pending) {
    pending = buildBackingStore(k);
    storeCache.set(cacheKey, pending);
  }
  return pending;
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
   * Optional logical scope to namespace the bucket — lets multiple limiters
   * share the same backing store without colliding (e.g. `pipeline-trigger`
   * vs `pipeline-cancel`). Defaults to `'default'`.
   */
  scope?: string;
  /**
   * Test-only hook to inject a fake store. When omitted, the middleware
   * lazily resolves the policy-cached InMemoryRateLimitStore (or
   * RedisRateLimitStore when `REDIS_RATE_LIMIT=true`).
   */
  store?: RateLimitStore | null;
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
  const policy: PolicyKey = {
    scope,
    capacity: opts.capacity,
    refillRate: opts.refillRate,
    refillIntervalMs: opts.refillIntervalMs,
  };

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
    const cacheKey = `${scope}:${identity}`;

    let store: RateLimitStore;
    try {
      store = await resolveStore(policy, opts.store);
    } catch (err) {
      log.warn({ err: (err as Error).message, scope, identity }, 'rateLimit failed to resolve store; failing open');
      next();
      return;
    }

    let result;
    try {
      result = await store.take(cacheKey, 1);
    } catch (err) {
      log.warn({ err: (err as Error).message, scope, identity, cacheKey }, 'rateLimit store.take failed; failing open');
      next();
      return;
    }

    if (!result.allowed) {
      const retryAfterSec = Math.max(1, Math.ceil(result.retryAfterMs / 1000));
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

    next();
  };
}

// ---------------------------------------------------------------------------
// Preconfigured limiter for pipeline READ endpoints (GET /api/pipelines/*).
//
// Policy: 200 req / min / user, bursting up to 200. Keyed by Cognito sub with
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
