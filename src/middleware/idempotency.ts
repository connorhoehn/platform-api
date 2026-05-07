// social-api/src/middleware/idempotency.ts
//
// Idempotency-Key middleware + in-memory cache helper.
//
// Two surfaces are exported:
//
//  1. `createIdempotencyCache<T>(ttlMs?)` — a small in-memory store used by
//     existing call sites (e.g. `pipelineTriggers.ts`) that pre-date the Redis
//     middleware. Kept for backwards compatibility; behavior is unchanged.
//
//  2. `idempotency({ scope, ttlSeconds?, redis? })` — Express middleware that
//     transparently dedups a POST/PUT/DELETE handler when callers provide the
//     `Idempotency-Key` request header.
//
//     Storage: Redis (24h TTL by default — `IDEMPOTENCY_TTL_SECONDS`); falls
//     back to a per-process in-memory store when Redis is unavailable so unit
//     tests and dev work without infra. The cached entry holds the response
//     status, headers, body and a SHA-256 of the request body so we can:
//
//       * REPLAY: same key + matching body hash → return the cached response
//                 verbatim with `X-Idempotent-Replay: true`
//       * CONFLICT: same key but a *different* body hash → 409 Conflict
//                   (RFC draft-ietf-httpapi-idempotency-key-header)
//
//     The middleware is keyed by `${scope}:${userId ?? 'anon'}:${header}` so
//     the same Idempotency-Key under different users / different routes never
//     collides.
//
// Phase 4 / multi-replica: Redis is the source of truth — replicas share the
// dedup window. The in-memory fallback is still a single-process cache, which
// is fine for tests but means replays may "miss" across replicas without
// Redis. Operate with Redis in production.

import { createHash } from 'crypto';
import type { NextFunction, Request, RequestHandler, Response } from 'express';
import { getRedisClient } from '../lib/redis-client';
import log from '../lib/logger';

// --- back-compat: in-memory cache ------------------------------------------

export interface IdempotencyCacheEntry<T> {
  response: T;
  cachedAt: number;
}

export interface IdempotencyCache<T> {
  get(key: string): IdempotencyCacheEntry<T> | null;
  set(key: string, response: T): void;
  sweep(): void;
}

const DEFAULT_LEGACY_TTL_MS = 5 * 60 * 1000; // 5 minutes — preserved

export function createIdempotencyCache<T>(
  ttlMs: number = DEFAULT_LEGACY_TTL_MS,
): IdempotencyCache<T> {
  const store = new Map<string, IdempotencyCacheEntry<T>>();

  function sweep(): void {
    const cutoff = Date.now() - ttlMs;
    for (const [k, entry] of store) {
      if (entry.cachedAt < cutoff) {
        store.delete(k);
      }
    }
  }

  return {
    get(key) {
      sweep();
      const entry = store.get(key);
      if (!entry) return null;
      if (Date.now() - entry.cachedAt >= ttlMs) {
        store.delete(key);
        return null;
      }
      return entry;
    },
    set(key, response) {
      sweep();
      store.set(key, { response, cachedAt: Date.now() });
    },
    sweep,
  };
}

// --- Redis-backed middleware -----------------------------------------------

/**
 * Cached HTTP response shape persisted under an Idempotency-Key. Stored as
 * JSON in Redis (or in-memory fallback). `bodyHash` is a SHA-256 of the
 * canonical request body — used to detect "same key, different body" attacks
 * and return 409.
 */
export interface CachedResponse {
  status: number;
  headers: Record<string, string>;
  body: unknown;
  bodyHash: string;
  cachedAt: number; // ms epoch — informational; TTL is enforced by Redis EXPIRE
}

const DEFAULT_TTL_SECONDS = 60 * 60 * 24; // 24h per spec

/**
 * Subset of the redis client API that the middleware uses. Lets tests inject
 * a fake without pulling in redis-mock or constructing a real connection.
 */
export interface IdempotencyStore {
  get(key: string): Promise<string | null>;
  /** SET with PX/EX TTL semantics; mirrored after node-redis v4 SET options. */
  set(key: string, value: string, opts: { EX: number }): Promise<unknown>;
}

export interface IdempotencyOptions {
  /** Logical scope — e.g. `pipeline-trigger`, `pipeline-approval`. Required. */
  scope: string;
  /** TTL for cached responses in seconds. Default 24h. */
  ttlSeconds?: number;
  /**
   * Optional store override. When omitted, the middleware lazily resolves the
   * shared `getRedisClient()` from `lib/redis-client`. When that returns null
   * (Redis unavailable), it transparently falls back to a per-process map so
   * tests and local dev still work.
   */
  store?: IdempotencyStore | null;
}

// Per-process fallback used when Redis is unavailable. Keyed identically to
// Redis so a later Redis recovery doesn't double-serve the same Idempotency-Key.
interface MemEntry {
  value: string;
  expiresAt: number;
}
const memFallback = new Map<string, MemEntry>();

function memStore(): IdempotencyStore {
  return {
    async get(key) {
      const entry = memFallback.get(key);
      if (!entry) return null;
      if (entry.expiresAt < Date.now()) {
        memFallback.delete(key);
        return null;
      }
      return entry.value;
    },
    async set(key, value, opts) {
      memFallback.set(key, {
        value,
        expiresAt: Date.now() + opts.EX * 1000,
      });
    },
  };
}

/**
 * Test-only: clear the in-memory fallback so suites don't leak state across
 * tests. Has no effect on Redis (which has its own TTL). Exported but not
 * documented for production use.
 */
export function _resetIdempotencyMemFallback(): void {
  memFallback.clear();
}

async function resolveStore(override?: IdempotencyStore | null): Promise<IdempotencyStore> {
  if (override) return override;
  const client = await getRedisClient();
  if (!client) return memStore();
  return {
    async get(key) {
      const v = await client.get(key);
      return typeof v === 'string' ? v : null;
    },
    async set(key, value, opts) {
      await client.set(key, value, { EX: opts.EX });
    },
  };
}

/**
 * Hash the request body deterministically. We stringify with sorted keys so
 * `{a:1, b:2}` and `{b:2, a:1}` produce the same hash — clients commonly
 * round-trip through different JSON serializers.
 */
export function hashBody(body: unknown): string {
  const canonical = stableStringify(body ?? null);
  return createHash('sha256').update(canonical).digest('hex');
}

function stableStringify(value: unknown): string {
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) {
    return '[' + value.map(stableStringify).join(',') + ']';
  }
  const keys = Object.keys(value as Record<string, unknown>).sort();
  return (
    '{' +
    keys
      .map((k) => JSON.stringify(k) + ':' + stableStringify((value as Record<string, unknown>)[k]))
      .join(',') +
    '}'
  );
}

/**
 * Express middleware: implements Idempotency-Key dedup with Redis storage.
 *
 * Behavior:
 *   - No header → next() (passthrough)
 *   - Header + cache hit + matching body → replay cached response with
 *     `X-Idempotent-Replay: true`
 *   - Header + cache hit + DIFFERENT body → 409 Conflict
 *   - Header + cache miss → wrap res.json/res.status/res.send and persist on
 *     successful (2xx) completion
 */
export function idempotency(opts: IdempotencyOptions): RequestHandler {
  const ttlSeconds = opts.ttlSeconds ?? DEFAULT_TTL_SECONDS;
  const scope = opts.scope;

  return async function idempotencyMiddleware(
    req: Request,
    res: Response,
    next: NextFunction,
  ): Promise<void> {
    const rawHeader = req.headers['idempotency-key'];
    const idempotencyKey = Array.isArray(rawHeader) ? rawHeader[0] : rawHeader;
    if (!idempotencyKey) {
      next();
      return;
    }

    const userId = req.user?.sub ?? 'anon';
    const cacheKey = `idem:${scope}:${userId}:${idempotencyKey}`;
    const requestHash = hashBody(req.body);

    let store: IdempotencyStore;
    try {
      store = await resolveStore(opts.store);
    } catch (err) {
      // If we can't even resolve a store, surface as 500 — the caller sent
      // an Idempotency-Key and we MUST honor it (or refuse).
      next(err);
      return;
    }

    let existing: string | null = null;
    try {
      existing = await store.get(cacheKey);
    } catch (err) {
      next(err);
      return;
    }

    if (existing) {
      let cached: CachedResponse;
      try {
        cached = JSON.parse(existing) as CachedResponse;
      } catch {
        // Corrupt entry — treat as miss and overwrite below.
        cached = null as unknown as CachedResponse;
      }
      if (cached) {
        if (cached.bodyHash !== requestHash) {
          res
            .status(409)
            .json({
              error: 'idempotency_key_conflict',
              message:
                'Idempotency-Key was reused with a different request body. ' +
                'Use a fresh key for new requests.',
            });
          return;
        }
        // Replay
        for (const [name, value] of Object.entries(cached.headers ?? {})) {
          res.setHeader(name, value);
        }
        res.setHeader('X-Idempotent-Replay', 'true');
        res.status(cached.status);
        if (cached.body === undefined || cached.body === null) {
          res.end();
        } else {
          res.json(cached.body);
        }
        return;
      }
    }

    // Cache miss — wrap response so we capture it on success.
    const captured: { status: number; body: unknown; headers: Record<string, string> } = {
      status: 200,
      body: undefined,
      headers: {},
    };

    const origStatus = res.status.bind(res);
    const origJson = res.json.bind(res);
    const origSetHeader = res.setHeader.bind(res);
    const origEnd = res.end.bind(res);

    res.status = (code: number) => {
      captured.status = code;
      return origStatus(code);
    };
    res.setHeader = ((name: string, value: number | string | readonly string[]) => {
      // Only persist string-y headers so we can JSON-roundtrip them.
      if (typeof value === 'string') {
        captured.headers[String(name)] = value;
      } else if (typeof value === 'number') {
        captured.headers[String(name)] = String(value);
      }
      return origSetHeader(name, value);
    }) as Response['setHeader'];
    res.json = ((body: unknown) => {
      captured.body = body;
      // Fire-and-forget the cache write before returning the response. We
      // intentionally don't await here — failing to cache shouldn't block the
      // client; the next replay will simply miss.
      void persist();
      return origJson(body);
    }) as Response['json'];
    res.end = ((...args: unknown[]) => {
      // For 204 / no-body responses we still want to remember status+headers.
      if (captured.body === undefined) {
        void persist();
      }
      return (origEnd as (...a: unknown[]) => Response)(...args);
    }) as Response['end'];

    let persisted = false;
    async function persist(): Promise<void> {
      if (persisted) return;
      persisted = true;
      // Only cache successful responses. Errors (4xx/5xx) should be retryable.
      if (captured.status < 200 || captured.status >= 300) return;
      const entry: CachedResponse = {
        status: captured.status,
        headers: captured.headers,
        body: captured.body ?? null,
        bodyHash: requestHash,
        cachedAt: Date.now(),
      };
      try {
        await store.set(cacheKey, JSON.stringify(entry), { EX: ttlSeconds });
      } catch (err) {
        // Best-effort — log but don't fail the request.
        log.warn({ err: (err as Error).message, scope, cacheKey }, 'idempotency failed to persist cache entry');
      }
    }

    next();
  };
}
