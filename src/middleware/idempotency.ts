// platform-api/src/middleware/idempotency.ts
//
// Idempotency-Key middleware + in-memory cache helper.
//
// NFR #97 — what is held where, and why (the Redis here is `noeviction` but
// has no persistence: every restart drops everything, NFR #88):
//
//   * Idempotency records (this file: pipeline-trigger, pipeline-cancel,
//     pipeline-approval, pipeline-replay, pipeline-resume-from-step,
//     pipeline-retry-from-node) — losing one lets a retried request start a
//     second run / approve twice, i.e. a duplicate side effect, so they are
//     DynamoDB conditional-write claims with a TTL (the #92 pattern), not Redis.
//   * Judge budget counters (`JudgeBudgetGuard`, INCRBYFLOAT, 48 h) — losing
//     one resets a spend cap once; nothing runs twice, so they stay in Redis.
//   * Feed-view / view-base snapshots — pure caches, re-created on a miss, so
//     they live on the allkeys-lru cache Redis instance (`REDIS_CACHE_URL`).
//
// Two surfaces are exported:
//
//  1. `createIdempotencyCache<T>(ttlMs?)` — a small in-memory store used by
//     existing call sites that pre-date the middleware. Kept for backwards
//     compatibility; behavior is unchanged.
//
//  2. `idempotency({ scope, ttlSeconds? })` — Express middleware that dedups a
//     POST/PUT/DELETE handler when callers provide the `Idempotency-Key`
//     request header. One request → one claim, across retries and replicas:
//
//       * CLAIM:    a conditional put of 'pending' (only when no live claim
//                   exists) before the handler runs; pending claims expire
//                   after PENDING_TTL_SECONDS so a crashed handler does not
//                   block the key for a day
//       * COMMIT:   a 2xx response replaces 'pending' with the cached response
//                   (status, headers, body, SHA-256 of the request body) for
//                   `ttlSeconds` (24 h by default)
//       * RELEASE:  a non-2xx response, a throw, or a connection that closes
//                   before a 2xx drops the pending claim so a retry may run
//       * REPLAY:   same key + matching body hash → the cached response
//                   verbatim with `X-Idempotent-Replay: true`
//       * IN FLIGHT: same key while the first request is still pending →
//                   409 `idempotency_in_progress` + `Retry-After: 2`
//       * CONFLICT: same key but a *different* body hash → 409 Conflict
//                   (RFC draft-ietf-httpapi-idempotency-key-header)
//
//     The middleware is keyed by `idem:${scope}:${userId ?? 'anon'}:${header}`
//     so the same Idempotency-Key under different users / routes never collides.
//
//     Storage: DynamoDB table `idempotency-claims` (pk `claimKey`, TTL
//     attribute `expiresAt`, created on first use) through the generalized
//     `DynamoClaimStore`. When DynamoDB cannot be reached, a request that
//     carries an Idempotency-Key is refused with 503
//     `idempotency-store-unavailable` — it fails closed (like #92) rather than
//     running a side effect it could not make idempotent. A per-process memory
//     store is used only when `IDEMPOTENCY_STORE=memory` or `NODE_ENV=test`.

import { createHash } from 'crypto';
import type { NextFunction, Request, RequestHandler, Response } from 'express';
import { type ClaimStore, DynamoClaimStore, MemoryClaimStore } from '../deck/deckReviseRequests';
import { tableName } from '../lib/ddb-table-name';
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

// --- Claim-backed middleware -----------------------------------------------

/**
 * Cached HTTP response shape persisted under an Idempotency-Key. Stored as
 * JSON in the claim. `bodyHash` is a SHA-256 of the canonical request body —
 * used to detect "same key, different body" and return 409.
 */
export interface CachedResponse {
  status: number;
  headers: Record<string, string>;
  body: unknown;
  bodyHash: string;
  cachedAt: number; // ms epoch — informational; expiry is the claim's TTL
}

const DEFAULT_TTL_SECONDS = 60 * 60 * 24; // 24h per spec
/** How long a claim may sit at 'pending' (handler running, or crashed) before a retry may take it over. */
export const PENDING_TTL_SECONDS = 120;
export const IDEMPOTENCY_CLAIMS_TABLE = tableName('idempotency-claims');
const PENDING = 'pending';

/**
 * The claim operations the middleware uses. Semantic, so a store cannot get
 * the conditions wrong; tests inject a fake.
 */
export interface IdempotencyStore {
  /** The live claim's value ('pending' or the cached response JSON), or null. */
  get(key: string): Promise<string | null>;
  /** Create the claim as 'pending' only when no live one exists; true when this caller now holds it. */
  claim(key: string, ttlSeconds: number): Promise<boolean>;
  /** Replace 'pending' with the cached response (only while still pending). */
  commit(key: string, value: string, ttlSeconds: number): Promise<void>;
  /** Drop the claim only while it is still pending. */
  release(key: string): Promise<void>;
}

export interface IdempotencyOptions {
  /** Logical scope — e.g. `pipeline-trigger`, `pipeline-approval`. Required. */
  scope: string;
  /** TTL for committed (cached) responses in seconds. Default 24h. */
  ttlSeconds?: number;
  /**
   * Optional store override for this middleware instance. When omitted, the
   * process-wide store is used (see `setIdempotencyStore` and the selection
   * rules in the header comment).
   */
  store?: IdempotencyStore | null;
}

/** A ClaimStore (deck-revise's contract) seen through the middleware's vocabulary. */
function fromClaimStore(claims: ClaimStore): IdempotencyStore {
  return {
    get: (key) => claims.get(key),
    claim: (key, ttlSeconds) => claims.create(key, PENDING, ttlSeconds),
    commit: (key, value, ttlSeconds) => claims.commit(key, value, ttlSeconds),
    release: (key) => claims.release(key),
  };
}

// Per-process store: tests and `IDEMPOTENCY_STORE=memory` only — one
// replica's worth of idempotency, nothing survives the process.
let memFallback = new MemoryClaimStore();

/**
 * Test-only: clear the in-memory store so suites don't leak state across
 * tests. Has no effect on DynamoDB (which has its own TTL).
 */
export function _resetIdempotencyMemFallback(): void {
  memFallback = new MemoryClaimStore();
}

let durable: IdempotencyStore | null = null;
let override: IdempotencyStore | null = null;

/** Tests / operators: the store every middleware instance uses (null = back to the selection rules). */
export function setIdempotencyStore(next: IdempotencyStore | null): void {
  override = next;
}

function useMemoryStore(): boolean {
  return process.env.IDEMPOTENCY_STORE === 'memory' || process.env.NODE_ENV === 'test';
}

function resolveStore(local?: IdempotencyStore | null): IdempotencyStore {
  if (local) return local;
  if (override) return override;
  if (useMemoryStore()) return fromClaimStore(memFallback);
  durable ??= fromClaimStore(new DynamoClaimStore(undefined, undefined, IDEMPOTENCY_CLAIMS_TABLE));
  return durable;
}

/**
 * Canonical SHA-256 of a request body. Key order is normalised so `{a,b}`
 * and `{b,a}` hash identically; arrays keep their order.
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

function parseCached(raw: string): CachedResponse | null {
  try {
    const parsed = JSON.parse(raw) as CachedResponse;
    return parsed && typeof parsed === 'object' && typeof parsed.status === 'number' ? parsed : null;
  } catch {
    return null;
  }
}

/**
 * What an existing claim means for this request: replay it, refuse it, or
 * (a corrupt value) let the handler run again.
 */
function answerFromExisting(existing: string, requestHash: string, res: Response): boolean {
  if (existing === PENDING) {
    res.setHeader('Retry-After', '2');
    res.status(409).json({
      error: 'idempotency_in_progress',
      message: 'A request with this Idempotency-Key is still being processed. Retry shortly.',
    });
    return true;
  }
  const cached = parseCached(existing);
  if (!cached) return false;
  if (cached.bodyHash !== requestHash) {
    res.status(409).json({
      error: 'idempotency_key_conflict',
      message:
        'Idempotency-Key was reused with a different request body. ' +
        'Use a fresh key for new requests.',
    });
    return true;
  }
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
  return true;
}

/**
 * Express middleware. See the header comment for the contract.
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
    const store = resolveStore(opts.store);

    // Claim before running anything: the claim is the guarantee. A retry that
    // lands while the first request is still running, or on another replica,
    // sees the pending row and is told to come back.
    let claimed = false;
    try {
      const existing = await store.get(cacheKey);
      if (existing !== null && answerFromExisting(existing, requestHash, res)) return;
      claimed = await store.claim(cacheKey, PENDING_TTL_SECONDS);
      if (!claimed) {
        const raced = await store.get(cacheKey);
        if (raced !== null && answerFromExisting(raced, requestHash, res)) return;
        // Someone else's claim vanished between our two reads (their handler
        // failed and released); take it now rather than bounce the caller.
        claimed = await store.claim(cacheKey, PENDING_TTL_SECONDS);
        if (!claimed) {
          res.setHeader('Retry-After', '2');
          res.status(409).json({
            error: 'idempotency_in_progress',
            message: 'A request with this Idempotency-Key is still being processed. Retry shortly.',
          });
          return;
        }
      }
    } catch (err) {
      log.warn({ err: (err as Error).message, scope, cacheKey }, 'idempotency claim store unavailable');
      res.status(503).json({
        error: 'idempotency-store-unavailable',
        message: 'The idempotency store is unreachable; the request was not run. Retry with the same key.',
      });
      return;
    }

    // Capture the outgoing response so a 2xx can be committed and anything
    // else releases the claim.
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
      if (typeof value === 'string') {
        captured.headers[String(name)] = value;
      } else if (typeof value === 'number') {
        captured.headers[String(name)] = String(value);
      }
      return origSetHeader(name, value);
    }) as Response['setHeader'];
    res.json = ((body: unknown) => {
      captured.body = body;
      void settle();
      return origJson(body);
    }) as Response['json'];
    res.end = ((...args: unknown[]) => {
      if (captured.body === undefined) {
        void settle();
      }
      return (origEnd as (...a: unknown[]) => Response)(...args);
    }) as Response['end'];
    // A socket that closes before the handler answered (client gave up, the
    // process is shutting down): the side effect may or may not have happened,
    // so the claim is released and the caller's retry decides.
    if (typeof (res as { once?: unknown }).once === 'function') {
      res.once('close', () => { void settle(); });
    }

    let settled = false;
    async function settle(): Promise<void> {
      if (settled) return;
      settled = true;
      const committed = captured.status >= 200 && captured.status < 300;
      try {
        if (committed) {
          const entry: CachedResponse = {
            status: captured.status,
            headers: captured.headers,
            body: captured.body ?? null,
            bodyHash: requestHash,
            cachedAt: Date.now(),
          };
          await store.commit(cacheKey, JSON.stringify(entry), ttlSeconds);
        } else {
          await store.release(cacheKey);
        }
      } catch (err) {
        log.warn(
          { err: (err as Error).message, scope, cacheKey, committed },
          committed ? 'idempotency failed to commit claim' : 'idempotency failed to release claim',
        );
      }
    }

    try {
      next();
    } catch (err) {
      captured.status = 500;
      void settle();
      throw err;
    }
  };
}

// ---------------------------------------------------------------------------
// runIdempotent — the same claim, taken from code instead of a request
// ---------------------------------------------------------------------------

export type IdempotentOutcome<T> =
  | { outcome: 'ran'; status: number; body: T }
  | { outcome: 'replayed'; status: number; body: T }
  /** Another caller holds the claim at 'pending' (running, or died holding it — see PENDING_TTL_SECONDS). */
  | { outcome: 'in_progress' }
  /** The key was committed for a different body. */
  | { outcome: 'key_conflict' };

/**
 * Run `handler` under the Idempotency-Key claim the middleware would take for
 * `POST` with this `scope`, `userId`, `key` and `body` — the same cache key
 * (`idem:<scope>:<userId>:<key>`), the same body hash, the same store. So a
 * server-side caller (a run draft's dispatch) and an HTTP retry with the same
 * key and body see one claim: whichever ran first, the other replays its
 * answer.
 *
 * A 2xx result is committed; anything else, or a throw, releases the claim so
 * a retry runs again. A claim whose holder died stays `pending` until
 * `PENDING_TTL_SECONDS` pass, then the next caller takes it over.
 *
 * Throws `IdempotencyStoreUnavailableError` when the store cannot be reached:
 * the handler never runs (fail closed, as the middleware's 503).
 */
export class IdempotencyStoreUnavailableError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'IdempotencyStoreUnavailableError';
  }
}

export async function runIdempotent<T>(
  opts: { scope: string; userId: string; key: string; body: unknown; ttlSeconds?: number; store?: IdempotencyStore | null },
  handler: () => Promise<{ status: number; body: T }>,
): Promise<IdempotentOutcome<T>> {
  const cacheKey = `idem:${opts.scope}:${opts.userId || 'anon'}:${opts.key}`;
  const requestHash = hashBody(opts.body);
  const store = resolveStore(opts.store);
  const fromExisting = (existing: string): IdempotentOutcome<T> | null => {
    if (existing === PENDING) return { outcome: 'in_progress' };
    const cached = parseCached(existing);
    if (!cached) return null;
    if (cached.bodyHash !== requestHash) return { outcome: 'key_conflict' };
    return { outcome: 'replayed', status: cached.status, body: cached.body as T };
  };

  try {
    const existing = await store.get(cacheKey);
    const answered = existing !== null ? fromExisting(existing) : null;
    if (answered) return answered;
    let claimed = await store.claim(cacheKey, PENDING_TTL_SECONDS);
    if (!claimed) {
      const raced = await store.get(cacheKey);
      const again = raced !== null ? fromExisting(raced) : null;
      if (again) return again;
      claimed = await store.claim(cacheKey, PENDING_TTL_SECONDS);
      if (!claimed) return { outcome: 'in_progress' };
    }
  } catch (err) {
    throw new IdempotencyStoreUnavailableError((err as Error).message);
  }

  let result: { status: number; body: T };
  try {
    result = await handler();
  } catch (err) {
    await store.release(cacheKey).catch((releaseErr: unknown) => {
      log.warn({ err: (releaseErr as Error).message, scope: opts.scope, cacheKey }, 'idempotency failed to release claim');
    });
    throw err;
  }
  const committed = result.status >= 200 && result.status < 300;
  try {
    if (committed) {
      const entry: CachedResponse = {
        status: result.status,
        headers: {},
        body: result.body ?? null,
        bodyHash: requestHash,
        cachedAt: Date.now(),
      };
      await store.commit(cacheKey, JSON.stringify(entry), opts.ttlSeconds ?? DEFAULT_TTL_SECONDS);
    } else {
      await store.release(cacheKey);
    }
  } catch (err) {
    log.warn({ err: (err as Error).message, scope: opts.scope, cacheKey, committed }, 'idempotency failed to settle claim');
  }
  return { outcome: 'ran', status: result.status, body: result.body };
}

/**
 * Drop the `runIdempotent` claim for (`scope`, `userId`, `key`) ONLY while it
 * is still `pending` — never a committed answer. For a caller that has proved,
 * by other means, that the pending holder is gone (run-draft dispatch: the
 * holder's replica incarnation lost its liveness lease, and the caller won the
 * conditional takeover of the draft), so it need not wait out
 * `PENDING_TTL_SECONDS`.
 */
export async function releasePendingClaim(
  opts: { scope: string; userId: string; key: string; store?: IdempotencyStore | null },
): Promise<void> {
  const cacheKey = `idem:${opts.scope}:${opts.userId || 'anon'}:${opts.key}`;
  await resolveStore(opts.store).release(cacheKey);
}
