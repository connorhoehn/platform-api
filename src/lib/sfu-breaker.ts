/**
 * Shared CircuitBreaker for all outbound LVS (live-video-streaming) calls.
 *
 * One process-wide breaker so a sick LVS opens the circuit for every route
 * that touches it — `/api/video/sessions`, `/api/recordings/.../playback-token`,
 * and any future LVS proxy — instead of each route trying its own retries
 * against the same upstream. Without this, an LVS hiccup hangs each request
 * for full Node fetch default and all replicas keep hammering it.
 *
 * Originally lived inside `routes/videoSessions.ts` (Phase 54.6 P1 #4); split
 * out for hub task #1001 so `recordings.ts` and future LVS-touching routes
 * share the same breaker state.
 */

import { CircuitBreaker, CircuitState } from 'distributed-core/transport';
import { AppError } from '../middleware/error-handler';

// 8s per-request abort budget. The SFU's POST /channels does an IVS channel
// create round-trip; 8s is comfortably above its p99 (~500ms) but below what
// a user perceives as "hung". Timeout → AbortError → counted as a breaker
// failure → 504 to the client.
const FETCH_TIMEOUT_MS = 8000;

// Breaker timeout is the *backstop* in case AbortSignal doesn't fire (e.g.
// a runtime that ignores the signal). Must exceed FETCH_TIMEOUT_MS so the
// abort path normally wins and we don't double-count.
const BREAKER_TIMEOUT_MS = 10_000;

// Open after 5 consecutive failures, attempt half-open probe every 30s,
// require 2 half-open successes to fully close. Tuned for SFU's typical
// outage profile (single-AZ blip → ~30s; full deploy → ~2min).
const sfuBreaker = new CircuitBreaker({
  name: 'video-api',
  failureThreshold: 5,
  successThreshold: 2,
  resetTimeout: 30_000,
  timeout: BREAKER_TIMEOUT_MS,
  halfOpenMaxCalls: 1,
});

/**
 * Internal marker for "the breaker said no" — lets callers distinguish
 * "fast-fail because circuit is open" (→ 503) from "actual upstream error".
 */
export class BreakerOpenError extends Error {
  constructor() {
    super('Video API circuit breaker is open');
    this.name = 'BreakerOpenError';
  }
}

/** Carries the underlying Response so callers can map status → AppError. */
export class UpstreamServerError extends Error {
  constructor(public readonly response: globalThis.Response) {
    super(`Video API upstream error ${response.status}`);
    this.name = 'UpstreamServerError';
  }
}

/**
 * Run a fetch against the SFU through the breaker with an abort-based
 * timeout. Caller still inspects `res.ok`/`res.status` for application-level
 * errors; only network failures, timeouts, and upstream 5xx are reported to
 * the breaker as failures. 4xx is client-shaped and doesn't trip the breaker.
 */
export async function callSfu(
  input: string,
  init: RequestInit = {},
): Promise<globalThis.Response> {
  if (sfuBreaker.getState() === CircuitState.OPEN) {
    throw new BreakerOpenError();
  }
  return sfuBreaker.execute(async () => {
    const res = await fetch(input, {
      ...init,
      signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
    });
    if (res.status >= 500) {
      throw new UpstreamServerError(res);
    }
    return res;
  });
}

/**
 * Translate an SFU call rejection into the right HTTP error for the client:
 *   - circuit open       → 503 fast-fail
 *   - AbortError/timeout → 504
 *   - upstream 5xx       → forward status
 *   - anything else      → 502 bad-gateway
 */
export function mapSfuError(err: unknown): AppError {
  if (err instanceof BreakerOpenError) {
    return new AppError(503, 'Video service temporarily unavailable');
  }
  if (err instanceof UpstreamServerError) {
    return new AppError(err.response.status, `Video API error: ${err.response.status}`);
  }
  if (
    err instanceof Error
    && (err.name === 'AbortError'
      || err.name === 'TimeoutError'
      || /timeout/i.test(err.message))
  ) {
    return new AppError(504, 'Video service timeout');
  }
  return new AppError(502, 'Video service unreachable');
}

/** Read-only breaker state — for diagnostic / test assertions. */
export function getSfuBreakerState(): CircuitState {
  return sfuBreaker.getState();
}

/** Exposed for tests that need to reset breaker state between cases. */
export function __resetSfuBreakerForTests(): void {
  sfuBreaker.reset();
}

/**
 * Exposed for tests that need to shrink the reset timeout to avoid real
 * 30s waits. NOT for production use — the production timeouts are tuned in
 * the constructor above.
 */
export function __configureSfuBreakerForTests(opts: {
  failureThreshold?: number;
  successThreshold?: number;
  resetTimeout?: number;
  halfOpenMaxCalls?: number;
}): void {
  sfuBreaker.updateOptions(opts);
}
