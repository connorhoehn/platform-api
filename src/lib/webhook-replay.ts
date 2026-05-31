// platform-api/src/lib/webhook-replay.ts
//
// Shared replay-window guard for webhook handlers (LVS, pipeline, ingest
// variants). HMAC verification alone proves the body was signed by the
// holder of the shared secret, but says nothing about WHEN it was signed —
// an attacker who captures one signed request can replay it indefinitely.
// This guard rejects requests whose `x-webhook-timestamp` header drifts
// from the receiver's clock by more than ±5 minutes.
//
// Posture: paired with HMAC. Handlers call `checkReplayWindow` ONLY when
// they are also enforcing an HMAC signature on the same request — when
// the handler is in dev pass-through mode (no secret configured, or
// SKIP_AUTH=true), there is no authenticity guarantee to begin with and
// adding a timestamp gate would just break existing unsigned dev fixtures.
//
// Header format: `x-webhook-timestamp` accepts ISO-8601 (e.g.
// "2026-05-31T04:00:00Z") OR a unix integer. Integers < 1e12 are
// interpreted as unix-seconds and upscaled to ms (Stripe-style); larger
// integers are treated as unix-ms directly.

export const WEBHOOK_TIMESTAMP_HEADER = 'x-webhook-timestamp';

export const DEFAULT_MAX_SKEW_MS = 300_000;

export interface ReplayCheckResult {
  ok: boolean;
  /** Populated when ok=false; safe to surface as a JSON error string. */
  reason?: string;
}

/**
 * Verify that `headerValue` (the `x-webhook-timestamp` header) is within
 * ±maxSkewMs of the current clock. Returns `{ ok: false, reason }` when
 * the header is missing, unparseable, too old, or too far in the future.
 *
 * `now` is injectable so tests can pin time without globally faking the
 * clock.
 */
export function checkReplayWindow(
  headerValue: string | string[] | undefined,
  maxSkewMs: number = DEFAULT_MAX_SKEW_MS,
  now: () => number = Date.now,
): ReplayCheckResult {
  const raw = Array.isArray(headerValue) ? headerValue[0] : headerValue;
  if (typeof raw !== 'string' || raw.length === 0) {
    return { ok: false, reason: 'missing x-webhook-timestamp header' };
  }

  let ts: number;
  if (/^\d+$/.test(raw)) {
    ts = Number(raw);
    if (!Number.isFinite(ts)) {
      return { ok: false, reason: 'invalid x-webhook-timestamp' };
    }
    // Heuristic: unix-seconds vs unix-ms. Any current-era ms timestamp is
    // >= 1e12 (Sep 2001). Values below that threshold are seconds.
    if (ts < 1e12) ts = ts * 1000;
  } else {
    const parsed = Date.parse(raw);
    if (Number.isNaN(parsed)) {
      return { ok: false, reason: 'invalid x-webhook-timestamp' };
    }
    ts = parsed;
  }

  const drift = now() - ts;
  if (drift > maxSkewMs) {
    return { ok: false, reason: 'x-webhook-timestamp too old' };
  }
  if (drift < -maxSkewMs) {
    return { ok: false, reason: 'x-webhook-timestamp too far in future' };
  }
  return { ok: true };
}
