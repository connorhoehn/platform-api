/**
 * Service-to-service auth envelope (HMAC-SHA256, v1).
 *
 * Wire format:  v1.<serviceId>.<unixTsSec>.<base64url(HMAC-SHA256(secret, "<serviceId>.<unixTsSec>.<payloadString>"))>
 *
 * - Version-prefixed ("v1.") so future formats (e.g. asymmetric "ed25519.") can
 *   coexist behind a single header and verifiers can fail fast on unknown versions.
 * - HMAC-SHA256 over the canonical string ties signature to (caller identity,
 *   wall-clock, payload). Timestamp is INSIDE the MAC input, not just appended,
 *   so replay or skew attacks can't be combined with payload tampering.
 * - Payload is canonicalised (sorted-keys JSON for objects, raw string otherwise)
 *   so two structurally-equal bodies sign identically. Otherwise innocent
 *   key-order changes would break verification.
 *
 * HMAC over JWT: HMAC is ~80 bytes on the wire vs JWT's ~200, requires no JOSE
 * header parsing, no `alg: none` foot-gun, and we own the clock-skew/ttl knobs
 * directly instead of relying on `exp`/`nbf` semantics across libraries.
 *
 * Key rotation: verifier accepts `secret: string | string[]`. During rotation
 * the receiver lists [newSecret, oldSecret]; signers swap to newSecret on a
 * staggered deploy. Once all signers are on newSecret, oldSecret is dropped on
 * the next deploy. No downtime, no coordinated cutover.
 *
 * Asymmetric future: swap MAC for Ed25519 sign/verify; signers hold private key,
 * verifiers fetch public keys (keyed by serviceId) from event-catalog. Wire
 * format becomes "ed25519.<serviceId>.<keyId>.<ts>.<base64url(sig)>".
 */

import { createHmac, timingSafeEqual } from 'crypto';

const VERSION = 'v1';
const DEFAULT_TTL_SEC = 300;          // 5 min absolute lifetime
const DEFAULT_TOLERANCE_SEC = 30;     // tolerated clock skew (past + future)

export interface SignOptions {
  secret: string;
  serviceId: string;
  ttlSec?: number; // informational; signer doesn't enforce, but recorded for tests
}

export interface VerifyOptions {
  secret: string | string[];
  allowedServices: string[];
  /**
   * The payload the caller signed. The receiver MUST supply this — we
   * re-canonicalise it here so a malicious caller can't slip a non-canonical
   * body past a permissive parser.
   */
  payload: string | object;
  ttlSec?: number;
  toleranceSec?: number;
  nowMs?: number;
}

export type VerifyResult =
  | { ok: true; serviceId: string }
  | {
      ok: false;
      reason: 'malformed' | 'stale' | 'future-skew' | 'unknown-service' | 'bad-signature';
    };

/**
 * Canonicalise the signed payload. Objects → sorted-key JSON; strings pass
 * through. Anything else is JSON.stringify'd (will throw for circular refs,
 * which is what we want — caller bug, not silent corruption).
 */
function canonicalise(payload: string | object): string {
  if (typeof payload === 'string') return payload;
  return stableStringify(payload);
}

function stableStringify(value: unknown): string {
  if (value === null || typeof value !== 'object') {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return '[' + value.map((v) => stableStringify(v)).join(',') + ']';
  }
  const keys = Object.keys(value as Record<string, unknown>).sort();
  const parts: string[] = [];
  for (const k of keys) {
    const v = (value as Record<string, unknown>)[k];
    if (v === undefined) continue;
    parts.push(JSON.stringify(k) + ':' + stableStringify(v));
  }
  return '{' + parts.join(',') + '}';
}

function base64url(buf: Buffer): string {
  return buf
    .toString('base64')
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/, '');
}

function base64urlDecode(s: string): Buffer | null {
  // base64url chars only; reject anything else early
  if (!/^[A-Za-z0-9_-]+$/.test(s)) return null;
  const padLen = (4 - (s.length % 4)) % 4;
  const padded = s.replace(/-/g, '+').replace(/_/g, '/') + '='.repeat(padLen);
  try {
    return Buffer.from(padded, 'base64');
  } catch {
    return null;
  }
}

function macOver(secret: string, serviceId: string, tsSec: number, payloadString: string): Buffer {
  const h = createHmac('sha256', secret);
  h.update(`${serviceId}.${tsSec}.${payloadString}`);
  return h.digest();
}

/**
 * Build a signed header value. Caller passes the same payload to the receiver
 * (e.g. as the HTTP body / WS frame body); receiver re-canonicalises and verifies.
 */
export function signEnvelope(
  payload: string | object,
  opts: SignOptions,
): string {
  if (!opts || !opts.secret || !opts.serviceId) {
    throw new Error('signEnvelope: secret and serviceId required');
  }
  const tsSec = Math.floor(Date.now() / 1000);
  const payloadString = canonicalise(payload);
  const mac = macOver(opts.secret, opts.serviceId, tsSec, payloadString);
  return `${VERSION}.${opts.serviceId}.${tsSec}.${base64url(mac)}`;
}

/**
 * Verify a header value against the same payload the caller signed.
 * Receiver MUST supply the payload (string or object) — we re-canonicalise
 * here so callers can't trick us with a non-canonical body.
 */
export function verifyEnvelope(
  headerValue: string,
  opts: VerifyOptions,
): VerifyResult {
  if (typeof headerValue !== 'string' || headerValue.length === 0) {
    return { ok: false, reason: 'malformed' };
  }
  const parts = headerValue.split('.');
  if (parts.length !== 4) return { ok: false, reason: 'malformed' };
  const [version, serviceId, tsStr, macB64] = parts;
  if (version !== VERSION) return { ok: false, reason: 'malformed' };
  if (!serviceId || !tsStr || !macB64) return { ok: false, reason: 'malformed' };
  if (!/^\d+$/.test(tsStr)) return { ok: false, reason: 'malformed' };
  const tsSec = Number(tsStr);
  if (!Number.isFinite(tsSec)) return { ok: false, reason: 'malformed' };
  const macBuf = base64urlDecode(macB64);
  if (!macBuf || macBuf.length !== 32) return { ok: false, reason: 'malformed' };

  // Service-allow-list check BEFORE signature work — cheaper, and avoids
  // burning HMAC compute on garbage callers. Note: allow-list does not
  // protect against an attacker spoofing serviceId, since they'd also
  // need the matching secret to forge a valid MAC.
  if (!opts.allowedServices.includes(serviceId)) {
    return { ok: false, reason: 'unknown-service' };
  }

  const nowMs = opts.nowMs ?? Date.now();
  const nowSec = Math.floor(nowMs / 1000);
  const ttlSec = opts.ttlSec ?? DEFAULT_TTL_SEC;
  const toleranceSec = opts.toleranceSec ?? DEFAULT_TOLERANCE_SEC;

  // future-skew check: tsSec > now + tolerance
  if (tsSec > nowSec + toleranceSec) {
    return { ok: false, reason: 'future-skew' };
  }
  // stale check: tsSec < now - ttl (tolerance only forgives future drift,
  // not past — staleness is what guards against replay).
  if (tsSec < nowSec - ttlSec) {
    return { ok: false, reason: 'stale' };
  }

  const payloadString = canonicalise(opts.payload);
  const secrets = Array.isArray(opts.secret) ? opts.secret : [opts.secret];
  for (const sec of secrets) {
    const expected = macOver(sec, serviceId, tsSec, payloadString);
    if (expected.length === macBuf.length && timingSafeEqual(expected, macBuf)) {
      return { ok: true, serviceId };
    }
  }
  return { ok: false, reason: 'bad-signature' };
}

// Exposed for tests / future call sites that need raw canonicalisation
// (e.g. logging the canonical form for debugging mismatches).
export const __internal = { canonicalise, base64url, base64urlDecode };
