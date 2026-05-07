// social-api/src/lib/webhookSignature.ts
//
// HMAC-SHA256 signature helpers for the public pipeline webhook endpoint.
//
// Convention follows GitHub's webhooks: every request carries an
// `X-Pipeline-Signature-256` header of the form `sha256=<hex>` where the hex
// portion is HMAC-SHA256(secret, raw_request_body). Verification uses
// `crypto.timingSafeEqual` against the recomputed digest to avoid leaking
// information through string-comparison timing.
//
// Secrets are 32-byte (64-hex-char) random values minted server-side the
// first time a webhook trigger binding is saved (see pipelineDefinitions.ts).
// Phase 1 behavior is preserved when a binding has no secret — the route
// accepts the request unsigned so existing dev fixtures keep working.

import {
  createHmac,
  randomBytes,
  timingSafeEqual,
} from 'crypto';

/**
 * Header name external systems send the signature in. Exported so the route
 * and the OpenAPI spec stay in lockstep.
 */
export const SIGNATURE_HEADER = 'x-pipeline-signature-256';

/**
 * Mint a fresh webhook secret. 32 random bytes encoded as 64 lowercase hex
 * characters — same shape GitHub uses, plenty of entropy, and copy-pasteable
 * straight into a webhook source's "secret" field.
 */
export function generateWebhookSecret(): string {
  return randomBytes(32).toString('hex');
}

/**
 * Compute the canonical signature header value for a (secret, rawBody) pair.
 * Returned as `sha256=<hex>` so callers can compare verbatim against the
 * incoming header. Useful for tests and for any internal call site that
 * forwards a webhook payload back into the route.
 */
export function computeSignature(secret: string, rawBody: Buffer): string {
  const digest = createHmac('sha256', secret).update(rawBody).digest('hex');
  return `sha256=${digest}`;
}

/**
 * Constant-time comparison of an incoming `X-Pipeline-Signature-256` header
 * against the digest derived from `(secret, rawBody)`. Returns `false` for
 * any of: missing header, malformed header (no `sha256=` prefix), wrong
 * length, or digest mismatch — never throws.
 */
export function verifySignature(
  secret: string,
  rawBody: Buffer,
  headerValue: string | undefined,
): boolean {
  if (!headerValue || typeof headerValue !== 'string') return false;
  if (!headerValue.startsWith('sha256=')) return false;

  const expected = computeSignature(secret, rawBody);
  // Equal-length is a precondition for `timingSafeEqual` — comparing buffers
  // of different lengths throws, so reject early.
  if (expected.length !== headerValue.length) return false;

  const a = Buffer.from(expected);
  const b = Buffer.from(headerValue);
  try {
    return timingSafeEqual(a, b);
  } catch {
    return false;
  }
}
