// social-api/src/lib/__tests__/webhookSignature.test.ts
//
// Tests for webhookSignature.ts: HMAC-SHA256 signing, constant-time
// verification, and the rejection paths that guard the webhook route.

import {
  SIGNATURE_HEADER,
  computeSignature,
  generateWebhookSecret,
  verifySignature,
} from '../webhookSignature';

// ---------------------------------------------------------------------------
// generateWebhookSecret
// ---------------------------------------------------------------------------

describe('generateWebhookSecret', () => {
  it('returns a 64-character lowercase hex string', () => {
    const secret = generateWebhookSecret();
    expect(secret).toMatch(/^[0-9a-f]{64}$/);
  });

  it('returns a different value on each call', () => {
    const a = generateWebhookSecret();
    const b = generateWebhookSecret();
    expect(a).not.toBe(b);
  });
});

// ---------------------------------------------------------------------------
// computeSignature
// ---------------------------------------------------------------------------

describe('computeSignature', () => {
  it('returns a sha256= prefixed hex digest', () => {
    const body = Buffer.from('{"event":"test"}');
    const sig = computeSignature('mysecret', body);
    expect(sig).toMatch(/^sha256=[0-9a-f]{64}$/);
  });

  it('is deterministic for the same secret + body', () => {
    const body = Buffer.from('hello');
    const a = computeSignature('s3cr3t', body);
    const b = computeSignature('s3cr3t', body);
    expect(a).toBe(b);
  });

  it('differs when the secret changes', () => {
    const body = Buffer.from('hello');
    const a = computeSignature('secretA', body);
    const b = computeSignature('secretB', body);
    expect(a).not.toBe(b);
  });

  it('differs when the body changes', () => {
    const a = computeSignature('secret', Buffer.from('payload-1'));
    const b = computeSignature('secret', Buffer.from('payload-2'));
    expect(a).not.toBe(b);
  });

  it('matches the known HMAC-SHA256 digest for a fixed input', () => {
    // Computed independently: echo -n 'hello' | openssl dgst -sha256 -hmac 'key' => b19c...
    const { createHmac } = require('crypto') as typeof import('crypto');
    const expected = 'sha256=' + createHmac('sha256', 'key').update('hello').digest('hex');
    expect(computeSignature('key', Buffer.from('hello'))).toBe(expected);
  });
});

// ---------------------------------------------------------------------------
// verifySignature
// ---------------------------------------------------------------------------

describe('verifySignature', () => {
  const secret = 'test-secret-32bytes-padded-here!';
  const body = Buffer.from('{"event":"pipeline.trigger"}');

  function validHeader(): string {
    return computeSignature(secret, body);
  }

  it('returns true for a correctly signed request', () => {
    expect(verifySignature(secret, body, validHeader())).toBe(true);
  });

  it('returns false when headerValue is undefined', () => {
    expect(verifySignature(secret, body, undefined)).toBe(false);
  });

  it('returns false when headerValue is empty string', () => {
    expect(verifySignature(secret, body, '')).toBe(false);
  });

  it('returns false when the sha256= prefix is missing', () => {
    const noPrefix = validHeader().slice('sha256='.length);
    expect(verifySignature(secret, body, noPrefix)).toBe(false);
  });

  it('returns false when the digest is wrong', () => {
    expect(verifySignature(secret, body, 'sha256=' + 'a'.repeat(64))).toBe(false);
  });

  it('returns false when the body has been tampered with', () => {
    const sig = validHeader();
    const tamperedBody = Buffer.from('{"event":"pipeline.cancel"}');
    expect(verifySignature(secret, tamperedBody, sig)).toBe(false);
  });

  it('returns false when the secret is wrong', () => {
    const sig = computeSignature('wrong-secret', body);
    expect(verifySignature(secret, body, sig)).toBe(false);
  });

  it('returns false for a header of different length (no crash)', () => {
    // timingSafeEqual throws on length mismatch — verifySignature must absorb it.
    expect(verifySignature(secret, body, 'sha256=abc')).toBe(false);
  });

  it('SIGNATURE_HEADER constant is lowercase (Express header convention)', () => {
    expect(SIGNATURE_HEADER).toBe(SIGNATURE_HEADER.toLowerCase());
    expect(SIGNATURE_HEADER).toBe('x-pipeline-signature-256');
  });
});
