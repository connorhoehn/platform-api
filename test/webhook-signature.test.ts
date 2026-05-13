// test/webhook-signature.test.ts
//
// Tests for webhookSignature: generateWebhookSecret, computeSignature, verifySignature.

import * as crypto from 'crypto';
import {
    SIGNATURE_HEADER,
    generateWebhookSecret,
    computeSignature,
    verifySignature,
} from '@connorhoehn/service-runtime';

// ─── generateWebhookSecret ────────────────────────────────────────────────

describe('generateWebhookSecret', () => {
    test('returns a 64-character hex string', () => {
        const secret = generateWebhookSecret();
        expect(secret).toMatch(/^[0-9a-f]{64}$/);
    });

    test('each call returns a different value', () => {
        const a = generateWebhookSecret();
        const b = generateWebhookSecret();
        expect(a).not.toBe(b);
    });
});

// ─── computeSignature ─────────────────────────────────────────────────────

describe('computeSignature', () => {
    test('returns a string starting with sha256=', () => {
        const sig = computeSignature('my-secret', Buffer.from('body'));
        expect(sig).toMatch(/^sha256=[0-9a-f]{64}$/);
    });

    test('is deterministic for the same secret+body', () => {
        const body = Buffer.from('{"event":"push"}');
        const a = computeSignature('secret', body);
        const b = computeSignature('secret', body);
        expect(a).toBe(b);
    });

    test('produces different signatures for different bodies', () => {
        const a = computeSignature('secret', Buffer.from('body-a'));
        const b = computeSignature('secret', Buffer.from('body-b'));
        expect(a).not.toBe(b);
    });

    test('produces different signatures for different secrets', () => {
        const body = Buffer.from('same body');
        const a = computeSignature('secret-1', body);
        const b = computeSignature('secret-2', body);
        expect(a).not.toBe(b);
    });

    test('matches manual HMAC-SHA256 computation', () => {
        const secret = 'known-secret';
        const body = Buffer.from('test payload');
        const expected =
            'sha256=' + crypto.createHmac('sha256', secret).update(body).digest('hex');
        expect(computeSignature(secret, body)).toBe(expected);
    });
});

// ─── verifySignature ──────────────────────────────────────────────────────

describe('verifySignature', () => {
    const secret = 'test-secret-abc';
    const body = Buffer.from('webhook payload');

    function validHeader(): string {
        return computeSignature(secret, body);
    }

    test('returns true for a valid signature', () => {
        expect(verifySignature(secret, body, validHeader())).toBe(true);
    });

    test('returns false when header is undefined', () => {
        expect(verifySignature(secret, body, undefined)).toBe(false);
    });

    test('returns false when header is empty string', () => {
        expect(verifySignature(secret, body, '')).toBe(false);
    });

    test('returns false when header lacks sha256= prefix', () => {
        const noPrefix = validHeader().replace('sha256=', '');
        expect(verifySignature(secret, body, noPrefix)).toBe(false);
    });

    test('returns false when signature is for different body', () => {
        const otherSig = computeSignature(secret, Buffer.from('other body'));
        expect(verifySignature(secret, body, otherSig)).toBe(false);
    });

    test('returns false when signature is for different secret', () => {
        const wrongSig = computeSignature('wrong-secret', body);
        expect(verifySignature(secret, body, wrongSig)).toBe(false);
    });

    test('returns false for a truncated signature', () => {
        const truncated = validHeader().slice(0, -4);
        expect(verifySignature(secret, body, truncated)).toBe(false);
    });

    test('returns false for a malformed sha256= value (not hex)', () => {
        expect(verifySignature(secret, body, 'sha256=not-valid-hex-value')).toBe(false);
    });
});

// ─── SIGNATURE_HEADER ─────────────────────────────────────────────────────

describe('SIGNATURE_HEADER', () => {
    test('equals x-pipeline-signature-256', () => {
        expect(SIGNATURE_HEADER).toBe('x-pipeline-signature-256');
    });
});
