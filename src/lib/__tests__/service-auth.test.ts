import { signEnvelope, verifyEnvelope, __internal } from '../service-auth';

const SECRET = 'shhhh-this-is-not-the-real-secret';
const SERVICE = 'edge-gateway';
const ALLOWED = ['edge-gateway', 'realtime-fanout', 'platform-api'];

describe('signEnvelope / verifyEnvelope — happy path', () => {
  it('round-trips a string payload', () => {
    const payload = 'channel:abc|seq:42|body:hello';
    const header = signEnvelope(payload, { secret: SECRET, serviceId: SERVICE });
    const result = verifyEnvelope(header, {
      secret: SECRET,
      allowedServices: ALLOWED,
      payload,
    });
    expect(result).toEqual({ ok: true, serviceId: SERVICE });
  });

  it('round-trips an object payload via canonical JSON', () => {
    const payload = { channel: 'abc', seq: 42, body: 'hello' };
    const header = signEnvelope(payload, { secret: SECRET, serviceId: SERVICE });
    const result = verifyEnvelope(header, {
      secret: SECRET,
      allowedServices: ALLOWED,
      payload,
    });
    expect(result).toEqual({ ok: true, serviceId: SERVICE });
  });

  it('canonicalisation is key-order-independent', () => {
    const payloadA = { a: 1, b: 2, nested: { x: 10, y: 20 } };
    const payloadB = { nested: { y: 20, x: 10 }, b: 2, a: 1 };
    const header = signEnvelope(payloadA, { secret: SECRET, serviceId: SERVICE });
    const result = verifyEnvelope(header, {
      secret: SECRET,
      allowedServices: ALLOWED,
      payload: payloadB,
    });
    expect(result).toEqual({ ok: true, serviceId: SERVICE });
  });

  it('header format matches v1.<svc>.<ts>.<b64url(32 bytes)>', () => {
    const header = signEnvelope('x', { secret: SECRET, serviceId: SERVICE });
    const parts = header.split('.');
    expect(parts).toHaveLength(4);
    expect(parts[0]).toBe('v1');
    expect(parts[1]).toBe(SERVICE);
    expect(parts[2]).toMatch(/^\d+$/);
    // base64url-decoded MAC must be 32 bytes (HMAC-SHA256 digest size)
    const mac = __internal.base64urlDecode(parts[3]);
    expect(mac).not.toBeNull();
    expect(mac!.length).toBe(32);
  });
});

describe('verifyEnvelope — failure modes', () => {
  it('wrong secret → bad-signature', () => {
    const payload = { hello: 'world' };
    const header = signEnvelope(payload, { secret: SECRET, serviceId: SERVICE });
    const result = verifyEnvelope(header, {
      secret: 'a-different-secret',
      allowedServices: ALLOWED,
      payload,
    });
    expect(result).toEqual({ ok: false, reason: 'bad-signature' });
  });

  it('stale timestamp (older than ttlSec) → stale', () => {
    const payload = 'p';
    const header = signEnvelope(payload, { secret: SECRET, serviceId: SERVICE });
    // Pretend "now" is way in the future, beyond ttlSec
    const futureNowMs = (Math.floor(Date.now() / 1000) + 10_000) * 1000;
    const result = verifyEnvelope(header, {
      secret: SECRET,
      allowedServices: ALLOWED,
      payload,
      ttlSec: 60,
      nowMs: futureNowMs,
    });
    expect(result).toEqual({ ok: false, reason: 'stale' });
  });

  it('future-skew beyond tolerance → future-skew', () => {
    const payload = 'p';
    const header = signEnvelope(payload, { secret: SECRET, serviceId: SERVICE });
    // Pretend "now" is way in the past, so the header timestamp is in our future
    const pastNowMs = (Math.floor(Date.now() / 1000) - 10_000) * 1000;
    const result = verifyEnvelope(header, {
      secret: SECRET,
      allowedServices: ALLOWED,
      payload,
      toleranceSec: 30,
      nowMs: pastNowMs,
    });
    expect(result).toEqual({ ok: false, reason: 'future-skew' });
  });

  it('future-skew within tolerance → ok', () => {
    const payload = 'p';
    const header = signEnvelope(payload, { secret: SECRET, serviceId: SERVICE });
    // Header was signed 5s in our "future" — within 30s tolerance
    const slightlyPastNowMs = (Math.floor(Date.now() / 1000) - 5) * 1000;
    const result = verifyEnvelope(header, {
      secret: SECRET,
      allowedServices: ALLOWED,
      payload,
      toleranceSec: 30,
      nowMs: slightlyPastNowMs,
    });
    expect(result).toEqual({ ok: true, serviceId: SERVICE });
  });

  it('unknown service (not in allowedServices) → unknown-service', () => {
    const payload = 'p';
    const header = signEnvelope(payload, { secret: SECRET, serviceId: 'rogue-service' });
    const result = verifyEnvelope(header, {
      secret: SECRET,
      allowedServices: ALLOWED,
      payload,
    });
    expect(result).toEqual({ ok: false, reason: 'unknown-service' });
  });

  it('tampered payload → bad-signature', () => {
    const original = { amount: 100, to: 'alice' };
    const header = signEnvelope(original, { secret: SECRET, serviceId: SERVICE });
    const tampered = { amount: 9999, to: 'alice' };
    const result = verifyEnvelope(header, {
      secret: SECRET,
      allowedServices: ALLOWED,
      payload: tampered,
    });
    expect(result).toEqual({ ok: false, reason: 'bad-signature' });
  });

  describe('malformed headers', () => {
    const cases: Array<[string, string]> = [
      ['empty string', ''],
      ['too few segments', 'v1.svc.123'],
      ['too many segments', 'v1.svc.123.mac.extra'],
      ['wrong version', 'v2.svc.123.AAAA'],
      ['non-numeric timestamp', 'v1.svc.notanumber.AAAA'],
      ['empty serviceId', 'v1..123.AAAA'],
      ['mac with non-base64url chars', 'v1.svc.123.!!!badmac!!!'],
      ['mac wrong length (not 32 bytes after decode)', 'v1.svc.123.QUFB'], // "AAA" → 3 bytes
    ];
    for (const [name, header] of cases) {
      it(`malformed: ${name}`, () => {
        const result = verifyEnvelope(header, {
          secret: SECRET,
          allowedServices: ALLOWED,
          payload: 'p',
        });
        expect(result).toEqual({ ok: false, reason: 'malformed' });
      });
    }
  });
});

describe('key rotation — dual-acceptance window', () => {
  it('verifier accepts header signed with either the old or new secret', () => {
    const OLD = 'old-secret-being-retired';
    const NEW = 'new-secret-rolling-out';
    const payload = { evt: 'channel.message' };

    const headerOld = signEnvelope(payload, { secret: OLD, serviceId: SERVICE });
    const headerNew = signEnvelope(payload, { secret: NEW, serviceId: SERVICE });

    const verifierOpts = {
      secret: [NEW, OLD],
      allowedServices: ALLOWED,
      payload,
    };

    expect(verifyEnvelope(headerOld, verifierOpts)).toEqual({ ok: true, serviceId: SERVICE });
    expect(verifyEnvelope(headerNew, verifierOpts)).toEqual({ ok: true, serviceId: SERVICE });
  });

  it('once oldSecret is dropped, headers signed with it stop verifying', () => {
    const OLD = 'old-secret';
    const NEW = 'new-secret';
    const payload = 'p';

    const headerOld = signEnvelope(payload, { secret: OLD, serviceId: SERVICE });
    const result = verifyEnvelope(headerOld, {
      secret: [NEW], // post-rotation: only the new secret is accepted
      allowedServices: ALLOWED,
      payload,
    });
    expect(result).toEqual({ ok: false, reason: 'bad-signature' });
  });
});

describe('canonicalisation edge cases', () => {
  it('omits undefined object values (mirrors JSON.stringify behaviour)', () => {
    const c = __internal.canonicalise({ a: 1, b: undefined, c: 3 });
    expect(c).toBe('{"a":1,"c":3}');
  });

  it('serialises arrays positionally', () => {
    const c = __internal.canonicalise({ xs: [3, 1, 2] });
    expect(c).toBe('{"xs":[3,1,2]}');
  });

  it('passes strings through unchanged', () => {
    expect(__internal.canonicalise('raw-string')).toBe('raw-string');
  });
});
