import { parseExpiryBody, MIN_EXPIRY_LEAD_MS } from '../recordings';

describe('parseExpiryBody', () => {
  const now = Date.parse('2026-09-15T00:00:00Z');
  it('accepts a date at least an hour out', () => {
    const r = parseExpiryBody({ expiresAt: '2026-10-15T00:00:00Z' }, now);
    expect('expiresAt' in r && r.expiresAt?.toISOString()).toBe('2026-10-15T00:00:00.000Z');
  });
  it('accepts null as "no expiration"', () => {
    expect(parseExpiryBody({ expiresAt: null }, now)).toEqual({ expiresAt: null });
  });
  it('rejects the past, junk, and an absent field', () => {
    expect(parseExpiryBody({ expiresAt: new Date(now + MIN_EXPIRY_LEAD_MS - 1).toISOString() }, now)).toHaveProperty('error');
    expect(parseExpiryBody({ expiresAt: 'soon' }, now)).toHaveProperty('error');
    expect(parseExpiryBody({}, now)).toHaveProperty('error');
    expect(parseExpiryBody({ expiresAt: '2099-01-01T00:00:00Z' }, now)).toHaveProperty('error');
  });
});
