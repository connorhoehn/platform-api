// The tenant key convention. The tests that matter here are the ones about
// AMBIGUITY: a composite key scheme that can be confused across tenants is a
// silent cross-tenant read, which no amount of downstream code review catches.

import {
  TENANTLESS,
  TENANT_DELIM,
  keyBelongsTo,
  parseScopedKey,
  scopedKey,
  tenantOf,
} from '../tenant';

describe('tenantOf', () => {
  it('resolves an org claim to that tenant', () => {
    expect(tenantOf('acme')).toBe('acme');
    expect(tenantOf('  acme  ')).toBe('acme');
  });

  it('maps an org-less principal to its OWN namespace, not a shared one', () => {
    // readOrgClaim returns undefined for a token with no org claim, and warns
    // against a `default-org` fallback precisely because it would fold every
    // such token into one bucket.
    expect(tenantOf(undefined)).toBe(TENANTLESS);
    expect(tenantOf(null)).toBe(TENANTLESS);
    expect(tenantOf('')).toBe(TENANTLESS);
    expect(tenantOf('   ')).toBe(TENANTLESS);
  });

  it('refuses a caller trying to claim the reserved org-less namespace', () => {
    // Otherwise a token claiming org "~" reads every org-less row present.
    expect(() => tenantOf(TENANTLESS)).toThrow(/reserved/);
  });

  it('refuses an org id containing the delimiter', () => {
    expect(() => tenantOf(`a${TENANT_DELIM}b`)).toThrow(/must not contain/);
  });
});

describe('scopedKey / parseScopedKey', () => {
  it('round-trips', () => {
    const k = scopedKey(tenantOf('acme'), 'doc-42');
    expect(k).toBe('acme|doc-42');
    expect(parseScopedKey(k)).toEqual({ tenant: 'acme', id: 'doc-42' });
  });

  it('handles the real id shapes this system mints', () => {
    const t = tenantOf('acme');
    for (const id of [
      'dm:dev-bob:dev-hank',
      '2026-08-25T17-57-18-422Z-9440ef59',
      '01J8XW9M4KQ2', // ulid-ish
      'doc-42#section-1',
      'a.b.c',
    ]) {
      expect(parseScopedKey(scopedKey(t, id))).toEqual({ tenant: 'acme', id });
    }
  });

  it('CANNOT be made ambiguous across tenants', () => {
    // The attack: make two different (tenant, id) pairs produce ONE key.
    //   org 'a|b' + id 'c'   -> 'a|b|c'
    //   org 'a'   + id 'b|c' -> 'a|b|c'
    // If both were allowed, tenant 'a' could read and OVERWRITE tenant
    // 'a|b''s row. Both inputs are refused instead of escaped.
    expect(() => tenantOf('a|b')).toThrow();
    expect(() => scopedKey(tenantOf('a'), 'b|c')).toThrow(/must not contain/);
    // …and a key with an extra delimiter is never parsed by guessing.
    expect(() => parseScopedKey('a|b|c')).toThrow(/malformed/);
  });

  it('rejects empty components', () => {
    expect(() => scopedKey('acme' as never, '')).toThrow(/must not be empty/);
    expect(() => parseScopedKey('acme|')).toThrow(/malformed/);
    expect(() => parseScopedKey('|doc')).toThrow(/malformed/);
    expect(() => parseScopedKey('nodelimiter')).toThrow(/malformed/);
  });

  it('keeps two tenants distinct for the SAME natural id', () => {
    const a = scopedKey(tenantOf('acme'), 'doc-42');
    const b = scopedKey(tenantOf('globex'), 'doc-42');
    expect(a).not.toBe(b);
    expect(keyBelongsTo(a, tenantOf('acme'))).toBe(true);
    expect(keyBelongsTo(a, tenantOf('globex'))).toBe(false);
  });

  it('org-less rows are isolated from every real org', () => {
    const orgless = scopedKey(TENANTLESS, 'doc-42');
    expect(keyBelongsTo(orgless, tenantOf('acme'))).toBe(false);
    expect(keyBelongsTo(orgless, TENANTLESS)).toBe(true);
  });

  it('keyBelongsTo is false for junk rather than throwing', () => {
    // Read paths assert with this; it must not turn a bad row into a 500.
    expect(keyBelongsTo('garbage', tenantOf('acme'))).toBe(false);
    expect(keyBelongsTo('', tenantOf('acme'))).toBe(false);
  });
});
