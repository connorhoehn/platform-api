// GAP B.2 — org-prefixed channel names.
import { orgChannel, parseChannelOrg } from '../org-channel';

describe('orgChannel', () => {
  it('prefixes with org:<orgId>: when an org is present', () => {
    expect(orgChannel('acme', 'abc-123')).toBe('org:acme:abc-123');
  });

  it('returns the bare name for org-less callers (fail-open legacy path)', () => {
    expect(orgChannel(undefined, 'abc-123')).toBe('abc-123');
    expect(orgChannel(null, 'abc-123')).toBe('abc-123');
    expect(orgChannel('', 'abc-123')).toBe('abc-123');
    expect(orgChannel('   ', 'abc-123')).toBe('abc-123');
  });

  it('trims the org id', () => {
    expect(orgChannel(' acme ', 'x')).toBe('org:acme:x');
  });

  it('round-trips through parseChannelOrg (gateway deriveChannelOrg mirror)', () => {
    expect(parseChannelOrg(orgChannel('acme', 'abc-123'))).toBe('acme');
    expect(parseChannelOrg('abc-123')).toBeNull();
    expect(parseChannelOrg('org::x')).toBeNull();
    expect(parseChannelOrg('org:solo')).toBe('solo');
  });
});
