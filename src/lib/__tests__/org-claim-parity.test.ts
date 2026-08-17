// Drift guard (GAP B.2, mirrors the gateway's GAP B.1 guard):
// platform-api's ORG_CLAIM_KEYS (src/lib/org-claim.ts) MUST stay
// byte-identical to websocket-gateway/src/utils/org-claim.ts. They are
// separate npm packages so they can't share an import; if they drift,
// platform-api mints a channel under one org while the gateway authorizes the
// same token under another (or as org-less) → the tenant gate blocks
// legitimate traffic or silently fails open.
//
// The gateway file is read from the sibling checkout; the comparison is
// skipped (with a console note) on machines that don't have it.

import fs from 'fs';
import path from 'path';
import { ORG_CLAIM_KEYS, readOrgClaim } from '../org-claim';

const GATEWAY_ORG_CLAIM = path.join(
  __dirname, '..', '..', '..', '..', 'websocket-gateway', 'src', 'utils', 'org-claim.ts',
);

/** Extract the ORG_CLAIM_KEYS = [...] string-literal array from a source file. */
function extractKeys(file: string): string[] {
  const src = fs.readFileSync(file, 'utf8');
  const m = src.match(/ORG_CLAIM_KEYS\s*=\s*\[([^\]]*)\]/);
  if (!m) throw new Error(`ORG_CLAIM_KEYS not found in ${file}`);
  return m[1]
    .split(',')
    .map((s) => s.trim().replace(/^['"]|['"]$/g, ''))
    .filter(Boolean);
}

describe('ORG_CLAIM_KEYS parity (platform-api <-> websocket-gateway)', () => {
  const hasSibling = fs.existsSync(GATEWAY_ORG_CLAIM);
  (hasSibling ? test : test.skip)(
    'keys match the gateway org-claim keys exactly',
    () => {
      expect([...ORG_CLAIM_KEYS]).toEqual(extractKeys(GATEWAY_ORG_CLAIM));
    },
  );
});

describe('readOrgClaim', () => {
  it('reads the highest-precedence claim', () => {
    expect(readOrgClaim({ 'custom:orgId': 'a', orgId: 'b' })).toBe('a');
    expect(readOrgClaim({ org_id: 'c' })).toBe('c');
  });

  it('returns undefined for org-less / empty claims', () => {
    expect(readOrgClaim({})).toBeUndefined();
    expect(readOrgClaim(null)).toBeUndefined();
    expect(readOrgClaim({ orgId: '   ' })).toBeUndefined();
    expect(readOrgClaim({ orgId: 42 as unknown as string })).toBeUndefined();
  });

  it('trims claim values', () => {
    expect(readOrgClaim({ orgId: ' acme ' })).toBe('acme');
  });
});
