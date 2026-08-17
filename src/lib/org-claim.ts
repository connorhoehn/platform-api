// lib/org-claim.ts
//
// MIRROR of websocket-gateway/src/utils/org-claim.ts (itself a mirror of
// cell-router/src/shard-key.ts). These are SEPARATE npm packages, so they
// cannot share a literal import without a workspace/publish change. Keep the
// key list BYTE-IDENTICAL to both — enforced by
// src/lib/__tests__/org-claim-parity.test.ts (reads the gateway sibling
// checkout when present). If platform-api mints a channel under org-X while
// the gateway authorizes the same token as org-Y (or org-less), the tenant
// gate either blocks legitimate traffic or silently fails open — the list is
// the load-bearing contract.

/** Org/tenant claim names, in precedence order. MUST match websocket-gateway/src/utils/org-claim.ts. */
export const ORG_CLAIM_KEYS = ['custom:orgId', 'orgId', 'org_id', 'org', 'custom:org_id'] as const;

/**
 * Read the org/tenant id from decoded JWT claims. Returns `undefined` when no
 * org claim is present (an org-less token). Callers MUST treat absence as
 * "org-less / unprefixed" — NEVER substitute the cell-router's `default-org`
 * placement fallback, which would fold every org-less token into one shared
 * tenant.
 */
export function readOrgClaim(decoded: Record<string, unknown> | null | undefined): string | undefined {
  if (!decoded) return undefined;
  for (const k of ORG_CLAIM_KEYS) {
    const v = decoded[k];
    if (typeof v === 'string' && v.trim() !== '') return v.trim();
  }
  return undefined;
}
