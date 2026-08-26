// lib/tenant.ts
//
// THE TENANT KEY CONVENTION.
//
// Tenant identity already exists (`readOrgClaim` reads the org claim off the
// JWT). What did not exist is a rule for how that tenant reaches STORAGE.
// Without one, tenancy degrades into a query-time `WHERE orgId = ?` filter
// that every new query has to remember — and the first one that forgets is a
// cross-tenant data leak with no error and no test failure.
//
// The rule: a tenant is part of the PARTITION KEY, not an attribute.
//
//   scopedKey('acme', 'doc-42')  ->  'acme|doc-42'
//
// Why the partition key and not an attribute:
//   - Isolation is structural. A query for tenant A physically cannot return
//     tenant B's rows, because it is a different partition. There is nothing
//     to forget.
//   - It is the only shape that stays correct under Scan, under a GSI, and
//     under a future migration to a per-tenant table or store.
//   - `HangoutRoomRepository` already does this (PK: orgId) and is the model;
//     every other table treated orgId as an attribute, or omitted it.
//
// ORG-LESS TOKENS ARE THEIR OWN TENANT, NOT A SHARED ONE.
// `readOrgClaim` deliberately returns undefined for a token with no org
// claim, and warns against substituting a `default-org` placeholder — doing
// that folds every unclaimed token into one bucket, which is the exact
// cross-tenant leak this module exists to prevent. Here that intent becomes
// a concrete key: org-less rows live under the reserved `~` tenant, which
// cannot collide with a real org id (see `assertTenantComponent`).
//
// LOCAL/SINGLE-TENANT DEV still goes through all of this — it simply runs
// with one tenant. That is the point: the multi-tenant path is the ONLY path,
// so it cannot rot while nobody is looking at it.

/** A resolved tenant. Opaque on purpose — build it with `tenantOf`. */
export type TenantId = string & { readonly __tenant: unique symbol };

/**
 * Separates tenant from natural id in a composite key.
 *
 * `|` is chosen because it is not legal in the id shapes this system mints
 * (ULIDs, uuids, slugs, `dm:a:b` lobby names, ISO timestamps) and not legal
 * in the org claims we accept. Any component containing it is REJECTED rather
 * than escaped — see the collision note on `assertTenantComponent`.
 */
export const TENANT_DELIM = '|';

/**
 * Tenant for principals whose token carries no org claim. Reserved: `~` is
 * rejected by `assertTenantComponent`, so no real org can ever land here.
 */
export const TENANTLESS = '~' as TenantId;

/**
 * Reject anything that would make a composite key ambiguous.
 *
 * This is the load-bearing check. Without it, `org='a|b' + id='c'` and
 * `org='a' + id='b|c'` both produce `'a|b|c'` — two different tenants
 * addressing one row. That is a silent cross-tenant read AND write. We refuse
 * the input instead of escaping it, because an escape scheme is one more
 * thing that can be implemented inconsistently on the parse side.
 */
function assertTenantComponent(value: string, what: 'tenant' | 'id'): void {
  if (value.length === 0) {
    throw new Error(`tenant key: ${what} must not be empty`);
  }
  if (value.includes(TENANT_DELIM)) {
    throw new Error(
      `tenant key: ${what} must not contain ${TENANT_DELIM.repeat(1)} (got ${JSON.stringify(value)}) — ` +
        'it would make the composite key ambiguous across tenants',
    );
  }
}

/**
 * Resolve a request's org claim into a tenant. `undefined` (an org-less
 * token) maps to the reserved TENANTLESS tenant — its own namespace, never a
 * shared one.
 */
export function tenantOf(orgId: string | null | undefined): TenantId {
  if (orgId == null || orgId.trim() === '') return TENANTLESS;
  const trimmed = orgId.trim();
  if (trimmed === TENANTLESS) {
    // A caller must not be able to claim the reserved namespace and read
    // every org-less row in the system.
    throw new Error(`tenant key: ${JSON.stringify(TENANTLESS)} is reserved for org-less principals`);
  }
  assertTenantComponent(trimmed, 'tenant');
  return trimmed as TenantId;
}

/**
 * Compose a tenant-scoped partition key.
 *
 * Use for tables whose natural key is a point-lookup id (documentId,
 * lobbyName, callId). For list-heavy tables where the tenant IS the partition
 * and the natural id is the sort key, use the tenant directly as PK instead —
 * that is what HangoutRoomRepository does, and it is equally correct.
 */
export function scopedKey(tenant: TenantId, id: string): string {
  assertTenantComponent(tenant, 'tenant');
  assertTenantComponent(id, 'id');
  return `${tenant}${TENANT_DELIM}${id}`;
}

/**
 * Split a composite key back into its parts.
 *
 * Splits on the FIRST delimiter only — but since neither component may
 * contain the delimiter, a well-formed key has exactly one. A key with more
 * is rejected: it did not come from `scopedKey`, and guessing which side the
 * extra delimiter belongs to is how you serve one tenant another's row.
 */
export function parseScopedKey(key: string): { tenant: TenantId; id: string } {
  const parts = key.split(TENANT_DELIM);
  if (parts.length !== 2 || !parts[0] || !parts[1]) {
    throw new Error(`tenant key: malformed composite key ${JSON.stringify(key)}`);
  }
  return { tenant: parts[0] as TenantId, id: parts[1] };
}

/** True when `key` belongs to `tenant`. For assertions on read paths. */
export function keyBelongsTo(key: string, tenant: TenantId): boolean {
  try {
    return parseScopedKey(key).tenant === tenant;
  } catch {
    return false;
  }
}
