// platform-api/src/lib/partition.ts
//
// WHICH PARTITIONS may this caller use? A partition is the tenant a room,
// team or call lobby lives in: a hangout room's / team's `orgId` (their PK),
// or a call lobby's `<tenant>:` prefix (`social:dm:u-a:u-b`).
//
// The rule (aws-agentcore social tenants, 2026-09-29):
//   - a caller whose realtime token carries a `tenants` claim may use ANY
//     tenant listed there (`['org-a', 'social']`) — `social` is a first-class
//     partition beside the caller's `org`;
//   - a caller without one (a Cognito token, an OrgIQ token that predates
//     the claim) may use only its own `orgId`, exactly as before.
//
// Routes that let a request name its partition (`?tenant=` / body `tenant`)
// resolve it through `requestedPartition` and refuse it unless
// `mayUsePartition` says yes; naming none keeps the caller's own org.

import type { Request } from 'express';

type PartitionUser = { orgId?: string; tenants?: string[] } | undefined;

/** Every partition this caller may use (empty when it has none). */
export function callerPartitions(user: PartitionUser): string[] {
  if (Array.isArray(user?.tenants) && user.tenants.length > 0) return user.tenants;
  return user?.orgId ? [user.orgId] : [];
}

/** May this caller use `partition`? */
export function mayUsePartition(user: PartitionUser, partition: string): boolean {
  return !!partition && callerPartitions(user).includes(partition);
}

/**
 * The partition a request names, if any: `?tenant=` or a body `tenant`
 * (trimmed; empty counts as none).
 */
export function requestedPartition(req: Request): string | undefined {
  const fromQuery = typeof req.query?.tenant === 'string' ? req.query.tenant : undefined;
  const body = req.body as { tenant?: unknown } | undefined;
  const fromBody = body && typeof body === 'object' && typeof body.tenant === 'string' ? body.tenant : undefined;
  const raw = (fromQuery ?? fromBody ?? '').trim();
  return raw || undefined;
}
