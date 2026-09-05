// lib/authorize.ts
//
// THE POLICY DECISION POINT.
//
// Authorization in this service was ~11 inline `req.user.sub === owner`
// comparisons scattered across routes and services, plus a handful of
// middlewares. Each one is defensible on its own; collectively they are a
// shape problem:
//   - There is no list of what rules exist, so nobody can audit them.
//   - Every new route re-derives the rule, and the one that forgets is a
//     hole (see `recording:listByLobby` — its own comment admitted that
//     "any authenticated user can list a room's recordings" and leaned on
//     the UI hiding the link, which is not security).
//   - Swapping in a real policy engine later means finding every site
//     first, rather than replacing one backend.
//
// So: ONE call. `authorize({ principal, action, resource })` → Decision.
// Today it is backed by plain TypeScript policies registered below. That is
// deliberate — no external engine, no cloud dependency. What matters is the
// SHAPE: principal / action / resource / decision, which is the vocabulary
// every policy engine speaks, so a future swap is a backend change instead
// of an audit.
//
// TENANCY IS ENFORCED BY THE FRAMEWORK, NOT BY EACH POLICY.
// A cross-tenant request is denied before any policy runs. Individual
// policies therefore cannot forget the tenant check — the same reasoning as
// putting the tenant in the partition key (see lib/tenant.ts): make the
// safe thing structural rather than remembered.

import { AppError } from '../middleware/error-handler';
import { TENANTLESS, type TenantId } from './tenant';

export interface Principal {
  /** Stable user id (the JWT `sub`). */
  userId: string;
  /** Resolved tenant — see lib/tenant.ts. */
  tenant: TenantId;
}

export interface Resource {
  /** e.g. 'lobby', 'document', 'recording'. */
  type: string;
  /** Natural id, when the action targets one. */
  id?: string;
  /**
   * Owning tenant, when known. When present it MUST match the principal's
   * tenant or the request is denied before any policy runs.
   */
  tenant?: TenantId;
  /** Anything a policy needs (ownerId, memberIds, …). */
  attrs?: Record<string, unknown>;
}

export interface AuthorizeRequest {
  principal: Principal;
  /** `<resource>:<verb>`, e.g. 'recording:listByLobby'. */
  action: string;
  resource: Resource;
}

export type Decision =
  | { allow: true }
  | { allow: false; reason: string; status: 403 | 404 };

export const ALLOW: Decision = { allow: true };

/**
 * Deny helper. `status` matters: use 404 when confirming EXISTENCE would
 * itself leak (e.g. "this DM exists and you're not in it"), 403 when the
 * resource is legitimately discoverable and the actor simply lacks rights.
 */
export function deny(reason: string, status: 403 | 404 = 403): Decision {
  return { allow: false, reason, status };
}

export type Policy = (req: AuthorizeRequest) => Decision | Promise<Decision>;

const policies = new Map<string, Policy>();

/** Register the policy for an action. Re-registering replaces it (tests). */
export function registerPolicy(action: string, policy: Policy): void {
  policies.set(action, policy);
}

/** Every registered action — for audit/debug surfaces and tests. */
export function registeredActions(): string[] {
  return [...policies.keys()].sort();
}

/**
 * The single decision point.
 *
 * DEFAULT DENY: an action with no registered policy is refused. A typo'd or
 * not-yet-written action must never fail open — that is the failure mode
 * this whole module exists to make impossible.
 */
export async function authorize(req: AuthorizeRequest): Promise<Decision> {
  // 1. Tenant isolation, enforced for every action before any policy runs.
  if (req.resource.tenant !== undefined && req.resource.tenant !== req.principal.tenant) {
    // 404, not 403: telling a stranger "that exists, in another tenant" is
    // itself a cross-tenant disclosure.
    return deny(
      `cross-tenant access refused for ${req.action}`,
      404,
    );
  }

  // 2. A principal with no identity has no rights.
  if (!req.principal.userId) {
    return deny(`${req.action} requires an authenticated principal`);
  }

  const policy = policies.get(req.action);
  if (!policy) {
    return deny(`no policy registered for action ${JSON.stringify(req.action)}`);
  }
  return policy(req);
}

/**
 * Throwable form for route handlers. Extends AppError so the existing
 * error middleware maps the status with no special-casing — a denial that
 * came back as a 500 would read as a bug rather than a refusal.
 */
export class AuthorizationError extends AppError {
  constructor(message: string, status: 403 | 404) {
    super(status, message);
    this.name = 'AuthorizationError';
  }
}

/** `authorize`, but throws `AuthorizationError` on deny. */
export async function requireAuthorized(req: AuthorizeRequest): Promise<void> {
  const decision = await authorize(req);
  if (!decision.allow) {
    throw new AuthorizationError(decision.reason, decision.status);
  }
}

// ---------------------------------------------------------------------------
// Lobby membership — the first real policy.
// ---------------------------------------------------------------------------

/**
 * Participants encoded in a lobby name, when the name carries them.
 *
 * A DM lobby is `dm:<userA>:<userB>` (ids sorted by the minting side), so
 * membership is decidable from the name alone with no lookup. A `room:`
 * lobby is NOT self-describing — membership lives in the room's member
 * list — so this returns null and the caller must supply `memberIds`.
 */
export function dmLobbyParticipants(lobbyName: string): string[] | null {
  if (!lobbyName.startsWith('dm:')) return null;
  const parts = lobbyName.slice('dm:'.length).split(':').filter(Boolean);
  return parts.length >= 2 ? parts : null;
}

/**
 * `recording:listByLobby` — may this principal list a lobby's recordings?
 *
 * Before this existed the route allowed ANY authenticated user to list ANY
 * lobby's recordings, including a private DM between two other people. Its
 * own comment said so and pointed at the UI hiding the link.
 */
registerPolicy('recording:listByLobby', (req) => {
  const lobbyName = req.resource.id ?? '';
  if (!lobbyName) return deny('lobbyName is required');

  const dmMembers = dmLobbyParticipants(lobbyName);
  if (dmMembers) {
    return dmMembers.includes(req.principal.userId)
      ? ALLOW
      // 404: confirming a DM between two named people exists is itself a
      // disclosure to someone who isn't in it.
      : deny('not a participant in this conversation', 404);
  }

  // An ORG-VISIBLE room is readable by the org — that is what the field
  // means. Membership alone denied everyone here, including the room's own
  // creator: an org room has no member ROWS (nobody has to join what is
  // already open to them), so `memberIds` came back empty and matched nobody.
  // The route resolves the room by the caller's own orgId, so a room that was
  // found at all is one the caller shares an org with; this grants exactly
  // what `visibility: 'org'` already declares and nothing wider.
  if (req.resource.attrs?.visibility === 'org') return ALLOW;

  // Non-DM lobbies (rooms, ad-hoc hangouts): the caller supplies the member
  // list. Absent means "could not establish membership" → deny, never allow.
  const memberIds = req.resource.attrs?.memberIds;
  if (!Array.isArray(memberIds)) {
    return deny('membership for this lobby could not be established');
  }
  return memberIds.includes(req.principal.userId)
    ? ALLOW
    : deny('not a member of this room');
});

/**
 * `recording:read` — may this principal see this one recording?
 *
 * Registered because the lookup behind it got WIDER. It used to be
 * `listByUser(caller, { limit: 100 }).find(...)`, where the caller's own
 * fan-out index was both the lookup and, incidentally, the access check —
 * you could only find what was already yours. That incidental coupling is
 * gone: the route now point-queries the recording by id first (via the
 * `recordingId` GSI, so a recording past the caller's 100th no longer
 * 404s), and asks about membership separately. Membership therefore has to
 * be an explicit decision, and this is where it lives.
 *
 * The caller passes `isParticipant` — the existence of the caller's row in
 * the participant fan-out table. Anything other than an explicit `true`
 * denies, so a lookup that failed or was never performed cannot fall open.
 *
 * 404, not 403: the id is a uuid, so a caller asking about a recording they
 * were not in either guessed it or was given it. Answering 403 confirms
 * that a recording with that id exists — and since recordings are made of
 * DMs between named people, that is the same disclosure
 * `recording:listByLobby` refuses above. Same reasoning, same status.
 */
registerPolicy('recording:read', (req) => {
  return req.resource.attrs?.isParticipant === true
    ? ALLOW
    : deny('recording not found', 404);
});

/** Build a Principal from an authenticated request's user. */
export function principalOf(
  user: { sub?: string; orgId?: string } | undefined,
  tenant: TenantId,
): Principal {
  return { userId: user?.sub ?? '', tenant: tenant ?? TENANTLESS };
}
