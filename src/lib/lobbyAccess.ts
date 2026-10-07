// platform-api/src/lib/lobbyAccess.ts
//
// NFR #175 (realtime-examples Loop 29c access sweep): may this caller use a
// call lobby — open or join its video session, read its sessions, recording
// state, reactions, or captions? Before this every /api/video route took any
// lobby name from any logged-in user, so a stranger could join a private DM's
// call and get a media token.
//
// The rules are the ones the rest of the platform already applies to the same
// names (no new ACL model):
//   dm:<a>:<b>[…]   the people the name lists (`dmLobbyParticipants`, as
//                   `recording:listByLobby`) — or a GUEST of the lobby's live
//                   call (below)
//   dmg:<hash>      a member of the group DM's chat channel `chat:dmg:<hash>`
//                   (`isChannelMember`, NFR #151) — or a guest of its live call
//
// Guests (join-knock, 2026-09-30). A DM's call is private, but it is not
// closed: someone already allowed on it may bring a third person in — by
// admitting their knock or by inviting them from the call's People control.
// That person is recorded as a guest on the lobby's ACTIVE session
// (`POST /api/video/sessions/guests`, which only a person allowed on the lobby
// may call) and is allowed on the lobby while that session is active. The
// grant dies with the call: a new call on the same DM has no guests.
//   room:<slug>     hangoutRooms' `assertCanAccess`: same org, and the room is
//                   org-visible, or the caller created it or is a member
//   <tenant>:room:<slug>
//                   the room `<slug>` in partition `<tenant>` (a tenant the
//                   caller may use): public → any caller of the tenant;
//                   private → its creator and members only. No such room →
//                   the lobby's rule as before. A private room's non-member
//                   is refused with the rooms route's 403
//                   (`RoomAccessDeniedError`), through the rooms route or the
//                   video API alike.
//   doc:<id> / <id> when a document by that id exists: #135 `document:read`
//   anything else   an ad-hoc lobby with no owner (the global hangout, a
//                   random-UUID call) — open to any logged-in user, as before
//
// Tenant-prefixed lobbies (OrgIQ, 2026-09-29). OrgIQ names a lobby
// `<tenant>:<rest>` — `orgiq:dm:u-nora:u-vera` from
// `dmLobbyName(ids, { prefix: 'orgiq:' })` — and its middleware gateway
// (`lobbyGuard`, aws-agentcore middleware/src/lib/realtime-gateway.ts) allows
// a lobby only under the caller's own `<org>:` prefix, and an `<org>:dm:<ids>`
// lobby only for one of those ids. The same rule here:
//   caller holds an OrgIQ realtime token (`authKind: 'orgiq-hs256'`)
//                   → exactly `lobbyGuard`: the lobby must be `<tenant>:<rest>`
//                     for a tenant the caller may use — any one in the
//                     token's `tenants` claim, or its `org` when the token
//                     carries no `tenants` (lib/partition.ts);
//                     `<tenant>:dm:<ids>` for a listed id only;
//                     `<tenant>:room:<slug>` by the room rule above; anything
//                     else under the prefix is open to the tenant. No unprefixed
//                     lobby (dm:, room:, a document id).
//   any other caller naming a `<tenant>:dm:<ids>` lobby
//                   → a listed id AND the caller may use `<tenant>` (its own
//                     org); a tenant DM is never an open ad-hoc lobby.
//
// Per-resource authority (LOBBY_AUTHORITY_REQUIRED, 2026-10-06). The prefix
// rule above cannot know whether the caller may use THAT initiative, team,
// run or private channel. With LOBBY_AUTHORITY_REQUIRED=true an OrgIQ token's
// lobby must ALSO be admitted by the OrgIQ BFF, asked per request
// (lib/lobbyAuthority.ts; the caller's own bearer, POST LOBBY_AUTHORITY_URL).
// Any failure refuses. While on, a lobby under one of the BFF's own tenant
// prefixes (LOBBY_AUTHORITY_TENANTS, default orgiq/assessment/social/
// pipelines) is refused to every non-OrgIQ caller. Off (default) = the prefix
// rule alone, as before.
//
// A refusal is 404: confirming that a DM has a call is itself a disclosure.
// The one exception is a private room's non-member, who gets the 403 the
// rooms route already gives them for that room (nothing new is disclosed).

import type { Request } from 'express';
import { AppError, NotFoundError } from '../middleware/error-handler';
import { RoomAccessDeniedError } from '../repositories/HangoutRoomRepository';
import { dmLobbyParticipants } from './authorize';
import { mayUsePartition } from './partition';
import { askLobbyAuthority, lobbyAuthorityRequired, lobbyAuthorityTenants } from './lobbyAuthority';

export interface LobbyAccessDeps {
  roomBySlug(orgId: string, slug: string): Promise<{ roomId: string; orgId: string; visibility?: string; createdBy?: string } | null>;
  isRoomMember(roomId: string, userId: string): Promise<boolean>;
  isChannelMember(channel: string, userId: string): Promise<boolean>;
  /** The document's owner when a document (or lifecycle row) by this id exists. */
  documentExists(documentId: string): Promise<boolean>;
  /** Throws (401/404) unless the caller may read the document. */
  assertCanReadDocument(req: Request, documentId: string): Promise<void>;
  /**
   * Whether `userId` is a guest of the lobby's active call — someone a person
   * allowed on it admitted or invited. Absent → nobody is a guest.
   */
  isLobbyGuest?(lobbyName: string, userId: string): Promise<boolean>;
  /**
   * The OrgIQ BFF's per-resource admission for an OrgIQ-token caller (see the
   * header). Absent → no per-resource check (the prefix rule alone). Asked
   * only after the prefix rule admits; a throw is a refusal.
   */
  orgiqLobbyAuthority?(req: Request, lobbyName: string): Promise<boolean>;
  /**
   * Is `<tenant>:` a partition only the OrgIQ BFF may admit to (see
   * orgiqLobbyAuthority)? True → any other kind of caller is refused there.
   */
  governedTenant?(tenant: string): boolean;
}

/**
 * A room lobby's standing for this caller: no such room in the partition,
 * open to the caller (public, or it is the creator / a member), or private
 * to others.
 */
type RoomVerdict = 'none' | 'open' | 'private';

/** The first segments that are lobby KINDS, never a tenant. */
const LOBBY_KINDS = new Set(['dm', 'dmg', 'room', 'doc']);

/**
 * `<tenant>:<rest>` → `{ tenant, rest }`, or null when the name has no tenant
 * prefix (no colon, or its first segment is a lobby kind like `dm`/`room`).
 */
export function tenantLobby(lobbyName: string): { tenant: string; rest: string } | null {
  const i = lobbyName.indexOf(':');
  if (i <= 0) return null;
  const tenant = lobbyName.slice(0, i);
  if (LOBBY_KINDS.has(tenant)) return null;
  return { tenant, rest: lobbyName.slice(i + 1) };
}

/** A DM rest (`dm:<a>:<b>…`, or a bare `dm`) → its ids; null when not a DM. */
function dmIds(rest: string): string[] | null {
  if (rest !== 'dm' && !rest.startsWith('dm:')) return null;
  return rest.slice(3).split(':').filter(Boolean);
}

/**
 * The middleware gateway's `lobbyGuard`, verbatim in effect: only lobbies
 * under a `<tenant>:` prefix the caller may use (any tenant in the token's
 * `tenants`, else its `org`); `<tenant>:dm:<ids>` only for one of the ids;
 * anything else under the prefix is open to the tenant.
 */
export function orgiqLobbyGuard(
  user: { sub?: string; orgId?: string; tenants?: string[] } | undefined,
  lobbyName: string,
): boolean {
  const userId = user?.sub ?? '';
  if (!userId) return false;
  const tenanted = tenantLobby(lobbyName);
  if (!tenanted || !tenanted.rest) return false;
  if (!mayUsePartition(user, tenanted.tenant)) return false;
  const ids = dmIds(tenanted.rest);
  return ids ? ids.includes(userId) : true;
}

function lookupOrg(req: Request): string {
  return req.user?.orgId ?? (process.env.SKIP_AUTH === 'true' ? 'default-org' : '');
}

export function makeLobbyAccess(deps: LobbyAccessDeps) {
  /**
   * One room lookup per (request, partition, slug): a route that checks the
   * same lobby twice (assert, then a list filter) reads the room once.
   */
  const roomCache = new WeakMap<Request, Map<string, Promise<RoomVerdict>>>();

  function roomVerdict(req: Request, partition: string, slug: string, userId: string): Promise<RoomVerdict> {
    let perReq = roomCache.get(req);
    if (!perReq) { perReq = new Map(); roomCache.set(req, perReq); }
    const key = `${partition}|${slug}`;
    let verdict = perReq.get(key);
    if (!verdict) {
      verdict = (async (): Promise<RoomVerdict> => {
        const room = partition ? await deps.roomBySlug(partition, slug).catch(() => null) : null;
        if (!room || room.orgId !== partition) return 'none';
        if (room.visibility === 'org' || room.createdBy === userId) return 'open';
        return (await deps.isRoomMember(room.roomId, userId).catch(() => false)) ? 'open' : 'private';
      })();
      perReq.set(key, verdict);
    }
    return verdict;
  }

  /** One BFF question per (request, lobby): assert + list filter ask once. */
  const authorityCache = new WeakMap<Request, Map<string, Promise<boolean>>>();
  function orgiqAuthority(req: Request, lobbyName: string): Promise<boolean> {
    if (!deps.orgiqLobbyAuthority) return Promise.resolve(true);
    let perReq = authorityCache.get(req);
    if (!perReq) { perReq = new Map(); authorityCache.set(req, perReq); }
    let verdict = perReq.get(lobbyName);
    if (!verdict) {
      verdict = deps.orgiqLobbyAuthority(req, lobbyName).then((ok) => ok === true, () => false);
      perReq.set(lobbyName, verdict);
    }
    return verdict;
  }

  /** A guest of the lobby's live call (see the header). Failure = not a guest. */
  function isGuest(lobbyName: string, userId: string): Promise<boolean> {
    return deps.isLobbyGuest ? deps.isLobbyGuest(lobbyName, userId).catch(() => false) : Promise.resolve(false);
  }

  /** The room slug of a `room:<slug>` rest, or null. */
  function roomSlug(rest: string): string | null {
    return rest.startsWith('room:') && rest.length > 'room:'.length ? rest.slice('room:'.length) : null;
  }

  /** true = allowed; false = refused (404); an AppError = refused with that error. */
  async function decide(req: Request, lobbyName: string): Promise<boolean | AppError> {
    const userId = req.user?.sub ?? '';
    if (!userId || !lobbyName) return false;
    const tenanted = tenantLobby(lobbyName);
    const tenantRoom = tenanted ? roomSlug(tenanted.rest) : null;

    if (req.user?.authKind === 'orgiq-hs256') {
      if (!orgiqLobbyGuard(req.user, lobbyName)) return false;
      if (!(await orgiqAuthority(req, lobbyName))) return false;
      if (!tenanted || !tenantRoom) return true;
      // A tenant grant opens the tenant's public rooms, not its private ones.
      const v = await roomVerdict(req, tenanted.tenant, tenantRoom, userId);
      return v === 'private' ? new RoomAccessDeniedError(tenantRoom) : true;
    }

    // A lobby under a BFF-governed tenant prefix is reachable only through an
    // OrgIQ token while the per-resource check is on: a Cognito or SKIP_AUTH
    // dev bearer naming `orgiq:initiative:<id>` would otherwise fall through
    // to the open ad-hoc rule below and skip the BFF entirely.
    if (tenanted && deps.governedTenant?.(tenanted.tenant)) return false;

    const tenantDm = tenanted ? dmIds(tenanted.rest) : null;
    if (tenanted && tenantDm) {
      if (!mayUsePartition(req.user, tenanted.tenant)) return false;
      return tenantDm.includes(userId) || (tenantDm.length > 0 && isGuest(lobbyName, userId));
    }
    if (tenanted && tenantRoom) {
      const v = await roomVerdict(req, tenanted.tenant, tenantRoom, userId);
      if (v !== 'none') {
        if (!mayUsePartition(req.user, tenanted.tenant)) return false;
        return v === 'private' ? new RoomAccessDeniedError(tenantRoom) : true;
      }
      // No such room: today's rule (an ad-hoc lobby) below.
    }
    const dm = dmLobbyParticipants(lobbyName);
    if (dm) return dm.includes(userId) || isGuest(lobbyName, userId);
    if (lobbyName.startsWith('dm:')) return false; // a malformed DM name admits no one
    if (lobbyName.startsWith('dmg:')) {
      return (await deps.isChannelMember(`chat:${lobbyName}`, userId).catch(() => false)) || isGuest(lobbyName, userId);
    }
    if (lobbyName.startsWith('room:')) {
      const slug = lobbyName.slice('room:'.length);
      const v = await roomVerdict(req, lookupOrg(req), slug, userId);
      if (v === 'none') return false;
      return v === 'private' ? new RoomAccessDeniedError(slug) : true;
    }
    const documentId = lobbyName.startsWith('doc:') ? lobbyName.slice('doc:'.length) : lobbyName;
    if (await deps.documentExists(documentId).catch(() => false)) {
      try { await deps.assertCanReadDocument(req, documentId); return true; } catch { return false; }
    }
    return !lobbyName.startsWith('doc:');
  }

  return {
    async canUseLobby(req: Request, lobbyName: string): Promise<boolean> {
      return (await decide(req, lobbyName)) === true;
    },
    /**
     * Throws unless the caller may use the lobby: 404 in general; a private
     * room's non-member gets the rooms route's own 403 (`RoomAccessDeniedError`).
     */
    async assertLobbyAccess(req: Request, lobbyName: string): Promise<void> {
      const verdict = await decide(req, lobbyName);
      if (verdict === true) return;
      if (verdict instanceof AppError) throw verdict;
      throw new NotFoundError('Session not found');
    },
  };
}

let defaultAccess: ReturnType<typeof makeLobbyAccess> | null = null;

/** The process-wide lobby check over the real repositories (lazy for tests). */
export function lobbyAccess(): ReturnType<typeof makeLobbyAccess> {
  if (defaultAccess) return defaultAccess;
  /* eslint-disable @typescript-eslint/no-require-imports */
  const repos = require('../repositories') as typeof import('../repositories');
  const { isChannelMember } = require('../deck/documentEditAccess') as typeof import('../deck/documentEditAccess');
  const { documentReadGate } = require('../documents/documentReadGate') as typeof import('../documents/documentReadGate');
  /* eslint-enable @typescript-eslint/no-require-imports */
  defaultAccess = makeLobbyAccess({
    roomBySlug: async (orgId, slug) => (await repos.hangoutRoomRepo.getRoomBySlug(orgId, slug)) as never,
    isRoomMember: (roomId, userId) => repos.hangoutRoomMemberRepo.isMember(roomId, userId),
    isChannelMember,
    documentExists: async (id) => (await documentReadGate().ownerOf(id)) !== undefined,
    assertCanReadDocument: async (req, id) => { await documentReadGate().assertCanRead(req, id); },
    isLobbyGuest: async (lobbyName, userId) => {
      const live = await repos.videoSessionRepo.findActiveByLobbyName(lobbyName);
      return !!live?.guests?.includes(userId);
    },
    // Read live, so the flag needs no restart-order games in tests; off →
    // admitted here and the prefix rule alone decides (today's behaviour).
    orgiqLobbyAuthority: async (req, lobbyName) => {
      if (!lobbyAuthorityRequired()) return true;
      const h = req.headers.authorization;
      return askLobbyAuthority(h?.startsWith('Bearer ') ? h.slice(7) : '', lobbyName);
    },
    governedTenant: (tenant) => lobbyAuthorityRequired() && lobbyAuthorityTenants().includes(tenant),
  });
  return defaultAccess;
}
