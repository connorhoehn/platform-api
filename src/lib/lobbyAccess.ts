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
//                   `recording:listByLobby`)
//   dmg:<hash>      a member of the group DM's chat channel `chat:dmg:<hash>`
//                   (`isChannelMember`, NFR #151)
//   room:<slug>     hangoutRooms' `assertCanAccess`: same org, and the room is
//                   org-visible, or the caller created it or is a member
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
//                   → exactly `lobbyGuard`: the lobby must be `<org>:<rest>`
//                     for the token's `org`; `<org>:dm:<ids>` for a listed id
//                     only; anything else under the prefix is open to the
//                     tenant. No unprefixed lobby (dm:, room:, a document id).
//   any other caller naming a `<tenant>:dm:<ids>` lobby
//                   → a listed id AND the caller's own org is `<tenant>`;
//                     a tenant DM is never an open ad-hoc lobby.
//
// A refusal is 404: confirming that a DM or private room has a call is itself
// a disclosure.

import type { Request } from 'express';
import { NotFoundError } from '../middleware/error-handler';
import { dmLobbyParticipants } from './authorize';

export interface LobbyAccessDeps {
  roomBySlug(orgId: string, slug: string): Promise<{ roomId: string; orgId: string; visibility?: string; createdBy?: string } | null>;
  isRoomMember(roomId: string, userId: string): Promise<boolean>;
  isChannelMember(channel: string, userId: string): Promise<boolean>;
  /** The document's owner when a document (or lifecycle row) by this id exists. */
  documentExists(documentId: string): Promise<boolean>;
  /** Throws (401/404) unless the caller may read the document. */
  assertCanReadDocument(req: Request, documentId: string): Promise<void>;
}

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
 * under the caller's own `<org>:` prefix; `<org>:dm:<ids>` only for one of
 * the ids; anything else under the prefix is open to the tenant.
 */
export function orgiqLobbyGuard(user: { sub?: string; orgId?: string } | undefined, lobbyName: string): boolean {
  const org = user?.orgId ?? '';
  const userId = user?.sub ?? '';
  if (!org || !userId) return false;
  const prefix = `${org}:`;
  if (!lobbyName.startsWith(prefix)) return false;
  const rest = lobbyName.slice(prefix.length);
  if (!rest) return false;
  const ids = dmIds(rest);
  return ids ? ids.includes(userId) : true;
}

function lookupOrg(req: Request): string {
  return req.user?.orgId ?? (process.env.SKIP_AUTH === 'true' ? 'default-org' : '');
}

export function makeLobbyAccess(deps: LobbyAccessDeps) {
  async function canUseLobby(req: Request, lobbyName: string): Promise<boolean> {
    const userId = req.user?.sub ?? '';
    if (!userId || !lobbyName) return false;
    if (req.user?.authKind === 'orgiq-hs256') return orgiqLobbyGuard(req.user, lobbyName);
    const tenanted = tenantLobby(lobbyName);
    const tenantDm = tenanted ? dmIds(tenanted.rest) : null;
    if (tenanted && tenantDm) return tenantDm.includes(userId) && req.user?.orgId === tenanted.tenant;
    const dm = dmLobbyParticipants(lobbyName);
    if (dm) return dm.includes(userId);
    if (lobbyName.startsWith('dm:')) return false; // a malformed DM name admits no one
    if (lobbyName.startsWith('dmg:')) return deps.isChannelMember(`chat:${lobbyName}`, userId).catch(() => false);
    if (lobbyName.startsWith('room:')) {
      const orgId = lookupOrg(req);
      const room = await deps.roomBySlug(orgId, lobbyName.slice('room:'.length)).catch(() => null);
      if (!room || room.orgId !== orgId) return false;
      if (room.visibility === 'org' || room.createdBy === userId) return true;
      return deps.isRoomMember(room.roomId, userId).catch(() => false);
    }
    const documentId = lobbyName.startsWith('doc:') ? lobbyName.slice('doc:'.length) : lobbyName;
    if (await deps.documentExists(documentId).catch(() => false)) {
      try { await deps.assertCanReadDocument(req, documentId); return true; } catch { return false; }
    }
    return !lobbyName.startsWith('doc:');
  }

  return {
    canUseLobby,
    /** Throws 404 unless the caller may use the lobby. */
    async assertLobbyAccess(req: Request, lobbyName: string): Promise<void> {
      if (!(await canUseLobby(req, lobbyName))) throw new NotFoundError('Session not found');
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
  });
  return defaultAccess;
}
