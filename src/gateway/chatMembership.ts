// A room's conversation follows the room's membership — the way a Teams
// channel follows its team. Rooms are platform-api's; chat membership is the
// gateway's (realtime-modules ChatService: `members` / `addMembers` /
// `removeMember` over the socket, keyed by channel `room:<slug>`). This is
// the mirror between the two, driven from the room routes.
//
// The mirror is best-effort: a room create or member change never fails
// because the gateway was unreachable — callers log and continue — and a
// private room converges lazily the next time it is loaded (`syncRoomOnce`).
// Org-visible rooms have no roster, so their conversation stays open.

import log from '../lib/logger';
import { GatewayServiceClient, type GatewayFrame, type ServiceIdentity } from './GatewayServiceClient';

export const roomChatChannel = (slug: string): string => `room:${slug}`;

export interface RoomChatMember {
  userId: string;
  displayName?: string;
}

export interface EnsureRoomChatMembersInput {
  slug: string;
  ownerId: string;
  ownerName?: string;
  members: RoomChatMember[];
  gatewayUrl?: string;
  /**
   * GAP-12: `members` is the room's COMPLETE roster, so anyone the gateway
   * has that this list doesn't is a stale addition to reconcile away — not
   * just a set of people to add.
   *
   * Most callers pass a partial list on purpose (e.g. "add this one new
   * member") and must NOT set this: reconciling removals against a partial
   * list would evict everyone the caller didn't happen to mention. Only set
   * it from a call site that just read the room's full membership from the
   * source of truth (see `syncRoomOnce` at hangoutRooms.ts's private-room
   * GET route).
   */
  authoritative?: boolean;
}

export interface RemoveRoomChatMemberInput {
  slug: string;
  userId: string;
  name?: string;
  byOwnerId: string;
  byOwnerName?: string;
  gatewayUrl?: string;
}

export interface ChatMembersView {
  open: boolean;
  members: Array<{ userId: string; role: string }>;
}

const REPLY_TIMEOUT_MS = 4000;

type ClientFactory = (identity: ServiceIdentity, gatewayUrl?: string) => GatewayServiceClient;
let clientFactory: ClientFactory = (identity, url) => new GatewayServiceClient(identity, url);
/** Tests swap the socket for a fake. */
export function _setClientFactory(f: ClientFactory | null): void {
  clientFactory = f ?? ((identity, url) => new GatewayServiceClient(identity, url));
}

function isFor(frame: GatewayFrame, channel: string): boolean {
  return frame.channel === channel;
}

function isChatError(frame: GatewayFrame, channel: string): boolean {
  return frame.type === 'error' && frame.service === 'chat' && isFor(frame, channel);
}

function errorMessage(frame: GatewayFrame): string {
  const e = frame.error as { code?: string; message?: string } | undefined;
  return e?.message ?? e?.code ?? 'the gateway refused';
}

/** Who the gateway says is in the channel, as the identity asking. */
export async function fetchChatMembers(client: GatewayServiceClient, channel: string): Promise<ChatMembersView> {
  const answer = client.waitFor(
    (f) => isFor(f, channel) && ((f.type === 'chat' && f.action === 'members') || isChatError(f, channel)),
    REPLY_TIMEOUT_MS,
  );
  client.send({ service: 'chat', action: 'members', channel });
  const frame = await answer;
  if (!frame) throw new Error(`gateway did not answer members for ${channel}`);
  if (isChatError(frame, channel)) throw new Error(errorMessage(frame));
  const raw = Array.isArray(frame.members) ? (frame.members as Array<Record<string, unknown>>) : [];
  return {
    open: frame.open === true,
    members: raw
      .filter((m) => typeof m.userId === 'string')
      .map((m) => ({ userId: m.userId as string, role: typeof m.role === 'string' ? m.role : 'member' })),
  };
}

/**
 * Make the room's conversation carry (at least) these people, as the
 * owner. The first add on an open channel makes the owner its owner and
 * closes it; everyone arriving through the room gets the whole history —
 * a room member is entitled to the room's past, unlike a person pulled
 * into a chat by hand. Returns who was added (empty when already in sync).
 *
 * When `input.authoritative` is set, also evicts anyone the gateway has
 * that `input.members` doesn't — the reconciliation a failed
 * `removeRoomChatMember` (see `mirrorRoomMemberRemoval`) never gets on its
 * own, since that call is fire-and-forget and not retried. Left unset
 * (the default), this only ever adds: a partial member list must never
 * drive removals, or it would evict everyone the caller didn't mention.
 */
export async function ensureRoomChatMembers(input: EnsureRoomChatMembersInput): Promise<{ added: string[]; removed: string[] }> {
  const channel = roomChatChannel(input.slug);
  const client = clientFactory({ userId: input.ownerId, displayName: input.ownerName ?? input.ownerId, kind: 'person' }, input.gatewayUrl);
  await client.connect();
  try {
    const current = await fetchChatMembers(client, channel);
    const present = new Set(current.open ? [] : current.members.map((m) => m.userId));
    const wanted = [...new Set([input.ownerId, ...input.members.map((m) => m.userId)])];
    const wantedSet = new Set(wanted);
    const missing = wanted.filter((id) => !present.has(id));
    // The owner is written by the gateway on the first add; adding them by
    // hand as well would make them a plain member of their own room.
    const toAdd = current.open ? missing.filter((id) => id !== input.ownerId) : missing;

    // An open channel has no meaningful member list (`present` is forced
    // empty above) — never treat that as "everyone is extra". And the
    // owner is always in `wanted` by construction, but the exclusion below
    // is kept explicit: this must never be the thing that removes them.
    const toRemove = input.authoritative && !current.open
      ? [...present].filter((id) => !wantedSet.has(id) && id !== input.ownerId)
      : [];

    let added: string[] = [];
    if (toAdd.length > 0) {
      const names: Record<string, string> = {};
      for (const m of input.members) if (m.displayName) names[m.userId] = m.displayName;
      if (input.ownerName) names[input.ownerId] = input.ownerName;
      const answer = client.waitFor(
        (f) => isFor(f, channel) && ((f.type === 'chat' && f.action === 'membersUpdated') || isChatError(f, channel)),
        REPLY_TIMEOUT_MS,
      );
      client.send({
        service: 'chat',
        action: 'addMembers',
        channel,
        userIds: toAdd,
        history: { mode: 'all' },
        ...(Object.keys(names).length ? { names } : {}),
      });
      const frame = await answer;
      if (!frame) throw new Error(`gateway did not answer addMembers for ${channel}`);
      if (isChatError(frame, channel)) throw new Error(errorMessage(frame));
      added = toAdd;
    }

    const removed: string[] = [];
    for (const userId of toRemove) {
      try {
        await removeOneChatMember(client, channel, userId);
        removed.push(userId);
      } catch (err) {
        // Best-effort, same as the rest of this module: leave it for the
        // next reconcile pass rather than failing the whole ensure over
        // one stale member.
        log.warn(
          { slug: input.slug, userId, err: err instanceof Error ? err.message : String(err) },
          'room chat membership reconcile-removal failed',
        );
      }
    }

    return { added, removed };
  } finally {
    await client.close();
  }
}

/** Send `removeMember` on an already-connected client and wait for it to land. */
async function removeOneChatMember(client: GatewayServiceClient, channel: string, userId: string, name?: string): Promise<void> {
  const answer = client.waitFor(
    (f) => isFor(f, channel) && ((f.type === 'chat' && f.action === 'membersUpdated') || isChatError(f, channel)),
    REPLY_TIMEOUT_MS,
  );
  client.send({ service: 'chat', action: 'removeMember', channel, userId, ...(name ? { name } : {}) });
  const frame = await answer;
  if (!frame) throw new Error(`gateway did not answer removeMember for ${channel}`);
  if (isChatError(frame, channel)) throw new Error(errorMessage(frame));
}

/** Take a person out of the room's conversation, as the owner (or as themselves). */
export async function removeRoomChatMember(input: RemoveRoomChatMemberInput): Promise<void> {
  const channel = roomChatChannel(input.slug);
  const client = clientFactory({ userId: input.byOwnerId, displayName: input.byOwnerName ?? input.byOwnerId, kind: 'person' }, input.gatewayUrl);
  await client.connect();
  try {
    await removeOneChatMember(client, channel, input.userId, input.name);
  } finally {
    await client.close();
  }
}

// ---------------------------------------------------------------------------
// Lazy convergence: a private room that predates the mirror, or one whose
// change the gateway missed, is brought in line the next time it is loaded —
// once per process per room, then again after the TTL.
// ---------------------------------------------------------------------------

const SYNC_TTL_MS = 10 * 60 * 1000;
const synced = new Map<string, number>();

export function _resetSyncCache(): void { synced.clear(); }

/** True when this slug was synced within the TTL (and marks it synced now otherwise). */
export function claimSync(slug: string, now = Date.now()): boolean {
  const until = synced.get(slug);
  if (until !== undefined && until > now) return false;
  synced.set(slug, now + SYNC_TTL_MS);
  return true;
}

/**
 * Fire-and-forget mirror of a room's full member list; never throws. The
 * loader runs only when the slug is due, so a page load that is already in
 * sync costs no member query.
 */
export function syncRoomOnce(slug: string, load: () => Promise<EnsureRoomChatMembersInput>): void {
  if (!claimSync(slug)) return;
  void load()
    .then((input) => ensureRoomChatMembers(input))
    .then(({ added, removed }) => {
      if (added.length || removed.length) log.info({ slug, added, removed }, 'room chat membership synced');
    })
    .catch((err: unknown) => {
      // Let the next load try again rather than waiting out the TTL.
      synced.delete(slug);
      log.warn({ slug, err: err instanceof Error ? err.message : String(err) }, 'room chat membership sync failed');
    });
}

/** Best-effort wrappers for the routes: log, never throw. */
export function mirrorRoomMembers(input: EnsureRoomChatMembersInput): void {
  void ensureRoomChatMembers(input).catch((err: unknown) => {
    log.warn({ slug: input.slug, err: err instanceof Error ? err.message : String(err) }, 'room chat membership mirror failed');
  });
}

export function mirrorRoomMemberRemoval(input: RemoveRoomChatMemberInput): void {
  void removeRoomChatMember(input).catch((err: unknown) => {
    log.warn({ slug: input.slug, userId: input.userId, err: err instanceof Error ? err.message : String(err) }, 'room chat member removal mirror failed');
  });
}
