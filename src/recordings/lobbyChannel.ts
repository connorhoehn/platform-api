// Which conversation a call's cards land in.
//
// A DM lobby and a room have a conversation of their own. A call started from
// a DOCUMENT has a lobby that is the document's UUID, and the document's chat
// panel joins `chat:call:<uuid>` for it — the same derivation the frontend's
// `roomFeedChannel` (useShareToRoom.ts) makes, hash fallback included. Until
// this existed here, every recording and transcript card for a document call
// was dropped on the floor: `chatChannelForLobby` returned null and the post
// step reported "no conversation" as if that were the whole story.
//
// The rule is DUPLICATED from the frontend rather than shared, because the
// shared home (realtime-modules) ships a committed dist behind a SHA pin and
// this is not worth a release cycle. The unit test pins three inputs against
// the frontend function's output; if that test goes red, one side drifted.

const MAX_CHANNEL_NAME_LENGTH = 100;
const UUID_RX = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/** `<lobby>` → `chat:call:<lobby>`, hashed past the 100-char channel cap. Byte-for-byte the frontend rule. */
export function callFeedChannelForLobby(lobbyName: string): string {
  if (lobbyName.startsWith('room:')) return lobbyName;
  const plain = `chat:call:${lobbyName}`;
  if (plain.length <= MAX_CHANNEL_NAME_LENGTH) return plain;
  let h = 0;
  for (let i = 0; i < lobbyName.length; i++) h = ((h * 31) + lobbyName.charCodeAt(i)) | 0;
  return `chat:call:${lobbyName.slice(0, 60)}-${Math.abs(h).toString(36)}`;
}

/** A recording's home in chat: DM lobbies, rooms, and document (UUID) lobbies have one; a bare lobby does not. */
export function chatChannelForLobby(lobby: string | null | undefined): string | null {
  if (!lobby) return null;
  if (lobby.startsWith('dmg:') || lobby.startsWith('dm:')) return `chat:${lobby}`;
  if (lobby.startsWith('room:')) return lobby;
  if (UUID_RX.test(lobby)) return callFeedChannelForLobby(lobby);
  return null;
}
