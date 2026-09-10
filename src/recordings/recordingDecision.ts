// Whether a call records, and WHY — decided once, where the session is made.
//
// Recording used to be a side effect nobody could see: DM calls recorded by
// default, rooms recorded when a setting said so, and a person whose call
// went unrecorded had no way to learn which rule had applied. The decision is
// now a value: `{ enabled, reason }`, stamped on the session row, returned to
// the client that made the call (so its chrome can say "armed"), and read
// back by the gateway when the call ends (so the card in the thread can say
// why there is no recording).
//
// The reasons are a closed vocabulary so every consumer can turn them into the
// same words:
//   room-setting-on / room-setting-off  — the room's `settings.recordingEnabled`
//   dm-both-opted-in                    — every party's profile has recordCalls
//   dm-not-opted-in:<userIds>           — these parties have not; the list is
//                                         who still has to turn it on
//   default-on                          — no rule applied (an ad-hoc or
//                                         document lobby keeps the legacy
//                                         always-record behaviour)

export type RecordingReason =
  | 'room-setting-off'
  | 'room-setting-on'
  | 'dm-both-opted-in'
  | `dm-not-opted-in:${string}`
  | 'default-on';

export interface RecordingDecision {
  enabled: boolean;
  reason: RecordingReason;
}

/** What a caller may say about recording when it creates the session. */
export interface RequestedRecording {
  enabled?: boolean;
  /** A reason the caller already knows — the rooms route sends its setting's. */
  reason?: string;
}

const KNOWN_REASONS: ReadonlySet<string> = new Set([
  'room-setting-off', 'room-setting-on', 'dm-both-opted-in', 'default-on',
]);

/** True for the closed vocabulary above, including the parameterised DM form. */
export function isRecordingReason(value: unknown): value is RecordingReason {
  return typeof value === 'string' && (KNOWN_REASONS.has(value) || value.startsWith('dm-not-opted-in:'));
}

/** `dm:alice:bob` → `['alice','bob']`; null for a hashed `dmg:` group or anything else. */
export function dmLobbyMembers(lobby: string): string[] | null {
  if (!lobby.startsWith('dm:')) return null;
  const members = lobby.slice('dm:'.length).split(':').filter(Boolean);
  return members.length >= 2 ? members : null;
}

export function isDmLobby(lobby: string): boolean {
  return lobby.startsWith('dm:') || lobby.startsWith('dmg:');
}

/**
 * Decide for one lobby.
 *
 * A DM is CONSENT-gated: it records only when every party has `recordCalls`
 * on their profile, whatever the caller asked for — the transcribe checkbox
 * sends `enabled: true` and must not be a way around the other person's
 * choice. A hashed `dmg:` group cannot name its members from the lobby alone,
 * so it is gated on the caller's own flag only; the reason still uses the DM
 * vocabulary because the fix (turn it on in your profile) is the same.
 *
 * Everything else takes what it was told: a room sends its setting and the
 * reason for it, an explicit `enabled: false` with no reason is read as a room
 * opt-out (the only caller that sends one), and no instruction at all keeps
 * the legacy default of recording.
 */
export async function decideRecording(input: {
  lobbyName: string;
  requested: RequestedRecording | null;
  callerId: string;
  getProfile: (userId: string) => Promise<{ recordCalls?: boolean } | null>;
}): Promise<RecordingDecision> {
  const { lobbyName, requested, callerId } = input;

  if (isDmLobby(lobbyName)) {
    const parties = dmLobbyMembers(lobbyName) ?? [callerId];
    const profiles = await Promise.all(parties.map(async (id) => {
      try { return await input.getProfile(id); } catch { return null; }
    }));
    const missing = parties.filter((_, i) => profiles[i]?.recordCalls !== true);
    if (missing.length === 0 && requested?.enabled !== false) {
      return { enabled: true, reason: 'dm-both-opted-in' };
    }
    // An explicit opt-out with everyone opted in is still a DM that will not
    // record; name the people rather than inventing a sixth reason.
    return { enabled: false, reason: `dm-not-opted-in:${(missing.length ? missing : [callerId]).join(',')}` };
  }

  if (requested && typeof requested.enabled === 'boolean') {
    if (isRecordingReason(requested.reason)) return { enabled: requested.enabled, reason: requested.reason };
    return requested.enabled
      ? { enabled: true, reason: 'default-on' }
      : { enabled: false, reason: 'room-setting-off' };
  }
  return { enabled: true, reason: 'default-on' };
}

/** The gateway-facing shape stamped on a call card. */
export function callCardRecordingFields(decision: RecordingDecision | null | undefined):
  | { recorded: true }
  | { recorded: false; notRecordedReason: RecordingReason }
  | Record<string, never> {
  if (!decision) return {};
  return decision.enabled ? { recorded: true } : { recorded: false, notRecordedReason: decision.reason };
}
