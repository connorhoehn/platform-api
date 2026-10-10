import type { VideoSessionRecord } from '../repositories/VideoSessionStore';
import type { TruthTrack } from './recording-truth';
import { tenantOf, TENANTLESS } from './tenant';

export interface RecordedCallWindow {
  channelArn: string;
  startedAt?: string;
  endedAt?: string;
  tracks: TruthTrack[];
}

export type RecordedCallBinding =
  | { status: 'bound'; callId: string; session: VideoSessionRecord }
  | { status: 'unresolved'; reason: 'missing-recording-window' | 'missing-recorded-participant'
      | 'unbound-recorded-participant' | 'mixed-call-bindings' | 'ambiguous-sessions' | 'no-exact-session' };

export function isGovernedRecordingLobby(lobbyName: unknown): boolean {
  return typeof lobbyName === 'string' && (lobbyName.startsWith('orgiq:') || lobbyName.startsWith('assessment:'));
}

const time = (value: unknown): number | null => {
  if (typeof value !== 'string' || !value) return null;
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : null;
};
const text = (value: unknown): value is string => typeof value === 'string' && value.trim().length > 0;
const recordedSeat = (track: TruthTrack): string | null => {
  // The query/media participant id is untrusted even for a camera. Only the
  // native signed-token seat establishes a canonical gateway association.
  // Historical absent fields and explicit mixed-token null sentinels fail.
  const id = track.tokenParticipantId;
  return text(id) ? id : null;
};

/**
 * A canonical call is established by every recorded media seat, never by a
 * lobby's newest signaling row. These fields are written only after the
 * gateway has acknowledged the same authenticated participant/client.
 * Screen source ids are opaque. Native JWT-derived tokenParticipantId can
 * supply their authenticated parent seat; a suffix or shared userId cannot.
 * Historical/unbound media stays unresolved.
 */
export function resolveRecordedCallBinding(sessions: VideoSessionRecord[], recording: RecordedCallWindow): RecordedCallBinding {
  const start = time(recording.startedAt), end = time(recording.endedAt);
  if (start == null || end == null || end < start) return { status: 'unresolved', reason: 'missing-recording-window' };
  if (!recording.tracks.length || recording.tracks.some(track => !track || !recordedSeat(track))) {
    return { status: 'unresolved', reason: 'missing-recorded-participant' };
  }
  const matched: Array<{ session: VideoSessionRecord; callId: string }> = [];
  let reason: Extract<RecordedCallBinding, { status: 'unresolved' }>['reason'] = 'no-exact-session';
  let conflictingExactSession = false;
  const uniqueSessions = new Map(sessions.map(session => [`${session.documentId}\u0000${session.sessionId}`, session]));
  for (const session of uniqueSessions.values()) {
    const sessionStart = time(session.startedAt), sessionEnd = time(session.endedAt);
    if (session.channelArn !== recording.channelArn || sessionStart == null || sessionStart > end
      || (session.endedAt && (sessionEnd == null || sessionEnd < start))) continue;
    let tenant: string;
    try { tenant = tenantOf(session.orgId); } catch { continue; }
    if (tenant === TENANTLESS || !text(session.sessionId) || !text(session.documentId)) continue;
    const callIds = new Set<string>();
    const containsEveryRecordedSeat = recording.tracks.every(track => !!session.participantHistory?.[recordedSeat(track)!]);
    let complete = true;
    for (const track of recording.tracks) {
      const pid = recordedSeat(track)!;
      const attendance = session.participantHistory?.[pid];
      const binding = attendance?.callBinding;
      const joined = time(attendance?.joinedAt), left = time(attendance?.leftAt), bound = time(binding?.boundAt);
      if (!attendance || attendance.participantId !== pid || !text(attendance.userId) || !text(attendance.clientId)
        || !binding || !text(binding.callId) || !text(binding.clientId)
        || binding.userId !== attendance.userId || binding.clientId !== attendance.clientId
        || !text(track.userId) || track.userId !== attendance.userId
        || binding.sessionId !== session.sessionId || binding.channelArn !== recording.channelArn
        || binding.lobbyName !== session.documentId || binding.tenant !== tenant
        || !Number.isSafeInteger(binding.epoch) || binding.epoch < 0
        || (binding.actor && (!text(binding.actor.sub) || !Number.isSafeInteger(binding.actor.epoch) || binding.actor.epoch < 0))
        || joined == null || joined > end || bound == null || bound < joined || bound > end
        || (attendance.leftAt && (left == null || left < start || bound > left))) {
        complete = false;
        reason = 'unbound-recorded-participant';
        break;
      }
      callIds.add(binding.callId);
    }
    if (!complete) { conflictingExactSession ||= containsEveryRecordedSeat; continue; }
    if (callIds.size !== 1) { reason = 'mixed-call-bindings'; conflictingExactSession = true; continue; }
    matched.push({ session, callId: [...callIds][0] });
  }
  if (matched.length > 1) return { status: 'unresolved', reason: 'ambiguous-sessions' };
  if (matched.length && conflictingExactSession) return { status: 'unresolved', reason: 'ambiguous-sessions' };
  if (matched.length === 1) return { status: 'bound', ...matched[0] };
  return { status: 'unresolved', reason };
}
