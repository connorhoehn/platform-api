// VideoSessionStore — abstraction over the persistence layer for video
// sessions. Two implementations exist:
//
//   - DDBVideoSessionStore (default) — durable persistence via DynamoDB.
//     This is `VideoSessionRepository`, which has been the production code
//     path for some time. Survives platform-api restarts; rolls forward
//     across multiple replicas via the shared DDB table.
//
//   - MemoryVideoSessionStore — process-local Map. Lost on restart.
//     Used in tests that don't want to mock DDB, and as an emergency
//     rollback path via the VIDEO_SESSIONS_STORE env flag if DDB
//     becomes the failure mode (see `createVideoSessionStore`).
//
// Both implementations honour the same VideoSessionRecord shape and
// the same idempotency semantics (createSession overwrites, addParticipant
// appends without dedupe — callers dedupe).
//
// TTL semantics:
//   - On record creation: optional `ttl` (epoch seconds). If unset,
//     records never auto-expire. Long-running lobbies stay until manually
//     ended; short-lived lobbies can request expiry via the create path.
//   - On endSession: TTL is automatically set to (endedAt + VIDEO_SESSION_TTL_DAYS),
//     defaulting to 7 days post-end so the record sticks around long enough
//     for analytics + replay queries but eventually gets garbage-collected.
//   - DDB requires the `ttl` attribute to be enabled on the table (see
//     `ensureVideoSessionsTTL` in src/lib/dynamo-setup.ts). The in-memory
//     store enforces TTL synchronously on every read.

export interface VideoSessionParticipant {
  userId: string;
  /**
   * Per-tab SFU participant id (returned by /api/channels/:arn/participant-tokens).
   * Used as the dedup key in participants[] instead of userId — multiple tabs
   * for the same user (or anon SKIP_AUTH=true sharing userId='dev-user') need
   * to ref-count separately, otherwise tab-1 unloading the page wipes tab-2's
   * participant slot and the session ends with the second tab orphaned.
   */
  participantId: string;
  displayName: string;
  joinedAt: string;
}

export interface VideoSessionRecord {
  /** Lobby name (a/k/a documentId; field kept for back-compat). */
  documentId: string;
  sessionId: string;
  channelArn: string;
  streamKey: string;
  status: 'active' | 'ended';
  startedAt: string;
  endedAt?: string;
  startedBy: string;
  participants: VideoSessionParticipant[];
  transcriptStatus?: 'pending' | 'processing' | 'available' | 'failed';
  transcript?: string;
  aiSummary?: string;
  /**
   * Epoch seconds. DynamoDB TTL attribute. When unset, records do not
   * auto-expire. `endSession` sets this automatically to endedAt + grace
   * window (default 7 days). The in-memory store filters expired records
   * out of reads to match DDB's eventual-consistency behaviour.
   */
  ttl?: number;
}

/**
 * Persistence contract for the video sessions feature. Both
 * `DDBVideoSessionStore` and `MemoryVideoSessionStore` implement this;
 * call sites depend only on the interface so they can be swapped via
 * the `VIDEO_SESSIONS_STORE` env flag.
 */
export interface VideoSessionStore {
  createSession(record: VideoSessionRecord): Promise<VideoSessionRecord>;
  endSession(documentId: string, sessionId: string, endedAt: string): Promise<void>;
  addParticipant(
    documentId: string,
    sessionId: string,
    participant: VideoSessionParticipant,
  ): Promise<void>;
  /** Remove a participant by per-tab participantId (the SFU-issued id). */
  removeParticipant(
    documentId: string,
    sessionId: string,
    participantId: string,
  ): Promise<{ remaining: number } | null>;
  /**
   * Legacy: remove all participants matching a userId. Kept for back-compat
   * with FE clients that haven't been updated to send participantId on end.
   */
  removeParticipantByUserId(
    documentId: string,
    sessionId: string,
    userId: string,
  ): Promise<{ remaining: number } | null>;
  getSession(documentId: string, sessionId: string): Promise<VideoSessionRecord | null>;
  getSessionsByDocument(documentId: string): Promise<VideoSessionRecord[]>;
  findActiveByLobbyName(lobbyName: string): Promise<VideoSessionRecord | null>;
  /**
   * DDB-only fallback (also implemented by the in-memory store for parity).
   * Used by the LVS recording-completed webhook when the Redis arn-index
   * has expired between session-create and call-end. Returns newest-first;
   * empty array when no session matches. Without this fallback, DM
   * recordings disappeared whenever Redis dropped the arn-index mid-call
   * (root cause of the 2026-05-23 "Past Calls hides DMs" bug).
   */
  findByChannelArn(channelArn: string): Promise<VideoSessionRecord[]>;
  /**
   * Every lobby still marked active, newest row per lobby.
   *
   * CANDIDATES, not an answer. `status: 'active'` leaks — a room call joins
   * twice and hangs up once — so the caller must ask the SFU whether each
   * channel is actually publishing before showing anything to a user. See
   * `GET /api/video/sessions/live`, the only caller.
   */
  listActiveCandidates(limit?: number): Promise<VideoSessionRecord[]>;
}

/**
 * Default grace window applied to `endSession` so records remain
 * queryable for ~a week post-call. Override per-deployment via
 * VIDEO_SESSION_TTL_DAYS.
 */
export const DEFAULT_VIDEO_SESSION_TTL_DAYS = 7;

/**
 * Compute the TTL epoch seconds for a session that just ended. Pure
 * helper, shared between both stores so they agree on the math.
 */
export function computeEndedSessionTtl(
  endedAtIso: string,
  graceDays = readGraceDaysFromEnv(),
): number {
  const endedMs = Date.parse(endedAtIso);
  const base = Number.isNaN(endedMs) ? Date.now() : endedMs;
  return Math.floor(base / 1000) + graceDays * 24 * 60 * 60;
}

function readGraceDaysFromEnv(): number {
  const raw = process.env.VIDEO_SESSION_TTL_DAYS;
  if (!raw) return DEFAULT_VIDEO_SESSION_TTL_DAYS;
  const n = Number.parseInt(raw, 10);
  return Number.isFinite(n) && n > 0 ? n : DEFAULT_VIDEO_SESSION_TTL_DAYS;
}
