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
  /**
   * Stable per-TAB key supplied by the client (see `clientId` in the join
   * body). Optional — older clients don't send one.
   *
   * WHY THIS EXISTS. `participantId` cannot be the idempotency key for a
   * join, because it is minted UPSTREAM: LVS builds it as
   * `${userId}-${Date.now().toString(36)}` (live-video-streaming
   * src/api/sessions/index.js:276), so it is different on every single
   * join. The old dedupe check — "is this participantId already in the
   * list?" — could therefore never fire, and any client that joined twice
   * (a room does: once through the room bridge, once through the call
   * widget) appended a second entry that nothing ever removed.
   *
   * `clientId` is the one identifier that survives a re-join from the same
   * tab, so it is what `addOrReplaceParticipant` collapses on. When it is
   * absent we fall back to collapsing on `userId`, which is right for the
   * double-join and slightly lossy for a user with two untagged tabs —
   * see `participantSupersedes`.
   */
  clientId?: string;
  displayName: string;
  joinedAt: string;
}

/**
 * Does `incoming` represent the same client as `existing`, such that adding
 * `incoming` should REPLACE `existing` rather than append beside it?
 *
 * The rule, in order:
 *   1. Both carry a `clientId` → same tab iff the ids match. Two tabs of one
 *      user each keep their own ref-count slot, which is the whole point of
 *      the per-tab key.
 *   2. `incoming` carries a `clientId` and `existing` does not → NOT the
 *      same client. An untagged entry is never displaced by a tagged one;
 *      otherwise a client mid-upgrade would evict a peer's slot.
 *   3. Neither carries one → collapse on `userId`. This is the case that
 *      fixes the room double-join with no client change: the room bridge
 *      and the call widget both join untagged as the same user, so the
 *      second join supersedes the first instead of leaking it.
 *
 * Rule 3 is deliberately lossy for one user in two untagged tabs — they get
 * one participant slot, holding the NEWEST participantId. That is strictly
 * better than the leak it replaces: removal is still by exact
 * participantId, so an older tab leaving cannot evict a newer one, and the
 * authoritative /end path defers to LVS's own count anyway. Tabs that send
 * a `clientId` are exact.
 */
export function participantSupersedes(
  existing: VideoSessionParticipant,
  incoming: VideoSessionParticipant,
): boolean {
  if (incoming.clientId) return existing.clientId === incoming.clientId;
  if (existing.clientId) return false;
  return existing.userId === incoming.userId;
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
  /** Verified tenant captured when the session was created. Optional for legacy rows. */
  orgId?: string;
  participants: VideoSessionParticipant[];
  transcriptStatus?: 'pending' | 'processing' | 'available' | 'failed';
  transcript?: string;
  aiSummary?: string;
  /**
   * Whether this call records and why — decided when the session was made
   * (see recordings/recordingDecision.ts). Absent on rows written before the
   * decision was recorded; readers must treat that as "unknown", not "off".
   */
  recording?: { enabled: boolean; reason: string };
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
  /**
   * Idempotent join. Appends `participant`, first removing every existing
   * entry that `participantSupersedes` says is the same client. Returns the
   * displaced entries so the caller can release them upstream (LVS keeps its
   * OWN participant set, and it is only ever decremented by the /end route —
   * a displaced entry left there keeps `participantsRemaining` above zero
   * forever, which is how the session never ends).
   *
   * Returns null when the session row does not exist.
   */
  addOrReplaceParticipant(
    documentId: string,
    sessionId: string,
    participant: VideoSessionParticipant,
  ): Promise<{ displaced: VideoSessionParticipant[]; remaining: number } | null>;
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
   * CANDIDATES, not an answer. `status: 'active'` is a lifecycle flag, and a
   * client that crashes or closes its laptop never says goodbye — so the
   * caller must ask the SFU whether each channel is actually publishing
   * before showing anything to a user. See `GET /api/video/sessions/live`.
   *
   * `opts.perLobby` (default true) keeps only the newest row per lobby, which
   * is what a badge wants. The stale-session reaper passes `false`: rows
   * BEHIND the newest are invisible to every read path, so collapsing would
   * leave them 'active' forever with nothing able to see them.
   */
  listActiveCandidates(
    limit?: number,
    opts?: { perLobby?: boolean },
  ): Promise<VideoSessionRecord[]>;
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
