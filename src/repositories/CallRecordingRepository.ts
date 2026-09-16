/**
 * CallRecordingRepository — persists LVS recording metadata indexed by
 * the gateway-side `callId`. Driven by the LVS `recording.completed`
 * webhook arriving at platform-api.
 *
 * Two tables to keep query patterns cheap:
 *
 *   1. `video-call-recordings` (primary)
 *      - PK: callId
 *      - SK: recordingId
 *      - Attributes: channelArn, lobbyName, sessionId, startedAt, endedAt,
 *                    durationMs, totalBytes, segments[], destination,
 *                    participants[], startedBy, ttl
 *
 *   2. `video-call-recording-participants` (reverse index — fan-out write)
 *      - PK: userId
 *      - SK: `${startedAt}#${recordingId}`
 *      - Attributes: callId, channelArn, durationMs, startedAt, endedAt
 *      - Lets the frontend cheap-query "show me my last 20 recordings"
 *        without GSI scaling pain. One row per participant per recording.
 *
 * TTL on both: default 30 days, matches the LVS RecordingGc worker so
 * we don't surface dangling metadata for already-deleted media.
 */

import {
  DynamoDBDocumentClient,
  PutCommand,
  GetCommand,
  QueryCommand,
  ScanCommand,
  DeleteCommand,
  UpdateCommand,
  BatchGetCommand,
} from '@aws-sdk/lib-dynamodb';
import { BaseRepository } from './BaseRepository';
import { tableName } from '../lib/ddb-table-name';
import {
  deriveMediaDurationMs,
  reconcileParticipants,
  type TruthTrack,
} from '../lib/recording-truth';

export const CALL_RECORDINGS_TABLE = tableName('video-call-recordings');
export const CALL_RECORDING_PARTICIPANTS_TABLE = tableName(
  'video-call-recording-participants',
);

/**
 * GSI that makes `recordingId` a point-lookup key.
 *
 * The primary table is keyed PK `callId` + SK `recordingId`, so a URL that
 * carries only a `recordingId` — which is every recording URL in the app,
 * and every share link — could not point-get. Six routes worked around
 * that by listing the caller's first 100 fan-out rows and scanning them in
 * memory, which means a user with 101 recordings got a 404 on a recording
 * that demonstrably exists. That is not a rare edge: the fan-out table
 * writes one row per participant per recording, so a chatty account passes
 * 100 in weeks.
 *
 * Paging that scan would have removed the 404 without removing the shape
 * that produced it — the next route to be written would reach for the same
 * bounded list. A `recordingId` index removes the shape: there is no
 * "how many did we look at" question left to get wrong.
 *
 * Projection is ALL so the detail routes need no follow-up GetItem.
 */
export const RECORDING_ID_INDEX = 'recordingId-index';

export interface RecordingSegment {
  key: string;
  url: string;
  size: number;
}

/**
 * One per-peer raw track recording, alongside the composed file. When the
 * call had a single participant or compose was disabled, this collapses
 * to `segments[]` and `tracks` is empty.
 */
export interface RecordingTrack {
  participantId?: string;
  userId?: string;
  key: string;
  url: string;
  size: number;
  /** 'video' | 'audio' — when ffmpeg recorded a/v together this stays unset. */
  kind?: 'video' | 'audio' | 'av';
}

/**
 * LVS's compose-job vocabulary, reproduced exactly (see
 * live-video-streaming/src/recording/RecordingManager.js — `composeStatus`
 * is validated there against this same set).
 */
export type ComposeStatus =
  | 'queued'
  | 'running'
  | 'ok'
  | 'degraded'
  | 'failed'
  | 'skipped';

/**
 * What the viewer can actually be told about this recording's media, and
 * the ONLY thing the playback surface is allowed to branch on.
 *
 * Three genuinely different situations were collapsed into one bare
 * "Playback error." because the API handed the player a fabricated
 * `composed.mp4` URL in every case and let the `<video>` element discover
 * the truth by 404ing:
 *
 *   'playable'   — a file exists and we know its name.
 *   'processing' — compose is queued or running. Nothing to play YET. This
 *                  is not an error and must never be rendered as one.
 *   'unavailable'— compose was skipped or failed and no per-leg media is
 *                  servable either. There will never be anything to play.
 *                  Also not an error — the recording is intact, it just
 *                  has no video, and saying so is the whole job.
 *
 * A real playback failure — the file is named, we handed over its URL, and
 * the browser still could not play it — is the fourth case, and it is the
 * only one the player itself gets to report.
 */
export type RecordingMediaState = 'playable' | 'processing' | 'unavailable';

export interface CallRecordingParticipant {
  userId: string;
  displayName?: string;
  participantId?: string;
  joinedAt?: string;
}

export interface CallRecording {
  callId: string;
  recordingId: string;
  channelArn: string;
  lobbyName: string;
  sessionId?: string;
  startedAt: string; // ISO
  endedAt: string; // ISO
  /**
   * Length of the RECORDED MEDIA. Derived from the track timeline by
   * `deriveMediaDurationMs` — NOT the SFU channel-session lifetime, which
   * is what LVS puts on the wire and what this field used to hold. See
   * `sessionDurationMs` for that number and `lib/recording-truth.ts` for
   * the measurement that motivated the split.
   */
  durationMs: number;
  /**
   * The SFU channel-session lifetime (ring + lobby wait + media +
   * teardown). Kept because it is genuinely useful for call analytics and
   * because throwing away the raw webhook number would make the change
   * irreversible — but it is NOT what "Duration" means to a viewer, so it
   * is never the field the UI renders. Absent on rows written before
   * 2026-08-25.
   */
  sessionDurationMs?: number;
  totalBytes: number;
  segments: RecordingSegment[];
  destination: 'local' | 's3';
  participants: CallRecordingParticipant[];
  /**
   * True number of people in the recording, derived by the webhook from
   * the recording's per-leg tracks[] ∪ the session roster. NOT always
   * `participants.length`: `participants[]` only holds userId-bearing
   * entries (roster + userId-tagged tracks), while LVS multi-leg tracks
   * carry participantId only — those legs are real tiles in the composed
   * video but can't be attributed to a userId. Absent on historical rows
   * (pre-2026-08-24); readers fall back to `participants.length`.
   */
  participantCount?: number;
  startedBy?: string;
  orgId?: string;
  ttl: number; // epoch seconds
  /** W8 — first-frame poster image generated server-side. Populated
   *  when the webhook payload carried it; null when ffmpeg thumbnail
   *  step failed (rare). Rendered by ui-components <RecordingList>. */
  thumbnailUrl?: string;
  /**
   * R2 (this commit) — composed playback file (ffmpeg xstack grid of all
   * per-peer tracks). When present, the FE uses this as the primary
   * "playback" target — one video showing all participants tiled — and
   * surfaces `tracks[]` as a per-peer raw-downloads list. Null when:
   *   - the call had a single producer (segments[] IS the recording), or
   *   - compose disabled by config / ffmpeg compose step failed.
   */
  composedUrl?: string;
  /** R2 — per-peer raw track files. Empty for single-producer calls. */
  tracks?: RecordingTrack[];
  /**
   * The LVS compose job's own word for what happened to this recording's
   * media. Carried verbatim from the `recording.completed` webhook — this
   * service deliberately does not invent a parallel vocabulary, because the
   * two would drift and then nobody could tell which one was lying.
   *
   *   'queued'   — eligible for compose; the background ffmpeg job has not
   *                finished. The webhook fires BEFORE compose completes, so
   *                this is the status most rows are born with.
   *   'running'  — ffmpeg is working.
   *   'ok'       — composed.mp4 exists.
   *   'degraded' — composed.mp4 ALSO exists and plays. This is not a broken
   *                state: the grade came from comparing output length to
   *                wall-clock, while the windowed pipeline deliberately
   *                drops dead air, so every call where somebody left early
   *                was marked down. Legacy rows keep the label. Do not
   *                render it as a failure.
   *   'failed'   — compose ran and produced nothing usable.
   *   'skipped'  — compose never ran because there were no legs at all.
   *                It used to also mean "only one leg, so no grid to
   *                build"; LVS composes a solo leg now, so that reading is
   *                historical.
   *
   * Absent on rows written before this field existed and on rows from an
   * LVS build that does not send it — `deriveComposeStatus` fills that gap
   * from the media that IS on the row, so readers never have to branch on
   * `undefined`.
   */
  composeStatus?: ComposeStatus;
  /**
   * The composed grid file, once LVS has actually produced it. Never
   * present off the `recording.completed` webhook — compose has not run at
   * that point — so this is written back by the read path after it
   * reconciles live status from LVS. It was already being read here via an
   * inline cast in routes/recordings.ts; declared so the two can't disagree.
   */
  composedSegment?: { key?: string; url?: string; size?: number };
  /** ISO timestamp of the last live compose-status reconciliation. */
  composeCheckedAt?: string;
  /**
   * Transcription pipeline state. Sparse — only set when the user opted
   * into transcription for this recording (recordingConfiguration.transcribe
   * was true at start). Lifecycle:
   *   undefined / 'pending' — outbox row published, worker hasn't picked up
   *   'processing'          — worker has the message, faster-whisper running
   *   'ready'               — transcript.json (and summary.md if present)
   *                           written to blob storage; UI can fetch
   *   'failed'              — exhausted DLQ retries
   */
  transcriptStatus?: 'pending' | 'processing' | 'ready' | 'failed';
  /**
   * ISO timestamp of the last `transcriptStatus` write — i.e. when we last
   * observed forward progress on this job.
   *
   * The stall check used to measure from `endedAt`, which is the wrong
   * clock: it answers "how long since the call finished", not "how long has
   * this job been waiting". They coincide for a fresh recording, which is
   * why the bug survived, and diverge completely for a re-triggered or
   * replayed one — a job enqueued thirty seconds ago against a recording
   * from a month back reported `waitedMs: 32106080` and was declared
   * stalled on the spot.
   *
   * Sparse: rows written before this field existed have none, and the read
   * path falls back to `endedAt` for them.
   */
  transcriptStatusAt?: string;
  /**
   * Summarisation state, which is NOT transcription state.
   *
   * The summary is a second, LLM-backed job over a transcript that already
   * exists. It has its own ways to fail — missing credentials, a rate
   * limit, an empty completion — none of which say anything about whether
   * the transcript is readable. Folding the two together meant one failed
   * `createLLMClient()` call marked `transcriptStatus: 'failed'` on a row
   * that had a perfectly good `transcriptKey` stamped by the same write,
   * and the UI reported the transcription as failed for a transcript that
   * served 200 the moment the status was flipped by hand.
   *
   * Sparse for the same reason as the transcript fields, and additionally
   * absent on every row written before this split; readers treat a missing
   * value as "no summary attempt recorded".
   */
  summaryStatus?: 'pending' | 'processing' | 'ready' | 'failed' | 'skipped';
  /** Last-error string when `summaryStatus === 'failed'`. Truncated to 200 chars. */
  summaryError?: string;
  /** Blob-storage key (S3/R2) for the merged transcript JSON. Sparse. */
  transcriptKey?: string;
  /** Blob-storage key for the LLM-generated markdown summary. Reserved for
   *  the future when summaries grow past the inline DDB limit. */
  summaryKey?: string;
  /** Inline markdown summary written by the summary consumer. Stored
   *  directly in DDB because a typical 30-min meeting summary is <20 KB,
   *  well under the 400 KB item cap. If summaries ever grow large we'd
   *  switch to summaryKey + blob storage; both fields kept so the
   *  migration is a write-shape change, not a type change. */
  summaryMarkdown?: string;
  /** Transcript stored on the row when `transcriptKey === 'inline'` (the in-process transcription pipeline). */
  transcriptJson?: string;
  /** ISO timestamp when the worker wrote the transcript. */
  transcriptGeneratedAt?: string;
  /** Last-error string when transcriptStatus === 'failed'. Truncated to 200 chars. */
  transcriptError?: string;
}

/**
 * Shape returned by both list endpoints. Shared so the user-scoped list
 * (fan-out index) and the lobby-scoped list (primary-table scan) can't
 * drift into rendering different fields for the same card.
 */
export interface RecordingListRow {
  callId: string;
  recordingId: string;
  startedAt: string;
  endedAt: string;
  /** Epoch seconds when the row expires — the fan-out row carries the primary's. Absent: kept until deleted. */
  ttl?: number;
  /** Media length — see CallRecording.durationMs. */
  durationMs: number;
  sessionDurationMs?: number;
  channelArn: string;
  lobbyName: string;
  thumbnailUrl?: string;
  composedUrl?: string;
  /** Distinct people, not sessions. */
  participantCount?: number;
  /** userId + displayName, so the list can render names, not raw ids. */
  participants?: Array<{ userId: string; displayName?: string }>;
  /**
   * Where this recording's media got to — the ONE field that says whether
   * a row can be opened.
   *
   * It was missing from both lists, and the cost was two dead features and
   * one active lie. ui-components' RecordingRow already renders a
   * Processing / Failed badge off `status`, and it could never fire because
   * nothing ever reached it. Worse, the app's `isRecordingPlayable` treats
   * an ABSENT status as playable — deliberately, so legacy rows still open
   * — so an absent field did not mean "unknown", it meant "yes": every row
   * in the in-channel strip was presented as playable, including the ones
   * whose compose had failed and that had nothing behind them at all.
   *
   * Always populated now, `deriveComposeStatus`-filled for rows that never
   * carried one, so the FE's legacy-tolerant branch stops being the branch
   * every row takes.
   */
  composeStatus?: ComposeStatus;
  /**
   * Transcription pipeline state, when there is one. Sparse by nature —
   * only recordings that opted into transcription have it — so `undefined`
   * here means "no transcript was ever asked for", NOT "still waiting".
   */
  transcriptStatus?: 'pending' | 'processing' | 'ready' | 'failed';
}

const RETENTION_DAYS = Number(process.env.RECORDING_RETENTION_DAYS ?? 30);

/** Identities denormalized onto each fan-out row, for list-view labels. */
const FANOUT_PARTICIPANT_CAP = 12;

/**
 * The set of userIds that must be able to find this recording:
 * `participants[] ∪ {startedBy}`. Unconditional union — see the
 * putRecording docblock for why the old "only when participants is empty"
 * fallback lost the caller on 18% of real recordings.
 *
 * Shared by putRecording and deleteRecording so the write set and the
 * delete set are the same set by construction.
 */
export function fanoutSetFor(rec: {
  participants?: Array<{ userId?: string }> | null;
  startedBy?: string;
}): Set<string> {
  const ids = new Set<string>();
  for (const p of rec.participants ?? []) {
    if (p?.userId) ids.add(p.userId);
  }
  if (rec.startedBy) ids.add(rec.startedBy);
  return ids;
}

/**
 * Read-path safety net for `durationMs`.
 *
 * Rows written before 2026-08-25 hold the SFU session lifetime in
 * `durationMs` — every one of them overstates its own media (measured:
 * 98/98 by >1s, median +10.2s). The backfill script rewrites them, but a
 * derive-on-read guard means a row that the backfill missed — or one
 * written by an LVS pod still on the old payload shape — cannot re-surface
 * the lie. Rows already carrying `sessionDurationMs` were written by the
 * corrected path and are passed through untouched.
 */
function normalizeDuration<T extends { durationMs?: number; sessionDurationMs?: number; tracks?: unknown }>(
  rec: T,
): T {
  if (rec.sessionDurationMs !== undefined) return rec;
  const tracks = Array.isArray(rec.tracks) ? (rec.tracks as TruthTrack[]) : undefined;
  if (!tracks || tracks.length === 0) return rec;
  return {
    ...rec,
    durationMs: deriveMediaDurationMs({ tracks, sessionDurationMs: rec.durationMs }),
    sessionDurationMs: rec.durationMs,
  };
}

/**
 * One row per recording, newest write wins.
 *
 * The fan-out index's sort key is `${startedAt}#${recordingId}`, so a
 * recording whose `startedAt` ever changed has TWO rows and a user sees the
 * same call listed twice. The writer no longer strands old keys and LVS now
 * sends a stable `startedAt`, but rows written before either fix are still
 * sitting in the table, and a list that shows one call twice is a bug the
 * user sees no matter which layer produced it.
 *
 * Rows arrive newest-first (ScanIndexForward: false), so the FIRST
 * occurrence of a recordingId is the one written against the latest
 * `startedAt` — keep it and drop the rest. Order is otherwise preserved.
 *
 * The dropped row is a duplicate by construction: the two differ only in
 * the timestamp that keyed them apart, so nothing is lost by collapsing them.
 */
export function dedupeByRecordingId<T extends { recordingId: string }>(rows: T[]): T[] {
  const seen = new Set<string>();
  const out: T[] = [];
  for (const r of rows) {
    if (seen.has(r.recordingId)) continue;
    seen.add(r.recordingId);
    out.push(r);
  }
  return out;
}

/** Basename of a stored object key/url, or null when unrecoverable. */
function basenameOf(candidate: string | undefined | null): string | null {
  if (typeof candidate !== 'string' || candidate.length === 0) return null;
  const name = candidate.split(/[\\/]/).pop();
  if (!name || !name.includes('.')) return null;
  return name;
}

/** The composed grid file's name, from whichever field carries it. */
function composedFilenameOf(rec: ComposableRecording): string | null {
  const seg = rec.composedSegment;
  if (isDeadFile(seg?.size)) return null;
  return basenameOf(seg?.key ?? seg?.url ?? rec.composedUrl);
}

/** First entry that is both named and not a stub. */
function firstLiveFilename(
  files: Array<{ key?: string; url?: string; size?: number }> | undefined,
): string | null {
  for (const f of files ?? []) {
    if (isDeadFile(f?.size)) continue;
    const name = basenameOf(f?.key ?? f?.url);
    if (name) return name;
  }
  return null;
}

/** Whatever this recording holds that a player could be pointed at. */
export interface ComposableRecording {
  composeStatus?: ComposeStatus;
  composedUrl?: string;
  composedSegment?: { key?: string; url?: string; size?: number };
  segments?: Array<{ key?: string; url?: string; size?: number }>;
  tracks?: Array<{ key?: string; url?: string; size?: number }>;
}

/**
 * Below this, a file cannot be a playable MP4 — it is a stub.
 *
 * 306 recordings on disk are 28-byte files: an `ftyp` box and nothing
 * after it, left by a historical truncated-mux bug. They have a name, a
 * size, and a URL that returns 200, so everything upstream of the decoder
 * thinks they are fine; the browser opens one, finds no `moov` atom, and
 * fires an error the player can only describe as "Playback error."
 *
 * A real MP4 with a moov atom does not fit in 64 bytes, so anything at or
 * under that is dead with certainty rather than by guess. The threshold is
 * deliberately far below the smallest real file we have on record (796
 * bytes) — the point is to catch stubs, not to adjudicate marginal files.
 * When `size` is absent we say nothing, because "unknown" is not "empty".
 */
const MIN_PLAYABLE_BYTES = 64;

/** A file we can be sure holds no decodable video. */
function isDeadFile(size: number | undefined): boolean {
  return typeof size === 'number' && size <= MIN_PLAYABLE_BYTES;
}

/**
 * The compose status of a row that never got one.
 *
 * LVS fires `recording.completed` BEFORE the background compose job runs and
 * never fires a second webhook, so no row this service persists has ever
 * carried a compose status — and `composedUrl` off that webhook is always
 * undefined too (verified in RecordingManager: nothing assigns
 * `record.composedUrl`). Live status therefore has to be read back from LVS.
 * When that read is unavailable, this reconstructs the status from the media
 * that IS on the row, using the same rule LVS uses to pick it:
 *
 *   one or more per-peer legs → there is something to compose → 'queued'
 *   none                      → nothing to compose → 'skipped'
 *
 * The threshold is ONE leg, not two. LVS used to require two — a solo call
 * had no grid to build, so 125 recordings sat permanently 'skipped' — and as
 * of LVS 92d2e74 a single leg composes onto a 1×1 canvas instead. Both
 * 'queued' and 'skipped' are now zero in live data; this rule is for legacy
 * rows and for when LVS cannot be reached.
 *
 * Deliberately conservative: it never claims 'ok' for a file it has not been
 * told exists, and never claims 'failed', because "compose failed" and
 * "compose has not run yet" are not distinguishable from row contents alone
 * and guessing between them is how the UI started lying in the first place.
 */
export function deriveComposeStatus(rec: ComposableRecording): ComposeStatus {
  if (rec.composeStatus) return rec.composeStatus;
  if (composedFilenameOf(rec)) return 'ok';
  return (rec.tracks?.length ?? 0) >= 1 ? 'queued' : 'skipped';
}

/**
 * What we can honestly tell the viewer, and the exact file to serve.
 *
 * The route this replaces ended with `?? 'composed.mp4'` — when it had no
 * idea what to play it named a file it had no reason to think existed, the
 * browser 404'd, and the player said "Playback error." That fallback is the
 * bug: it converts "we don't know" into "you broke it".
 *
 * Playback target order — composed grid, then a raw segment, then a per-peer
 * leg. Every step skips stub files, so a name that leads to 28 bytes is not
 * treated as a name; 306 recordings on disk are exactly that, and each one
 * used to reach the player as a working URL and come back "Playback error."
 * Telling someone up front that a file holds nothing is a different message
 * from letting their browser find out.
 *
 * The per-peer fallback is conditional. A leg is real, watchable media, and
 * for a legacy solo recording it is the whole recording — write those off
 * and 125 perfectly good videos vanish for no reason. But a multi-party
 * call whose compose is still `queued` also has legs sitting in `tracks[]`,
 * and serving the first would present one person's camera as the recording
 * of a conversation. Smaller than "Playback error.", still a lie, and it
 * would go permanent wherever compose never lands. So the fallback applies
 * only once compose has stopped: while it is still coming, the honest
 * answer is that it is still coming.
 */
export function describeMedia(rec: ComposableRecording): {
  composeStatus: ComposeStatus;
  mediaState: RecordingMediaState;
  /** Filename to serve from the LVS segments route; null when nothing to serve. */
  playbackFilename: string | null;
} {
  const composeStatus = deriveComposeStatus(rec);
  const stillComing = composeStatus === 'queued' || composeStatus === 'running';
  const playbackFilename =
    composedFilenameOf(rec)
    ?? firstLiveFilename(rec.segments)
    ?? (stillComing ? null : firstLiveFilename(rec.tracks));

  if (playbackFilename) {
    return { composeStatus, mediaState: 'playable', playbackFilename };
  }
  // Nothing to serve. Whether that is temporary or permanent is exactly the
  // distinction the old code threw away.
  return {
    composeStatus,
    mediaState: stillComing ? 'processing' : 'unavailable',
    playbackFilename: null,
  };
}

export class CallRecordingRepository extends BaseRepository {
  private participantsTable: string;

  constructor(docClient: DynamoDBDocumentClient) {
    super(CALL_RECORDINGS_TABLE, docClient);
    this.participantsTable = CALL_RECORDING_PARTICIPANTS_TABLE;
  }

  /**
   * Persist a completed recording + fan out one row per participant
   * into the reverse-index table.
   *
   * Idempotent in the payload — re-running the same webhook overwrites both
   * tables rather than adding to them. That sentence used to be here without
   * the qualifier, and it was not true: the fan-out sort key embeds
   * `startedAt`, the webhook did not carry one, and the receiver synthesized
   * it from its own clock, so two deliveries of one event wrote two rows.
   * `startedAt` now comes off the wire (or off the recordingId), and the
   * eviction below cleans up after any writer that still changes it.
   *
   * Visibility guarantee: the fan-out set is ALWAYS `participants[] ∪
   * {startedBy}`. The union with `startedBy` is unconditional and that is
   * the whole point — the previous code only fell back to `startedBy` when
   * `participants[]` was *empty*, which quietly lost exactly the case that
   * matters most: `participants[]` is the SFU's LIVE roster at
   * recording-complete time, and the person who PLACED the call has
   * usually already hung up by then. So a non-empty roster containing only
   * the callee looked like a complete answer, `startedBy` was skipped, and
   * the caller never got an index row. Measured on live data 2026-08-25:
   * 37 of 200 recordings (18%) had no fan-out row for their own
   * `startedBy` — permanently invisible in `GET /api/recordings` to the
   * one person who made the call. Every one of those had a non-empty
   * `participants[]`, so the old empty-only fallback could never fire.
   */
  async putRecording(rec: Omit<CallRecording, 'ttl'>): Promise<void> {
    const ttl = Math.floor(Date.now() / 1000) + RETENTION_DAYS * 86400;
    const full: CallRecording = { ...rec, ttl };

    // Evict fan-out rows this recording left at a PREVIOUS `startedAt`.
    //
    // The primary table is keyed (callId, recordingId), so re-putting a
    // recording overwrites itself — which is what made the "idempotent"
    // claim above look true. The fan-out table is keyed
    // (userId, `${startedAt}#${recordingId}`), and `startedAt` is DATA, not
    // identity. Change it by a millisecond and the same recording writes a
    // second row that nothing ever deletes, and `GET /api/recordings`
    // returns the same call twice.
    //
    // That is not hypothetical: recordingId
    // 2026-09-07T10-30-33-757Z-4b2365cf carried two rows 30ms apart,
    // identical in every other attribute. The upstream cause (a synthesized
    // `startedAt`) is fixed at the webhook and in LVS, but a write path that
    // silently strands its own old key is worth closing regardless — a
    // backfill, a replay, or a clock correction would all reopen it.
    //
    // One GetItem, and only on an actual change.
    const prev = await this.getItem<CallRecording>({
      callId: rec.callId,
      recordingId: rec.recordingId,
    }).catch(() => null);
    if (prev && prev.startedAt && prev.startedAt !== rec.startedAt) {
      // Union of both fan-out sets: the roster can shift between writes,
      // and a stranded row under the OLD key is exactly what we're here to
      // remove, whichever set it came from.
      const stale = new Set([...fanoutSetFor(prev), ...fanoutSetFor(rec)]);
      await Promise.all(
        Array.from(stale).map((userId) =>
          this.docClient
            .send(
              new DeleteCommand({
                TableName: this.participantsTable,
                Key: {
                  userId,
                  startedAtRecordingId: `${prev.startedAt}#${rec.recordingId}`,
                },
              }),
            )
            .catch(() => { /* best-effort — listByUser dedupes as well */ }),
        ),
      );
    }

    await this.putItem(full as unknown as Record<string, unknown>);

    const fanoutUserIds = fanoutSetFor(rec);

    // Fan out participant index rows. Promise.all is fine — DDB
    // PutItem is independent per row, and we want one round-trip-batch
    // not 5 serial calls for a 5-person call.
    if (fanoutUserIds.size > 0) {
      await Promise.all(
        Array.from(fanoutUserIds).map((userId) =>
          this.docClient.send(
            new PutCommand({
              TableName: this.participantsTable,
              Item: {
                userId,
                startedAtRecordingId: `${rec.startedAt}#${rec.recordingId}`,
                callId: rec.callId,
                recordingId: rec.recordingId,
                channelArn: rec.channelArn,
                lobbyName: rec.lobbyName,
                startedAt: rec.startedAt,
                endedAt: rec.endedAt,
                // Media length, not session lifetime. The fan-out row has
                // no tracks[] of its own, so this value cannot be re-derived
                // on read — it is the reason the backfill script has to
                // touch BOTH tables (scripts/backfill-recording-truth.ts).
                durationMs: rec.durationMs,
                sessionDurationMs: rec.sessionDurationMs,
                // W8 — denormalize so list view doesn't need a join
                // back to the primary table for thumbnail rendering.
                thumbnailUrl: rec.thumbnailUrl,
                // R2 — denormalize composedUrl onto the fan-out row so
                // the list view can pick "playback target" without a
                // join. tracks[] stays on the primary record (only the
                // detail view needs the raw per-peer URLs).
                composedUrl: rec.composedUrl,
                // Denormalize the reconciled PEOPLE count for the chip
                // render. `participants[]` has already been deduped by
                // userId and scoped to this recording by the webhook, so
                // the `.length` fallback is now a people count too — it
                // used to be a join count (one entry per rejoin), which is
                // how a 2-person DM rendered "7 👥".
                participantCount: rec.participantCount ?? rec.participants?.length ?? 0,
                // Denormalize identities so the list view can render
                // "DM · Bob Martinez & Hank Hill" instead of splitting raw
                // userIds out of the lobby string. Capped so a large room
                // can't push the index row toward the 400KB item limit.
                participants: (rec.participants ?? [])
                  .slice(0, FANOUT_PARTICIPANT_CAP)
                  .map((p) => ({ userId: p.userId, displayName: p.displayName })),
                ttl,
              },
            }),
          ),
        ),
      );
    }
  }

  async getRecording(callId: string, recordingId: string): Promise<CallRecording | null> {
    const rec = await this.getItem<CallRecording>({ callId, recordingId });
    return rec ? normalizeDuration(rec) : null;
  }

  /**
   * Point-get a recording from its id alone, via the `recordingId` GSI.
   *
   * This is the lookup the primary key cannot do (PK `callId` + SK
   * `recordingId`) and the reason every id-only route used to scan.
   * `recordingId` is a uuid, so the query is a single-item read; if a
   * duplicate ever appeared we take the first and log, rather than
   * pretending the ambiguity away.
   */
  async findByRecordingId(recordingId: string): Promise<CallRecording | null> {
    const out = await this.docClient.send(
      new QueryCommand({
        TableName: CALL_RECORDINGS_TABLE,
        IndexName: RECORDING_ID_INDEX,
        KeyConditionExpression: 'recordingId = :r',
        ExpressionAttributeValues: { ':r': recordingId },
        Limit: 1,
      }),
    );
    const item = (out.Items ?? [])[0] as CallRecording | undefined;
    return item ? normalizeDuration(item) : null;
  }

  /**
   * Does this user have a fan-out row for this recording — i.e. did they
   * participate (or start it), and have they not since untagged themselves?
   *
   * A point-get, because by the time we call this we have `startedAt` off
   * the primary row and can build the exact SK. That is the whole trick:
   * the fan-out index was always the authorization source of truth, but
   * without `startedAt` its key was unreachable, so the routes approximated
   * it by listing a page and scanning — and the approximation went wrong
   * the moment a user had more recordings than the page held.
   *
   * Keeping the fan-out row as the answer (rather than re-deriving it from
   * `participants[] ∪ {startedBy}` on the primary row) preserves the GDPR
   * untag path in `deleteByUserId`: that deletes the index row, and a user
   * who has untagged themselves must stay untagged.
   */
  async hasParticipantRow(
    userId: string,
    startedAt: string,
    recordingId: string,
  ): Promise<boolean> {
    const out = await this.docClient.send(
      new GetCommand({
        TableName: this.participantsTable,
        Key: { userId, startedAtRecordingId: `${startedAt}#${recordingId}` },
        ProjectionExpression: 'userId',
      }),
    );
    return Boolean(out.Item);
  }

  /**
   * The whole id-only lookup, in two point-reads.
   *
   * Returns `null` when no such recording exists, and
   * `{ detail, isParticipant: false }` when it exists but this user was not
   * in it — the caller decides how to answer, and for recordings that
   * answer is 404 either way (see routes/recordings.ts).
   *
   * Replaces `listByUser(userId, { limit: 100 })` + `Array.find` in six
   * routes. The old form 404'd on recordings that exist whenever the caller
   * had more than 100 fan-out rows; this form has no page to fall off.
   */
  async resolveForUser(
    userId: string,
    recordingId: string,
  ): Promise<{ detail: CallRecording; isParticipant: boolean } | null> {
    const detail = await this.findByRecordingId(recordingId);
    if (!detail) return null;
    const isParticipant = await this.hasParticipantRow(
      userId,
      detail.startedAt,
      recordingId,
    );
    return { detail, isParticipant };
  }

  /**
   * Write back a compose status reconciled from LVS.
   *
   * LVS fires no webhook when the background compose job finishes, so the
   * only way this service ever learns is by asking — and asking on every
   * read forever, for a value that stops changing once it reaches a
   * terminal state, would be silly. Caching it here means the LVS round
   * trip stops as soon as compose settles.
   *
   * Best-effort by construction: a failed write just means we ask again
   * next time. Conditional on the row existing so a reconciliation for a
   * deleted recording can't resurrect it as a status-only ghost.
   */
  /**
   * When the recording expires — the row's `ttl`, which DynamoDB TTL turns
   * into deletion — on the primary row AND every participant fan-out row,
   * because "list my recordings" reads the fan-out and a recording that
   * expired from one and not the other would be listed but not openable.
   * `null` removes the attribute: the recording is kept until someone
   * deletes it.
   */
  async updateExpiry(rec: Pick<CallRecording, 'callId' | 'recordingId' | 'startedAt' | 'participants' | 'startedBy'>, expiresAt: Date | null): Promise<void> {
    const ttl = expiresAt ? Math.floor(expiresAt.getTime() / 1000) : null;
    const expr = ttl === null
      ? { UpdateExpression: 'REMOVE #ttl', ExpressionAttributeNames: { '#ttl': 'ttl' } }
      : { UpdateExpression: 'SET #ttl = :ttl', ExpressionAttributeNames: { '#ttl': 'ttl' }, ExpressionAttributeValues: { ':ttl': ttl } };
    await this.docClient.send(
      new UpdateCommand({
        TableName: CALL_RECORDINGS_TABLE,
        Key: { callId: rec.callId, recordingId: rec.recordingId },
        ...expr,
        ConditionExpression: 'attribute_exists(callId)',
      }),
    );
    const fanout = fanoutSetFor(rec as CallRecording);
    await Promise.all(
      Array.from(fanout).map((userId) =>
        this.docClient.send(
          new UpdateCommand({
            TableName: this.participantsTable,
            Key: { userId, startedAtRecordingId: `${rec.startedAt}#${rec.recordingId}` },
            ...expr,
            ConditionExpression: 'attribute_exists(userId)',
          }),
        ).catch((e: unknown) => {
          // A fan-out row that was never written (an older recording) is
          // not a reason to fail the change the person asked for.
          if ((e as { name?: string })?.name === 'ConditionalCheckFailedException') return;
          throw e;
        }),
      ),
    );
  }

  async updateComposeState(
    callId: string,
    recordingId: string,
    patch: { composeStatus: ComposeStatus; composedSegment?: { key?: string; url?: string; size?: number } },
  ): Promise<void> {
    const sets = ['composeStatus = :cs', 'composeCheckedAt = :ca'];
    const values: Record<string, unknown> = {
      ':cs': patch.composeStatus,
      ':ca': new Date().toISOString(),
    };
    if (patch.composedSegment) {
      sets.push('composedSegment = :seg');
      values[':seg'] = patch.composedSegment;
    }
    await this.docClient.send(
      new UpdateCommand({
        TableName: CALL_RECORDINGS_TABLE,
        Key: { callId, recordingId },
        UpdateExpression: `SET ${sets.join(', ')}`,
        ExpressionAttributeValues: values,
        ConditionExpression: 'attribute_exists(callId)',
      }),
    );
  }

  /**
   * Owner-initiated hard delete (audit C4). Removes the primary row from
   * video-call-recordings AND every fan-out row for the recording from
   * video-call-recording-participants. The MP4 stays in LVS storage until
   * the retention sweep — same lifecycle as before.
   *
   * Best-effort participant cleanup: per-row failures are swallowed so
   * one stuck DDB key can't strand the primary deletion. Callers should
   * not surface the count to end-users.
   */
  async deleteRecording(callId: string, recordingId: string): Promise<void> {
    const rec = await this.getRecording(callId, recordingId);
    if (rec) {
      // Mirror putRecording's fan-out set EXACTLY — same helper, so the
      // two can't drift. Any row putRecording wrote, this deletes; a
      // narrower set here would strand an index row that keeps surfacing
      // in GET /api/recordings after the recording is gone.
      const fanoutUserIds = fanoutSetFor(rec);
      if (fanoutUserIds.size > 0) {
        await Promise.all(
          Array.from(fanoutUserIds).map((userId) =>
            this.docClient
              .send(
                new DeleteCommand({
                  TableName: this.participantsTable,
                  Key: { userId, startedAtRecordingId: `${rec.startedAt}#${rec.recordingId}` },
                }),
              )
              .catch(() => { /* swallow — participant index is fan-out, not source of truth */ }),
          ),
        );
      }
    }
    await this.docClient.send(
      new DeleteCommand({
        TableName: CALL_RECORDINGS_TABLE,
        Key: { callId, recordingId },
      }),
    );
  }

  /** All recordings for a given call (usually 1, sometimes 2 if media paused mid-call). */
  async listByCall(callId: string): Promise<CallRecording[]> {
    const out = await this.docClient.send(
      new QueryCommand({
        TableName: CALL_RECORDINGS_TABLE,
        KeyConditionExpression: 'callId = :c',
        ExpressionAttributeValues: { ':c': callId },
      }),
    );
    return ((out.Items ?? []) as CallRecording[]).map(normalizeDuration);
  }

  /**
   * Recent recordings for a user via the fan-out index. Descending by
   * startedAt — newest first. Bounded by `limit` (default 50) and
   * optionally `cursor` (the last `startedAtRecordingId` from a prior
   * page) for keyset pagination.
   */
  async listByUser(
    userId: string,
    opts?: { limit?: number; cursor?: string },
  ): Promise<{ items: RecordingListRow[]; cursor?: string }> {
    const out = await this.docClient.send(
      new QueryCommand({
        TableName: this.participantsTable,
        KeyConditionExpression: 'userId = :u',
        ExpressionAttributeValues: { ':u': userId },
        ScanIndexForward: false, // newest first
        Limit: opts?.limit ?? 50,
        ...(opts?.cursor
          ? {
              ExclusiveStartKey: {
                userId,
                startedAtRecordingId: opts.cursor,
              },
            }
          : {}),
      }),
    );
    const rows = (out.Items ?? []).map((i) => ({
      callId: i.callId as string,
      recordingId: i.recordingId as string,
      startedAt: i.startedAt as string,
      endedAt: i.endedAt as string,
      // Media length. The fan-out row has no tracks[], so this cannot be
      // re-derived here — it is whatever the writer stored, which is why
      // scripts/backfill-recording-truth.ts had to rewrite this table.
      durationMs: i.durationMs as number,
      sessionDurationMs: i.sessionDurationMs as number | undefined,
      channelArn: i.channelArn as string,
      lobbyName: i.lobbyName as string,
      thumbnailUrl: i.thumbnailUrl as string | undefined,
      composedUrl: i.composedUrl as string | undefined,
      participantCount: i.participantCount as number | undefined,
      // Denormalized identities (userId + displayName). Absent on rows
      // written before 2026-08-25; the FE falls back to the lobby-derived
      // ids for those.
      participants: Array.isArray(i.participants)
        ? (i.participants as CallRecordingParticipant[])
        : undefined,
    }));
    // Two guards the fan-out index cannot provide on its own, in one pass.
    const items = await this.hydrateComposeState(dedupeByRecordingId(rows));
    const cursor = out.LastEvaluatedKey
      ? (out.LastEvaluatedKey.startedAtRecordingId as string)
      : undefined;
    return { items, cursor };
  }

  /**
   * Fill in `composeStatus` / `transcriptStatus` for fan-out rows.
   *
   * The fan-out table cannot carry them. Not "does not" — cannot: both are
   * MUTABLE. `updateComposeState` and `updateTranscriptStatus` write the
   * primary table only, and a denormalized copy on the index row would be
   * frozen at webhook time, which is before compose has even started. A
   * stale "queued" badge on a recording that finished an hour ago is a
   * different wrong answer, not a better one.
   *
   * So we join — one BatchGetItem over the page (≤100 keys, well inside the
   * limit, one round trip) projecting only what `deriveComposeStatus` needs.
   * Rows the join misses keep `composeStatus: undefined`, which the FE reads
   * as "legacy, let it open" — the same tolerance as before this existed.
   */
  private async hydrateComposeState(
    rows: RecordingListRow[],
  ): Promise<RecordingListRow[]> {
    if (rows.length === 0) return rows;
    // BatchGetItem caps at 100 keys per table per call; a page is ≤100 by
    // construction (listByUser's Limit) but chunk anyway so a future caller
    // raising the limit doesn't get a ValidationException instead of a list.
    const chunks: RecordingListRow[][] = [];
    for (let i = 0; i < rows.length; i += 100) chunks.push(rows.slice(i, i + 100));

    const byId = new Map<string, ComposableRecording & { transcriptStatus?: CallRecording['transcriptStatus'] }>();
    for (const chunk of chunks) {
      try {
        const res = await this.docClient.send(
          new BatchGetCommand({
            RequestItems: {
              [CALL_RECORDINGS_TABLE]: {
                Keys: chunk.map((r) => ({ callId: r.callId, recordingId: r.recordingId })),
                // `segments`/`tracks` are reserved-word-free; composeStatus and
                // the rest are plain attribute names. Projected narrowly so a
                // 50-row page doesn't drag 50 full track manifests across.
                ProjectionExpression:
                  'recordingId, composeStatus, composedUrl, composedSegment, segments, tracks, transcriptStatus',
              },
            },
          }),
        );
        for (const raw of res.Responses?.[CALL_RECORDINGS_TABLE] ?? []) {
          byId.set(raw.recordingId as string, raw as never);
        }
      } catch {
        // A failed join costs badges, not the list. Falling through with
        // `undefined` is the pre-existing behaviour, so this degrades to
        // exactly what shipped before rather than to an error page.
      }
    }

    return rows.map((r) => {
      const primary = byId.get(r.recordingId);
      if (!primary) return r;
      return {
        ...r,
        composeStatus: deriveComposeStatus(primary),
        transcriptStatus: primary.transcriptStatus,
      };
    });
  }

  /**
   * W6 — List recordings for a given lobby (e.g. `room:lounge`). Used by the
   * RoomAdminPage "View recordings" filter so room admins can see every
   * recording produced inside their room.
   *
   * TODO: v1 implementation does a Scan filtered by `lobbyName` (capped at
   * 100 rows per page) since there's no GSI on lobbyName today. Acceptable
   * because the primary table is bounded by RETENTION_DAYS, but if volume
   * grows we should promote `lobbyName` to a GSI HASH with `startedAt` as
   * the RANGE so this becomes a Query.
   *
   * The cursor is the opaque LastEvaluatedKey from the prior page (encoded
   * to a base64 JSON string so it round-trips cleanly through the URL).
   * Results are returned in whatever order DDB Scan yields — callers that
   * need recency-ordering should sort client-side.
   */
  async listByLobbyName(
    lobbyName: string,
    limit = 50,
    cursor?: string,
  ): Promise<{ items: RecordingListRow[]; cursor?: string }> {
    const cappedLimit = Math.max(1, Math.min(100, limit));
    let exclusiveStartKey: Record<string, unknown> | undefined;
    if (cursor) {
      try {
        exclusiveStartKey = JSON.parse(Buffer.from(cursor, 'base64').toString('utf8')) as Record<string, unknown>;
      } catch {
        // Bad cursor → ignore and start from the top.
        exclusiveStartKey = undefined;
      }
    }
    const out = await this.docClient.send(
      new ScanCommand({
        TableName: CALL_RECORDINGS_TABLE,
        FilterExpression: 'lobbyName = :l',
        ExpressionAttributeValues: { ':l': lobbyName },
        Limit: cappedLimit,
        ...(exclusiveStartKey ? { ExclusiveStartKey: exclusiveStartKey } : {}),
      }),
    );
    // This one scans the PRIMARY table, so unlike listByUser it has the
    // full row — tracks[] and the raw roster — and can re-derive the truth
    // on read rather than trusting whatever the writer of the day stored.
    const items: RecordingListRow[] = (out.Items ?? []).map((raw) => {
      const i = normalizeDuration(raw as unknown as CallRecording);
      const people = reconcileParticipants({
        participants: i.participants,
        tracks: i.tracks,
        startedBy: i.startedBy,
        startedAt: i.startedAt,
        endedAt: i.endedAt,
      });
      return {
        callId: i.callId,
        recordingId: i.recordingId,
        startedAt: i.startedAt,
        endedAt: i.endedAt,
        durationMs: i.durationMs,
        sessionDurationMs: i.sessionDurationMs,
        channelArn: i.channelArn,
        lobbyName: i.lobbyName,
        thumbnailUrl: i.thumbnailUrl,
        composedUrl: i.composedUrl,
        // This scan has the full primary row, so the compose state needs no
        // join — it is right here, and omitting it was what left the FE's
        // "is this playable" question permanently unanswerable.
        composeStatus: deriveComposeStatus(i),
        transcriptStatus: i.transcriptStatus,
        ttl: typeof i.ttl === 'number' ? (i.ttl as number) : undefined,
        // Distinct PEOPLE. Deliberately re-derived instead of reading the
        // stored scalar: legacy rows stored `participants.length`, which
        // counted rejoins (the "2, 3, 4, 5, 6, 7 👥 over one evening" bug).
        // `undefined` rather than 0 when we know nothing — the FE renders
        // no chip for undefined, and "0 people were in this call" would be
        // its own small lie.
        participantCount: people.participantCount || undefined,
        participants: people.participants.length
          ? people.participants.map((p) => ({
              userId: p.userId,
              displayName: p.displayName,
            }))
          : undefined,
      };
    });
    const nextCursor = out.LastEvaluatedKey
      ? Buffer.from(JSON.stringify(out.LastEvaluatedKey), 'utf8').toString('base64')
      : undefined;
    return { items, ...(nextCursor ? { cursor: nextCursor } : {}) };
  }

  /**
   * P5 (H7) — GDPR delete. Removes the user from:
   *  - the participant fan-out table (all their rows in
   *    video-call-recording-participants)
   *  - the participants[] array inside video-call-recordings rows they
   *    were part of (so list-by-call no longer shows them)
   *
   * Does NOT delete the underlying MP4 — that's LVS's responsibility
   * via the GC sweep against retention. Does NOT delete the
   * video-call-recordings row when other participants are still on it.
   *
   * Returns the count of touched rows for audit logging. Best-effort:
   * partial failures log and continue (DDB retries handle transient).
   */
  async deleteByUserId(userId: string): Promise<{ indexRowsDeleted: number; recordingsRedacted: number }> {
    let indexRowsDeleted = 0;
    let recordingsRedacted = 0;

    // Drain the reverse-index page-by-page so a user with thousands of
    // recordings doesn't OOM the process.
    let cursor: string | undefined;
    const touchedRecordingIds = new Set<string>();
    const recordingCallIds = new Map<string, string>(); // recordingId -> callId

    do {
      const page = await this.listByUser(userId, { limit: 100, cursor });
      for (const item of page.items) {
        // Delete the participant-index row.
        try {
          await this.docClient.send(
            new DeleteCommand({
              TableName: this.participantsTable,
              Key: { userId, startedAtRecordingId: `${item.startedAt}#${item.recordingId}` },
            }),
          );
          indexRowsDeleted += 1;
          touchedRecordingIds.add(item.recordingId);
          recordingCallIds.set(item.recordingId, item.callId);
        } catch (_) { /* swallow — best-effort */ }
      }
      cursor = page.cursor;
    } while (cursor);

    // Redact this user from each recording's participants[] array.
    for (const recordingId of touchedRecordingIds) {
      const callId = recordingCallIds.get(recordingId);
      if (!callId) continue;
      const rec = await this.getRecording(callId, recordingId);
      if (!rec) continue;
      const filtered = (rec.participants ?? []).filter((p) => p.userId !== userId);
      try {
        await this.docClient.send(
          new UpdateCommand({
            TableName: CALL_RECORDINGS_TABLE,
            Key: { callId, recordingId },
            UpdateExpression: 'SET participants = :p',
            ExpressionAttributeValues: { ':p': filtered },
          }),
        );
        recordingsRedacted += 1;
      } catch (_) { /* swallow */ }
    }

    return { indexRowsDeleted, recordingsRedacted };
  }

  /**
   * Flip transcriptStatus on a recording row. Used at three transition points:
   *   1. lvsWebhooks  → 'pending'    (right after publishWithOutbox)
   *   2. worker start → 'processing' (after MQ receive, before whisper run)
   *   3. worker done  → 'ready'      (with transcriptKey, transcriptGeneratedAt)
   *   4. DLQ          → 'failed'     (with transcriptError)
   *
   * Conditional on the row existing (attribute_exists(callId)) so a transcript
   * update for a webhook-not-yet-persisted recording fails fast rather than
   * silently creating a transcript-only ghost row.
   *
   * Every call also stamps `transcriptStatusAt`. That timestamp is what the
   * stall check measures from — see the field's own comment for why
   * `endedAt` was the wrong clock — and stamping it here rather than at the
   * four call sites means no future transition can forget to.
   *
   * `summaryStatus` / `summaryError` ride along on the same write because
   * the summary consumer settles both halves in one place, but they are an
   * independent axis: a failed summary must not move `transcriptStatus`.
   * Passing `null` for either error field REMOVEs it, so a redelivery that
   * finally succeeds does not leave the previous attempt's error string
   * sitting next to a 'ready' status.
   */
  async updateTranscriptStatus(
    callId: string,
    recordingId: string,
    patch: {
      status: 'pending' | 'processing' | 'ready' | 'failed';
      transcriptKey?: string;
      /** The transcript itself, when stored on the row (`transcriptKey === 'inline'`). */
      transcriptJson?: string;
      summaryKey?: string;
      summaryMarkdown?: string;
      transcriptGeneratedAt?: string;
      transcriptError?: string | null;
      /** `skipped`: the transcript is ready but there was no speech to summarise. */
      summaryStatus?: 'pending' | 'processing' | 'ready' | 'failed' | 'skipped';
      summaryError?: string | null;
      /** Override for the status clock. Tests only; production uses now. */
      statusAt?: string;
    },
  ): Promise<void> {
    const sets: string[] = ['transcriptStatus = :s', 'transcriptStatusAt = :sat'];
    const removes: string[] = [];
    const names: Record<string, string> = {};
    const values: Record<string, unknown> = {
      ':s': patch.status,
      ':sat': patch.statusAt ?? new Date().toISOString(),
    };
    if (patch.summaryStatus !== undefined) {
      sets.push('summaryStatus = :ss');
      values[':ss'] = patch.summaryStatus;
    }
    if (patch.summaryError === null) {
      removes.push('summaryError');
    } else if (patch.summaryError !== undefined) {
      sets.push('summaryError = :serr');
      values[':serr'] = String(patch.summaryError).slice(0, 200);
    }
    if (patch.transcriptKey !== undefined) {
      sets.push('transcriptKey = :k');
      values[':k'] = patch.transcriptKey;
    }
    if (patch.transcriptJson !== undefined) {
      sets.push('transcriptJson = :tj');
      values[':tj'] = patch.transcriptJson;
    }
    if (patch.summaryKey !== undefined) {
      sets.push('summaryKey = :sk');
      values[':sk'] = patch.summaryKey;
    }
    if (patch.summaryMarkdown !== undefined) {
      sets.push('summaryMarkdown = :sm');
      values[':sm'] = patch.summaryMarkdown;
    }
    if (patch.transcriptGeneratedAt !== undefined) {
      sets.push('transcriptGeneratedAt = :ga');
      values[':ga'] = patch.transcriptGeneratedAt;
    }
    if (patch.transcriptError === null) {
      removes.push('transcriptError');
    } else if (patch.transcriptError !== undefined) {
      sets.push('transcriptError = :err');
      // Truncate to 200 chars so a multi-MB stack trace can't bloat the row.
      values[':err'] = String(patch.transcriptError).slice(0, 200);
    }
    const updateExpression = removes.length
      ? `SET ${sets.join(', ')} REMOVE ${removes.join(', ')}`
      : `SET ${sets.join(', ')}`;
    await this.docClient.send(
      new UpdateCommand({
        TableName: CALL_RECORDINGS_TABLE,
        Key: { callId, recordingId },
        UpdateExpression: updateExpression,
        ExpressionAttributeNames: Object.keys(names).length ? names : undefined,
        ExpressionAttributeValues: values,
        ConditionExpression: 'attribute_exists(callId)',
      }),
    );
  }
}

/**
 * Schema for scripts/setup-dynamodb.sh:
 *
 *   aws dynamodb create-table \
 *     --table-name ${DDB_TABLE_PREFIX}video-call-recordings \
 *     --attribute-definitions \
 *       AttributeName=callId,AttributeType=S \
 *       AttributeName=recordingId,AttributeType=S \
 *     --key-schema \
 *       AttributeName=callId,KeyType=HASH \
 *       AttributeName=recordingId,KeyType=RANGE \
 *     --global-secondary-indexes \
 *       'IndexName=recordingId-index,KeySchema=[{AttributeName=recordingId,KeyType=HASH}],Projection={ProjectionType=ALL}' \
 *     --billing-mode PAY_PER_REQUEST
 *
 *   # The GSI is what lets a URL carrying only a recordingId point-get.
 *   # Without it findByRecordingId throws ValidationException — which is
 *   # the intended failure: a loud misconfiguration beats a silent fallback
 *   # to the bounded scan this index exists to delete.
 *
 *   aws dynamodb create-table \
 *     --table-name ${DDB_TABLE_PREFIX}video-call-recording-participants \
 *     --attribute-definitions \
 *       AttributeName=userId,AttributeType=S \
 *       AttributeName=startedAtRecordingId,AttributeType=S \
 *     --key-schema \
 *       AttributeName=userId,KeyType=HASH \
 *       AttributeName=startedAtRecordingId,KeyType=RANGE \
 *     --billing-mode PAY_PER_REQUEST
 *
 *   # TTL on both:
 *   aws dynamodb update-time-to-live --table-name ... \
 *     --time-to-live-specification "Enabled=true, AttributeName=ttl"
 */
