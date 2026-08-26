// lib/recording-truth.ts
//
// Three pure derivations that decide what a recording row is ALLOWED to
// claim about itself. They live here — not inline in the webhook — because
// the same answers are needed on the write path (lvsWebhooks), the read
// path (CallRecordingRepository), and the one-shot backfill script, and
// three copies of "roughly this logic" is how the UI started lying in the
// first place.
//
// Background — what the LVS `recording.completed` payload actually is:
//
//   durationMs   the SFU CHANNEL SESSION lifetime (endedAt - startedAt in
//                RecordingManager). It counts the ring, the lobby wait and
//                the teardown, not the media. Measured 2026-08-25 against
//                ffprobe on 98 composed.mp4 files: 98/98 overstated,
//                median +10.2s, worst +4556% (23.3s claimed / 0.5s of
//                media). This is the number the UI rendered as "Duration".
//
//   tracks[]     one entry per recorded leg, each with `startedAtMs` /
//                `endedAtMs` / `durationMs` RELATIVE TO THE RECORDING
//                EPOCH. This is the media timeline — the composed grid is
//                built from exactly these legs, so its length is the span
//                they cover.
//
//   participants the VideoSession's LIVE roster, which is the channel's
//                lifetime roster: one append per join, never deduped, never
//                scoped to a single recording. A 2-person DM produced rows
//                with 7 entries (4x hank + 3x bob) whose oldest joinedAt
//                predated the recording by 46 minutes.

/** Track shape we depend on. Everything is optional — LVS versions vary. */
export interface TruthTrack {
  userId?: string;
  participantId?: string;
  legId?: string;
  startedAtMs?: number;
  endedAtMs?: number;
  durationMs?: number;
  key?: string;
  url?: string;
  file?: string;
  size?: number;
  kind?: string;
}

export interface TruthParticipant {
  userId: string;
  displayName?: string;
  participantId?: string;
  joinedAt?: string;
}

// --------------------------------------------------------------------
// 1. Duration
// --------------------------------------------------------------------

/**
 * How long the RECORDED MEDIA runs, in ms.
 *
 * Order of preference:
 *   1. `composedDurationMs` — if a future LVS ever ships the probed length
 *      of composed.mp4 on the webhook, that is ground truth (LVS already
 *      computes it into `composeReport.outputProbedDurationMs`, it just
 *      isn't on the wire yet because compose runs AFTER the completed
 *      event). Accepted here so adopting it is a one-field change.
 *   2. the track timeline — `max(endedAtMs) - min(startedAtMs)`, i.e. the
 *      span from the first leg opening to the last leg closing. The
 *      composed grid covers exactly this window.
 *   3. `max(track.durationMs)` for older payloads that carry durations but
 *      no offsets.
 *   4. the session lifetime, as a last resort, because "some number" beats
 *      "0s" on a row with no tracks at all (single-producer / legacy).
 *
 * WHY THE TRACK TIMELINE AND NOT AN ffprobe OF THE FILE: platform-api does
 * not own the media volume (LVS does, and for `destination: 's3'` nobody
 * owns a local path at all), and compose finishes asynchronously after the
 * webhook lands — so there is no file to probe at write time. The track
 * timeline is on the payload we already receive, costs no I/O, and is the
 * same window ffmpeg composes over. Measured: median absolute error vs
 * ffprobe drops from 10,211ms (session lifetime) to 146ms.
 *
 * The residual: recordings whose compose came out `degraded` (ffmpeg exits
 * 0 but writes a sparse/truncated grid — dev-box RTP starvation) have a
 * composed.mp4 much SHORTER than the leg timeline. The timeline is still
 * the honest statement of how long the call's media ran; the file being
 * short is a compose defect, not a duration we should adopt. The FE also
 * prefers the player's own `video.duration` once loaded, so the metadata
 * card can no longer contradict the timeline sitting above it.
 *
 * Never returns more than the session lifetime (when known) — media cannot
 * outlast the session that produced it — and never returns a negative.
 */
export function deriveMediaDurationMs(input: {
  tracks?: TruthTrack[] | null;
  /** The SFU session lifetime, i.e. the legacy `durationMs`. */
  sessionDurationMs?: number | null;
  /** Probed length of composed.mp4, when a caller has it. */
  composedDurationMs?: number | null;
}): number {
  const sessionMs =
    typeof input.sessionDurationMs === 'number' && Number.isFinite(input.sessionDurationMs)
      ? Math.max(0, input.sessionDurationMs)
      : null;

  const clamp = (v: number): number => {
    const rounded = Math.max(0, Math.round(v));
    return sessionMs != null ? Math.min(rounded, sessionMs) : rounded;
  };

  if (
    typeof input.composedDurationMs === 'number'
    && Number.isFinite(input.composedDurationMs)
    && input.composedDurationMs > 0
  ) {
    return clamp(input.composedDurationMs);
  }

  const tracks = Array.isArray(input.tracks) ? input.tracks : [];
  const num = (v: unknown): number | null =>
    typeof v === 'number' && Number.isFinite(v) ? v : null;

  const ends = tracks.map((t) => num(t?.endedAtMs)).filter((v): v is number => v != null);
  if (ends.length > 0) {
    const starts = tracks.map((t) => num(t?.startedAtMs)).filter((v): v is number => v != null);
    const first = starts.length > 0 ? Math.min(...starts) : 0;
    const last = Math.max(...ends);
    const span = last - first;
    if (span > 0) return clamp(span);
  }

  const durs = tracks.map((t) => num(t?.durationMs)).filter((v): v is number => v != null);
  if (durs.length > 0) {
    const longest = Math.max(...durs);
    if (longest > 0) return clamp(longest);
  }

  return sessionMs ?? 0;
}

// --------------------------------------------------------------------
// 2. Participants
// --------------------------------------------------------------------

/**
 * How far BEFORE a recording's own `startedAt` a roster entry may have
 * joined and still plausibly belong to that recording.
 *
 * Recording start normally lags the first join (LVS starts recording when
 * the second producer appears), so pre-start joins are the common case, not
 * the anomaly: 181/200 rows measured had at least one. Only wildly old
 * entries — a channel roster carried across an entire evening — are stale.
 * Measured join-age percentiles: p50 1s, p75 19s, p90 411s, p99 2748s.
 * 15 minutes sits above the ordinary shape and below the roster-rot cases.
 */
export const ROSTER_JOIN_GRACE_MS = Number(
  process.env.RECORDING_ROSTER_GRACE_MS ?? 15 * 60 * 1000,
);

export interface ReconciledParticipants {
  /** Deduped by userId, scoped to this recording, initiator included. */
  participants: TruthParticipant[];
  /** Distinct PEOPLE, never a session/join count. */
  participantCount: number;
}

/**
 * Turn "the channel's lifetime roster" into "the people in THIS recording".
 *
 * Three defects being corrected, all measured on live data 2026-08-25:
 *
 *   a. NOT DEDUPED — every refresh/rejoin appends another entry, so one
 *      2-person DM channel rendered 2, 3, 4, 5, 6 and 7 people over one
 *      evening. 18/200 rows had duplicate userIds; the worst had 9 entries
 *      for 4 people.
 *
 *   b. NOT SCOPED — entries survive from earlier calls in the same lobby.
 *      22/200 rows carried a join more than 5 minutes older than their own
 *      recording; the worst was 10.6 hours.
 *
 *   c. THE INITIATOR IS MISSING — `session.participants` is the roster at
 *      recording-COMPLETE time, and the person who placed the call has
 *      often already hung up. 37/200 rows omitted their own `startedBy`.
 *      That is the same bug as the silent fan-out loss (they never saw
 *      their own recording) and it is why 2-person DMs rendered "1 person".
 *
 * Scoping is deliberately CONSERVATIVE. Narrowing the window can only ever
 * remove people, and removing a real participant is a worse lie than
 * keeping a stale one — so when the scoped roster ends up smaller than the
 * number of legs actually recorded, we discard the scoping and keep the
 * full deduped roster. Under-reporting is never introduced by this
 * function; the count is additionally floored at the recorded-leg count,
 * since a leg that carries only a `participantId` (LVS multi-leg tracks
 * have no userId) is still a real tile in the composed video.
 */
export function reconcileParticipants(input: {
  participants?: TruthParticipant[] | null;
  tracks?: TruthTrack[] | null;
  startedBy?: string | null;
  startedAt?: string | null;
  endedAt?: string | null;
}): ReconciledParticipants {
  const raw = (Array.isArray(input.participants) ? input.participants : []).filter(
    (p): p is TruthParticipant => !!p && typeof p.userId === 'string' && p.userId.length > 0,
  );
  const tracks = Array.isArray(input.tracks) ? input.tracks : [];

  const startedAtMs = parseIso(input.startedAt);
  const endedAtMs = parseIso(input.endedAt);

  // (b) window: joined before the recording ended, and not absurdly before
  // it started.
  const inWindow = (p: TruthParticipant): boolean => {
    const joined = parseIso(p.joinedAt);
    if (joined == null) return true; // no timestamp → can't disqualify
    if (endedAtMs != null && joined > endedAtMs) return false; // joined after it ended
    if (startedAtMs != null && joined < startedAtMs - ROSTER_JOIN_GRACE_MS) return false;
    return true;
  };

  const legPeers = new Set<string>();
  for (const t of tracks) {
    const key = t?.userId ?? t?.participantId ?? t?.legId;
    if (key != null && String(key).length > 0) legPeers.add(String(key));
  }

  const scoped = raw.filter(inWindow);
  // Conservative fallback — scoping may only ever REMOVE people, so if it
  // removed more than the recorded legs can account for (or emptied the
  // roster outright), distrust the scoping and keep the full roster. A
  // stale name on the chip strip is a smaller lie than a missing one.
  const scopingIsSafe =
    scoped.length === raw.length
    || (scoped.length > 0 && distinctCount(scoped) >= legPeers.size);
  const base = scopingIsSafe ? scoped : raw;

  // (a) dedupe by userId, keeping the entry with the most recent joinedAt
  // (the freshest display name / participantId for this call).
  const byUser = new Map<string, TruthParticipant>();
  for (const p of base) {
    const prev = byUser.get(p.userId);
    if (!prev) { byUser.set(p.userId, p); continue; }
    const a = parseIso(prev.joinedAt) ?? -Infinity;
    const b = parseIso(p.joinedAt) ?? -Infinity;
    byUser.set(p.userId, b >= a ? mergeParticipant(prev, p) : mergeParticipant(p, prev));
  }

  // Recorded legs that carry a userId are participants even when the live
  // roster dropped them (they left before the call ended).
  for (const t of tracks) {
    const userId = typeof t?.userId === 'string' ? t.userId : undefined;
    if (userId && !byUser.has(userId)) {
      byUser.set(userId, {
        userId,
        participantId: typeof t?.participantId === 'string' ? t.participantId : undefined,
      });
    }
  }

  // (c) the initiator was, definitionally, in their own call.
  if (input.startedBy && !byUser.has(input.startedBy)) {
    byUser.set(input.startedBy, { userId: input.startedBy });
  }

  const participants = Array.from(byUser.values());
  return {
    participants,
    participantCount: Math.max(participants.length, legPeers.size),
  };
}

/** Prefer `primary`'s fields, filling blanks from `secondary`. */
function mergeParticipant(primary: TruthParticipant, secondary: TruthParticipant): TruthParticipant {
  return {
    userId: primary.userId,
    displayName: primary.displayName ?? secondary.displayName,
    participantId: primary.participantId ?? secondary.participantId,
    joinedAt: primary.joinedAt ?? secondary.joinedAt,
  };
}

function distinctCount(ps: TruthParticipant[]): number {
  return new Set(ps.map((p) => p.userId)).size;
}

function parseIso(v: string | null | undefined): number | null {
  if (!v || typeof v !== 'string') return null;
  const t = Date.parse(v);
  return Number.isFinite(t) ? t : null;
}

// --------------------------------------------------------------------
// 3. Track URLs
// --------------------------------------------------------------------

/**
 * LVS stamps `tracks[].url` (and `.file`) with the absolute on-disk path of
 * the recorder pod — in local dev that is a
 * `file:///Users/<developer>/Projects/...` URI. Serving it does two bad
 * things at once: a browser cannot open it, and the API leaks a developer's
 * home directory (in production, the pod's filesystem layout) to every
 * caller.
 *
 * Rewrite each track onto the SAME proxied shape the composed file already
 * uses — `/api/channels/:arn/recordings/:id/segments/:filename` — derived
 * from the track's storage `key`, and drop the raw path fields entirely.
 * When no key is recoverable the url is dropped rather than passed through:
 * a missing download link is strictly better than one that leaks a path and
 * 404s anyway.
 */
export function publicizeTracks<T extends TruthTrack>(
  tracks: T[] | null | undefined,
  opts: { channelArn: string; recordingId: string; publicBaseUrl: string },
): Array<Omit<T, 'file'>> {
  if (!Array.isArray(tracks)) return [];
  const base = (opts.publicBaseUrl || '').replace(/\/+$/, '');
  return tracks.map((t) => {
    const { file: _file, ...rest } = t as TruthTrack & { file?: unknown };
    const filename = trackFilename(t);
    const url = filename
      ? `${base}/api/channels/${encodeURIComponent(opts.channelArn)}/recordings/${encodeURIComponent(opts.recordingId)}/segments/${encodeURIComponent(filename)}`
      : undefined;
    const out = { ...rest } as Record<string, unknown>;
    if (url) out.url = url;
    else delete out.url;
    return out as Omit<T, 'file'>;
  });
}

/** Basename of the track's stored object, or null when unrecoverable. */
function trackFilename(t: TruthTrack): string | null {
  const candidate = t?.key ?? t?.url ?? t?.file;
  if (typeof candidate !== 'string' || candidate.length === 0) return null;
  const name = candidate.split(/[\\/]/).pop();
  if (!name || !name.includes('.')) return null;
  return name;
}
