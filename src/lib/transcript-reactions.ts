/**
 * transcript-reactions — pure helper that injects in-call emoji
 * reactions into a transcript document as synthetic segments, e.g.
 *
 *   { startMs: 192000, endMs: 193000, speakerId: 'u-7',
 *     speakerLabel: 'Connor', text: 'reacted with 👍', kind: 'reaction' }
 *
 * which renders in the prompt/transcript views as
 *   [03:12] Connor: reacted with 👍
 *
 * Used in two places:
 *   1. GET /recordings/:recordingId/transcript (src/routes/recordings.ts)
 *      — playback UI sees reactions inline with speech segments.
 *   2. the call-transcription pipeline (src/recordings/recordingPipelines.ts)
 *      — the summarising model's prompt sees the reaction lines alongside speech.
 *
 * Pure function, no I/O.
 */

export interface TranscriptSegmentLike {
  startMs: number;
  endMs: number;
  speakerId: string;
  speakerLabel: string;
  text: string;
  /** 'reaction' on injected segments; absent on speech segments. */
  kind?: string;
}

export interface ReactionLike {
  userId?: string | null;
  displayName?: string;
  emoji: string;
  at: string; // ISO
}

const REACTION_SEGMENT_DURATION_MS = 1000;

/**
 * Returns a NEW doc whose segments are the original speech segments plus
 * one synthetic segment per reaction, stably sorted ascending by startMs.
 *
 * Reaction offsets are computed relative to `recordingStartedAtIso` and
 * clamped to 0 (a reaction stamped before the recording actually started
 * — clock skew — lands at the top rather than being dropped). Reactions
 * with an unparseable `at` are skipped.
 */
export function mergeReactionsIntoTranscript<
  S extends TranscriptSegmentLike,
  D extends { segments: S[] },
>(
  doc: D,
  reactions: ReactionLike[],
  recordingStartedAtIso: string,
): Omit<D, 'segments'> & { segments: Array<S | TranscriptSegmentLike> } {
  const startedAtMs = Date.parse(recordingStartedAtIso);

  const reactionSegments: TranscriptSegmentLike[] = [];
  for (const r of reactions) {
    const atMs = Date.parse(r.at);
    if (Number.isNaN(atMs) || Number.isNaN(startedAtMs)) continue;
    const startMs = Math.max(0, atMs - startedAtMs);
    reactionSegments.push({
      startMs,
      endMs: startMs + REACTION_SEGMENT_DURATION_MS,
      speakerId: r.userId ?? 'reaction',
      speakerLabel: r.displayName ?? r.userId ?? 'Someone',
      text: `reacted with ${r.emoji}`,
      kind: 'reaction',
    });
  }

  // Array.prototype.sort is stable (ES2019+), so speech segments keep
  // their original relative order and a reaction at the same startMs
  // lands after the speech segment it interleaves with.
  const segments: Array<S | TranscriptSegmentLike> = [
    ...(doc.segments ?? []),
    ...reactionSegments,
  ].sort((a, b) => a.startMs - b.startMs);

  return { ...doc, segments };
}
