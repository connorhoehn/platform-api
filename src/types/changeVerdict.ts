// platform-api/src/types/changeVerdict.ts
//
// The agentic verdict attached to ONE proposed document change.
//
// SHARED CONTRACT. The review-queue UI renders against exactly these fields; a
// second, divergent definition on the client is the failure mode this file
// exists to prevent, so the shape below is the one both halves agree on and it
// does not change without both halves changing together.
//
// Plain TypeScript interfaces on purpose — no zod, no ts-rest, no tRPC. Runtime
// schema libraries are banned in this project.
//
// WHAT A VERDICT IS, AND WHAT IT IS NOT
//
// It is advice attached to a human decision. Nothing in this codebase applies a
// change because a verdict said `pass`; the accept path is `POST
// .../work-items/:itemId/applied`, it is driven by a person, and it requires a
// receipt proving the edit actually landed. A verdict never touches it.
//
// THERE IS NO 'failed' VERDICT, DELIBERATELY
//
// When the reviewing model cannot be reached, times out, or answers with
// something unusable, that is NOT a `ChangeReviewVerdict`. The route answers
// non-2xx with `{ state: 'failed', ... }` (see VerdictFailure below) and writes
// nothing to the cache. Encoding failure as a fourth verdict value would put it
// one careless `??` away from being rendered as a chip beside a change, and a
// chip is read as a judgement.

/** The three things the reviewer is allowed to say. */
export type ChangeVerdict = 'pass' | 'concern' | 'fail';

export interface ChangeReviewVerdict {
  verdict: ChangeVerdict;
  /** One line, always shown beside the chip. */
  headline: string;
  /** Expandable detail. Optional. */
  reasoning?: string;
  producedAt: string;      // ISO
  /** The change revision this verdict was computed against. */
  forRevision: string;
}

/**
 * What the endpoints return alongside the verdict.
 *
 * `currentRevision` is what the change hashes to RIGHT NOW. The UI compares it
 * to `verdict.forRevision`: equal means the verdict was computed against the
 * change as it currently stands; different means the change has been edited
 * since and the verdict describes text that no longer exists. That second case
 * must render as stale, never as a live judgement — a confident `pass` sitting
 * on a change that has since been rewritten is worse than no verdict at all.
 */
export interface ChangeVerdictResponse {
  /** Present only when it was computed against `currentRevision`. */
  verdict: ChangeReviewVerdict | null;
  /** Served from cache without calling the model. */
  cached: boolean;
  /** Hash of the change's current content. See lib/changeVerdict.ts. */
  currentRevision: string;
  /**
   * The newest verdict we hold for this change that was computed against a
   * DIFFERENT revision. Offered so the UI can show "reviewed before your edit"
   * rather than an empty space — it is history, not a verdict on this text.
   */
  stale?: ChangeReviewVerdict;
}

/**
 * The failure shape. Carried in a non-2xx body so no caller can mistake it for
 * a verdict by reading `.verdict` off it.
 */
export interface VerdictFailure {
  state: 'failed';
  /** Which way it failed — for the operator, and for choosing retry wording. */
  reason: 'provider_unavailable' | 'timed_out' | 'unusable_reply';
  error: string;
  detail?: string;
  currentRevision: string;
}
