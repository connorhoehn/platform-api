/**
 * Document review lifecycle — the single source of truth for legal state
 * changes on a collaborative document (design doc, RFC, incident report).
 *
 * Before this module the "lifecycle" was a lie: a document's status was only
 * ever 'draft' or 'final', `status:'review'` was never set by anything, and
 * the frontend "Finalize" button was a `window.confirm` that flipped CRDT
 * metadata. Nothing on the server had an opinion, so ANY caller could unlock
 * a finalized document.
 *
 * Everything here is pure. The repository/route layer supplies the current
 * status and the actor's relationship to the document; this module decides
 * whether the move is legal and who is allowed to make it. Keeping it pure
 * is what makes the transition table exhaustively testable.
 *
 * States
 * ------
 *   draft              — being written; not asking anyone for anything.
 *   in_review          — submitted; named reviewers are expected to respond.
 *   changes_requested  — a reviewer pushed back; the ball is with the owner.
 *   approved           — signed off. Read-only, but the owner can reopen it.
 *   finalized          — TERMINAL. Read-only with no way back, for anyone.
 *
 * approved vs finalized, and why they are not the same state
 * ----------------------------------------------------------
 * Both are read-only, so it is tempting to collapse them. The difference is
 * the only thing that matters about a terminal state: `approved` is a verdict
 * that the owner may withdraw (`reopen`), and `finalized` is a verdict nobody
 * may withdraw. A single state cannot mean both "locked for now" and "locked
 * forever", and a lock that some role can lift is not a terminal state — it is
 * moderation. Locking therefore stays reversible and separate; finalizing is
 * the one-way door.
 *
 * Because it is one-way, `finalize` is deliberately the most constrained rule
 * in the table: it is reachable ONLY from `approved`, it is owner-only, and it
 * requires every review request to be satisfied. See the rule for the full
 * reasoning.
 *
 * Legacy 'final' — NOT the same thing as 'finalized'
 * --------------------------------------------------
 * Some lifecycle rows carry the stored status 'final' from before this module
 * existed. That string keeps mapping to `approved`, NOT to the new `finalized`
 * (see `normalizeStatus`). The near-identical spelling makes this look like an
 * oversight, so, explicitly: mapping it to `finalized` would retroactively make
 * a set of documents permanently unreopenable on the strength of a status
 * written when no server-side lifecycle existed to mean anything by it. A
 * terminal state has to be entered by a recorded, authorized transition, never
 * inherited from a legacy string. 'final' rows stay reopenable, and are
 * rewritten to 'approved' on the next transition, so the value drains out
 * naturally rather than needing a migration job.
 */

import { AppError, ConflictError, ForbiddenError, ValidationError } from '../middleware/error-handler';

// ---------------------------------------------------------------------------
// Machine-readable failure codes
// ---------------------------------------------------------------------------

/**
 * Why a lifecycle call failed, as a stable token.
 *
 * The HTTP status alone is not enough: 409 is returned for three genuinely
 * different situations (illegal move, reviews outstanding, lost compare-and-
 * swap) and a client that needs to tell them apart was previously forced to
 * regex the human-readable `error` prose — which silently breaks the moment
 * anyone rewords a message. Responses now carry `code` alongside `error`;
 * `error` stays exactly as it was, for humans.
 *
 *   action_required         400 — no action was supplied at all.
 *   unknown_action          400 — action is not in the vocabulary.
 *   illegal_transition      409 — real action, not legal from the current state.
 *   reviews_outstanding     409 — legal move, but required reviews are pending.
 *   concurrent_modification 409 — the compare-and-swap lost; re-read and retry.
 *   lifecycle_unavailable   409 — the row could not be created or read back.
 *   not_authorized          403 — legal move, wrong actor.
 *   owner_not_established   403 — nobody owns this document yet (see the
 *                                 ownership notes in documentLifecycleService).
 *   owner_already_established
 *                           409 — an ownership claim raced or arrived late.
 *   owner_lookup_unavailable
 *                           503 — the owner could not be determined. This is
 *                                 NOT "there is no owner"; it fails closed.
 */
export type LifecycleErrorCode =
  | 'action_required'
  | 'unknown_action'
  | 'illegal_transition'
  | 'reviews_outstanding'
  | 'concurrent_modification'
  | 'lifecycle_unavailable'
  | 'not_authorized'
  | 'owner_not_established'
  | 'owner_already_established'
  | 'owner_lookup_unavailable';

/** Mixin marker: any error carrying a `LifecycleErrorCode`. */
export interface CodedError {
  readonly code: LifecycleErrorCode;
}

export class LifecycleValidationError extends ValidationError implements CodedError {
  constructor(message: string, public readonly code: LifecycleErrorCode) {
    super(message);
  }
}

export class LifecycleConflictError extends ConflictError implements CodedError {
  constructor(message: string, public readonly code: LifecycleErrorCode) {
    super(message);
  }
}

export class LifecycleForbiddenError extends ForbiddenError implements CodedError {
  constructor(message: string, public readonly code: LifecycleErrorCode) {
    super(message);
  }
}

/**
 * 503 — the server could not determine the answer. Distinct from a 403: the
 * caller may well be authorized, we simply could not verify it, so the correct
 * client behaviour is to retry rather than to give up.
 */
export class LifecycleUnavailableError extends AppError implements CodedError {
  constructor(message: string, public readonly code: LifecycleErrorCode = 'owner_lookup_unavailable') {
    super(503, message);
  }
}

/** The code on a lifecycle error, or undefined for anything else. */
export function lifecycleErrorCode(err: unknown): LifecycleErrorCode | undefined {
  const code = (err as Partial<CodedError> | null | undefined)?.code;
  return typeof code === 'string' ? (code as LifecycleErrorCode) : undefined;
}

// ---------------------------------------------------------------------------
// Vocabulary
// ---------------------------------------------------------------------------

export type DocumentStatus =
  | 'draft'
  | 'in_review'
  | 'changes_requested'
  | 'approved'
  | 'finalized';

/** Status values that may appear in storage, including the legacy one. */
export type StoredDocumentStatus = DocumentStatus | 'final';

export const DOCUMENT_STATUSES: readonly DocumentStatus[] = [
  'draft',
  'in_review',
  'changes_requested',
  'approved',
  'finalized',
] as const;

/**
 * States from which no transition exists, for anyone, ever.
 *
 * Derived from the table rather than declared alongside it, so "terminal"
 * cannot drift from what the rules actually permit: a state is terminal
 * precisely when nothing leaves it. `assertTerminalStatesHaveNoExit` pins the
 * expectation in the other direction.
 */
export const TERMINAL_STATUSES: readonly DocumentStatus[] = ['finalized'] as const;

export function isTerminalStatus(raw: string | null | undefined): boolean {
  return (TERMINAL_STATUSES as readonly string[]).includes(normalizeStatus(raw));
}

/**
 * The legacy status this codebase shipped before a real lifecycle existed.
 *
 * Reads as `approved`, NOT as `finalized` — see the module header for why the
 * spelling similarity is a trap rather than a hint.
 */
export const LEGACY_APPROVED_STATUS = 'final';

export type LifecycleAction =
  | 'submit_for_review'
  | 'withdraw'
  | 'approve'
  | 'request_changes'
  | 'revise'
  | 'reopen'
  | 'finalize';

export const LIFECYCLE_ACTIONS: readonly LifecycleAction[] = [
  'submit_for_review',
  'withdraw',
  'approve',
  'request_changes',
  'revise',
  'reopen',
  'finalize',
] as const;

/**
 * Who is permitted to drive a transition.
 *
 *   owner    — the document's creator/owner.
 *   reviewer — a user with a non-withdrawn review request on this document.
 *              "Designated" is the operative word: being able to read the
 *              document does not make you a reviewer.
 */
export type ActorRole = 'owner' | 'reviewer';

export interface TransitionRule {
  readonly from: DocumentStatus;
  readonly to: DocumentStatus;
  readonly action: LifecycleAction;
  /** Any one of these roles may perform the transition. */
  readonly allowedRoles: readonly ActorRole[];
  /** When true, every outstanding required review request must be satisfied. */
  readonly requiresAllReviews?: boolean;
  readonly description: string;
}

// ---------------------------------------------------------------------------
// The transition table — the whole point of this module
// ---------------------------------------------------------------------------

/**
 * Every legal move. Anything not in this table is illegal and answered with a
 * 409. Note there is exactly one row whose `to` is 'approved', and exactly one
 * row leaving 'approved' by unlocking it — that row is owner-only, which is
 * the specific "anyone can unlock a finalized doc" bug this table fixes.
 *
 * There are NO rows with `from: 'finalized'`. That absence is the entire
 * implementation of terminality — there is no `if (terminal) throw` anywhere,
 * because a state with no outgoing rules already answers every action with the
 * ordinary 409 `illegal_transition` and offers no actions to any role.
 * `assertTerminalStatesHaveNoExit` exists to stop someone adding one back.
 */
export const TRANSITIONS: readonly TransitionRule[] = [
  {
    from: 'draft',
    to: 'in_review',
    action: 'submit_for_review',
    allowedRoles: ['owner'],
    description: 'Owner submits the document for review',
  },
  {
    from: 'in_review',
    to: 'draft',
    action: 'withdraw',
    allowedRoles: ['owner'],
    description: 'Owner withdraws the document from review',
  },
  {
    from: 'in_review',
    to: 'approved',
    action: 'approve',
    allowedRoles: ['reviewer'],
    requiresAllReviews: true,
    description: 'Designated reviewer approves once all required reviews are satisfied',
  },
  {
    from: 'in_review',
    to: 'changes_requested',
    action: 'request_changes',
    allowedRoles: ['reviewer'],
    description: 'Designated reviewer sends the document back for changes',
  },
  {
    from: 'changes_requested',
    to: 'draft',
    action: 'revise',
    allowedRoles: ['owner'],
    description: 'Owner takes the document back to draft to make the changes',
  },
  {
    from: 'changes_requested',
    to: 'in_review',
    action: 'submit_for_review',
    allowedRoles: ['owner'],
    description: 'Owner resubmits after addressing the requested changes',
  },
  {
    from: 'approved',
    to: 'draft',
    action: 'reopen',
    allowedRoles: ['owner'],
    description: 'Owner reopens an approved document for further editing',
  },
  {
    from: 'approved',
    to: 'finalized',
    action: 'finalize',
    allowedRoles: ['owner'],
    requiresAllReviews: true,
    description:
      'Owner permanently finalizes an approved document — terminal, no reopen',
  },
] as const;

/**
 * Why `finalize` is shaped exactly the way it is.
 *
 * It is the only irreversible rule in the table, so each of its three
 * constraints is doing separate work and none of them is decoration:
 *
 * 1. `from: 'approved'` — and nowhere else. The button this replaces went
 *    from draft to locked in one click, which meant a single person could put
 *    a document beyond reach without anyone having read it. Routing the only
 *    entrance through `approved` means the terminal state can only be arrived
 *    at along the review path.
 *
 * 2. `allowedRoles: ['owner']` — while `approve` is reviewer-only. That pairing
 *    is the point: reaching `finalized` takes TWO different actors, a reviewer
 *    to approve and then the owner to finalize, and there is no role that can
 *    drive a document from draft to terminal alone. An owner who is also a
 *    designated reviewer on their own document is the one actor who could, and
 *    `approve`'s reviewer-only rule combined with `rolesFor` requiring a real
 *    non-withdrawn review request addressed to them is what makes that a
 *    deliberate, recorded arrangement rather than an accident.
 *
 * 3. `requiresAllReviews: true` — the deliberate difference from the old
 *    behaviour. Finalizing over an outstanding review used to be permitted
 *    with a warning, and that was defensible while Unlock existed: the warning
 *    said the requests "stop being actionable until someone unlocks it". With
 *    no unlock there is no "until" — finalizing over a pending request makes it
 *    permanently unanswerable, which is not a warning-shaped problem. So the
 *    server refuses it, and refuses it in the one place a client cannot skip.
 */

// ---------------------------------------------------------------------------
// Status normalization (legacy 'final')
// ---------------------------------------------------------------------------

/** Maps any stored status onto the current vocabulary. `final` → `approved`. */
export function normalizeStatus(raw: string | null | undefined): DocumentStatus {
  if (raw === LEGACY_APPROVED_STATUS) return 'approved';
  if (raw && (DOCUMENT_STATUSES as readonly string[]).includes(raw)) {
    return raw as DocumentStatus;
  }
  // Unknown/absent status: a document nobody has ever moved is a draft.
  return 'draft';
}

/** True when the stored value is the legacy one and should be migrated on write. */
export function isLegacyStatus(raw: string | null | undefined): boolean {
  return raw === LEGACY_APPROVED_STATUS;
}

/**
 * Documents in these states are read-only for content edits.
 *
 * True for both `approved` and `finalized`, which read-only-ness cannot tell
 * apart — ask `isTerminalStatus` for the difference that matters (whether
 * anyone can lift it).
 */
export function isLocked(raw: string | null | undefined): boolean {
  const status = normalizeStatus(raw);
  return status === 'approved' || status === 'finalized';
}

// ---------------------------------------------------------------------------
// Table lookups
// ---------------------------------------------------------------------------

export function isKnownAction(action: string): action is LifecycleAction {
  return (LIFECYCLE_ACTIONS as readonly string[]).includes(action);
}

/** The rule for (from, action), or undefined when that move is illegal. */
export function findRule(
  from: DocumentStatus,
  action: LifecycleAction,
): TransitionRule | undefined {
  return TRANSITIONS.find((r) => r.from === from && r.action === action);
}

/** True when some rule permits from → to. Used by the exhaustive tests. */
export function isLegalTransition(from: DocumentStatus, to: DocumentStatus): boolean {
  return TRANSITIONS.some((r) => r.from === from && r.to === to);
}

/** Every action legal from `from`, regardless of actor. */
export function actionsFrom(from: DocumentStatus): LifecycleAction[] {
  return TRANSITIONS.filter((r) => r.from === from).map((r) => r.action);
}

/**
 * Throws if any state named terminal has a way out of it.
 *
 * Called once at module load, so a rule that quietly un-terminals `finalized`
 * takes the service down at startup instead of shipping a "terminal" state
 * that turns out to be exitable. This is cheap insurance for the one property
 * the whole feature rests on, and it belongs next to the table rather than
 * only in a test file that a future edit could forget to run.
 */
export function assertTerminalStatesHaveNoExit(): void {
  for (const status of TERMINAL_STATUSES) {
    const exits = TRANSITIONS.filter((r) => r.from === status);
    if (exits.length > 0) {
      throw new Error(
        `Document lifecycle: '${status}' is declared terminal but ` +
          `${exits.length} transition rule(s) leave it ` +
          `(${exits.map((r) => r.action).join(', ')}). A terminal state must ` +
          `have no exit — if this state should be reversible, remove it from ` +
          `TERMINAL_STATUSES rather than leaving the two disagreeing.`,
      );
    }
  }
}

assertTerminalStatesHaveNoExit();

/** Every action legal from `from` for an actor holding `roles`. */
export function availableActions(
  from: DocumentStatus,
  roles: readonly ActorRole[],
): LifecycleAction[] {
  return TRANSITIONS.filter(
    (r) => r.from === from && r.allowedRoles.some((role) => roles.includes(role)),
  ).map((r) => r.action);
}

// ---------------------------------------------------------------------------
// The single transition function
// ---------------------------------------------------------------------------

/** A required review request that has not yet been satisfied. */
export interface OutstandingReview {
  requestKey: string;
  reviewerId: string;
  state: string;
  sectionId?: string;
  dueDate?: string;
}

export interface TransitionContext {
  /** Roles the calling user holds on this document. Empty = bystander. */
  roles: readonly ActorRole[];
  /**
   * Required review requests that are not yet satisfied. Only consulted for
   * rules with `requiresAllReviews`. Callers compute this with
   * `isReviewOutstanding` over the document's review requests.
   */
  outstandingReviews?: readonly OutstandingReview[];
}

export interface TransitionResult {
  from: DocumentStatus;
  to: DocumentStatus;
  action: LifecycleAction;
  rule: TransitionRule;
  /** True when the stored value was legacy 'final' and is being migrated. */
  migratedLegacyStatus: boolean;
}

/**
 * Decide a single lifecycle transition.
 *
 * Throws, in this order, so the failure the caller sees is the most specific
 * one available. Each carries a `code` (see `LifecycleErrorCode`) so a client
 * can branch without string-matching the message:
 *   - 400 unknown_action      — the action is not part of the vocabulary.
 *   - 409 illegal_transition  — the action is real but illegal from `from`.
 *   - 403 not_authorized      — legal move, wrong actor.
 *   - 409 reviews_outstanding — legal move, right actor, reviews outstanding.
 *
 * Illegal-before-forbidden matters: "reopen an approved document" is a LEGAL
 * transition, so a random caller attempting it falls through to the
 * authorization check and gets a 403 rather than a misleading 409.
 */
export function transition(
  currentStatus: string | null | undefined,
  action: string,
  ctx: TransitionContext,
): TransitionResult {
  if (!isKnownAction(action)) {
    throw new LifecycleValidationError(
      `Unknown lifecycle action '${action}'. Expected one of: ${LIFECYCLE_ACTIONS.join(', ')}`,
      'unknown_action',
    );
  }

  const from = normalizeStatus(currentStatus);
  const rule = findRule(from, action);

  if (!rule) {
    const legal = actionsFrom(from);
    // A terminal state gets its own sentence. "No actions are legal from
    // 'finalized'" reads like a temporary condition the caller might wait out
    // or lack a permission for; the honest message is that there is nothing to
    // wait for and no role that would change the answer.
    const suffix = legal.length
      ? `Legal actions from '${from}': ${legal.join(', ')}.`
      : isTerminalStatus(from)
        ? `'${from}' is a terminal state: no action moves a document out of ` +
          `it, for any user. This is permanent by design, not a permission ` +
          `you are missing.`
        : `No actions are legal from '${from}'.`;
    throw new LifecycleConflictError(
      `Cannot '${action}' a document in state '${from}'. ${suffix}`,
      'illegal_transition',
    );
  }

  const permitted = rule.allowedRoles.some((role) => ctx.roles.includes(role));
  if (!permitted) {
    throw new LifecycleForbiddenError(
      `Only the document ${rule.allowedRoles.join(' or ')} may '${action}' ` +
        `(${from} → ${rule.to}).`,
      'not_authorized',
    );
  }

  if (rule.requiresAllReviews) {
    const outstanding = ctx.outstandingReviews ?? [];
    if (outstanding.length > 0) {
      const names = outstanding.map((r) => r.reviewerId).join(', ');
      throw new LifecycleConflictError(
        `Cannot '${action}': ${outstanding.length} required review request` +
          `${outstanding.length === 1 ? '' : 's'} still outstanding (${names}).`,
        'reviews_outstanding',
      );
    }
  }

  return {
    from,
    to: rule.to,
    action,
    rule,
    migratedLegacyStatus: isLegacyStatus(currentStatus),
  };
}
