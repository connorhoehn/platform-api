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
 *   approved           — signed off. Read-only until the owner reopens it.
 *
 * Legacy: existing rows carry status 'final'. It is treated as equivalent to
 * 'approved' on read (see `normalizeStatus`) and rewritten to 'approved' the
 * next time the document transitions, so the legacy value drains out of the
 * table naturally rather than needing a migration job.
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
  | 'approved';

/** Status values that may appear in storage, including the legacy one. */
export type StoredDocumentStatus = DocumentStatus | 'final';

export const DOCUMENT_STATUSES: readonly DocumentStatus[] = [
  'draft',
  'in_review',
  'changes_requested',
  'approved',
] as const;

/** The legacy status this codebase shipped before a real lifecycle existed. */
export const LEGACY_APPROVED_STATUS = 'final';

export type LifecycleAction =
  | 'submit_for_review'
  | 'withdraw'
  | 'approve'
  | 'request_changes'
  | 'revise'
  | 'reopen';

export const LIFECYCLE_ACTIONS: readonly LifecycleAction[] = [
  'submit_for_review',
  'withdraw',
  'approve',
  'request_changes',
  'revise',
  'reopen',
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
 * row leaving 'approved' — that row is owner-only, which is the specific
 * "anyone can unlock a finalized doc" bug this table fixes.
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
] as const;

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

/** Documents in these states are read-only for content edits. */
export function isLocked(raw: string | null | undefined): boolean {
  return normalizeStatus(raw) === 'approved';
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
    const suffix = legal.length
      ? `Legal actions from '${from}': ${legal.join(', ')}.`
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
