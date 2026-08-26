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

import { ConflictError, ForbiddenError, ValidationError } from '../middleware/error-handler';

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
 * one available:
 *   - ValidationError (400) — the action is not part of the vocabulary.
 *   - ConflictError   (409) — the action is real but illegal from `from`.
 *   - ForbiddenError  (403) — legal move, wrong actor.
 *   - ConflictError   (409) — legal move, right actor, reviews outstanding.
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
    throw new ValidationError(
      `Unknown lifecycle action '${action}'. Expected one of: ${LIFECYCLE_ACTIONS.join(', ')}`,
    );
  }

  const from = normalizeStatus(currentStatus);
  const rule = findRule(from, action);

  if (!rule) {
    const legal = actionsFrom(from);
    const suffix = legal.length
      ? `Legal actions from '${from}': ${legal.join(', ')}.`
      : `No actions are legal from '${from}'.`;
    throw new ConflictError(
      `Cannot '${action}' a document in state '${from}'. ${suffix}`,
    );
  }

  const permitted = rule.allowedRoles.some((role) => ctx.roles.includes(role));
  if (!permitted) {
    throw new ForbiddenError(
      `Only the document ${rule.allowedRoles.join(' or ')} may '${action}' ` +
        `(${from} → ${rule.to}).`,
    );
  }

  if (rule.requiresAllReviews) {
    const outstanding = ctx.outstandingReviews ?? [];
    if (outstanding.length > 0) {
      const names = outstanding.map((r) => r.reviewerId).join(', ');
      throw new ConflictError(
        `Cannot '${action}': ${outstanding.length} required review request` +
          `${outstanding.length === 1 ? '' : 's'} still outstanding (${names}).`,
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
