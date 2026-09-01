/**
 * Document lifecycle service — ties the pure transition table
 * (`src/lib/document-lifecycle.ts`) to storage, authorization and signals.
 *
 * Routes stay thin: they parse the request and call `performTransition`.
 *
 * ---------------------------------------------------------------------------
 * Ownership
 * ---------------------------------------------------------------------------
 * Ownership is a THREE-valued question, not a nullable string. Collapsing it
 * to `string | null` is what produced the vulnerability this module was
 * rewritten to close: `ownerId === null` was read as "unowned, so you may act
 * as owner", and a failed creator lookup also produced `null` — so a transient
 * repository error silently WIDENED permissions. `rolesFor` is consumed by
 * `documentReviewRequests.ts` too, so that hole also let any authenticated
 * caller appoint reviewers on any document.
 *
 * `OwnerResolution` therefore distinguishes:
 *   resolved    — we know who owns it. The only state that can grant 'owner'.
 *   absent      — we looked, the read SUCCEEDED, and there is no owner on
 *                 record anywhere. Grants nothing; the document is claimable.
 *   unavailable — we could not determine the owner. Grants nothing and is
 *                 reported as 503, NOT 403: "I cannot tell" is not "no".
 *
 * Resolution order:
 *   1. The lifecycle row's recorded `ownerId` — authoritative once set.
 *   2. A server-recorded creator on the document record (see `resolveCreator`).
 *   3. Otherwise `absent` — nobody holds the owner role, and the only way to
 *      establish one is `claimOwnership`, an explicit, logged, single-shot
 *      call. There is deliberately no implicit claim on the transition path:
 *      merely touching a document must never make you its owner.
 *
 * Why step 2 catches so little, and why nothing else was promoted into it
 * ----------------------------------------------------------------------
 * These endpoints are keyed on the CRDT-document id space (the same
 * `:documentId` that `documentImportExport.ts` reads out of `crdt-documents`).
 * `typed-documents` is a DIFFERENT id space — `randomUUID()` values minted by
 * `POST /document-types/:typeId/documents` — so for ordinary editor documents
 * this lookup structurally misses. That, not merely "the API was never
 * called", is why every document in the app is ownerless today.
 *
 * `crdt-documents` DOES carry a `createdBy` in the right id space, and it is
 * deliberately NOT used as an ownership signal here. It is written by the
 * gateway from a verified JWT `sub` in the normal case and cannot be set from
 * the request body — but the field is guaranteed present, not valid: the
 * gateway substitutes the literal `'unknown'` or a bare per-connection UUID
 * when a socket has no user context, and it can be degraded to `'unknown'` by
 * a later metadata edit. Telling a genuine subject id apart from a connection
 * UUID means guessing at identity formats, and guessing wrong in the
 * permissive direction hands a document to a ghost while guessing wrong in
 * the strict direction locks its real owner out forever. There is no
 * dependable owner signal there, so this module does not invent one — it
 * requires a human to say so, once, on the record.
 *
 * Reviewer resolution: a user is a designated reviewer iff a non-withdrawn
 * review request on this document is addressed to them. Being able to open
 * the document does not make you a reviewer — that is precisely the hole
 * that let any caller unlock a finalized document.
 */

import {
  documentLifecycleRepo,
  documentReviewRequestRepo,
  typedDocumentRepo,
} from '../repositories';
import type {
  DocumentLifecycleItem,
} from '../repositories/DocumentLifecycleRepository';
import { LifecycleConcurrencyError } from '../repositories/DocumentLifecycleRepository';
import {
  isReviewOutstanding,
  type DocumentReviewRequest,
} from '../repositories/DocumentReviewRequestRepository';
import {
  transition,
  normalizeStatus,
  isLegacyStatus,
  availableActions,
  lifecycleErrorCode,
  LifecycleConflictError,
  LifecycleForbiddenError,
  LifecycleUnavailableError,
  LEGACY_APPROVED_STATUS,
  type ActorRole,
  type DocumentStatus,
  type LifecycleAction,
  type OutstandingReview,
} from '../lib/document-lifecycle';
import { publishSignal } from '../lib/event-catalog';
import log from '../lib/logger';

export interface DocumentLifecycleView {
  documentId: string;
  status: DocumentStatus;
  ownerId: string | null;
  updatedAt: string | null;
  updatedBy: string | null;
  version: number;
  lastAction?: LifecycleAction;
  lastReason?: string;
  /** Present when the row still carries the pre-lifecycle 'final' value. */
  legacyStatus?: string;
  history: DocumentLifecycleItem['history'];
  /** True until the document has a persisted lifecycle row. */
  implicit: boolean;
}

/**
 * The outcome of asking "who owns this document?".
 *
 * `absent` and `unavailable` are deliberately separate constructors. See the
 * module header: conflating them is the bug.
 */
export type OwnerResolution =
  | { state: 'resolved'; ownerId: string; source: 'lifecycle' | 'creator' }
  | { state: 'absent' }
  | { state: 'unavailable'; reason: string };

export interface LifecycleContext {
  documentId: string;
  record: DocumentLifecycleItem | null;
  status: DocumentStatus;
  owner: OwnerResolution;
  /**
   * The owner when one is established, else null — for display only.
   * NEVER branch on this for an authorization decision: it cannot tell
   * `absent` from `unavailable`. Use `ctx.owner.state`.
   */
  ownerId: string | null;
  requests: DocumentReviewRequest[];
  outstanding: OutstandingReview[];
}

/** Load everything an authorization decision needs, in two reads. */
export async function loadContext(documentId: string): Promise<LifecycleContext> {
  const [record, requests] = await Promise.all([
    documentLifecycleRepo.get(documentId),
    documentReviewRequestRepo.getRequestsForDocument(documentId),
  ]);

  const owner: OwnerResolution = record?.ownerId
    ? { state: 'resolved', ownerId: record.ownerId, source: 'lifecycle' }
    : await resolveCreator(documentId);

  return {
    documentId,
    record,
    status: record ? record.status : 'draft',
    owner,
    ownerId: owner.state === 'resolved' ? owner.ownerId : null,
    requests,
    outstanding: requests.filter(isReviewOutstanding).map(toOutstanding),
  };
}

/**
 * Borrow ownership from the document's server-recorded creator.
 *
 * The creator comes from `typed-documents.createdBy`, which is written from
 * `req.user!.sub` on an authenticated route (`routes/typedDocuments.ts:291`)
 * and is never accepted from the request body — so it is a server-side
 * identity, safe to promote to ownership.
 *
 * Two ways to have no creator, and they are NOT the same:
 *   - the read succeeded and returned no row / no `createdBy` → `absent`.
 *     This is the common case: the lifecycle endpoints are keyed on the
 *     CRDT-document id space, which is disjoint from the `randomUUID()` ids
 *     in `typed-documents`, so most documents genuinely have no creator here.
 *   - the read threw → `unavailable`. An error looking up the owner is not
 *     evidence that there is no owner. Previously this returned `null` and
 *     the caller was granted 'owner' — a repository blip widened permissions.
 */
async function resolveCreator(documentId: string): Promise<OwnerResolution> {
  try {
    const doc = await typedDocumentRepo.get(documentId);
    if (doc?.createdBy) {
      return { state: 'resolved', ownerId: doc.createdBy, source: 'creator' };
    }
    return { state: 'absent' };
  } catch (err) {
    const reason = err instanceof Error ? err.message : String(err);
    log.warn(
      { documentId, err: reason },
      'creator lookup failed; refusing to infer ownership (failing closed)',
    );
    return { state: 'unavailable', reason };
  }
}

function toOutstanding(req: DocumentReviewRequest): OutstandingReview {
  return {
    requestKey: req.requestKey,
    reviewerId: req.reviewerId,
    state: req.state,
    ...(req.sectionId !== undefined ? { sectionId: req.sectionId } : {}),
    ...(req.dueDate !== undefined ? { dueDate: req.dueDate } : {}),
  };
}

/**
 * Roles `userId` holds on this document given the loaded context.
 *
 * 'owner' is granted ONLY against a positively resolved owner id. An absent
 * owner grants nothing (use `claimOwnership` to establish one) and an
 * unresolvable owner grants nothing either — this function fails closed in
 * both directions and never infers ownership from the lack of a record.
 */
export function rolesFor(ctx: LifecycleContext, userId: string): ActorRole[] {
  const roles: ActorRole[] = [];
  if (ctx.owner.state === 'resolved' && ctx.owner.ownerId === userId) {
    roles.push('owner');
  }
  if (isDesignatedReviewer(ctx.requests, userId)) roles.push('reviewer');
  return roles;
}

export function isDesignatedReviewer(
  requests: readonly DocumentReviewRequest[],
  userId: string,
): boolean {
  return requests.some(
    (r) => r.reviewerId === userId && r.state !== 'withdrawn',
  );
}

/** Shape returned by GET /lifecycle and every transition response. */
export function toView(
  ctx: LifecycleContext,
  record: DocumentLifecycleItem | null = ctx.record,
): DocumentLifecycleView {
  const view: DocumentLifecycleView = {
    documentId: ctx.documentId,
    status: record ? record.status : 'draft',
    // `||`, not `??`: an empty-string ownerId on the row is no owner, and the
    // view must not disagree with what `rolesFor` decided.
    ownerId: record?.ownerId || ctx.ownerId,
    updatedAt: record?.updatedAt ?? null,
    updatedBy: record?.updatedBy ?? null,
    version: record?.version ?? 0,
    history: record?.history ?? [],
    implicit: record === null,
  };
  if (record?.lastAction !== undefined) view.lastAction = record.lastAction;
  if (record?.lastReason !== undefined) view.lastReason = record.lastReason;
  if (record?.legacyStatus !== undefined) view.legacyStatus = record.legacyStatus;
  return view;
}

/**
 * What an UNAUTHENTICATED caller may see.
 *
 * `GET /lifecycle` is `optionalAuth` so an anonymous reader can still tell
 * whether a document is a draft or approved. The full view is a different
 * matter: `history` is a per-document audit trail of actor ids and free-text
 * reasons ("needs a rollback plan"), `outstandingReviews` is a roster of who
 * has been asked to review what, and `ownerId`/`updatedBy` name individuals.
 * None of that is needed to render a status badge, and handing a directory of
 * user ids and internal review commentary to an unauthenticated caller is not
 * a trade worth making — so anonymous responses carry the state only.
 *
 * `lastAction` survives the cut: it is a fixed vocabulary word with no actor
 * or free text attached.
 */
export interface PublicDocumentLifecycleView {
  documentId: string;
  status: DocumentStatus;
  updatedAt: string | null;
  version: number;
  lastAction?: LifecycleAction;
  legacyStatus?: string;
  implicit: boolean;
}

export function toPublicView(
  ctx: LifecycleContext,
  record: DocumentLifecycleItem | null = ctx.record,
): PublicDocumentLifecycleView {
  const view: PublicDocumentLifecycleView = {
    documentId: ctx.documentId,
    status: record ? record.status : 'draft',
    updatedAt: record?.updatedAt ?? null,
    version: record?.version ?? 0,
    implicit: record === null,
  };
  if (record?.lastAction !== undefined) view.lastAction = record.lastAction;
  if (record?.legacyStatus !== undefined) view.legacyStatus = record.legacyStatus;
  return view;
}

/** Actions `userId` may take from the current state. Drives FE button state. */
export function availableActionsFor(
  ctx: LifecycleContext,
  userId: string | undefined,
): LifecycleAction[] {
  if (!userId) return [];
  return availableActions(ctx.status, rolesFor(ctx, userId));
}

/**
 * Turn a bare `not_authorized` into the specific reason when the denial was
 * really about not knowing the owner. Any other error passes through.
 */
function refineAuthorizationFailure(
  err: unknown,
  ctx: LifecycleContext,
  documentId: string,
): unknown {
  if (lifecycleErrorCode(err) !== 'not_authorized') return err;

  if (ctx.owner.state === 'unavailable') {
    // 503, not 403. The caller may well be the owner; we could not check.
    // Answering 403 here would report a server fault as a permission decision
    // and invite the client to give up on a retryable failure.
    log.warn(
      { documentId, reason: ctx.owner.reason },
      'denying lifecycle transition: owner could not be determined',
    );
    return new LifecycleUnavailableError(
      `Could not determine who owns document ${documentId}; the request was ` +
        `refused rather than guessed. Retry shortly.`,
    );
  }

  if (ctx.owner.state === 'absent') {
    return new LifecycleForbiddenError(
      `Document ${documentId} has no established owner, so no one holds the ` +
        `owner role on it. Claim ownership explicitly ` +
        `(POST .../lifecycle/ownership) before driving owner-only transitions.`,
      'owner_not_established',
    );
  }

  return err;
}

export interface ClaimOwnershipParams {
  documentId: string;
  userId: string;
  /** Pre-loaded context; re-loaded when omitted. */
  ctx?: LifecycleContext;
}

export interface ClaimOwnershipResult {
  lifecycle: DocumentLifecycleView;
  /** True when this call established ownership; false when it was already ours. */
  claimed: boolean;
}

/**
 * Establish an owner for a document that provably has none.
 *
 * This replaces trust-on-first-use. The difference is not who CAN end up
 * owning an ownerless document — any authenticated caller still can — but that
 * it now takes a deliberate, separate, single-shot request instead of
 * happening silently as a side effect of pressing a button, and it is recorded:
 * the resulting `ownerId` is persisted on the lifecycle row and surfaced by
 * `GET /lifecycle`, and the claim is written to the audit log at WARN.
 *
 * That matters concretely here. Every document in this app currently has no
 * lifecycle row, so under the old rule the FIRST person to open a document in
 * the frontend that is about to ship would have become its permanent owner
 * without anyone, including them, being aware of it. Requiring an explicit
 * claim means a read can never claim, and a wrong claim is visible rather than
 * invisible. The honest limitation: where a document carries no server-side
 * creator at all, there is no trustworthy signal to derive the true owner
 * from, and this does not invent one — it makes the guess explicit and
 * attributable instead of silent.
 *
 * Fails closed when the owner cannot be determined: an unreadable creator
 * lookup must not be mistaken for "unowned, yours for the taking".
 */
export async function claimOwnership(
  params: ClaimOwnershipParams,
): Promise<ClaimOwnershipResult> {
  const ctx = params.ctx ?? (await loadContext(params.documentId));

  if (ctx.owner.state === 'unavailable') {
    log.warn(
      { documentId: params.documentId, reason: ctx.owner.reason },
      'refusing ownership claim: owner could not be determined',
    );
    throw new LifecycleUnavailableError(
      `Could not determine whether document ${params.documentId} already has ` +
        `an owner; the claim was refused rather than granted. Retry shortly.`,
    );
  }

  if (ctx.owner.state === 'resolved') {
    if (ctx.owner.ownerId === params.userId) {
      return { lifecycle: toView(ctx), claimed: false };
    }
    throw new LifecycleConflictError(
      `Document ${params.documentId} is already owned by another user.`,
      'owner_already_established',
    );
  }

  const record = await documentLifecycleRepo.initialize(
    params.documentId,
    params.userId,
    ctx.status,
  );

  if (!record) {
    // Lost the create race — someone claimed it between our read and write.
    // Re-read rather than assume: the winner may legitimately be us.
    const after = await loadContext(params.documentId);
    if (after.owner.state === 'resolved' && after.owner.ownerId === params.userId) {
      return { lifecycle: toView(after), claimed: false };
    }
    throw new LifecycleConflictError(
      `Document ${params.documentId} was claimed by another user concurrently.`,
      'owner_already_established',
    );
  }

  // Audited at WARN, not DEBUG: this is a permission grant on a document with
  // no prior owner, and it is the one place ownership appears from nowhere.
  log.warn(
    {
      event: 'document_ownership_claimed',
      documentId: params.documentId,
      ownerId: params.userId,
      status: ctx.status,
    },
    'document ownership claimed by first explicit claimant',
  );

  return { lifecycle: toView(ctx, record), claimed: true };
}

export interface PerformTransitionParams {
  documentId: string;
  action: string;
  userId: string;
  reason?: string;
  /** Pre-loaded context; re-loaded when omitted. */
  ctx?: LifecycleContext;
}

export interface PerformTransitionResult {
  lifecycle: DocumentLifecycleView;
  from: DocumentStatus;
  to: DocumentStatus;
  action: LifecycleAction;
}

/**
 * The one way a document changes lifecycle state.
 *
 * Decides with the pure transition table, persists with a compare-and-swap on
 * the current status, then broadcasts `doc:lifecycle_changed` so open editors
 * react live.
 */
export async function performTransition(
  params: PerformTransitionParams,
): Promise<PerformTransitionResult> {
  const ctx = params.ctx ?? (await loadContext(params.documentId));
  const roles = rolesFor(ctx, params.userId);

  // Decide against the RAW stored value, not the hydrated one: the repository
  // normalizes 'final' → 'approved' on read, and handing the normalized value
  // to `transition` would report migratedLegacyStatus:false, so the write's
  // compare-and-swap would look for 'approved' and lose against the 'final'
  // still in storage.
  const storedStatus = ctx.record?.legacyStatus ?? ctx.record?.status ?? 'draft';

  // Throws 400 unknown_action / 409 illegal_transition / 403 not_authorized /
  // 409 reviews_outstanding, in that order.
  let decision;
  try {
    decision = transition(storedStatus, params.action, {
      roles,
      outstandingReviews: ctx.outstanding,
    });
  } catch (err) {
    // A generic "wrong actor" 403 is misleading when the real reason is that
    // the owner is unknown. Re-report those two cases honestly — and only
    // here, where we know the denial actually turned on the owner role.
    // A designated reviewer's transition never reaches this branch, so an
    // owner-lookup failure does not block review responses.
    throw refineAuthorizationFailure(err, ctx, params.documentId);
  }

  const at = new Date().toISOString();

  // Materialize the row for a document that never had one. `transition` has
  // already established that the caller holds a role, which for an unowned
  // document is impossible — so the owner recorded here is always the
  // previously-resolved one, never "whoever called first".
  let record = ctx.record;
  if (!record) {
    if (ctx.owner.state !== 'resolved') {
      // Unreachable via `transition` (no role is grantable without a resolved
      // owner), but asserted rather than defaulted: the old code defaulted to
      // `params.userId` here, which is exactly how a bystander became owner.
      throw new LifecycleForbiddenError(
        `Document ${params.documentId} has no established owner.`,
        'owner_not_established',
      );
    }
    record =
      (await documentLifecycleRepo.initialize(
        params.documentId,
        ctx.owner.ownerId,
        decision.from,
      )) ?? (await documentLifecycleRepo.get(params.documentId));
    if (!record) {
      throw new LifecycleConflictError(
        'Document lifecycle row could not be created',
        'lifecycle_unavailable',
      );
    }
    // Lost the create race against a concurrent transition — re-decide against
    // whatever actually landed rather than trusting our stale read.
    if (record.status !== decision.from) {
      return performTransition({ ...params, ctx: await loadContext(params.documentId) });
    }
  }

  let updated: DocumentLifecycleItem;
  try {
    updated = await documentLifecycleRepo.applyTransition({
      documentId: params.documentId,
      expectedFrom: decision.from,
      ...(decision.migratedLegacyStatus
        ? { legacyFrom: LEGACY_APPROVED_STATUS }
        : {}),
      to: decision.to,
      action: decision.action,
      actor: params.userId,
      at,
      ...(params.reason !== undefined ? { reason: params.reason } : {}),
      history: record.history,
    });
  } catch (err) {
    if (err instanceof LifecycleConcurrencyError) {
      throw new LifecycleConflictError(
        `${err.message}. Re-read the document lifecycle and retry.`,
        'concurrent_modification',
      );
    }
    throw err;
  }

  publishSignal(`doc:${params.documentId}`, 'doc:lifecycle_changed', {
    documentId: params.documentId,
    from: decision.from,
    to: decision.to,
    action: decision.action,
    actor: params.userId,
    at,
    version: updated.version,
    ...(params.reason !== undefined ? { reason: params.reason } : {}),
  }).catch((err: unknown) => {
    log.error(
      {
        event: 'doc:lifecycle_changed',
        err: err instanceof Error ? err.message : String(err),
      },
      'signal publish failed',
    );
  });

  return {
    lifecycle: toView(ctx, updated),
    from: decision.from,
    to: decision.to,
    action: decision.action,
  };
}

// Re-exported so route modules have one import site for the vocabulary.
export { normalizeStatus, isLegacyStatus };
