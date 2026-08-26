/**
 * Document lifecycle service — ties the pure transition table
 * (`src/lib/document-lifecycle.ts`) to storage, authorization and signals.
 *
 * Routes stay thin: they parse the request and call `performTransition`.
 *
 * Ownership resolution, in order:
 *   1. The lifecycle row's recorded `ownerId` — authoritative once set.
 *   2. The typed-document's `createdBy`, for documents that predate this
 *      table but do have a server-side creator.
 *   3. Trust-on-first-use: a document with neither has no server-side owner
 *      at all (its status only ever lived in CRDT metadata), so the first
 *      caller to drive a transition claims ownership and it is recorded
 *      permanently. Every subsequent caller is checked against it.
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
  LEGACY_APPROVED_STATUS,
  type ActorRole,
  type DocumentStatus,
  type LifecycleAction,
  type OutstandingReview,
} from '../lib/document-lifecycle';
import { publishSignal } from '../lib/event-catalog';
import { ConflictError } from '../middleware/error-handler';
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

export interface LifecycleContext {
  documentId: string;
  record: DocumentLifecycleItem | null;
  status: DocumentStatus;
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

  let ownerId = record?.ownerId || null;
  if (!ownerId) {
    ownerId = await lookupCreator(documentId);
  }

  return {
    documentId,
    record,
    status: record ? record.status : 'draft',
    ownerId,
    requests,
    outstanding: requests.filter(isReviewOutstanding).map(toOutstanding),
  };
}

async function lookupCreator(documentId: string): Promise<string | null> {
  try {
    const doc = await typedDocumentRepo.get(documentId);
    return doc?.createdBy ?? null;
  } catch (err) {
    // A document that isn't a typed document (or a table that doesn't exist
    // in this environment) simply has no creator to borrow. Not fatal.
    log.debug(
      { documentId, err: err instanceof Error ? err.message : String(err) },
      'typed-document creator lookup failed; falling back to TOFU ownership',
    );
    return null;
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

/** Roles `userId` holds on this document given the loaded context. */
export function rolesFor(ctx: LifecycleContext, userId: string): ActorRole[] {
  const roles: ActorRole[] = [];
  // No recorded owner anywhere → trust-on-first-use (see module header).
  if (ctx.ownerId === null || ctx.ownerId === userId) roles.push('owner');
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
    ownerId: record?.ownerId ?? ctx.ownerId,
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

/** Actions `userId` may take from the current state. Drives FE button state. */
export function availableActionsFor(
  ctx: LifecycleContext,
  userId: string | undefined,
): LifecycleAction[] {
  if (!userId) return [];
  return availableActions(ctx.status, rolesFor(ctx, userId));
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

  // Throws ValidationError(400) / ConflictError(409) / ForbiddenError(403).
  const decision = transition(storedStatus, params.action, {
    roles,
    outstandingReviews: ctx.outstanding,
  });

  const at = new Date().toISOString();

  // Materialize the row for a document that never had one. The owner recorded
  // here is the TOFU claim described in the module header.
  let record = ctx.record;
  if (!record) {
    const owner = ctx.ownerId ?? params.userId;
    record =
      (await documentLifecycleRepo.initialize(params.documentId, owner, decision.from)) ??
      (await documentLifecycleRepo.get(params.documentId));
    if (!record) {
      throw new ConflictError('Document lifecycle row could not be created');
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
      throw new ConflictError(
        `${err.message}. Re-read the document lifecycle and retry.`,
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
