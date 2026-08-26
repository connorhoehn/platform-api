/**
 * Named review requests — "Alice, please review section 3 by Friday".
 *
 *   POST   /api/documents/:documentId/review-requests
 *   GET    /api/documents/:documentId/review-requests
 *   PATCH  /api/documents/:documentId/review-requests/:requestKey
 *   POST   /api/documents/:documentId/sections/:sectionId/review-requests
 *   GET    /api/documents/:documentId/sections/:sectionId/review-requests
 *
 * A request is what makes someone a designated reviewer, so creating one is
 * owner-only — otherwise anybody could appoint themselves and approve the
 * document.
 *
 * Responding to a request (PATCH → approved / changes_requested) records the
 * reviewer's verdict but does NOT move the document by itself; the document
 * transition is a separate, explicit call to
 * `POST /api/documents/:id/lifecycle/transitions`. The PATCH response carries
 * the current lifecycle view and the caller's `availableActions` so the
 * frontend can chain the two without a second round trip to find out.
 */

import { Router, type Request, type Response } from 'express';
import { requireAuth } from '../middleware/auth';
import { documentReviewRequestRepo, profileRepo } from '../repositories';
import {
  REVIEW_REQUEST_STATES,
  buildRequestKey,
  type ReviewRequestState,
} from '../repositories/DocumentReviewRequestRepository';
import {
  loadContext,
  rolesFor,
  toView,
  availableActionsFor,
} from '../services/documentLifecycleService';
import { publishSignal } from '../lib/event-catalog';
import {
  asyncHandler,
  ValidationError,
  ForbiddenError,
  NotFoundError,
} from '../middleware/error-handler';
import log from '../lib/logger';

export const documentReviewRequestsRouter = Router({ mergeParams: true });
export const sectionReviewRequestsRouter = Router({ mergeParams: true });

function emit(
  documentId: string,
  event: 'doc:review_requested' | 'doc:review_request_updated',
  payload: Record<string, unknown>,
): void {
  publishSignal(`doc:${documentId}`, event, payload).catch((err: unknown) => {
    log.error(
      { event, err: err instanceof Error ? err.message : String(err) },
      'signal publish failed',
    );
  });
}

async function displayNameFor(userId: string): Promise<string> {
  const profile = await profileRepo.getProfile(userId);
  return profile?.displayName ?? userId;
}

function parseDueDate(raw: unknown): string | undefined {
  if (raw === undefined || raw === null || raw === '') return undefined;
  if (typeof raw !== 'string' || !Number.isFinite(Date.parse(raw))) {
    throw new ValidationError('dueDate must be an ISO-8601 date string');
  }
  return new Date(raw).toISOString();
}

/** Shared create handler; sectionId comes from the path or the body. */
async function createRequest(
  req: Request,
  res: Response,
  sectionIdFromPath?: string,
): Promise<void> {
  const { documentId } = req.params;
  const userId = req.user!.sub;
  const body = (req.body ?? {}) as {
    reviewerId?: string;
    sectionId?: string;
    dueDate?: string;
    note?: string;
    required?: boolean;
  };

  const reviewerId = body.reviewerId;
  if (!reviewerId) {
    throw new ValidationError('reviewerId is required');
  }

  const ctx = await loadContext(documentId);
  if (!rolesFor(ctx, userId).includes('owner')) {
    throw new ForbiddenError(
      'Only the document owner may request reviews',
    );
  }

  const sectionId = sectionIdFromPath ?? body.sectionId;
  const dueDate = parseDueDate(body.dueDate);

  const request = await documentReviewRequestRepo.createRequest({
    documentId,
    reviewerId,
    reviewerName: await displayNameFor(reviewerId),
    requestedBy: userId,
    requestedByName: await displayNameFor(userId),
    ...(sectionId !== undefined ? { sectionId } : {}),
    ...(dueDate !== undefined ? { dueDate } : {}),
    ...(body.note !== undefined ? { note: body.note } : {}),
    ...(body.required !== undefined ? { required: body.required } : {}),
  });

  emit(documentId, 'doc:review_requested', {
    documentId,
    ...(sectionId !== undefined ? { sectionId } : {}),
    request: request as unknown as Record<string, unknown>,
  });

  res.status(201).json({ request });
}

// POST /api/documents/:documentId/review-requests
documentReviewRequestsRouter.post(
  '/',
  requireAuth,
  asyncHandler(async (req: Request, res: Response) => createRequest(req, res)),
);

// GET /api/documents/:documentId/review-requests[?state=pending]
documentReviewRequestsRouter.get(
  '/',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const { documentId } = req.params;
    const state = req.query['state'] as string | undefined;
    const all = await documentReviewRequestRepo.getRequestsForDocument(documentId);
    const requests = state ? all.filter((r) => r.state === state) : all;
    res.status(200).json({ requests });
  }),
);

// PATCH /api/documents/:documentId/review-requests/:requestKey
//
// Auth depends on the change:
//   state approved | changes_requested → the addressed reviewer only
//   state withdrawn | pending          → the requester or the document owner
//   dueDate                            → the requester or the document owner
documentReviewRequestsRouter.patch(
  '/:requestKey',
  requireAuth,
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const { documentId, requestKey } = req.params;
    const userId = req.user!.sub;
    const body = (req.body ?? {}) as {
      state?: string;
      comment?: string;
      dueDate?: string | null;
    };

    const existing = await documentReviewRequestRepo.getRequest(documentId, requestKey);
    if (!existing) {
      throw new NotFoundError('Review request not found');
    }

    const ctx = await loadContext(documentId);
    const isOwner = rolesFor(ctx, userId).includes('owner');
    const isRequester = existing.requestedBy === userId;
    const isAddressedReviewer = existing.reviewerId === userId;
    const canManage = isOwner || isRequester;

    let updated = existing;
    let changed = false;

    if (body.state !== undefined) {
      if (!(REVIEW_REQUEST_STATES as readonly string[]).includes(body.state)) {
        throw new ValidationError(
          `state must be one of: ${REVIEW_REQUEST_STATES.join(', ')}`,
        );
      }
      const state = body.state as ReviewRequestState;

      if (state === 'approved' || state === 'changes_requested') {
        if (!isAddressedReviewer) {
          throw new ForbiddenError(
            'Only the reviewer this request is addressed to may respond to it',
          );
        }
      } else if (!canManage) {
        throw new ForbiddenError(
          'Only the requester or the document owner may withdraw or reopen a review request',
        );
      }

      const result = await documentReviewRequestRepo.setState({
        documentId,
        requestKey,
        state,
        actor: userId,
        ...(body.comment !== undefined ? { comment: body.comment } : {}),
      });
      if (!result) throw new NotFoundError('Review request not found');
      updated = result;
      changed = true;
    }

    if (body.dueDate !== undefined) {
      if (!canManage) {
        throw new ForbiddenError(
          'Only the requester or the document owner may change a review due date',
        );
      }
      const dueDate = body.dueDate === null ? null : (parseDueDate(body.dueDate) ?? null);
      const result = await documentReviewRequestRepo.setDueDate(
        documentId,
        requestKey,
        dueDate,
      );
      if (!result) throw new NotFoundError('Review request not found');
      updated = result;
      changed = true;
    }

    if (!changed) {
      throw new ValidationError('No fields to update (expected state and/or dueDate)');
    }

    emit(documentId, 'doc:review_request_updated', {
      documentId,
      ...(updated.sectionId !== undefined ? { sectionId: updated.sectionId } : {}),
      requestKey,
      state: updated.state,
      request: updated as unknown as Record<string, unknown>,
    });

    // Re-read so `availableActions` reflects the request we just changed —
    // approving the last outstanding request unblocks the document `approve`.
    const after = await loadContext(documentId);
    res.status(200).json({
      request: updated,
      lifecycle: toView(after),
      availableActions: availableActionsFor(after, userId),
      outstandingReviews: after.outstanding,
    });
  }),
);

// POST /api/documents/:documentId/sections/:sectionId/review-requests
sectionReviewRequestsRouter.post(
  '/',
  requireAuth,
  asyncHandler(async (req: Request, res: Response) =>
    createRequest(req, res, req.params['sectionId']),
  ),
);

// GET /api/documents/:documentId/sections/:sectionId/review-requests
sectionReviewRequestsRouter.get(
  '/',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const { documentId, sectionId } = req.params;
    const requests = await documentReviewRequestRepo.getRequestsForSection(
      documentId,
      sectionId,
    );
    res.status(200).json({ requests });
  }),
);

export { buildRequestKey };
