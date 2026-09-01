import { Router, Request, Response } from 'express';
import { requireAuth } from '../middleware/auth';
import { sectionReviewRepo, profileRepo, documentReviewRequestRepo } from '../repositories';
import { isOverdue } from '../repositories/DocumentReviewRequestRepository';
import { publishSignal } from '../lib/event-catalog';
import {
  asyncHandler,
  ValidationError,
} from '../middleware/error-handler';
import log from '../lib/logger';

export const sectionReviewsRouter = Router({ mergeParams: true });
export const myReviewsRouter = Router();

/**
 * A reviewer's verdict on ONE SECTION. Deliberately NOT the document-lifecycle
 * vocabulary in `../lib/document-lifecycle` (`draft`/`in_review`/`approved`/…)
 * nor the review-REQUEST states in `DocumentReviewRequestRepository`
 * (`pending`/`approved`/`changes_requested`/`withdrawn`): those describe where
 * a whole document or a named request has got to, this describes what one
 * person said about one section. `approved` and `changes_requested` happen to
 * spell the same in all three — they are still separate vocabularies, and a
 * status is only valid here if it is in THIS list.
 *
 * This is the write boundary. It is not a claim about what is already stored:
 * the reads below (and `SectionReview.status`, still a plain `string`) return
 * historical rows verbatim, including statuses this list would now reject.
 */
export const SECTION_REVIEW_STATUSES = [
  'reviewed',
  'approved',
  'changes_requested',
] as const;

export type SectionReviewStatus = (typeof SECTION_REVIEW_STATUSES)[number];

/** Same cap document comments use (`documentComments.ts`). */
const MAX_COMMENT_LENGTH = 10000;

/** Keep a rejected value out of the log/response at unbounded length. */
function describe(value: unknown): string {
  const raw = typeof value === 'string' ? value : JSON.stringify(value) ?? String(value);
  return raw.length > 80 ? `${raw.slice(0, 80)}…` : raw;
}

// POST /api/documents/:documentId/sections/:sectionId/reviews
sectionReviewsRouter.post('/', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { documentId, sectionId } = req.params;
  const userId = req.user!.sub;
  const { status, comment } = req.body as { status?: unknown; comment?: unknown };

  if (!status) {
    throw new ValidationError('status is required');
  }

  if (
    typeof status !== 'string'
    || !(SECTION_REVIEW_STATUSES as readonly string[]).includes(status)
  ) {
    throw new ValidationError(
      `status must be one of: ${SECTION_REVIEW_STATUSES.join(', ')} (got: ${describe(status)})`,
    );
  }

  let validComment: string | undefined;
  if (comment !== undefined) {
    if (typeof comment !== 'string') {
      throw new ValidationError('comment must be a string');
    }
    if (comment.length > MAX_COMMENT_LENGTH) {
      throw new ValidationError(
        `comment must be ${MAX_COMMENT_LENGTH} characters or fewer (got: ${comment.length})`,
      );
    }
    validComment = comment;
  }

  // Look up display name from profile
  let displayName = userId;
  const profile = await profileRepo.getProfile(userId);
  if (profile?.displayName) {
    displayName = profile.displayName;
  }

  const review = await sectionReviewRepo.submitReview({
    documentId,
    sectionId,
    userId,
    displayName,
    status,
    timestamp: new Date().toISOString(),
    ...(validComment !== undefined && { comment: validComment }),
  });

  publishSignal(`doc:${documentId}`, 'doc:section_updated', {
    type: 'section:review',
    documentId,
    sectionId,
    review,
  }).catch((err: unknown) => {
    log.error({ event: 'doc:section_updated', err: err instanceof Error ? err.message : String(err) }, 'signal publish failed');
  });

  res.status(201).json({ review });
}));

// GET /api/documents/:documentId/sections/:sectionId/reviews
sectionReviewsRouter.get('/', asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { documentId, sectionId } = req.params;
  const reviews = await sectionReviewRepo.getReviewsForSection(documentId, sectionId);
  res.status(200).json({ reviews });
}));

// Document-level reviews router (mounted separately)
export const documentReviewsRouter = Router({ mergeParams: true });

// GET /api/documents/:documentId/reviews
documentReviewsRouter.get('/', asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { documentId } = req.params;
  const reviews = await sectionReviewRepo.getReviewsForDocument(documentId);
  res.status(200).json({ reviews });
}));

// GET /api/reviews/mine[?state=pending][&overdue=true]
//
// The reviewer's inbox. Historically this returned only `reviews` — the
// section reviews the caller has already SUBMITTED — which is why it had no
// frontend consumers: it answered "what did I say" rather than "what is
// waiting on me". It now also returns `requests`: the named review requests
// ADDRESSED to the caller, with due dates.
//
// `reviews` keeps its exact previous shape and semantics so any existing
// consumer is unaffected; `requests` and `summary` are additive. `state` and
// `overdue` filter `requests` only.
myReviewsRouter.get('/mine', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const userId = req.user!.sub;
  const state = req.query['state'] as string | undefined;
  const overdueOnly = req.query['overdue'] === 'true';

  const [reviews, allRequests] = await Promise.all([
    sectionReviewRepo.getUserReviews(userId),
    documentReviewRequestRepo.getRequestsForReviewer(userId, state),
  ]);

  const now = new Date();
  const requests = (overdueOnly ? allRequests.filter((r) => isOverdue(r, now)) : allRequests)
    // Soonest due first; requests with no due date sort last.
    .slice()
    .sort((a, b) => {
      const av = a.dueDate ? Date.parse(a.dueDate) : Number.POSITIVE_INFINITY;
      const bv = b.dueDate ? Date.parse(b.dueDate) : Number.POSITIVE_INFINITY;
      return av - bv;
    });

  res.status(200).json({
    reviews,
    requests,
    summary: {
      pending: allRequests.filter((r) => r.state === 'pending').length,
      overdue: allRequests.filter((r) => isOverdue(r, now)).length,
      total: allRequests.length,
    },
  });
}));
