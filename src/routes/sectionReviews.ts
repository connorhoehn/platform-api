import { Router, Request, Response } from 'express';
import { requireAuth } from '../middleware/auth';
import { sectionReviewRepo, profileRepo } from '../repositories';
import { broadcastService } from '../services/broadcast';
import {
  asyncHandler,
  ValidationError,
} from '../middleware/error-handler';

export const sectionReviewsRouter = Router({ mergeParams: true });
export const myReviewsRouter = Router();

// POST /api/documents/:documentId/sections/:sectionId/reviews
sectionReviewsRouter.post('/', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { documentId, sectionId } = req.params;
  const userId = req.user!.sub;
  const { status, comment } = req.body as { status?: string; comment?: string };

  if (!status) {
    throw new ValidationError('status is required');
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
    ...(comment !== undefined && { comment }),
  });

  // Broadcast real-time event (non-fatal)
  void broadcastService.emit(`doc:${documentId}`, 'social:post' as any, {
    type: 'section:review',
    documentId,
    sectionId,
    review,
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

// GET /api/reviews/mine
myReviewsRouter.get('/mine', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const userId = req.user!.sub;
  const reviews = await sectionReviewRepo.getUserReviews(userId);
  res.status(200).json({ reviews });
}));
