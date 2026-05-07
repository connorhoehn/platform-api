import { Router, Request, Response } from 'express';
import { requireAuth } from '../middleware/auth';
import { broadcastService } from '../services/broadcast';
import { documentCommentRepo } from '../repositories';
import {
  asyncHandler,
  ValidationError,
} from '../middleware/error-handler';

// documentCommentsRouter is mounted at /documents — mergeParams:true exposes :documentId
export const documentCommentsRouter = Router({ mergeParams: true });

// POST /api/documents/:documentId/comments — create a comment or threaded reply
documentCommentsRouter.post('/:documentId/comments', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { documentId } = req.params;
  const { sectionId, text, parentCommentId } = req.body as {
    sectionId?: string;
    text?: string;
    parentCommentId?: string;
  };
  const userId = req.user!.sub;

  if (!sectionId || typeof sectionId !== 'string' || !sectionId.trim()) {
    throw new ValidationError('sectionId is required');
  }
  const trimmedText = (text ?? '').trim();
  if (!trimmedText || trimmedText.length > 10000) {
    throw new ValidationError('text is required (max 10000 chars)');
  }

  const now = new Date().toISOString();
  const comment = await documentCommentRepo.createComment({
    documentId,
    sectionId: sectionId.trim(),
    text: trimmedText,
    userId,
    displayName: req.user!.email ?? userId,
    color: '#4A90D9',
    timestamp: now,
    ...(parentCommentId ? { parentCommentId } : {}),
  });

  // Broadcast doc:comment_added to document channel (non-fatal if Redis unavailable)
  void broadcastService.emit(`doc-comments:${documentId}`, 'doc:comment_added', {
    documentId,
    comment,
  });

  res.status(201).json({ comment });
}));

// GET /api/documents/:documentId/comments — list comments, optionally filtered by sectionId
documentCommentsRouter.get('/:documentId/comments', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { documentId } = req.params;
  const sectionId = req.query['sectionId'] as string | undefined;
  const limit = Math.min(parseInt(req.query['limit'] as string ?? '50', 10) || 50, 200);

  let result;
  if (sectionId) {
    result = await documentCommentRepo.getCommentsForSection(sectionId, limit);
  } else {
    result = await documentCommentRepo.getCommentsForDocument(documentId, limit);
  }

  res.status(200).json({ comments: result.items });
}));

// PATCH /api/documents/:documentId/comments/:commentId — resolve/unresolve a thread
documentCommentsRouter.patch('/:documentId/comments/:commentId', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { documentId, commentId } = req.params;
  const { resolved } = req.body as { resolved?: boolean };
  const userId = req.user!.sub;

  if (typeof resolved !== 'boolean') {
    throw new ValidationError('resolved (boolean) is required');
  }

  const now = new Date().toISOString();
  if (resolved) {
    await documentCommentRepo.resolveThread(documentId, commentId, userId, now);
  } else {
    await documentCommentRepo.unresolveThread(documentId, commentId);
  }

  // Broadcast resolve/unresolve event (non-fatal if Redis unavailable)
  void broadcastService.emit(`doc-comments:${documentId}`, 'doc:comment_resolved', {
    documentId,
    commentId,
    resolved,
    ...(resolved ? { resolvedBy: userId, resolvedAt: now } : {}),
  });

  res.status(200).json({
    comment: {
      documentId,
      commentId,
      resolved,
      ...(resolved ? { resolvedBy: userId, resolvedAt: now } : {}),
    },
  });
}));

// DELETE /api/documents/:documentId/comments/:commentId — delete a comment
documentCommentsRouter.delete('/:documentId/comments/:commentId', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { documentId, commentId } = req.params;

  await documentCommentRepo.deleteComment(documentId, commentId);

  // Broadcast delete event (non-fatal if Redis unavailable)
  void broadcastService.emit(`doc-comments:${documentId}`, 'doc:comment_deleted', {
    documentId,
    commentId,
  });

  res.status(204).send();
}));
