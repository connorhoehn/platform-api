import { Router, Request, Response } from 'express';
import { requireRoomMembership } from '../middleware/require-membership';
import { asyncHandler } from '../middleware/error-handler';
import * as commentsService from '../services/comments-service';

// commentsRouter is mounted at /rooms/:roomId/posts/:postId — mergeParams:true exposes :roomId and :postId
export const commentsRouter = Router({ mergeParams: true });

// POST /api/rooms/:roomId/posts/:postId/comments — create a comment or reply (CONT-06, CONT-07)
commentsRouter.post('/', requireRoomMembership, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { roomId, postId } = req.params;
  const { content, parentCommentId } = req.body as { content?: string; parentCommentId?: string };
  const item = await commentsService.createComment(roomId, postId, req.user!.sub, { ...(content !== undefined ? { content } : {}), ...(parentCommentId ? { parentCommentId } : {}) });
  res.status(201).json(item);
}));

// GET /api/rooms/:roomId/posts/:postId/comments — list all comments for a post (flat array; clients group by parentCommentId)
commentsRouter.get('/', requireRoomMembership, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { roomId, postId } = req.params;
  const comments = await commentsService.listComments(roomId, postId);
  res.status(200).json({ comments });
}));

// DELETE /api/rooms/:roomId/posts/:postId/comments/:commentId — delete own comment (CONT-08)
commentsRouter.delete('/:commentId', asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { postId, commentId } = req.params;
  await commentsService.deleteComment(postId, commentId, req.user!.sub);
  res.status(204).send();
}));
