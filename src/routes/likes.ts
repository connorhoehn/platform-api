import { Router, Request, Response } from 'express';
import { requireRoomMembership } from '../middleware/require-membership';
import { asyncHandler } from '../middleware/error-handler';
import * as likesService from '../services/likes-service';

// postLikesRouter is mounted at /rooms/:roomId/posts/:postId — mergeParams:true exposes :roomId and :postId
export const postLikesRouter = Router({ mergeParams: true });

// POST /api/rooms/:roomId/posts/:postId/likes — like a post (REAC-01)
postLikesRouter.post('/', requireRoomMembership, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { roomId, postId } = req.params;
  const result = await likesService.likePost(roomId, postId, req.user!.sub);
  res.status(201).json(result);
}));

// DELETE /api/rooms/:roomId/posts/:postId/likes — unlike a post (REAC-02)
postLikesRouter.delete('/', requireRoomMembership, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { postId } = req.params;
  await likesService.unlikePost(postId, req.user!.sub);
  res.status(204).send();
}));

// GET /api/rooms/:roomId/posts/:postId/likes — who liked a post with display names and count (REAC-06)
postLikesRouter.get('/', requireRoomMembership, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { roomId, postId } = req.params;
  const result = await likesService.listPostLikes(roomId, postId);
  res.status(200).json(result);
}));

// commentLikesRouter is mounted at /rooms/:roomId/posts/:postId/comments/:commentId — mergeParams:true exposes all params
export const commentLikesRouter = Router({ mergeParams: true });

// POST /api/rooms/:roomId/posts/:postId/comments/:commentId/likes — like a comment (REAC-03)
commentLikesRouter.post('/', requireRoomMembership, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { roomId, postId, commentId } = req.params;
  const result = await likesService.likeComment(roomId, postId, commentId, req.user!.sub);
  res.status(201).json(result);
}));

// DELETE /api/rooms/:roomId/posts/:postId/comments/:commentId/likes — unlike a comment (REAC-04)
commentLikesRouter.delete('/', requireRoomMembership, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { commentId } = req.params;
  await likesService.unlikeComment(commentId, req.user!.sub);
  res.status(204).send();
}));
