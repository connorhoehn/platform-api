import { Router, Request, Response } from 'express';
import { requireRoomMembership } from '../middleware/require-membership';
import { asyncHandler } from '../middleware/error-handler';
import * as reactionsService from '../services/reactions-service';

// reactionsRouter is mounted at /rooms/:roomId/posts/:postId — mergeParams:true exposes :roomId and :postId
export const reactionsRouter = Router({ mergeParams: true });

// POST /api/rooms/:roomId/posts/:postId/reactions — add an emoji reaction to a post (REAC-05)
// Body: { emoji: string }
reactionsRouter.post('/reactions', requireRoomMembership, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { roomId, postId } = req.params;
  const { emoji } = req.body as { emoji?: string };
  const result = await reactionsService.addReaction(roomId, postId, req.user!.sub, emoji);
  res.status(201).json(result);
}));

// DELETE /api/rooms/:roomId/posts/:postId/reactions/:emoji — remove an emoji reaction from a post (REAC-05)
reactionsRouter.delete('/reactions/:emoji', requireRoomMembership, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { postId } = req.params;
  const emoji = decodeURIComponent(req.params.emoji);
  await reactionsService.removeReaction(postId, req.user!.sub, emoji);
  res.status(204).send();
}));
