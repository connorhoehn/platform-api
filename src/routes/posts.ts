import { Router, Request, Response } from 'express';
import { requireRoomMembership } from '../middleware/require-membership';
import { asyncHandler } from '../middleware/error-handler';
import * as postsService from '../services/posts-service';

// postsRouter is mounted at /rooms/:roomId — mergeParams:true exposes :roomId
export const postsRouter = Router({ mergeParams: true });

// POST /api/rooms/:roomId/posts — create a post (CONT-01)
postsRouter.post('/', requireRoomMembership, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { roomId } = req.params;
  const { content } = req.body as { content?: string };
  const result = await postsService.createPost(roomId, req.user!.sub, content);
  res.status(201).json(result);
}));

// PUT /api/rooms/:roomId/posts/:postId — edit own post (CONT-02)
postsRouter.put('/:postId', asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { roomId, postId } = req.params;
  const { content } = req.body as { content?: string };
  const result = await postsService.editPost(roomId, postId, req.user!.sub, content);
  res.status(200).json(result);
}));

// DELETE /api/rooms/:roomId/posts/:postId — delete own post (CONT-03)
postsRouter.delete('/:postId', asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { roomId, postId } = req.params;
  await postsService.deletePost(roomId, postId, req.user!.sub);
  res.status(204).send();
}));

// GET /api/rooms/:roomId/posts — paginated room feed, newest-first (CONT-04)
postsRouter.get('/', requireRoomMembership, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { roomId } = req.params;
  const limit = parseInt(req.query['limit'] as string ?? '20', 10) || 20;
  const cursor = req.query['cursor'] as string | undefined;
  const result = await postsService.listRoomPosts(roomId, { limit, ...(cursor ? { cursor } : {}) });
  res.status(200).json(result);
}));

// userPostsRouter is mounted at /posts in index.ts (top-level, no roomId context)
// GET /api/posts?userId=:uid — get all posts by a user across all rooms (CONT-05)
export const userPostsRouter = Router();

userPostsRouter.get('/', asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const userId = (req.query['userId'] as string) ?? req.user!.sub;
  const posts = await postsService.listUserPosts(userId);
  res.status(200).json({ posts });
}));
