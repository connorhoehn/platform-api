import { skipAuthEnabled } from '../lib/insecureDevMode';
import { Request, Response, NextFunction } from 'express';
import { roomRepo } from '../repositories';
import log from '../lib/logger';

/**
 * Express middleware that gates access to room-scoped routes.
 * Checks that the authenticated user (req.user.sub) is a member
 * of the room identified by req.params.roomId.
 *
 * Returns 403 if the user is not a member.
 *
 * Usage:
 *   router.use(requireRoomMembership);
 *   // or on specific routes:
 *   router.post('/', requireRoomMembership, handler);
 */
export async function requireRoomMembership(
  req: Request,
  res: Response,
  next: NextFunction,
): Promise<void> {
  // In local dev (SKIP_AUTH=true), skip membership check so any dev identity can post
  if (skipAuthEnabled()) {
    next();
    return;
  }

  try {
    const { roomId } = req.params;
    const userId = req.user!.sub;

    if (!roomId) {
      res.status(400).json({ error: 'roomId is required' });
      return;
    }

    const isMember = await roomRepo.isMember(roomId, userId);
    if (!isMember) {
      res.status(403).json({ error: 'You must be a member of this room' });
      return;
    }

    next();
  } catch (err) {
    log.error({ err, roomId: req.params.roomId, userId: req.user?.sub }, 'error checking room membership');
    res.status(500).json({ error: 'Internal server error' });
  }
}
