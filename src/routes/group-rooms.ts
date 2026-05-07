import { randomUUID } from 'crypto';
import { Router, Request, Response } from 'express';
import { setCachedRoom } from '../lib/cache';
import { roomRepo, groupRepo } from '../repositories';
import type { RoomItem } from '../repositories';
import {
  asyncHandler,
  ValidationError,
  NotFoundError,
  ForbiddenError,
} from '../middleware/error-handler';

export const groupRoomsRouter = Router({ mergeParams: true });

// POST /api/groups/:groupId/rooms — create group-scoped room (ROOM-02, ROOM-05)
groupRoomsRouter.post('/', asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { name } = req.body as { name?: string };

  if (!name || name.length < 1 || name.length > 100) {
    throw new ValidationError('name is required (max 100 chars)');
  }

  // Verify group exists
  const group = await groupRepo.getGroup(req.params.groupId);
  if (!group) {
    throw new NotFoundError('Group not found');
  }

  // Group-admin guard — fetch caller's membership in the group
  const callerMembership = await groupRepo.getMembership(req.params.groupId, req.user!.sub);
  const callerRole = callerMembership?.role;
  // Treat absent status field as 'active'; treat invited-only as not a member
  const callerStatus = callerMembership?.status;
  if (!callerRole || (callerStatus === 'invited') || (callerRole !== 'owner' && callerRole !== 'admin')) {
    throw new ForbiddenError('Only group owners and admins can create rooms');
  }

  const roomId = randomUUID();
  const channelId = randomUUID();
  const now = new Date().toISOString();

  const roomItem: RoomItem = {
    roomId,
    channelId,
    name: req.body.name,
    type: 'group',
    ownerId: req.user!.sub,
    groupId: req.params.groupId,
    createdAt: now,
    updatedAt: now,
  };

  // Write room item
  await roomRepo.createRoom(roomItem);

  // Auto-enroll creator as owner
  await roomRepo.addMember({
    roomId,
    userId: req.user!.sub,
    role: 'owner',
    joinedAt: now,
  });

  // Populate cache with newly created group room
  void setCachedRoom(roomId, roomItem);

  res.status(201).json({ roomId, channelId, name: req.body.name, type: 'group', groupId: req.params.groupId, ownerId: req.user!.sub, role: 'owner', createdAt: now });
}));
