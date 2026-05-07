import { ulid } from 'ulid';
import { Router, Request, Response } from 'express';
import { broadcastService } from '../services/broadcast';
import { publishSocialEvent } from '../lib/aws-clients';
import { getCachedRoom, setCachedRoom } from '../lib/cache';
import { roomRepo } from '../repositories';
import type { RoomItem, RoomMemberItem } from '../repositories';
import {
  asyncHandler,
  NotFoundError,
  ForbiddenError,
  ConflictError,
} from '../middleware/error-handler';

export const roomMembersRouter = Router({ mergeParams: true });

// POST /api/rooms/:roomId/join — join a room (ROOM-04)
roomMembersRouter.post('/join', asyncHandler(async (req: Request, res: Response): Promise<void> => {
  // Verify room exists (check cache first)
  let roomItem = await getCachedRoom<RoomItem>(req.params.roomId);
  if (!roomItem) {
    roomItem = await roomRepo.getRoom(req.params.roomId);
    if (!roomItem) {
      throw new NotFoundError('Room not found');
    }
    void setCachedRoom(req.params.roomId, roomItem);
  }

  // Check if caller is already a member
  const existing = await roomRepo.getMembership(req.params.roomId, req.user!.sub);
  if (existing) {
    throw new ConflictError('Already a member of this room');
  }

  // Write member record + outbox record atomically
  const outboxId = ulid();
  const now = new Date().toISOString();

  const memberItem: RoomMemberItem = {
    roomId: req.params.roomId,
    userId: req.user!.sub,
    role: 'member',
    joinedAt: now,
  };

  await roomRepo.addMemberWithOutbox(memberItem, {
    outboxId,
    status: 'UNPROCESSED',
    eventType: 'social.room.join',
    queueName: 'social-rooms',
    payload: JSON.stringify({ roomId: req.params.roomId, userId: req.user!.sub, timestamp: now }),
    createdAt: now,
  });

  res.status(201).json({ roomId: req.params.roomId, userId: req.user!.sub, role: 'member', joinedAt: now });

  // Broadcast social:member_joined to room channel (non-fatal if Redis unavailable)
  // We already have roomItem in hand from the membership check — avoid another cache lookup
  void broadcastService.emit(roomItem.channelId, 'social:member_joined', {
    roomId: req.params.roomId, userId: req.user!.sub, joinedAt: now,
  });
  // No publishSocialEvent — outbox record handles delivery
}));

// DELETE /api/rooms/:roomId/leave — leave a room (RTIM-04 leave half)
roomMembersRouter.delete('/leave', asyncHandler(async (req: Request, res: Response): Promise<void> => {
  // Verify room exists and get channelId for broadcast
  const roomItem = await roomRepo.getRoom(req.params.roomId);
  if (!roomItem) {
    throw new NotFoundError('Room not found');
  }

  // Verify caller is a member
  const membership = await roomRepo.getMembership(req.params.roomId, req.user!.sub);
  if (!membership) {
    throw new NotFoundError('You are not a member of this room');
  }

  // Owners cannot leave their own room (prevents orphaned rooms)
  if (membership.role === 'owner') {
    throw new ForbiddenError('Room owners cannot leave their own room');
  }

  // Delete member record
  await roomRepo.removeMember(req.params.roomId, req.user!.sub);

  res.status(200).json({ roomId: req.params.roomId, userId: req.user!.sub, left: true });

  // Broadcast social:member_left to room channel (non-fatal if Redis unavailable)
  void broadcastService.emit(roomItem.channelId, 'social:member_left', {
    roomId: req.params.roomId, userId: req.user!.sub, leftAt: new Date().toISOString(),
  });
  // Publish social.room.leave event to EventBridge (log-and-continue)
  void publishSocialEvent('social.room.leave', {
    roomId: req.params.roomId,
    userId: req.user!.sub,
  });
}));

// GET /api/rooms/:roomId/members — list room members (ROOM-06)
roomMembersRouter.get('/members', asyncHandler(async (req: Request, res: Response): Promise<void> => {
  // Verify room exists
  const roomItem = await roomRepo.getRoom(req.params.roomId);
  if (!roomItem) {
    throw new NotFoundError('Room not found');
  }

  // Check caller is a member (authorization gate)
  const callerMembership = await roomRepo.getMembership(req.params.roomId, req.user!.sub);
  if (!callerMembership) {
    throw new ForbiddenError('You are not a member of this room');
  }

  // Query all members of this room
  const members = await roomRepo.getRoomMembers(req.params.roomId);

  const result = members.map(item => ({
    roomId: item.roomId,
    userId: item.userId,
    role: item.role,
    joinedAt: item.joinedAt,
  }));

  res.status(200).json({ members: result });
}));

// GET /api/rooms — list all rooms the caller belongs to (ROOM-08)
// Uses GSI userId-roomId-index on social-room-members to avoid full-table Scan
// NOTE: Mounted as a separate top-level router at /rooms in index.ts
export const myRoomsRouter = Router();

myRoomsRouter.get('/', asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const rooms = await roomRepo.getRoomsByUser(req.user!.sub);

  res.status(200).json({ rooms });
}));
