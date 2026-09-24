import { ulid } from 'ulid';
import { Router, Request, Response } from 'express';
import { publishSignal, publishDurable } from '../lib/event-catalog';
import { getCachedRoom, setCachedRoom } from '../lib/cache';
import { groupRepo, roomRepo } from '../repositories';
import type { RoomItem, RoomMemberItem } from '../repositories';
import {
  asyncHandler,
  NotFoundError,
  ForbiddenError,
  ConflictError,
} from '../middleware/error-handler';
import log from '../lib/logger';

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

  // NFR #175: joining is what requireRoomMembership then trusts, so a DM room
  // is never self-joinable and a group's room takes an active membership of
  // that group. Refused as not-found — a stranger learns nothing.
  if (roomItem.type === 'dm') throw new NotFoundError('Room not found');
  if (roomItem.type === 'group') {
    const gm = roomItem.groupId ? await groupRepo.getMembership(roomItem.groupId, req.user!.sub) : null;
    if (!gm || (gm.status !== undefined && gm.status !== 'active')) throw new NotFoundError('Room not found');
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

  publishSignal(roomItem.channelId, 'social:member_joined', {
    roomId: req.params.roomId, userId: req.user!.sub, joinedAt: now,
  }).catch((err: unknown) => {
    log.error({ event: 'social:member_joined', err: err instanceof Error ? err.message : String(err) }, 'signal publish failed');
  });
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

  publishSignal(roomItem.channelId, 'social:member_left', {
    roomId: req.params.roomId, userId: req.user!.sub, leftAt: new Date().toISOString(),
  }).catch((err: unknown) => {
    log.error({ event: 'social:member_left', err: err instanceof Error ? err.message : String(err) }, 'signal publish failed');
  });
  publishDurable('social.room.leave', {
    roomId: req.params.roomId,
    userId: req.user!.sub,
  }).catch((err: unknown) => {
    log.error({ event: 'social.room.leave', err: err instanceof Error ? err.message : String(err) }, 'durable publish failed');
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
