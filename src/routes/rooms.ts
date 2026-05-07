import { randomUUID } from 'crypto';
import { QueryCommand, GetCommand } from '@aws-sdk/lib-dynamodb';
import { Router, Request, Response } from 'express';
import { docClient } from '../lib/aws-clients';
import { setCachedRoom } from '../lib/cache';
import { roomRepo } from '../repositories';
import type { RoomItem } from '../repositories';
import {
  asyncHandler,
  ValidationError,
  ForbiddenError,
  ConflictError,
  AppError,
} from '../middleware/error-handler';
import { tableName } from '../lib/ddb-table-name';
const REL_TABLE = tableName('social-relationships');

export const roomsRouter = Router();

// POST /api/rooms/dm — create DM room (ROOM-03, ROOM-05)
// IMPORTANT: defined BEFORE /:roomId to avoid Express matching '/dm' as a roomId value
roomsRouter.post('/dm', asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { targetUserId } = req.body as { targetUserId?: string };

  if (!targetUserId) {
    throw new ValidationError('targetUserId is required');
  }

  const callerId = req.user!.sub;

  if (targetUserId === callerId) {
    throw new ValidationError('Cannot create a DM with yourself');
  }

  // Mutual-friend guard:
  // Check caller follows target
  const followingResult = await docClient.send(new QueryCommand({
    TableName: REL_TABLE,
    KeyConditionExpression: 'followerId = :fid',
    ExpressionAttributeValues: { ':fid': callerId },
  }));
  const followeeSet = new Set(
    (followingResult.Items ?? []).map(i => i['followeeId'] as string)
  );

  // Check target follows caller (point query — O(1), not scan)
  const reverseResult = await docClient.send(new GetCommand({
    TableName: REL_TABLE,
    Key: { followerId: targetUserId, followeeId: callerId },
  }));
  const targetFollowsCaller = !!reverseResult.Item;

  if (!followeeSet.has(targetUserId) || !targetFollowsCaller) {
    throw new ForbiddenError('DM rooms can only be created between mutual friends');
  }

  // Deterministic DM roomId — sorted to ensure same key regardless of who initiates (ROOM-03)
  const dmRoomId = ['dm', ...[callerId, targetUserId].sort()].join('#');
  const channelId = randomUUID();
  const now = new Date().toISOString();

  // Write room item with ConditionExpression to prevent duplicate DM rooms (TOCTOU-safe)
  try {
    await roomRepo.createRoomConditional({
      roomId: dmRoomId,
      channelId,
      name: `dm-${callerId.slice(-6)}-${targetUserId.slice(-6)}`,
      type: 'dm',
      ownerId: callerId,
      dmPeerUserId: targetUserId,
      createdAt: now,
      updatedAt: now,
    });
  } catch (err: unknown) {
    if (
      err !== null &&
      typeof err === 'object' &&
      'name' in err &&
      (err as { name: string }).name === 'ConditionalCheckFailedException'
    ) {
      // Preserve existing response shape — include the roomId so clients can
      // act on the duplicate (e.g. open the existing DM). This adds a field
      // beyond the standard `{ error }` envelope, so use a custom AppError.
      throw new DmExistsError(dmRoomId);
    }
    throw err;
  }

  // Auto-enroll BOTH users: caller as 'owner', peer as 'member'
  await roomRepo.addMember({ roomId: dmRoomId, userId: callerId, role: 'owner', joinedAt: now });
  await roomRepo.addMember({ roomId: dmRoomId, userId: targetUserId, role: 'member', joinedAt: now });

  // Populate cache with newly created DM room
  void setCachedRoom(dmRoomId, {
    roomId: dmRoomId, channelId, name: `dm-${callerId.slice(-6)}-${targetUserId.slice(-6)}`,
    type: 'dm', ownerId: callerId, dmPeerUserId: targetUserId, createdAt: now, updatedAt: now,
  });

  res.status(201).json({ roomId: dmRoomId, channelId, type: 'dm', dmPeerUserId: targetUserId, createdAt: now });
}));

// Custom AppError variant — existing endpoint returns an extra `roomId` alongside
// the `error` field when the DM already exists. The central middleware maps
// status+message; this variant overrides the body shape via its own handler.
class DmExistsError extends ConflictError {
  public readonly roomId: string;
  constructor(roomId: string) {
    super('A DM room already exists between these two users');
    this.roomId = roomId;
  }
}

// POST /api/rooms — create standalone room (ROOM-01, ROOM-05, ROOM-07)
roomsRouter.post('/', asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { name } = req.body as { name?: string };

  if (!name || name.length < 1 || name.length > 100) {
    throw new ValidationError('name is required (max 100 chars)');
  }

  const roomId = randomUUID();
  const channelId = randomUUID();
  const now = new Date().toISOString();

  const roomItem: RoomItem = {
    roomId,
    channelId,
    name: req.body.name,
    type: 'standalone',
    ownerId: req.user!.sub,
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

  // Populate cache with newly created room
  void setCachedRoom(roomId, roomItem);

  res.status(201).json({ roomId, channelId, name: req.body.name, type: 'standalone', ownerId: req.user!.sub, role: 'owner', createdAt: now });
}));

// Local error middleware — handles DmExistsError's extra `roomId` response field.
// Other AppErrors fall through to the app-level handler.
roomsRouter.use((err: unknown, _req: Request, res: Response, next: (e?: unknown) => void): void => {
  if (err instanceof DmExistsError) {
    res.status(err.status).json({ error: err.message, roomId: err.roomId });
    return;
  }
  next(err);
});

// Silence unused-import warning for AppError (it's used by the class chain).
void AppError;
