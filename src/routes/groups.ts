import { randomUUID } from 'crypto';
import { TransactionCanceledException } from '@aws-sdk/client-dynamodb';
import { Router, Request, Response } from 'express';
import { getCachedGroup, setCachedGroup, invalidateGroupCache } from '../lib/cache';
import { groupRepo } from '../repositories';
import type { GroupItem, GroupMemberItem } from '../repositories';
import {
  asyncHandler,
  ValidationError,
  NotFoundError,
  ForbiddenError,
  ConflictError,
} from '../middleware/error-handler';

export const groupsRouter = Router();

// POST /api/groups — create a group (GRUP-01)
groupsRouter.post('/', asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { name, description, visibility } = req.body as {
    name?: string;
    description?: string;
    visibility?: string;
  };

  // Validation
  if (!name || name.length < 1 || name.length > 100) {
    throw new ValidationError('name is required (max 100 chars)');
  }
  if (description !== undefined && description.length > 500) {
    throw new ValidationError('description must be 500 chars or fewer');
  }
  if (visibility !== undefined && visibility !== 'public' && visibility !== 'private') {
    throw new ValidationError('visibility must be public or private');
  }

  const callerId = req.user!.sub;
  const groupId = randomUUID();
  const now = new Date().toISOString();

  const groupItem: GroupItem = {
    groupId,
    name,
    description: description ?? '',
    visibility: (visibility as 'public' | 'private') ?? 'public',
    ownerId: callerId,
    createdAt: now,
    updatedAt: now,
  };

  const memberItem: GroupMemberItem = {
    groupId,
    userId: callerId,
    role: 'owner',
    joinedAt: now,
  };

  try {
    await groupRepo.createGroupWithOwner(groupItem, memberItem);
  } catch (err) {
    if (err instanceof TransactionCanceledException) {
      throw new ConflictError('Group creation conflict — please retry');
    }
    throw err;
  }

  // Populate cache with newly created group
  void setCachedGroup(groupId, groupItem);

  res.status(201).json({ ...groupItem, role: 'owner' });
}));

// GET /api/groups/:groupId — get group details
groupsRouter.get('/:groupId', asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { groupId } = req.params;
  const callerId = req.user!.sub;

  // Check Redis cache first
  let group = await getCachedGroup<GroupItem>(groupId);

  if (!group) {
    group = await groupRepo.getGroup(groupId);

    if (!group) {
      throw new NotFoundError('Group not found');
    }

    // Populate cache on miss
    void setCachedGroup(groupId, group);
  }

  // Check caller membership
  const memberItem = await groupRepo.getMembership(groupId, callerId);
  const isMember = memberItem !== null;

  // Private group gate: non-members cannot see it
  if (group.visibility === 'private' && !isMember) {
    throw new ForbiddenError('This group is private');
  }

  res.status(200).json({ ...group, role: isMember ? memberItem!.role : null });
}));

// DELETE /api/groups/:groupId — delete a group (GRUP-02)
groupsRouter.delete('/:groupId', asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { groupId } = req.params;
  const callerId = req.user!.sub;

  const group = await groupRepo.getGroup(groupId);

  if (!group) {
    throw new NotFoundError('Group not found');
  }

  if (group.ownerId !== callerId) {
    throw new ForbiddenError('Only the group owner can delete this group');
  }

  await groupRepo.deleteGroup(groupId);

  // Invalidate cache for deleted group
  void invalidateGroupCache(groupId);

  res.status(200).json({ message: 'Group deleted' });
}));

// PATCH /api/groups/:groupId/visibility — update visibility (GRUP-05)
groupsRouter.patch('/:groupId/visibility', asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { groupId } = req.params;
  const callerId = req.user!.sub;
  const { visibility } = req.body as { visibility?: string };

  // Validate visibility first
  if (!visibility || (visibility !== 'public' && visibility !== 'private')) {
    throw new ValidationError('visibility must be public or private');
  }

  const group = await groupRepo.getGroup(groupId);

  if (!group) {
    throw new NotFoundError('Group not found');
  }

  if (group.ownerId !== callerId) {
    throw new ForbiddenError('Only the group owner can change visibility');
  }

  const updated = await groupRepo.updateGroupVisibility(groupId, visibility as 'public' | 'private');

  // Invalidate stale cache; next GET will re-populate
  void invalidateGroupCache(groupId);

  res.status(200).json(updated);
}));
