import { Router, Request, Response } from 'express';
import { groupRepo } from '../repositories';
import {
  asyncHandler,
  ValidationError,
  NotFoundError,
  ForbiddenError,
  ConflictError,
} from '../middleware/error-handler';

export const groupMembersRouter = Router({ mergeParams: true });

// POST /api/groups/:groupId/invite — invite a user (GRUP-03)
groupMembersRouter.post('/invite', asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { groupId } = req.params;
  const callerId = req.user!.sub;
  const { userId } = req.body as { userId?: string };

  if (!userId) {
    throw new ValidationError('userId is required');
  }

  if (userId === callerId) {
    throw new ValidationError('Cannot invite yourself');
  }

  const group = await groupRepo.getGroup(groupId);
  if (!group) {
    throw new NotFoundError('Group not found');
  }

  const callerMembership = await groupRepo.getMembership(groupId, callerId);
  if (!callerMembership || (callerMembership.role !== 'owner' && callerMembership.role !== 'admin')) {
    throw new ForbiddenError('Only group owners and admins can invite members');
  }

  const targetMembership = await groupRepo.getMembership(groupId, userId);
  if (targetMembership && (targetMembership.status === 'active' || targetMembership.status === undefined)) {
    throw new ConflictError('User is already a member');
  }

  await groupRepo.addMember({
    groupId,
    userId,
    role: 'member',
    status: 'invited',
    invitedAt: new Date().toISOString(),
    joinedAt: '',
  });

  res.status(201).json({ groupId, userId, status: 'invited' });
}));

// POST /api/groups/:groupId/invitations/:action — accept or decline an invitation (GRUP-04)
groupMembersRouter.post('/invitations/:action', asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { groupId, action } = req.params;
  const callerId = req.user!.sub;

  if (action !== 'accept' && action !== 'decline') {
    throw new ValidationError('action must be accept or decline');
  }

  const group = await groupRepo.getGroup(groupId);
  if (!group) {
    throw new NotFoundError('Group not found');
  }

  const membership = await groupRepo.getMembership(groupId, callerId);
  if (!membership || membership.status !== 'invited') {
    throw new NotFoundError('No pending invitation found');
  }

  if (action === 'accept') {
    await groupRepo.updateMemberStatus(groupId, callerId, 'active', new Date().toISOString());
    res.status(200).json({ message: 'Invitation accepted', groupId });
  } else {
    await groupRepo.removeMember(groupId, callerId);
    res.status(200).json({ message: 'Invitation declined' });
  }
}));

// POST /api/groups/:groupId/join — join a public group without invitation (GRUP-06)
groupMembersRouter.post('/join', asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { groupId } = req.params;
  const callerId = req.user!.sub;

  const group = await groupRepo.getGroup(groupId);
  if (!group) {
    throw new NotFoundError('Group not found');
  }

  if (group.visibility === 'private') {
    throw new ForbiddenError('This group is private. You must be invited to join.');
  }

  const existing = await groupRepo.getMembership(groupId, callerId);
  if (existing && (existing.status === 'active' || existing.status === undefined)) {
    throw new ConflictError('Already a member of this group');
  }

  await groupRepo.addMember({
    groupId,
    userId: callerId,
    role: 'member',
    status: 'active',
    joinedAt: new Date().toISOString(),
  });

  res.status(201).json({ groupId, userId: callerId, role: 'member' });
}));

// DELETE /api/groups/:groupId/leave — leave a group (GRUP-07)
groupMembersRouter.delete('/leave', asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { groupId } = req.params;
  const callerId = req.user!.sub;

  const group = await groupRepo.getGroup(groupId);
  if (!group) {
    throw new NotFoundError('Group not found');
  }

  const membership = await groupRepo.getMembership(groupId, callerId);
  if (!membership || membership.status === 'invited') {
    throw new NotFoundError('You are not a member of this group');
  }

  if (membership.role === 'owner') {
    throw new ForbiddenError('Group owner cannot leave. Transfer ownership or delete the group.');
  }

  await groupRepo.removeMember(groupId, callerId);

  res.status(200).json({ message: 'Left group successfully' });
}));

// GET /api/groups/:groupId/members — list all members with roles (GRUP-08, GRUP-09)
groupMembersRouter.get('/members', asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { groupId } = req.params;
  const callerId = req.user!.sub;

  const group = await groupRepo.getGroup(groupId);
  if (!group) {
    throw new NotFoundError('Group not found');
  }

  const callerMembership = await groupRepo.getMembership(groupId, callerId);
  const isActiveMember = callerMembership !== null &&
    (callerMembership.status === 'active' || callerMembership.status === undefined);

  if (group.visibility === 'private' && !isActiveMember) {
    throw new ForbiddenError('This group is private');
  }

  const members = await groupRepo.getGroupMembers(groupId);

  const result = members.map((item) => ({
    userId: item.userId,
    role: item.role,
    joinedAt: item.joinedAt,
  }));

  res.status(200).json({ members: result });
}));
