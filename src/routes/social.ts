import {
  DeleteCommand,
  GetCommand,
  QueryCommand,
} from '@aws-sdk/lib-dynamodb';
import { Router, Request, Response } from 'express';
import { docClient, publishSocialEvent } from '../lib/aws-clients';
import { publishWithOutbox } from '../services/outbox-publisher';
import { profileRepo } from '../repositories';
import {
  asyncHandler,
  ValidationError,
  NotFoundError,
} from '../middleware/error-handler';
import { tableName } from '../lib/ddb-table-name';
const REL_TABLE = tableName('social-relationships');

export const socialRouter = Router();

interface PublicProfile {
  userId: string;
  displayName: string;
  avatarUrl: string;
  visibility: string;
}

// Helper: batch-get public profile fields for a list of user IDs
async function enrichWithProfiles(userIds: string[]): Promise<PublicProfile[]> {
  if (userIds.length === 0) return [];

  const profiles = await profileRepo.batchGetProfiles(userIds);
  return profiles.map((item) => ({
    userId: item.userId,
    displayName: item.displayName,
    avatarUrl: item.avatarUrl,
    visibility: item.visibility,
  }));
}

// POST /api/social/follow/:userId — follow a user (SOCL-01)
socialRouter.post('/follow/:userId', asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const followerId = req.user!.sub;
  const followeeId = req.params['userId'];

  if (followerId === followeeId) {
    throw new ValidationError('Cannot follow yourself');
  }

  const now = new Date().toISOString();

  await publishWithOutbox({
    target: {
      TableName: REL_TABLE,
      Item: { followerId, followeeId, createdAt: now },
      ConditionExpression: 'attribute_not_exists(followeeId)',
    },
    eventType: 'social.follow',
    queueName: 'social-follows',
    eventPayload: { followerId, followeeId },
    conflictMessage: 'Already following this user',
  });

  res.status(201).json({ followerId, followeeId });
  // No publishSocialEvent — outbox record handles delivery
}));

// DELETE /api/social/follow/:userId — unfollow a user (SOCL-02)
socialRouter.delete('/follow/:userId', asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const followerId = req.user!.sub;
  const followeeId = req.params['userId'];

  const existing = await docClient.send(
    new GetCommand({
      TableName: REL_TABLE,
      Key: { followerId, followeeId },
    }),
  );

  if (!existing.Item) {
    throw new NotFoundError('Not following this user');
  }

  await docClient.send(
    new DeleteCommand({
      TableName: REL_TABLE,
      Key: { followerId, followeeId },
    }),
  );

  res.status(200).json({ message: 'Unfollowed successfully' });
  // Publish social.unfollow event to EventBridge (log-and-continue)
  void publishSocialEvent('social.unfollow', {
    followerId,
    followeeId,
  });
}));

// GET /api/social/followers — list users who follow the caller (SOCL-04)
// Uses GSI followeeId-followerId-index to avoid full-table Scan
socialRouter.get('/followers', asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const callerId = req.user!.sub;

  const queryResult = await docClient.send(
    new QueryCommand({
      TableName: REL_TABLE,
      IndexName: 'followeeId-followerId-index',
      KeyConditionExpression: 'followeeId = :fid',
      ExpressionAttributeValues: { ':fid': callerId },
    }),
  );

  const followerIds = (queryResult.Items ?? []).map((i) => i['followerId'] as string);

  if (followerIds.length === 0) {
    res.status(200).json({ followers: [] });
    return;
  }

  const followers = await enrichWithProfiles(followerIds);
  res.status(200).json({ followers });
}));

// GET /api/social/following — list users the caller follows (SOCL-05)
socialRouter.get('/following', asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const callerId = req.user!.sub;

  const result = await docClient.send(
    new QueryCommand({
      TableName: REL_TABLE,
      KeyConditionExpression: 'followerId = :fid',
      ExpressionAttributeValues: { ':fid': callerId },
    }),
  );

  const followeeIds = (result.Items ?? []).map((i) => i['followeeId'] as string);

  if (followeeIds.length === 0) {
    res.status(200).json({ following: [] });
    return;
  }

  const following = await enrichWithProfiles(followeeIds);
  res.status(200).json({ following });
}));

// GET /api/social/friends — mutual follows (SOCL-03, SOCL-06)
// A "friend" = someone where caller follows them AND they follow caller
socialRouter.get('/friends', asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const callerId = req.user!.sub;

  // 1. Get everyone caller follows (PK query — no scan needed)
  const followingResult = await docClient.send(
    new QueryCommand({
      TableName: REL_TABLE,
      KeyConditionExpression: 'followerId = :fid',
      ExpressionAttributeValues: { ':fid': callerId },
    }),
  );
  const followeeSet = new Set(
    (followingResult.Items ?? []).map((i) => i['followeeId'] as string),
  );

  // 2. Get everyone who follows caller (GSI query — no Scan needed)
  const followersResult = await docClient.send(
    new QueryCommand({
      TableName: REL_TABLE,
      IndexName: 'followeeId-followerId-index',
      KeyConditionExpression: 'followeeId = :fid',
      ExpressionAttributeValues: { ':fid': callerId },
    }),
  );
  const followerSet = new Set(
    (followersResult.Items ?? []).map((i) => i['followerId'] as string),
  );

  // 3. Intersect: mutual follows only
  const friendIds = [...followeeSet].filter((id) => followerSet.has(id));

  if (friendIds.length === 0) {
    res.status(200).json({ friends: [] });
    return;
  }

  const friends = await enrichWithProfiles(friendIds);
  res.status(200).json({ friends });
}));
