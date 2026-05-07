/**
 * LikesService — business logic for post + comment likes (REAC-01/02/03/04/06).
 *
 * Enforces: parent entity (post/comment) must exist before like/unlike,
 * uniqueness via a ConditionExpression (already-liked → ConflictError),
 * and enriches list-likes responses with display names via ProfileRepository.
 *
 * Routes pass callerId / target ids and receive plain DTOs back.
 */
import {
  GetCommand,
  PutCommand,
  DeleteCommand,
  QueryCommand,
} from '@aws-sdk/lib-dynamodb';
import { docClient, publishSocialEvent } from '../lib/aws-clients';
import { broadcastToRoom } from './room-broadcaster';
import { profileRepo } from '../repositories';
import {
  NotFoundError,
  ConflictError,
} from '../middleware/error-handler';
import { tableName } from '../lib/ddb-table-name';

const LIKES_TABLE = tableName('social-likes');
const POSTS_TABLE = tableName('social-posts');
const COMMENTS_TABLE = tableName('social-comments');

export interface LikeRecord {
  targetId: string;
  userId: string;
  type: 'like';
  createdAt: string;
}

export interface LikedByList {
  count: number;
  likedBy: { userId: string; displayName: string }[];
}

async function assertPostExists(roomId: string, postId: string): Promise<void> {
  const postResult = await docClient.send(new GetCommand({
    TableName: POSTS_TABLE,
    Key: { roomId, postId },
  }));
  if (!postResult.Item) {
    throw new NotFoundError('Post not found');
  }
}

async function assertCommentExists(postId: string, commentId: string): Promise<void> {
  const commentResult = await docClient.send(new GetCommand({
    TableName: COMMENTS_TABLE,
    Key: { postId, commentId },
  }));
  if (!commentResult.Item) {
    throw new NotFoundError('Comment not found');
  }
}

async function insertLike(targetId: string, userId: string, createdAt: string): Promise<void> {
  try {
    await docClient.send(new PutCommand({
      TableName: LIKES_TABLE,
      Item: { targetId, userId, type: 'like', createdAt },
      ConditionExpression: 'attribute_not_exists(#uid)',
      ExpressionAttributeNames: { '#uid': 'userId' },
    }));
  } catch (err) {
    if ((err as { name?: string }).name === 'ConditionalCheckFailedException') {
      throw new ConflictError('Already liked');
    }
    throw err;
  }
}

async function removeLike(targetId: string, userId: string): Promise<void> {
  const likeResult = await docClient.send(new GetCommand({
    TableName: LIKES_TABLE,
    Key: { targetId, userId },
  }));
  if (!likeResult.Item) {
    throw new NotFoundError('Like not found');
  }

  await docClient.send(new DeleteCommand({
    TableName: LIKES_TABLE,
    Key: { targetId, userId },
  }));
}

export async function likePost(
  roomId: string,
  postId: string,
  userId: string,
): Promise<LikeRecord> {
  await assertPostExists(roomId, postId);

  const targetId = `post:${postId}`;
  const createdAt = new Date().toISOString();

  await insertLike(targetId, userId, createdAt);

  await broadcastToRoom(roomId, 'social:like', {
    targetId, userId, type: 'like', createdAt,
  });

  void publishSocialEvent('social.like', {
    targetId,
    userId,
    roomId,
    postId,
  });

  return { targetId, userId, type: 'like', createdAt };
}

export async function unlikePost(postId: string, userId: string): Promise<void> {
  const targetId = `post:${postId}`;
  await removeLike(targetId, userId);
}

export async function listPostLikes(
  roomId: string,
  postId: string,
): Promise<LikedByList> {
  await assertPostExists(roomId, postId);

  const targetId = `post:${postId}`;

  const queryResult = await docClient.send(new QueryCommand({
    TableName: LIKES_TABLE,
    KeyConditionExpression: 'targetId = :tid',
    FilterExpression: '#t = :like',
    ExpressionAttributeNames: { '#t': 'type' },
    ExpressionAttributeValues: { ':tid': targetId, ':like': 'like' },
  }));

  const userIds = (queryResult.Items ?? []).map((item) => item['userId'] as string);

  let likedBy: { userId: string; displayName: string }[] = [];
  if (userIds.length > 0) {
    const profiles = await profileRepo.batchGetProfiles(userIds);
    const profileMap = new Map<string, string>();
    for (const profile of profiles) {
      profileMap.set(profile.userId, profile.displayName ?? profile.userId);
    }

    likedBy = userIds.map((uid) => ({
      userId: uid,
      displayName: profileMap.get(uid) ?? uid,
    }));
  }

  return { count: userIds.length, likedBy };
}

export async function likeComment(
  roomId: string,
  postId: string,
  commentId: string,
  userId: string,
): Promise<LikeRecord> {
  await assertCommentExists(postId, commentId);

  const targetId = `comment:${commentId}`;
  const createdAt = new Date().toISOString();

  await insertLike(targetId, userId, createdAt);

  await broadcastToRoom(roomId, 'social:like', {
    targetId, userId, type: 'like', createdAt,
  });

  void publishSocialEvent('social.like', {
    targetId,
    userId,
    roomId,
    commentId,
  });

  return { targetId, userId, type: 'like', createdAt };
}

export async function unlikeComment(commentId: string, userId: string): Promise<void> {
  const targetId = `comment:${commentId}`;
  await removeLike(targetId, userId);
}
