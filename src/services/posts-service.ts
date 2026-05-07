/**
 * PostsService — business logic for social posts.
 *
 * Routes delegate to these functions; the service owns validation,
 * authorization checks, and composition of repo + outbox + broadcaster
 * calls. All invariant violations throw AppError subclasses so the
 * central error middleware can map them to HTTP responses.
 *
 * Importable / testable without Express — callers pass primitives +
 * plain object inputs and receive primitives / DTOs back.
 */
import { ulid } from 'ulid';
import {
  GetCommand,
  QueryCommand,
  UpdateCommand,
  DeleteCommand,
} from '@aws-sdk/lib-dynamodb';
import { docClient } from '../lib/aws-clients';
import { broadcastToRoom } from './room-broadcaster';
import { publishWithOutbox } from './outbox-publisher';
import {
  ValidationError,
  NotFoundError,
  ForbiddenError,
} from '../middleware/error-handler';
import { tableName } from '../lib/ddb-table-name';

const POSTS_TABLE = tableName('social-posts');

export interface PostItem {
  roomId: string;
  postId: string;
  authorId: string;
  content: string;
  createdAt: string;
  updatedAt: string;
}

export interface CreatedPost {
  roomId: string;
  postId: string;
  authorId: string;
  content: string;
  createdAt: string;
}

export interface UpdatedPost {
  roomId: string;
  postId: string;
  content: string;
  updatedAt: string;
}

export interface ListPostsResult {
  posts: PostItem[];
  nextCursor: string | null;
}

function validateContent(content: string | undefined): string {
  const trimmed = (content ?? '').trim();
  if (!trimmed || trimmed.length > 10000) {
    throw new ValidationError('content is required (max 10000 chars)');
  }
  return trimmed;
}

export async function createPost(
  roomId: string,
  authorId: string,
  content: string | undefined,
): Promise<CreatedPost> {
  const trimmedContent = validateContent(content);

  const postId = ulid();
  const now = new Date().toISOString();

  await publishWithOutbox({
    target: {
      TableName: POSTS_TABLE,
      Item: {
        roomId,
        postId,
        authorId,
        content: trimmedContent,
        createdAt: now,
        updatedAt: now,
      },
    },
    eventType: 'social.post.created',
    queueName: 'social-posts',
    eventPayload: { roomId, postId, authorId },
  });

  await broadcastToRoom(roomId, 'social:post', {
    roomId, postId, authorId, content: trimmedContent, createdAt: now,
  });

  return { roomId, postId, authorId, content: trimmedContent, createdAt: now };
}

export async function editPost(
  roomId: string,
  postId: string,
  callerId: string,
  content: string | undefined,
): Promise<UpdatedPost> {
  const trimmedContent = validateContent(content);

  const result = await docClient.send(new QueryCommand({
    TableName: POSTS_TABLE,
    KeyConditionExpression: 'roomId = :rid AND postId = :pid',
    ExpressionAttributeValues: { ':rid': roomId, ':pid': postId },
  }));
  const post = result.Items?.[0];
  if (!post) {
    throw new NotFoundError('Post not found');
  }
  if (post['authorId'] !== callerId) {
    throw new ForbiddenError('You can only edit your own posts');
  }

  const now = new Date().toISOString();
  await docClient.send(new UpdateCommand({
    TableName: POSTS_TABLE,
    Key: { roomId, postId },
    UpdateExpression: 'SET #c = :content, updatedAt = :now',
    ExpressionAttributeNames: { '#c': 'content' },
    ExpressionAttributeValues: { ':content': trimmedContent, ':now': now },
  }));

  return { roomId, postId, content: trimmedContent, updatedAt: now };
}

export async function deletePost(
  roomId: string,
  postId: string,
  callerId: string,
): Promise<void> {
  const result = await docClient.send(new GetCommand({
    TableName: POSTS_TABLE,
    Key: { roomId, postId },
  }));
  if (!result.Item) {
    throw new NotFoundError('Post not found');
  }
  if (result.Item['authorId'] !== callerId) {
    throw new ForbiddenError('You can only delete your own posts');
  }

  await docClient.send(new DeleteCommand({
    TableName: POSTS_TABLE,
    Key: { roomId, postId },
  }));
}

export interface ListPostsOptions {
  limit?: number;
  cursor?: string;
}

export async function listRoomPosts(
  roomId: string,
  options: ListPostsOptions = {},
): Promise<ListPostsResult> {
  const limit = Math.min(options.limit ?? 20, 100);
  let exclusiveStartKey: Record<string, unknown> | undefined;
  if (options.cursor) {
    try {
      exclusiveStartKey = JSON.parse(Buffer.from(options.cursor, 'base64').toString('utf8'));
    } catch {
      throw new ValidationError('Invalid cursor');
    }
  }

  const result = await docClient.send(new QueryCommand({
    TableName: POSTS_TABLE,
    KeyConditionExpression: 'roomId = :rid',
    ExpressionAttributeValues: { ':rid': roomId },
    ScanIndexForward: false,
    Limit: limit,
    ...(exclusiveStartKey ? { ExclusiveStartKey: exclusiveStartKey } : {}),
  }));

  const posts = (result.Items ?? []) as PostItem[];
  const nextCursor = result.LastEvaluatedKey
    ? Buffer.from(JSON.stringify(result.LastEvaluatedKey)).toString('base64')
    : null;

  return { posts, nextCursor };
}

export async function listUserPosts(userId: string): Promise<PostItem[]> {
  const result = await docClient.send(new QueryCommand({
    TableName: POSTS_TABLE,
    IndexName: 'authorId-postId-index',
    KeyConditionExpression: 'authorId = :uid',
    ExpressionAttributeValues: { ':uid': userId },
    ScanIndexForward: false,
  }));

  return (result.Items ?? []) as PostItem[];
}
