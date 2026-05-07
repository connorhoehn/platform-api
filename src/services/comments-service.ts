/**
 * CommentsService — business logic for comments on posts (CONT-06/07/08).
 *
 * Owns validation, parent-entity existence checks, authorization for
 * delete, and broadcast + EventBridge fan-out. Routes stay thin and
 * delegate all of this to the exported functions below.
 */
import { ulid } from 'ulid';
import {
  GetCommand,
  PutCommand,
  DeleteCommand,
  QueryCommand,
} from '@aws-sdk/lib-dynamodb';
import { docClient, publishSocialEvent } from '../lib/aws-clients';
import { broadcastToRoom } from './room-broadcaster';
import {
  ValidationError,
  NotFoundError,
  ForbiddenError,
} from '../middleware/error-handler';
import { tableName } from '../lib/ddb-table-name';

const COMMENTS_TABLE = tableName('social-comments');
const POSTS_TABLE = tableName('social-posts');

export interface CommentItem {
  postId: string;
  commentId: string;
  authorId: string;
  content: string;
  parentCommentId?: string;
  createdAt: string;
}

export interface CreateCommentInput {
  content?: string;
  parentCommentId?: string;
}

function validateContent(content: string | undefined): string {
  const trimmed = (content ?? '').trim();
  if (!trimmed || trimmed.length > 10000) {
    throw new ValidationError('content is required (max 10000 chars)');
  }
  return trimmed;
}

export async function createComment(
  roomId: string,
  postId: string,
  authorId: string,
  input: CreateCommentInput,
): Promise<CommentItem> {
  const trimmedContent = validateContent(input.content);
  const parentCommentId = input.parentCommentId;

  const postResult = await docClient.send(new GetCommand({
    TableName: POSTS_TABLE,
    Key: { roomId, postId },
  }));
  if (!postResult.Item) {
    throw new NotFoundError('Post not found');
  }

  if (parentCommentId) {
    const parentResult = await docClient.send(new GetCommand({
      TableName: COMMENTS_TABLE,
      Key: { postId, commentId: parentCommentId },
    }));
    if (!parentResult.Item) {
      throw new NotFoundError('Parent comment not found');
    }
  }

  const commentId = ulid();
  const now = new Date().toISOString();

  const item: CommentItem = {
    postId,
    commentId,
    authorId,
    content: trimmedContent,
    createdAt: now,
    ...(parentCommentId ? { parentCommentId } : {}),
  };

  await docClient.send(new PutCommand({
    TableName: COMMENTS_TABLE,
    Item: item,
  }));

  await broadcastToRoom(roomId, 'social:comment', {
    roomId, postId, commentId, authorId, content: trimmedContent,
    ...(parentCommentId ? { parentCommentId } : {}),
    createdAt: now,
  });

  void publishSocialEvent('social.comment.created', {
    roomId,
    postId,
    commentId,
    authorId,
  });

  return item;
}

export async function listComments(
  roomId: string,
  postId: string,
): Promise<CommentItem[]> {
  const postResult = await docClient.send(new GetCommand({
    TableName: POSTS_TABLE,
    Key: { roomId, postId },
  }));
  if (!postResult.Item) {
    throw new NotFoundError('Post not found');
  }

  const result = await docClient.send(new QueryCommand({
    TableName: COMMENTS_TABLE,
    KeyConditionExpression: 'postId = :pid',
    ExpressionAttributeValues: { ':pid': postId },
    ScanIndexForward: false,
  }));

  return (result.Items ?? []) as CommentItem[];
}

export async function deleteComment(
  postId: string,
  commentId: string,
  callerId: string,
): Promise<void> {
  const result = await docClient.send(new GetCommand({
    TableName: COMMENTS_TABLE,
    Key: { postId, commentId },
  }));
  if (!result.Item) {
    throw new NotFoundError('Comment not found');
  }
  if (result.Item['authorId'] !== callerId) {
    throw new ForbiddenError('You can only delete your own comments');
  }

  await docClient.send(new DeleteCommand({
    TableName: COMMENTS_TABLE,
    Key: { postId, commentId },
  }));
}
