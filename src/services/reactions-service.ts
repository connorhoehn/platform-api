/**
 * ReactionsService — business logic for emoji reactions to posts (REAC-05).
 *
 * Validates that the emoji is in the supported whitelist, confirms the
 * target post exists, and uses publishWithOutbox for the create path so
 * "already reacted" becomes ConflictError with the existing user-facing
 * message. Delete path looks up + deletes, 404s if absent.
 */
import {
  GetCommand,
  DeleteCommand,
} from '@aws-sdk/lib-dynamodb';
import { docClient } from '../lib/aws-clients';
import { broadcastToRoom } from './room-broadcaster';
import { publishWithOutbox } from './outbox-publisher';
import {
  ValidationError,
  NotFoundError,
} from '../middleware/error-handler';
import { tableName } from '../lib/ddb-table-name';

const LIKES_TABLE = tableName('social-likes');
const POSTS_TABLE = tableName('social-posts');

export const VALID_EMOJI = new Set([
  '\u2764\uFE0F',     // heart
  '\uD83D\uDE02',     // joy
  '\uD83D\uDC4D',     // thumbs up
  '\uD83D\uDC4E',     // thumbs down
  '\uD83D\uDE2E',     // open-mouth
  '\uD83D\uDE22',     // cry
  '\uD83D\uDE21',     // angry
  '\uD83C\uDF89',     // party
  '\uD83D\uDD25',     // fire
  '\u26A1',           // bolt
  '\uD83D\uDCAF',     // 100
  '\uD83D\uDE80',     // rocket
]);

export interface ReactionRecord {
  targetId: string;
  userId: string;
  type: 'reaction';
  emoji: string;
  createdAt: string;
}

export async function addReaction(
  roomId: string,
  postId: string,
  userId: string,
  emoji: string | undefined,
): Promise<ReactionRecord> {
  if (!emoji || !VALID_EMOJI.has(emoji)) {
    throw new ValidationError('Invalid emoji. Must be one of the 12 supported types');
  }

  const postResult = await docClient.send(new GetCommand({
    TableName: POSTS_TABLE,
    Key: { roomId, postId },
  }));
  if (!postResult.Item) {
    throw new NotFoundError('Post not found');
  }

  const targetId = `post:${postId}:reaction`;
  const createdAt = new Date().toISOString();

  await publishWithOutbox({
    target: {
      TableName: LIKES_TABLE,
      Item: { targetId, userId, type: 'reaction', emoji, createdAt },
      ConditionExpression: 'attribute_not_exists(#uid)',
      ExpressionAttributeNames: { '#uid': 'userId' },
    },
    eventType: 'social.reaction',
    queueName: 'social-reactions',
    eventPayload: { targetId, userId, roomId, postId, emoji },
    conflictMessage: 'Already reacted. Delete your existing reaction first.',
  });

  await broadcastToRoom(roomId, 'social:like', {
    targetId, userId, type: 'reaction', emoji, createdAt,
  });

  return { targetId, userId, type: 'reaction', emoji, createdAt };
}

export async function removeReaction(
  postId: string,
  userId: string,
  emoji: string,
): Promise<void> {
  if (!VALID_EMOJI.has(emoji)) {
    throw new ValidationError('Invalid emoji');
  }

  const targetId = `post:${postId}:reaction`;

  const reactionResult = await docClient.send(new GetCommand({
    TableName: LIKES_TABLE,
    Key: { targetId, userId },
  }));
  if (!reactionResult.Item) {
    throw new NotFoundError('Reaction not found');
  }

  await docClient.send(new DeleteCommand({
    TableName: LIKES_TABLE,
    Key: { targetId, userId },
  }));
}
