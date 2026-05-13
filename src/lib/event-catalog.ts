/**
 * Event Catalog — singleton for platform-api event registration and publishing.
 *
 * Declares all events this service produces (both signal and durable),
 * provides `publishSignal` for gateway-compatible Redis broadcasts,
 * and `publishDurable` for at-least-once message-queue delivery via DynamoDB outbox.
 */
import {
  EventCatalog,
  buildChannelMessageEnvelope,
  buildSignalMessage,
  channelMessageRedisChannel,
  channelNodesRedisKey,
} from '@connorhoehn/event-catalog';
import { createClient, RedisClientType } from 'redis';
import { PutCommand } from '@aws-sdk/lib-dynamodb';
import { ulid } from 'ulid';
import { docClient } from './aws-clients';
import { tableName } from './ddb-table-name';
import log from './logger';

// ---------------------------------------------------------------------------
// Catalog instance
// ---------------------------------------------------------------------------

const REDIS_URL = `redis://${process.env.REDIS_ENDPOINT ?? 'redis'}:${process.env.REDIS_PORT ?? '6379'}`;
const MESSAGE_QUEUE_URL = process.env.MESSAGE_QUEUE_URL ?? 'http://localhost:3004';
const EVENT_CATALOG_URL = process.env.EVENT_CATALOG_URL ?? '';

export const catalog = new EventCatalog({
  redis: { url: REDIS_URL },
  messageQueue: { url: MESSAGE_QUEUE_URL },
  serviceName: 'platform-api',
  registryUrl: EVENT_CATALOG_URL || undefined,
});

const OUTBOX_TABLE = tableName('social-outbox');

// ---------------------------------------------------------------------------
// Signal event declarations (Redis pub/sub, ephemeral)
// ---------------------------------------------------------------------------

catalog.declare({
  name: 'social:post',
  transport: 'signal',
  producer: 'platform-api',
  description: 'A new post was created in a social room',
  schema: {
    type: 'object',
    properties: {
      roomId: { type: 'string' },
      postId: { type: 'string' },
      authorId: { type: 'string' },
      content: { type: 'string' },
      createdAt: { type: 'string' },
    },
    required: ['roomId', 'postId', 'authorId', 'content', 'createdAt'],
  },
});

catalog.declare({
  name: 'social:comment',
  transport: 'signal',
  producer: 'platform-api',
  description: 'A comment was added to a post',
  schema: {
    type: 'object',
    properties: {
      roomId: { type: 'string' },
      postId: { type: 'string' },
      commentId: { type: 'string' },
      authorId: { type: 'string' },
      content: { type: 'string' },
      createdAt: { type: 'string' },
      parentCommentId: { type: 'string' },
    },
    required: ['roomId', 'postId', 'commentId', 'authorId', 'content', 'createdAt'],
  },
});

catalog.declare({
  name: 'social:like',
  transport: 'signal',
  producer: 'platform-api',
  description: 'A like/reaction was added',
  schema: {
    type: 'object',
    properties: {
      targetId: { type: 'string' },
      userId: { type: 'string' },
      type: { type: 'string' },
      createdAt: { type: 'string' },
      emoji: { type: 'string' },
    },
    required: ['targetId', 'userId', 'type', 'createdAt'],
  },
});

catalog.declare({
  name: 'social:member_joined',
  transport: 'signal',
  producer: 'platform-api',
  description: 'A member joined a social room',
  schema: {
    type: 'object',
    properties: {
      roomId: { type: 'string' },
      userId: { type: 'string' },
      joinedAt: { type: 'string' },
    },
    required: ['roomId', 'userId', 'joinedAt'],
  },
});

catalog.declare({
  name: 'social:member_left',
  transport: 'signal',
  producer: 'platform-api',
  description: 'A member left a social room',
  schema: {
    type: 'object',
    properties: {
      roomId: { type: 'string' },
      userId: { type: 'string' },
      leftAt: { type: 'string' },
    },
    required: ['roomId', 'userId', 'leftAt'],
  },
});

catalog.declare({
  name: 'doc:comment_added',
  transport: 'signal',
  producer: 'platform-api',
  description: 'A comment was added to a document',
  schema: {
    type: 'object',
    properties: {
      documentId: { type: 'string' },
      comment: { type: 'object' },
    },
    required: ['documentId', 'comment'],
  },
});

catalog.declare({
  name: 'doc:comment_resolved',
  transport: 'signal',
  producer: 'platform-api',
  description: 'A document comment was resolved or unresolved',
  schema: {
    type: 'object',
    properties: {
      documentId: { type: 'string' },
      commentId: { type: 'string' },
      resolved: { type: 'boolean' },
      resolvedBy: { type: 'string' },
      resolvedAt: { type: 'string' },
    },
    required: ['documentId', 'commentId', 'resolved'],
  },
});

catalog.declare({
  name: 'doc:comment_deleted',
  transport: 'signal',
  producer: 'platform-api',
  description: 'A document comment was deleted',
  schema: {
    type: 'object',
    properties: {
      documentId: { type: 'string' },
      commentId: { type: 'string' },
    },
    required: ['documentId', 'commentId'],
  },
});

catalog.declare({
  name: 'doc:section_updated',
  transport: 'signal',
  producer: 'platform-api',
  description: 'A document section was updated',
  schema: {
    type: 'object',
    properties: {
      documentId: { type: 'string' },
      sectionId: { type: 'string' },
      action: { type: 'string' },
    },
    required: ['documentId', 'sectionId', 'action'],
  },
});

catalog.declare({
  name: 'ingest:progress',
  transport: 'signal',
  producer: 'platform-api',
  description: 'Ingest pipeline progress event (various shapes with type discriminator)',
  schema: {
    type: 'object',
    properties: {
      type: { type: 'string' },
    },
    required: ['type'],
  },
});

// ---------------------------------------------------------------------------
// Durable event declarations (message-queue, at-least-once)
// ---------------------------------------------------------------------------

catalog.declare({
  name: 'social.post.created',
  transport: 'durable',
  producer: 'platform-api',
  description: 'Durable notification that a post was created',
  schema: {
    type: 'object',
    properties: {
      roomId: { type: 'string' },
      postId: { type: 'string' },
      authorId: { type: 'string' },
    },
    required: ['roomId', 'postId', 'authorId'],
  },
});

catalog.declare({
  name: 'social.comment.created',
  transport: 'durable',
  producer: 'platform-api',
  description: 'Durable notification that a comment was created',
  schema: {
    type: 'object',
    properties: {
      roomId: { type: 'string' },
      postId: { type: 'string' },
      commentId: { type: 'string' },
      authorId: { type: 'string' },
    },
    required: ['roomId', 'postId', 'commentId', 'authorId'],
  },
});

catalog.declare({
  name: 'social.reaction',
  transport: 'durable',
  producer: 'platform-api',
  description: 'Durable reaction event',
  schema: {
    type: 'object',
    properties: {
      targetId: { type: 'string' },
      userId: { type: 'string' },
      roomId: { type: 'string' },
      postId: { type: 'string' },
      emoji: { type: 'string' },
    },
    required: ['targetId', 'userId', 'roomId', 'postId', 'emoji'],
  },
});

catalog.declare({
  name: 'social.like',
  transport: 'durable',
  producer: 'platform-api',
  description: 'Durable like event',
  schema: {
    type: 'object',
    properties: {
      targetId: { type: 'string' },
      userId: { type: 'string' },
      roomId: { type: 'string' },
      postId: { type: 'string' },
      commentId: { type: 'string' },
    },
    required: ['targetId', 'userId', 'roomId'],
  },
});

catalog.declare({
  name: 'social.room.join',
  transport: 'durable',
  producer: 'platform-api',
  description: 'Durable room join event',
  schema: {
    type: 'object',
    properties: {
      roomId: { type: 'string' },
      userId: { type: 'string' },
      timestamp: { type: 'string' },
    },
    required: ['roomId', 'userId', 'timestamp'],
  },
});

catalog.declare({
  name: 'social.room.leave',
  transport: 'durable',
  producer: 'platform-api',
  description: 'Durable room leave event',
  schema: {
    type: 'object',
    properties: {
      roomId: { type: 'string' },
      userId: { type: 'string' },
    },
    required: ['roomId', 'userId'],
  },
});

catalog.declare({
  name: 'social.unfollow',
  transport: 'durable',
  producer: 'platform-api',
  description: 'Durable unfollow event',
  schema: {
    type: 'object',
    properties: {
      followerId: { type: 'string' },
      targetUserId: { type: 'string' },
    },
    required: ['followerId', 'targetUserId'],
  },
});

catalog.declare({
  name: 'pipeline.run.started',
  transport: 'durable',
  producer: 'platform-api',
  description: 'A pipeline run was started',
  schema: {
    type: 'object',
    properties: {
      runId: { type: 'string' },
      pipelineId: { type: 'string' },
      triggeredBy: { type: 'string' },
      startedAt: { type: 'string' },
    },
    required: ['runId', 'pipelineId'],
  },
});

catalog.declare({
  name: 'pipeline.run.completed',
  transport: 'durable',
  producer: 'platform-api',
  description: 'A pipeline run completed successfully',
  schema: {
    type: 'object',
    properties: {
      runId: { type: 'string' },
      pipelineId: { type: 'string' },
      durationMs: { type: 'number' },
      completedAt: { type: 'string' },
    },
    required: ['runId'],
  },
});

catalog.declare({
  name: 'pipeline.run.failed',
  transport: 'durable',
  producer: 'platform-api',
  description: 'A pipeline run failed',
  schema: {
    type: 'object',
    properties: {
      runId: { type: 'string' },
      pipelineId: { type: 'string' },
      error: { type: 'string' },
      failedAt: { type: 'string' },
    },
    required: ['runId'],
  },
});

// ---------------------------------------------------------------------------
// Gateway-compatible Redis broadcast client
// ---------------------------------------------------------------------------

let broadcastClient: RedisClientType | null = null;

async function getBroadcastClient(): Promise<RedisClientType> {
  if (broadcastClient?.isReady) return broadcastClient;

  const client = createClient({ url: REDIS_URL }) as RedisClientType;
  client.on('error', (err: Error) => {
    console.warn('[event-catalog] broadcast Redis error:', err.message);
  });
  await client.connect();
  broadcastClient = client;
  return broadcastClient;
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

export async function connectCatalog(): Promise<void> {
  await catalog.connect();
  await getBroadcastClient();
  console.log('[event-catalog] connected');
}

export async function disconnectCatalog(): Promise<void> {
  await catalog.disconnect();
  if (broadcastClient?.isReady) {
    await broadcastClient.disconnect();
    broadcastClient = null;
  }
  console.log('[event-catalog] disconnected');
}

// ---------------------------------------------------------------------------
// publishSignal — gateway-compatible broadcast
// ---------------------------------------------------------------------------

/**
 * Publishes a signal event to the websocket-gateway via Redis pub/sub
 * using the gateway's expected envelope format.
 *
 * The gateway listens on `websocket:route:{channelId}` and expects:
 * {
 *   type: "channel_message",
 *   channel: channelId,
 *   message: { type: eventName, channel: channelId, payload, timestamp },
 *   excludeClientId: null,
 *   fromNode: "platform-api",
 *   seq: 0,
 *   timestamp: iso,
 *   targetNodes: [...]
 * }
 */
export async function publishSignal(
  channelId: string,
  eventName: string,
  payload: Record<string, unknown>,
): Promise<void> {
  const client = await getBroadcastClient();

  // Look up which gateway nodes are subscribed to this channel
  const targetNodes = await client.sMembers(channelNodesRedisKey(channelId));
  if (targetNodes.length === 0) return; // No listeners, skip

  const now = new Date().toISOString();
  const envelope = buildChannelMessageEnvelope({
    channel: channelId,
    message: buildSignalMessage({
      eventName,
      channel: channelId,
      payload,
      timestamp: now,
    }),
    fromNode: 'platform-api',
    timestamp: now,
    targetNodes,
  });

  await client.publish(channelMessageRedisChannel(channelId), JSON.stringify(envelope));
}

// ---------------------------------------------------------------------------
// publishDurable — writes to DynamoDB outbox for at-least-once delivery
// ---------------------------------------------------------------------------

/**
 * Writes a durable event to the outbox table. The outbox-drainer picks it up
 * and publishes via the message-queue transport, guaranteeing at-least-once
 * delivery even if the message-queue is temporarily unavailable.
 */
export async function publishDurable(
  eventName: string,
  payload: Record<string, unknown>,
): Promise<void> {
  const now = new Date().toISOString();
  await docClient.send(
    new PutCommand({
      TableName: OUTBOX_TABLE,
      Item: {
        outboxId: ulid(),
        status: 'UNPROCESSED',
        eventType: eventName,
        queueName: 'platform-events',
        payload: JSON.stringify(payload),
        createdAt: now,
      },
    }),
  );
}
