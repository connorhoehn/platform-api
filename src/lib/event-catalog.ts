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

// `ingest:progress` is an envelope event carrying 10 distinct sub-shapes
// discriminated by `payload.type`. The actual sub-type union is
// IngestEvent in src/ingest/types.ts — keep this list in sync.
// Phase P3.2 — exercises event-catalog Phase E3.6 discriminator validation.
export const INGEST_PROGRESS_DISCRIMINATOR_VALUES = [
  'sync.started',
  'sync.file.fetched',
  'item.status',
  'item.mapped',
  'item.materialized',
  'item.pipeline.spawned',
  'item.failed',
  'item.review.triggered',
  'sync.completed',
  'source.status',
] as const;

catalog.declare({
  name: 'ingest:progress',
  transport: 'signal',
  producer: 'platform-api',
  description: 'Ingest pipeline progress event — envelope carrying 10 sub-shapes discriminated by payload.type.',
  schema: {
    type: 'object',
    properties: {
      type: { type: 'string', enum: [...INGEST_PROGRESS_DISCRIMINATOR_VALUES] },
    },
    required: ['type'],
  },
  discriminator: {
    field: 'type',
    values: [...INGEST_PROGRESS_DISCRIMINATOR_VALUES],
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

// Phase 54.6.3 — `ingest:item.queued` is the durable handoff between the
// ingest sync stage (file discovery) and the ingest processing stage
// (mapping → materialization → pipeline trigger). The previous in-process
// partition queue could only fan out within a single replica; the durable
// queue lets any platform-api replica pick the item up. FIFO with
// `partitionKey: sourceId` preserves per-source ordering through the queue.
//
// Tags are embedded in the payload so the consumer doesn't need an in-memory
// tag cache from the producer replica.
catalog.declare({
  name: 'ingest:item.queued',
  transport: 'durable',
  producer: 'platform-api',
  queue: 'ingest-items',
  fifo: true,
  partitionKey: (payload: unknown) => {
    const p = payload as { sourceId?: string };
    return p.sourceId ?? 'unknown';
  },
  retention: 7 * 24 * 60 * 60, // 7 days
  dlqAfterAttempts: 3,
  description: 'A newly synced ingest item is ready to be processed (mapped, materialized, pipeline-triggered). Replaces the in-memory partition queue.',
  schema: {
    type: 'object',
    properties: {
      ingestId: { type: 'string' },
      sourceId: { type: 'string' },
      tags: { type: 'array', items: { type: 'string' } },
    },
    required: ['ingestId', 'sourceId'],
  },
  tags: ['service:ingest', 'phase:54.6.3'],
});

// Phase 57b — `activity.recorded` is produced by websocket-gateway as
// transport:'both' (declared in gateway-event-declarations.js). We declare
// it locally so platform-api can `catalog.subscribe()` to the durable side
// and drain into the user-activity table.
catalog.declare({
  name: 'activity.recorded',
  transport: 'both',
  producer: 'websocket-gateway',
  description: 'User-initiated activity feed event consumed by platform-api into the user-activity DDB table.',
  schema: {
    type: 'object',
    properties: {
      eventType: { type: 'string' },
      detail: { type: 'object', additionalProperties: true },
      timestamp: { type: 'string' },
      userId: { type: ['string', 'null'] },
      displayName: { type: 'string' },
    },
    required: ['eventType', 'timestamp'],
    additionalProperties: true,
  },
  tags: ['service:activity', 'phase:57'],
});

// ---------------------------------------------------------------------------
// distributed-core signal-transport pipeline events (Phase P3.3)
//
// Declared by platform-api on distributed-core's behalf because DC is a
// library, not a service — it cannot register itself with the central
// catalog. The events flow over DC's in-process EventBus inside this
// service and are bridged to gateway WS clients via the pipeline-bridge
// service.
//
// IMPORTANT: these are SEPARATE events from the durable `pipeline.run.*`
// declarations above, despite the name-string overlap on the deprecated
// dot-form alias. Different transports, different schemas:
//   - colon-form (here):  in-process bus; payload uses `at`
//   - dot-form (above):   durable queue; payload uses `completedAt`/`failedAt`/`startedAt`
//
// PipelineExecutor.ts also dual-emits a deprecated dot-form alias on the
// same in-process bus (e.g. `pipeline.run.completed` alongside the
// canonical colon-form). The gateway's pipeline-bridge maintains a
// DEPRECATED_DUAL_EMIT_ALIASES Set to suppress the dot-form half — that
// hardcoded list must stay in sync with distributed-core's
// CANONICAL_TO_DEPRECATED map in PipelineExecutor.ts:85.
// ---------------------------------------------------------------------------

catalog.declare({
  name: 'pipeline:run:started',
  transport: 'signal',
  producer: 'distributed-core',
  description:
    'In-process EventBus event from PipelineExecutor when a run starts. '
    + 'Bridged to gateway WS clients via pipeline:event frames. The platform-api '
    + 'durable counterpart `pipeline.run.started` (declared separately above) '
    + 'subscribes to this and re-publishes to message-queue with a renamed '
    + '`startedAt` field instead of `at`.',
  tags: ['producer:distributed-core', 'pipeline'],
  schema: {
    type: 'object',
    properties: {
      runId: { type: 'string' },
      pipelineId: { type: 'string' },
      triggeredBy: { type: 'object', additionalProperties: true },
      at: { type: 'string', description: 'ISO-8601 timestamp' },
    },
    required: ['runId', 'pipelineId', 'at'],
  },
});

catalog.declare({
  name: 'pipeline:run:completed',
  transport: 'signal',
  producer: 'distributed-core',
  description:
    'In-process EventBus event from PipelineExecutor when a run completes. '
    + 'Bridged to gateway WS clients via pipeline:event frames. The platform-api '
    + 'durable counterpart `pipeline.run.completed` (declared separately above) '
    + 'subscribes to this and re-publishes to message-queue with `completedAt` instead of `at`.',
  tags: ['producer:distributed-core', 'pipeline'],
  schema: {
    type: 'object',
    properties: {
      runId: { type: 'string' },
      durationMs: { type: 'number' },
      at: { type: 'string', description: 'ISO-8601 timestamp' },
    },
    required: ['runId', 'durationMs', 'at'],
  },
});

catalog.declare({
  name: 'pipeline:run:failed',
  transport: 'signal',
  producer: 'distributed-core',
  description:
    'In-process EventBus event from PipelineExecutor when a run fails. '
    + 'Bridged to gateway WS clients via pipeline:event frames. The platform-api '
    + 'durable counterpart `pipeline.run.failed` (declared separately above) '
    + 'subscribes to this and re-publishes to message-queue with `failedAt` instead of `at`.',
  tags: ['producer:distributed-core', 'pipeline'],
  schema: {
    type: 'object',
    properties: {
      runId: { type: 'string' },
      error: {
        type: 'object',
        properties: {
          nodeId: { type: 'string' },
          message: { type: 'string' },
        },
        required: ['message'],
      },
      at: { type: 'string', description: 'ISO-8601 timestamp' },
    },
    required: ['runId', 'error', 'at'],
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
