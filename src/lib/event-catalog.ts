/**
 * Event Catalog — singleton for platform-api event registration and publishing.
 *
 * Declares all events this service produces (both signal and durable),
 * provides `publishSignal` for gateway-compatible Redis broadcasts,
 * and `publishDurable` for at-least-once message-queue delivery via DynamoDB outbox.
 */
import {
  EventCatalog,
  EventDeclaration,
  pipelineRaftDeclarations,
  pipelineRaftStateMachineDeclarations,
  pipelineEventDeclarations,
} from '@connorhoehn/event-catalog';
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
// Declarations — exported for offline contract-test (event-catalog v0.3.32+)
// ---------------------------------------------------------------------------

// `ingest:progress` carries 10 distinct sub-shapes discriminated by payload.type.
// The actual sub-type union is IngestEvent in src/ingest/types.ts — keep in sync.
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

// Platform-api's own event declarations as a plain array. Exported so the
// offline contract-test CLI can fingerprint them without a live catalog server
// (`event-catalog snapshot --from-source --declarations=src/lib/event-catalog.ts`).
const ownDeclarations: EventDeclaration[] = [
  // --- Signal events (Redis pub/sub, ephemeral) ---
  {
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
  },
  {
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
  },
  {
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
  },
  {
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
  },
  {
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
  },
  {
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
  },
  {
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
  },
  {
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
  },
  {
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
  },
  {
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
  },
  // --- Durable events (message-queue, at-least-once) ---
  {
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
  },
  {
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
  },
  {
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
  },
  {
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
  },
  {
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
  },
  {
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
  },
  {
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
  },
  {
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
  },
  {
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
  },
  {
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
  },
  // Phase 54.6.3 — durable handoff between ingest sync (file discovery) and
  // ingest processing (mapping → materialization → pipeline trigger). FIFO with
  // partitionKey: sourceId preserves per-source ordering. Tags are embedded so
  // the consumer doesn't need an in-memory tag cache from the producer replica.
  {
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
  },
  // Phase 57b — produced by websocket-gateway (transport:'both'). Declared
  // locally so platform-api can subscribe() to the durable side and drain
  // into the user-activity DDB table.
  {
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
  },
];

// Full set: own events + imported pipeline/raft declarations.
// Exported so the offline CLI can snapshot without a live catalog server.
// wave20-c: pipelineRaftStateMachineDeclarations covers the 4 SM state-
// transition events (synthesized/updated/checkpointed/terminated).
// wave20-c: pipelineEventDeclarations covers 23 consolidated pipeline events.
export const declarations: EventDeclaration[] = [
  ...ownDeclarations,
  ...pipelineRaftDeclarations,
  ...pipelineRaftStateMachineDeclarations,
  ...pipelineEventDeclarations,
];

// Register all declarations at module load time.
declarations.forEach(d => catalog.declare(d));

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

export async function connectCatalog(): Promise<void> {
  await catalog.connect();
  log.info('[event-catalog] connected');

  // wave22-c — fingerprint drift report at boot. Compares every
  // locally-declared event's fingerprint against what the registry
  // has and logs a one-line summary plus per-event detail when any
  // are stale. Best-effort: failures degrade to a warn log so a flaky
  // catalog doesn't block boot.
  try {
    const reports = await catalog.checkAllSchemaFreshness();
    const stale = reports.filter(r => r.status === 'stale');
    const unknown = reports.filter(r => r.status === 'unknown');
    const fresh = reports.length - stale.length - unknown.length;
    log.info(
      { fresh, stale: stale.length, unknown: unknown.length, total: reports.length },
      '[event-catalog] schema-freshness report',
    );
    for (const r of stale) {
      log.warn(
        { eventName: r.eventName, localFingerprint: r.localFingerprint.slice(0, 12), remoteFingerprint: (r.remoteFingerprint ?? '').slice(0, 12) },
        '[event-catalog] schema drift detected — local declaration is out of sync with registry',
      );
    }
  } catch (err) {
    log.warn({ err: (err as Error).message }, '[event-catalog] schema-freshness check failed (non-fatal)');
  }
}

export async function disconnectCatalog(): Promise<void> {
  await catalog.disconnect();
  log.info('[event-catalog] disconnected');
}

// ---------------------------------------------------------------------------
// publishSignal — gateway-compatible broadcast (wave1-ec-3)
// ---------------------------------------------------------------------------

/**
 * Publishes a signal event to the websocket-gateway via the
 * `@connorhoehn/event-catalog` library's `EventCatalog.publishSignal()`
 * (wave1-ec-3).
 *
 * Compared to the prior direct-Redis bypass, the library hop adds:
 *   - Schema validation against the declared event (throws on bad
 *     payload); warns when the event is undeclared and continues.
 *   - Discriminator validation (used by `ingest:progress` sub-types).
 *   - Centralized envelope construction (the gateway's
 *     `channel_message` shape) + target-nodes lookup against the
 *     `websocket:channel:{channelId}:nodes` Redis set.
 *
 * Audit reference: `.agent/platform-audit-2026-05-14/event-catalog.md`
 * P0 — "Schema validation on the signal path."
 */
export async function publishSignal(
  channelId: string,
  eventName: string,
  payload: Record<string, unknown>,
): Promise<void> {
  await catalog.publishSignal(channelId, eventName, payload);
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
