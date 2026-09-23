/**
 * Event Catalog — singleton for platform-api event registration and publishing.
 *
 * Declares all events this service produces (both signal and durable),
 * provides `publishSignal` for gateway-compatible Redis broadcasts,
 * and `publishDurable` for at-least-once message-queue delivery via DynamoDB outbox.
 */
import {
  EventCatalog,
  pipelineRaftDeclarations,
  pipelineRaftStateMachineDeclarations,
  pipelineEventDeclarations,
} from '@connorhoehn/event-catalog';
import { workGraphEventDeclarations } from '@connorhoehn/realtime-modules/work-graph';
import { PutCommand } from '@aws-sdk/lib-dynamodb';
import { ulid } from 'ulid';
import { createServiceRequestHeaders } from '@connorhoehn/service-runtime';
import { docClient } from './aws-clients';
import { tableName } from './ddb-table-name';
import log from './logger';
import { pendingFields } from './outbox-pending';

// ---------------------------------------------------------------------------
// Catalog instance
// ---------------------------------------------------------------------------

const REDIS_URL = `redis://${process.env.REDIS_ENDPOINT ?? 'redis'}:${process.env.REDIS_PORT ?? '6379'}`;
const MESSAGE_QUEUE_URL = process.env.MESSAGE_QUEUE_URL ?? 'http://localhost:3004';
const EVENT_CATALOG_URL = process.env.EVENT_CATALOG_URL ?? '';

export const catalog = new EventCatalog({
  redis: { url: REDIS_URL },
  messageQueue: { url: MESSAGE_QUEUE_URL,
    requestHeaders: process.env.MQ_SERVICE_AUTH_SECRET?.trim() ? createServiceRequestHeaders({
      secret: process.env.MQ_SERVICE_AUTH_SECRET, serviceId: 'platform-api', audience: 'message-queue',
    }) : undefined },
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
  name: 'doc:comment_updated',
  transport: 'signal',
  producer: 'platform-api',
  description: 'A document comment was edited; durable state is available through the comments API',
  schema: {
    type: 'object',
    properties: { documentId: { type: 'string' }, comment: { type: 'object' } },
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
  name: 'doc:work_updated',
  transport: 'signal',
  producer: 'platform-api',
  description: 'Work fields on a document changed (on doc-work:<documentId> and doc-work-scope:<scopeId>); ids and enums only — re-read GET /api/documents/:id/work',
  schema: {
    type: 'object',
    properties: {
      documentId: { type: 'string' },
      revision: { type: 'number' },
      fields: { type: 'array', items: { type: 'string' } },
      status: { type: 'string' },
      rank: { type: 'string' },
      scopeId: { type: 'string' },
    },
    required: ['documentId', 'revision', 'fields'],
  },
});

catalog.declare({
  name: 'doc:run_draft_updated',
  transport: 'signal',
  producer: 'platform-api',
  description: 'A run draft on a document changed status or content (on doc-work:<documentId>); ids and enums only',
  schema: {
    type: 'object',
    properties: {
      documentId: { type: 'string' },
      draftId: { type: 'string' },
      status: { type: 'string' },
      revision: { type: 'number' },
      runId: { type: 'string' },
    },
    required: ['documentId', 'draftId', 'status', 'revision'],
  },
});

catalog.declare({
  name: 'document.work.updated',
  transport: 'durable',
  producer: 'platform-api',
  description: 'Durable record that work fields on a document changed (for a later work-graph task projection)',
  schema: {
    type: 'object',
    properties: {
      documentId: { type: 'string' },
      organizationId: { type: 'string' },
      revision: { type: 'number' },
      fields: { type: 'array', items: { type: 'string' } },
      updatedBy: { type: 'string' },
      status: { type: 'string' },
      scopeId: { type: 'string' },
    },
    required: ['documentId', 'revision', 'fields', 'updatedBy'],
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
  name: 'doc:lifecycle_changed',
  transport: 'signal',
  producer: 'platform-api',
  description:
    'A document moved between review-lifecycle states (draft / in_review / changes_requested / approved)',
  schema: {
    type: 'object',
    properties: {
      documentId: { type: 'string' },
      from: { type: 'string' },
      to: { type: 'string' },
      action: { type: 'string' },
      actor: { type: 'string' },
      at: { type: 'string' },
      reason: { type: 'string' },
      version: { type: 'number' },
    },
    required: ['documentId', 'from', 'to', 'action', 'actor', 'at'],
  },
});

catalog.declare({
  name: 'doc:review_requested',
  transport: 'signal',
  producer: 'platform-api',
  description: 'A named review request was created or re-issued on a document or section',
  schema: {
    type: 'object',
    properties: {
      documentId: { type: 'string' },
      sectionId: { type: 'string' },
      request: { type: 'object' },
    },
    required: ['documentId', 'request'],
  },
});

catalog.declare({
  name: 'doc:review_request_updated',
  transport: 'signal',
  producer: 'platform-api',
  description: 'A named review request changed state (approved / changes_requested / withdrawn) or due date',
  schema: {
    type: 'object',
    properties: {
      documentId: { type: 'string' },
      sectionId: { type: 'string' },
      requestKey: { type: 'string' },
      state: { type: 'string' },
      request: { type: 'object' },
    },
    required: ['documentId', 'requestKey', 'request'],
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
      // triggeredBy is the PipelineRunTrigger object emitted by DC's
      // PipelineExecutor: { triggerType, userId?, payload? }. Object — not
      // string — was the source of strict-mode validation rejections.
      triggeredBy: { type: 'object', additionalProperties: true },
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
      // error is a PipelineRunError struct from DC: { nodeId, message, stack? }.
      // Object — not string — was the source of strict-mode rejections.
      error: { type: 'object', additionalProperties: true },
      failedAt: { type: 'string' },
    },
    required: ['runId'],
  },
});

// A composer edit's outcome (the `deck-revise` pipeline, src/deck/deckRevisePipeline.ts).
// The live phases ride the Redis relay only; the outcome is also durable so an
// MQ consumer learns it. Ids only — never the instruction or any slide text.
for (const [name, description] of [
  ['pipeline.deck.revise.completed', 'A composer edit of a presentation finished (revisionId null when nothing changed)'],
  ['pipeline.deck.revise.failed', 'A composer edit of a presentation failed; nothing was written'],
] as const) {
  catalog.declare({
    name,
    transport: 'durable',
    producer: 'platform-api',
    description,
    schema: {
      type: 'object',
      properties: {
        runId: { type: 'string' },
        pipelineRunId: { type: 'string' },
        documentId: { type: 'string' },
        requestId: { type: 'string' },
        userId: { type: 'string' },
        revisionId: { type: ['string', 'null'] },
        status: { type: 'number' },
        code: { type: 'string' },
      },
      required: ['documentId', 'requestId'],
    },
  });
}

// Hangout transcription pipeline — `transcription:job.queued` is the durable
// handoff from platform-api's lvsWebhooks (recording.completed receipt) to the
// transcription-worker container. Payload carries the recordingId + per-peer
// mp4 tracks[] (each with participantId + url + startedAtMs); the worker
// fetches each leg, demuxes audio via ffmpeg, runs faster-whisper, merges
// segments by participant, writes transcript.json to blob storage, then
// publishes `transcription:transcript.ready` to trigger summary generation.
//
// Long-running: per-recording processing on CPU takes minutes (3-5x real-time
// for large-v3-turbo on 4-8 vCPU). visibility_timeout on the MQ side is set
// to 3600s. dlqAfterAttempts=3 — a transcription that fails three times is
// almost certainly a corrupt source file, not a transient infra issue.
catalog.declare({
  name: 'transcription:job.queued',
  transport: 'durable',
  producer: 'platform-api',
  queue: 'transcription-job-queued',
  fifo: false,
  retention: 7 * 24 * 60 * 60, // 7 days
  dlqAfterAttempts: 3,
  description: 'A hangout recording has completed with transcribe=true; the transcription-worker should process per-peer audio tracks into a merged transcript.',
  schema: {
    type: 'object',
    properties: {
      recordingId: { type: 'string' },
      callId: { type: 'string' },
      channelArn: { type: 'string' },
      // tracks[] mirrors the shape RecordingManager writes: per-leg
      // {legId, participantId, url, kind, startedAtMs, endedAtMs}. The
      // worker uses startedAtMs as the merge anchor (clock-skew correction)
      // and participantId for speaker attribution.
      tracks: {
        type: 'array',
        items: {
          type: 'object',
          properties: {
            legId: { type: 'string' },
            participantId: { type: 'string' },
            userId: { type: ['string', 'null'] },
            url: { type: 'string' },
            kind: { type: 'string' },
            startedAtMs: { type: 'number' },
            endedAtMs: { type: 'number' },
          },
          required: ['legId', 'url', 'startedAtMs'],
        },
      },
    },
    required: ['recordingId', 'callId', 'tracks'],
  },
  tags: ['service:transcription'],
});

// `transcription:transcript.ready` — emitted by the worker after the
// merged transcript JSON has been written. Triggers the summary worker
// (platform-api consumer) to call createLLMClient + the COE-style markdown
// prompt and write summary.md alongside the transcript. Routes through a
// SEPARATE queue (transcription-transcript-ready) from job.queued —
// different consumers, must not compete for messages on the same queue.
// Queue names use the auto-derived form (colon/dot → dash) because the
// durable transport routes by name-derived queue regardless of what the
// `queue` declare field says.
catalog.declare({
  name: 'transcription:transcript.ready',
  transport: 'durable',
  producer: 'transcription-worker',
  queue: 'transcription-transcript-ready',
  fifo: false,
  retention: 7 * 24 * 60 * 60,
  dlqAfterAttempts: 3,
  description: 'A transcript.json has been written for a recording; the summary worker should generate summary.md.',
  schema: {
    type: 'object',
    properties: {
      recordingId: { type: 'string' },
      callId: { type: 'string' },
      transcriptKey: { type: 'string' },
      durationMs: { type: 'number' },
      speakerCount: { type: 'number' },
    },
    required: ['recordingId', 'transcriptKey'],
  },
  tags: ['service:transcription'],
});

// Wave A (2026-08-25) — `transcription:summary.ready` closes the loop after
// the summary consumer persists summaryMarkdown on the recording row.
// Consumed by the summary-projector (single durable consumer), which
// projects the summary's Action Items onto the call-bound document as
// section items and then fan-ins matching pipelines through
// `dispatchPlatformEvent`. Payload is deliberately light — the projector
// re-reads summaryMarkdown from the recording row by (callId, recordingId).
catalog.declare({
  name: 'transcription:summary.ready',
  transport: 'durable',
  producer: 'platform-api',
  queue: 'transcription-summary-ready',
  fifo: false,
  retention: 7 * 24 * 60 * 60,
  dlqAfterAttempts: 3,
  description: 'A meeting-summary markdown has been generated and persisted for a recording; projectors and summary-bound pipelines should run.',
  schema: {
    type: 'object',
    properties: {
      recordingId: { type: 'string' },
      callId: { type: 'string' },
      channelArn: { type: 'string' },
      lobbyName: { type: 'string' },
      summaryChars: { type: 'number' },
    },
    required: ['recordingId', 'callId'],
  },
  tags: ['service:transcription'],
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

// `call:reaction.recorded` — produced by websocket-gateway when a user
// fires an in-call emoji reaction. Declared locally so platform-api can
// `catalog.subscribe()` to the durable side and drain into the
// call-reactions DDB table (playback + transcript injection).
catalog.declare({
  name: 'call:reaction.recorded',
  transport: 'durable',
  producer: 'websocket-gateway',
  queue: 'call-reaction-recorded',
  fifo: false,
  retention: 7 * 24 * 60 * 60,
  dlqAfterAttempts: 3,
  description: 'An in-call emoji reaction fired by a participant; drained by platform-api into the call-reactions table for playback and transcript injection.',
  schema: {
    type: 'object',
    properties: {
      reactionId: { type: 'string' },
      channel: { type: 'string' },
      lobbyName: { type: 'string' },
      userId: { type: ['string', 'null'] },
      displayName: { type: 'string' },
      emoji: { type: 'string' },
      effect: { type: 'string' },
      at: { type: 'string' },
    },
    required: ['reactionId', 'channel', 'lobbyName', 'emoji', 'at'],
  },
  tags: ['service:call'],
});

// ---------------------------------------------------------------------------
// platform-api Raft state-machine signal events (wave2-pa-1 / wave9-d / wave20-c)
//
// `PipelineRaftStateMachine` (src/pipeline/PipelineRaftStateMachine.ts) is a
// per-replica RaftStateMachineBase (Node EventEmitter) that emits lifecycle
// events under the `pipeline:raft:*` namespace to disambiguate from DC's
// PipelineExecutor:
//
//   - DC executor:        pipeline:run:started  (producer: distributed-core)
//   - platform-api Raft:  pipeline:raft:run:started (producer: platform-api)
//
// Transport is `signal` (in-process). These events never cross the gateway
// bridge — they exist for in-process consumers (MultiRaft coordinator,
// postmortem aggregators, SM unit test suite).
//
// wave20-c: Register pipelineRaftStateMachineDeclarations (the 4 SM state-
// transition events: synthesized, updated, checkpointed, terminated) in
// addition to the full pipelineRaftDeclarations for the creation event.
// ---------------------------------------------------------------------------

// wave20-g — collapse the three for-loops into single declareAll() calls.
// Same semantics (`declare()` is local-only; the HTTP bulk happens later
// in `syncToRemoteRegistry`), but the intent — "register this whole
// imported list" — reads cleanly at the call site.
catalog.declareAll(pipelineRaftDeclarations);

// wave20-c — Raft SM state-transition declarations (synthesized/updated/
// checkpointed/terminated). Declared separately from pipelineRaftDeclarations
// so consumers that only care about post-create state changes can register
// from this list. The EC registry's additive compat check treats re-declarations
// of already-registered events as benign no-ops.
catalog.declareAll(pipelineRaftStateMachineDeclarations);

// wave20-c — P0+P1+P2 pipeline event declarations (from @connorhoehn/event-catalog v0.3.18).
// Covers 23 consolidated events: approval (both transports), checkpoint,
// run:cancelled/stuck/started/completed/failed/orphaned/reassigned/resumed-from-step/retry,
// step:started/completed/failed/breakpoint/context-mutations, llm:prompt/response,
// join:waiting/fired.
catalog.declareAll(pipelineEventDeclarations);

// Active now work graph: six authenticated source lifecycle streams share the
// dedicated projection queue, while projection revision notifications use a
// separate durable queue for the gateway. The consumer intentionally drains
// the declaration's queue through getDurableTransport(); this installed
// event-catalog version's high-level subscribe(eventName) derives a queue name
// and does not honor a declaration-level queue override.
// event-catalog 0.3.x's fingerprint walker treats repeated object references
// as cycles even when the declaration is valid JSON. The shared contract
// intentionally reuses schema fragments, so materialize each declaration as
// its wire-equivalent JSON value before registration.
catalog.declareAll(JSON.parse(JSON.stringify(workGraphEventDeclarations)));

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
 *   - Discriminator validation for events declaring a discriminator.
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
  const outboxId = ulid();
  await docClient.send(
    new PutCommand({
      TableName: OUTBOX_TABLE,
      Item: {
        outboxId,
        status: 'UNPROCESSED',
        ...pendingFields(outboxId),
        eventType: eventName,
        queueName: 'platform-events',
        payload: JSON.stringify(payload),
        createdAt: now,
      },
    }),
  );
}
