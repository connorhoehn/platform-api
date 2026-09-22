import {
  DynamoDBDocumentClient,
  GetCommand,
  QueryCommand,
  TransactWriteCommand,
  type TransactWriteCommandInput,
} from '@aws-sdk/lib-dynamodb';
import { intervalOverlapsWorkDay, workDayWindow } from '@connorhoehn/realtime-modules/work-graph';
import { tableName } from '../lib/ddb-table-name';

/**
 * These dependency-free shapes intentionally mirror realtime-modules'
 * frozen work-graph contract. The platform service injects that package's
 * pure projectWorkEvent reducer at composition time; keeping the repository
 * structural lets this branch be verified before the private package is
 * published and pinned by this consumer.
 */
export type WorkSourceKind =
  | 'cloud-compute'
  | 'local-compute'
  | 'document'
  | 'pipeline'
  | 'conversation'
  | 'meeting';

export type WorkNodeKind =
  | 'project'
  | 'terminal'
  | 'agent'
  | 'run'
  | 'document'
  | 'change'
  | 'conversation'
  | 'meeting'
  | 'transcript'
  | 'task';

export type WorkRelation =
  | 'works-on'
  | 'runs-in'
  | 'operates-on'
  | 'produced'
  | 'edited'
  | 'attended'
  | 'discussed'
  | 'derived-from';

export type WorkStatus =
  | 'idle'
  | 'running'
  | 'waiting'
  | 'completed'
  | 'error'
  | 'stopped'
  | 'stale';

export interface WorkSourceRef {
  source: WorkSourceKind;
  resourceId: string;
  resourceVersion?: string;
}

export type WorkSourcePayload =
  | {
    kind: 'cloud-compute';
    lifecycle: 'created' | 'started' | 'waiting' | 'completed' | 'stopped' | 'failed' | 'deleted';
    boxId: string;
    jobId?: string;
    agentId?: string;
    projectId?: string;
    attempt?: number;
    safeLabel?: string;
  }
  | {
    kind: 'local-compute';
    lifecycle: 'session-started' | 'session-ended' | 'job-started' | 'job-finished' | 'heartbeat' | 'disconnected';
    machineId: string;
    sessionId?: string;
    jobId?: string;
    projectId?: string;
    heartbeatAt?: string;
    safeLabel?: string;
  }
  | {
    kind: 'document';
    lifecycle: 'revision-saved' | 'deleted';
    documentId: string;
    revisionId: string;
    attributedActorId?: string;
    producedByRunId?: string;
    safeLabel?: string;
  }
  | {
    kind: 'pipeline';
    lifecycle: 'started' | 'waiting' | 'completed' | 'failed' | 'cancelled';
    pipelineId: string;
    runId: string;
    attempt: number;
    artifactIds?: string[];
    safeLabel?: string;
  }
  | {
    kind: 'conversation';
    lifecycle: 'contributed' | 'membership-added' | 'membership-removed';
    conversationId: string;
    conversationKind: 'dm' | 'channel';
    explicitRelatedResource?: WorkSourceRef;
    safeLabel?: string;
  }
  | {
    kind: 'meeting';
    lifecycle:
      | 'attendance-started'
      | 'attendance-ended'
      | 'recording-started'
      | 'recording-ready'
      | 'recording-failed'
      | 'recording-deleted'
      | 'transcript-ready'
      | 'transcript-failed'
      | 'transcript-deleted';
    meetingId: string;
    recordingId?: string;
    transcriptId?: string;
    recordingOwnerId?: string;
    safeLabel?: string;
  };

export interface AuthenticatedWorkEvent {
  schemaVersion: 1;
  eventId: string;
  idempotencyKey: string;
  source: WorkSourceKind;
  sourceSequence?: string;
  occurredAt: string;
  receivedAt: string;
  actor: {
    actorId: string;
    organizationId: string;
    sourceSubject: string;
  };
  producer: {
    serviceId: string;
    organizationId: string;
    credentialId: string;
  };
  resource: WorkSourceRef;
  payload: WorkSourcePayload;
}

export interface StoredWorkSession {
  id: string;
  organizationId: string;
  actorId: string;
  source: WorkSourceKind;
  timezone: string;
  startedAt: string;
  updatedAt: string;
  endedAt?: string;
  revision: number;
}

export interface StoredWorkNode {
  id: string;
  organizationId: string;
  actorId: string;
  sessionId?: string;
  kind: WorkNodeKind;
  sourceRef: WorkSourceRef;
  policyRef: string;
  title: string;
  description?: string;
  status: WorkStatus;
  startedAt?: string;
  updatedAt: string;
  endedAt?: string;
  sourceEventId: string;
  sourceSequence?: string;
  deletedAt?: string;
  revision: number;
}

export interface StoredWorkEdge {
  id: string;
  organizationId: string;
  actorId: string;
  fromId: string;
  toId: string;
  relation: WorkRelation;
  policyRef: string;
  label?: string;
  status: WorkStatus;
  startedAt?: string;
  updatedAt: string;
  endedAt?: string;
  sourceEventId: string;
  provenance: 'source-event' | 'user-confirmed';
  deletedAt?: string;
  revision: number;
}

export interface WorkProjectionState {
  schemaVersion: 1;
  organizationId: string;
  actorId: string;
  revision: number;
  sessions: Record<string, StoredWorkSession>;
  nodes: Record<string, StoredWorkNode>;
  edges: Record<string, StoredWorkEdge>;
  sourceCheckpoints: Partial<Record<WorkSourceKind, string>>;
  appliedEventIds: string[];
}

export type WorkGraphReducer = (
  current: WorkProjectionState,
  event: AuthenticatedWorkEvent,
) => WorkProjectionState;

const WORK_GRAPH_TABLE = tableName('work-graph');
const EVENT_RETENTION_SECONDS = 30 * 24 * 60 * 60;
const DELTA_RETENTION_SECONDS = 30 * 60;
const MAX_APPLIED_EVENT_IDS = 1_000;
const MAX_TRANSACTION_ITEMS = 100;
const MAX_FENCED_READ_ATTEMPTS = 3;
export const WORK_GRAPH_TARGET_LOOKUP_INDEX = 'work-graph-target-lookup';
/** How many days of one target's lookup rows a locate reads before giving up. */
const MAX_TARGET_LOOKUP_DAYS = 32;

type ItemKind = 'meta' | 'session' | 'node' | 'edge' | 'event' | 'delta' | 'outbox';

interface StoredItem extends Record<string, unknown> {
  PK: string;
  SK: string;
  itemKind: ItemKind;
}

interface MetaItem extends StoredItem {
  itemKind: 'meta';
  schemaVersion: 1;
  organizationId: string;
  actorId: string;
  day: string;
  revision: number;
  sourceCheckpoints: Partial<Record<WorkSourceKind, string>>;
  appliedEventIds: string[];
  erasing?: boolean;
  eraseExpectedRevision?: number;
}

export type StoredWorkGraphDeltaOperation =
  | { kind: 'upsert-node'; node: StoredWorkNode }
  | { kind: 'remove-node'; nodeId: string }
  | { kind: 'upsert-edge'; edge: StoredWorkEdge }
  | { kind: 'remove-edge'; edgeId: string };

export interface StoredWorkGraphDeltaBatch {
  previousWatermark: number;
  watermark: number;
  committedAt: string;
  operations: StoredWorkGraphDeltaOperation[];
}

/** A caller made a decision against a projection revision that is no longer current. */
export class WorkGraphConcurrencyError extends Error {
  constructor(
    readonly expectedRevision: number,
    message = `Work graph projection is no longer at revision ${expectedRevision}`,
  ) {
    super(message);
    this.name = 'WorkGraphConcurrencyError';
  }
}

/** The stored partition did not match its organization/actor isolation boundary. */
export class WorkGraphDataIntegrityError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'WorkGraphDataIntegrityError';
  }
}

/** DynamoDB cannot atomically commit more than 100 operations. */
export class WorkGraphTransactionTooLargeError extends Error {
  constructor(readonly itemCount: number) {
    super(`Work graph event requires ${itemCount} atomic writes; DynamoDB allows 100`);
    this.name = 'WorkGraphTransactionTooLargeError';
  }
}

function partitionKey(organizationId: string, actorId: string, day: string): string {
  if (!organizationId || !actorId || !isCalendarDate(day)) {
    throw new RangeError('organizationId, actorId, and a valid UTC calendar day are required');
  }
  return `WG#${organizationId}#${actorId}#${day}`;
}

function isCalendarDate(value: string): boolean {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return false;
  const parsed = new Date(`${value}T00:00:00.000Z`);
  return !Number.isNaN(parsed.valueOf()) && parsed.toISOString().slice(0, 10) === value;
}

function utcDay(timestamp: string): string {
  const parsed = new Date(timestamp);
  if (Number.isNaN(parsed.valueOf())) throw new RangeError('event occurredAt must be an ISO timestamp');
  return parsed.toISOString().slice(0, 10);
}

function eventSortKey(event: Pick<AuthenticatedWorkEvent, 'source' | 'eventId'>): string {
  return `EVENT#${event.source}#${event.eventId}`;
}

function withoutStorageFields<T extends object>(item: StoredItem): T {
  const {
    PK: _pk,
    SK: _sk,
    itemKind: _kind,
    day: _day,
    targetLookupPk: _targetLookupPk,
    targetLookupSk: _targetLookupSk,
    targetActorId: _targetActorId,
    ...value
  } = item;
  return value as unknown as T;
}

function sameValue(left: unknown, right: unknown): boolean {
  return JSON.stringify(left) === JSON.stringify(right);
}

function isConditionalFailure(error: unknown): boolean {
  const candidate = error as {
    name?: string;
    CancellationReasons?: Array<{ Code?: string }>;
  };
  return candidate?.name === 'ConditionalCheckFailedException'
    || (candidate?.name === 'TransactionCanceledException'
      && candidate.CancellationReasons?.some((reason) => reason.Code === 'ConditionalCheckFailed') === true);
}

function initialProjection(event: AuthenticatedWorkEvent): WorkProjectionState {
  return {
    schemaVersion: 1,
    organizationId: event.actor.organizationId,
    actorId: event.actor.actorId,
    revision: 0,
    sessions: {},
    nodes: {},
    edges: {},
    sourceCheckpoints: {},
    appliedEventIds: [],
  };
}

function assertProjection(
  state: WorkProjectionState,
  event: AuthenticatedWorkEvent,
  expectedRevision: number,
): void {
  if (state.organizationId !== event.actor.organizationId || state.actorId !== event.actor.actorId) {
    throw new RangeError('reducer returned a projection outside the event actor scope');
  }
  if (state.schemaVersion !== 1 || state.revision !== expectedRevision + 1) {
    throw new RangeError('reducer must return schema version 1 and increment the projection revision once');
  }
}

function entityWrites(
  pk: string,
  organizationId: string,
  actorId: string,
  day: string,
  prefix: 'SESSION' | 'NODE' | 'EDGE',
  itemKind: 'session' | 'node' | 'edge',
  before: Record<string, StoredWorkSession | StoredWorkNode | StoredWorkEdge>,
  after: Record<string, StoredWorkSession | StoredWorkNode | StoredWorkEdge>,
): NonNullable<TransactWriteCommandInput['TransactItems']> {
  const writes: NonNullable<TransactWriteCommandInput['TransactItems']> = [];
  for (const [id, value] of Object.entries(after)) {
    if (sameValue(before[id], value)) continue;
    writes.push({
      Put: {
        TableName: WORK_GRAPH_TABLE,
        Item: {
          PK: pk,
          SK: `${prefix}#${id}`,
          itemKind,
          ...value,
          ...(itemKind === 'node' || itemKind === 'edge'
            ? {
                day,
                targetLookupPk: `WGTARGET#${organizationId}`,
                targetLookupSk: `${itemKind.toUpperCase()}#${id}`,
                targetActorId: actorId,
              }
            : {}),
        },
      },
    });
  }
  for (const id of Object.keys(before)) {
    if (after[id] !== undefined) continue;
    writes.push({ Delete: { TableName: WORK_GRAPH_TABLE, Key: { PK: pk, SK: `${prefix}#${id}` } } });
  }
  return writes;
}

function deltaOperations(
  before: WorkProjectionState,
  after: WorkProjectionState,
): StoredWorkGraphDeltaOperation[] {
  const operations: StoredWorkGraphDeltaOperation[] = [];
  for (const [id, node] of Object.entries(after.nodes)) {
    if (!sameValue(before.nodes[id], node)) operations.push({ kind: 'upsert-node', node });
  }
  for (const id of Object.keys(before.nodes)) {
    if (after.nodes[id] === undefined) operations.push({ kind: 'remove-node', nodeId: id });
  }
  for (const [id, edge] of Object.entries(after.edges)) {
    if (!sameValue(before.edges[id], edge)) operations.push({ kind: 'upsert-edge', edge });
  }
  for (const id of Object.keys(before.edges)) {
    if (after.edges[id] === undefined) operations.push({ kind: 'remove-edge', edgeId: id });
  }
  return operations;
}

function revisionKey(revision: number): string {
  return String(revision).padStart(12, '0');
}

/**
 * DynamoDB-backed implementation of realtime-modules' frozen
 * WorkGraphRepository interface.
 */
export class WorkGraphRepository {
  constructor(
    private readonly docClient: DynamoDBDocumentClient,
    private readonly reduceEvent: WorkGraphReducer,
  ) {}

  async loadProjection(
    organizationId: string,
    actorId: string,
    day: string,
  ): Promise<WorkProjectionState | null> {
    const pk = partitionKey(organizationId, actorId, day);
    // A paginated Query is not a snapshot. Fence it with strongly-consistent
    // META reads so a browser never receives a mixed revision (which could
    // otherwise re-introduce an entity that a concurrent revocation removed).
    for (let attempt = 0; attempt < MAX_FENCED_READ_ATTEMPTS; attempt += 1) {
      const before = await this.readMeta(pk);
      if (!before || before.erasing === true) return null;
      const items = await this.queryPartition(pk);
      const after = await this.readMeta(pk);
      if (!after || after.erasing === true) return null;
      if (before.revision !== after.revision
        || before.organizationId !== after.organizationId
        || before.actorId !== after.actorId) continue;
      const projection = this.hydrateProjection(items, organizationId, actorId);
      if (projection && projection.revision === after.revision) return projection;
    }
    throw new WorkGraphConcurrencyError(-1, 'Work graph projection changed during a paginated read');
  }

  /**
   * A local calendar day can cross two UTC partitions. The source projection
   * remains UTC-sharded for durable ingestion; this bounded read joins only
   * the UTC days touched by the requested IANA day and filters by the shared
   * half-open day window. It deliberately does not scan an organization.
   */
  async loadProjectionForLocalDay(
    organizationId: string,
    actorId: string,
    day: string,
    timezone: string,
  ): Promise<WorkProjectionState | null> {
    const window = workDayWindow(day, timezone);
    const startDay = window.start.slice(0, 10);
    const lastInstant = new Date(Date.parse(window.end) - 1).toISOString().slice(0, 10);
    const utcDays = startDay === lastInstant ? [startDay] : [startDay, lastInstant];
    const projections = (await Promise.all(utcDays.map((utcDay) => this.loadProjection(
      organizationId,
      actorId,
      utcDay,
    )))).filter((projection): projection is WorkProjectionState => projection !== null);
    if (projections.length === 0) return null;

    const latest = <T extends { id: string; updatedAt: string; revision: number }>(
      values: T[],
    ): Record<string, T> => values.reduce<Record<string, T>>((result, value) => {
      const existing = result[value.id];
      if (!existing
        || value.updatedAt > existing.updatedAt
        || (value.updatedAt === existing.updatedAt && value.revision > existing.revision)) {
        result[value.id] = value;
      }
      return result;
    }, {});
    const overlaps = (value: { startedAt?: string; updatedAt: string; endedAt?: string }): boolean => {
      try {
        return intervalOverlapsWorkDay(value.startedAt ?? value.updatedAt, value.endedAt, window);
      } catch {
        // An invalid stored time cannot be safely positioned in the reader's
        // calendar day, so fail closed rather than guessing.
        return false;
      }
    };
    const nodes = latest(projections.flatMap((projection) => Object.values(projection.nodes))
      .filter((node) => !node.deletedAt && overlaps(node)));
    const sessions = latest(projections.flatMap((projection) => Object.values(projection.sessions))
      .filter(overlaps));
    const edges = latest(projections.flatMap((projection) => Object.values(projection.edges))
      .filter((edge) => !edge.deletedAt
        && overlaps(edge)
        && nodes[edge.fromId] !== undefined
        && nodes[edge.toId] !== undefined));

    // Each source commit increments exactly one UTC partition by one, so the
    // sum changes for every committed event in this bounded local view. It is
    // a read watermark only; replay continues to use its authoritative UTC
    // partition and must reset/refetch when a cross-partition scope changes.
    const revision = projections.reduce((total, projection) => total + projection.revision, 0);
    if (!Number.isSafeInteger(revision)) {
      throw new WorkGraphDataIntegrityError('local-day projection revision exceeded safe integer range');
    }
    return {
      schemaVersion: 1,
      organizationId,
      actorId,
      revision,
      sessions,
      nodes,
      edges,
      sourceCheckpoints: Object.assign({}, ...projections.map((projection) => projection.sourceCheckpoints)),
      appliedEventIds: projections.flatMap((projection) => projection.appliedEventIds).slice(-MAX_APPLIED_EVENT_IDS),
    };
  }

  async commitEvent(
    event: AuthenticatedWorkEvent,
    expectedRevision: number,
  ): Promise<WorkProjectionState> {
    if (event.actor.organizationId !== event.producer.organizationId) {
      throw new RangeError('producer and actor organizations must match');
    }
    const day = utcDay(event.occurredAt);
    const pk = partitionKey(event.actor.organizationId, event.actor.actorId, day);
    const eventKey = { PK: pk, SK: eventSortKey(event) };

    if (await this.eventExists(eventKey)) {
      const existing = await this.loadProjection(event.actor.organizationId, event.actor.actorId, day);
      if (!existing) throw new WorkGraphDataIntegrityError('event marker exists without a projection');
      return existing;
    }

    const current = await this.loadProjection(event.actor.organizationId, event.actor.actorId, day)
      ?? initialProjection(event);
    if (current.revision !== expectedRevision) {
      throw new WorkGraphConcurrencyError(expectedRevision);
    }

    const reduced = this.reduceEvent(current, event);
    assertProjection(reduced, event, expectedRevision);
    const next: WorkProjectionState = {
      ...reduced,
      appliedEventIds: [...reduced.appliedEventIds].slice(-MAX_APPLIED_EVENT_IDS),
    };

    const writes: NonNullable<TransactWriteCommandInput['TransactItems']> = [
      ...entityWrites(pk, next.organizationId, next.actorId, day, 'SESSION', 'session', current.sessions, next.sessions),
      ...entityWrites(pk, next.organizationId, next.actorId, day, 'NODE', 'node', current.nodes, next.nodes),
      ...entityWrites(pk, next.organizationId, next.actorId, day, 'EDGE', 'edge', current.edges, next.edges),
      {
        Put: {
          TableName: WORK_GRAPH_TABLE,
          Item: {
            PK: pk,
            SK: eventKey.SK,
            itemKind: 'event',
            eventId: event.eventId,
            idempotencyKey: event.idempotencyKey,
            source: event.source,
            occurredAt: event.occurredAt,
            ttl: Math.floor(Date.parse(event.receivedAt) / 1_000) + EVENT_RETENTION_SECONDS,
          },
          ConditionExpression: 'attribute_not_exists(PK)',
        },
      },
      {
        Put: {
          TableName: WORK_GRAPH_TABLE,
          Item: {
            PK: pk,
            SK: `DELTA#${revisionKey(next.revision)}`,
            itemKind: 'delta',
            previousWatermark: current.revision,
            watermark: next.revision,
            committedAt: event.receivedAt,
            operations: deltaOperations(current, next),
            ttl: Math.floor(Date.parse(event.receivedAt) / 1_000) + DELTA_RETENTION_SECONDS,
          },
          ConditionExpression: 'attribute_not_exists(PK)',
        },
      },
      {
        Put: {
          TableName: WORK_GRAPH_TABLE,
          Item: {
            PK: pk,
            SK: `OUTBOX#${String(next.revision).padStart(12, '0')}#${event.eventId}`,
            itemKind: 'outbox',
            eventType: 'work-graph.projection.changed.v1',
            organizationId: next.organizationId,
            actorId: next.actorId,
            day,
            revision: next.revision,
            sourceEventId: event.eventId,
            createdAt: event.receivedAt,
            ttl: Math.floor(Date.parse(event.receivedAt) / 1_000) + EVENT_RETENTION_SECONDS,
          },
          ConditionExpression: 'attribute_not_exists(PK)',
        },
      },
      {
        Put: {
          TableName: WORK_GRAPH_TABLE,
          Item: {
            PK: pk,
            SK: 'META',
            itemKind: 'meta',
            schemaVersion: 1,
            organizationId: next.organizationId,
            actorId: next.actorId,
            day,
            revision: next.revision,
            sourceCheckpoints: next.sourceCheckpoints,
            appliedEventIds: next.appliedEventIds,
          },
          ConditionExpression: expectedRevision === 0
            ? 'attribute_not_exists(PK)'
            : '#revision = :expectedRevision AND attribute_not_exists(#erasing)',
          ExpressionAttributeNames: expectedRevision === 0
            ? undefined
            : { '#revision': 'revision', '#erasing': 'erasing' },
          ExpressionAttributeValues: expectedRevision === 0
            ? undefined
            : { ':expectedRevision': expectedRevision },
        },
      },
    ];

    if (writes.length > MAX_TRANSACTION_ITEMS) {
      throw new WorkGraphTransactionTooLargeError(writes.length);
    }

    try {
      await this.docClient.send(new TransactWriteCommand({ TransactItems: writes }));
      return next;
    } catch (error) {
      if (!isConditionalFailure(error)) throw error;

      // A racing delivery of the same event is success, not a version error.
      if (await this.eventExists(eventKey)) {
        const existing = await this.loadProjection(event.actor.organizationId, event.actor.actorId, day);
        if (existing) return existing;
      }
      throw new WorkGraphConcurrencyError(expectedRevision);
    }
  }

  async locateTarget(
    organizationId: string,
    target: { kind: 'node' | 'edge'; id: string },
  ): Promise<{ personId: string; day: string; timezone: string } | null> {
    const result = await this.docClient.send(new QueryCommand({
      TableName: WORK_GRAPH_TABLE,
      IndexName: WORK_GRAPH_TARGET_LOOKUP_INDEX,
      KeyConditionExpression: 'targetLookupPk = :pk AND targetLookupSk = :sk',
      ExpressionAttributeValues: {
        ':pk': `WGTARGET#${organizationId}`,
        ':sk': `${target.kind.toUpperCase()}#${target.id}`,
      },
      // One row per day the projection touched this target. A long-lived
      // entity — a document that is still being revised after midnight — has
      // several, so the query cannot be capped at "one or it is ambiguous".
      Limit: MAX_TARGET_LOOKUP_DAYS,
    }));
    const rows = (result.Items ?? [])
      .map((item) => item as Record<string, unknown>)
      .filter((row) => typeof row.targetActorId === 'string'
        && typeof row.day === 'string'
        && isCalendarDate(row.day));
    if (rows.length === 0) return null;
    // Two PEOPLE with the same target id is genuinely ambiguous provenance and
    // still discloses nothing. The same person on two days is not: it is one
    // entity that outlived a day boundary, and the day it was last projected
    // into is the one whose graph still describes it.
    const actors = new Set(rows.map((row) => String(row.targetActorId)));
    if (actors.size !== 1) return null;
    const newest = rows
      .map((row) => String(row.day))
      .sort((left, right) => (left < right ? 1 : left > right ? -1 : 0))[0]!;
    return { personId: String(rows[0]!.targetActorId), day: newest, timezone: 'UTC' };
  }

  async listDeltas(
    organizationId: string,
    actorId: string,
    day: string,
    afterWatermark: number,
    throughWatermark: number,
    limit: number,
  ): Promise<{ batches: StoredWorkGraphDeltaBatch[]; complete: boolean }> {
    if (!Number.isSafeInteger(afterWatermark)
      || !Number.isSafeInteger(throughWatermark)
      || afterWatermark < 0
      || throughWatermark < afterWatermark
      || !Number.isSafeInteger(limit)
      || limit < 1
      // The gateway deliberately requests one item beyond its 1,000-batch
      // delivery ceiling so it can distinguish a complete replay from a
      // silently truncated one.
      || limit > 1_001) throw new RangeError('Invalid work graph replay range');
    if (throughWatermark === afterWatermark) return { batches: [], complete: true };

    const pk = partitionKey(organizationId, actorId, day);
    const result = await this.docClient.send(new QueryCommand({
      TableName: WORK_GRAPH_TABLE,
      KeyConditionExpression: 'PK = :pk AND SK BETWEEN :from AND :through',
      ExpressionAttributeValues: {
        ':pk': pk,
        ':from': `DELTA#${revisionKey(afterWatermark + 1)}`,
        ':through': `DELTA#${revisionKey(throughWatermark)}`,
      },
      ConsistentRead: true,
      Limit: limit + 1,
    }));
    const batches = (result.Items ?? []).slice(0, limit).flatMap((row) => {
      if (row.itemKind !== 'delta'
        || !Number.isSafeInteger(row.previousWatermark)
        || !Number.isSafeInteger(row.watermark)
        || typeof row.committedAt !== 'string'
        || !Array.isArray(row.operations)) return [];
      return [{
        previousWatermark: Number(row.previousWatermark),
        watermark: Number(row.watermark),
        committedAt: row.committedAt,
        operations: row.operations as StoredWorkGraphDeltaOperation[],
      }];
    });
    let expected = afterWatermark;
    for (const batch of batches) {
      if (batch.previousWatermark !== expected || batch.watermark !== expected + 1) {
        return { batches, complete: false };
      }
      expected = batch.watermark;
    }
    return {
      batches,
      complete: expected === throughWatermark && (result.Items?.length ?? 0) <= limit,
    };
  }

  async eraseActorDay(
    organizationId: string,
    actorId: string,
    day: string,
    expectedRevision: number,
  ): Promise<void> {
    const pk = partitionKey(organizationId, actorId, day);
    const meta = await this.readMeta(pk);
    // A retry after the final conditional delete is idempotent regardless of
    // the revision the caller originally erased.
    if (!meta) return;

    const resuming = meta.erasing === true && meta.eraseExpectedRevision === expectedRevision;
    const lockedRevision = expectedRevision + 1;
    if (!resuming) {
      if (meta.revision !== expectedRevision || meta.erasing === true) {
        throw new WorkGraphConcurrencyError(expectedRevision);
      }
      try {
        await this.docClient.send(new TransactWriteCommand({
          TransactItems: [{
            Update: {
              TableName: WORK_GRAPH_TABLE,
              Key: { PK: pk, SK: 'META' },
              UpdateExpression: 'SET #erasing = :true, #eraseExpectedRevision = :expected, #revision = :locked',
              ConditionExpression: '#revision = :expected AND attribute_not_exists(#erasing)',
              ExpressionAttributeNames: {
                '#erasing': 'erasing',
                '#eraseExpectedRevision': 'eraseExpectedRevision',
                '#revision': 'revision',
              },
              ExpressionAttributeValues: {
                ':true': true,
                ':expected': expectedRevision,
                ':locked': lockedRevision,
              },
            },
          }],
        }));
      } catch (error) {
        if (isConditionalFailure(error)) throw new WorkGraphConcurrencyError(expectedRevision);
        throw error;
      }
    }

    // Enumerate only after the META lock is committed. A projection commit
    // writes entities and META together, so the lock prevents any unlisted
    // entity from racing in between enumeration and deletion.
    const items = await this.queryPartition(pk);
    const lockedMeta = items.find((item): item is MetaItem => item.SK === 'META' && item.itemKind === 'meta');
    if (!lockedMeta
      || lockedMeta.erasing !== true
      || lockedMeta.eraseExpectedRevision !== expectedRevision
      || lockedMeta.revision !== lockedRevision) {
      throw new WorkGraphConcurrencyError(expectedRevision);
    }

    const keys = items
      .filter((item) => item.SK !== 'META')
      .map((item) => ({ PK: pk, SK: item.SK }));
    for (let offset = 0; offset < keys.length; offset += MAX_TRANSACTION_ITEMS) {
      const chunk = keys.slice(offset, offset + MAX_TRANSACTION_ITEMS);
      await this.docClient.send(new TransactWriteCommand({
        TransactItems: chunk.map((Key) => ({ Delete: { TableName: WORK_GRAPH_TABLE, Key } })),
      }));
    }

    try {
      await this.docClient.send(new TransactWriteCommand({
        TransactItems: [{
          Delete: {
            TableName: WORK_GRAPH_TABLE,
            Key: { PK: pk, SK: 'META' },
            ConditionExpression: '#revision = :locked AND #erasing = :true',
            ExpressionAttributeNames: { '#revision': 'revision', '#erasing': 'erasing' },
            ExpressionAttributeValues: { ':locked': lockedRevision, ':true': true },
          },
        }],
      }));
    } catch (error) {
      if (isConditionalFailure(error)) throw new WorkGraphConcurrencyError(expectedRevision);
      throw error;
    }
  }

  private async eventExists(Key: { PK: string; SK: string }): Promise<boolean> {
    const result = await this.docClient.send(new GetCommand({
      TableName: WORK_GRAPH_TABLE,
      Key,
      ConsistentRead: true,
    }));
    return result.Item !== undefined;
  }

  private async readMeta(pk: string): Promise<MetaItem | null> {
    const result = await this.docClient.send(new GetCommand({
      TableName: WORK_GRAPH_TABLE,
      Key: { PK: pk, SK: 'META' },
      ConsistentRead: true,
    }));
    const item = result.Item as MetaItem | undefined;
    return item?.itemKind === 'meta' && item.SK === 'META' ? item : null;
  }

  private async queryPartition(pk: string): Promise<StoredItem[]> {
    const items: StoredItem[] = [];
    let exclusiveStartKey: Record<string, unknown> | undefined;
    do {
      const result = await this.docClient.send(new QueryCommand({
        TableName: WORK_GRAPH_TABLE,
        KeyConditionExpression: 'PK = :pk',
        ExpressionAttributeValues: { ':pk': pk },
        ConsistentRead: true,
        ...(exclusiveStartKey ? { ExclusiveStartKey: exclusiveStartKey } : {}),
      }));
      items.push(...((result.Items ?? []) as StoredItem[]));
      exclusiveStartKey = result.LastEvaluatedKey;
    } while (exclusiveStartKey);
    return items;
  }

  private hydrateProjection(
    items: StoredItem[],
    organizationId: string,
    actorId: string,
  ): WorkProjectionState | null {
    const meta = items.find((item): item is MetaItem => item.SK === 'META' && item.itemKind === 'meta');
    if (!meta || meta.erasing === true) return null;
    if (meta.organizationId !== organizationId || meta.actorId !== actorId) {
      throw new WorkGraphDataIntegrityError('projection metadata crossed its organization or actor boundary');
    }

    const state: WorkProjectionState = {
      schemaVersion: 1,
      organizationId,
      actorId,
      revision: meta.revision,
      sessions: {},
      nodes: {},
      edges: {},
      sourceCheckpoints: meta.sourceCheckpoints ?? {},
      appliedEventIds: (meta.appliedEventIds ?? []).slice(-MAX_APPLIED_EVENT_IDS),
    };
    for (const item of items) {
      if (item.itemKind !== 'session' && item.itemKind !== 'node' && item.itemKind !== 'edge') continue;
      const value = withoutStorageFields<StoredWorkSession | StoredWorkNode | StoredWorkEdge>(item);
      if (value.organizationId !== organizationId || value.actorId !== actorId) {
        throw new WorkGraphDataIntegrityError(`${item.SK} crossed its organization or actor boundary`);
      }
      if (item.itemKind === 'session') state.sessions[value.id] = value as StoredWorkSession;
      if (item.itemKind === 'node') state.nodes[value.id] = value as StoredWorkNode;
      if (item.itemKind === 'edge') state.edges[value.id] = value as StoredWorkEdge;
    }
    return state;
  }
}
