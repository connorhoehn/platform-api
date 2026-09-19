import {
  DynamoDBDocumentClient,
  GetCommand,
  QueryCommand,
  TransactWriteCommand,
  type TransactWriteCommandInput,
} from '@aws-sdk/lib-dynamodb';
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
const MAX_APPLIED_EVENT_IDS = 1_000;
const MAX_TRANSACTION_ITEMS = 100;

type ItemKind = 'meta' | 'session' | 'node' | 'edge' | 'event';

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
  const { PK: _pk, SK: _sk, itemKind: _kind, ...value } = item;
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
        Item: { PK: pk, SK: `${prefix}#${id}`, itemKind, ...value },
      },
    });
  }
  for (const id of Object.keys(before)) {
    if (after[id] !== undefined) continue;
    writes.push({ Delete: { TableName: WORK_GRAPH_TABLE, Key: { PK: pk, SK: `${prefix}#${id}` } } });
  }
  return writes;
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
    const items = await this.queryPartition(pk);
    return this.hydrateProjection(items, organizationId, actorId);
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
      ...entityWrites(pk, 'SESSION', 'session', current.sessions, next.sessions),
      ...entityWrites(pk, 'NODE', 'node', current.nodes, next.nodes),
      ...entityWrites(pk, 'EDGE', 'edge', current.edges, next.edges),
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
            expiresAt: Math.floor(Date.parse(event.receivedAt) / 1_000) + EVENT_RETENTION_SECONDS,
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

  async eraseActorDay(
    organizationId: string,
    actorId: string,
    day: string,
    expectedRevision: number,
  ): Promise<void> {
    const pk = partitionKey(organizationId, actorId, day);
    const items = await this.queryPartition(pk);
    const meta = items.find((item): item is MetaItem => item.SK === 'META' && item.itemKind === 'meta');
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
