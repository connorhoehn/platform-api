import {
  DynamoDBDocumentClient,
  PutCommand,
  QueryCommand,
} from '@aws-sdk/lib-dynamodb';
import { tableName } from '../lib/ddb-table-name';

export type WorkCollaborationCapability =
  | 'message'
  | 'view-document'
  | 'edit-document'
  | 'join-conversation'
  | 'join-meeting'
  | 'observe-terminal'
  | 'control-terminal'
  | 'view-transcript'
  | 'view-run'
  | 'dispatch-run';

export type WorkCollaborationTarget =
  | { kind: 'node'; id: string }
  | { kind: 'edge'; id: string };

export interface WorkCollaborationRequest {
  schemaVersion: 1;
  id: string;
  organizationId: string;
  requesterId: string;
  ownerId: string;
  target: WorkCollaborationTarget;
  capability: WorkCollaborationCapability;
  state: 'requested' | 'accepted' | 'declined' | 'expired' | 'revoked';
  createdAt: string;
  updatedAt: string;
  expiresAt: string;
  revision: number;
}

interface CollaborationItem extends Record<string, unknown> {
  PK: string;
  SK: string;
  collaborationLookupPk: string;
  collaborationLookupSk: string;
  entityType: 'work-collaboration-request';
  ttl: number;
  request: WorkCollaborationRequest;
}

export const WORK_COLLABORATION_LOOKUP_INDEX = 'work-graph-collaboration-lookup';
const WORK_GRAPH_TABLE = tableName('work-graph');
const ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]*$/;
const CAPABILITIES = new Set<WorkCollaborationCapability>([
  'message',
  'view-document',
  'edit-document',
  'join-conversation',
  'join-meeting',
  'observe-terminal',
  'control-terminal',
  'view-transcript',
  'view-run',
  'dispatch-run',
]);
const STATES = new Set<WorkCollaborationRequest['state']>([
  'requested',
  'accepted',
  'declined',
  'expired',
  'revoked',
]);
const MAX_REQUEST_MS = 24 * 60 * 60 * 1_000;

export class WorkCollaborationConflictError extends Error {
  readonly code = 'WORK_COLLABORATION_REVISION_CONFLICT';

  constructor(
    readonly requestId: string,
    readonly expectedRevision: number | null,
  ) {
    super(`Collaboration request ${requestId} no longer has the expected revision.`);
    this.name = 'WorkCollaborationConflictError';
  }
}

export class WorkCollaborationValidationError extends Error {
  readonly code = 'WORK_COLLABORATION_INVALID_REQUEST';

  constructor(message: string) {
    super(message);
    this.name = 'WorkCollaborationValidationError';
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function hasExactKeys(value: Record<string, unknown>, keys: readonly string[]): boolean {
  const actual = Reflect.ownKeys(value);
  const expected = new Set(keys);
  return actual.length === expected.size
    && actual.every((key) => typeof key === 'string' && expected.has(key));
}

function isId(value: unknown): value is string {
  return typeof value === 'string'
    && value.length > 0
    && value.length <= 128
    && ID_PATTERN.test(value);
}

function isTimestamp(value: unknown): value is string {
  if (typeof value !== 'string' || value.length > 64) return false;
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) && new Date(parsed).toISOString() === value;
}

function parseRequest(value: unknown): WorkCollaborationRequest | null {
  if (!isRecord(value) || !hasExactKeys(value, [
    'schemaVersion',
    'id',
    'organizationId',
    'requesterId',
    'ownerId',
    'target',
    'capability',
    'state',
    'createdAt',
    'updatedAt',
    'expiresAt',
    'revision',
  ])) return null;

  if (!isRecord(value.target)
    || !hasExactKeys(value.target, ['kind', 'id'])
    || (value.target.kind !== 'node' && value.target.kind !== 'edge')
    || !isId(value.target.id)
    || value.schemaVersion !== 1
    || !isId(value.id)
    || !isId(value.organizationId)
    || !isId(value.requesterId)
    || !isId(value.ownerId)
    || value.requesterId === value.ownerId
    || !CAPABILITIES.has(value.capability as WorkCollaborationCapability)
    || !STATES.has(value.state as WorkCollaborationRequest['state'])
    || !isTimestamp(value.createdAt)
    || !isTimestamp(value.updatedAt)
    || !isTimestamp(value.expiresAt)
    || !Number.isSafeInteger(value.revision)
    || (value.revision as number) < 1) return null;

  const createdAt = Date.parse(value.createdAt);
  const updatedAt = Date.parse(value.updatedAt);
  const expiresAt = Date.parse(value.expiresAt);
  if (updatedAt < createdAt || expiresAt <= createdAt || expiresAt - createdAt > MAX_REQUEST_MS) {
    return null;
  }

  return {
    schemaVersion: 1,
    id: value.id,
    organizationId: value.organizationId,
    requesterId: value.requesterId,
    ownerId: value.ownerId,
    target: { kind: value.target.kind, id: value.target.id },
    capability: value.capability as WorkCollaborationCapability,
    state: value.state as WorkCollaborationRequest['state'],
    createdAt: value.createdAt,
    updatedAt: value.updatedAt,
    expiresAt: value.expiresAt,
    revision: value.revision as number,
  };
}

function ownerPartition(organizationId: string, ownerId: string): string {
  return `WGCOLLAB#${organizationId}#${ownerId}`;
}

function requestSortKey(requestId: string): string {
  return `REQUEST#${requestId}`;
}

function lookupPartition(organizationId: string): string {
  return `WGCOLLAB#${organizationId}`;
}

function isConditionalFailure(error: unknown): boolean {
  return typeof error === 'object'
    && error !== null
    && 'name' in error
    && error.name === 'ConditionalCheckFailedException';
}

/** DynamoDB persistence for capability-specific work collaboration requests. */
export class WorkCollaborationRepository {
  constructor(
    private readonly docClient: DynamoDBDocumentClient,
    private readonly table: string = WORK_GRAPH_TABLE,
  ) {}

  async getRequest(
    organizationId: string,
    requestId: string,
  ): Promise<WorkCollaborationRequest | null> {
    if (!isId(organizationId) || !isId(requestId)) return null;
    const response = await this.docClient.send(new QueryCommand({
      TableName: this.table,
      IndexName: WORK_COLLABORATION_LOOKUP_INDEX,
      KeyConditionExpression: 'collaborationLookupPk = :organization AND collaborationLookupSk = :request',
      ExpressionAttributeValues: {
        ':organization': lookupPartition(organizationId),
        ':request': requestSortKey(requestId),
      },
      Limit: 2,
    }));
    if (!response.Items || response.Items.length !== 1) return null;
    return this.readItem(response.Items[0], organizationId, requestId);
  }

  async putRequest(
    candidate: WorkCollaborationRequest,
    expectedRevision: number | null,
  ): Promise<WorkCollaborationRequest> {
    const request = parseRequest(candidate);
    if (!request) {
      throw new WorkCollaborationValidationError('Collaboration request does not match the frozen contract.');
    }
    if (expectedRevision === null ? request.revision !== 1 : request.revision !== expectedRevision + 1) {
      throw new WorkCollaborationValidationError('Collaboration request revision must advance exactly once.');
    }

    const item: CollaborationItem = {
      PK: ownerPartition(request.organizationId, request.ownerId),
      SK: requestSortKey(request.id),
      collaborationLookupPk: lookupPartition(request.organizationId),
      collaborationLookupSk: requestSortKey(request.id),
      entityType: 'work-collaboration-request',
      ttl: Math.floor(Date.parse(request.expiresAt) / 1_000),
      request,
    };

    try {
      await this.docClient.send(new PutCommand({
        TableName: this.table,
        Item: item,
        ConditionExpression: expectedRevision === null
          ? 'attribute_not_exists(PK) AND attribute_not_exists(SK)'
          : 'attribute_exists(PK) AND attribute_exists(SK) AND #request.#revision = :expectedRevision',
        ...(expectedRevision === null ? {} : {
          ExpressionAttributeNames: { '#request': 'request', '#revision': 'revision' },
          ExpressionAttributeValues: { ':expectedRevision': expectedRevision },
        }),
      }));
    } catch (error) {
      if (isConditionalFailure(error)) {
        throw new WorkCollaborationConflictError(request.id, expectedRevision);
      }
      throw error;
    }
    return request;
  }

  async findAcceptedRequest(
    organizationId: string,
    ownerId: string,
    requesterId: string,
    target: WorkCollaborationTarget,
    capability: WorkCollaborationCapability,
    now: string,
  ): Promise<WorkCollaborationRequest | null> {
    if (!isId(organizationId)
      || !isId(ownerId)
      || !isId(requesterId)
      || !isId(target.id)
      || !CAPABILITIES.has(capability)
      || !isTimestamp(now)) return null;

    const matches: WorkCollaborationRequest[] = [];
    let exclusiveStartKey: Record<string, unknown> | undefined;
    do {
      const response = await this.docClient.send(new QueryCommand({
        TableName: this.table,
        KeyConditionExpression: 'PK = :partition AND begins_with(SK, :requestPrefix)',
        ExpressionAttributeValues: {
          ':partition': ownerPartition(organizationId, ownerId),
          ':requestPrefix': 'REQUEST#',
        },
        ...(exclusiveStartKey ? { ExclusiveStartKey: exclusiveStartKey } : {}),
      }));
      for (const item of response.Items ?? []) {
        if (!isRecord(item) || typeof item.SK !== 'string') continue;
        const requestId = item.SK.slice('REQUEST#'.length);
        const request = this.readItem(item, organizationId, requestId);
        if (request
          && request.ownerId === ownerId
          && request.requesterId === requesterId
          && request.target.kind === target.kind
          && request.target.id === target.id
          && request.capability === capability
          && request.state === 'accepted'
          && Date.parse(request.expiresAt) > Date.parse(now)) matches.push(request);
      }
      exclusiveStartKey = response.LastEvaluatedKey;
    } while (exclusiveStartKey);

    // Multiple active grants are valid, but selection is deterministic and
    // reveals no count. Every returned row has already passed strict parsing.
    return matches.sort((left, right) => right.updatedAt.localeCompare(left.updatedAt))[0] ?? null;
  }

  private readItem(
    value: unknown,
    organizationId: string,
    requestId: string,
  ): WorkCollaborationRequest | null {
    if (!isRecord(value) || value.entityType !== 'work-collaboration-request') return null;
    const request = parseRequest(value.request);
    if (!request
      || request.organizationId !== organizationId
      || request.id !== requestId
      || value.PK !== ownerPartition(request.organizationId, request.ownerId)
      || value.SK !== requestSortKey(request.id)
      || value.collaborationLookupPk !== lookupPartition(request.organizationId)
      || value.collaborationLookupSk !== requestSortKey(request.id)) return null;
    return request;
  }
}
