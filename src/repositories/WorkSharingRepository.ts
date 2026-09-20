import {
  DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
  QueryCommand,
  TransactWriteCommand,
} from '@aws-sdk/lib-dynamodb';
import { tableName } from '../lib/ddb-table-name';

/**
 * Structural mirrors of the frozen realtime-modules work-graph contract.
 * C1 replaces these with package imports when it installs the private
 * realtime-modules artifact; keeping this file dependency-free lets the
 * repository land before the consumer pin changes.
 */
export type SharingAudience =
  | { kind: 'people'; personIds: string[] }
  | { kind: 'conversation'; conversationIds: string[] }
  | { kind: 'organization' };

export interface SharingSelection {
  nodeIds: string[];
  edgeIds: string[];
  disclosure: 'existence' | 'summary' | 'details';
}

export interface WorkSharingGrant {
  schemaVersion: 1;
  id: string;
  organizationId: string;
  ownerId: string;
  audience: SharingAudience;
  selection: SharingSelection;
  state: 'active' | 'paused' | 'stopped' | 'expired';
  createdAt: string;
  updatedAt: string;
  expiresAt: string;
  revision: number;
  historyGrantId?: string;
}

export interface WorkHistoryGrant extends Omit<WorkSharingGrant, 'historyGrantId'> {
  dayFrom: string;
  dayThrough: string;
}

export interface WorkSharingViewerContext {
  /** Resolved from the authenticated server identity, never request input. */
  viewerId: string;
  /** Memberships resolved by the server; browsers cannot assert these IDs. */
  conversationIds: string[];
}

export type StoredWorkGrant = WorkSharingGrant | WorkHistoryGrant;

interface WorkSharingGrantItem {
  PK: string;
  SK: string;
  grantLookupPk: string;
  grantLookupSk: string;
  entityType: 'work-sharing-grant';
  /** Epoch seconds for asynchronous cleanup only. Reads never trust TTL. */
  ttl: number;
  grant: unknown;
}

const WORK_GRAPH_TABLE = tableName('work-graph');
export const WORK_SHARING_GRANT_LOOKUP_INDEX = 'work-graph-grant-lookup';

const ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]*$/;
const MAX_ID_LENGTH = 128;
const MAX_NODES = 250;
const MAX_EDGES = 500;
const MIN_SHARING_MS = 5 * 60 * 1_000;
const MAX_SHARING_MS = 24 * 60 * 60 * 1_000;
const MAX_HISTORY_SHARING_MS = 7 * 24 * 60 * 60 * 1_000;

type JsonRecord = Record<string, unknown>;

export class WorkSharingConflictError extends Error {
  readonly code = 'WORK_SHARING_REVISION_CONFLICT';

  constructor(
    readonly grantId: string,
    readonly expectedRevision: number | null,
  ) {
    super(`Sharing grant ${grantId} no longer has the expected revision.`);
    this.name = 'WorkSharingConflictError';
  }
}

export class WorkSharingValidationError extends Error {
  readonly code = 'WORK_SHARING_INVALID_GRANT';

  constructor(message: string) {
    super(message);
    this.name = 'WorkSharingValidationError';
  }
}

function isRecord(value: unknown): value is JsonRecord {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function hasExactKeys(
  value: unknown,
  required: readonly string[],
  optional: readonly string[] = [],
): value is JsonRecord {
  if (!isRecord(value)) return false;
  const allowed = new Set([...required, ...optional]);
  const keys = Reflect.ownKeys(value);
  return required.every((key) => Object.prototype.hasOwnProperty.call(value, key))
    && keys.every((key) => typeof key === 'string' && allowed.has(key));
}

function isId(value: unknown): value is string {
  return typeof value === 'string'
    && value.length > 0
    && value.length <= MAX_ID_LENGTH
    && ID_PATTERN.test(value);
}

function isTimestamp(value: unknown): value is string {
  if (typeof value !== 'string' || value.length === 0 || value.length > 64) return false;
  const match = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,3}))?Z$/.exec(value);
  if (!match) return false;
  const [, yearText, monthText, dayText, hourText, minuteText, secondText, fractionText] = match;
  const year = Number(yearText);
  const month = Number(monthText);
  const day = Number(dayText);
  const hour = Number(hourText);
  const minute = Number(minuteText);
  const second = Number(secondText);
  const millisecond = Number((fractionText ?? '').padEnd(3, '0') || '0');
  const date = new Date(Date.UTC(year, month - 1, day, hour, minute, second, millisecond));
  return date.getUTCFullYear() === year
    && date.getUTCMonth() === month - 1
    && date.getUTCDate() === day
    && date.getUTCHours() === hour
    && date.getUTCMinutes() === minute
    && date.getUTCSeconds() === second;
}

function isCalendarDate(value: unknown): value is string {
  if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(value)) return false;
  const [year, month, day] = value.split('-').map(Number);
  const date = new Date(Date.UTC(year, month - 1, day));
  return date.getUTCFullYear() === year
    && date.getUTCMonth() === month - 1
    && date.getUTCDate() === day;
}

function isUniqueIdArray(value: unknown, maximum: number, requireValue = false): value is string[] {
  return Array.isArray(value)
    && (!requireValue || value.length > 0)
    && value.length <= maximum
    && value.every(isId)
    && new Set(value).size === value.length;
}

function isAudience(value: unknown): value is SharingAudience {
  if (!isRecord(value)) return false;
  if (value.kind === 'organization') return hasExactKeys(value, ['kind']);
  if (value.kind === 'people') {
    return hasExactKeys(value, ['kind', 'personIds'])
      && isUniqueIdArray(value.personIds, 100, true);
  }
  return value.kind === 'conversation'
    && hasExactKeys(value, ['kind', 'conversationIds'])
    && isUniqueIdArray(value.conversationIds, 100, true);
}

function isSelection(value: unknown): value is SharingSelection {
  return hasExactKeys(value, ['nodeIds', 'edgeIds', 'disclosure'])
    && isUniqueIdArray(value.nodeIds, MAX_NODES)
    && isUniqueIdArray(value.edgeIds, MAX_EDGES)
    && value.nodeIds.length + value.edgeIds.length > 0
    && (value.disclosure === 'existence'
      || value.disclosure === 'summary'
      || value.disclosure === 'details');
}

function parseGrant(value: unknown): StoredWorkGrant | null {
  if (!isRecord(value)) return null;
  const isHistory = Object.prototype.hasOwnProperty.call(value, 'dayFrom')
    || Object.prototype.hasOwnProperty.call(value, 'dayThrough');
  const optional = isHistory ? [] : ['historyGrantId'];
  const required = [
    'schemaVersion',
    'id',
    'organizationId',
    'ownerId',
    'audience',
    'selection',
    'state',
    'createdAt',
    'updatedAt',
    'expiresAt',
    'revision',
    ...(isHistory ? ['dayFrom', 'dayThrough'] : []),
  ];
  if (!hasExactKeys(value, required, optional)) return null;
  if (value.schemaVersion !== 1
    || !isId(value.id)
    || !isId(value.organizationId)
    || !isId(value.ownerId)
    || !isAudience(value.audience)
    || !isSelection(value.selection)
    || !['active', 'paused', 'stopped', 'expired'].includes(value.state as string)
    || !isTimestamp(value.createdAt)
    || !isTimestamp(value.updatedAt)
    || !isTimestamp(value.expiresAt)
    || typeof value.revision !== 'number'
    || !Number.isSafeInteger(value.revision)
    || value.revision < 1) return null;

  const createdAt = Date.parse(value.createdAt);
  const updatedAt = Date.parse(value.updatedAt);
  const expiresAt = Date.parse(value.expiresAt);
  const maximum = isHistory ? MAX_HISTORY_SHARING_MS : MAX_SHARING_MS;
  if (updatedAt < createdAt
    || expiresAt <= createdAt
    || expiresAt - createdAt < MIN_SHARING_MS
    || expiresAt - createdAt > maximum) return null;

  if (isHistory) {
    if (!isCalendarDate(value.dayFrom)
      || !isCalendarDate(value.dayThrough)
      || value.dayFrom > value.dayThrough
      || Date.parse(`${value.dayThrough}T00:00:00.000Z`)
        - Date.parse(`${value.dayFrom}T00:00:00.000Z`) >= MAX_HISTORY_SHARING_MS) return null;
  } else if (value.historyGrantId !== undefined && !isId(value.historyGrantId)) {
    return null;
  }

  // Rebuild the value from allowlisted fields so persistence metadata and
  // unknown stored properties can never escape this repository.
  const base: WorkSharingGrant = {
    schemaVersion: 1,
    id: value.id,
    organizationId: value.organizationId,
    ownerId: value.ownerId,
    audience: cloneAudience(value.audience),
    selection: {
      nodeIds: [...value.selection.nodeIds],
      edgeIds: [...value.selection.edgeIds],
      disclosure: value.selection.disclosure,
    },
    state: value.state as WorkSharingGrant['state'],
    createdAt: value.createdAt,
    updatedAt: value.updatedAt,
    expiresAt: value.expiresAt,
    revision: value.revision,
    ...(!isHistory && value.historyGrantId !== undefined
      ? { historyGrantId: value.historyGrantId as string }
      : {}),
  };
  return isHistory
    ? { ...base, dayFrom: value.dayFrom as string, dayThrough: value.dayThrough as string }
    : base;
}

function cloneAudience(audience: SharingAudience): SharingAudience {
  switch (audience.kind) {
    case 'people': return { kind: 'people', personIds: [...audience.personIds] };
    case 'conversation': return { kind: 'conversation', conversationIds: [...audience.conversationIds] };
    case 'organization': return { kind: 'organization' };
  }
}

function ownerPartition(organizationId: string, ownerId: string): string {
  return `WGGRANT#${organizationId}#${ownerId}`;
}

function grantSortKey(grantId: string): string {
  return `GRANT#${grantId}`;
}

function lookupPartition(organizationId: string): string {
  return `WGGRANT#${organizationId}`;
}

function isConditionalFailure(error: unknown): boolean {
  return typeof error === 'object'
    && error !== null
    && 'name' in error
    && error.name === 'ConditionalCheckFailedException';
}

function isEffectiveForViewer(
  grant: StoredWorkGrant,
  viewer: WorkSharingViewerContext,
  nowMs: number,
): boolean {
  if (grant.state !== 'active' || Date.parse(grant.expiresAt) <= nowMs) return false;
  switch (grant.audience.kind) {
    case 'people':
      return grant.audience.personIds.includes(viewer.viewerId);
    case 'conversation': {
      const memberships = new Set(viewer.conversationIds);
      return grant.audience.conversationIds.some((id) => memberships.has(id));
    }
    case 'organization':
      // Both the query and stored-row validation bind the result to the
      // authenticated organization supplied by the server caller.
      return true;
  }
}

/** DynamoDB implementation of the frozen WorkSharingRepository contract. */
export class WorkSharingRepository {
  constructor(
    private readonly docClient: DynamoDBDocumentClient,
    private readonly table: string = WORK_GRAPH_TABLE,
  ) {}

  async getGrant(
    organizationId: string,
    grantId: string,
    now: string,
  ): Promise<StoredWorkGrant | null> {
    if (!isId(organizationId) || !isId(grantId) || !isTimestamp(now)) return null;
    // A DynamoDB GSI is eventually consistent.  It is useful for discovering
    // the owner partition, but it is not an authorization read: a just-paused
    // or revoked grant must not be returned from an old index image.
    const response = await this.docClient.send(new QueryCommand({
      TableName: this.table,
      IndexName: WORK_SHARING_GRANT_LOOKUP_INDEX,
      KeyConditionExpression: 'grantLookupPk = :organization AND grantLookupSk = :grant',
      ExpressionAttributeValues: {
        ':organization': lookupPartition(organizationId),
        ':grant': grantSortKey(grantId),
      },
      Limit: 2,
    }));

    // A grant ID is unique within an organization. Ambiguous/corrupt lookup
    // results fail closed instead of selecting an arbitrary owner's row.
    if (!response.Items || response.Items.length !== 1) return null;
    const discovered = response.Items[0];
    if (!isRecord(discovered)
      || typeof discovered.PK !== 'string'
      || typeof discovered.SK !== 'string'
      || discovered.SK !== grantSortKey(grantId)) return null;

    // Re-read the exact primary-key row consistently.  In particular, never
    // authorize from the stale `grant` blob returned by the lookup GSI.
    const current = await this.docClient.send(new GetCommand({
      TableName: this.table,
      Key: { PK: discovered.PK, SK: discovered.SK },
      ConsistentRead: true,
    }));
    const grant = this.readStoredGrant(current.Item, organizationId, undefined, grantId);
    if (!grant
      || grant.state === 'expired'
      || Date.parse(grant.expiresAt) <= Date.parse(now)) return null;
    return grant;
  }

  async listEffectiveGrants(
    organizationId: string,
    ownerId: string,
    viewer: WorkSharingViewerContext,
    now: string,
  ): Promise<StoredWorkGrant[]> {
    if (!isId(organizationId)
      || !isId(ownerId)
      || !isTimestamp(now)
      || !isId(viewer.viewerId)
      || !isUniqueIdArray(viewer.conversationIds, 100)) return [];

    const items: Record<string, unknown>[] = [];
    let exclusiveStartKey: Record<string, unknown> | undefined;
    do {
      const response = await this.docClient.send(new QueryCommand({
        TableName: this.table,
        KeyConditionExpression: 'PK = :partition AND begins_with(SK, :grantPrefix)',
        ExpressionAttributeValues: {
          ':partition': ownerPartition(organizationId, ownerId),
          ':grantPrefix': 'GRANT#',
        },
        // This is an authorization query on the base table. TTL cleanup is
        // asynchronous, so effective state is still checked below.
        ConsistentRead: true,
        ...(exclusiveStartKey ? { ExclusiveStartKey: exclusiveStartKey } : {}),
      }));
      items.push(...(response.Items ?? []));
      exclusiveStartKey = response.LastEvaluatedKey;
    } while (exclusiveStartKey);

    const nowMs = Date.parse(now);
    return items.flatMap((item) => {
      const grant = this.readStoredGrant(item, organizationId, ownerId);
      return grant && isEffectiveForViewer(grant, viewer, nowMs) ? [grant] : [];
    });
  }

  /** Current owner controls. Stopped/expired rows remain durable audit data
   * but are not returned as mutable UI state. */
  async listOwnedGrants(
    organizationId: string,
    ownerId: string,
    now: string,
  ): Promise<StoredWorkGrant[]> {
    if (!isId(organizationId) || !isId(ownerId) || !isTimestamp(now)) return [];
    const grants: StoredWorkGrant[] = [];
    let exclusiveStartKey: Record<string, unknown> | undefined;
    do {
      const response = await this.docClient.send(new QueryCommand({
        TableName: this.table,
        KeyConditionExpression: 'PK = :partition AND begins_with(SK, :grantPrefix)',
        ExpressionAttributeValues: {
          ':partition': ownerPartition(organizationId, ownerId),
          ':grantPrefix': 'GRANT#',
        },
        ConsistentRead: true,
        ...(exclusiveStartKey ? { ExclusiveStartKey: exclusiveStartKey } : {}),
      }));
      for (const item of response.Items ?? []) {
        const grant = this.readStoredGrant(item, organizationId, ownerId);
        if (grant
          && (grant.state === 'active' || grant.state === 'paused')
          && Date.parse(grant.expiresAt) > Date.parse(now)) grants.push(grant);
      }
      if (grants.length > 500) return [];
      exclusiveStartKey = response.LastEvaluatedKey;
    } while (exclusiveStartKey);
    return grants.sort((left, right) => right.updatedAt.localeCompare(left.updatedAt));
  }

  async putGrant(
    candidate: StoredWorkGrant,
    expectedRevision: number | null,
  ): Promise<StoredWorkGrant> {
    const grant = parseGrant(candidate);
    if (!grant) throw new WorkSharingValidationError('Sharing grant does not match the frozen contract.');
    if (expectedRevision === null ? grant.revision !== 1 : grant.revision !== expectedRevision + 1) {
      throw new WorkSharingValidationError('Sharing grant revision must advance exactly once.');
    }

    const item: WorkSharingGrantItem = {
      PK: ownerPartition(grant.organizationId, grant.ownerId),
      SK: grantSortKey(grant.id),
      grantLookupPk: lookupPartition(grant.organizationId),
      grantLookupSk: grantSortKey(grant.id),
      entityType: 'work-sharing-grant',
      ttl: Math.floor(Date.parse(grant.expiresAt) / 1_000),
      grant,
    };

    try {
      await this.docClient.send(new PutCommand({
        TableName: this.table,
        Item: item as unknown as Record<string, unknown>,
        ConditionExpression: expectedRevision === null
          ? 'attribute_not_exists(PK) AND attribute_not_exists(SK)'
          : 'attribute_exists(PK) AND attribute_exists(SK) AND #grant.#revision = :expectedRevision',
        ...(expectedRevision === null ? {} : {
          ExpressionAttributeNames: {
            '#grant': 'grant',
            '#revision': 'revision',
          },
          ExpressionAttributeValues: { ':expectedRevision': expectedRevision },
        }),
      }));
    } catch (error) {
      if (isConditionalFailure(error)) {
        throw new WorkSharingConflictError(grant.id, expectedRevision);
      }
      throw error;
    }
    return grant;
  }

  /**
   * Creates the current grant and its optional historical companion as one
   * authorization change.  A current row must never point at a history grant
   * that was not committed (or vice versa), because readers evaluate either
   * row independently.
   */
  async putGrantPair(
    currentCandidate: WorkSharingGrant,
    historyCandidate: WorkHistoryGrant,
  ): Promise<{ current: WorkSharingGrant; history: WorkHistoryGrant }> {
    const current = parseGrant(currentCandidate);
    const history = parseGrant(historyCandidate);
    if (!current || 'dayFrom' in current || !history || !('dayFrom' in history)) {
      throw new WorkSharingValidationError('Sharing grant pair does not match the frozen contract.');
    }
    if (current.revision !== 1
      || history.revision !== 1
      || current.organizationId !== history.organizationId
      || current.ownerId !== history.ownerId
      || current.historyGrantId !== history.id) {
      throw new WorkSharingValidationError('Sharing grant pair is not internally consistent.');
    }

    const itemFor = (grant: StoredWorkGrant): WorkSharingGrantItem => ({
      PK: ownerPartition(grant.organizationId, grant.ownerId),
      SK: grantSortKey(grant.id),
      grantLookupPk: lookupPartition(grant.organizationId),
      grantLookupSk: grantSortKey(grant.id),
      entityType: 'work-sharing-grant',
      ttl: Math.floor(Date.parse(grant.expiresAt) / 1_000),
      grant,
    });

    try {
      await this.docClient.send(new TransactWriteCommand({
        TransactItems: [current, history].map((grant) => ({
          Put: {
            TableName: this.table,
            Item: itemFor(grant),
            ConditionExpression: 'attribute_not_exists(PK) AND attribute_not_exists(SK)',
          },
        })),
      }));
    } catch (error) {
      if (isConditionalFailure(error)
        || (error as { name?: string })?.name === 'TransactionCanceledException') {
        throw new WorkSharingConflictError(current.id, null);
      }
      throw error;
    }
    return { current, history };
  }

  private readStoredGrant(
    item: unknown,
    organizationId: string,
    ownerId?: string,
    grantId?: string,
  ): StoredWorkGrant | null {
    if (!isRecord(item)
      || item.entityType !== 'work-sharing-grant'
      || item.PK !== ownerPartition(organizationId, ownerId ?? this.ownerFromItem(item))
      || (typeof item.SK !== 'string' || !item.SK.startsWith('GRANT#'))
      || item.grantLookupPk !== lookupPartition(organizationId)
      || item.grantLookupSk !== item.SK) return null;

    const grant = parseGrant(item.grant);
    if (!grant
      || grant.organizationId !== organizationId
      || (ownerId !== undefined && grant.ownerId !== ownerId)
      || item.PK !== ownerPartition(grant.organizationId, grant.ownerId)
      || item.SK !== grantSortKey(grant.id)
      || (grantId !== undefined && grant.id !== grantId)) return null;
    return grant;
  }

  private ownerFromItem(item: JsonRecord): string {
    if (!isRecord(item.grant) || !isId(item.grant.ownerId)) return '';
    return item.grant.ownerId;
  }
}
