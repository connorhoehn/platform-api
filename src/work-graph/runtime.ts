import { createHash, randomUUID } from 'crypto';
import {
  BatchGetCommand,
  DeleteCommand,
  GetCommand,
  PutCommand,
  QueryCommand,
  ScanCommand,
  type DynamoDBDocumentClient,
} from '@aws-sdk/lib-dynamodb';
import { SERVICE_AUTH_HEADER, signEnvelope } from '@connorhoehn/service-runtime';
import { projectWorkEvent } from '@connorhoehn/realtime-modules/work-graph';
import type {
  WorkGraphDeltaOperation,
  WorkGraphQueryV2,
  WorkGraphSnapshotV2,
} from '@connorhoehn/realtime-modules/work-graph';
import { docClient } from '../lib/aws-clients';
import { tableName } from '../lib/ddb-table-name';
import { WorkGraphRepository, type WorkSourceKind } from '../repositories/WorkGraphRepository';
import { authorizationEntityDigest, revisionExplainedByAdditions } from './authorizationRevision';
import { publishProjectionChangedSignal } from '../services/work-graph-outbox-drainer';
import { WorkSharingRepository, type SharingAudience } from '../repositories/WorkSharingRepository';
import {
  WorkCollaborationRepository,
  type WorkCollaborationCapability,
  type WorkCollaborationRequest,
} from '../repositories/WorkCollaborationRepository';
import { WorkSourceAccountRepository } from '../repositories/WorkSourceAccountRepository';
import { createWorkGraphCursorCodec } from './cursor';
import { parsePipelineStepResourceId } from './sources/mapPipeline';
import { runRowLocator } from './sources/runRowLocator';
import { DynamoWorkActivityReader } from './sources/activityReader';
import {
  WorkGraphOrchestrator,
  WorkGraphUnavailableError,
  type AuthenticatedWorkGraphViewer,
  type WorkGraphDirectory,
  type WorkSourceActionAdapter,
} from './orchestrator';
import type {
  WorkAclTarget,
  WorkGraphPolicyScope,
  WorkSourceAclAdapter,
  WorkSourceAclDecision,
  WorkPolicyDecision,
} from './policy';

const WORK_GRAPH_TABLE = tableName('work-graph');
const CHAT_MEMBERS_TABLE = tableName(process.env.DYNAMODB_CHAT_MEMBERS_TABLE || 'chat-members');
const DOCUMENTS_TABLE = tableName(process.env.DYNAMODB_DOCUMENTS_TABLE || 'crdt-documents');
const PIPELINE_RUNS_TABLE = tableName('pipeline-runs');
const RECORDINGS_TABLE = tableName('video-call-recordings');
const HANGOUT_ROOMS_TABLE = tableName('hangout-rooms');
const HANGOUT_ROOM_MEMBERS_TABLE = tableName('hangout-room-members');
const MAX_DIRECTORY_USERS = 500;

type Ddb = DynamoDBDocumentClient;

function sourceRefs(target: WorkAclTarget, source: WorkSourceKind): Array<{ source: WorkSourceKind; resourceId: string }> {
  if (target.kind === 'node') return target.sourceRef.source === source ? [target.sourceRef] : [];
  return target.endpointRefs.filter((ref) => ref.source === source);
}

function sourceOwnedResourceId(target: WorkAclTarget, source: WorkSourceKind, resourceId: string): string {
  if (resourceId.includes(':') || target.kind !== 'node') return resourceId;
  if (source === 'cloud-compute') {
    if (target.node.kind === 'terminal') return `box:${resourceId}`;
    if (target.node.kind === 'run') return `job:${resourceId}`;
  }
  if (source === 'local-compute') {
    if (target.node.kind === 'terminal') return `machine:${resourceId}`;
    if (target.node.kind === 'run') return `job:${resourceId}`;
  }
  // The recording ACL decides transcript capability from this prefix. Without
  // it a transcript node was authorized as its meeting, so a viewer allowed to
  // read the transcript never received `view-transcript` and Ask stayed off.
  if (source === 'meeting') {
    if (target.node.kind === 'transcript') return `transcript:${resourceId}`;
    if (target.node.kind === 'meeting') return `meeting:${resourceId}`;
  }
  return resourceId;
}

/**
 * Rows EXAMINED per round trip. A Scan's `Limit` counts what it looks at, not
 * what the filter keeps, so sizing it by how many MATCHES the caller wants
 * made an authorization that wants at most two rows walk the whole table two
 * at a time — dozens of round trips per node, per request, growing with every
 * meeting and run ever recorded. Correct, and slow enough to turn into
 * withheld nodes the moment a request runs out of time.
 */
const SCAN_PAGE_SIZE = 200;
/** A poll cannot walk an unbounded table; a hit past this is not found. */
const MAX_SCAN_PAGES = 25;

async function scanAll(
  client: Ddb,
  input: ConstructorParameters<typeof ScanCommand>[0],
  maximum = MAX_DIRECTORY_USERS,
): Promise<Record<string, unknown>[]> {
  const rows: Record<string, unknown>[] = [];
  let ExclusiveStartKey: Record<string, unknown> | undefined;
  let pages = 0;
  do {
    const result = await client.send(new ScanCommand({
      ...input,
      ...(ExclusiveStartKey ? { ExclusiveStartKey } : {}),
      Limit: SCAN_PAGE_SIZE,
    }));
    rows.push(...((result.Items ?? []) as Record<string, unknown>[]));
    ExclusiveStartKey = result.LastEvaluatedKey;
    pages += 1;
  } while (ExclusiveStartKey && rows.length < maximum && pages < MAX_SCAN_PAGES);
  return rows.slice(0, maximum);
}

/**
 * Principals a browser request needs before it can be authorized. Each is a
 * create-only join (`ensureVerifiedSourcePrincipal`), so once it exists it is
 * never rewritten — which made writing all six on every read pure waste: six
 * conditional Puts that fail their condition, then six Gets, on every graph,
 * people, preview and ask request.
 */
const VIEWER_PRINCIPALS: ReadonlyArray<{
  source: WorkSourceKind;
  producerServiceId: 'platform-api' | 'websocket-gateway' | 'aws-agentcore';
}> = [
  { source: 'conversation', producerServiceId: 'websocket-gateway' },
  { source: 'document', producerServiceId: 'websocket-gateway' },
  { source: 'cloud-compute', producerServiceId: 'aws-agentcore' },
  { source: 'local-compute', producerServiceId: 'aws-agentcore' },
  // Pipeline and meeting joins were only created at the authenticated
  // pipeline-trigger and video-session entrypoints, so a person who had
  // never used either had their own run and recording events rejected at
  // ingest. The bind is the same create-only one: this authenticated
  // subject to its own same-named source subject, never another actor's.
  { source: 'pipeline', producerServiceId: 'platform-api' },
  { source: 'meeting', producerServiceId: 'platform-api' },
];

/** How long a verified set of principals is trusted without re-reading it. */
export const PRINCIPALS_VERIFIED_TTL_MS = 5 * 60_000;
/**
 * `lastSeenAt` drives the rail's "fresh" flag, which is a 5-minute window
 * (`listPeople`), so refreshing it more than once a minute buys nothing.
 */
export const IDENTITY_REFRESH_MS = 60_000;
const MAX_REMEMBERED_VIEWERS = 10_000;

/** A bounded "until" map: oldest entry goes first when full. */
function remember(map: Map<string, number>, key: string, until: number): void {
  map.delete(key);
  if (map.size >= MAX_REMEMBERED_VIEWERS) {
    const oldest = map.keys().next().value;
    if (oldest !== undefined) map.delete(oldest);
  }
  map.set(key, until);
}

export class DynamoWorkGraphDirectory implements WorkGraphDirectory {
  private readonly sourceAccounts: WorkSourceAccountRepository;
  /** `org\u0000subject` → until when its six principals are known good. */
  private readonly principalsVerifiedUntil = new Map<string, number>();
  /** `org\u0000subject` → until when `lastSeenAt` need not be rewritten. */
  private readonly identityFreshUntil = new Map<string, number>();

  constructor(private readonly client: Ddb, private readonly now: () => number = Date.now) {
    this.sourceAccounts = new WorkSourceAccountRepository(client);
  }

  private async conversationIds(userId: string): Promise<string[]> {
    const rows = await scanAll(this.client, {
      TableName: CHAT_MEMBERS_TABLE,
      FilterExpression: 'userId = :user AND attribute_not_exists(removedAt)',
      ExpressionAttributeValues: { ':user': userId },
      ProjectionExpression: 'channel',
    }, 100);
    return [...new Set(rows.flatMap((row) => typeof row.channel === 'string' ? [row.channel] : []))];
  }

  private async registerIdentity(organizationId: string, viewerId: string): Promise<void> {
    const key = `${organizationId}\u0000${viewerId}`;
    const nowMs = this.now();
    if ((this.identityFreshUntil.get(key) ?? 0) > nowMs) return;
    await this.client.send(new PutCommand({
      TableName: WORK_GRAPH_TABLE,
      Item: {
        PK: `WGIDENTITY#${organizationId}`,
        SK: `USER#${viewerId}`,
        itemKind: 'identity',
        organizationId,
        viewerId,
        lastSeenAt: new Date(nowMs).toISOString(),
      },
    }));
    remember(this.identityFreshUntil, key, nowMs + IDENTITY_REFRESH_MS);
  }

  /**
   * Read first; write only a principal that is missing. A verified set is
   * remembered for a few minutes so steady-state reads touch no principal
   * rows at all. A mapping that exists but names someone else still fails,
   * exactly as the create-only write did.
   */
  private async ensurePrincipals(organizationId: string, subject: string): Promise<boolean> {
    const key = `${organizationId}\u0000${subject}`;
    if ((this.principalsVerifiedUntil.get(key) ?? 0) > this.now()) return true;
    const joins = await Promise.all(VIEWER_PRINCIPALS.map(async ({ source, producerServiceId }) => {
      const current = await this.sourceAccounts.resolve(source, producerServiceId, organizationId, subject);
      if (current) return current.organizationId === organizationId && current.actorId === subject;
      return this.sourceAccounts.ensureVerifiedSourcePrincipal({ source, producerServiceId, organizationId, subject });
    }));
    if (joins.some((joined) => !joined)) {
      this.principalsVerifiedUntil.delete(key);
      return false;
    }
    remember(this.principalsVerifiedUntil, key, this.now() + PRINCIPALS_VERIFIED_TTL_MS);
    return true;
  }

  async resolveViewer(subject: string, organizationClaim?: string): Promise<AuthenticatedWorkGraphViewer | null> {
    if (!subject || !organizationClaim) return null;
    await this.registerIdentity(organizationClaim, subject);
    if (!await this.ensurePrincipals(organizationClaim, subject)) return null;
    return {
      viewerId: subject,
      organizationId: organizationClaim,
      conversationIds: await this.conversationIds(subject),
    };
  }

  async resolveViewerById(organizationId: string, viewerId: string): Promise<AuthenticatedWorkGraphViewer | null> {
    if (!await this.personBelongsToOrganization(organizationId, viewerId)) return null;
    return { viewerId, organizationId, conversationIds: await this.conversationIds(viewerId) };
  }

  async personBelongsToOrganization(organizationId: string, personId: string): Promise<boolean> {
    const result = await this.client.send(new GetCommand({
      TableName: WORK_GRAPH_TABLE,
      Key: { PK: `WGIDENTITY#${organizationId}`, SK: `USER#${personId}` },
      ConsistentRead: true,
    }));
    return result.Item?.organizationId === organizationId && result.Item?.viewerId === personId;
  }

  async listPeople(organizationId: string): Promise<Array<{ personId: string; updatedAt: string; fresh: boolean }>> {
    const rows = await scanAll(this.client, {
      TableName: WORK_GRAPH_TABLE,
      FilterExpression: 'PK = :pk AND itemKind = :identity',
      ExpressionAttributeValues: { ':pk': `WGIDENTITY#${organizationId}`, ':identity': 'identity' },
    });
    const now = Date.now();
    return rows.flatMap((row) => {
      if (row.organizationId !== organizationId
        || typeof row.viewerId !== 'string'
        || typeof row.lastSeenAt !== 'string') return [];
      return [{
        personId: row.viewerId,
        updatedAt: row.lastSeenAt,
        fresh: Number.isFinite(Date.parse(row.lastSeenAt)) && Date.parse(row.lastSeenAt) >= now - 5 * 60_000,
      }];
    });
  }

  async audienceBelongsToOrganization(
    organizationId: string,
    ownerId: string,
    audience: SharingAudience,
  ): Promise<boolean> {
    if (audience.kind === 'organization') return true;
    if (audience.kind === 'people') {
      const values = await Promise.all(audience.personIds.map((id) => this.personBelongsToOrganization(organizationId, id)));
      return values.every(Boolean);
    }
    const ownerMemberships = new Set(await this.conversationIds(ownerId));
    return audience.conversationIds.every((id) => ownerMemberships.has(id));
  }
}

interface SourceAuthorization {
  ownerValid: boolean;
  viewerHasDirectAccess: boolean;
  capabilities: WorkCollaborationCapability[];
}

const COMPUTE_CAPABILITY_PATH = '/internal/work-graph/compute-capabilities';

function computeSourceUrl(): string | null {
  const value = process.env.WORK_GRAPH_COMPUTE_SOURCE_URL?.trim();
  return value ? value.replace(/\/+$/, '') : null;
}

async function computeSourceCall<T>(path: string, body: Record<string, unknown>): Promise<{
  decision: 'allow'; value: T;
} | { decision: 'deny' | 'unavailable' }> {
  const url = computeSourceUrl();
  const secret = (process.env.WORK_GRAPH_SERVICE_AUTH_SECRET || process.env.SERVICE_AUTH_SECRET || '').trim();
  if (!url || !secret) return { decision: 'unavailable' };
  try {
    const response = await fetch(`${url}${COMPUTE_CAPABILITY_PATH}/${path}`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        [SERVICE_AUTH_HEADER]: signEnvelope(body, { secret, serviceId: 'platform-api' }),
      },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(5_000),
    });
    if (response.status === 403 || response.status === 404) return { decision: 'deny' };
    if (!response.ok) return { decision: 'unavailable' };
    return { decision: 'allow', value: await response.json() as T };
  } catch {
    return { decision: 'unavailable' };
  }
}

/**
 * Every source row one authorization pass needs, read once.
 *
 * Authorizing a target used to read its own row, and a day's graph authorizes
 * every node in it: a hundred nodes meant a hundred round trips, and for the
 * run-backed ones a FILTERED SCAN — which walks the whole table to find one
 * row. Ten steps of one run scanned that same run ten times. The reads are the
 * same reads; they are just issued once for the whole pass and answered from
 * memory after that.
 *
 * A row remains "exactly one match or nothing": two rows claiming one id is
 * ambiguous provenance and still authorizes nothing, exactly as before.
 */
interface AclBatch {
  /** `runId` → its run row; absent when unknown or ambiguous. */
  runs: Map<string, Record<string, unknown> | null>;
  /** Any of `callId` / `recordingId` / `transcriptId` → its recording row. */
  recordings: Map<string, Record<string, unknown> | null>;
  documents: Map<string, Record<string, unknown> | null>;
  /** `channel\u0000userId` → membership, memoized across targets. */
  members: Map<string, Promise<boolean>>;
  /** One compute-source authorization per resource, not per target. */
  compute: Map<string, Promise<SourceAuthorization>>;
  /** `roomId` → the people a booked room concerns, memoized per request. */
  rooms: Map<string, Promise<string[]>>;
}

function emptyAclBatch(): AclBatch {
  return { runs: new Map(), recordings: new Map(), documents: new Map(), members: new Map(), compute: new Map(), rooms: new Map() };
}

/** Indexes rows by an id attribute, marking an id claimed twice as ambiguous. */
function indexRows(
  into: Map<string, Record<string, unknown> | null>,
  rows: Record<string, unknown>[],
  attributes: readonly string[],
  wanted: Set<string>,
): void {
  for (const row of rows) {
    for (const attribute of attributes) {
      const value = row[attribute];
      if (typeof value !== 'string' || !wanted.has(value)) continue;
      into.set(value, into.has(value) ? null : row);
    }
  }
  for (const id of wanted) if (!into.has(id)) into.set(id, null);
}

/** `id IN (:v0, :v1, …)`, chunked so one expression never exceeds DynamoDB's cap. */
const MAX_IN_OPERANDS = 90;
/**
 * `scanAll` pages at `min(100, maximum - read)`, so a small maximum walks the
 * table a couple of items per round trip — which is how one run lookup became
 * dozens of calls. A prefetch always asks for a full page.
 */
const MIN_ACL_SCAN_ROWS = 100;

function chunked<T>(values: T[], size: number): T[][] {
  const chunks: T[][] = [];
  for (let index = 0; index < values.length; index += size) chunks.push(values.slice(index, index + size));
  return chunks;
}

/** Exported for tests: the batched read is the reason a hundred-node day is fast. */
export class DynamoSourceAclAdapter implements WorkSourceAclAdapter {
  constructor(readonly source: WorkSourceKind, private readonly client: Ddb) {}

  async authorizeDisclosure(scope: WorkGraphPolicyScope, targets: WorkAclTarget[]): Promise<Map<string, WorkSourceAclDecision>> {
    const result = new Map<string, WorkSourceAclDecision>();
    const resourceIds = targets.flatMap((target) => sourceRefs(target, this.source)
      .map((ref) => sourceOwnedResourceId(target, this.source, ref.resourceId)));
    const batch = await this.prefetch(resourceIds);
    // The reads are done; the per-target work is now in-memory, so the pass
    // no longer serializes a round trip behind every node.
    const decided = await Promise.all(targets.map(async (target) => ({
      target,
      decisions: await Promise.all(sourceRefs(target, this.source).map((ref) => this.authorizeRef(
        scope,
        sourceOwnedResourceId(target, this.source, ref.resourceId),
        batch,
      ))),
    })));
    for (const { target, decisions } of decided) {
      if (decisions.length === 0 || decisions.some((decision) => !decision.ownerValid)) {
        result.set(target.id, { targetId: target.id, decision: 'deny', policyRevision: scope.policyRevision });
        continue;
      }
      const direct = decisions.every((decision) => decision.viewerHasDirectAccess);
      // Capabilities describe source-supported collaboration choices. Direct
      // access affects disclosure, but an active graph share must still be
      // able to request a source-approved capability such as terminal control.
      const capabilities = decisions[0].capabilities
        .filter((capability) => decisions.every((decision) => decision.capabilities.includes(capability)));
      result.set(target.id, {
        targetId: target.id,
        decision: 'allow',
        maximumDisclosure: direct ? 'details' : 'summary',
        capabilities,
        policyRevision: scope.policyRevision,
      });
    }
    return result;
  }

  async authorizeCapability(
    scope: WorkGraphPolicyScope,
    target: WorkAclTarget,
    capability: WorkCollaborationCapability,
  ): Promise<WorkPolicyDecision> {
    const refs = sourceRefs(target, this.source)
      .map((ref) => sourceOwnedResourceId(target, this.source, ref.resourceId));
    const batch = await this.prefetch(refs);
    const decisions = await Promise.all(refs.map((ref) => this.authorizeRef(scope, ref, batch)));
    return decisions.length > 0
      && decisions.every((decision) => decision.ownerValid
        && decision.capabilities.includes(capability))
      ? 'allow'
      : 'deny';
  }

  /**
   * Reads every row this pass will ask about, in one query per table.
   *
   * `pipeline` and `meeting` rows are found by a non-key attribute, so they
   * cost a Scan either way — but one Scan filtered to the ids this pass wants,
   * rather than one per target that pages the table two items at a time.
   */
  private async prefetch(resourceIds: string[]): Promise<AclBatch> {
    const batch = emptyAclBatch();
    const distinct = [...new Set(resourceIds)];
    if (distinct.length === 0) return batch;

    if (this.source === 'pipeline') {
      // By id, through the locator: a Scan the first time a run is seen, a
      // keyed read after. The row is always read fresh; only its key is known.
      const runIds = [...new Set(distinct.map((id) => parsePipelineStepResourceId(id).runId))];
      const rows = await runRowLocator.rows(this.client, PIPELINE_RUNS_TABLE, runIds);
      for (const [runId, row] of rows) batch.runs.set(runId, row);
      return batch;
    }

    if (this.source === 'meeting') {
      const ids = new Set(distinct.map((id) => id.replace(/^(meeting|recording|transcript):/, '')));
      await Promise.all(chunked([...ids], MAX_IN_OPERANDS).map(async (chunk) => {
        const values = Object.fromEntries(chunk.map((id, index) => [`:m${index}`, id]));
        const names = Object.keys(values).join(', ');
        const rows = await scanAll(this.client, {
          TableName: RECORDINGS_TABLE,
          FilterExpression: `callId IN (${names}) OR recordingId IN (${names}) OR transcriptId IN (${names})`,
          ExpressionAttributeValues: values,
        }, Math.max(chunk.length * 2, MIN_ACL_SCAN_ROWS));
        indexRows(batch.recordings, rows, ['callId', 'recordingId', 'transcriptId'], new Set(chunk));
      }));
      return batch;
    }

    if (this.source === 'document') {
      const documentIds = new Set(distinct.map((id) => (id.includes(':') ? id.slice(0, id.lastIndexOf(':')) : id)));
      await Promise.all(chunked([...documentIds], 100).map(async (chunk) => {
        const response = await this.client.send(new BatchGetCommand({
          RequestItems: { [DOCUMENTS_TABLE]: { Keys: chunk.map((documentId) => ({ documentId })) } },
        }));
        const rows = (response.Responses?.[DOCUMENTS_TABLE] ?? []) as Record<string, unknown>[];
        indexRows(batch.documents, rows, ['documentId'], new Set(chunk));
      }));
      return batch;
    }
    return batch;
  }

  /** Membership is asked for the owner and the viewer of every target; ask once. */
  private memberOnce(batch: AclBatch, channel: string, userId: string): Promise<boolean> {
    const key = `${channel}\u0000${userId}`;
    const known = batch.members.get(key);
    if (known) return known;
    const pending = this.isConversationMember(channel, userId);
    batch.members.set(key, pending);
    return pending;
  }

  /**
   * Who a booked room belongs to: the person who created it and everyone on
   * its roster. Read only when no recording exists, so an in-progress or
   * finished call is still decided by its own record.
   */
  private async roomPeople(organizationId: string, roomId: string): Promise<string[]> {
    const room = (await this.client.send(new GetCommand({
      TableName: HANGOUT_ROOMS_TABLE,
      Key: { orgId: organizationId, roomId },
    })).catch(() => undefined))?.Item;
    if (!room) return [];
    const members = await this.client.send(new QueryCommand({
      TableName: HANGOUT_ROOM_MEMBERS_TABLE,
      KeyConditionExpression: 'roomId = :room',
      ExpressionAttributeValues: { ':room': roomId },
      ProjectionExpression: 'userId',
    })).catch(() => undefined);
    return [
      ...(typeof room.createdBy === 'string' ? [room.createdBy] : []),
      ...((members?.Items ?? []).flatMap((item) => (typeof item.userId === 'string' ? [item.userId] : []))),
    ];
  }

  private async isConversationMember(channel: string, userId: string): Promise<boolean> {
    const result = await this.client.send(new GetCommand({
      TableName: CHAT_MEMBERS_TABLE,
      Key: { channel, userId },
      ConsistentRead: true,
    }));
    return !!result.Item && result.Item.removedAt === undefined;
  }

  private async authorizeRef(
    scope: WorkGraphPolicyScope,
    resourceId: string,
    batch: AclBatch,
  ): Promise<SourceAuthorization> {
    if (this.source === 'conversation') {
      const [owner, viewer] = await Promise.all([
        this.memberOnce(batch, resourceId, scope.personId),
        this.memberOnce(batch, resourceId, scope.viewerId),
      ]);
      return { ownerValid: owner, viewerHasDirectAccess: viewer, capabilities: ['message', 'join-conversation'] };
    }
    if (this.source === 'document') {
      const documentId = resourceId.includes(':') ? resourceId.slice(0, resourceId.lastIndexOf(':')) : resourceId;
      const row = batch.documents.get(documentId) ?? undefined;
      const channel = typeof row?.channel === 'string' ? row.channel : undefined;
      const ownerValid = row?.createdBy === scope.personId
        || (!!channel && await this.memberOnce(batch, channel, scope.personId));
      const viewer = row?.createdBy === scope.viewerId
        || (!!channel && await this.memberOnce(batch, channel, scope.viewerId));
      return { ownerValid, viewerHasDirectAccess: viewer, capabilities: ['view-document', ...(row?.createdBy === scope.viewerId ? ['edit-document' as const] : [])] };
    }
    if (this.source === 'pipeline') {
      // A step is addressed `runId::stepId` and a workspace `runId::sessionId`;
      // the run's own row is what decides whether either is visible, so
      // authorize the run. A resource the run does not own is denied.
      const row = batch.runs.get(parsePipelineStepResourceId(resourceId).runId) ?? undefined;
      return {
        ownerValid: row?.triggeredBy === scope.personId,
        viewerHasDirectAccess: row?.triggeredBy === scope.viewerId || !!row,
        capabilities: ['view-run'],
      };
    }
    if (this.source === 'meeting') {
      const meetingId = resourceId.replace(/^(meeting|recording|transcript):/, '');
      const row = batch.recordings.get(meetingId) ?? undefined;
      const people = new Set<string>([
        ...(Array.isArray(row?.participants)
          ? row.participants.flatMap((value) => value && typeof value === 'object' && typeof (value as { userId?: unknown }).userId === 'string'
            ? [(value as { userId: string }).userId] : [])
          : []),
        ...(typeof row?.startedBy === 'string' ? [row.startedBy] : []),
      ]);
      // A room that has not happened yet has no recording to be authorized
      // from — it is a booking, and the people it concerns are whoever made it
      // and whoever is expected on it. Looked up only for a meeting with no
      // recording, which is the rare case, and memoized for the request.
      if (!row) {
        if (!batch.rooms.has(meetingId)) {
          batch.rooms.set(meetingId, this.roomPeople(scope.organizationId, meetingId));
        }
        for (const person of await (batch.rooms.get(meetingId) as Promise<string[]>)) people.add(person);
      }
      return {
        ownerValid: people.has(scope.personId),
        viewerHasDirectAccess: people.has(scope.viewerId),
        capabilities: resourceId.startsWith('transcript:') ? ['view-transcript'] : ['join-meeting'],
      };
    }
    const candidates = resourceId.includes(':')
      ? [resourceId]
      : this.source === 'cloud-compute'
        ? [`box:${resourceId}`, `job:${resourceId}`]
        : [`machine:${resourceId}`, `job:${resourceId}`];
    const memoKey = candidates.join('|');
    const memoized = batch.compute.get(memoKey);
    if (memoized) return memoized;
    const pending = this.authorizeCompute(scope, candidates);
    batch.compute.set(memoKey, pending);
    return pending;
  }

  /** One compute-source round trip per resource, however many targets name it. */
  private async authorizeCompute(scope: WorkGraphPolicyScope, candidates: string[]): Promise<SourceAuthorization> {
    let unavailable = false;
    for (const candidate of candidates) {
      const response = await computeSourceCall<{
        allowed: boolean;
        directAccess: boolean;
        capabilities: WorkCollaborationCapability[];
      }>('authorize', {
        requestId: randomUUID(),
        organizationId: scope.organizationId,
        viewerId: scope.viewerId,
        personId: scope.personId,
        resource: { source: this.source, resourceId: candidate },
      });
      if (response.decision === 'unavailable') { unavailable = true; continue; }
      if (response.decision !== 'allow' || response.value.allowed !== true) continue;
      return {
        ownerValid: true,
        viewerHasDirectAccess: response.value.directAccess === true,
        capabilities: response.value.capabilities.filter((value) => value === 'observe-terminal' || value === 'control-terminal'),
      };
    }
    if (unavailable) throw new Error('compute source authorization unavailable');
    return { ownerValid: false, viewerHasDirectAccess: false, capabilities: [] };
  }
}

export class DynamoSourceActionAdapter implements WorkSourceActionAdapter {
  constructor(readonly source: WorkSourceKind, private readonly client: Ddb) {}

  async exchangeAction(
    scope: WorkGraphPolicyScope,
    target: WorkAclTarget,
    capability: WorkCollaborationCapability,
    acceptedRequest?: WorkCollaborationRequest,
  ): Promise<{ decision: 'allow'; descriptor: unknown } | { decision: 'deny' | 'unavailable' }> {
    if (target.kind !== 'node' || target.sourceRef.source !== this.source) return { decision: 'deny' };
    const resourceId = sourceOwnedResourceId(target, this.source, target.sourceRef.resourceId);
    if (this.source === 'document' && (capability === 'view-document' || capability === 'edit-document')) {
      // A `document` node's resourceId is the bare documentId; a `change`
      // (revision) node's is `${documentId}:${revisionId}` — see
      // projectEvent.js's `kind: 'change'` construction. Splitting on the
      // LAST colon recovers the documentId in both shapes, EXCEPT when the
      // colon sits at position 0 (no real documentId before it) — a
      // degenerate resourceId that should never reach here validated, but
      // `activity.ts`'s own revision-history builder already treats the same
      // shape as untrustworthy (`separator <= 0` — see its `revisionsByDocument`
      // loop) rather than as ordinary data. This mirrors that distrust: a
      // route built from an empty documentId is `/documents/` — a
      // syntactically "safe" path (isSafeWorkGraphActionPath doesn't reject
      // it) that nonetheless names no document, and silently lands the host
      // on the bare explorer instead of the document it asked to open. Refuse
      // it here instead, so the caller gets an honest 404 rather than a
      // lying 200.
      const separator = resourceId.lastIndexOf(':');
      const documentId = separator > 0 ? resourceId.slice(0, separator) : separator === -1 ? resourceId : '';
      if (!documentId) return { decision: 'deny' };
      return { decision: 'allow', descriptor: { kind: 'route', capability, path: `/documents/${encodeURIComponent(documentId)}` } };
    }
    if (this.source === 'conversation' && (capability === 'message' || capability === 'join-conversation')) {
      return { decision: 'allow', descriptor: { kind: 'route', capability, path: `/social/chat/${encodeURIComponent(resourceId)}` } };
    }
    if (this.source === 'pipeline' && capability === 'view-run') {
      // Opening a step opens the run it belongs to — the run page is where a
      // step is actually readable.
      const rows = await scanAll(this.client, {
        TableName: PIPELINE_RUNS_TABLE,
        FilterExpression: 'runId = :run',
        ExpressionAttributeValues: { ':run': parsePipelineStepResourceId(resourceId).runId },
      }, 2);
      const row = rows.length === 1 ? rows[0] : undefined;
      if (typeof row?.pipelineId !== 'string' || typeof row.runId !== 'string') return { decision: 'deny' };
      return { decision: 'allow', descriptor: { kind: 'route', capability, path: `/pipelines/${encodeURIComponent(row.pipelineId)}/runs/${encodeURIComponent(row.runId)}` } };
    }
    if (this.source === 'meeting' && (capability === 'view-transcript' || capability === 'join-meeting')) {
      const rows = await scanAll(this.client, {
        TableName: RECORDINGS_TABLE,
        FilterExpression: 'callId = :id OR recordingId = :id OR transcriptId = :id',
        ExpressionAttributeValues: { ':id': resourceId.replace(/^(meeting|recording|transcript):/, '') },
      }, 2);
      const row = rows.length === 1 ? rows[0] : undefined;
      if (typeof row?.recordingId !== 'string') return { decision: 'deny' };
      return { decision: 'allow', descriptor: { kind: 'route', capability, path: `/social/recordings/${encodeURIComponent(row.recordingId)}` } };
    }
    if ((this.source === 'cloud-compute' || this.source === 'local-compute')
      && (capability === 'observe-terminal' || capability === 'control-terminal')) {
      if (capability === 'control-terminal' && !acceptedRequest) return { decision: 'deny' };
      const requestKey = acceptedRequest?.id ?? randomUUID();
      const sourceRequestId = randomUUID();
      const requestedExpiresAt = acceptedRequest?.expiresAt
        ?? new Date(Date.now() + 5 * 60_000).toISOString();
      const opened = await computeSourceCall<{
        allowed: boolean;
        session: { sessionId: string; capability: 'observe' | 'control'; expiresAt: string; active: boolean };
      }>('open', {
        requestId: sourceRequestId,
        organizationId: scope.organizationId,
        viewerId: scope.viewerId,
        personId: scope.personId,
        resource: { source: this.source, resourceId },
        capability: capability === 'control-terminal' ? 'control' : 'observe',
        expiresAt: requestedExpiresAt,
      });
      if (opened.decision !== 'allow') return { decision: opened.decision };
      if (opened.value.allowed !== true
        || typeof opened.value.session?.sessionId !== 'string'
        || typeof opened.value.session.expiresAt !== 'string') return { decision: 'unavailable' };
      const sessionId = opened.value.session.sessionId;
      await this.client.send(new PutCommand({
        TableName: WORK_GRAPH_TABLE,
        Item: {
          PK: `WGCAPABILITY#${scope.organizationId}#${scope.personId}`,
          SK: `REQUEST#${requestKey}#SESSION#${sessionId}`,
          itemKind: 'work-capability-session',
          organizationId: scope.organizationId,
          viewerId: scope.viewerId,
          personId: scope.personId,
          requestKey,
          sessionId,
          source: this.source,
          capability,
          expiresAt: opened.value.session.expiresAt,
          ttl: Math.floor(Date.parse(opened.value.session.expiresAt) / 1_000),
        },
        ConditionExpression: 'attribute_not_exists(PK) AND attribute_not_exists(SK)',
      }));
      return {
        decision: 'allow',
        descriptor: {
          kind: 'source-session', capability, sessionId,
          connectPath: `/social/work-graph/compute/${encodeURIComponent(scope.personId)}/${encodeURIComponent(requestKey)}/${encodeURIComponent(sessionId)}`,
          expiresAt: opened.value.session.expiresAt,
        },
      };
    }
    return { decision: 'unavailable' };
  }
}

export interface StoredComputeCapabilitySession {
  organizationId: string;
  viewerId: string;
  personId: string;
  requestKey: string;
  sessionId: string;
  source: 'cloud-compute' | 'local-compute';
  capability: 'observe-terminal' | 'control-terminal';
  expiresAt: string;
}

function readComputeSession(value: Record<string, unknown> | undefined): StoredComputeCapabilitySession | null {
  if (!value || value.itemKind !== 'work-capability-session'
    || typeof value.organizationId !== 'string' || typeof value.viewerId !== 'string'
    || typeof value.personId !== 'string' || typeof value.requestKey !== 'string'
    || typeof value.sessionId !== 'string' || typeof value.expiresAt !== 'string'
    || (value.source !== 'cloud-compute' && value.source !== 'local-compute')
    || (value.capability !== 'observe-terminal' && value.capability !== 'control-terminal')) return null;
  return value as unknown as StoredComputeCapabilitySession;
}

function computeSessionKey(
  organizationId: string,
  personId: string,
  requestKey: string,
  sessionId: string,
): { PK: string; SK: string } {
  return {
    PK: `WGCAPABILITY#${organizationId}#${personId}`,
    SK: `REQUEST#${requestKey}#SESSION#${sessionId}`,
  };
}

export async function loadComputeCapabilitySession(
  organizationId: string,
  viewerId: string,
  personId: string,
  requestKey: string,
  sessionId: string,
): Promise<StoredComputeCapabilitySession | null> {
  const result = await docClient.send(new GetCommand({
    TableName: WORK_GRAPH_TABLE,
    Key: computeSessionKey(organizationId, personId, requestKey, sessionId),
    ConsistentRead: true,
  }));
  const session = readComputeSession(result.Item);
  return session
    && session.organizationId === organizationId
    && session.viewerId === viewerId
    && session.personId === personId
    && session.requestKey === requestKey
    && session.sessionId === sessionId
    && Date.parse(session.expiresAt) > Date.now()
    ? session : null;
}

export async function useComputeCapabilitySession(
  session: StoredComputeCapabilitySession,
): Promise<{ allowed: true; result?: { status?: string; cancelled?: boolean } } | { allowed: false }> {
  const response = await computeSourceCall<{
    allowed: boolean;
    result?: { status?: string; cancelled?: boolean };
  }>('use', {
    requestId: randomUUID(),
    sessionId: session.sessionId,
    capability: session.capability === 'control-terminal' ? 'control' : 'observe',
  });
  if (response.decision !== 'allow' || response.value.allowed !== true) return { allowed: false };
  return { allowed: true, ...(response.value.result ? { result: response.value.result } : {}) };
}

export async function revalidateComputeCapabilitySession(
  session: StoredComputeCapabilitySession,
): Promise<boolean> {
  const response = await computeSourceCall<{ allowed: boolean }>('revalidate', {
    requestId: randomUUID(), sessionId: session.sessionId,
  });
  return response.decision === 'allow' && response.value.allowed === true;
}

export async function closeComputeCapabilitySession(session: StoredComputeCapabilitySession): Promise<void> {
  await docClient.send(new DeleteCommand({
    TableName: WORK_GRAPH_TABLE,
    Key: computeSessionKey(session.organizationId, session.personId, session.requestKey, session.sessionId),
  }));
  await computeSourceCall('close', { requestId: randomUUID(), sessionId: session.sessionId });
}

async function closeComputeSessions(
  organizationId: string,
  personId: string,
  requestKey?: string,
): Promise<void> {
  let exclusiveStartKey: Record<string, unknown> | undefined;
  do {
    const result = await docClient.send(new QueryCommand({
      TableName: WORK_GRAPH_TABLE,
      KeyConditionExpression: requestKey
        ? 'PK = :pk AND begins_with(SK, :request)'
        : 'PK = :pk',
      ExpressionAttributeValues: {
        ':pk': `WGCAPABILITY#${organizationId}#${personId}`,
        ...(requestKey ? { ':request': `REQUEST#${requestKey}#` } : {}),
      },
      ConsistentRead: true,
      ...(exclusiveStartKey ? { ExclusiveStartKey: exclusiveStartKey } : {}),
    }));
    for (const item of result.Items ?? []) {
      const session = readComputeSession(item);
      if (session) await closeComputeCapabilitySession(session);
    }
    exclusiveStartKey = result.LastEvaluatedKey;
  } while (exclusiveStartKey);
}

// The commit nudges gateways straight away over pub/sub; the outbox drainer's
// copy of the same signal is the catch-up for a nudge that was lost.
const graphRepository = new WorkGraphRepository(docClient, projectWorkEvent, (committed) => {
  void publishProjectionChangedSignal(committed).catch(() => undefined);
});
const sharingRepository = new WorkSharingRepository(docClient);
const collaborationRepository = new WorkCollaborationRepository(docClient);
const directory = new DynamoWorkGraphDirectory(docClient);
const sourceKinds: WorkSourceKind[] = ['cloud-compute', 'local-compute', 'document', 'pipeline', 'conversation', 'meeting'];
const sourceAcls = new Map(sourceKinds.map((source) => [source, new DynamoSourceAclAdapter(source, docClient)]));
const sourceActions = new Map(sourceKinds.map((source) => [source, new DynamoSourceActionAdapter(source, docClient)]));

function cursorSecret(): string {
  const value = process.env.WORK_GRAPH_CURSOR_SECRET || process.env.SERVICE_AUTH_SECRET;
  if (!value || Buffer.byteLength(value, 'utf8') < 32) {
    if (process.env.ACTIVE_NOW_WORK_GRAPH_ENABLED === 'true') {
      throw new Error('WORK_GRAPH_CURSOR_SECRET (or SERVICE_AUTH_SECRET) must contain at least 32 bytes');
    }
    return 'active-now-disabled-local-cursor-secret';
  }
  return value;
}

export const workGraphOrchestrator = new WorkGraphOrchestrator({
  graph: graphRepository,
  sharing: sharingRepository,
  collaboration: collaborationRepository,
  directory,
  locator: { locate: (organizationId, target) => graphRepository.locateTarget(organizationId, target) },
  collaborationLifecycle: {
    // Gateway capability sessions reauthorize every operation and expire from
    // the durable request. A rejected notification can delay closure but can
    // never make a revoked request usable again.
    async accepted() {},
    async revoked(request) {
      await closeComputeSessions(request.organizationId, request.ownerId, request.id);
    },
  },
  sharingLifecycle: {
    async revoked(grant) {
      await closeComputeSessions(grant.organizationId, grant.ownerId);
    },
  },
  sourceAcls,
  sourceActions,
  cursorCodec: createWorkGraphCursorCodec(cursorSecret()),
  activityReader: new DynamoWorkActivityReader(docClient),
  async policyRevision(scope) {
    const viewer = await directory.resolveViewerById(scope.organizationId, scope.viewerId);
    if (!viewer) return 'unavailable';
    const grants = scope.viewerId === scope.personId
      ? []
      : await sharingRepository.listEffectiveGrants(scope.organizationId, scope.personId, viewer, new Date().toISOString());
    return createHash('sha256').update(JSON.stringify({
      organizationId: scope.organizationId,
      viewerId: scope.viewerId,
      personId: scope.personId,
      conversations: [...viewer.conversationIds].sort(),
      grants: grants.map((grant) => ({ id: grant.id, revision: grant.revision, state: grant.state, expiresAt: grant.expiresAt })).sort((a, b) => a.id.localeCompare(b.id)),
    })).digest('base64url').slice(0, 32);
  },
  now: () => new Date().toISOString(),
  id: randomUUID,
});

export { graphRepository as workGraphRepository };

export interface GatewayWorkGraphQuery {
  personId: string;
  day: string;
  timezone: string;
}

/**
 * The additive schemaVersion 2 window a gateway subscription asks this service
 * to project. Omitting it leaves the authorize/replay bodies and responses
 * byte-for-byte as they were for a v1 gateway.
 */
export interface GatewayWorkGraphViewRequest {
  windowStart?: string;
  windowEnd?: string;
  mode?: 'live' | 'as-of';
}

/**
 * The v2 activity view. It travels beside the v1 delta operations rather than
 * replacing them: these `operations` are freshness leases, a different thing
 * from the graph mutations in a delta batch.
 */
export interface GatewayWorkGraphViewV2 {
  query: WorkGraphQueryV2;
  temporal: WorkGraphSnapshotV2['temporal'];
  efforts: WorkGraphSnapshotV2['efforts'];
  details: WorkGraphSnapshotV2['details'];
  operations: WorkGraphSnapshotV2['operations'];
  eventBuckets: WorkGraphSnapshotV2['eventBuckets'];
}

export interface GatewayWorkGraphAccess {
  decision: 'allow' | 'deny' | 'unavailable';
  policyRevision: string;
  grantState: 'active' | 'missing';
  expiresAt?: string;
  watermark: number;
  /** Echoed only for a schemaVersion 2 request. Its absence means v1-only. */
  schemaVersion?: 2;
  /**
   * Present only for an allowed schemaVersion 2 request. It is derived from
   * the same authorization decision as the `policyRevision` beside it.
   */
  view?: GatewayWorkGraphViewV2;
}

/**
 * Projects the authorized v2 view for one gateway subscription window. The
 * strict shared validator runs first, so a view that cannot satisfy the v2
 * invariants fails here instead of reaching a browser.
 */
async function gatewayWorkGraphView(
  viewer: AuthenticatedWorkGraphViewer,
  query: GatewayWorkGraphQuery,
  requested: GatewayWorkGraphViewRequest,
): Promise<GatewayWorkGraphViewV2> {
  const snapshot = await workGraphOrchestrator.getGraphV2(
    viewer,
    query.personId,
    query.day,
    query.timezone,
    {
      ...(requested.windowStart ? { windowStart: requested.windowStart } : {}),
      ...(requested.windowEnd ? { windowEnd: requested.windowEnd } : {}),
      ...(requested.mode ? { mode: requested.mode } : {}),
    },
  );
  return {
    query: snapshot.query,
    temporal: snapshot.temporal,
    efforts: snapshot.efforts,
    details: snapshot.details,
    operations: snapshot.operations,
    eventBuckets: snapshot.eventBuckets,
  };
}

/**
 * Re-resolves the gateway's verified identity against platform-owned account
 * and membership rows. No membership or policy revision supplied by a socket
 * is trusted at this boundary.
 */
export async function authorizeGatewayWorkGraph(
  organizationId: string,
  viewerId: string,
  query: GatewayWorkGraphQuery,
  requestedView?: GatewayWorkGraphViewRequest,
): Promise<GatewayWorkGraphAccess> {
  const viewer = await directory.resolveViewerById(organizationId, viewerId);
  if (!viewer) return { decision: 'deny', policyRevision: 'unavailable', grantState: 'missing', watermark: 0 };
  try {
    const result = await workGraphOrchestrator.getGraph(
      viewer,
      query.personId,
      query.day,
      query.timezone,
    );
    // The v2 view is derived from this same decision, never from a cached one.
    const view = requestedView
      ? await gatewayWorkGraphView(viewer, query, requestedView)
      : undefined;
    let expiresAt: string | undefined;
    if (viewerId !== query.personId) {
      const grants = (await sharingRepository.listEffectiveGrants(
        organizationId,
        query.personId,
        viewer,
        new Date().toISOString(),
      )).filter((grant) => !('dayFrom' in grant)
        || (grant.dayFrom <= query.day && query.day <= grant.dayThrough));
      expiresAt = grants.map((grant) => grant.expiresAt).sort()[0];
    }
    return {
      decision: 'allow',
      policyRevision: result.snapshot.scope.policyRevision,
      grantState: 'active',
      ...(expiresAt ? { expiresAt } : {}),
      watermark: result.snapshot.watermark,
      ...(view ? { schemaVersion: 2 as const, view } : {}),
    };
  } catch (error) {
    if (error instanceof WorkGraphUnavailableError) {
      return { decision: 'deny', policyRevision: 'unavailable', grantState: 'missing', watermark: 0 };
    }
    return { decision: 'unavailable', policyRevision: 'unavailable', grantState: 'missing', watermark: 0 };
  }
}

export async function replayGatewayWorkGraph(
  organizationId: string,
  viewerId: string,
  query: GatewayWorkGraphQuery,
  request: {
    afterWatermark: number;
    throughWatermark: number;
    since: string;
    limit: number;
    /**
     * The authorization revision the caller's stream last accepted. When the
     * current one differs, the replay runs to the current watermark and
     * advances the stream only if entities created inside it explain the
     * whole difference (NFR #22); otherwise `policyChanged`.
     */
    fromPolicyRevision?: string;
  },
  requestedView?: GatewayWorkGraphViewRequest,
): Promise<{
  batches: Array<{
    previousWatermark: number;
    watermark: number;
    policyRevision: string;
    operations: WorkGraphDeltaOperation[];
    committedAt: string;
    /** Set when this batch moves the stream from an older, verified revision. */
    policyAdvancedFrom?: string;
    /** Present only for a schemaVersion 2 request. */
    view?: GatewayWorkGraphViewV2;
  }>;
  complete: boolean;
  /** The revision the batches carry; with `fromPolicyRevision`, the one to accept. */
  policyRevision?: string;
  /** `fromPolicyRevision` cannot be advanced to the current one: invalidate. */
  policyChanged?: true;
  /** Echoed only for a schemaVersion 2 request, with the refreshed view. */
  schemaVersion?: 2;
  view?: GatewayWorkGraphViewV2;
}> {
  // A non-UTC local day spans two UTC projection partitions; its watermark
  // packs both partitions' revisions, and `listLocalDayDeltas` replays each
  // partition from its own cursor and merges them (NFR #22). A cursor it
  // cannot split (issued before the packing) answers incomplete: one reset.
  const viewer = await directory.resolveViewerById(organizationId, viewerId);
  if (!viewer) return { batches: [], complete: false };
  const current = await workGraphOrchestrator.getGraph(
    viewer,
    query.personId,
    query.day,
    query.timezone,
  );
  if (request.throughWatermark > current.snapshot.watermark) return { batches: [], complete: false };
  const currentRevision = current.snapshot.scope.policyRevision;
  const advancing = request.fromPolicyRevision !== undefined && request.fromPolicyRevision !== currentRevision;
  // Proving an advance needs every entity created up to the state the current
  // revision describes, so it replays through the current watermark.
  const throughWatermark = advancing ? current.snapshot.watermark : request.throughWatermark;
  const stored = await graphRepository.listLocalDayDeltas(
    organizationId,
    query.personId,
    query.day,
    query.timezone,
    request.afterWatermark,
    throughWatermark,
    request.limit,
  );
  const visibleNodes = new Map(current.snapshot.nodes.map((node) => [node.id, node]));
  const visibleEdges = new Map(current.snapshot.edges.map((edge) => [edge.id, edge]));
  const authorizedNodes = new Map(current.authorization.nodes.map((node) => [node.id, node]));
  const authorizedEdges = new Map(current.authorization.edges.map((edge) => [edge.id, edge]));
  // Digests of disclosed entities first created inside this replay: the only
  // difference an advance may explain.
  const created: bigint[] = [];
  // With a revision to check against, the answer is only ever returned when
  // the visible set at the stream's revision is contained in the current one
  // (equal, or grown by proven additions). A node the viewer cannot see now
  // was then never visible to this stream, so its change or removal is
  // skipped instead of forcing a refetch — a private document edited on a
  // shared day was a reset for every non-owner viewer.
  const setChecked = request.fromPolicyRevision !== undefined;
  let safe = stored.complete;
  const mapped = stored.batches.map((batch) => {
    if (Date.parse(batch.committedAt) < Date.parse(request.since)) safe = false;
    const operations: WorkGraphDeltaOperation[] = [];
    for (const operation of batch.operations) {
      if (operation.kind === 'upsert-node') {
        const publicId = current.authorization.publicTargetIds.get(operation.node.id);
        const node = publicId ? visibleNodes.get(publicId) : undefined;
        if (node) operations.push({ kind: 'upsert-node', node });
        else if (!setChecked) safe = false; // an opaque prior node id cannot be safely removed
        const authorized = publicId ? authorizedNodes.get(publicId) : undefined;
        if (operation.created && authorized) created.push(authorizationEntityDigest('node', authorized));
      } else if (operation.kind === 'remove-node') {
        // Disclosure may have used a reader-scoped opaque id. Only the set
        // check can say the viewer never held it (a held node that went away
        // changes the revision non-additively, and the stream is invalidated).
        if (!setChecked) safe = false;
      } else if (operation.kind === 'upsert-edge') {
        const edge = visibleEdges.get(operation.edge.id);
        operations.push(edge
          ? { kind: 'upsert-edge', edge }
          : { kind: 'remove-edge', edgeId: operation.edge.id });
        const authorized = authorizedEdges.get(operation.edge.id);
        if (operation.created && authorized) created.push(authorizationEntityDigest('edge', authorized));
      } else {
        operations.push({ kind: 'remove-edge', edgeId: operation.edgeId });
      }
    }
    return {
      previousWatermark: batch.previousWatermark,
      watermark: batch.watermark,
      policyRevision: currentRevision,
      operations,
      committedAt: batch.committedAt,
    };
  });
  if (advancing) {
    // A stream may only move to the current revision when the replay reaches
    // the state it describes and the entities created on the way account for
    // every bit of the difference. Anything else is a real permission change.
    const reachesCurrent = safe && (mapped.at(-1)?.watermark ?? request.afterWatermark) === current.snapshot.watermark;
    if (!reachesCurrent) return { batches: [], complete: false };
    if (!revisionExplainedByAdditions(request.fromPolicyRevision!, currentRevision, created)) {
      return { batches: [], complete: false, policyChanged: true };
    }
  }
  const batches = advancing
    ? mapped.map((batch) => ({ ...batch, policyAdvancedFrom: request.fromPolicyRevision! }))
    : mapped;
  if (!requestedView) {
    return { batches, complete: safe, ...(request.fromPolicyRevision !== undefined ? { policyRevision: currentRevision } : {}) };
  }
  // The activity layer is derived, not delta-encoded: every batch carries the
  // whole refreshed view, so effort membership can never drift away from the
  // node/edge state the same authorization decision produced. A zero-batch
  // replay still returns it, because a lease can expire with no mutation.
  const view = await gatewayWorkGraphView(viewer, query, requestedView);
  return {
    batches: batches.map((batch) => ({ ...batch, view })),
    complete: safe,
    ...(request.fromPolicyRevision !== undefined ? { policyRevision: currentRevision } : {}),
    schemaVersion: 2 as const,
    view,
  };
}
