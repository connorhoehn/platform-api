import { createHash, randomUUID } from 'crypto';
import {
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
import { WorkSharingRepository, type SharingAudience } from '../repositories/WorkSharingRepository';
import {
  WorkCollaborationRepository,
  type WorkCollaborationCapability,
  type WorkCollaborationRequest,
} from '../repositories/WorkCollaborationRepository';
import { WorkSourceAccountRepository } from '../repositories/WorkSourceAccountRepository';
import { createWorkGraphCursorCodec } from './cursor';
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

async function scanAll(
  client: Ddb,
  input: ConstructorParameters<typeof ScanCommand>[0],
  maximum = MAX_DIRECTORY_USERS,
): Promise<Record<string, unknown>[]> {
  const rows: Record<string, unknown>[] = [];
  let ExclusiveStartKey: Record<string, unknown> | undefined;
  do {
    const result = await client.send(new ScanCommand({
      ...input,
      ...(ExclusiveStartKey ? { ExclusiveStartKey } : {}),
      Limit: Math.min(100, maximum - rows.length),
    }));
    rows.push(...((result.Items ?? []) as Record<string, unknown>[]));
    ExclusiveStartKey = result.LastEvaluatedKey;
  } while (ExclusiveStartKey && rows.length < maximum);
  return rows;
}

class DynamoWorkGraphDirectory implements WorkGraphDirectory {
  private readonly sourceAccounts: WorkSourceAccountRepository;

  constructor(private readonly client: Ddb) {
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
    const now = new Date().toISOString();
    await this.client.send(new PutCommand({
      TableName: WORK_GRAPH_TABLE,
      Item: {
        PK: `WGIDENTITY#${organizationId}`,
        SK: `USER#${viewerId}`,
        itemKind: 'identity',
        organizationId,
        viewerId,
        lastSeenAt: now,
      },
    }));
  }

  async resolveViewer(subject: string, organizationClaim?: string): Promise<AuthenticatedWorkGraphViewer | null> {
    if (!subject || !organizationClaim) return null;
    await this.registerIdentity(organizationClaim, subject);
    const joins = await Promise.all([
      this.sourceAccounts.ensureVerifiedSourcePrincipal({ source: 'conversation', producerServiceId: 'websocket-gateway', organizationId: organizationClaim, subject }),
      this.sourceAccounts.ensureVerifiedSourcePrincipal({ source: 'document', producerServiceId: 'websocket-gateway', organizationId: organizationClaim, subject }),
      this.sourceAccounts.ensureVerifiedSourcePrincipal({ source: 'cloud-compute', producerServiceId: 'aws-agentcore', organizationId: organizationClaim, subject }),
      this.sourceAccounts.ensureVerifiedSourcePrincipal({ source: 'local-compute', producerServiceId: 'aws-agentcore', organizationId: organizationClaim, subject }),
      // Pipeline and meeting joins were only created at the authenticated
      // pipeline-trigger and video-session entrypoints, so a person who had
      // never used either had their own run and recording events rejected at
      // ingest. The bind is the same create-only one: this authenticated
      // subject to its own same-named source subject, never another actor's.
      this.sourceAccounts.ensureVerifiedSourcePrincipal({ source: 'pipeline', producerServiceId: 'platform-api', organizationId: organizationClaim, subject }),
      this.sourceAccounts.ensureVerifiedSourcePrincipal({ source: 'meeting', producerServiceId: 'platform-api', organizationId: organizationClaim, subject }),
    ]);
    if (joins.some((joined) => !joined)) return null;
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

class DynamoSourceAclAdapter implements WorkSourceAclAdapter {
  constructor(readonly source: WorkSourceKind, private readonly client: Ddb) {}

  async authorizeDisclosure(scope: WorkGraphPolicyScope, targets: WorkAclTarget[]): Promise<Map<string, WorkSourceAclDecision>> {
    const result = new Map<string, WorkSourceAclDecision>();
    for (const target of targets) {
      const decisions = await Promise.all(sourceRefs(target, this.source).map((ref) => this.authorizeRef(
        scope,
        sourceOwnedResourceId(target, this.source, ref.resourceId),
      )));
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
    const decisions = await Promise.all(sourceRefs(target, this.source).map((ref) => this.authorizeRef(
      scope,
      sourceOwnedResourceId(target, this.source, ref.resourceId),
    )));
    return decisions.length > 0
      && decisions.every((decision) => decision.ownerValid
        && decision.capabilities.includes(capability))
      ? 'allow'
      : 'deny';
  }

  private async isConversationMember(channel: string, userId: string): Promise<boolean> {
    const result = await this.client.send(new GetCommand({
      TableName: CHAT_MEMBERS_TABLE,
      Key: { channel, userId },
      ConsistentRead: true,
    }));
    return !!result.Item && result.Item.removedAt === undefined;
  }

  private async authorizeRef(scope: WorkGraphPolicyScope, resourceId: string): Promise<SourceAuthorization> {
    if (this.source === 'conversation') {
      const [owner, viewer] = await Promise.all([
        this.isConversationMember(resourceId, scope.personId),
        this.isConversationMember(resourceId, scope.viewerId),
      ]);
      return { ownerValid: owner, viewerHasDirectAccess: viewer, capabilities: ['message', 'join-conversation'] };
    }
    if (this.source === 'document') {
      const documentId = resourceId.includes(':') ? resourceId.slice(0, resourceId.lastIndexOf(':')) : resourceId;
      const row = (await this.client.send(new GetCommand({
        TableName: DOCUMENTS_TABLE,
        Key: { documentId },
        ConsistentRead: true,
      }))).Item;
      const channel = typeof row?.channel === 'string' ? row.channel : undefined;
      const ownerValid = row?.createdBy === scope.personId
        || (!!channel && await this.isConversationMember(channel, scope.personId));
      const viewer = row?.createdBy === scope.viewerId
        || (!!channel && await this.isConversationMember(channel, scope.viewerId));
      return { ownerValid, viewerHasDirectAccess: viewer, capabilities: ['view-document', ...(row?.createdBy === scope.viewerId ? ['edit-document' as const] : [])] };
    }
    if (this.source === 'pipeline') {
      const rows = await scanAll(this.client, {
        TableName: PIPELINE_RUNS_TABLE,
        FilterExpression: 'runId = :run',
        ExpressionAttributeValues: { ':run': resourceId.replace(/^run:/, '') },
      }, 2);
      const row = rows.length === 1 ? rows[0] : undefined;
      return {
        ownerValid: row?.triggeredBy === scope.personId,
        viewerHasDirectAccess: row?.triggeredBy === scope.viewerId || !!row,
        capabilities: ['view-run'],
      };
    }
    if (this.source === 'meeting') {
      const rows = await scanAll(this.client, {
        TableName: RECORDINGS_TABLE,
        FilterExpression: 'callId = :id OR recordingId = :id OR transcriptId = :id',
        ExpressionAttributeValues: { ':id': resourceId.replace(/^(meeting|recording|transcript):/, '') },
      }, 2);
      const row = rows.length === 1 ? rows[0] : undefined;
      const people = new Set<string>([
        ...(Array.isArray(row?.participants)
          ? row.participants.flatMap((value) => value && typeof value === 'object' && typeof (value as { userId?: unknown }).userId === 'string'
            ? [(value as { userId: string }).userId] : [])
          : []),
        ...(typeof row?.startedBy === 'string' ? [row.startedBy] : []),
      ]);
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

class DynamoSourceActionAdapter implements WorkSourceActionAdapter {
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
      const documentId = resourceId.includes(':') ? resourceId.slice(0, resourceId.lastIndexOf(':')) : resourceId;
      return { decision: 'allow', descriptor: { kind: 'route', capability, path: `/documents/${encodeURIComponent(documentId)}` } };
    }
    if (this.source === 'conversation' && (capability === 'message' || capability === 'join-conversation')) {
      return { decision: 'allow', descriptor: { kind: 'route', capability, path: `/social/chat/${encodeURIComponent(resourceId)}` } };
    }
    if (this.source === 'pipeline' && capability === 'view-run') {
      const rows = await scanAll(this.client, {
        TableName: PIPELINE_RUNS_TABLE,
        FilterExpression: 'runId = :run',
        ExpressionAttributeValues: { ':run': resourceId.replace(/^run:/, '') },
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

const graphRepository = new WorkGraphRepository(docClient, projectWorkEvent);
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
  request: { afterWatermark: number; throughWatermark: number; since: string; limit: number },
  requestedView?: GatewayWorkGraphViewRequest,
): Promise<{
  batches: Array<{
    previousWatermark: number;
    watermark: number;
    policyRevision: string;
    operations: WorkGraphDeltaOperation[];
    committedAt: string;
    /** Present only for a schemaVersion 2 request. */
    view?: GatewayWorkGraphViewV2;
  }>;
  complete: boolean;
  /** Echoed only for a schemaVersion 2 request, with the refreshed view. */
  schemaVersion?: 2;
  view?: GatewayWorkGraphViewV2;
}> {
  // A non-UTC local day can span two UTC projection partitions. Its snapshot
  // watermark is a composite revision, not a cursor into either partition's
  // delta log. An empty barrier is safe; any observed change must force the
  // gateway client to fetch a newly authorized composite snapshot.
  const viewer = await directory.resolveViewerById(organizationId, viewerId);
  if (!viewer) return { batches: [], complete: false };
  if (query.timezone !== 'UTC' && request.afterWatermark < request.throughWatermark) {
    // The barrier still has to answer in the version it was asked in: a v2
    // caller that receives no `schemaVersion` reads the deployment as v1-only
    // and stops asking for the activity layer altogether.
    if (!requestedView) return { batches: [], complete: false };
    return {
      batches: [],
      complete: false,
      schemaVersion: 2 as const,
      view: await gatewayWorkGraphView(viewer, query, requestedView),
    };
  }
  const current = await workGraphOrchestrator.getGraph(
    viewer,
    query.personId,
    query.day,
    query.timezone,
  );
  if (request.throughWatermark > current.snapshot.watermark) return { batches: [], complete: false };
  const stored = await graphRepository.listDeltas(
    organizationId,
    query.personId,
    query.day,
    request.afterWatermark,
    request.throughWatermark,
    request.limit,
  );
  const visibleNodes = new Map(current.snapshot.nodes.map((node) => [node.id, node]));
  const visibleEdges = new Map(current.snapshot.edges.map((edge) => [edge.id, edge]));
  let safe = stored.complete;
  const batches = stored.batches.map((batch) => {
    if (Date.parse(batch.committedAt) < Date.parse(request.since)) safe = false;
    const operations: WorkGraphDeltaOperation[] = [];
    for (const operation of batch.operations) {
      if (operation.kind === 'upsert-node') {
        const publicId = current.authorization.publicTargetIds.get(operation.node.id);
        const node = publicId ? visibleNodes.get(publicId) : undefined;
        if (node) operations.push({ kind: 'upsert-node', node });
        else safe = false; // an opaque prior node id cannot be safely removed
      } else if (operation.kind === 'remove-node') {
        safe = false; // disclosure may have used a reader-scoped opaque id
      } else if (operation.kind === 'upsert-edge') {
        const edge = visibleEdges.get(operation.edge.id);
        operations.push(edge
          ? { kind: 'upsert-edge', edge }
          : { kind: 'remove-edge', edgeId: operation.edge.id });
      } else {
        operations.push({ kind: 'remove-edge', edgeId: operation.edgeId });
      }
    }
    return {
      previousWatermark: batch.previousWatermark,
      watermark: batch.watermark,
      policyRevision: current.snapshot.scope.policyRevision,
      operations,
      committedAt: batch.committedAt,
    };
  });
  if (!requestedView) return { batches, complete: safe };
  // The activity layer is derived, not delta-encoded: every batch carries the
  // whole refreshed view, so effort membership can never drift away from the
  // node/edge state the same authorization decision produced. A zero-batch
  // replay still returns it, because a lease can expire with no mutation.
  const view = await gatewayWorkGraphView(viewer, query, requestedView);
  return {
    batches: batches.map((batch) => ({ ...batch, view })),
    complete: safe,
    schemaVersion: 2 as const,
    view,
  };
}
