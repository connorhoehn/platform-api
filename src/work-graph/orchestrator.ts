import type {
  WorkProjectionState,
  WorkSourceKind,
} from '../repositories/WorkGraphRepository';
import type {
  SharingAudience,
  SharingSelection,
  StoredWorkGrant,
  WorkHistoryGrant,
  WorkSharingGrant,
  WorkSharingViewerContext,
} from '../repositories/WorkSharingRepository';
import type {
  WorkCollaborationCapability,
  WorkCollaborationRequest,
  WorkCollaborationTarget,
} from '../repositories/WorkCollaborationRepository';
import {
  authorizeWorkCapability,
  authorizeWorkGraph,
  grantAppliesToScope,
  type AuthorizedWorkGraph,
  type ViewerWorkEdge,
  type ViewerWorkNode,
  type WorkGraphPolicyScope,
  type WorkAclTarget,
  type WorkSourceAclAdapter,
  type WorkSourceStatus,
  sourcesForWorkTarget,
} from './policy';
import type { WorkGraphCursorCodec } from './cursor';
import { workGraphAuthorizationRevision } from './authorizationRevision';
import {
  buildWorkActivity,
  parsePreviewHandle,
  type RevisionPreview,
  type WorkActivityReader,
} from './activity';
import {
  buildWorkGraphSnapshotV2,
  workDayWindow,
  type WorkGraphActivityV2,
  type WorkGraphQueryV2,
  type WorkGraphSnapshotV2,
} from '@connorhoehn/realtime-modules/work-graph';

const MAX_SHARING_MS = 24 * 60 * 60 * 1_000;
const MAX_HISTORY_MS = 7 * 24 * 60 * 60 * 1_000;
const MIN_SHARING_MS = 5 * 60 * 1_000;
const MAX_COLLABORATION_MS = 24 * 60 * 60 * 1_000;
const CURSOR_LIFETIME_MS = 15 * 60 * 1_000;

export interface AuthenticatedWorkGraphViewer extends WorkSharingViewerContext {
  organizationId: string;
}

export interface WorkGraphDirectory {
  /** Joins an authenticated platform subject to its server-owned account/org. */
  resolveViewer(subject: string, organizationClaim?: string): Promise<AuthenticatedWorkGraphViewer | null>;
  /** Re-resolves memberships for a stored platform account during acceptance. */
  resolveViewerById(organizationId: string, viewerId: string): Promise<AuthenticatedWorkGraphViewer | null>;
  personBelongsToOrganization(organizationId: string, personId: string): Promise<boolean>;
  listPeople(organizationId: string): Promise<Array<{
    personId: string;
    updatedAt: string;
    fresh: boolean;
  }>>;
  audienceBelongsToOrganization(organizationId: string, ownerId: string, audience: SharingAudience): Promise<boolean>;
}

export interface WorkGraphStore {
  loadProjection(organizationId: string, actorId: string, day: string): Promise<WorkProjectionState | null>;
  /** Implemented by the durable UTC-sharded repository for IANA-local reads. */
  loadProjectionForLocalDay?(
    organizationId: string,
    actorId: string,
    day: string,
    timezone: string,
  ): Promise<WorkProjectionState | null>;
}

export interface WorkSharingStore {
  getGrant(organizationId: string, grantId: string, now: string): Promise<StoredWorkGrant | null>;
  listEffectiveGrants(
    organizationId: string,
    ownerId: string,
    viewer: WorkSharingViewerContext,
    now: string,
  ): Promise<StoredWorkGrant[]>;
  putGrant(grant: StoredWorkGrant, expectedRevision: number | null): Promise<StoredWorkGrant>;
  listOwnedGrants?(
    organizationId: string,
    ownerId: string,
    now: string,
  ): Promise<StoredWorkGrant[]>;
  /** Required for atomic current+history creation; partial writes are forbidden. */
  putGrantPair?(
    current: WorkSharingGrant,
    history: WorkHistoryGrant,
  ): Promise<{ current: WorkSharingGrant; history: WorkHistoryGrant }>;
}

export interface WorkCollaborationStore {
  getRequest(organizationId: string, requestId: string): Promise<WorkCollaborationRequest | null>;
  putRequest(
    request: WorkCollaborationRequest,
    expectedRevision: number | null,
  ): Promise<WorkCollaborationRequest>;
  findAcceptedRequest(
    organizationId: string,
    ownerId: string,
    requesterId: string,
    target: WorkCollaborationTarget,
    capability: WorkCollaborationCapability,
    now: string,
  ): Promise<WorkCollaborationRequest | null>;
  listOwnedRequests?(
    organizationId: string,
    ownerId: string,
    now: string,
  ): Promise<WorkCollaborationRequest[]>;
}

export interface WorkTargetLocator {
  locate(
    organizationId: string,
    target: WorkCollaborationTarget,
  ): Promise<{ personId: string; day: string; timezone: string } | null>;
}

export interface CollaborationLifecycleEnforcer {
  accepted(request: WorkCollaborationRequest): Promise<void>;
  revoked(request: WorkCollaborationRequest): Promise<void>;
}

export interface SharingLifecycleEnforcer {
  revoked(grant: StoredWorkGrant): Promise<void>;
}

export type WorkGraphActionDescriptor =
  | { kind: 'route'; capability: WorkCollaborationCapability; path: string }
  | {
    kind: 'source-session';
    capability: 'observe-terminal' | 'control-terminal';
    sessionId: string;
    connectPath: string;
    expiresAt: string;
  };

export type WorkSourceActionResult =
  | { decision: 'allow'; descriptor: unknown }
  | { decision: 'deny' | 'unavailable' };

export interface WorkSourceActionAdapter {
  readonly source: WorkSourceKind;
  exchangeAction(
    scope: WorkGraphPolicyScope,
    target: WorkAclTarget,
    capability: WorkCollaborationCapability,
    acceptedRequest?: WorkCollaborationRequest,
  ): Promise<WorkSourceActionResult>;
}

export interface WorkReference {
  version: 1;
  personId: string;
  day: string;
  timezone: string;
  sessionId?: string;
  target: WorkCollaborationTarget;
  eventId?: string;
  observedAt?: string;
}

export interface WorkReferenceResolution {
  reference: WorkReference;
  state: 'available' | 'unavailable';
  node?: ViewerWorkNode;
  edge?: ViewerWorkEdge;
}

export interface WorkGraphSnapshot {
  schemaVersion: 1;
  scope: {
    personId: string;
    day: string;
    timezone: string;
    policyRevision: string;
  };
  revision: number;
  watermark: number;
  cursor: string;
  nodes: ViewerWorkNode[];
  edges: ViewerWorkEdge[];
  sources: WorkSourceStatus[];
  partial: boolean;
}

/** Selected interval inside one person's real IANA local day. */
export interface WorkGraphWindowInput {
  windowStart?: string;
  windowEnd?: string;
  mode?: 'live' | 'as-of';
}

/** Default interval when the caller asks for v2 without naming one. */
const DEFAULT_WINDOW_MS = 15 * 60 * 1_000;

export interface TranscriptCitation {
  segmentId: string;
  at: string;
  speaker: string;
  text: string;
}

export interface TranscriptAnswer {
  /** Quoted transcript text, or null when nothing in it supports an answer. */
  answer: string | null;
  /** Always extractive: this service never generates transcript content. */
  grounding: 'transcript-extract';
  citations: TranscriptCitation[];
}

const STOP_WORDS = new Set([
  'a', 'an', 'and', 'any', 'anything', 'are', 'as', 'at', 'be', 'but', 'by', 'can', 'did',
  'do', 'does', 'for', 'from', 'had', 'has', 'have', 'how', 'i', 'in', 'is', 'it', 'me',
  'of', 'on', 'or', 'that', 'the', 'their', 'there', 'they', 'this', 'to', 'was', 'we',
  'were', 'what', 'when', 'where', 'which', 'who', 'why', 'will', 'with', 'you', 'your',
]);
const MAX_CITATIONS = 3;

function terms(value: string): string[] {
  return value
    .toLowerCase()
    .split(/[^\p{L}\p{N}]+/u)
    .filter((word) => word.length > 2 && !STOP_WORDS.has(word));
}

/**
 * Ranks segments by how much of the question they actually contain. Ties keep
 * transcript order so the quoted answer still reads chronologically.
 */
function answerFromTranscript(
  question: string,
  segments: ReadonlyArray<{ id: string; at: string; speaker: string; text: string }>,
): TranscriptAnswer {
  const wanted = new Set(terms(question));
  if (wanted.size === 0) return { answer: null, grounding: 'transcript-extract', citations: [] };
  const scored = segments
    .map((segment, order) => {
      const words = new Set(terms(segment.text));
      let score = 0;
      for (const term of wanted) if (words.has(term)) score += 1;
      return { segment, order, score };
    })
    .filter((entry) => entry.score > 0)
    .sort((left, right) => right.score - left.score || left.order - right.order)
    .slice(0, MAX_CITATIONS)
    .sort((left, right) => left.order - right.order);
  if (scored.length === 0) return { answer: null, grounding: 'transcript-extract', citations: [] };
  const citations = scored.map((entry) => ({
    segmentId: entry.segment.id,
    at: entry.segment.at,
    speaker: entry.segment.speaker,
    text: entry.segment.text,
  }));
  return {
    answer: citations.map((citation) => `${citation.speaker}: ${citation.text}`).join(' '),
    grounding: 'transcript-extract',
    citations,
  };
}

export class WorkGraphInputError extends Error {
  readonly code = 'WORK_GRAPH_INVALID_REQUEST';
}

export class WorkGraphUnavailableError extends Error {
  readonly code = 'WORK_GRAPH_UNAVAILABLE';
}

export class WorkGraphAuthorizationUnavailableError extends Error {
  readonly code = 'WORK_GRAPH_AUTHORIZATION_UNAVAILABLE';
}

export class WorkGraphCapabilityDeniedError extends Error {
  readonly code = 'WORK_GRAPH_CAPABILITY_DENIED';
}

export class WorkGraphConflictError extends Error {
  readonly code = 'WORK_GRAPH_REVISION_CONFLICT';
}

export interface WorkGraphOrchestratorDependencies {
  graph: WorkGraphStore;
  sharing: WorkSharingStore;
  collaboration: WorkCollaborationStore;
  directory: WorkGraphDirectory;
  locator: WorkTargetLocator;
  collaborationLifecycle: CollaborationLifecycleEnforcer;
  sharingLifecycle?: SharingLifecycleEnforcer;
  sourceAcls: ReadonlyMap<WorkSourceKind, WorkSourceAclAdapter>;
  sourceActions: ReadonlyMap<WorkSourceKind, WorkSourceActionAdapter>;
  cursorCodec: WorkGraphCursorCodec;
  /** Required only to answer a schemaVersion 2 request. v1 never reads it. */
  activityReader?: WorkActivityReader;
  policyRevision(scope: Omit<WorkGraphPolicyScope, 'policyRevision'>): Promise<string>;
  now(): string;
  id(): string;
}

export interface CreateGrantInput {
  audience: SharingAudience;
  selection: SharingSelection;
  expiresAt: string;
  history?: { dayFrom: string; dayThrough: string; expiresAt: string };
}

export interface UpdateGrantInput {
  expectedRevision: number;
  action?: 'pause' | 'resume' | 'stop';
  audience?: SharingAudience;
  selection?: SharingSelection;
  expiresAt?: string;
}

export interface CreateCollaborationInput {
  target: WorkCollaborationTarget;
  capability: WorkCollaborationCapability;
  expiresAt: string;
}

function assertDuration(now: string, expiresAt: string, minimum: number, maximum: number): void {
  const start = Date.parse(now);
  const end = Date.parse(expiresAt);
  if (!Number.isFinite(end) || end - start < minimum || end - start > maximum) {
    throw new WorkGraphInputError('Expiration is outside the allowed range.');
  }
}

function unavailable(): WorkGraphUnavailableError {
  return new WorkGraphUnavailableError('This work item is unavailable.');
}

function isSafePath(value: unknown): value is string {
  if (typeof value !== 'string') return false;
  const hasUnsafeCharacter = [...value].some((character) => {
    const code = character.charCodeAt(0);
    return code < 32 || code === 127 || /\s/u.test(character);
  });
  return !hasUnsafeCharacter
    && value.length > 0
    && value.length <= 2_048
    && value.startsWith('/')
    && !value.startsWith('//')
    && !value.includes('\\')
    && !value.includes('?')
    && !value.includes('#');
}

function exactObject(value: unknown, keys: readonly string[]): value is Record<string, unknown> {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) return false;
  const own = Reflect.ownKeys(value);
  const allowed = new Set(keys);
  return own.length === allowed.size
    && own.every((key) => typeof key === 'string' && allowed.has(key));
}

function validateActionDescriptor(
  value: unknown,
  capability: WorkCollaborationCapability,
  now: string,
): WorkGraphActionDescriptor | null {
  if (exactObject(value, ['kind', 'capability', 'path'])
    && value.kind === 'route'
    && value.capability === capability
    && isSafePath(value.path)) {
    return { kind: 'route', capability, path: value.path };
  }
  if (exactObject(value, ['kind', 'capability', 'sessionId', 'connectPath', 'expiresAt'])
    && value.kind === 'source-session'
    && (capability === 'observe-terminal' || capability === 'control-terminal')
    && value.capability === capability
    && typeof value.sessionId === 'string'
    && value.sessionId.length > 0
    && value.sessionId.length <= 128
    && isSafePath(value.connectPath)
    && typeof value.expiresAt === 'string'
    && Number.isFinite(Date.parse(value.expiresAt))
    && new Date(Date.parse(value.expiresAt)).toISOString() === value.expiresAt
    && Date.parse(value.expiresAt) > Date.parse(now)) {
    return {
      kind: 'source-session',
      capability,
      sessionId: value.sessionId,
      connectPath: value.connectPath,
      expiresAt: value.expiresAt,
    };
  }
  return null;
}

export class WorkGraphOrchestrator {
  constructor(private readonly deps: WorkGraphOrchestratorDependencies) {}

  resolveViewer(subject: string, organizationClaim?: string): Promise<AuthenticatedWorkGraphViewer | null> {
    return this.deps.directory.resolveViewer(subject, organizationClaim);
  }

  async listPeople(viewer: AuthenticatedWorkGraphViewer): Promise<Array<{
    personId: string;
    sharing: 'active' | 'historical';
    fresh: boolean;
    updatedAt: string;
  }>> {
    const now = this.deps.now();
    const candidates = await this.deps.directory.listPeople(viewer.organizationId);
    const summaries = await Promise.all(candidates.map(async (candidate) => {
      if (candidate.personId === viewer.viewerId) return null;
      const grants = await this.deps.sharing.listEffectiveGrants(
        viewer.organizationId,
        candidate.personId,
        viewer,
        now,
      );
      const active = grants.filter((grant) => grant.state === 'active' && Date.parse(grant.expiresAt) > Date.parse(now));
      if (active.length === 0) return null;
      return {
        personId: candidate.personId,
        sharing: active.some((grant) => !('dayFrom' in grant)) ? 'active' as const : 'historical' as const,
        fresh: candidate.fresh && active.some((grant) => !('dayFrom' in grant)),
        updatedAt: candidate.updatedAt,
      };
    }));
    return summaries.filter((value): value is NonNullable<typeof value> => value !== null);
  }

  async getGraph(
    viewer: AuthenticatedWorkGraphViewer,
    personId: string,
    day: string,
    timezone: string,
    previousCursor?: string,
  ): Promise<{ snapshot: WorkGraphSnapshot; authorization: AuthorizedWorkGraph }> {
    if (!await this.deps.directory.personBelongsToOrganization(viewer.organizationId, personId)) throw unavailable();
    const now = this.deps.now();
    const scopeBase = {
      organizationId: viewer.organizationId,
      viewerId: viewer.viewerId,
      personId,
      day,
      timezone,
    };
    const sourceScope: WorkGraphPolicyScope = { ...scopeBase, policyRevision: await this.deps.policyRevision(scopeBase) };

    const projection = this.deps.graph.loadProjectionForLocalDay
      ? await this.deps.graph.loadProjectionForLocalDay(viewer.organizationId, personId, day, timezone)
      : await this.deps.graph.loadProjection(viewer.organizationId, personId, day);
    let grants: StoredWorkGrant[];
    if (viewer.viewerId === personId) {
      grants = [this.ownerGrant(viewer, projection, now, day)];
    } else {
      grants = (await this.deps.sharing.listEffectiveGrants(
        viewer.organizationId,
        personId,
        viewer,
        now,
      )).filter((grant) => grantAppliesToScope(grant, { day, timezone }, now));
      if (grants.length === 0) throw unavailable();
    }

    const empty: WorkProjectionState = projection ?? {
      schemaVersion: 1,
      organizationId: viewer.organizationId,
      actorId: personId,
      revision: 0,
      sessions: {},
      nodes: {},
      edges: {},
      sourceCheckpoints: {},
      appliedEventIds: [],
    };
    const authorization = await authorizeWorkGraph(empty, sourceScope, grants, this.deps.sourceAcls, now);
    const policyRevision = workGraphAuthorizationRevision(sourceScope.policyRevision, authorization);
    const scope: WorkGraphPolicyScope = { ...scopeBase, policyRevision };
    if (previousCursor && !this.deps.cursorCodec.verify(previousCursor, scope, now)) {
      throw new WorkGraphInputError('Cursor is invalid, expired, or belongs to another graph scope.');
    }
    const subscriptionGeneration = this.deps.id();
    const cursor = this.deps.cursorCodec.issue({
      schemaVersion: 1,
      ...scope,
      watermark: empty.revision,
      subscriptionGeneration,
      issuedAt: now,
      expiresAt: new Date(Date.parse(now) + CURSOR_LIFETIME_MS).toISOString(),
    });
    const nodes = authorization.nodes.slice().sort((a, b) => a.id.localeCompare(b.id)).slice(0, 250);
    const nodeIds = new Set(nodes.map((node) => node.id));
    const edges = authorization.edges.slice()
      .sort((a, b) => a.id.localeCompare(b.id))
      .filter((edge) => nodeIds.has(edge.fromId) && nodeIds.has(edge.toId))
      .slice(0, 500);
    return {
      snapshot: {
        schemaVersion: 1,
        scope: { personId, day, timezone, policyRevision },
        revision: empty.revision,
        watermark: empty.revision,
        cursor,
        nodes,
        edges,
        sources: authorization.sources,
        partial: authorization.partial,
      },
      authorization,
    };
  }

  /**
   * Resolves the requested interval against the person's real local day. DST
   * is handled by the shared day-window helper, so a 23- or 25-hour day keeps
   * exact bounds instead of assuming a fixed offset.
   */
  private resolveWindow(
    personId: string,
    day: string,
    timezone: string,
    input: WorkGraphWindowInput,
    now: string,
  ): WorkGraphQueryV2 {
    let bounds;
    try {
      bounds = workDayWindow(day, timezone);
    } catch {
      throw new WorkGraphInputError('A valid day and timezone are required.');
    }
    const dayStart = Date.parse(bounds.start);
    // The window is half-open on the day, so the last addressable instant is
    // one millisecond before the next local midnight.
    const dayEnd = Date.parse(bounds.end) - 1;
    const parse = (value: string | undefined): number | undefined => {
      if (value === undefined) return undefined;
      const parsed = Date.parse(value);
      if (!Number.isFinite(parsed) || new Date(parsed).toISOString() !== value) {
        throw new WorkGraphInputError('Window bounds must be exact ISO timestamps.');
      }
      return parsed;
    };
    const requestedEnd = parse(input.windowEnd);
    const requestedStart = parse(input.windowStart);
    const end = Math.max(dayStart + 1, Math.min(requestedEnd ?? Date.parse(now), dayEnd));
    const start = Math.max(dayStart, Math.min(requestedStart ?? end - DEFAULT_WINDOW_MS, end - 1));
    if (requestedStart !== undefined && requestedEnd !== undefined && requestedStart >= requestedEnd) {
      throw new WorkGraphInputError('Window start must precede window end.');
    }
    return {
      schemaVersion: 2,
      personId,
      day,
      timezone,
      windowStart: new Date(start).toISOString(),
      windowEnd: new Date(end).toISOString(),
      mode: input.mode === 'as-of' ? 'as-of' : 'live',
    };
  }

  /**
   * Opt-in schemaVersion 2 answer. It reuses the identical v1 authorization
   * path, then derives efforts, details, operations and event buckets from
   * that already-filtered graph. A host without an activity reader gets a 400
   * rather than an activity layer assembled from nothing.
   */
  async getActivity(
    viewer: AuthenticatedWorkGraphViewer,
    personId: string,
    day: string,
    timezone: string,
    input: WorkGraphWindowInput = {},
    previousCursor?: string,
  ): Promise<{ snapshot: WorkGraphSnapshot; query: WorkGraphQueryV2; activity: WorkGraphActivityV2 }> {
    const reader = this.deps.activityReader;
    if (!reader) throw new WorkGraphInputError('Schema version 2 is not available on this deployment.');
    const now = this.deps.now();
    const query = this.resolveWindow(personId, day, timezone, input, now);
    const { snapshot, authorization } = await this.getGraph(viewer, personId, day, timezone, previousCursor);
    const activity = await buildWorkActivity({
      query,
      nodes: snapshot.nodes,
      edges: snapshot.edges,
      authorization,
      reader,
      now,
    });
    return { snapshot, query, activity };
  }

  /** Assembles and strictly validates the browser-facing v2 snapshot body. */
  async getGraphV2(
    viewer: AuthenticatedWorkGraphViewer,
    personId: string,
    day: string,
    timezone: string,
    input: WorkGraphWindowInput = {},
    previousCursor?: string,
  ): Promise<WorkGraphSnapshotV2> {
    const { snapshot, query, activity } = await this.getActivity(viewer, personId, day, timezone, input, previousCursor);
    const built = buildWorkGraphSnapshotV2({ snapshot: snapshot as never, query, activity });
    // Failing closed keeps a partially derived activity layer off the wire.
    if (!built.ok) throw new WorkGraphUnavailableError('This work item is unavailable.');
    return built.value;
  }

  async getNode(
    viewer: AuthenticatedWorkGraphViewer,
    personId: string,
    day: string,
    timezone: string,
    id: string,
  ): Promise<ViewerWorkNode> {
    const { snapshot, authorization } = await this.getGraph(viewer, personId, day, timezone);
    const node = snapshot.nodes.find((candidate) => candidate.id === id);
    if (node) return node;
    if (authorization.unavailableTargetIds.has(id)) {
      throw new WorkGraphAuthorizationUnavailableError('Work graph authorization is temporarily unavailable.');
    }
    throw unavailable();
  }

  async getEdge(
    viewer: AuthenticatedWorkGraphViewer,
    personId: string,
    day: string,
    timezone: string,
    id: string,
  ): Promise<ViewerWorkEdge> {
    const { snapshot, authorization } = await this.getGraph(viewer, personId, day, timezone);
    const edge = snapshot.edges.find((candidate) => candidate.id === id);
    if (edge) return edge;
    if (authorization.unavailableTargetIds.has(id)) {
      throw new WorkGraphAuthorizationUnavailableError('Work graph authorization is temporarily unavailable.');
    }
    throw unavailable();
  }

  async createGrant(
    viewer: AuthenticatedWorkGraphViewer,
    input: CreateGrantInput,
  ): Promise<{ current: WorkSharingGrant; history?: WorkHistoryGrant }> {
    const now = this.deps.now();
    assertDuration(now, input.expiresAt, MIN_SHARING_MS, MAX_SHARING_MS);
    if (!await this.deps.directory.audienceBelongsToOrganization(
      viewer.organizationId,
      viewer.viewerId,
      input.audience,
    )) throw new WorkGraphInputError('Audience is not available to this account.');

    const currentId = this.deps.id();
    const historyId = input.history ? this.deps.id() : undefined;
    const current: WorkSharingGrant = {
      schemaVersion: 1,
      id: currentId,
      organizationId: viewer.organizationId,
      ownerId: viewer.viewerId,
      audience: input.audience,
      selection: input.selection,
      state: 'active',
      createdAt: now,
      updatedAt: now,
      expiresAt: input.expiresAt,
      revision: 1,
      ...(historyId ? { historyGrantId: historyId } : {}),
    };
    if (!input.history) {
      return { current: await this.deps.sharing.putGrant(current, null) as WorkSharingGrant };
    }
    assertDuration(now, input.history.expiresAt, MIN_SHARING_MS, MAX_HISTORY_MS);
    if (input.history.dayFrom > input.history.dayThrough) {
      throw new WorkGraphInputError('History day range is invalid.');
    }
    const dayRangeMs = Date.parse(`${input.history.dayThrough}T00:00:00.000Z`)
      - Date.parse(`${input.history.dayFrom}T00:00:00.000Z`);
    if (dayRangeMs >= MAX_HISTORY_MS) throw new WorkGraphInputError('History day range exceeds seven days.');
    const history: WorkHistoryGrant = {
      schemaVersion: 1,
      id: historyId!,
      organizationId: viewer.organizationId,
      ownerId: viewer.viewerId,
      audience: input.audience,
      selection: input.selection,
      state: 'active',
      createdAt: now,
      updatedAt: now,
      expiresAt: input.history.expiresAt,
      revision: 1,
      dayFrom: input.history.dayFrom,
      dayThrough: input.history.dayThrough,
    };
    if (!this.deps.sharing.putGrantPair) {
      throw new WorkGraphAuthorizationUnavailableError('Atomic history sharing is not available.');
    }
    return this.deps.sharing.putGrantPair(current, history);
  }

  async listOwnedGrants(viewer: AuthenticatedWorkGraphViewer): Promise<StoredWorkGrant[]> {
    if (!this.deps.sharing.listOwnedGrants) return [];
    return this.deps.sharing.listOwnedGrants(
      viewer.organizationId,
      viewer.viewerId,
      this.deps.now(),
    );
  }

  async updateGrant(
    viewer: AuthenticatedWorkGraphViewer,
    grantId: string,
    input: UpdateGrantInput,
  ): Promise<StoredWorkGrant> {
    const now = this.deps.now();
    const current = await this.deps.sharing.getGrant(viewer.organizationId, grantId, now);
    if (!current || current.ownerId !== viewer.viewerId) throw unavailable();
    if (current.revision !== input.expectedRevision) throw new WorkGraphConflictError('Sharing grant revision conflict.');
    if (input.action && (input.audience || input.selection || input.expiresAt)) {
      throw new WorkGraphInputError('A lifecycle action cannot be combined with grant edits.');
    }

    let next: StoredWorkGrant;
    if (input.action) {
      if (input.action === 'pause' && current.state !== 'active') throw new WorkGraphInputError('Only an active grant can pause.');
      if (input.action === 'resume' && current.state !== 'paused') throw new WorkGraphInputError('Only a paused grant can resume.');
      if (input.action === 'resume' && Date.parse(current.expiresAt) <= Date.parse(now)) throw unavailable();
      if (input.action === 'stop' && (current.state === 'stopped' || current.state === 'expired')) throw unavailable();
      next = {
        ...current,
        state: input.action === 'pause' ? 'paused' : input.action === 'resume' ? 'active' : 'stopped',
        updatedAt: now,
        revision: current.revision + 1,
      };
    } else {
      if (!input.audience && !input.selection && !input.expiresAt) {
        throw new WorkGraphInputError('At least one grant change is required.');
      }
      if (input.audience && !await this.deps.directory.audienceBelongsToOrganization(
        viewer.organizationId,
        viewer.viewerId,
        input.audience,
      )) throw new WorkGraphInputError('Audience is not available to this account.');
      if (input.expiresAt) {
        const maximum = 'dayFrom' in current ? MAX_HISTORY_MS : MAX_SHARING_MS;
        assertDuration(current.createdAt, input.expiresAt, MIN_SHARING_MS, maximum);
        if (Date.parse(input.expiresAt) <= Date.parse(now)) throw new WorkGraphInputError('Expiration must be in the future.');
      }
      next = {
        ...current,
        ...(input.audience ? { audience: input.audience } : {}),
        ...(input.selection ? { selection: input.selection } : {}),
        ...(input.expiresAt ? { expiresAt: input.expiresAt } : {}),
        updatedAt: now,
        revision: current.revision + 1,
      };
    }
    try {
      const stored = await this.deps.sharing.putGrant(next, current.revision);
      if ((stored.state === 'paused' || stored.state === 'stopped') && this.deps.sharingLifecycle) {
        await this.deps.sharingLifecycle.revoked(stored);
      }
      return stored;
    } catch (error) {
      if ((error as { code?: string }).code === 'WORK_SHARING_REVISION_CONFLICT') {
        throw new WorkGraphConflictError('Sharing grant revision conflict.');
      }
      throw error;
    }
  }

  /**
   * Serves one revision's preview bytes.
   *
   * The handle names a node and a revision the viewer was already given, and
   * nothing more: it is not a capability. Access is re-decided here against
   * the live graph and the source's own ACL, so a handle kept from an earlier
   * session stops working the moment the share does.
   */
  async getRevisionPreview(
    viewer: AuthenticatedWorkGraphViewer,
    personId: string,
    day: string,
    timezone: string,
    handle: string,
  ): Promise<RevisionPreview> {
    const reader = this.deps.activityReader;
    if (!reader) throw new WorkGraphInputError('Revision previews are not available on this deployment.');
    const parsed = parsePreviewHandle(handle);
    if (!parsed) throw unavailable();

    const { snapshot, authorization } = await this.getGraph(viewer, personId, day, timezone);
    const node = snapshot.nodes.find((candidate) => candidate.id === parsed.nodeId);
    if (!node || node.kind !== 'document' || node.locked || node.disclosure !== 'details') throw unavailable();

    const target = authorization.targets.get(parsed.nodeId);
    if (!target || target.kind !== 'node') throw unavailable();
    const decision = await authorizeWorkCapability(
      { ...snapshot.scope, organizationId: viewer.organizationId, viewerId: viewer.viewerId },
      target,
      'view-document',
      this.deps.sourceAcls,
    );
    if (decision === 'unavailable') throw new WorkGraphAuthorizationUnavailableError('Preview authorization is unavailable.');
    if (decision !== 'allow') throw new WorkGraphCapabilityDeniedError('Viewing this revision is not permitted.');

    // The revision has to be one this same snapshot listed. A handle naming a
    // revision the viewer was never shown is indistinguishable from absent.
    const activity = await buildWorkActivity({
      query: this.resolveWindow(personId, day, timezone, {}, this.deps.now()),
      nodes: snapshot.nodes,
      edges: snapshot.edges,
      authorization,
      reader,
      now: this.deps.now(),
    });
    const listed = activity.details
      .find((detail) => detail.nodeId === parsed.nodeId)?.artifact?.revisions
      .some((revision) => revision.id === parsed.revisionId);
    if (!listed) throw unavailable();

    const preview = await reader.revisionPreview(target.sourceRef.resourceId, parsed.revisionId);
    if (!preview) throw unavailable();
    return preview;
  }

  /**
   * Answers a question from an authorized transcript, quoting it.
   *
   * The answer is extracted from segments this viewer may already read; no
   * text is generated, so the reply cannot state anything the transcript does
   * not. Access is re-checked against the source at call time — a snapshot the
   * caller happens to hold is not authorization — and a question with no
   * supporting segment returns no answer rather than a plausible one.
   */
  async askTranscript(
    viewer: AuthenticatedWorkGraphViewer,
    personId: string,
    day: string,
    timezone: string,
    nodeId: string,
    question: string,
  ): Promise<TranscriptAnswer> {
    const reader = this.deps.activityReader;
    if (!reader) throw new WorkGraphInputError('Transcript questions are not available on this deployment.');
    const trimmed = question.trim();
    if (trimmed.length === 0 || trimmed.length > 500) throw new WorkGraphInputError('A question of 1 to 500 characters is required.');

    const { snapshot, authorization } = await this.getGraph(viewer, personId, day, timezone);
    const node = snapshot.nodes.find((candidate) => candidate.id === nodeId);
    if (!node || node.kind !== 'transcript' || node.locked || node.disclosure !== 'details') throw unavailable();

    const target = authorization.targets.get(nodeId);
    if (!target || target.kind !== 'node') throw unavailable();
    const decision = await authorizeWorkCapability(
      { ...snapshot.scope, organizationId: viewer.organizationId, viewerId: viewer.viewerId },
      target,
      'view-transcript',
      this.deps.sourceAcls,
    );
    if (decision === 'unavailable') throw new WorkGraphAuthorizationUnavailableError('Transcript authorization is unavailable.');
    if (decision !== 'allow') throw new WorkGraphCapabilityDeniedError('Reading this transcript is not permitted.');

    const facts = await reader.transcript(target.sourceRef.resourceId);
    if (!facts || facts.segments.length === 0) throw unavailable();
    return answerFromTranscript(trimmed, facts.segments);
  }

  async resolveReferences(
    viewer: AuthenticatedWorkGraphViewer,
    references: WorkReference[],
  ): Promise<WorkReferenceResolution[]> {
    return Promise.all(references.map(async (reference) => {
      try {
        const { snapshot } = await this.getGraph(
          viewer,
          reference.personId,
          reference.day,
          reference.timezone,
        );
        if (reference.target.kind === 'node') {
          const node = snapshot.nodes.find((candidate) => candidate.id === reference.target.id);
          return node ? { reference, state: 'available' as const, node } : { reference, state: 'unavailable' as const };
        }
        const edge = snapshot.edges.find((candidate) => candidate.id === reference.target.id);
        return edge ? { reference, state: 'available' as const, edge } : { reference, state: 'unavailable' as const };
      } catch {
        // Missing, denied, revoked, expired, cross-org, and source outages are
        // deliberately indistinguishable in a recipient-specific preview.
        return { reference, state: 'unavailable' as const };
      }
    }));
  }

  async createCollaborationRequest(
    viewer: AuthenticatedWorkGraphViewer,
    input: CreateCollaborationInput,
  ): Promise<WorkCollaborationRequest> {
    const now = this.deps.now();
    assertDuration(now, input.expiresAt, 1, MAX_COLLABORATION_MS);
    const located = await this.deps.locator.locate(viewer.organizationId, input.target);
    if (!located || located.personId === viewer.viewerId) throw unavailable();
    const { snapshot, authorization } = await this.getGraph(
      viewer,
      located.personId,
      located.day,
      located.timezone,
    );
    const projected = input.target.kind === 'node'
      ? snapshot.nodes.find((item) => item.id === input.target.id)
      : snapshot.edges.find((item) => item.id === input.target.id);
    if (!projected) {
      if (authorization.unavailableTargetIds.has(input.target.id)) {
        throw new WorkGraphAuthorizationUnavailableError('Work graph authorization is temporarily unavailable.');
      }
      throw unavailable();
    }
    if (!projected.capabilities.includes(input.capability)) {
      throw new WorkGraphCapabilityDeniedError('The requested collaboration capability is not available.');
    }
    const target = authorization.targets.get(input.target.id);
    if (!target) throw unavailable();
    const capability = await authorizeWorkCapability(
      {
        organizationId: viewer.organizationId,
        viewerId: viewer.viewerId,
        personId: located.personId,
        day: located.day,
        timezone: located.timezone,
        policyRevision: snapshot.scope.policyRevision,
      },
      target,
      input.capability,
      this.deps.sourceAcls,
    );
    if (capability === 'unavailable') {
      throw new WorkGraphAuthorizationUnavailableError('Work graph authorization is temporarily unavailable.');
    }
    if (capability !== 'allow') {
      throw new WorkGraphCapabilityDeniedError('The requested collaboration capability is not available.');
    }
    return this.deps.collaboration.putRequest({
      schemaVersion: 1,
      id: this.deps.id(),
      organizationId: viewer.organizationId,
      requesterId: viewer.viewerId,
      ownerId: located.personId,
      target: input.target,
      capability: input.capability,
      state: 'requested',
      createdAt: now,
      updatedAt: now,
      expiresAt: input.expiresAt,
      revision: 1,
    }, null);
  }

  async listOwnedCollaborationRequests(
    viewer: AuthenticatedWorkGraphViewer,
  ): Promise<WorkCollaborationRequest[]> {
    if (!this.deps.collaboration.listOwnedRequests) return [];
    return this.deps.collaboration.listOwnedRequests(
      viewer.organizationId,
      viewer.viewerId,
      this.deps.now(),
    );
  }

  async exchangeAction(
    viewer: AuthenticatedWorkGraphViewer,
    input: { target: WorkCollaborationTarget; capability: WorkCollaborationCapability },
  ): Promise<WorkGraphActionDescriptor> {
    const now = this.deps.now();
    const located = await this.deps.locator.locate(viewer.organizationId, input.target);
    if (!located) throw unavailable();
    const { snapshot, authorization } = await this.getGraph(
      viewer,
      located.personId,
      located.day,
      located.timezone,
    );
    const projected = input.target.kind === 'node'
      ? snapshot.nodes.find((item) => item.id === input.target.id)
      : snapshot.edges.find((item) => item.id === input.target.id);
    if (!projected?.capabilities.includes(input.capability)) {
      // Action denial is intentionally indistinguishable from a missing or
      // revoked target; unlike a request composer, this exchanges live access.
      throw unavailable();
    }
    const target = authorization.targets.get(input.target.id);
    if (!target) throw unavailable();
    const scope: WorkGraphPolicyScope = {
      organizationId: viewer.organizationId,
      viewerId: viewer.viewerId,
      personId: located.personId,
      day: located.day,
      timezone: located.timezone,
      policyRevision: snapshot.scope.policyRevision,
    };
    const capabilityDecision = await authorizeWorkCapability(
      scope,
      target,
      input.capability,
      this.deps.sourceAcls,
    );
    if (capabilityDecision === 'unavailable') {
      throw new WorkGraphAuthorizationUnavailableError('Work graph authorization is temporarily unavailable.');
    }
    if (capabilityDecision !== 'allow') throw unavailable();

    let acceptedRequest: WorkCollaborationRequest | undefined;
    if (input.capability === 'control-terminal') {
      acceptedRequest = await this.deps.collaboration.findAcceptedRequest(
        viewer.organizationId,
        located.personId,
        viewer.viewerId,
        input.target,
        input.capability,
        now,
      ) ?? undefined;
      if (!acceptedRequest) throw unavailable();
    }

    const sources = sourcesForWorkTarget(target);
    // An action must have exactly one authoritative source owner. Cross-source
    // relationships remain inspectable but cannot mint an ambiguous action.
    if (sources.length !== 1) throw unavailable();
    const adapter = this.deps.sourceActions.get(sources[0]);
    if (!adapter) {
      throw new WorkGraphAuthorizationUnavailableError('Work graph authorization is temporarily unavailable.');
    }
    let result: WorkSourceActionResult;
    try {
      result = await adapter.exchangeAction(scope, target, input.capability, acceptedRequest);
    } catch {
      throw new WorkGraphAuthorizationUnavailableError('Work graph authorization is temporarily unavailable.');
    }
    if (result.decision === 'unavailable') {
      throw new WorkGraphAuthorizationUnavailableError('Work graph authorization is temporarily unavailable.');
    }
    if (result.decision !== 'allow') throw unavailable();
    const descriptor = validateActionDescriptor(result.descriptor, input.capability, now);
    if (!descriptor || (descriptor.kind === 'source-session' && descriptor.sessionId === target.id)) {
      throw new WorkGraphAuthorizationUnavailableError('Work graph authorization is temporarily unavailable.');
    }
    return descriptor;
  }

  async updateCollaborationRequest(
    viewer: AuthenticatedWorkGraphViewer,
    requestId: string,
    input: { expectedRevision: number; action: 'accept' | 'decline' | 'revoke' },
  ): Promise<WorkCollaborationRequest> {
    const now = this.deps.now();
    const current = await this.deps.collaboration.getRequest(viewer.organizationId, requestId);
    if (!current || (current.ownerId !== viewer.viewerId && current.requesterId !== viewer.viewerId)) throw unavailable();
    if (current.revision !== input.expectedRevision) throw new WorkGraphConflictError('Collaboration request revision conflict.');
    if (Date.parse(current.expiresAt) <= Date.parse(now)) {
      if (current.state !== 'expired' && current.state !== 'revoked' && current.state !== 'declined') {
        const expired = await this.deps.collaboration.putRequest({
          ...current,
          state: 'expired',
          updatedAt: now,
          revision: current.revision + 1,
        }, current.revision);
        await this.deps.collaborationLifecycle.revoked(expired);
      }
      throw unavailable();
    }
    if ((input.action === 'accept' || input.action === 'decline') && current.ownerId !== viewer.viewerId) throw unavailable();
    if (input.action === 'accept' && current.state !== 'requested') throw new WorkGraphConflictError('Collaboration request is no longer pending.');
    if (input.action === 'decline' && current.state !== 'requested') throw new WorkGraphConflictError('Collaboration request is no longer pending.');
    if (input.action === 'revoke' && current.state !== 'requested' && current.state !== 'accepted') {
      throw new WorkGraphConflictError('Collaboration request is no longer revocable.');
    }

    if (input.action === 'accept') {
      const located = await this.deps.locator.locate(viewer.organizationId, current.target);
      if (!located || located.personId !== current.ownerId) throw unavailable();
      const requester = await this.deps.directory.resolveViewerById(
        current.organizationId,
        current.requesterId,
      );
      if (!requester
        || requester.viewerId !== current.requesterId
        || requester.organizationId !== current.organizationId) throw unavailable();
      const { snapshot, authorization } = await this.getGraph(
        requester,
        located.personId,
        located.day,
        located.timezone,
      );
      const projected = current.target.kind === 'node'
        ? snapshot.nodes.find((item) => item.id === current.target.id)
        : snapshot.edges.find((item) => item.id === current.target.id);
      if (!projected?.capabilities.includes(current.capability)) {
        throw new WorkGraphCapabilityDeniedError('The requested collaboration capability is no longer available.');
      }
      const target = authorization.targets.get(current.target.id);
      if (!target) throw unavailable();
      const decision = await authorizeWorkCapability({
        organizationId: current.organizationId,
        viewerId: current.requesterId,
        personId: current.ownerId,
        day: located.day,
        timezone: located.timezone,
        policyRevision: snapshot.scope.policyRevision,
      }, target, current.capability, this.deps.sourceAcls);
      if (decision === 'unavailable') throw new WorkGraphAuthorizationUnavailableError('Work graph authorization is temporarily unavailable.');
      if (decision !== 'allow') throw new WorkGraphCapabilityDeniedError('The requested collaboration capability is no longer available.');
    }

    const next: WorkCollaborationRequest = {
      ...current,
      state: input.action === 'accept' ? 'accepted' : input.action === 'decline' ? 'declined' : 'revoked',
      updatedAt: now,
      revision: current.revision + 1,
    };
    let stored: WorkCollaborationRequest;
    try {
      stored = await this.deps.collaboration.putRequest(next, current.revision);
    } catch (error) {
      if ((error as { code?: string }).code === 'WORK_COLLABORATION_REVISION_CONFLICT') {
        throw new WorkGraphConflictError('Collaboration request revision conflict.');
      }
      throw error;
    }
    if (stored.state === 'accepted') await this.deps.collaborationLifecycle.accepted(stored);
    if (stored.state === 'revoked') await this.deps.collaborationLifecycle.revoked(stored);
    return stored;
  }

  private ownerGrant(
    viewer: AuthenticatedWorkGraphViewer,
    projection: WorkProjectionState | null,
    now: string,
    day: string,
  ): WorkHistoryGrant {
    return {
      schemaVersion: 1,
      id: 'owner-self-access',
      organizationId: viewer.organizationId,
      ownerId: viewer.viewerId,
      audience: { kind: 'people', personIds: [viewer.viewerId] },
      selection: {
        nodeIds: Object.keys(projection?.nodes ?? {}),
        edgeIds: Object.keys(projection?.edges ?? {}),
        disclosure: 'details',
      },
      state: 'active',
      createdAt: now,
      updatedAt: now,
      expiresAt: new Date(Date.parse(now) + MAX_SHARING_MS).toISOString(),
      revision: 1,
      // Self-access is not a persisted live grant. Bound the synthetic grant
      // to the requested day so the same live-only rule cannot accidentally
      // hide the owner's own historical graph.
      dayFrom: day,
      dayThrough: day,
    };
  }
}
