import type {
  StoredWorkEdge,
  StoredWorkNode,
  WorkProjectionState,
  WorkSourceKind,
  WorkSourceRef,
} from '../repositories/WorkGraphRepository';
import type {
  SharingSelection,
  StoredWorkGrant,
} from '../repositories/WorkSharingRepository';
import type { WorkCollaborationCapability } from '../repositories/WorkCollaborationRepository';
import { filterForViewer } from '@connorhoehn/realtime-modules/work-graph';
import { parsePipelineStepResourceId } from './sources/mapPipeline';

export type WorkDisclosure = 'existence' | 'summary' | 'details';
export type WorkPolicyDecision = 'allow' | 'deny' | 'unavailable';

export interface WorkGraphPolicyScope {
  organizationId: string;
  viewerId: string;
  personId: string;
  day: string;
  timezone: string;
  policyRevision: string;
}

export type WorkAclTarget =
  | {
    kind: 'node';
    id: string;
    policyRef: string;
    sourceRef: WorkSourceRef;
    node: StoredWorkNode;
  }
  | {
    kind: 'edge';
    id: string;
    policyRef: string;
    endpointRefs: WorkSourceRef[];
    edge: StoredWorkEdge;
  };

export interface WorkSourceAclDecision {
  targetId: string;
  decision: WorkPolicyDecision;
  maximumDisclosure?: WorkDisclosure;
  capabilities?: WorkCollaborationCapability[];
  policyRevision: string;
}

export interface WorkSourceAclAdapter {
  readonly source: WorkSourceKind;
  authorizeDisclosure(
    scope: WorkGraphPolicyScope,
    targets: WorkAclTarget[],
  ): Promise<Map<string, WorkSourceAclDecision>>;
  authorizeCapability(
    scope: WorkGraphPolicyScope,
    target: WorkAclTarget,
    capability: WorkCollaborationCapability,
  ): Promise<WorkPolicyDecision>;
}

export interface ViewerWorkNode {
  id: string;
  kind: StoredWorkNode['kind'];
  title: string;
  description?: string;
  status: StoredWorkNode['status'];
  disclosure: WorkDisclosure;
  startedAt?: string;
  updatedAt: string;
  endedAt?: string;
  capabilities: WorkCollaborationCapability[];
  locked?: boolean;
}

export interface ViewerWorkEdge {
  id: string;
  fromId: string;
  toId: string;
  relation: StoredWorkEdge['relation'];
  label?: string;
  status: StoredWorkEdge['status'];
  disclosure: WorkDisclosure;
  startedAt?: string;
  updatedAt: string;
  endedAt?: string;
  capabilities: WorkCollaborationCapability[];
}

export interface WorkSourceStatus {
  source: WorkSourceKind;
  health: 'available' | 'authorization-unavailable';
  checkedAt: string;
}

export interface AuthorizedWorkGraph {
  nodes: ViewerWorkNode[];
  edges: ViewerWorkEdge[];
  sources: WorkSourceStatus[];
  partial: boolean;
  /** Internal-only IDs whose source authorization could not be established. */
  unavailableTargetIds: Set<string>;
  /** Internal-only targets retained for capability reauthorization. */
  targets: Map<string, WorkAclTarget>;
  /** Internal-to-reader ID mapping used only while filtering replay deltas. */
  publicTargetIds: Map<string, string>;
}

const DISCLOSURE_RANK: Record<WorkDisclosure, number> = {
  existence: 0,
  summary: 1,
  details: 2,
};
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

function minimumDisclosure(left: WorkDisclosure, right: WorkDisclosure): WorkDisclosure {
  return DISCLOSURE_RANK[left] <= DISCLOSURE_RANK[right] ? left : right;
}

function safeCapabilities(values: readonly WorkCollaborationCapability[] | undefined): WorkCollaborationCapability[] {
  return (values ?? []).filter((value) => CAPABILITIES.has(value));
}

function intersectCapabilities(sets: WorkCollaborationCapability[][]): WorkCollaborationCapability[] {
  if (sets.length === 0) return [];
  return sets[0].filter((capability) => sets.slice(1).every((set) => set.includes(capability)));
}

export function sourcesForWorkTarget(target: WorkAclTarget): WorkSourceKind[] {
  if (target.kind === 'node') return [target.sourceRef.source];
  return [...new Set(target.endpointRefs.map((ref) => ref.source))];
}

function ownNode(projection: WorkProjectionState, node: StoredWorkNode | undefined): node is StoredWorkNode {
  return !!node
    && !node.deletedAt
    && node.organizationId === projection.organizationId
    && node.actorId === projection.actorId;
}

/**
 * The work that changed a shared document after the grant named it.
 *
 * A grant selects node ids, and ids are minted when work happens: a run that
 * edits a deck the owner shared this morning has a node id no grant could
 * have listed, so a reader watching "Connor's activity" saw the deck and not
 * the edit happening to it (NFR #125). The rule lives here, in the
 * projection's disclosure, and in no client: a grant on a document covers
 * what changes that document — the revisions written to it (`edited` into
 * it), the run that wrote them (`produced` it, from the run or from one of
 * its steps) and that run's other steps — at the grant's own disclosure and
 * still subject to every source's own ACL. Nothing else widens: not what
 * the run read, not the workspace it ran in, not another document it wrote.
 * A grant that names no document derives nothing.
 */
function derivedNodeIds(projection: WorkProjectionState, selectedNodes: ReadonlySet<string>): Set<string> {
  const derived = new Set<string>();
  const runIds = new Set<string>();
  for (const edge of Object.values(projection.edges)) {
    if (edge.deletedAt
      || edge.organizationId !== projection.organizationId
      || edge.actorId !== projection.actorId
      || !selectedNodes.has(edge.toId)) continue;
    const document = projection.nodes[edge.toId];
    const from = projection.nodes[edge.fromId];
    if (!ownNode(projection, document) || document.kind !== 'document' || !ownNode(projection, from)) continue;
    if (edge.relation === 'edited' && from.kind === 'change' && from.sourceRef.source === 'document') {
      derived.add(from.id);
    } else if (edge.relation === 'produced' && from.kind === 'run' && from.sourceRef.source === 'pipeline') {
      runIds.add(parsePipelineStepResourceId(from.sourceRef.resourceId).runId);
    }
  }
  if (runIds.size === 0) return derived;
  for (const node of Object.values(projection.nodes)) {
    if (!ownNode(projection, node) || node.kind !== 'run' || node.sourceRef.source !== 'pipeline') continue;
    if (runIds.has(parsePipelineStepResourceId(node.sourceRef.resourceId).runId)) derived.add(node.id);
  }
  return derived;
}

function selectedTargets(
  projection: WorkProjectionState,
  selection: SharingSelection,
): Map<string, WorkAclTarget> {
  const targets = new Map<string, WorkAclTarget>();
  const named = new Set(selection.nodeIds);
  const derived = derivedNodeIds(projection, named);
  const selectedNodes = new Set([...named, ...derived]);
  const selectedEdges = new Set(selection.edgeIds);

  for (const node of Object.values(projection.nodes)) {
    if (!selectedNodes.has(node.id)
      || node.deletedAt
      || node.organizationId !== projection.organizationId
      || node.actorId !== projection.actorId) continue;
    targets.set(node.id, {
      kind: 'node',
      id: node.id,
      policyRef: node.policyRef,
      sourceRef: node.sourceRef,
      node,
    });
  }

  for (const edge of Object.values(projection.edges)) {
    if (edge.deletedAt
      || edge.organizationId !== projection.organizationId
      || edge.actorId !== projection.actorId) continue;
    // A derived node's relationships were never in the grant's list either:
    // an edge is in when the grant named it, or when it touches derived work
    // and stays inside the widened set.
    if (!selectedEdges.has(edge.id) && !(derived.has(edge.fromId) || derived.has(edge.toId))) continue;
    const from = projection.nodes[edge.fromId];
    const to = projection.nodes[edge.toId];
    // Missing, deleted, or unselected endpoints fail closed before any ACL call.
    if (!from || !to || from.deletedAt || to.deletedAt
      || !selectedNodes.has(from.id) || !selectedNodes.has(to.id)) continue;
    targets.set(edge.id, {
      kind: 'edge',
      id: edge.id,
      policyRef: edge.policyRef,
      endpointRefs: [from.sourceRef, to.sourceRef],
      edge,
    });
  }
  return targets;
}

function localCalendarDay(now: string, timezone: string): string | null {
  const timestamp = Date.parse(now);
  if (!Number.isFinite(timestamp)) return null;
  try {
    const parts = new Intl.DateTimeFormat('en-CA', {
      timeZone: timezone,
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      calendar: 'gregory',
      numberingSystem: 'latn',
    }).formatToParts(new Date(timestamp));
    const part = (kind: Intl.DateTimeFormatPartTypes) => parts.find((item) => item.type === kind)?.value;
    const year = part('year');
    const month = part('month');
    const day = part('day');
    return year && month && day ? `${year}-${month}-${day}` : null;
  } catch {
    return null;
  }
}

/**
 * A normal grant is deliberately live-only. Historical disclosure requires
 * the separate day-bounded companion grant; otherwise a still-active live
 * opt-in could be replayed against arbitrary old calendar dates.
 */
export function grantAppliesToScope(
  grant: StoredWorkGrant,
  scope: Pick<WorkGraphPolicyScope, 'day' | 'timezone'>,
  checkedAt: string,
): boolean {
  if ('dayFrom' in grant) return grant.dayFrom <= scope.day && scope.day <= grant.dayThrough;
  return localCalendarDay(checkedAt, scope.timezone) === scope.day;
}

/**
 * Intersects voluntary selection with every affected source owner's ACL.
 * A missing adapter, thrown lookup, omitted decision, stale policy revision,
 * or explicit unavailable result withholds the target and marks that source
 * partial. Explicit deny withholds without turning a healthy source into 503.
 */
export async function authorizeWorkGraph(
  projection: WorkProjectionState,
  scope: WorkGraphPolicyScope,
  grants: StoredWorkGrant[],
  adapters: ReadonlyMap<WorkSourceKind, WorkSourceAclAdapter>,
  checkedAt: string,
  /** Filled with each source adapter's wall time (ms) as `acl.<source>` when given. */
  timing?: Record<string, number>,
): Promise<AuthorizedWorkGraph> {
  if (projection.organizationId !== scope.organizationId || projection.actorId !== scope.personId) {
    return {
      nodes: [], edges: [], sources: [], partial: false,
      unavailableTargetIds: new Set(), targets: new Map(), publicTargetIds: new Map(),
    };
  }

  const effective = grants.filter((grant) => grant.organizationId === scope.organizationId
    && grant.ownerId === scope.personId
    && grant.state === 'active'
    && Date.parse(grant.expiresAt) > Date.parse(checkedAt)
    && grantAppliesToScope(grant, scope, checkedAt));

  const targetGrants = new Map<string, StoredWorkGrant[]>();
  const allTargets = new Map<string, WorkAclTarget>();
  for (const grant of effective) {
    const targets = selectedTargets(projection, grant.selection);
    for (const [id, target] of targets) {
      allTargets.set(id, target);
      const current = targetGrants.get(id) ?? [];
      current.push(grant);
      targetGrants.set(id, current);
    }
  }

  const bySource = new Map<WorkSourceKind, WorkAclTarget[]>();
  for (const target of allTargets.values()) {
    for (const source of sourcesForWorkTarget(target)) {
      const values = bySource.get(source) ?? [];
      values.push(target);
      bySource.set(source, values);
    }
  }

  const decisions = new Map<WorkSourceKind, Map<string, WorkSourceAclDecision>>();
  const unavailableSources = new Set<WorkSourceKind>();
  await Promise.all([...bySource.entries()].map(async ([source, targets]) => {
    const adapter = adapters.get(source);
    if (!adapter) {
      unavailableSources.add(source);
      return;
    }
    const started = Date.now();
    try {
      const result = await adapter.authorizeDisclosure(scope, targets);
      if (!(result instanceof Map)) unavailableSources.add(source);
      else decisions.set(source, result);
    } catch {
      unavailableSources.add(source);
    } finally {
      if (timing) timing[`acl.${source}`] = Date.now() - started;
    }
  }));

  const nodeDecisions = new Map<string, {
    targetId: string;
    decision: 'allow';
    maximumDisclosure: WorkDisclosure;
    capabilities: WorkCollaborationCapability[];
    existenceLabel?: string;
    policyRevision: string;
  }>();
  const edgeDecisions = new Map<string, {
    targetId: string;
    decision: 'allow';
    maximumDisclosure: WorkDisclosure;
    capabilities: WorkCollaborationCapability[];
    existenceLabel?: string;
    policyRevision: string;
  }>();
  const unavailableTargetIds = new Set<string>();

  const authorizeTarget = (target: WorkAclTarget): { disclosure: WorkDisclosure; capabilities: WorkCollaborationCapability[] } | null => {
    const sourceDecisions: WorkSourceAclDecision[] = [];
    for (const source of sourcesForWorkTarget(target)) {
      if (unavailableSources.has(source)) {
        unavailableTargetIds.add(target.id);
        return null;
      }
      const decision = decisions.get(source)?.get(target.id);
      if (!decision
        || decision.targetId !== target.id
        || decision.policyRevision !== scope.policyRevision
        || decision.decision === 'unavailable'
        || (decision.decision === 'allow' && !decision.maximumDisclosure)) {
        unavailableSources.add(source);
        unavailableTargetIds.add(target.id);
        return null;
      }
      if (decision.decision !== 'allow') return null;
      sourceDecisions.push(decision);
    }

    let bestGrantDisclosure: WorkDisclosure = 'existence';
    for (const grant of targetGrants.get(target.id) ?? []) {
      if (DISCLOSURE_RANK[grant.selection.disclosure] > DISCLOSURE_RANK[bestGrantDisclosure]) {
        bestGrantDisclosure = grant.selection.disclosure;
      }
    }
    let disclosure = bestGrantDisclosure;
    for (const decision of sourceDecisions) {
      disclosure = minimumDisclosure(disclosure, decision.maximumDisclosure!);
    }
    return {
      disclosure,
      capabilities: intersectCapabilities(sourceDecisions.map((decision) => safeCapabilities(decision.capabilities))),
    };
  };

  for (const target of allTargets.values()) {
    const allowed = authorizeTarget(target);
    if (!allowed) continue;
    const decision = {
      targetId: target.id,
      decision: 'allow' as const,
      maximumDisclosure: allowed.disclosure,
      capabilities: allowed.capabilities,
      // The shared library requires a sanitized existence label and owns
      // placeholder sequencing/collision handling. It never receives a
      // source label or internal identifier for this disclosure level.
      ...(allowed.disclosure === 'existence' ? { existenceLabel: 'Shared work item' } : {}),
      policyRevision: scope.policyRevision,
    };
    if (target.kind === 'node') nodeDecisions.set(target.id, decision);
    else edgeDecisions.set(target.id, decision);
  }

  // Keep browser DTO construction in the installed shared library. This
  // avoids a second, subtly divergent placeholder and selection filter in the
  // HTTP path; the platform supplies only precomputed, source-authorized
  // allow decisions above.
  const projected = effective.length === 0
    ? { nodes: [], edges: [] }
    : filterForViewer(projection as never, {
      scope: scope as never,
      grant: effective[0] as never,
      nodeDecisions: nodeDecisions as never,
      edgeDecisions: edgeDecisions as never,
    });
  const nodes = projected.nodes as unknown as ViewerWorkNode[];
  const edges = projected.edges as unknown as ViewerWorkEdge[];
  const publicTargetIds = new Map<string, string>();
  const visibleNodeIds = new Set(nodes.map((node) => node.id));
  for (const [internalId, decision] of nodeDecisions) {
    if (decision.maximumDisclosure !== 'existence') {
      if (visibleNodeIds.has(internalId)) publicTargetIds.set(internalId, internalId);
      continue;
    }
    // Ask the same shared projection code for this one decision to obtain its
    // stable opaque placeholder. This is intentionally not a local hash or
    // counter: replay must use exactly the library's viewer/query binding.
    const single = filterForViewer(projection as never, {
      scope: scope as never,
      grant: effective[0] as never,
      nodeDecisions: new Map([[internalId, decision]]) as never,
      edgeDecisions: new Map() as never,
    }) as unknown as { nodes: ViewerWorkNode[] };
    const publicId = single.nodes[0]?.id;
    if (publicId && visibleNodeIds.has(publicId)) publicTargetIds.set(internalId, publicId);
  }
  for (const edge of edges) publicTargetIds.set(edge.id, edge.id);

  const sources: WorkSourceStatus[] = [...bySource.keys()].sort().map((source) => ({
    source,
    health: unavailableSources.has(source) ? 'authorization-unavailable' : 'available',
    checkedAt,
  }));
  return {
    nodes,
    edges,
    sources,
    partial: unavailableSources.size > 0,
    unavailableTargetIds,
    targets: allTargets,
    publicTargetIds,
  };
}

export async function authorizeWorkCapability(
  scope: WorkGraphPolicyScope,
  target: WorkAclTarget,
  capability: WorkCollaborationCapability,
  adapters: ReadonlyMap<WorkSourceKind, WorkSourceAclAdapter>,
): Promise<WorkPolicyDecision> {
  for (const source of sourcesForWorkTarget(target)) {
    const adapter = adapters.get(source);
    if (!adapter) return 'unavailable';
    try {
      const decision = await adapter.authorizeCapability(scope, target, capability);
      if (decision !== 'allow') return decision;
    } catch {
      return 'unavailable';
    }
  }
  return 'allow';
}
