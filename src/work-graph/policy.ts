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
const EXISTENCE_UPDATED_AT = '1970-01-01T00:00:00.000Z';

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

function selectedTargets(
  projection: WorkProjectionState,
  selection: SharingSelection,
): Map<string, WorkAclTarget> {
  const targets = new Map<string, WorkAclTarget>();
  const selectedNodes = new Set(selection.nodeIds);
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
    if (!selectedEdges.has(edge.id)
      || edge.deletedAt
      || edge.organizationId !== projection.organizationId
      || edge.actorId !== projection.actorId) continue;
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

function viewerNode(
  node: StoredWorkNode,
  disclosure: WorkDisclosure,
  capabilities: WorkCollaborationCapability[],
  publicId: string,
): ViewerWorkNode {
  if (disclosure === 'existence') {
    return {
      id: publicId,
      kind: 'task',
      title: 'Shared work item',
      status: 'idle',
      disclosure,
      updatedAt: EXISTENCE_UPDATED_AT,
      capabilities: [],
      locked: true,
    };
  }
  const base: ViewerWorkNode = {
    id: publicId,
    kind: node.kind,
    title: node.title,
    status: node.status,
    disclosure,
    updatedAt: node.updatedAt,
    capabilities,
  };
  return disclosure === 'details'
    ? {
      ...base,
      ...(node.description ? { description: node.description } : {}),
      ...(node.startedAt ? { startedAt: node.startedAt } : {}),
      ...(node.endedAt ? { endedAt: node.endedAt } : {}),
    }
    : base;
}

function viewerEdge(
  edge: StoredWorkEdge,
  disclosure: WorkDisclosure,
  capabilities: WorkCollaborationCapability[],
  fromId: string,
  toId: string,
): ViewerWorkEdge | null {
  // Relationship existence itself is sensitive; there is no safe fabricated
  // relation for an existence-only edge.
  if (disclosure === 'existence') return null;
  const base: ViewerWorkEdge = {
    id: edge.id,
    fromId,
    toId,
    relation: edge.relation,
    status: edge.status,
    disclosure,
    updatedAt: edge.updatedAt,
    capabilities,
  };
  return disclosure === 'details'
    ? {
      ...base,
      ...(edge.label ? { label: edge.label } : {}),
      ...(edge.startedAt ? { startedAt: edge.startedAt } : {}),
      ...(edge.endedAt ? { endedAt: edge.endedAt } : {}),
    }
    : base;
}

function appliesToDay(grant: StoredWorkGrant, day: string): boolean {
  if ('dayFrom' in grant) return grant.dayFrom <= day && day <= grant.dayThrough;
  return true;
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
): Promise<AuthorizedWorkGraph> {
  if (projection.organizationId !== scope.organizationId || projection.actorId !== scope.personId) {
    return { nodes: [], edges: [], sources: [], partial: false, unavailableTargetIds: new Set(), targets: new Map() };
  }

  const effective = grants.filter((grant) => grant.organizationId === scope.organizationId
    && grant.ownerId === scope.personId
    && grant.state === 'active'
    && Date.parse(grant.expiresAt) > Date.parse(checkedAt)
    && appliesToDay(grant, scope.day));

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
    try {
      const result = await adapter.authorizeDisclosure(scope, targets);
      if (!(result instanceof Map)) unavailableSources.add(source);
      else decisions.set(source, result);
    } catch {
      unavailableSources.add(source);
    }
  }));

  const nodes: ViewerWorkNode[] = [];
  const edges: ViewerWorkEdge[] = [];
  const publicNodeIds = new Map<string, string>();
  const unavailableTargetIds = new Set<string>();
  let placeholder = 0;

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
    if (target.kind !== 'node') continue;
    const allowed = authorizeTarget(target);
    if (!allowed) continue;
    const publicId = allowed.disclosure === 'existence'
      ? `work_placeholder_${++placeholder}`
      : target.id;
    nodes.push(viewerNode(target.node, allowed.disclosure, allowed.capabilities, publicId));
    publicNodeIds.set(target.id, publicId);
  }
  for (const target of allTargets.values()) {
    if (target.kind !== 'edge') continue;
    const fromId = publicNodeIds.get(target.edge.fromId);
    const toId = publicNodeIds.get(target.edge.toId);
    if (!fromId || !toId) continue;
    const allowed = authorizeTarget(target);
    if (!allowed) continue;
    const projected = viewerEdge(target.edge, allowed.disclosure, allowed.capabilities, fromId, toId);
    if (projected) edges.push(projected);
  }

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
