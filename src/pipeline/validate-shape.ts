// platform-api/src/pipeline/validate-shape.ts
//
// Structural validation of a pipeline definition, server-side.
//
// Until now the only thing standing between a broken pipeline and `published`
// was the browser: the editor runs its own `validatePipeline` and refuses to
// publish. That is the wrong place for the check to live. Pipelines here are
// increasingly written by agents posting straight at this API, and the publish
// route did no validation at all — it bumped the version, set `status:
// 'published'`, and stored whatever it was handed. A definition with dangling
// edges, two triggers, or a cycle went live and failed at run time, far from
// whatever produced it.
//
// This module is the server-side gate. Deliberately STRUCTURAL only: platform-api
// keeps an untyped view of the definition so the two repos can evolve
// independently (see TYPES_SYNC.md and the header of routes/pipelineDefinitions.ts),
// so per-node-type config rules stay in the editor where the node types are
// defined. What is checked here is what is wrong about a *graph* no matter what
// the nodes mean.
//
// Severity follows the same contract the editor uses, because agents and humans
// should get the same answer: errors block publish, warnings are advisory. A
// draft save is never blocked — a pipeline under construction is legitimately
// half-wired, and refusing to store it would make the editor unusable.

export type ShapeIssueCode =
  | 'NODES_MISSING'
  | 'NODES_EMPTY'
  | 'NODE_ID_MISSING'
  | 'NODE_ID_DUPLICATE'
  | 'EDGES_MISSING'
  | 'EDGE_ENDPOINT_UNKNOWN'
  | 'EDGE_SELF_LOOP'
  | 'TRIGGER_MISSING'
  | 'TRIGGER_MULTIPLE'
  | 'CYCLE_DETECTED'
  | 'NODE_ORPHANED';

export interface ShapeIssue {
  code: ShapeIssueCode;
  message: string;
  severity: 'error' | 'warning';
  nodeId?: string;
  edgeId?: string;
}

export interface ShapeValidation {
  errors: ShapeIssue[];
  warnings: ShapeIssue[];
}

/** True when the definition has no errors — i.e. it may be published. */
export function isPublishable(result: ShapeValidation): boolean {
  return result.errors.length === 0;
}

interface LooseNode {
  id?: unknown;
  type?: unknown;
}

interface LooseEdge {
  id?: unknown;
  source?: unknown;
  target?: unknown;
}

function asArray(value: unknown): unknown[] | null {
  return Array.isArray(value) ? value : null;
}

function idOf(value: unknown): string | null {
  return typeof value === 'string' && value.length > 0 ? value : null;
}

/**
 * Validate the graph shape of a pipeline definition.
 *
 * Never throws: a definition that is malformed enough to be unreadable comes
 * back as errors, because this runs on request bodies from anywhere.
 */
export function validateShape(definition: unknown): ShapeValidation {
  const errors: ShapeIssue[] = [];
  const warnings: ShapeIssue[] = [];

  const def = (definition ?? {}) as { nodes?: unknown; edges?: unknown };

  const rawNodes = asArray(def.nodes);
  if (!rawNodes) {
    errors.push({
      code: 'NODES_MISSING',
      message: 'Definition has no `nodes` array.',
      severity: 'error',
    });
    return { errors, warnings };
  }
  if (rawNodes.length === 0) {
    errors.push({
      code: 'NODES_EMPTY',
      message: 'Definition has no nodes.',
      severity: 'error',
    });
    return { errors, warnings };
  }

  const rawEdges = asArray(def.edges);
  if (!rawEdges) {
    errors.push({
      code: 'EDGES_MISSING',
      message: 'Definition has no `edges` array.',
      severity: 'error',
    });
    return { errors, warnings };
  }

  // --- nodes: ids present and unique ---------------------------------------
  const known = new Set<string>();
  const triggers: string[] = [];
  for (const raw of rawNodes) {
    const node = (raw ?? {}) as LooseNode;
    const id = idOf(node.id);
    if (!id) {
      errors.push({
        code: 'NODE_ID_MISSING',
        message: 'A node has no id.',
        severity: 'error',
      });
      continue;
    }
    if (known.has(id)) {
      errors.push({
        code: 'NODE_ID_DUPLICATE',
        message: `More than one node uses the id "${id}".`,
        severity: 'error',
        nodeId: id,
      });
      continue;
    }
    known.add(id);
    if (node.type === 'trigger') triggers.push(id);
  }

  // --- edges: endpoints resolve, no self-loops -----------------------------
  const succ = new Map<string, string[]>();
  const degree = new Map<string, number>();
  for (const id of known) {
    succ.set(id, []);
    degree.set(id, 0);
  }

  for (const raw of rawEdges) {
    const edge = (raw ?? {}) as LooseEdge;
    const edgeId = idOf(edge.id) ?? undefined;
    const source = idOf(edge.source);
    const target = idOf(edge.target);

    if (!source || !known.has(source) || !target || !known.has(target)) {
      // A dangling edge is the single most common way an agent-authored
      // definition is broken: it renames a node and leaves an edge behind.
      errors.push({
        code: 'EDGE_ENDPOINT_UNKNOWN',
        message: `Edge ${edgeId ?? '(unnamed)'} points at a node that does not exist (${String(
          edge.source,
        )} → ${String(edge.target)}).`,
        severity: 'error',
        edgeId,
      });
      continue;
    }
    if (source === target) {
      errors.push({
        code: 'EDGE_SELF_LOOP',
        message: `Edge ${edgeId ?? '(unnamed)'} connects "${source}" to itself.`,
        severity: 'error',
        edgeId,
        nodeId: source,
      });
      continue;
    }
    (succ.get(source) as string[]).push(target);
    degree.set(source, (degree.get(source) ?? 0) + 1);
    degree.set(target, (degree.get(target) ?? 0) + 1);
  }

  // --- triggers -------------------------------------------------------------
  if (triggers.length === 0) {
    errors.push({
      code: 'TRIGGER_MISSING',
      message: 'Pipeline has no trigger node, so nothing can start it.',
      severity: 'error',
    });
  } else if (triggers.length > 1) {
    errors.push({
      code: 'TRIGGER_MULTIPLE',
      message: `Pipeline has ${triggers.length} trigger nodes; exactly one is allowed.`,
      severity: 'error',
      nodeId: triggers[1],
    });
  }

  // --- cycles ---------------------------------------------------------------
  const cycleNode = findCycle(known, succ);
  if (cycleNode) {
    errors.push({
      code: 'CYCLE_DETECTED',
      message: `Pipeline contains a cycle through "${cycleNode}"; it would never finish.`,
      severity: 'error',
      nodeId: cycleNode,
    });
  }

  // --- orphans (advisory) ---------------------------------------------------
  // A node wired to nothing at all cannot run and cannot have been intended;
  // it is a warning rather than an error because a draft legitimately passes
  // through that state on the way to being wired up.
  for (const id of known) {
    if ((degree.get(id) ?? 0) === 0 && rawNodes.length > 1) {
      warnings.push({
        code: 'NODE_ORPHANED',
        message: `Node "${id}" is not connected to anything.`,
        severity: 'warning',
        nodeId: id,
      });
    }
  }

  return { errors, warnings };
}

/**
 * Return some node that lies on a cycle, or null when the graph is acyclic.
 *
 * Kahn's algorithm: whatever cannot be drained has a cycle behind it. The
 * lowest such id is returned so the message is stable across calls.
 */
function findCycle(known: Set<string>, succ: Map<string, string[]>): string | null {
  const indegree = new Map<string, number>();
  for (const id of known) indegree.set(id, 0);
  for (const [, targets] of succ) {
    for (const t of targets) indegree.set(t, (indegree.get(t) ?? 0) + 1);
  }

  const ready = [...known].filter((id) => (indegree.get(id) ?? 0) === 0);
  let drained = 0;
  while (ready.length > 0) {
    const id = ready.pop() as string;
    drained++;
    for (const next of succ.get(id) ?? []) {
      const left = (indegree.get(next) ?? 1) - 1;
      indegree.set(next, left);
      if (left === 0) ready.push(next);
    }
  }
  if (drained === known.size) return null;

  const stuck = [...known].filter((id) => (indegree.get(id) ?? 0) > 0).sort();
  return stuck[0] ?? null;
}
