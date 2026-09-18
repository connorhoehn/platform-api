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
  | 'NODE_ORPHANED'
  | 'NODE_KIND_UNKNOWN'
  | 'EDGE_HANDLE_UNKNOWN'
  | 'EDGE_PAYLOAD_MISMATCH'
  | 'NODE_CONFIG_MISSING';

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

export interface ShapeOptions {
  /**
   * Step kinds this server can execute, from `listStepKinds()`. When given, a
   * node whose type is not among them is reported. Left out, the check is
   * skipped — so the validator stays pure and testable without the registry.
   */
  knownKinds?: Iterable<string>;
  /**
   * The handles a registered step kind declares, when it declares any.
   *
   * Handles were checked nowhere on this side, so an edge leaving
   * `condition.out` — a handle a condition does not have, its real ones being
   * `true` and `false` — published without complaint and then routed nowhere at
   * run time. The editor has refused that edge all along; an agent posting the
   * same definition straight at the API got a 200.
   *
   * Canonical node types have a fixed handle set, known below. A registered
   * kind is only checked when its handler declares `outputs`/`inputs` — the
   * alternative is guessing, and guessing is how the editor came to refuse
   * `llm:judge.fail`, which is a real handle that a real handler really routes
   * on. Undeclared stays unchecked.
   */
  handlesFor?: (kind: string) => { outputs?: readonly string[]; inputs?: readonly string[] } | undefined;
  /**
   * What a step kind takes in and hands on, when it declares it.
   *
   * Handles answer "does this wire exist"; this answers "may this travel down
   * it". `image:vision-tag.out → image:resize.in` joins two real handles and
   * gives a resizer a list of tags — it published, and failed at run time.
   *
   * Only a declared mismatch is reported. A kind that declares nothing is not
   * guessed at, for the same reason the handle checks do not guess.
   */
  payloadFor?: (kind: string) => { consumes?: string; produces?: string } | undefined;
}

/**
 * Configuration a canonical node type cannot run without.
 *
 * These mirror the editor's own per-type rules exactly. The editor has always
 * refused to publish an `llm` with no prompt or a `condition` with no
 * expression; this side validated none of it, so an agent posting the same
 * definition got a 200 and a live pipeline that failed at the first step.
 * "Per-node-type config stays in the editor" was a reasonable line to draw
 * when people were the only authors, and it stopped being one.
 *
 * Only the canonical types are listed. A registered kind's configuration is
 * the executor's business and is not guessed at here — the same rule the
 * handle and payload checks follow.
 */
export const REQUIRED_FIELDS: Record<string, readonly string[]> = {
  llm: ['provider', 'model', 'systemPrompt', 'userPromptTemplate'],
  agent: ['prompt'],
  transform: ['transformType', 'expression'],
  condition: ['expression'],
  action: ['actionType'],
  join: ['mode', 'mergeStrategy'],
  scatter: ['itemsPath'],
};

/**
 * "a condition", "an llm" — the message is read by people as well as agents.
 *
 * Sound, not spelling: `llm` begins with a consonant letter and a vowel sound
 * ("ell-em"), so the letter test gets it wrong. Only the kinds that exist need
 * an answer, so they are listed rather than guessed at phonetically.
 */
const TAKES_AN: ReadonlySet<string> = new Set(['llm', 'agent', 'action']);

function articleFor(kind: string): string {
  return TAKES_AN.has(kind) || /^[aeiou]/i.test(kind) ? 'an' : 'a';
}

/** Present, and not a blank string. */
function isSupplied(value: unknown): boolean {
  if (value === undefined || value === null) return false;
  if (typeof value === 'string') return value.trim().length > 0;
  return true;
}

/**
 * Steps that hand on whatever they are given.
 *
 * A condition routes, a fork copies, a join merges, a scatter unrolls, an
 * approval pauses. None of them changes what the payload IS, so their output
 * class is whatever reached them — resolved by walking back. Without this,
 * `image-source → condition → image:resize` would look like a mismatch.
 */
/** `image` → `an image`, `data` → `a data object`. Read aloud in a sentence. */
function article(kind: string): string {
  if (kind === 'image') return 'an image';
  if (kind === 'data') return 'a data object';
  return kind;
}

const PASSTHROUGH_KINDS: ReadonlySet<string> = new Set([
  'condition',
  'fork',
  'join',
  'scatter',
  'approval',
]);

/**
 * Outgoing handles of the canonical node types.
 *
 * `fork` and `join` are dynamic — `branch-0…`, `in-0…` — so they are matched by
 * pattern rather than listed.
 */
const CANONICAL_OUTPUTS: Record<string, readonly string[]> = {
  trigger: ['out'],
  llm: ['out', 'error'],
  agent: ['out', 'error'],
  transform: ['out'],
  condition: ['true', 'false'],
  action: ['out', 'error'],
  join: ['out'],
  approval: ['approved', 'rejected'],
};

/** Incoming handles of the canonical node types. A trigger takes none. */
const CANONICAL_INPUTS: Record<string, readonly string[]> = {
  llm: ['in'],
  agent: ['in'],
  transform: ['in'],
  condition: ['in'],
  action: ['in'],
  fork: ['in'],
  approval: ['in'],
};

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
  sourceHandle?: unknown;
  targetHandle?: unknown;
}

/** Whether `handle` is one this node type can route out of. `null` = cannot tell. */
function outputAllowed(
  type: string,
  handle: string,
  declared: readonly string[] | undefined,
): boolean | null {
  if (type === 'fork') return /^branch-\d+$/.test(handle);
  const canonical = CANONICAL_OUTPUTS[type];
  if (canonical) return canonical.includes(handle);
  if (declared && declared.length > 0) return declared.includes(handle);
  return null;
}

/**
 * What a node's output carries, walking back through pass-through steps.
 *
 * Returns null for "cannot tell", which is most of the time and is always a
 * pass: an undeclared kind, a pass-through fed by nothing or by two different
 * classes, or a graph that loops (which `CYCLE_DETECTED` owns, not this).
 */
function resolveProduced(
  nodeId: string,
  typeById: Map<string, string>,
  incoming: Map<string, string[]>,
  payloadFor: ((kind: string) => { consumes?: string; produces?: string } | undefined) | undefined,
  seen: Set<string>,
): string | null {
  const type = typeById.get(nodeId);
  if (!type) return null;
  if (!PASSTHROUGH_KINDS.has(type)) return payloadFor?.(type)?.produces ?? null;
  if (seen.has(nodeId)) return null;
  seen.add(nodeId);

  const sources = incoming.get(nodeId) ?? [];
  if (sources.length === 0) return null;
  let resolved: string | null = null;
  for (const source of sources) {
    const up = resolveProduced(source, typeById, incoming, payloadFor, seen);
    if (up === null) return null;
    if (resolved === null) resolved = up;
    else if (resolved !== up) return null; // mixed feed — cannot tell
  }
  return resolved;
}

/** Whether `handle` is one this node type can be entered by. `null` = cannot tell. */
function inputAllowed(
  type: string,
  handle: string,
  declared: readonly string[] | undefined,
): boolean | null {
  if (type === 'trigger') return false;
  if (type === 'join') return /^in-\d+$/.test(handle);
  const canonical = CANONICAL_INPUTS[type];
  if (canonical) return canonical.includes(handle);
  if (declared && declared.length > 0) return declared.includes(handle);
  return null;
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
export function validateShape(
  definition: unknown,
  options: ShapeOptions = {},
): ShapeValidation {
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
  const knownKinds = options.knownKinds ? new Set(options.knownKinds) : null;
  const handlesFor = options.handlesFor;
  const payloadFor = options.payloadFor;
  /** Node id → its declared type, for the handle checks below. */
  const typeById = new Map<string, string>();
  const known = new Set<string>();
  const triggers: string[] = [];
  const unknownKinds: Array<{ id: string; kind: string }> = [];
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
    typeById.set(id, typeof node.type === 'string' ? node.type : '');
    if (node.type === 'trigger') triggers.push(id);

    const kind = typeof node.type === 'string' ? node.type : '';
    if (knownKinds && (!kind || !knownKinds.has(kind))) {
      unknownKinds.push({ id, kind });
    }

    // Configuration the step cannot run without. Canonical types only.
    const required = REQUIRED_FIELDS[kind];
    if (required) {
      const data = (node as { data?: unknown }).data;
      const bag = (data && typeof data === 'object' ? data : {}) as Record<string, unknown>;
      for (const field of required) {
        if (!isSupplied(bag[field])) {
          errors.push({
            code: 'NODE_CONFIG_MISSING',
            message: `Step "${id}" is ${articleFor(kind)} ${kind} and has no ${field}.`,
            severity: 'error',
            nodeId: id,
          });
        }
      }
    }
  }

  // --- edges: endpoints resolve, no self-loops, handles exist --------------
  const succ = new Map<string, string[]>();
  const degree = new Map<string, number>();
  /** Edges that survived the endpoint and handle checks, for the payload pass. */
  const validEdges: Array<{ edgeId?: string; source: string; target: string }> = [];
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
    // Handles. Only reported when the answer is known — an undeclared
    // registered kind is left alone rather than guessed at.
    const sourceType = typeById.get(source) ?? '';
    const targetType = typeById.get(target) ?? '';
    const sourceHandle = typeof edge.sourceHandle === 'string' ? edge.sourceHandle : 'out';
    const targetHandle = typeof edge.targetHandle === 'string' ? edge.targetHandle : 'in';

    const outOk = outputAllowed(sourceType, sourceHandle, handlesFor?.(sourceType)?.outputs);
    if (outOk === false) {
      errors.push({
        code: 'EDGE_HANDLE_UNKNOWN',
        message: `Edge ${edgeId ?? '(unnamed)'} leaves "${source}" by "${sourceHandle}", which a ${sourceType} step does not have.`,
        severity: 'error',
        edgeId,
        nodeId: source,
      });
      continue;
    }
    const inOk = inputAllowed(targetType, targetHandle, handlesFor?.(targetType)?.inputs);
    if (inOk === false) {
      errors.push({
        code: 'EDGE_HANDLE_UNKNOWN',
        message:
          targetType === 'trigger'
            ? `Edge ${edgeId ?? '(unnamed)'} points into trigger "${target}", but a trigger is where a pipeline begins.`
            : `Edge ${edgeId ?? '(unnamed)'} enters "${target}" by "${targetHandle}", which a ${targetType} step does not have.`,
        severity: 'error',
        edgeId,
        nodeId: target,
      });
      continue;
    }

    (succ.get(source) as string[]).push(target);
    degree.set(source, (degree.get(source) ?? 0) + 1);
    degree.set(target, (degree.get(target) ?? 0) + 1);
    validEdges.push({ edgeId, source, target });
  }

  // --- payloads: what travels down an edge, vs what the far end takes -------
  //
  // A second pass, because resolving what a pass-through step hands on means
  // walking back through the graph, and the graph is not complete until every
  // edge above has been accepted.
  if (payloadFor) {
    const incoming = new Map<string, string[]>();
    for (const e of validEdges) {
      const list = incoming.get(e.target);
      if (list) list.push(e.source);
      else incoming.set(e.target, [e.source]);
    }
    for (const e of validEdges) {
      const targetType = typeById.get(e.target) ?? '';
      const wanted = payloadFor(targetType)?.consumes;
      if (!wanted) continue; // takes anything
      const got = resolveProduced(e.source, typeById, incoming, payloadFor, new Set());
      if (got === null || got === wanted) continue;
      errors.push({
        code: 'EDGE_PAYLOAD_MISMATCH',
        message: `Edge ${e.edgeId ?? '(unnamed)'} feeds "${e.target}" with ${article(got)}, but ${targetType} needs ${article(wanted)}.`,
        severity: 'error',
        edgeId: e.edgeId,
        nodeId: e.target,
      });
    }
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

  // --- unrecognised step kinds (advisory) -----------------------------------
  // A node whose kind has no handler cannot run, so this is arguably an error.
  // It is a warning for now because handler registration happens at bootstrap
  // and this route answers from whatever the CURRENT process has registered —
  // a replica that had not finished registering a plugin's handlers, or a
  // deployment mid-roll, would otherwise reject a publish that is perfectly
  // sound. Worth promoting to an error once registration timing is pinned down.
  for (const { id, kind } of unknownKinds) {
    warnings.push({
      code: 'NODE_KIND_UNKNOWN',
      message: kind
        ? `Node "${id}" has kind "${kind}", which this server has no handler for.`
        : `Node "${id}" has no kind.`,
      severity: 'warning',
      nodeId: id,
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
