// platform-api/src/pipeline/definition-metadata.ts
//
// The vocabulary the pipelines page groups and marks definitions by, and the
// inference that gives a definition saved before these fields existed the same
// answers a new one carries explicitly. Pure: no I/O, no clock, unit-tested.
//
// Stored fields (additive on the definition JSON, see
// docs/design/pipelines-page/PLAN.md §2.1 in realtime-examples):
//
//   workType   the group on the page      documents | agents | conversations | other
//   kind       the row's mark              document | presentation | diagram | agent | …
//   route      short step labels           ['/doc', 'Read', 'Plan', 'Approval', 'Suggestions']
//   origin     where it came from          { kind: 'system' | 'template' | 'agent' | 'blank' | 'import', … }
//
// `decorateDefinition` fills what is missing ON THE RESPONSE ONLY. Nothing
// here writes; a legacy row is rewritten when its owner next saves it (the
// editor round-trips the whole definition), never by a read. The answer
// carries `inferred: ['workType', …]` so a consumer can tell a stored value
// from a guess.
//
// The same enum lives in `@connorhoehn/realtime-modules` (`client/pipelines/
// catalog.ts`); platform-api does not consume that bundle, so a contract test
// pins the values here.

export type WorkType = 'documents' | 'agents' | 'conversations' | 'other';

export const WORK_TYPE_ORDER: readonly WorkType[] = ['documents', 'agents', 'conversations', 'other'];

export const WORK_TYPE_LABEL: Record<WorkType, string> = {
  documents: 'Documents & presentations',
  agents: 'Agents & experiments',
  conversations: 'Conversations & media',
  other: 'Other',
};

export type PipelineKind =
  | 'document' | 'presentation' | 'diagram'
  | 'agent' | 'experiment'
  | 'recording' | 'call' | 'conversation' | 'media'
  | 'workflow';

export const PIPELINE_KINDS: readonly PipelineKind[] = [
  'document', 'presentation', 'diagram',
  'agent', 'experiment',
  'recording', 'call', 'conversation', 'media',
  'workflow',
];

export const KIND_LABEL: Record<PipelineKind, string> = {
  document: 'Document',
  presentation: 'Presentation',
  diagram: 'Diagram',
  agent: 'Agent',
  experiment: 'Experiment',
  recording: 'Recording',
  call: 'Call',
  conversation: 'Conversation',
  media: 'Media',
  workflow: 'Workflow',
};

export const KIND_WORK_TYPE: Record<PipelineKind, WorkType> = {
  document: 'documents',
  presentation: 'documents',
  diagram: 'documents',
  agent: 'agents',
  experiment: 'agents',
  recording: 'conversations',
  call: 'conversations',
  conversation: 'conversations',
  media: 'conversations',
  workflow: 'other',
};

export type OriginKind = 'system' | 'template' | 'agent' | 'blank' | 'import';
export const ORIGIN_KINDS: readonly OriginKind[] = ['system', 'template', 'agent', 'blank', 'import'];

export interface DefinitionOrigin {
  kind: OriginKind;
  templateId?: string;
  /** The /agent text, verbatim, capped at MAX_INSTRUCTION_CHARS. */
  instruction?: string;
  plannerSource?: 'model' | 'fallback';
  plannedAt?: string;
}

/** The longest instruction `origin.instruction` and `/generate` accept. */
export const MAX_INSTRUCTION_CHARS = 4000;
/** `route` shows this many steps before folding the rest into "… +N". */
export const MAX_ROUTE_ENTRIES = 6;

export type InferredField = 'workType' | 'kind' | 'route';

/** What every consumer gets back from the list and detail routes. */
export interface DecoratedDefinitionFields {
  workType: WorkType;
  kind: PipelineKind;
  route: string[];
  origin?: DefinitionOrigin;
  /** Present only when at least one field was inferred rather than stored. */
  inferred?: InferredField[];
}

interface NodeLike { id?: unknown; type?: unknown; data?: unknown }
interface EdgeLike { source?: unknown; target?: unknown }
type Def = Record<string, unknown>;

export function isWorkType(v: unknown): v is WorkType {
  return typeof v === 'string' && (WORK_TYPE_ORDER as readonly string[]).includes(v);
}
export function isPipelineKind(v: unknown): v is PipelineKind {
  return typeof v === 'string' && (PIPELINE_KINDS as readonly string[]).includes(v);
}
export function isOriginKind(v: unknown): v is OriginKind {
  return typeof v === 'string' && (ORIGIN_KINDS as readonly string[]).includes(v);
}
export function isRoute(v: unknown): v is string[] {
  return Array.isArray(v) && v.length > 0 && v.every((s) => typeof s === 'string' && s.trim() !== '');
}

// A hand-edited or half-written row may hold a null node or edge; it is
// skipped here, never a 500 for the whole list.
function nodesOf(def: Def): NodeLike[] {
  return Array.isArray(def.nodes) ? (def.nodes as unknown[]).filter((n): n is NodeLike => !!n && typeof n === 'object') : [];
}
function edgesOf(def: Def): EdgeLike[] {
  return Array.isArray(def.edges) ? (def.edges as unknown[]).filter((e): e is EdgeLike => !!e && typeof e === 'object') : [];
}
function dataOf(node: NodeLike): Record<string, unknown> {
  return node.data && typeof node.data === 'object' ? (node.data as Record<string, unknown>) : {};
}
function typeOf(node: NodeLike): string {
  if (typeof node.type === 'string' && node.type) return node.type;
  const t = dataOf(node).type;
  return typeof t === 'string' ? t : '';
}

// ─── kind ─────────────────────────────────────────────────────────────

/**
 * Node-type evidence, most specific first. A transcription pipeline also
 * publishes to a chat, and a conversation summary also writes a page, so the
 * order is the tie-break: what the pipeline is FOR beats what it touches.
 */
const KIND_BY_NODE_TYPE: ReadonlyArray<[PipelineKind, RegExp]> = [
  ['presentation', /^deck[:.]|^presentation[:.]|slides?[:.]|\.pptx/i],
  ['diagram', /^diagram[:.]|excalidraw|whiteboard/i],
  ['recording', /^recording[:.]|^transcription[:.]|transcribe|captions?/i],
  ['call', /^call[:.]|^video[:.]|^lobby[:.]/i],
  ['media', /^media[:.]|^image[:.]|thumbnail|^photo[:.]/i],
  ['conversation', /^chat:read-history$|^chat:summar|^conversation[:.]/i],
  ['document', /^document[:.]|^page[:.]|^doc[:.]/i],
  ['agent', /^agent$|^devbox[:.]|^agent[:.]/i],
];

const KIND_BY_NAME: ReadonlyArray<[PipelineKind, RegExp]> = [
  ['presentation', /\b(deck|presentation|slides?)\b/i],
  ['diagram', /\b(diagram|whiteboard|sketch)\b/i],
  ['recording', /\b(recording|transcri\w*|captions?)\b/i],
  ['call', /\b(call|meeting|hangout)\b/i],
  ['media', /\b(media|images?|photos?|catalog)\b/i],
  ['conversation', /\b(conversation|summar\w*|thread|chat)\b/i],
  ['document', /\b(document|page|doc|notes?)\b/i],
  ['experiment', /\b(experiment|a\/b|trial)\b/i],
  ['agent', /\b(agent|loop|devbox)\b/i],
];

function inferKindFromNodes(def: Def): PipelineKind | null {
  const types = nodesOf(def).map(typeOf).filter(Boolean);
  if (types.length === 0) return null;
  for (const [kind, re] of KIND_BY_NODE_TYPE) {
    if (types.some((t) => re.test(t))) return kind;
  }
  // An `agent` node whose data says it runs in a devbox, or carries a loop
  // step marker, is the chat's /agent loop shape.
  const anyLoop = nodesOf(def).some((n) => {
    const d = dataOf(n);
    return d.runtime === 'devbox' || (d.loop !== undefined && d.loop !== null);
  });
  return anyLoop ? 'agent' : null;
}

function inferKindFromOrigin(def: Def): PipelineKind | null {
  const origin = def.origin as { kind?: unknown } | undefined;
  if (origin && origin.kind === 'agent') return 'agent';
  return null;
}

function inferKindFromName(def: Def): PipelineKind | null {
  const text = [def.name, def.id, ...(Array.isArray(def.tags) ? def.tags : [])]
    .filter((v) => typeof v === 'string')
    .join(' ');
  if (!text.trim()) return null;
  for (const [kind, re] of KIND_BY_NAME) {
    if (re.test(text)) return kind;
  }
  return null;
}

/** Explicit wins; else nodes, then origin, then the name; else `workflow`. */
export function inferKind(def: Def): PipelineKind {
  if (isPipelineKind(def.kind)) return def.kind;
  return inferKindFromNodes(def) ?? inferKindFromOrigin(def) ?? inferKindFromName(def) ?? 'workflow';
}

/** Explicit wins; else the kind's group. */
export function inferWorkType(def: Def, kind: PipelineKind = inferKind(def)): WorkType {
  if (isWorkType(def.workType)) return def.workType;
  return KIND_WORK_TYPE[kind];
}

// ─── route ────────────────────────────────────────────────────────────

/**
 * Kahn's algorithm with ties broken by id; a cycle remainder is appended, not
 * dropped. A port of the host's `topologicalNodeOrder.ts`.
 */
export function topologicalNodeIds(def: Def): string[] {
  const nodes = nodesOf(def).filter((n) => typeof n.id === 'string') as Array<NodeLike & { id: string }>;
  if (nodes.length === 0) return [];
  const adj = new Map<string, string[]>();
  const indegree = new Map<string, number>();
  for (const n of nodes) { adj.set(n.id, []); indegree.set(n.id, 0); }
  for (const e of edgesOf(def)) {
    if (typeof e.source !== 'string' || typeof e.target !== 'string') continue;
    if (!adj.has(e.source) || !indegree.has(e.target)) continue;
    adj.get(e.source)!.push(e.target);
    indegree.set(e.target, (indegree.get(e.target) ?? 0) + 1);
  }
  const result: string[] = [];
  const ready = nodes.filter((n) => (indegree.get(n.id) ?? 0) === 0).map((n) => n.id).sort();
  while (ready.length > 0) {
    const id = ready.shift()!;
    result.push(id);
    for (const next of (adj.get(id) ?? []).slice().sort()) {
      const deg = (indegree.get(next) ?? 0) - 1;
      indegree.set(next, deg);
      if (deg === 0) ready.push(next);
    }
  }
  if (result.length < nodes.length) {
    const seen = new Set(result);
    result.push(...nodes.filter((n) => !seen.has(n.id)).map((n) => n.id).sort());
  }
  return result;
}

/** "Plan the edit (Haiku)" → "Plan the edit"; whitespace collapsed; ≤ 24 chars. */
function shortenLabel(label: string): string {
  const cleaned = label.replace(/\s*\([^)]*\)\s*$/, '').replace(/\s+/g, ' ').trim();
  return cleaned.length > 24 ? `${cleaned.slice(0, 23).trimEnd()}…` : cleaned;
}

/** The label a step shows in the route: its short label, else its label, else its type. */
export function routeLabel(node: NodeLike): string {
  const type = typeOf(node);
  const data = dataOf(node);
  if (type === 'approval') return 'Approval';
  const short = typeof data.shortLabel === 'string' && data.shortLabel.trim() ? data.shortLabel.trim() : null;
  const label = typeof data.label === 'string' && data.label.trim() ? data.label.trim() : null;
  if (type === 'trigger') {
    // "/doc command" → "/doc"; "Composer edit" stays.
    const text = short ?? label ?? '';
    const cmd = /^(\/[\w-]+)/.exec(text);
    if (cmd) return cmd[1];
    if (text) return shortenLabel(text);
    const triggerType = typeof data.triggerType === 'string' ? data.triggerType : '';
    return triggerType ? shortenLabel(triggerType) : 'Trigger';
  }
  if (short) return short;
  if (label) return shortenLabel(label);
  return type ? shortenLabel(type.split(':').pop() ?? type) : 'Step';
}

/**
 * The definition's steps as short labels in execution order, at most
 * MAX_ROUTE_ENTRIES with the rest folded into "… +N". Derived — never a second
 * copy of the graph; system builders set `route` explicitly so their text is
 * exact rather than heuristic.
 */
export function routeSummary(def: Def): string[] {
  const byId = new Map<string, NodeLike>();
  for (const n of nodesOf(def)) if (typeof n.id === 'string') byId.set(n.id, n);
  const labels = topologicalNodeIds(def).map((id) => routeLabel(byId.get(id)!));
  if (labels.length <= MAX_ROUTE_ENTRIES) return labels;
  const shown = labels.slice(0, MAX_ROUTE_ENTRIES - 1);
  return [...shown, `… +${labels.length - shown.length}`];
}

// ─── origin ───────────────────────────────────────────────────────────

function readOrigin(def: Def): DefinitionOrigin | undefined {
  const raw = def.origin;
  if (!raw || typeof raw !== 'object') return def.system === true ? { kind: 'system' } : undefined;
  const o = raw as Record<string, unknown>;
  if (!isOriginKind(o.kind)) return def.system === true ? { kind: 'system' } : undefined;
  const out: DefinitionOrigin = { kind: o.kind };
  if (typeof o.templateId === 'string') out.templateId = o.templateId;
  if (typeof o.instruction === 'string') out.instruction = o.instruction.slice(0, MAX_INSTRUCTION_CHARS);
  if (o.plannerSource === 'model' || o.plannerSource === 'fallback') out.plannerSource = o.plannerSource;
  if (typeof o.plannedAt === 'string') out.plannedAt = o.plannedAt;
  return out;
}

// ─── decorate ─────────────────────────────────────────────────────────

/**
 * The definition with `workType`, `kind`, `route` and `origin` filled in on
 * the RESPONSE. The input is not mutated and nothing is persisted: a stored
 * legacy row stays exactly as it was until its owner saves it again.
 */
export function decorateDefinition<T extends Def>(def: T): T & DecoratedDefinitionFields {
  const inferred: InferredField[] = [];
  const kind = inferKind(def);
  if (!isPipelineKind(def.kind)) inferred.push('kind');
  const workType = inferWorkType(def, kind);
  if (!isWorkType(def.workType)) inferred.push('workType');
  const route = isRoute(def.route) ? def.route.map((s) => s.trim()) : routeSummary(def);
  if (!isRoute(def.route)) inferred.push('route');
  const origin = readOrigin(def);
  const out = { ...def, workType, kind, route, ...(inferred.length ? { inferred } : {}) } as T & DecoratedDefinitionFields;
  // A stored origin that is not one of ours is dropped, not passed through.
  if (origin) out.origin = origin;
  else delete out.origin;
  return out;
}
