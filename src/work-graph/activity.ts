/**
 * Builds the opt-in v2 activity layer over an already-authorized v1 snapshot.
 *
 * Every field here is derived from projected source evidence or from a read of
 * the source's own record. Nothing is inferred from presence, from the current
 * lifecycle status of a neighbour, or from "the same person on the same day":
 * efforts follow the relationships the sources declared, details exist only for
 * nodes the viewer already sees at `details` disclosure, and a lease that has
 * expired is dropped rather than reported as still running.
 */
import {
  bucketWorkEvents,
  deriveWorkEfforts,
  detailsForDisclosedNodes,
  freshWorkOperations,
  WORK_GRAPH_LIMITS,
  WORK_GRAPH_V2_LIMITS,
  type DeriveWorkEffortsInput,
  type ViewerWorkEffort,
  type ViewerArtifactRevision,
  type ViewerWorkActivityDetail,
  type ViewerWorkNodeLink,
  type ViewerWorkOperation,
  type WorkRunPause,
  type WorkGraphActivityV2,
  type WorkGraphQueryV2,
} from '@connorhoehn/realtime-modules/work-graph';
import type { StoredWorkNode } from '../repositories/WorkGraphRepository';
import { distinctHeading } from '../deck/distinctHeading';
import { parsePipelineStepResourceId } from './sources/mapPipeline';
import type { AuthorizedWorkGraph, ViewerWorkEdge, ViewerWorkNode } from './policy';

/** A composer edit, as its run recorded it. Every field is the run's own. */
export interface DocumentEditFacts {
  /** The instruction the person typed, verbatim (trimmed to a label). */
  instruction: string;
  /** Where it applied: "Whole deck", "Slide “Risks”", "Slide “Risks” · title". */
  scope: string;
  documentId: string;
  /** The presentation's title, as the load step read it. */
  title?: string;
  /** The revision the edit was made against. */
  baseRevisionId?: string;
  /** `request`: the editor's unsaved deck; `head`: the newest saved revision. */
  specFrom?: 'request' | 'head';
  /** The model's own one-line account of the change. */
  summary?: string;
  changedSlides?: number;
  /** The revision the write step saved; absent when nothing was saved. */
  revisionId?: string;
  /** Set when the write rebased a scoped edit onto a newer head. */
  rebasedOnto?: string;
  /** The write step finished and saved nothing because nothing changed. */
  unchanged?: boolean;
}

/** Source facts, already read from the owning service's own record. */
export interface PipelineRunFacts {
  runId: string;
  /** The pipeline definition's own name, for example "Presentation agent". */
  agentLabel?: string;
  status: 'running' | 'waiting' | 'completed' | 'error' | 'stopped';
  attempt: number;
  startedAt: string;
  updatedAt: string;
  /** Step/tool identities this run actually executed. */
  tools: string[];
  /** Sanitized failure summary. Never a stack trace, path, or credential. */
  error?: string;
  /**
   * The step this run is executing right now, taken from the run's own step
   * state. A run is "running" for as long as it holds the lease; this says
   * which of its steps is actually doing the work.
   */
  runningStep?: { stepId: string };
  /**
   * NFR #126: why a `waiting` run waits — an approval gate or a breakpoint,
   * and the step it waits at. Read from the run's own row; absent otherwise.
   */
  pause?: WorkRunPause;
  /**
   * What a document-edit run (a composer edit, `deck-revise`) was asked to do
   * and what it wrote, copied from the run's own step outputs: the trigger's
   * instruction and scope, the load step's base revision, the propose step's
   * summary, the write step's revision. Absent for every other run.
   */
  edit?: DocumentEditFacts;
  /**
   * Present only when the node names ONE step of the run (`runId#stepId`).
   * Every field is copied from the step's own recorded output or its live
   * iteration events — nothing here is averaged over the run or guessed.
   */
  step?: {
    stepId: string;
    /** `sonnet`, `opus` … exactly as the step was configured. */
    model?: string;
    /** Turns of the agent loop this step has actually taken. */
    iterations?: number;
    contextTokens?: number;
    contextBudgetTokens?: number;
    /** `devbox` when the step ran in a cloud workspace, `local` otherwise. */
    runtime?: string;
    /** The devbox session the step executed in, when it had one. */
    sessionId?: string;
    boxId?: string;
    /** Why the loop stopped: `complete`, `budget`, `max_turns`, `timeout`… */
    stopReason?: string;
  };
}

/** An authorized handle for one revision's preview bytes. */
export const PREVIEW_HANDLE_SEPARATOR = '.';

/**
 * Names a revision preview with values the viewer already holds: the opaque
 * node id this snapshot gave them and the revision id it already listed. It
 * carries no source identifier and grants nothing on its own — the route it
 * is handed to re-authorizes the node before serving a byte.
 */
export function previewHandle(nodeId: string, revisionId: string, slideId?: string): string {
  return [nodeId, revisionId, ...(slideId ? [slideId] : [])].join(PREVIEW_HANDLE_SEPARATOR);
}

export interface ParsedPreviewHandle {
  nodeId: string;
  revisionId: string;
  /** Present only for a per-slide handle, `<nodeId>.<revisionId>.<slideId>`. */
  slideId?: string;
}

/**
 * Every reading of a handle, most specific first.
 *
 * A node id may itself contain dots, so `a.b.c` is either slide `c` of
 * revision `b` on node `a`, or revision `c` on node `a.b`. Both are returned
 * and the caller keeps whichever one this snapshot actually listed — guessing
 * here would answer for a revision the viewer was never shown.
 */
export function parsePreviewHandles(handle: string): ParsedPreviewHandle[] {
  const parts = handle.split(PREVIEW_HANDLE_SEPARATOR);
  const candidates: ParsedPreviewHandle[] = [];
  if (parts.length >= 3) {
    const slideId = parts[parts.length - 1]!;
    const revisionId = parts[parts.length - 2]!;
    const nodeId = parts.slice(0, parts.length - 2).join(PREVIEW_HANDLE_SEPARATOR);
    if (nodeId && revisionId && slideId) candidates.push({ nodeId, revisionId, slideId });
  }
  if (parts.length >= 2) {
    const revisionId = parts[parts.length - 1]!;
    const nodeId = parts.slice(0, parts.length - 1).join(PREVIEW_HANDLE_SEPARATOR);
    if (nodeId && revisionId) candidates.push({ nodeId, revisionId });
  }
  return candidates;
}

export interface RevisionPreview {
  contentType: 'image/svg+xml' | 'image/png' | 'image/jpeg' | 'image/webp';
  body: Buffer;
}

/**
 * One revision as the source's own record describes it: the slides it is made
 * of and the documents it declared it was built from. A presentation stores
 * this per revision, so slide 7 of v1 is a different slide from slide 7 of v2.
 */
export interface ArtifactRevisionFacts {
  id: string;
  /** How the source numbered it, for example `v2`. */
  label?: string;
  /** When the source recorded it. A revision with no time cannot be listed. */
  createdAt?: string;
  /** How many slides the source says this revision has. */
  slideCount: number;
  slides: Array<{ id: string; index: number; title: string }>;
  /** The revision's own heading (a deck's title slide), as the source stored it. */
  heading?: string;
  /**
   * The documents this revision cites, as the generator recorded them. Absent
   * when the source declared none — never the run's inputs guessed into place.
   */
  sources?: Array<{ documentId: string; title: string; kind?: 'document' | 'transcript' }>;
}

export interface ArtifactFacts {
  /** The document's own title in its store, which the graph node may abbreviate. */
  name?: string;
  mediaKind: 'presentation' | 'document' | 'image';
  pageCount?: number;
  /** Addressable units of the newest available revision. */
  anchors?: Array<{ kind: 'slide' | 'page' | 'block'; id: string; label: string; index?: number }>;
  /** What each stored revision is made of, keyed by the source's revision id. */
  revisions?: ArtifactRevisionFacts[];
  /** Comments this viewer may read, counted at the query cutoff. */
  feedbackCount?: number;
}

export interface TranscriptFacts {
  segments: Array<{ id: string; at: string; speaker: string; text: string }>;
  speakerCount: number;
  actionItemCount: number;
  /** Whether the source says the transcript finished processing. */
  ready: boolean;
}

export interface MeetingFacts {
  startedAt?: string;
  endedAt?: string;
  recorded: boolean;
  transcriptAvailable: boolean;
  /** Attendees the source recorded, already resolved to display names. */
  participants?: Array<{ id: string; label: string; avatarUrl?: string }>;
  /**
   * When a room is EXPECTED to happen, for one that has not. It is not
   * `startedAt`: nothing has started, and saying so would be a lie a card
   * cannot tell apart from a meeting in progress.
   */
  scheduledFor?: { at: string; durationMinutes?: number };
  /** Whether anything was actually put in a calendar. Usually nothing was. */
  calendar?: 'none' | 'invited';
}

export interface DocumentAttentionFacts {
  actorId: string;
  observedAt: string;
  expiresAt: string;
  revisionId?: string;
}

/** Each method reads one source of truth. A `null` result discloses nothing. */
export interface WorkActivityReader {
  pipelineRun(runId: string): Promise<PipelineRunFacts | null>;
  /**
   * The same facts for many resources at once. Optional: a reader without it
   * is asked one at a time, which is correct but is one round trip per node.
   */
  pipelineRuns?(resources: string[]): Promise<Map<string, PipelineRunFacts>>;
  artifact(documentId: string): Promise<ArtifactFacts | null>;
  /**
   * The stored bytes for one revision, or for one slide of it when the caller
   * names a slide. Null when the source has none.
   */
  revisionPreview(documentId: string, revisionId: string, slideId?: string): Promise<RevisionPreview | null>;
  transcript(transcriptId: string): Promise<TranscriptFacts | null>;
  meeting(meetingId: string): Promise<MeetingFacts | null>;
  /** Authoritative human attention, with its own expiry. Never a socket. */
  attention(documentId: string): Promise<DocumentAttentionFacts | null>;
}

/** A document's own record and its attention state, read once per build. */
type DocumentFacts = [
  Awaited<ReturnType<WorkActivityReader['artifact']>>,
  Awaited<ReturnType<WorkActivityReader['attention']>>,
];

export interface BuildWorkActivityInput {
  query: WorkGraphQueryV2;
  nodes: ViewerWorkNode[];
  edges: ViewerWorkEdge[];
  authorization: AuthorizedWorkGraph;
  reader: WorkActivityReader;
  now: string;
  /** Filled with phase wall times (ms) when given: runs, meetings, describe. */
  timing?: Record<string, number>;
}

/** A source lease, not a lifecycle flag: five minutes without evidence is stale. */
const FRESHNESS_LEASE_MS = WORK_GRAPH_LIMITS.heartbeatStaleMs;
const PROCESS_KINDS = new Set(['run', 'agent', 'terminal']);
/**
 * Relationships that describe a process doing work. `derived-from` names the
 * evidence a run consumed, which is provenance rather than execution, so it
 * never animates as a live operation.
 */
const OPERATION_RELATIONS = new Set(['produced', 'runs-in', 'operates-on', 'works-on']);

/**
 * Work started from a conversation is anchored on that conversation. The
 * default anchors are the containers a person already thinks in; a chat is one
 * of them, and without it an `/agent` run would sit in the graph attached to
 * nothing. The conversation node still comes from the conversation source —
 * this only says which kinds may name an effort.
 */
const WORK_EFFORT_ANCHORS = ['project', 'task', 'meeting', 'conversation'] as const;

function instant(value: string | undefined): number {
  const parsed = value === undefined ? Number.NaN : Date.parse(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

/**
 * The run a cluster of run nodes began as. Its steps are created after it, so
 * newest-first would name an effort after whichever step happened to run last.
 * A member the viewer may not read can never supply the name.
 */
/**
 * The pipeline run a node belongs to, read from the resource id its own source
 * wrote: a step is addressed `runId::stepId`, and a run is addressed `runId`.
 *
 * This is containment the source stated, not an inference from adjacency — and
 * unlike the `derived-from` relationship it does not depend on delivery order.
 * The projection queue is drained in concurrent batches, so a step can be
 * projected BEFORE its run, and a cross-source edge whose counterpart does not
 * exist yet is dropped for good. Every step of a run then floated unframed.
 */
function pipelineRunOf(authorization: AuthorizedWorkGraph, node: ViewerWorkNode): string | undefined {
  // Only the run and its steps are addressed this way. A workspace is a
  // `terminal` addressed by its session id, which is not a run id and must
  // never be read as one; it joins its run through the `runs-in` relationship
  // it was projected with, in the same event.
  if (node.kind !== 'run' && node.kind !== 'agent') return undefined;
  const stored = storedNode(authorization, node.id);
  if (stored?.sourceRef.source !== 'pipeline') return undefined;
  return parsePipelineStepResourceId(stored.sourceRef.resourceId).runId;
}

function earliestRun(members: readonly ViewerWorkNode[]): ViewerWorkNode | undefined {
  return members
    .filter((member) => (member.kind === 'run' || member.kind === 'agent')
      && member.disclosure !== 'existence' && !member.locked)
    .sort((left, right) => instant(left.startedAt) - instant(right.startedAt)
      || left.id.localeCompare(right.id))[0];
}

/** `200000 → '200k'`, `146_320 → '146k'`, `900 → '900'`. Never rounded up to a lie. */
export function formatTokens(value: number): string {
  if (!Number.isFinite(value) || value < 0) return '0';
  return value >= 1_000 ? `${Math.floor(value / 1_000)}k` : String(Math.floor(value));
}

/** `sonnet` → `Sonnet`, `claude-3-5-haiku` → `Haiku`. The name a person typed. */
function modelName(model: string): string {
  const last = model.trim().split(/[\s/]+/).pop() ?? model;
  const known = ['opus', 'sonnet', 'haiku'].find((name) => last.toLowerCase().includes(name));
  const label = known ?? last;
  return label.charAt(0).toUpperCase() + label.slice(1);
}

/**
 * "Sonnet · 146k / 200k · 4 iterations" — model, context against the budget it
 * was given, and turns actually taken. Each clause appears only when the step
 * reported it; a step that reported nothing yields no line at all.
 */
export function agentStepLines(step: NonNullable<PipelineRunFacts['step']>): string[] {
  const clauses = [
    step.model ? modelName(step.model) : undefined,
    step.contextTokens !== undefined
      ? (step.contextBudgetTokens !== undefined
        ? `${formatTokens(step.contextTokens)} / ${formatTokens(step.contextBudgetTokens)}`
        : formatTokens(step.contextTokens))
      : undefined,
    step.iterations !== undefined
      ? `${step.iterations} iteration${step.iterations === 1 ? '' : 's'}`
      : undefined,
    // `budget` and `max_turns` are why a loop stopped short. Saying so is the
    // difference between a finished step and one that ran out of room.
    step.stopReason && step.stopReason !== 'complete' ? `stopped: ${step.stopReason}` : undefined,
  ].filter((value): value is string => !!value);
  const head = clauses.length ? clauses.join(' · ') : undefined;
  // The workspace the step ran in, named only when the source gave us one.
  const workspace = step.runtime === 'devbox' && step.sessionId
    ? `Cloud workspace · ${step.sessionId}`
    : undefined;
  return [head, workspace].filter((value): value is string => !!value);
}

/**
 * A failed edit, in the words of its cause. The run's error is
 * `<step>: deck-revise <code>: <reason>`; a reader needs the reason, and for
 * a conflict the word "conflict" — the step id and the pipeline id are ours.
 */
export function editFailure(error: string | undefined): string {
  if (!error) return 'The edit failed before it saved anything.';
  const parsed = /^(?:[A-Za-z0-9_:-]+: )?[a-z]+-(?:revise|edit) ([a-z-]+): ([\s\S]+)$/.exec(error.trim());
  if (!parsed) return error;
  const [, code, reason] = parsed;
  if (code === 'revision-conflict') return `Conflict: ${reason}`;
  return reason!;
}

/** The one line that says what an edit (or one step of it) did. */
export function editSummary(edit: DocumentEditFacts, stepId: string | undefined, status: string): string | undefined {
  const deck = edit.title ?? 'the presentation';
  const settled = status === 'completed';
  if (stepId === 'load') {
    if (!edit.baseRevisionId) return settled ? `Read ${deck}` : `Reading ${deck}`;
    return `${settled ? 'Read' : 'Reading'} ${edit.baseRevisionId}${edit.specFrom === 'request' ? ' with the editor’s unsaved changes' : ''}`;
  }
  if (stepId === 'propose') return edit.summary ?? (settled ? 'Wrote the edit' : 'Writing the edit');
  if (stepId === 'check') return edit.summary ?? (settled ? 'Checked the edit' : 'Checking the edit');
  if (stepId === 'write' || !stepId) {
    if (edit.revisionId) {
      return `${stepId ? 'Saved' : 'Wrote'} ${edit.revisionId} of ${deck}${edit.rebasedOnto ? ` (rebased onto ${edit.rebasedOnto})` : ''}`;
    }
    if (edit.unchanged) return `Nothing changed — ${deck} was left as it was`;
    return settled ? undefined : `Editing ${deck}`;
  }
  return undefined;
}

/** Scope, and the revision it started from → the revision it wrote. */
export function editLines(edit: DocumentEditFacts, stepId: string | undefined): string[] {
  const from = edit.baseRevisionId;
  const revision = edit.revisionId
    ? (from ? `${from} → ${edit.revisionId}` : edit.revisionId)
    : (from ? `From ${from}` : undefined);
  const changed = edit.changedSlides !== undefined && edit.changedSlides > 0 && (!stepId || stepId !== 'load')
    ? `${edit.changedSlides} slide${edit.changedSlides === 1 ? '' : 's'} changed`
    : undefined;
  const lines = [edit.scope, stepId === 'load' ? undefined : revision, changed];
  // The run's pane also says what the model said it did; a step's own summary already is that.
  if (!stepId && edit.summary && edit.revisionId) lines.push(edit.summary);
  return lines.filter((line): line is string => !!line);
}

function storedNode(authorization: AuthorizedWorkGraph, nodeId: string): StoredWorkNode | undefined {
  const target = authorization.targets.get(nodeId);
  return target && target.kind === 'node' ? target.node : undefined;
}

function clock(value: string | undefined, timezone: string): string | undefined {
  if (!value) return undefined;
  const parsed = Date.parse(value);
  if (!Number.isFinite(parsed)) return undefined;
  return new Intl.DateTimeFormat('en-GB', {
    timeZone: timezone, hour: '2-digit', minute: '2-digit', hourCycle: 'h23',
  }).format(new Date(parsed));
}

/**
 * Statuses that mean the process is over. A lease is evidence that something
 * is WORKING, so a run that finished — including one that finished by
 * spending the budget it was given — must not keep one just because its last
 * event was recent. It read as still running for five minutes after it
 * stopped.
 */
const SETTLED_STATUSES = new Set(['completed', 'error', 'stopped']);

/**
 * A step cannot outlive the run it belongs to. Its own terminal transition can
 * be lost — the run ends, its session is torn down, and a step that was still
 * `running` never hears about it — so the run's state settles its steps too.
 * Without this a finished loop kept steps reading `running`, holding leases
 * and animating operations for five minutes after there was nothing there.
 */
function settled(node: ViewerWorkNode, runStatus: string | undefined): boolean {
  return SETTLED_STATUSES.has(node.status) || (runStatus !== undefined && SETTLED_STATUSES.has(runStatus));
}

function lease(observedAt: string | undefined, now: string): { observedAt: string; expiresAt: string } | undefined {
  const observed = observedAt === undefined ? Number.NaN : Date.parse(observedAt);
  if (!Number.isFinite(observed)) return undefined;
  const expires = observed + FRESHNESS_LEASE_MS;
  if (expires <= Date.parse(now)) return undefined;
  return { observedAt: new Date(observed).toISOString(), expiresAt: new Date(expires).toISOString() };
}

function trim(value: string | undefined, limit = WORK_GRAPH_LIMITS.labelLength): string | undefined {
  const text = value?.trim();
  if (!text) return undefined;
  return text.length <= limit ? text : `${text.slice(0, limit - 1)}…`;
}

function link(node: ViewerWorkNode | undefined, meta?: string): ViewerWorkNodeLink | undefined {
  if (!node || node.locked || node.disclosure === 'existence') return undefined;
  const label = trim(node.title);
  if (!label) return undefined;
  return { nodeId: node.id, label, ...(trim(meta) ? { meta: trim(meta) as string } : {}) };
}

/**
 * Labels a revision the way its own source numbered it. `v2` stays `v2` and
 * `rev-6` reads `rev 6`; only an id with no number in it falls back to its
 * position, which is the one case where the source told us nothing.
 */
/**
 * The "where" half of a source-written context line (`"Gateway · Coverage
 * checks"` → `"Gateway"`). The convention belongs to the source; when it does
 * not follow it, the whole line is the context.
 */
function contextOf(node: ViewerWorkNode): string {
  const description = node.description?.trim();
  if (!description) return node.title;
  const [head] = description.split('·');
  return head.trim() || description;
}

/** The "what" half of that same line: the work stream the anchor represents. */
function streamOf(node: ViewerWorkNode): string | undefined {
  const description = node.description?.trim();
  if (!description || !description.includes('·')) return undefined;
  const tail = description.slice(description.lastIndexOf('·') + 1).trim();
  return tail || undefined;
}

function revisionLabel(revisionId: string, index: number): string {
  const digits = /(\d+)\s*$/.exec(revisionId);
  if (!digits) return `rev ${index + 1}`;
  return /^v/i.test(revisionId) ? `v${digits[1]}` : `rev ${digits[1]}`;
}

interface GraphIndex {
  byId: Map<string, ViewerWorkNode>;
  /** Outgoing edges keyed by the `fromId` node. */
  out: Map<string, ViewerWorkEdge[]>;
  in: Map<string, ViewerWorkEdge[]>;
}

function indexGraph(nodes: ViewerWorkNode[], edges: ViewerWorkEdge[]): GraphIndex {
  const byId = new Map(nodes.map((node) => [node.id, node]));
  const out = new Map<string, ViewerWorkEdge[]>();
  const incoming = new Map<string, ViewerWorkEdge[]>();
  for (const edge of edges) {
    if (!byId.has(edge.fromId) || !byId.has(edge.toId)) continue;
    out.set(edge.fromId, [...(out.get(edge.fromId) ?? []), edge]);
    incoming.set(edge.toId, [...(incoming.get(edge.toId) ?? []), edge]);
  }
  return { byId, out, in: incoming };
}

/** The run that a source said produced this document, if the viewer sees it. */
function producingRun(index: GraphIndex, documentId: string): ViewerWorkNode | undefined {
  // The NEWEST producing run. A deck edited three times has four producing
  // runs; taking the first edge read the oldest, so a failure from before
  // v2–v4 were saved stayed on the card as "v5 failed" (NFR Loop 28).
  let newest: ViewerWorkNode | undefined;
  let newestAt = -Infinity;
  for (const edge of index.in.get(documentId) ?? []) {
    const from = index.byId.get(edge.fromId);
    if (edge.relation !== 'produced' || !from || (from.kind !== 'run' && from.kind !== 'agent')) continue;
    const at = Date.parse(from.startedAt ?? from.updatedAt);
    const when = Number.isFinite(at) ? at : -Infinity;
    if (!newest || when > newestAt) { newest = from; newestAt = when; }
  }
  return newest;
}

/**
 * The step that draws a presentation's slides. A run holding its lease is
 * "running"; a run inside THIS step is rendering, which is what the pending
 * attempt on the card is allowed to say it is doing.
 */
const PRESENTATION_RENDER_STEP = 'deck';

/**
 * Whether the run is rendering right now, from the run's own step state: the
 * run record's running step, or — when the loop files each step as its own
 * node — that step node's own record. Never inferred from the run's status.
 */
function isRendering(runState: PipelineRunFacts, runFacts: Map<string, PipelineRunFacts>): boolean {
  if (runState.runningStep?.stepId === PRESENTATION_RENDER_STEP) return true;
  return [...runFacts.values()].some((facts) => facts.runId === runState.runId
    && facts.status === 'running'
    && facts.step?.stepId === PRESENTATION_RENDER_STEP);
}

/** The evidence a run declared it consumed. Only `derived-from` counts. */
function runInputs(index: GraphIndex, runId: string): ViewerWorkNode[] {
  return (index.out.get(runId) ?? [])
    .filter((edge) => edge.relation === 'derived-from')
    .map((edge) => index.byId.get(edge.toId))
    .filter((node): node is ViewerWorkNode => !!node);
}

export async function buildWorkActivity(input: BuildWorkActivityInput): Promise<WorkGraphActivityV2> {
  const { query, nodes, edges, authorization, reader, now, timing } = input;
  let phaseStarted = Date.now();
  const phase = (name: string): void => {
    if (timing) timing[name] = Date.now() - phaseStarted;
    phaseStarted = Date.now();
  };
  const index = indexGraph(nodes, edges);
  const details = new Map<string, ViewerWorkActivityDetail>();
  const position = new Map(nodes.map((node, at) => [node.id, at]));
  const operations: ViewerWorkOperation[] = [];
  const detailed = nodes.filter((node) => node.disclosure === 'details' && !node.locked);

  // A transcript's first line names its meeting's span, so the meeting branch
  // records what it computed rather than each branch re-reading the source.
  const meetingLines = new Map<string, string>();

  const setDetail = (nodeId: string, patch: Partial<ViewerWorkActivityDetail>): void => {
    const current = details.get(nodeId) ?? { nodeId };
    details.set(nodeId, { ...current, ...patch });
  };

  // Revision history comes from the document's own authorized save records:
  // one projected `change` node per revision, so a revision the viewer may not
  // see never reaches the revision list.
  const revisionsByDocument = new Map<string, Array<{ id: string; label: string; createdAt: string }>>();
  // A revision node is titled "<deck> · revision" from its event; renamed with
  // its document below, so the two never disagree about what the deck is called.
  const changeNodesByDocument = new Map<string, Array<{ title: string }>>();
  for (const node of detailed) {
    if (node.kind !== 'change') continue;
    const stored = storedNode(authorization, node.id);
    const resourceId = stored?.sourceRef.resourceId ?? '';
    const separator = resourceId.lastIndexOf(':');
    if (stored?.sourceRef.source !== 'document' || separator <= 0) continue;
    const documentId = resourceId.slice(0, separator);
    changeNodesByDocument.set(documentId, [...(changeNodesByDocument.get(documentId) ?? []), node]);
    const revisionId = resourceId.slice(separator + 1);
    const at = node.startedAt ?? node.updatedAt;
    revisionsByDocument.set(documentId, [
      ...(revisionsByDocument.get(documentId) ?? []),
      { id: revisionId, label: revisionId, createdAt: new Date(Date.parse(at)).toISOString() },
    ]);
  }
  for (const [documentId, list] of revisionsByDocument) {
    list.sort((left, right) => Date.parse(left.createdAt) - Date.parse(right.createdAt));
    revisionsByDocument.set(documentId, list.map((revision, position) => ({
      ...revision,
      label: revisionLabel(revision.id, position),
    })));
  }

  // Which node a source record belongs to, so a document a revision cites by
  // its own id can be named as the node this viewer already sees — and dropped
  // entirely when they see no such node.
  const nodesByResource = new Map<string, ViewerWorkNode>();
  for (const node of nodes) {
    const resourceId = storedNode(authorization, node.id)?.sourceRef.resourceId;
    if (resourceId && !nodesByResource.has(resourceId)) nodesByResource.set(resourceId, node);
  }

  const runFacts = new Map<string, PipelineRunFacts>();
  const runNodes = detailed
    .filter((node) => node.kind === 'run' || node.kind === 'agent')
    .map((node) => ({ node, resourceId: storedNode(authorization, node.id)?.sourceRef.resourceId }))
    .filter((entry): entry is { node: ViewerWorkNode; resourceId: string } => !!entry.resourceId);
  if (reader.pipelineRuns) {
    // One read for the whole day's runs. Every step of a run resolves from the
    // same row, so a loop with ten steps costs one lookup rather than eleven.
    const byResource = await reader.pipelineRuns(runNodes.map((entry) => entry.resourceId));
    for (const entry of runNodes) {
      const facts = byResource.get(entry.resourceId);
      if (facts) runFacts.set(entry.node.id, facts);
    }
  } else {
    await Promise.all(runNodes.map(async (entry) => {
      const facts = await reader.pipelineRun(entry.resourceId);
      if (facts) runFacts.set(entry.node.id, facts);
    }));
  }

  phase('runs');
  // Meetings first: a transcript's first line names its meeting's span, and
  // resolving both in one parallel pass left that line to a race.
  const ordered = [
    ...detailed.filter((node) => node.kind === 'meeting'),
    ...detailed.filter((node) => node.kind !== 'meeting'),
  ];
  // The state of each pipeline run, by the resource id its own steps name.
  const runStatusById = new Map(nodes.flatMap((node) => {
    const stored = storedNode(authorization, node.id);
    if (stored?.sourceRef.source !== 'pipeline') return [];
    const parsed = parsePipelineStepResourceId(stored.sourceRef.resourceId);
    return parsed.stepId ? [] : [[parsed.runId, node.status] as const];
  }));
  const runStatusOf = (node: ViewerWorkNode): string | undefined => {
    const runId = pipelineRunOf(authorization, node);
    return runId ? runStatusById.get(runId) : undefined;
  };

  const describe = async (node: ViewerWorkNode): Promise<void> => {
    const stored = storedNode(authorization, node.id);
    const freshness = settled(node, runStatusOf(node))
      ? undefined
      : lease(stored?.updatedAt ?? node.updatedAt, now);

    if (node.kind === 'run' || node.kind === 'agent') {
      const facts = runFacts.get(node.id);
      const inputs = runInputs(index, node.id)
        .map((target) => link(target, target.kind === 'transcript' ? 'Meeting transcript' : undefined))
        .filter((value): value is ViewerWorkNodeLink => !!value);
      const produced = (index.out.get(node.id) ?? [])
        .filter((edge) => edge.relation === 'produced')
        .map((edge) => index.byId.get(edge.toId))
        .filter((target): target is ViewerWorkNode => !!target)[0];
      // The run's own record is authoritative for its outcome; the projected
      // node status can lag the source by one lifecycle event. That authority
      // has to reach `node.status` itself, not just this detail's summary —
      // otherwise a reader sees "Running" in the header chip while the body
      // (sourced from `summary` below) says the run failed. `node` is the same
      // object the caller also hands to the v2 snapshot builder, so correcting
      // it here reaches the wire.
      const failed = facts?.status === 'error' || (!facts && node.status === 'error');
      if (failed && node.status !== 'error') {
        node.status = 'error';
        node.endedAt ??= facts?.updatedAt ?? now;
      }
      // The same authority for WHEN it started: the projected node carries
      // the time its lifecycle event was stamped, which for a re-observed or
      // re-published "started" event is not the run's start. The run page
      // reads the record's `startedAt`, so the card must too, or the two
      // disagree about the same run.
      if (facts?.startedAt && Number.isFinite(Date.parse(facts.startedAt)) && node.startedAt !== facts.startedAt) {
        node.startedAt = facts.startedAt;
      }
      const edit = facts?.edit;
      const stepId = facts?.step?.stepId;
      const summary = failed
        ? trim(edit ? editFailure(facts?.error) : (facts?.error ?? 'The run failed before it produced a revision.'))
        : edit ? trim(editSummary(edit, stepId, node.status))
          : produced ? trim(`Creating the next revision of ${produced.title}`) : undefined;
      // The presentation the edit wrote into, when the viewer can see it: the
      // run's own `produced` edge, else the document node its source
      // addresses by the id the run recorded. `meta` is the revision it
      // wrote, so a pane can open the deck AT that revision, not at the head.
      const editTarget = edit
        ? (produced ?? nodes.find((candidate) => candidate.kind === 'document'
          && storedNode(authorization, candidate.id)?.sourceRef.resourceId === edit.documentId))
        : undefined;
      // The load step names the revision it READ; the run and its other steps
      // the one the edit wrote.
      const workItem = edit && editTarget
        ? link(editTarget, stepId === 'load' ? edit.baseRevisionId : edit.revisionId)
        : undefined;
      const tools = facts?.tools.slice(0, 16) ?? [];
      // "Presentation agent · 3 tools": the definition's own name and the
      // number of steps this run actually recorded.
      const agentLine = facts?.agentLabel && tools.length
        ? `${facts.agentLabel} · ${tools.length} tool${tools.length === 1 ? '' : 's'}`
        : trim(facts?.agentLabel);
      // A node that names ONE step reads as that step's own loop, not as the
      // run: "Sonnet · 146k / 200k · 4 iterations", and the workspace it is
      // running in. Every clause is dropped when the step did not report it.
      const stepLines = facts?.step ? agentStepLines(facts.step) : [];
      const lines = [
        ...(edit ? editLines(edit, stepId) : []),
        ...(agentLine && !edit ? [agentLine] : []),
        ...stepLines,
      ].map((line) => trim(line)).filter((line): line is string => !!line).slice(0, WORK_GRAPH_V2_LIMITS.detailLines);
      setDetail(node.id, {
        ...(summary ? { summary } : {}),
        ...(lines.length ? { lines } : {}),
        // What the person asked for, verbatim — on the run and on each step,
        // so any node of the edit says what was being edited.
        ...(edit ? { excerpt: trim(edit.instruction) as string } : {}),
        ...(workItem ? { workItem } : {}),
        ...(tools.length ? { tools } : {}),
        ...(inputs.length ? { inputs } : {}),
        ...(freshness ? { freshness } : {}),
        // "Needs approval" / "Paused at write" instead of a bare "Waiting" —
        // only while BOTH the record and the projected node say waiting, so a
        // run that resumed never carries a stale reason.
        ...(facts?.pause && facts.status === 'waiting' && node.status === 'waiting' ? { pause: facts.pause } : {}),
      });
      return;
    }

    if (node.kind === 'terminal') {
      const item = (index.out.get(node.id) ?? [])
        .filter((edge) => edge.relation === 'works-on')
        .map((edge) => index.byId.get(edge.toId))
        .filter((target): target is ViewerWorkNode => !!target)[0];
      const workItem = item ? link(item, trim(item.description)) : undefined;
      const started = clock(node.startedAt, query.timezone);
      setDetail(node.id, {
        // What the source says this session is doing, not a status word we
        // chose for it.
        ...(trim(node.description) ? { badge: trim(node.description) as string } : {}),
        ...(item ? { lines: [`${contextOf(item)} / ${item.title}`] } : {}),
        ...(started ? { footer: { label: `Cloud session · ${started}`, note: 'Shared' } } : {}),
        ...(workItem ? { workItem } : {}),
        ...(freshness ? { freshness } : {}),
      });
      return;
    }

    if (node.kind === 'transcript') {
      const facts = stored ? await reader.transcript(stored.sourceRef.resourceId) : null;
      if (!facts) return;
      // The meeting this transcript derives from names its first line; a
      // transcript with no disclosed meeting simply does not get one.
      const source = (index.out.get(node.id) ?? [])
        .filter((edge) => edge.relation === 'derived-from')
        .map((edge) => index.byId.get(edge.toId))
        .find((target) => target?.kind === 'meeting');
      const meetingLine = source && meetingLines.get(source.id);
      const lines = [
        ...(meetingLine ? [`${source.title} · ${meetingLine}`] : []),
        `${facts.speakerCount} speakers · ${facts.actionItemCount} action item${facts.actionItemCount === 1 ? '' : 's'}`,
      ];
      const excerpt = trim(facts.segments[0]?.text.replace(/[.!?]+$/, ''));
      setDetail(node.id, {
        lines,
        ...(facts.ready ? { badge: 'Ready' } : {}),
        ...(excerpt ? { excerpt } : {}),
        transcript: {
          segments: facts.segments.slice(0, 500),
          // Ask is a held source capability, not a hint the panel may set.
          askEnabled: node.capabilities.includes('view-transcript'),
        },
      });
      return;
    }

    if (node.kind === 'meeting') {
      const facts = stored ? await reader.meeting(stored.sourceRef.resourceId) : null;
      if (!facts) return;
      const from = clock(facts.startedAt, query.timezone);
      const through = clock(facts.endedAt, query.timezone);
      const minutes = facts.startedAt && facts.endedAt
        ? Math.round((Date.parse(facts.endedAt) - Date.parse(facts.startedAt)) / 60_000)
        : undefined;
      const span = from && through ? `${from}–${through}${minutes ? ` · ${minutes} min` : ''}` : undefined;
      if (span) meetingLines.set(node.id, `${from}–${through}`);
      // "transcript available" has to mean available to THIS viewer: the
      // source having one says nothing about whether they may read it.
      const transcript = (index.in.get(node.id) ?? [])
        .filter((edge) => edge.relation === 'derived-from')
        .map((edge) => index.byId.get(edge.fromId))
        .find((item) => item?.kind === 'transcript' && !item.locked && item.disclosure === 'details');
      const recorded = facts.recorded
        ? `Recorded · transcript ${facts.transcriptAvailable && transcript ? 'available' : 'pending'}`
        : undefined;
      const participants = (facts.participants ?? [])
        .filter((person) => trim(person.label))
        .slice(0, 12)
        .map((person) => ({
          id: person.id,
          label: trim(person.label) as string,
          ...(person.avatarUrl && person.avatarUrl.startsWith('/') ? { avatarUrl: person.avatarUrl } : {}),
        }));
      // A room that has not happened says when it is expected and for how
      // long — "05:00 · 30 min" — which is a different claim from a span that
      // already ran, and must not be mistaken for one.
      const expectedAt = clock(facts.scheduledFor?.at, query.timezone);
      const proposed = expectedAt
        ? `${expectedAt}${facts.scheduledFor?.durationMinutes ? ` · ${facts.scheduledFor.durationMinutes} min` : ''}`
        : undefined;
      // Nothing was put in anyone's calendar. Saying so is the difference
      // between an invitation and a room somebody still has to be told about.
      const calendar = facts.calendar === 'none' && proposed ? 'No calendar invite' : undefined;
      const lines = [span, proposed, calendar, recorded].filter((value): value is string => !!value);
      setDetail(node.id, {
        ...(lines.length ? { lines: lines.slice(0, WORK_GRAPH_V2_LIMITS.detailLines) } : {}),
        ...(participants.length ? { participants } : {}),
      });
      return;
    }

    if (node.kind === 'document') {
      const documentId = stored?.sourceRef.resourceId;
      // Read, and the stored name applied, before any node was described.
      const [facts, attention] = (documentId ? documentFacts.get(documentId) : undefined) ?? [null, null];
      const run = producingRun(index, node.id);
      // What each revision is actually made of, as its own source recorded it.
      // A revision the source has no record of keeps whatever the projected
      // change node said and gains nothing.
      const storedRevisions = new Map((facts?.revisions ?? []).map((revision) => [revision.id, revision]));
      // Two records of the same history: the projected `change` nodes, which
      // are what a save event writes, and the document's own revision list,
      // which is what a generator appends directly. Either one alone misses
      // half of them, so they are merged and de-duplicated by revision id.
      const projected = documentId ? revisionsByDocument.get(documentId) ?? [] : [];
      const listedIds = new Set(projected.map((revision) => revision.id));
      // When both records hold a revision, the document's own row says when
      // it was written: the change node's time is only when a save event
      // claimed it, and a re-emitted or backdated event disagrees with the
      // row. Mixing the two clocks ordered a Sprint review "v3 v4 v1 v2" —
      // v1/v2 at their events' 09:27/09:31, v3/v4 at their 04:23 row writes —
      // so the pane called v2 "available" and "First draft" named v3's time.
      const revisions = [
        ...projected.map((revision) => {
          const written = storedRevisions.get(revision.id)?.createdAt;
          return written && Number.isFinite(Date.parse(written))
            ? { ...revision, createdAt: new Date(Date.parse(written)).toISOString() }
            : revision;
        }),
        ...[...storedRevisions.values()]
          .filter((revision) => !listedIds.has(revision.id) && !!revision.createdAt)
          .map((revision) => ({
            id: revision.id,
            label: trim(revision.label, WORK_GRAPH_LIMITS.labelLength) ?? revision.id,
            createdAt: new Date(Date.parse(revision.createdAt as string)).toISOString(),
          })),
      ]
        // Oldest first: the newest revision is the one the pane opens on, and
        // the contract reads the list in the order the source made them.
        .sort((left, right) => Date.parse(left.createdAt) - Date.parse(right.createdAt)
          || left.id.localeCompare(right.id))
        .slice(-WORK_GRAPH_V2_LIMITS.revisions);
      const runSources = run
        ? runInputs(index, run.id)
          .map((target) => link(
            target,
            target.kind === 'transcript' ? 'Meeting transcript' : trim(target.description),
          ))
          .filter((value): value is ViewerWorkNodeLink => !!value)
        : [];
      // Provenance is what the newest revision itself cited, when it cited
      // anything: a deck records the documents its slides were written from,
      // which is narrower and more truthful than everything its run read. A
      // citation the viewer has no node for is dropped, not named by its id.
      const declaredSources = [...revisions].reverse()
        .map((revision) => storedRevisions.get(revision.id)?.sources)
        .find((list) => !!list && list.length > 0);
      const sources = declaredSources
        ? declaredSources
          .map((source) => nodesByResource.get(source.documentId))
          .map((target) => (target
            ? link(target, target.kind === 'transcript' ? 'Meeting transcript' : trim(target.description))
            : undefined))
          .filter((value): value is ViewerWorkNodeLink => !!value)
        : runSources;

      const newestRevision = revisions[revisions.length - 1];
      if (!run) {
        // A document nobody's run produced is an input, not an artifact: it
        // says what it is and which revision was read, and nothing more.
        const label = [trim(facts?.name) ?? node.title, newestRevision?.label, clock(newestRevision?.createdAt, query.timezone)]
          .filter((part): part is string => !!part)
          .join(' · ');
        if (label) setDetail(node.id, { lines: [label] });
        return;
      }
      if (revisions.length === 0 && !facts) return;

      const newest = newestRevision;
      const runState = run ? runFacts.get(run.id) : undefined;
      const pendingStatus: 'generating' | 'failed' | undefined = runState?.status === 'running' ? 'generating'
        : runState?.status === 'error' ? 'failed'
          : undefined;
      // The attempt is named by the run's own attempt counter, and the pending
      // revision id is the one the source has not yet written.
      const pendingLabel = `v${revisions.length + 1}`;
      // "Rendering v3" is the run's own step state speaking, not a word chosen
      // for a running run: a generating attempt says nothing until the step
      // that draws the slides is the one executing.
      const renderingMessage = pendingStatus === 'generating' && runState && isRendering(runState, runFacts)
        ? `Rendering ${pendingLabel}`
        : undefined;
      const pending = pendingStatus && runState
        ? {
          revisionId: pendingLabel,
          attemptId: `attempt-${runState.attempt}`,
          label: pendingLabel,
          status: pendingStatus,
          startedAt: new Date(Date.parse(runState.startedAt)).toISOString(),
          updatedAt: new Date(Math.max(Date.parse(runState.updatedAt), Date.parse(runState.startedAt))).toISOString(),
          ...(pendingStatus === 'failed' && trim(runState.error) ? { message: trim(runState.error) as string } : {}),
          ...(renderingMessage ? { message: renderingMessage } : {}),
        }
        : undefined;

      const anchoredRevisions: ViewerArtifactRevision[] = revisions.map((revision, position) => {
        // Every slide of THIS revision, named by the revision's own record, so
        // the navigator moves between real slides rather than repeating the
        // newest revision's outline on every one of them. Each slide's bytes
        // are reachable as `<previewHandle>.<slideId>`.
        const slides = storedRevisions.get(revision.id)?.slides ?? [];
        const anchors = slides.length
          ? slides.map((slide) => ({ kind: 'slide' as const, id: slide.id, label: slide.title, index: slide.index }))
          : position === revisions.length - 1 ? facts?.anchors ?? [] : [];
        return {
          ...revision,
          // The handle is offered for every listed revision; the route decides
          // whether bytes exist, so a missing preview is a 404 and not a
          // differently-shaped snapshot.
          previewHandle: previewHandle(node.id, revision.id),
          ...(anchors.length ? { anchors: anchors.slice(0, WORK_GRAPH_V2_LIMITS.anchors) } : {}),
        };
      });
      // The newest revision's own slide count, which is what the pane counts
      // against; the document's `pageCount` is the fallback for a source that
      // records no per-revision slides.
      const pageCount = (newest && storedRevisions.get(newest.id)?.slideCount) || facts?.pageCount;
      // The newest revision's own heading, only where it names something the
      // document title does not ("Titled “Audit E2E”" under audit-e2e-…).
      const heading = newest
        ? distinctHeading(storedRevisions.get(newest.id)?.heading, facts?.name ?? node.title)
        : undefined;

      const firstDraft = revisions[0] && clock(revisions[0].createdAt, query.timezone);
      setDetail(node.id, {
        ...(newest && pending ? { summary: `${pending.label} ${pending.status} · ${newest.label} available` } : {}),
        // "First draft · 09:27" · "Shared": when it started, and that the
        // viewer is seeing it because it was shared with them.
        ...(firstDraft ? { footer: { label: `First draft · ${firstDraft}`, note: 'Shared' } } : {}),
        ...(sources.length ? { sources } : {}),
        artifact: {
          mediaKind: facts?.mediaKind ?? 'document',
          ...(pageCount ? { pageCount } : {}),
          ...(heading ? { heading } : {}),
          revisions: anchoredRevisions,
          ...(pending ? { pending } : {}),
        },
        ...(facts?.feedbackCount ? { feedback: { count: facts.feedbackCount, through: now } } : {}),
        ...(attention && Date.parse(attention.expiresAt) > Date.parse(now)
          && attention.actorId === query.personId
          ? {
            attention: {
              kind: 'reviewing' as const,
              observedAt: attention.observedAt,
              expiresAt: attention.expiresAt,
              ...(attention.revisionId && revisions.some((revision) => revision.id === attention.revisionId)
                ? { revisionId: attention.revisionId }
                : {}),
            },
          }
          : {}),
      });
      return;
    }

    if ((node.kind === 'project' || node.kind === 'task') && trim(node.description)) {
      setDetail(node.id, { lines: [trim(node.description) as string] });
    }
  };
  phaseStarted = Date.now();
  // Every document's own row and attention state, in one parallel wave before
  // anything is described (NFR #157: they were two reads in a row per
  // document, inside the describe wave). Their stored names are applied here
  // too: a deck's sources name the documents it drew on, and reading a title
  // while that document's own branch was renaming it answered "Project
  // documents" on one build and "Release plan" on the next.
  const documentReads = new Map<string, Promise<DocumentFacts>>();
  for (const node of ordered) {
    const documentId = node.kind === 'document' ? storedNode(authorization, node.id)?.sourceRef.resourceId : undefined;
    if (documentId && !documentReads.has(documentId)) {
      documentReads.set(documentId, Promise.all([reader.artifact(documentId), reader.attention(documentId)]));
    }
  }
  const documentFacts = new Map(await Promise.all(
    [...documentReads].map(async ([documentId, read]) => [documentId, await read] as const),
  ));
  for (const node of ordered) {
    const documentId = node.kind === 'document' ? storedNode(authorization, node.id)?.sourceRef.resourceId : undefined;
    // The document is called what its owner named it. The projected node
    // title comes from whatever event first saw it — a deck step's produced
    // event carried the spec's title — so one deck read "Audit E2E" on some
    // cards and "audit-e2e-1790150600" on others (live 2026-09-23). The
    // store's title wins wherever the store has one.
    const storedName = documentId ? trim(documentFacts.get(documentId)?.[0]?.name) : undefined;
    if (!storedName || !documentId) continue;
    if (storedName !== node.title) node.title = storedName;
    for (const change of changeNodesByDocument.get(documentId) ?? []) {
      if (change.title.endsWith(' · revision')) change.title = `${storedName} · revision`;
    }
  }
  phase('documents');
  await Promise.all(ordered.filter((node) => node.kind === 'meeting').map(describe));
  phase('meetings');
  await Promise.all(ordered.filter((node) => node.kind !== 'meeting').map(describe));
  phase('describe');

  // An operation is evidence that one process edge was observed running. Both
  // the process node and the relationship must still be inside their lease.
  for (const edge of edges) {
    const from = index.byId.get(edge.fromId);
    const to = index.byId.get(edge.toId);
    const process = [from, to].find((node) => node && PROCESS_KINDS.has(node.kind) && node.status === 'running');
    // An attempt identifier is source detail, so an operation lease requires
    // the same full disclosure a node detail does.
    if (!process || process.locked || process.disclosure !== 'details') continue;
    // Liveness is the process node's own fresh source evidence. The
    // relationship only says which one is being worked on: a `produced` edge
    // is written by the save record and is not itself marked running.
    if (edge.disclosure === 'existence') continue;
    if (!OPERATION_RELATIONS.has(edge.relation) || edge.fromId !== process.id) continue;
    if (settled(process, runStatusOf(process))) continue;
    // A step that has taken no turn has not started working in the workspace
    // it was given. Claiming an operation for it animates a process that does
    // not exist yet.
    if (runFacts.get(process.id)?.step?.iterations === 0) continue;
    const stored = storedNode(authorization, process.id);
    const evidence = lease(stored?.updatedAt ?? process.updatedAt, now);
    if (!evidence) continue;
    const facts = runFacts.get(process.id);
    operations.push({
      edgeId: edge.id,
      processNodeId: process.id,
      attemptId: `attempt-${facts?.attempt ?? 1}`,
      ...evidence,
    });
  }

  const observations = [
    ...nodes.flatMap((node) => [node.startedAt, node.updatedAt]),
    ...edges.flatMap((edge) => [edge.startedAt, edge.updatedAt]),
  ].filter((value): value is string => !!value).map((at) => ({ at }));

  const effortInput: DeriveWorkEffortsInput = {
    nodes,
    edges,
    contextBefore: query.windowStart,
    // A work stream with no artifact is named by its anchor's own context
    // line. Only the anchor may name the effort; a member never can.
    titleFor: (anchor, members) => {
      if (members.some((member) => member.kind === 'document')) return undefined;
      // A conversation is a container, not a piece of work: "general" names
      // nothing a person recognises. The run started in it does. Only a member
      // the viewer may actually read can speak, same rule as the outcome.
      if (anchor.kind === 'conversation') {
        const run = earliestRun(members);
        if (run) return run.title;
      }
      return streamOf(anchor);
    },
    subtitleFor: (effort) => {
      // The subtitle describes the artifact the effort is named after, so a
      // supporting input document cannot speak for the whole frame.
      const artifact = effort.nodeIds
        .map((id) => ({ node: index.byId.get(id), detail: details.get(id) }))
        .filter((entry) => entry.detail?.artifact && entry.node?.kind === 'document')
        .sort((left, right) => instant(right.node?.updatedAt) - instant(left.node?.updatedAt))
        .map((entry) => entry.detail)[0];
      if (artifact?.artifact) {
        const available = artifact.artifact.revisions[artifact.artifact.revisions.length - 1]?.label;
        const pending = artifact.artifact.pending;
        if (pending && available) return `${pending.label} ${pending.status} · ${available} available`;
        if (available) return `${available} available`;
      }
      const anchor = index.byId.get(effort.anchorNodeId);
      if (!anchor) return undefined;
      // "Gateway · Cloud project": where the work lives, and what kind of
      // container this anchor is.
      const kind = anchor.kind === 'project' ? 'Cloud project' : anchor.kind === 'task' ? 'Work item' : undefined;
      if (streamOf(anchor) && kind) return `${contextOf(anchor)} · ${kind}`;
      return trim(anchor.description);
    },
  };
  const efforts = deriveWorkEfforts({ ...effortInput, anchorKinds: WORK_EFFORT_ANCHORS });

  // A run whose conversation this person has never posted in — or one started
  // outside a conversation altogether — reaches no container anchor, and
  // `deriveWorkEfforts` leaves it ungrouped rather than guessing. Ungrouped is
  // honest but invisible, so a second pass anchors on the runs nothing claimed.
  // The run names its own effort, which is why it carries a real title.
  const claimed = new Set(efforts.flatMap((effort) => effort.nodeIds));
  const orphanRuns = nodes.filter((node) => (node.kind === 'run' || node.kind === 'agent') && !claimed.has(node.id));
  if (orphanRuns.length > 0) {
    const remaining = nodes.filter((node) => !claimed.has(node.id));
    const remainingIds = new Set(remaining.map((node) => node.id));
    efforts.push(...deriveWorkEfforts({
      ...effortInput,
      nodes: remaining,
      edges: edges.filter((edge) => remainingIds.has(edge.fromId) && remainingIds.has(edge.toId)),
      anchorKinds: ['run', 'agent'],
      // The anchor here is whichever run node was touched last, which for an
      // agent loop is a STEP. The effort is the whole loop, so it is named
      // after the run that started it — the earliest — not its latest step.
      titleFor: (_anchor, members) => earliestRun(members)?.title,
    }).slice(0, Math.max(0, WORK_GRAPH_V2_LIMITS.efforts - efforts.length)));
  }

  // One run is one effort. Efforts holding nodes of the same pipeline run are
  // merged into whichever of them already frames that run — the containment is
  // in the resource id the source wrote, so it holds whether or not the
  // `derived-from` relationship survived the order it was delivered in.
  const runOf = new Map(nodes.flatMap((node) => {
    const runId = pipelineRunOf(authorization, node);
    return runId ? [[node.id, runId] as const] : [];
  }));
  const homeByRun = new Map<string, ViewerWorkEffort>();
  const merged: ViewerWorkEffort[] = [];
  for (const effort of efforts) {
    const runIds = [...new Set(effort.nodeIds.flatMap((id) => (runOf.get(id) ? [runOf.get(id) as string] : [])))];
    // Only an effort that is ENTIRELY one run's work may be absorbed; an
    // effort anchored on a conversation or a project keeps its own frame and
    // takes the run's nodes with it.
    const home = runIds.length === 1 ? homeByRun.get(runIds[0]) : undefined;
    if (home && effort.anchorNodeId !== home.anchorNodeId) {
      home.nodeIds = [...new Set([...home.nodeIds, ...effort.nodeIds])];
      home.edgeIds = [...new Set([...home.edgeIds, ...effort.edgeIds])];
      home.contextNodeIds = [...new Set([...home.contextNodeIds, ...effort.contextNodeIds])];
      continue;
    }
    merged.push(effort);
    for (const runId of runIds) {
      // The effort that frames the RUN's own node is the home, whichever came
      // first: it is the one whose anchor can name the whole loop.
      const holdsTheRun = effort.nodeIds.some((id) => storedNode(authorization, id)?.sourceRef.resourceId === runId);
      if (!homeByRun.has(runId) || holdsTheRun) homeByRun.set(runId, effort);
    }
  }
  efforts.length = 0;
  efforts.push(...merged);
  // An anchor that something else PRODUCED is not its own piece of work: a
  // room a run booked belongs to that run's frame, and only anchors an effort
  // of its own when nothing produced it. The relationship is the source's, so
  // this never merges two efforts that merely look related.
  for (let pass = 0; pass < 3; pass += 1) {
    const memberOf = new Map<string, ViewerWorkEffort>();
    for (const effort of efforts) for (const id of effort.nodeIds) memberOf.set(id, effort);
    const folded = efforts.filter((effort) => {
      const producers = edges
        .filter((edge) => edge.relation === 'produced' && edge.toId === effort.anchorNodeId)
        // A step produced it, and the step belongs to a run. The frame is the
        // RUN's, so folding into the producing step's own effort would put the
        // room in a second frame beside the loop that booked it.
        .map((edge) => homeByRun.get(runOf.get(edge.fromId) ?? '') ?? memberOf.get(edge.fromId))
        .filter((home): home is ViewerWorkEffort => !!home && home.id !== effort.id);
      // Produced by exactly one effort, or it stays where it is.
      const home = new Set(producers).size === 1 ? producers[0] : undefined;
      if (!home) return true;
      home.nodeIds = [...new Set([...home.nodeIds, ...effort.nodeIds])];
      home.edgeIds = [...new Set([...home.edgeIds, ...effort.edgeIds])];
      home.contextNodeIds = [...new Set([...home.contextNodeIds, ...effort.contextNodeIds])];
      return false;
    });
    if (folded.length === efforts.length) break;
    efforts.length = 0;
    efforts.push(...folded);
  }

  // Anything the source says belongs to a framed run, but that reached no
  // effort of its own — a step whose every relationship was dropped.
  const framed = new Set(efforts.flatMap((effort) => effort.nodeIds));
  for (const [nodeId, runId] of runOf) {
    const home = homeByRun.get(runId);
    if (!home || framed.has(nodeId)) continue;
    home.nodeIds = [...new Set([...home.nodeIds, nodeId])];
  }
  // Whatever a framed member is related to belongs in the same frame: the
  // workspace a step runs in and the artifact it produced are a `terminal` and
  // a `document`, neither of which may anchor an effort, so alone they were
  // framed by nothing and the pane could not show a loop's deck under it. The
  // relationship was declared by a source, and a node reached from exactly one
  // effort is unambiguous; one reached from several is left where it is.
  for (let hop = 0; hop < 3; hop += 1) {
    const memberOf = new Map<string, ViewerWorkEffort | null>();
    for (const effort of efforts) for (const id of effort.nodeIds) memberOf.set(id, effort);
    let absorbed = false;
    for (const node of nodes) {
      if (memberOf.has(node.id)) continue;
      const homes = new Set(edges
        .filter((edge) => edge.fromId === node.id || edge.toId === node.id)
        .map((edge) => memberOf.get(edge.fromId === node.id ? edge.toId : edge.fromId))
        .filter((effort): effort is ViewerWorkEffort => !!effort));
      if (homes.size !== 1) continue;
      const [home] = homes;
      home.nodeIds = [...home.nodeIds, node.id];
      absorbed = true;
    }
    if (!absorbed) break;
  }

  // Named LAST, from everything the effort ended up framing. The RUN names it
  // whenever the frame holds one — a step is a part of the work and a room or
  // a deck is a thing the work made; neither names a frame that has a run. A
  // run is the pipeline resource with no step of its own in its id, so this
  // holds however the members were gathered and whatever the anchor is.
  for (const effort of efforts) {
    const members = effort.nodeIds
      .map((id) => index.byId.get(id))
      .filter((node): node is ViewerWorkNode => !!node);
    const run = members.find((node) => {
      if (node.kind !== 'run' && node.kind !== 'agent') return false;
      const stored = storedNode(authorization, node.id);
      return stored?.sourceRef.source === 'pipeline'
        && !parsePipelineStepResourceId(stored.sourceRef.resourceId).stepId;
    });
    const anchor = index.byId.get(effort.anchorNodeId);
    const named = run
      ?? ((anchor?.kind === 'run' || anchor?.kind === 'agent') ? earliestRun(members) : undefined);
    if (named?.title) effort.title = named.title;
  }

  // Every relationship BETWEEN an effort's own members belongs to it, however
  // the members got there.
  for (const effort of efforts) {
    const members = new Set(effort.nodeIds);
    effort.edgeIds = [...new Set([
      ...effort.edgeIds,
      ...edges.filter((edge) => members.has(edge.fromId) && members.has(edge.toId)).map((edge) => edge.id),
    ])];
  }

  // Latest-state projection cannot reconstruct a past minute, so an as-of
  // request is answered honestly as recent work rather than as replay.
  const observedAt = new Date(Math.max(Date.parse(now), Date.parse(query.windowEnd))).toISOString();
  const earliest = observations
    .map((observation) => Date.parse(observation.at))
    .filter((value) => Number.isFinite(value))
    .sort((left, right) => left - right)[0];

  return {
    temporal: {
      mode: query.mode === 'live' ? 'live' : 'recent',
      observedAt,
      coverage: {
        from: new Date(Math.min(earliest ?? Date.parse(query.windowStart), Date.parse(query.windowStart))).toISOString(),
        through: observedAt,
        complete: false,
      },
    },
    efforts,
    // In node order, not in the order concurrent reads happened to finish:
    // two builds of the same graph answer the same bytes (a view hash, NFR #85).
    details: detailsForDisclosedNodes(nodes, [...details.values()].sort((a, b) => (
      (position.get(a.nodeId) ?? Number.MAX_SAFE_INTEGER) - (position.get(b.nodeId) ?? Number.MAX_SAFE_INTEGER)
      || a.nodeId.localeCompare(b.nodeId)
    ))),
    operations: freshWorkOperations(operations, now),
    eventBuckets: bucketWorkEvents(query, observations, { observedAt }),
  };
}
