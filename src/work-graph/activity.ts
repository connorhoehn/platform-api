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
  type ViewerWorkActivityDetail,
  type ViewerWorkNodeLink,
  type ViewerWorkOperation,
  type WorkGraphActivityV2,
  type WorkGraphQueryV2,
} from '@connorhoehn/realtime-modules/work-graph';
import type { StoredWorkNode } from '../repositories/WorkGraphRepository';
import type { AuthorizedWorkGraph, ViewerWorkEdge, ViewerWorkNode } from './policy';

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
}

/** An authorized handle for one revision's preview bytes. */
export const PREVIEW_HANDLE_SEPARATOR = '.';

/**
 * Names a revision preview with values the viewer already holds: the opaque
 * node id this snapshot gave them and the revision id it already listed. It
 * carries no source identifier and grants nothing on its own — the route it
 * is handed to re-authorizes the node before serving a byte.
 */
export function previewHandle(nodeId: string, revisionId: string): string {
  return `${nodeId}${PREVIEW_HANDLE_SEPARATOR}${revisionId}`;
}

export function parsePreviewHandle(handle: string): { nodeId: string; revisionId: string } | null {
  const separator = handle.lastIndexOf(PREVIEW_HANDLE_SEPARATOR);
  if (separator <= 0 || separator === handle.length - 1) return null;
  return { nodeId: handle.slice(0, separator), revisionId: handle.slice(separator + 1) };
}

export interface RevisionPreview {
  contentType: 'image/svg+xml' | 'image/png' | 'image/jpeg' | 'image/webp';
  body: Buffer;
}

export interface ArtifactFacts {
  /** The document's own title in its store, which the graph node may abbreviate. */
  name?: string;
  mediaKind: 'presentation' | 'document' | 'image';
  pageCount?: number;
  /** Addressable units of the newest available revision. */
  anchors?: Array<{ kind: 'slide' | 'page' | 'block'; id: string; label: string; index?: number }>;
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
  artifact(documentId: string): Promise<ArtifactFacts | null>;
  /** The stored bytes for one revision, or null when the source has none. */
  revisionPreview(documentId: string, revisionId: string): Promise<RevisionPreview | null>;
  transcript(transcriptId: string): Promise<TranscriptFacts | null>;
  meeting(meetingId: string): Promise<MeetingFacts | null>;
  /** Authoritative human attention, with its own expiry. Never a socket. */
  attention(documentId: string): Promise<DocumentAttentionFacts | null>;
}

export interface BuildWorkActivityInput {
  query: WorkGraphQueryV2;
  nodes: ViewerWorkNode[];
  edges: ViewerWorkEdge[];
  authorization: AuthorizedWorkGraph;
  reader: WorkActivityReader;
  now: string;
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

function instant(value: string | undefined): number {
  const parsed = value === undefined ? Number.NaN : Date.parse(value);
  return Number.isFinite(parsed) ? parsed : 0;
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
  for (const edge of index.in.get(documentId) ?? []) {
    const from = index.byId.get(edge.fromId);
    if (edge.relation === 'produced' && from && (from.kind === 'run' || from.kind === 'agent')) return from;
  }
  return undefined;
}

/** The evidence a run declared it consumed. Only `derived-from` counts. */
function runInputs(index: GraphIndex, runId: string): ViewerWorkNode[] {
  return (index.out.get(runId) ?? [])
    .filter((edge) => edge.relation === 'derived-from')
    .map((edge) => index.byId.get(edge.toId))
    .filter((node): node is ViewerWorkNode => !!node);
}

export async function buildWorkActivity(input: BuildWorkActivityInput): Promise<WorkGraphActivityV2> {
  const { query, nodes, edges, authorization, reader, now } = input;
  const index = indexGraph(nodes, edges);
  const details = new Map<string, ViewerWorkActivityDetail>();
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
  for (const node of detailed) {
    if (node.kind !== 'change') continue;
    const stored = storedNode(authorization, node.id);
    const resourceId = stored?.sourceRef.resourceId ?? '';
    const separator = resourceId.lastIndexOf(':');
    if (stored?.sourceRef.source !== 'document' || separator <= 0) continue;
    const documentId = resourceId.slice(0, separator);
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

  const runFacts = new Map<string, PipelineRunFacts>();
  await Promise.all(detailed
    .filter((node) => node.kind === 'run' || node.kind === 'agent')
    .map(async (node) => {
      const stored = storedNode(authorization, node.id);
      if (!stored) return;
      const facts = await reader.pipelineRun(stored.sourceRef.resourceId);
      if (facts) runFacts.set(node.id, facts);
    }));

  // Meetings first: a transcript's first line names its meeting's span, and
  // resolving both in one parallel pass left that line to a race.
  const ordered = [
    ...detailed.filter((node) => node.kind === 'meeting'),
    ...detailed.filter((node) => node.kind !== 'meeting'),
  ];
  const describe = async (node: ViewerWorkNode): Promise<void> => {
    const stored = storedNode(authorization, node.id);
    const freshness = lease(stored?.updatedAt ?? node.updatedAt, now);

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
      // node status can lag the source by one lifecycle event.
      const failed = facts?.status === 'error' || (!facts && node.status === 'error');
      const summary = failed
        ? trim(facts?.error ?? 'The run failed before it produced a revision.')
        : produced ? trim(`Creating the next revision of ${produced.title}`) : undefined;
      const tools = facts?.tools.slice(0, 16) ?? [];
      // "Presentation agent · 3 tools": the definition's own name and the
      // number of steps this run actually recorded.
      const agentLine = facts?.agentLabel && tools.length
        ? `${facts.agentLabel} · ${tools.length} tool${tools.length === 1 ? '' : 's'}`
        : trim(facts?.agentLabel);
      setDetail(node.id, {
        ...(summary ? { summary } : {}),
        ...(agentLine ? { lines: [agentLine] } : {}),
        ...(tools.length ? { tools } : {}),
        ...(inputs.length ? { inputs } : {}),
        ...(freshness ? { freshness } : {}),
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
      const participants = (facts.participants ?? [])
        .filter((person) => trim(person.label))
        .slice(0, 12)
        .map((person) => ({
          id: person.id,
          label: trim(person.label) as string,
          ...(person.avatarUrl && person.avatarUrl.startsWith('/') ? { avatarUrl: person.avatarUrl } : {}),
        }));
      setDetail(node.id, {
        ...(span ? { lines: [span] } : {}),
        ...(participants.length ? { participants } : {}),
      });
      return;
    }

    if (node.kind === 'document') {
      const documentId = stored?.sourceRef.resourceId;
      const facts = documentId ? await reader.artifact(documentId) : null;
      const revisions = documentId ? revisionsByDocument.get(documentId) ?? [] : [];
      const run = producingRun(index, node.id);
      const attention = documentId ? await reader.attention(documentId) : null;
      const sources = run
        ? runInputs(index, run.id)
          .map((target) => link(
            target,
            target.kind === 'transcript' ? 'Meeting transcript' : trim(target.description),
          ))
          .filter((value): value is ViewerWorkNodeLink => !!value)
        : [];

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
      const pending = pendingStatus && runState
        ? {
          revisionId: `v${revisions.length + 1}`,
          attemptId: `attempt-${runState.attempt}`,
          label: `v${revisions.length + 1}`,
          status: pendingStatus,
          startedAt: new Date(Date.parse(runState.startedAt)).toISOString(),
          updatedAt: new Date(Math.max(Date.parse(runState.updatedAt), Date.parse(runState.startedAt))).toISOString(),
          ...(pendingStatus === 'failed' && trim(runState.error) ? { message: trim(runState.error) as string } : {}),
        }
        : undefined;

      const anchoredRevisions = revisions.map((revision, position) => ({
        ...revision,
        // The handle is offered for every listed revision; the route decides
        // whether bytes exist, so a missing preview is a 404 and not a
        // differently-shaped snapshot.
        previewHandle: previewHandle(node.id, revision.id),
        ...(position === revisions.length - 1 && facts?.anchors?.length
          ? { anchors: facts.anchors.slice(0, 200) }
          : {}),
      }));

      const firstDraft = revisions[0] && clock(revisions[0].createdAt, query.timezone);
      setDetail(node.id, {
        ...(newest && pending ? { summary: `${pending.label} ${pending.status} · ${newest.label} available` } : {}),
        // "First draft · 09:27" · "Shared": when it started, and that the
        // viewer is seeing it because it was shared with them.
        ...(firstDraft ? { footer: { label: `First draft · ${firstDraft}`, note: 'Shared' } } : {}),
        ...(sources.length ? { sources } : {}),
        artifact: {
          mediaKind: facts?.mediaKind ?? 'document',
          ...(facts?.pageCount ? { pageCount: facts.pageCount } : {}),
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
  await Promise.all(ordered.filter((node) => node.kind === 'meeting').map(describe));
  await Promise.all(ordered.filter((node) => node.kind !== 'meeting').map(describe));

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

  const efforts = deriveWorkEfforts({
    nodes,
    edges,
    contextBefore: query.windowStart,
    // A work stream with no artifact is named by its anchor's own context
    // line. Only the anchor may name the effort; a member never can.
    titleFor: (anchor, members) => (members.some((member) => member.kind === 'document')
      ? undefined
      : streamOf(anchor)),
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
  });

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
    details: detailsForDisclosedNodes(nodes, [...details.values()]),
    operations: freshWorkOperations(operations, now),
    eventBuckets: bucketWorkEvents(query, observations, { observedAt }),
  };
}
