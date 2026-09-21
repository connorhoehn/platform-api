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
  status: 'running' | 'waiting' | 'completed' | 'error' | 'stopped';
  attempt: number;
  startedAt: string;
  updatedAt: string;
  /** Step/tool identities this run actually executed. */
  tools: string[];
  /** Sanitized failure summary. Never a stack trace, path, or credential. */
  error?: string;
}

export interface ArtifactFacts {
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
}

export interface MeetingFacts {
  startedAt?: string;
  endedAt?: string;
  recorded: boolean;
  transcriptAvailable: boolean;
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

function revisionLabel(revisionId: string, index: number): string {
  return /^v\d+$/i.test(revisionId) ? revisionId.toLowerCase() : `rev ${index + 1}`;
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

  await Promise.all(detailed.map(async (node) => {
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
      setDetail(node.id, {
        ...(summary ? { summary } : {}),
        ...(facts?.tools.length ? { tools: facts.tools.slice(0, 16) } : {}),
        ...(inputs.length ? { inputs } : {}),
        ...(freshness ? { freshness } : {}),
      });
      return;
    }

    if (node.kind === 'terminal') {
      const workItem = (index.out.get(node.id) ?? [])
        .filter((edge) => edge.relation === 'works-on')
        .map((edge) => index.byId.get(edge.toId))
        .filter((target): target is ViewerWorkNode => !!target)
        .map((target) => link(target, trim(target.description)))
        .filter((value): value is ViewerWorkNodeLink => !!value)[0];
      const started = clock(node.startedAt, query.timezone);
      setDetail(node.id, {
        ...(trim(node.description) ? { summary: trim(node.description) as string } : {}),
        ...(started ? { lines: [`Session started ${started}`] } : {}),
        ...(workItem ? { workItem } : {}),
        ...(freshness ? { freshness } : {}),
      });
      return;
    }

    if (node.kind === 'transcript') {
      const facts = stored ? await reader.transcript(stored.sourceRef.resourceId) : null;
      if (!facts) return;
      const lines = [`${facts.speakerCount} speakers · ${facts.actionItemCount} action item${facts.actionItemCount === 1 ? '' : 's'}`];
      setDetail(node.id, {
        lines,
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
      const lines = [
        from && through ? `${from}–${through}${minutes ? ` · ${minutes} min` : ''}` : undefined,
        facts.recorded ? `Recorded · transcript ${facts.transcriptAvailable ? 'available' : 'pending'}` : undefined,
      ].filter((value): value is string => !!value);
      if (lines.length) setDetail(node.id, { lines });
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

      if (revisions.length === 0 && !facts) {
        if (trim(node.description)) setDetail(node.id, { summary: trim(node.description) as string });
        return;
      }

      const newest = revisions[revisions.length - 1];
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
        ...(position === revisions.length - 1 && facts?.anchors?.length
          ? { anchors: facts.anchors.slice(0, 200) }
          : {}),
      }));

      setDetail(node.id, {
        ...(newest ? { summary: `First draft · ${clock(revisions[0].createdAt, query.timezone) ?? revisions[0].label}` } : {}),
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
      setDetail(node.id, { summary: trim(node.description) as string });
    }
  }));

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
      return trim(anchor?.description);
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
