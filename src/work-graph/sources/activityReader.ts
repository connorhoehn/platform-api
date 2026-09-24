/**
 * Reads the v2 activity facts from each source's own authoritative record.
 *
 * It performs no authorization of its own: the orchestrator has already
 * decided which nodes this viewer may see at `details` disclosure, and only
 * those node identities reach these lookups. Every method returns `null` when
 * the source has nothing to say, so an absent record discloses nothing rather
 * than producing a plausible-looking placeholder.
 */
import { memoOnce } from '../buildMemo';
import { getSystemPipeline } from '../../pipeline/systemPipelines';
import { GetCommand, QueryCommand, ScanCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { tableName } from '../../lib/ddb-table-name';
import log from '../../lib/logger';
import {
  getPresentationDocumentStore,
  resolveRevisionSlides,
  withRevisionSlides,
  type PresentationDocumentRow,
  type PresentationDocumentStore,
  type StoredSlide,
} from '../../deck/presentationDocuments';
import { SVG_CONTENT_TYPE } from '../../deck/renderSlideSvg';
import { mergeRevisions, queryAllRevisionItems } from '../../deck/presentationRevisionItems';
import type { AssetBlobStore } from '../../pipeline/image/ImageBlobStore';
import { getAssetBlobStore } from '../../pipeline/image/visionServices';
import type {
  DocumentEditFacts,
  ArtifactFacts,
  ArtifactRevisionFacts,
  RevisionPreview,
  DocumentAttentionFacts,
  MeetingFacts,
  PipelineRunFacts,
  TranscriptFacts,
  WorkActivityReader,
} from '../activity';
import { parsePipelineStepResourceId } from './mapPipeline';
import { runRowLocator } from './runRowLocator';
import { recordingLocator } from './recordingLocator';

const PIPELINE_RUNS_TABLE = tableName('pipeline-runs');
const RECORDINGS_TABLE = tableName('video-call-recordings');
const DOCUMENTS_TABLE = tableName(process.env.DYNAMODB_DOCUMENTS_TABLE || 'crdt-documents');
const DOCUMENT_COMMENTS_TABLE = tableName('document-comments');
const DOCUMENT_LIFECYCLE_TABLE = tableName('document-lifecycle');
const PIPELINE_DEFINITIONS_TABLE = tableName('pipeline-definitions');
const PROFILES_TABLE = tableName('social-profiles');
const HANGOUT_ROOMS_TABLE = tableName('hangout-rooms');
const HANGOUT_ROOM_MEMBERS_TABLE = tableName('hangout-room-members');

/** A lifecycle transition is evidence of attention for this long, no longer. */
const ATTENTION_LEASE_MS = 10 * 60 * 1_000;
const MAX_TRANSCRIPT_SEGMENTS = 500;
const MAX_PREVIEW_BYTES = 2 * 1024 * 1024;
const ALLOWED_PREVIEW_TYPES = new Set(['image/svg+xml', 'image/png', 'image/jpeg', 'image/webp']);

/** Markup with no script, no event handler and no outbound reference. */
function isInertSvg(markup: string): boolean {
  const lowered = markup.toLowerCase();
  if (lowered.includes('<script') || lowered.includes('<foreignobject') || lowered.includes('<use')) return false;
  if (/\son[a-z]+\s*=/.test(lowered)) return false;
  if (/(href|src)\s*=\s*["']?\s*(?!#)/.test(lowered)) return false;
  return lowered.includes('<svg');
}
const ERROR_SUMMARY_LENGTH = 160;
const MAX_REVISIONS = 100;
const MAX_SLIDES_PER_REVISION = 200;
const MAX_SOURCES_PER_REVISION = 16;

/**
 * How the presentation store and the blob store are reached. Both are process
 * singletons built on first use, so they are resolved at call time rather than
 * at construction: a deployment with no blob store still serves everything
 * else the reader can answer.
 */
export interface WorkActivityReaderDependencies {
  presentations?: () => PresentationDocumentStore;
  blobs?: () => AssetBlobStore;
}

type Row = Record<string, unknown>;

function text(value: unknown, limit: number): string | undefined {
  if (typeof value !== 'string') return undefined;
  const trimmed = value.trim().replace(/\s+/g, ' ');
  if (!trimmed) return undefined;
  return trimmed.length <= limit ? trimmed : `${trimmed.slice(0, limit - 1)}…`;
}

/**
 * A stored failure as a reader should see it. Older runs stored the agent
 * runner's message with the run id in front ("a10af720-…: claude exited 1:
 * …"); the id means nothing on a pane that already names the run, so it is
 * stripped here, at read time — the stored row is left as it was.
 */
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const LEADING_RUN_ID = /^(?:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\s*:\s*)+/i;
export function readableError(value: unknown): string | undefined {
  if (typeof value !== 'string') return undefined;
  return text(value.trim().replace(LEADING_RUN_ID, ''), ERROR_SUMMARY_LENGTH);
}

function iso(value: unknown): string | undefined {
  if (typeof value !== 'string') return undefined;
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? new Date(parsed).toISOString() : undefined;
}

/**
 * NFR #126: why a paused run waits, from the row's own `pause`, and only when
 * it agrees with the row's status — a row that resumed or finished says none.
 */
function runPause(row: Row): PipelineRunFacts['pause'] {
  const status = String(row.status);
  if (status !== 'paused_at_breakpoint' && status !== 'awaiting_approval') return undefined;
  const pause = row.pause && typeof row.pause === 'object' ? row.pause as Row : undefined;
  if (!pause || pause.reason !== status) return { reason: status };
  const step = text(pause.stepId, 60);
  return { reason: status, ...(step ? { step } : {}) };
}

/** Folds the platform's extra lifecycle states onto the five we can describe. */
function runStatus(value: unknown): PipelineRunFacts['status'] {
  switch (String(value)) {
    case 'running':
    case 'resuming':
      return 'running';
    // NFR #126: a run stopped at a breakpoint or an approval gate is waiting
    // on a person, which is neither running nor stopped.
    case 'waiting':
    case 'paused':
    case 'pending':
    case 'paused_at_breakpoint':
    case 'awaiting_approval':
      return 'waiting';
    // Spending the budget is how a bounded run finishes, not how it fails.
    case 'completed':
    case 'succeeded':
    case 'budget':
      return 'completed';
    case 'failed':
    case 'rejected':
    case 'interrupted':
    case 'stuck':
      return 'error';
    default:
      return 'stopped';
  }
}

async function scanOne(client: DynamoDBDocumentClient, table: string, expression: string, values: Row): Promise<Row | undefined> {
  // Paged: `Limit` counts rows READ before the filter, and a page also stops
  // at 1 MB, so a single call found a row only if it happened to sit in the
  // table's first page — a run past it had no pane facts at all.
  const rows: Row[] = [];
  let startKey: Record<string, unknown> | undefined;
  let pages = 0;
  do {
    const result = await client.send(new ScanCommand({
      TableName: table,
      FilterExpression: expression,
      ExpressionAttributeValues: values,
      ...(startKey ? { ExclusiveStartKey: startKey } : {}),
    }));
    rows.push(...((result.Items ?? []) as Row[]));
    startKey = result.LastEvaluatedKey as Record<string, unknown> | undefined;
    pages += 1;
  } while (startKey && rows.length < 2 && pages < 50);
  // More than one match is ambiguous provenance, so nothing is disclosed.
  return rows.length === 1 ? rows[0] : undefined;
}

/**
 * A composer edit's facts, from the run's own step outputs. Recognised by
 * shape, not by pipeline id: a trigger that names a document and carries an
 * instruction, as `deck-revise` (and any later document-edit pipeline) does.
 * Nothing is taken from the run's live context or guessed from a neighbour.
 */
export function documentEditFacts(steps: Row): DocumentEditFacts | undefined {
  const output = (stepId: string): Row => {
    const summary = steps[stepId];
    const out = summary && typeof summary === 'object' ? (summary as Row).output : undefined;
    return out && typeof out === 'object' && !Array.isArray(out) ? out as Row : {};
  };
  const trigger = output('trigger');
  const instruction = text(trigger.instruction, 160);
  const documentId = text(trigger.documentId, 128);
  if (!instruction || !documentId) return undefined;
  const load = output('load');
  const propose = output('propose');
  const check = output('check');
  const write = output('write');
  // The slide's own title from the deck the edit was made against; its id
  // ("transcript") is an internal key a reader cannot place.
  const slideId = text(trigger.slideId, 64);
  const spec = load.spec && typeof load.spec === 'object' ? load.spec as Row : undefined;
  const slides = Array.isArray(spec?.slides) ? spec!.slides as Row[] : [];
  const slideIndex = slideId ? slides.findIndex((slide) => slide?.id === slideId) : -1;
  const slideTitle = slideIndex >= 0 ? text(slides[slideIndex]!.title, 60) : undefined;
  const target = trigger.target && typeof trigger.target === 'object' ? trigger.target as Row : undefined;
  const field = text(target?.field, 24);
  const index = typeof target?.index === 'number' && Number.isInteger(target.index) ? target.index : undefined;
  const slideLabel = slideId
    ? `Slide${slideIndex >= 0 ? ` ${slideIndex + 1}` : ''} “${slideTitle ?? slideId}”`
    : undefined;
  const fieldLabel = field ? `${field}${index !== undefined ? ` ${index + 1}` : ''}` : undefined;
  const scope = [slideLabel ?? (fieldLabel ? undefined : 'Whole deck'), fieldLabel].filter(Boolean).join(' · ') || 'Whole deck';
  const changed = Array.isArray(write.changedSlideIds) ? write.changedSlideIds
    : Array.isArray(check.changedSlideIds) ? check.changedSlideIds
      : Array.isArray(propose.changedSlideIds) ? propose.changedSlideIds : undefined;
  // The check's summary carries the measured facts; the model's own claim
  // (propose) stands only until the check has spoken.
  const summary = text(write.summary ?? check.summary ?? propose.summary, 220);
  const writeDone = (steps.write as Row | undefined)?.status === 'completed';
  const revisionId = write.written === true ? text(write.revisionId, 64) : undefined;
  const specFrom = load.specFrom === 'request' || load.specFrom === 'head' ? load.specFrom : undefined;
  return {
    instruction,
    scope,
    documentId,
    ...(text(load.title, 80) ? { title: text(load.title, 80) as string } : {}),
    ...(text(load.baseRevisionId ?? trigger.baseRevisionId, 64) ? { baseRevisionId: text(load.baseRevisionId ?? trigger.baseRevisionId, 64) as string } : {}),
    ...(specFrom ? { specFrom } : {}),
    ...(summary ? { summary } : {}),
    ...(changed ? { changedSlides: changed.length } : {}),
    ...(revisionId ? { revisionId } : {}),
    ...(revisionId && text(write.rebasedOnto, 64) ? { rebasedOnto: text(write.rebasedOnto, 64) as string } : {}),
    ...(writeDone && write.written === false ? { unchanged: true } : {}),
  };
}

/**
 * Node id → the label a person gave it, from the run's definition
 * (`definition.nodes[].data.label`) and, for an /agent loop's own row, from the
 * plan it checkpointed (`checkpoint.context.plan.steps[].title`, filed as
 * `agent:<id>`, `deck:<id>`, … — matched on the plan step id after the colon).
 */
/** A step's place in the plan's declared order (`stepLabelsOf` keeps it); unknown steps sort last. */
function planOrder(labels: Map<string, string>, stepId: string): number {
  const keys = [...labels.keys()];
  const index = keys.indexOf(stepId) >= 0 ? keys.indexOf(stepId) : keys.indexOf(stepId.replace(/:\d+$/, ''));
  return index >= 0 ? index : Number.MAX_SAFE_INTEGER;
}

/** The definition's trigger node ids (`data.type === 'trigger'`), plus the conventional `trigger`. */
export function triggerStepIds(definition: unknown): Set<string> {
  const ids = new Set<string>(['trigger']);
  const nodes = (definition as { nodes?: unknown } | undefined)?.nodes;
  if (Array.isArray(nodes)) {
    for (const node of nodes as Row[]) {
      if (typeof node?.id === 'string' && ((node.data as Row | undefined)?.type === 'trigger' || node.type === 'trigger')) ids.add(node.id);
    }
  }
  return ids;
}

export function stepLabelsOf(definition: unknown, row: Row): Map<string, string> {
  const labels = new Map<string, string>();
  const nodes = (definition as { nodes?: unknown } | undefined)?.nodes;
  if (Array.isArray(nodes)) {
    for (const node of nodes as Row[]) {
      const label = typeof (node?.data as Row | undefined)?.label === 'string' ? (node.data as Row).label as string : undefined;
      if (typeof node?.id === 'string' && label?.trim()) labels.set(node.id, label.trim());
    }
  }
  const plan = ((row.checkpoint as Row | undefined)?.context as Row | undefined)?.plan as Row | undefined;
  const planSteps = Array.isArray(plan?.steps) ? (plan!.steps as Row[]) : [];
  const titles = new Map<string, string>();
  for (const step of planSteps) {
    if (typeof step?.id === 'string' && typeof step.title === 'string' && step.title.trim()) titles.set(step.id, step.title.trim());
  }
  if (titles.size > 0) {
    const steps = row.stepsSummary && typeof row.stepsSummary === 'object' ? Object.keys(row.stepsSummary as Row) : [];
    // Walk the plan, not the summary, so the map's order is the plan's.
    for (const [planId, title] of titles) {
      for (const key of steps) {
        if (!labels.has(key) && key.split(':')[1] === planId) labels.set(key, title);
      }
    }
  }
  return labels;
}

export class DynamoWorkActivityReader implements WorkActivityReader {
  constructor(
    private readonly client: DynamoDBDocumentClient,
    private readonly deps: WorkActivityReaderDependencies = {},
  ) {}

  async pipelineRun(resource: string): Promise<PipelineRunFacts | null> {
    return (await this.pipelineRuns([resource])).get(resource) ?? null;
  }

  /**
   * Every run a graph names, in one lookup instead of one per node.
   *
   * `pipeline-runs` is keyed `(pipelineId, runId)`, so a run named by its id
   * alone is a filtered Scan the first time and a keyed read after that
   * (`runRowLocator`). A day with fifty run and step nodes asked fifty times,
   * ten of them for the SAME run because each step is its own node; the rows
   * are read once. Two rows claiming one id is ambiguous and discloses nothing.
   */
  async pipelineRuns(resources: string[]): Promise<Map<string, PipelineRunFacts>> {
    const facts = new Map<string, PipelineRunFacts>();
    const distinct = [...new Set(resources)];
    if (distinct.length === 0) return facts;
    const runIds = [...new Set(distinct.map((resource) => parsePipelineStepResourceId(resource).runId))];
    const rows = await runRowLocator.rows(this.client, PIPELINE_RUNS_TABLE, runIds);
    await Promise.all(distinct.map(async (resource) => {
      const row = rows.get(parsePipelineStepResourceId(resource).runId);
      const derived = row ? await this.factsFromRun(resource, row) : null;
      if (derived) facts.set(resource, derived);
    }));
    return facts;
  }

  /** What one run node — or one step of it — says about itself, from its row. */
  private async factsFromRun(resource: string, row: Row): Promise<PipelineRunFacts | null> {
    const { runId: id, stepId } = parsePipelineStepResourceId(resource);
    if (stepId) return this.pipelineStep(id, stepId, row);
    const steps = row.stepsSummary && typeof row.stepsSummary === 'object' && !Array.isArray(row.stepsSummary)
      ? (row.stepsSummary as Row)
      : {};
    // The definition's own name, so the card says "Presentation agent" rather
    // than an internal pipeline id.
    // The definitions table is keyed (userId, pipelineId): the owner is the
    // person who triggered this run, which is the same person the graph is of.
    // Once per (owner, pipeline) per graph build — a day of fifty deck-revise
    // runs asked for the same definition fifty times (NFR #16).
    const definitionKey = { userId: String(row.triggeredBy ?? ''), pipelineId: String(row.pipelineId ?? '') };
    const definition = await memoOnce(
      `pipeline-definition\u0000${definitionKey.userId}\u0000${definitionKey.pipelineId}`,
      () => this.client.send(new GetCommand({ TableName: PIPELINE_DEFINITIONS_TABLE, Key: definitionKey })).catch(() => undefined),
    );
    // A step's name is the label its plan gave it — the definition's node
    // label, or for an /agent loop's own row the plan step's title — never the
    // node id the engine files it under (`agent:s1` meant nothing to a reader).
    // A system pipeline (deck-revise, document edits, …) has no stored row;
    // its definition is the registry's.
    const plan = (definition?.Item as Row | undefined)?.definition ?? getSystemPipeline(String(row.pipelineId ?? ''));
    const stepLabels = stepLabelsOf(plan, row);
    // The trigger is how the run started, not a tool it used ("Composer edit"
    // was listed first under "Tools in this run" and counted in "· 5 tools").
    const triggers = triggerStepIds(plan);
    // The tools are the steps this run actually recorded, in execution order.
    const tools = Object.entries(steps)
      .filter(([stepId]) => !triggers.has(stepId))
      .map(([stepId, summary]) => ({
        stepId,
        // A generated pipeline's steps are keyed by uuid; its title, when the
        // step recorded one, is the name a reader can use.
        name: text((summary as Row | undefined)?.title ?? (summary as Row | undefined)?.name, 60)
          ?? text(stepLabels.get(stepId) ?? stepLabels.get(stepId.replace(/:\d+$/, '')), 60),
        startedAt: Date.parse(iso((summary as Row | undefined)?.startedAt) ?? '') || 0,
      }))
      // Execution order, and where two steps carry the same start (or none),
      // the order the plan declares them — never the ids' alphabetical order:
      // a deck-revise run's uuid-keyed steps listed "Check the edit" before
      // "Read the deck".
      .sort((left, right) => left.startedAt - right.startedAt
        || planOrder(stepLabels, left.stepId) - planOrder(stepLabels, right.stepId)
        || left.stepId.localeCompare(right.stepId))
      // A bare uuid is not a tool name — live, a failed run listed
      // "a10af720-…" under "Tools in this run".
      .map((step) => step.name ?? (UUID.test(step.stepId) ? undefined : text(step.stepId, 60)))
      .filter((value): value is string => !!value);
    const startedAt = iso(row.startedAt);
    if (!startedAt) return null;
    const edit = documentEditFacts(steps);
    const agentLabel = text((definition?.Item as Row | undefined)?.name, 80);
    const attempts = Object.values(steps)
      .map((summary) => (Array.isArray((summary as Row | undefined)?.attempts) ? ((summary as Row).attempts as unknown[]).length : 1));
    // The step the run is inside right now. A forked node is filed as `id:2`,
    // so the summary key is trimmed back to the step the plan declared.
    const running = Object.entries(steps)
      .filter(([, summary]) => (summary as Row | undefined)?.status === 'running')
      .map(([stepId, summary]) => ({
        stepId: stepId.split(':')[0]!,
        startedAt: Date.parse(iso((summary as Row | undefined)?.startedAt) ?? '') || 0,
      }))
      .sort((left, right) => right.startedAt - left.startedAt)[0];
    return {
      runId: id,
      status: runStatus(row.status),
      attempt: Math.max(1, ...attempts),
      startedAt,
      updatedAt: iso(row.lastEventAt) ?? iso(row.completedAt) ?? startedAt,
      tools: [...new Set(tools)],
      ...(running && text(running.stepId, 60) ? { runningStep: { stepId: text(running.stepId, 60) as string } } : {}),
      ...(runPause(row) ? { pause: runPause(row) } : {}),
      ...(agentLabel ? { agentLabel } : {}),
      ...(readableError(row.error) ? { error: readableError(row.error) as string } : {}),
      ...(edit ? { edit } : {}),
    };
  }

  /**
   * One step of a run, read from the run's own persisted summary. A step that
   * the run has no record of returns `null` rather than a plausible-looking
   * placeholder — the node keeps its projected title and says nothing more.
   *
   * A forked node executes more than once and is filed as `stepId`, `stepId:2`
   * …; the newest of those is what the step node describes.
   */
  private async pipelineStep(runId: string, stepId: string, row: Row): Promise<PipelineRunFacts | null> {
    const steps = row.stepsSummary && typeof row.stepsSummary === 'object' && !Array.isArray(row.stepsSummary)
      ? (row.stepsSummary as Row)
      : {};
    const summary = Object.entries(steps)
      .filter(([key]) => key === stepId || key.startsWith(`${stepId}:`))
      .map(([, value]) => (value && typeof value === 'object' ? value as Row : undefined))
      .filter((value): value is Row => !!value)
      .sort((left, right) => Date.parse(iso(left.completedAt) ?? iso(left.startedAt) ?? '') - Date.parse(iso(right.completedAt) ?? iso(right.startedAt) ?? ''))
      .pop();
    if (!summary) return null;
    const startedAt = iso(summary.startedAt) ?? iso(row.startedAt);
    if (!startedAt) return null;
    // `agent` is the live loop telemetry the run persistence writes from
    // `pipeline:step:agent:iteration`; `output` is what the finished step
    // returned. The finished value wins where both say something.
    const live = summary.agent && typeof summary.agent === 'object' ? summary.agent as Row : {};
    const output = summary.output && typeof summary.output === 'object' ? summary.output as Row : {};
    const number = (...values: unknown[]): number | undefined => {
      for (const value of values) {
        if (typeof value === 'number' && Number.isFinite(value) && value >= 0) return Math.round(value);
      }
      return undefined;
    };
    const step: NonNullable<PipelineRunFacts['step']> = {
      stepId,
      ...(text(output.model ?? live.model, 40) ? { model: text(output.model ?? live.model, 40) as string } : {}),
      ...(number(output.iterations, live.iterations, live.iteration) !== undefined
        ? { iterations: number(output.iterations, live.iterations, live.iteration) as number } : {}),
      ...(number(output.contextTokens, live.contextTokens) !== undefined
        ? { contextTokens: number(output.contextTokens, live.contextTokens) as number } : {}),
      ...(number(output.contextBudgetTokens, live.contextBudgetTokens) !== undefined
        ? { contextBudgetTokens: number(output.contextBudgetTokens, live.contextBudgetTokens) as number } : {}),
      ...(text(output.runtime ?? live.runtime, 20) ? { runtime: text(output.runtime ?? live.runtime, 20) as string } : {}),
      ...(text(output.sessionId ?? live.sessionId, 80) ? { sessionId: text(output.sessionId ?? live.sessionId, 80) as string } : {}),
      ...(text(output.boxId ?? live.boxId, 80) ? { boxId: text(output.boxId ?? live.boxId, 80) as string } : {}),
      ...(text(output.stopReason, 30) ? { stopReason: text(output.stopReason, 30) as string } : {}),
    };
    const attempts = Array.isArray(summary.attempts) ? summary.attempts.length : 1;
    const edit = documentEditFacts(steps);
    return {
      runId,
      status: runStatus(summary.status),
      attempt: Math.max(1, attempts),
      startedAt,
      updatedAt: iso(summary.completedAt) ?? iso(row.lastEventAt) ?? startedAt,
      tools: [],
      step,
      ...(readableError(summary.error) ? { error: readableError(summary.error) as string } : {}),
      ...(edit ? { edit } : {}),
    };
  }

  /**
   * A booked room, from its own record. It reports no times: `createdAt` is
   * when the room was made, not when it is meant to happen, and the proposed
   * time is not a field on the room — so nothing is claimed about when it
   * starts rather than a plausible wrong answer.
   */
  private async bookedRoom(roomId: string): Promise<MeetingFacts | null> {
    const room = await scanOne(this.client, HANGOUT_ROOMS_TABLE, 'roomId = :id', { ':id': roomId });
    if (!room) return null;
    const roster = await this.client.send(new QueryCommand({
      TableName: HANGOUT_ROOM_MEMBERS_TABLE,
      KeyConditionExpression: 'roomId = :room',
      ExpressionAttributeValues: { ':room': roomId },
      ProjectionExpression: 'userId',
    })).catch(() => undefined);
    const expected = [
      ...(text(room.createdBy, 128) ? [text(room.createdBy, 128) as string] : []),
      ...((roster?.Items ?? []).flatMap((item) => (typeof item.userId === 'string' ? [item.userId] : []))),
    ];
    const participants = (await Promise.all([...new Set(expected)].slice(0, 12).map(async (userId) => {
      const profile = (await this.client.send(new GetCommand({
        TableName: PROFILES_TABLE, Key: { userId },
      })).catch(() => undefined))?.Item as Row | undefined;
      const label = text(profile?.displayName, 80);
      return label ? { id: userId, label } : null;
    }))).filter((value): value is { id: string; label: string } => !!value);
    const startsAt = iso(room.startsAt);
    const duration = typeof room.durationMinutes === 'number' && Number.isFinite(room.durationMinutes)
      && room.durationMinutes > 0
      ? Math.round(room.durationMinutes)
      : undefined;
    return {
      ...(participants.length ? { participants } : {}),
      // Expected, not started. `createdAt` is when the room was MADE.
      ...(startsAt ? { scheduledFor: { at: startsAt, ...(duration ? { durationMinutes: duration } : {}) } } : {}),
      // Nothing was put in anyone's calendar; the room is the invitation.
      calendar: 'none',
      recorded: false,
      transcriptAvailable: false,
    };
  }

  async artifact(documentId: string): Promise<ArtifactFacts | null> {
    // The comment count does not depend on the row: asked at the same time,
    // not after it (NFR #157).
    const counted = this.client.send(new QueryCommand({
      TableName: DOCUMENT_COMMENTS_TABLE,
      KeyConditionExpression: 'documentId = :doc',
      ExpressionAttributeValues: { ':doc': documentId },
      Select: 'COUNT',
    })).catch(() => undefined);
    const row = (await this.client.send(new GetCommand({
      TableName: DOCUMENTS_TABLE,
      Key: { documentId },
    }))).Item as Row | undefined;
    if (!row) return null;

    const kind = String(row.type ?? row.documentType ?? '').toLowerCase();
    const mediaKind: ArtifactFacts['mediaKind'] = kind.includes('present') || kind.includes('deck') || kind.includes('slide')
      ? 'presentation'
      : kind.includes('image')
        ? 'image'
        : 'document';

    // Slide/page identities are the document's own outline entries, not a
    // count invented from a preview image.
    const outline = Array.isArray(row.outline) ? row.outline : Array.isArray(row.slides) ? row.slides : [];
    const anchors = outline
      .map((entry, position) => {
        const item = entry as Row | undefined;
        const label = text(item?.title ?? item?.label, 80);
        const id = text(item?.id, 120);
        if (!label || !id) return undefined;
        return {
          kind: (mediaKind === 'presentation' ? 'slide' : 'page') as 'slide' | 'page',
          id,
          label,
          index: position + 1,
        };
      })
      .filter((value): value is NonNullable<typeof value> => !!value);

    const pageCount = typeof row.pageCount === 'number' && Number.isSafeInteger(row.pageCount) && row.pageCount > 0
      ? row.pageCount
      : anchors.length || undefined;
    // An older revision's slide list is a manifest off the row (NFR #36); put
    // it back in place so each revision still names its own slides.
    // NFR #188: a row whose revisions are items (`revisionCount` set) gets
    // them back from `presentation-revisions`, merged with any array left.
    const [comments, hydrated] = await Promise.all([counted, Promise.resolve()
      .then(async () => {
        const base = row as unknown as PresentationDocumentRow;
        if (typeof base.revisionCount !== 'number') return base;
        const items = await queryAllRevisionItems(this.client, documentId);
        return { ...base, revisions: mergeRevisions(base.revisions, items) };
      })
      .then((full) => withRevisionSlides((this.deps.presentations ?? getPresentationDocumentStore)(), full))
      .catch(() => row as unknown as PresentationDocumentRow)]);
    const revisions = storedRevisionFacts(hydrated as unknown as Row);
    return {
      ...(text(row.title, 120) ? { name: text(row.title, 120) as string } : {}),
      mediaKind,
      ...(pageCount ? { pageCount } : {}),
      ...(anchors.length ? { anchors } : {}),
      ...(revisions.length ? { revisions } : {}),
      ...(comments?.Count ? { feedbackCount: comments.Count } : {}),
    };
  }

  /**
   * Serves a stored preview, and refuses one that could execute. An SVG is
   * markup: a source that stored a script, an event handler, or an external
   * reference in one would get it run inside an authorized viewer's page, so
   * those are rejected here rather than sanitized into something subtly
   * different from what the source actually holds.
   */
  async revisionPreview(documentId: string, revisionId: string, slideId?: string): Promise<RevisionPreview | null> {
    if (slideId !== undefined) return this.slidePreview(documentId, revisionId, slideId);
    const row = (await this.client.send(new GetCommand({
      TableName: DOCUMENTS_TABLE,
      Key: { documentId },
    }))).Item as Row | undefined;
    const previews = row?.revisionPreviews;
    if (!previews || typeof previews !== 'object' || Array.isArray(previews)) return null;
    const entry = (previews as Row)[revisionId] as Row | undefined;
    // Since the row diet (NFR #4) a revision's cover names its slide rather
    // than carrying a copy of the markup; the slide's own blob answers.
    if (entry && typeof entry.body !== 'string' && typeof entry.slideId === 'string' && entry.slideId) {
      return this.slidePreview(documentId, revisionId, entry.slideId);
    }
    if (!entry || typeof entry.body !== 'string') return null;
    const contentType = String(entry.contentType ?? '');
    if (!ALLOWED_PREVIEW_TYPES.has(contentType)) return null;
    const body = entry.encoding === 'base64'
      ? Buffer.from(entry.body, 'base64')
      : Buffer.from(entry.body, 'utf8');
    if (body.byteLength === 0 || body.byteLength > MAX_PREVIEW_BYTES) return null;
    if (contentType === 'image/svg+xml' && !isInertSvg(body.toString('utf8'))) return null;
    return { contentType: contentType as RevisionPreview['contentType'], body };
  }

  /**
   * One slide's own rendered image.
   *
   * The revision record names the blob the generator wrote for that slide; the
   * bytes come from the blob store the record points at, and are held to the
   * same inert-markup rule as an inline preview. A slide the record does not
   * name — or whose bytes are gone — is absent, never another slide's image.
   */
  private async slidePreview(documentId: string, revisionId: string, slideId: string): Promise<RevisionPreview | null> {
    const store = (this.deps.presentations ?? getPresentationDocumentStore);
    const resolved = await Promise.resolve()
      .then(async (): Promise<{ contentType: string; body: Buffer } | null> => {
        const presentations = store();
        // The store reads the row's own markup first and the blob the record
        // names second. The row is what survives a blob volume that does not,
        // which is exactly how every slide of a seeded deck 404'd while its
        // inline cover kept serving.
        if (presentations.slidePreview) return presentations.slidePreview(documentId, revisionId, slideId);
        // A store that predates the method: same two places, same order.
        const row = await presentations.get(documentId);
        const revision = row?.revisions?.find((candidate) => candidate.id === revisionId);
        const slide = [
          ...(revision ? (await resolveRevisionSlides(presentations, revision)) ?? [] : []),
          ...(row?.revisionSlides?.[revisionId] ?? []),
        ].find((entry) => entry?.id === slideId);
        if (!slide) return null;
        if (slide.svg) {
          return { contentType: slide.asset?.contentType || SVG_CONTENT_TYPE, body: Buffer.from(slide.svg, 'utf8') };
        }
        if (!slide.asset?.key) return null;
        const blob = await (this.deps.blobs ?? getAssetBlobStore)().getBlob(slide.asset.key);
        return blob ? { contentType: blob.asset?.contentType || slide.asset.contentType, body: blob.body } : null;
      })
      .catch((error: unknown) => {
        log.warn({ documentId, revisionId, slideId, err: (error as Error)?.message }, 'work-graph: slide preview store unavailable');
        return null;
      });
    // Only now is the slide really absent: neither the row nor the blob store
    // has it. Saying so is what turns "a volume did not survive a restart"
    // into a log line rather than an hour of curling.
    if (!resolved) {
      log.warn({ documentId, revisionId, slideId }, 'work-graph: a revision names a slide whose bytes are gone');
      return null;
    }
    const contentType = String(resolved.contentType ?? '');
    if (!ALLOWED_PREVIEW_TYPES.has(contentType)) return null;
    if (resolved.body.byteLength === 0 || resolved.body.byteLength > MAX_PREVIEW_BYTES) return null;
    if (contentType === 'image/svg+xml' && !isInertSvg(resolved.body.toString('utf8'))) return null;
    return { contentType: contentType as RevisionPreview['contentType'], body: resolved.body };
  }

  async transcript(transcriptId: string): Promise<TranscriptFacts | null> {
    const id = transcriptId.replace(/^transcript:/, '');
    const row = (await recordingLocator.find(this.client, RECORDINGS_TABLE, `transcript:${id}`)) ?? undefined;
    if (!row || row.transcriptStatus !== 'ready' || typeof row.transcriptJson !== 'string') return null;
    let parsed: unknown;
    try {
      parsed = JSON.parse(row.transcriptJson);
    } catch {
      return null;
    }
    const raw = Array.isArray(parsed)
      ? parsed
      : Array.isArray((parsed as Row | undefined)?.segments)
        ? ((parsed as Row).segments as unknown[])
        : [];
    const segments = raw
      .map((entry, position) => {
        const item = entry as Row | undefined;
        const at = iso(item?.at ?? item?.start ?? item?.timestamp);
        const speaker = text(item?.speaker, 60);
        const body = text(item?.text, 1_000);
        if (!at || !speaker || !body) return undefined;
        return { id: text(item?.id, 120) ?? `segment-${position + 1}`, at, speaker, text: body };
      })
      .filter((value): value is { id: string; at: string; speaker: string; text: string } => !!value)
      .sort((left, right) => Date.parse(left.at) - Date.parse(right.at))
      .slice(0, MAX_TRANSCRIPT_SEGMENTS);
    if (segments.length === 0) return null;
    const actionItems = Array.isArray((parsed as Row | undefined)?.actionItems)
      ? ((parsed as Row).actionItems as unknown[]).length
      : 0;
    return {
      segments,
      ready: true,
      speakerCount: new Set(segments.map((segment) => segment.speaker)).size,
      actionItemCount: actionItems,
    };
  }

  async meeting(meetingId: string): Promise<MeetingFacts | null> {
    const id = meetingId.replace(/^meeting:/, '');
    const row = (await recordingLocator.find(this.client, RECORDINGS_TABLE, `meeting:${id}`)) ?? undefined;
    // A room that has not happened yet has no recording. It is still a real
    // meeting with real people expected on it, and saying nothing about it
    // left the card blank for the person who had just booked it.
    if (!row) return this.bookedRoom(id);
    const attendees = Array.isArray(row.participants)
      ? row.participants
        .map((entry) => text((entry as Row | undefined)?.userId, 128))
        .filter((value): value is string => !!value)
      : [];
    const participants = (await Promise.all([...new Set(attendees)].slice(0, 12).map(async (userId) => {
      const profile = (await this.client.send(new GetCommand({
        TableName: PROFILES_TABLE, Key: { userId },
      })).catch(() => undefined))?.Item as Row | undefined;
      // Without a profile the person is still a real attendee, but naming
      // them by their account id would put an identifier on the card.
      const label = text(profile?.displayName, 80);
      return label ? { id: userId, label } : null;
    }))).filter((value): value is { id: string; label: string } => !!value);
    return {
      ...(participants.length ? { participants } : {}),
      ...(iso(row.startedAt) ? { startedAt: iso(row.startedAt) as string } : {}),
      ...(iso(row.endedAt) ? { endedAt: iso(row.endedAt) as string } : {}),
      recorded: typeof row.recordingId === 'string' && row.recordingId.length > 0,
      transcriptAvailable: row.transcriptStatus === 'ready',
    };
  }

  async attention(documentId: string): Promise<DocumentAttentionFacts | null> {
    // The authoritative lifecycle row records who last acted on the document
    // and when. That is a real, expiring observation — unlike an open socket,
    // which says nothing about whether a person is reading anything.
    const row = (await this.client.send(new GetCommand({
      TableName: DOCUMENT_LIFECYCLE_TABLE,
      Key: { documentId },
    })).catch(() => undefined))?.Item as Row | undefined;
    const observedAt = iso(row?.updatedAt);
    const actorId = text(row?.updatedBy, 128);
    if (!row || !observedAt || !actorId) return null;
    const expiresAt = new Date(Date.parse(observedAt) + ATTENTION_LEASE_MS).toISOString();
    return { actorId, observedAt, expiresAt };
  }
}

/**
 * What each stored revision is made of, read from the document's own row.
 *
 * `revisions[]` is written by the presentation store; `revisionSlides[rev]` is
 * the same list keyed by revision, and either is authoritative. A row that
 * records neither — the hand-seeded deck, or any text document — yields no
 * revision facts at all, so nothing downstream invents a slide list for it.
 */
function storedRevisionFacts(row: Row): ArtifactRevisionFacts[] {
  const stored = Array.isArray(row.revisions) ? row.revisions : [];
  const byRevision = row.revisionSlides && typeof row.revisionSlides === 'object' && !Array.isArray(row.revisionSlides)
    ? row.revisionSlides as Row
    : {};
  return stored
    .slice(0, MAX_REVISIONS)
    .map((entry) => {
      const revision = entry as Row | undefined;
      const id = text(revision?.id, 120);
      if (!id) return undefined;
      // Either place is authoritative; the revision's own list wins when it has
      // one, and an empty list there is not a claim that the revision is empty.
      const own = Array.isArray(revision?.slides) ? revision.slides as unknown[] : [];
      const recorded = own.length
        ? own
        : Array.isArray(byRevision[id]) ? byRevision[id] as unknown[] : [];
      const slides = recorded
        .slice(0, MAX_SLIDES_PER_REVISION)
        .map((value, position) => {
          const slide = value as Partial<StoredSlide> | undefined;
          const slideId = text(slide?.id, 120);
          if (!slideId) return undefined;
          const index = typeof slide?.index === 'number' && Number.isSafeInteger(slide.index) && slide.index > 0
            ? slide.index
            : position + 1;
          // A slide the source did not title is still a real slide; its number
          // is the only thing we know about it, so that is what it is called.
          return { id: slideId, index, title: text(slide?.title, 80) ?? `Slide ${index}` };
        })
        .filter((value): value is { id: string; index: number; title: string } => !!value);
      const declared = Array.isArray(revision?.sources) ? revision.sources as unknown[] : [];
      const sources = declared
        .slice(0, MAX_SOURCES_PER_REVISION)
        .map((value) => {
          const source = value as Row | undefined;
          const sourceDocument = text(source?.documentId, 200);
          const title = text(source?.title, 120);
          const kind = source?.kind === 'transcript' || source?.kind === 'document' ? source.kind : undefined;
          return sourceDocument && title
            ? { documentId: sourceDocument, title, ...(kind ? { kind } : {}) }
            : undefined;
        })
        .filter((value): value is { documentId: string; title: string; kind?: 'document' | 'transcript' } => !!value);
      const label = text(revision?.label, 80);
      const heading = text(revision?.heading, 120);
      const createdAt = iso(revision?.createdAt);
      const slideCount = typeof revision?.slideCount === 'number'
        && Number.isSafeInteger(revision.slideCount)
        && revision.slideCount > 0
        ? revision.slideCount
        : slides.length;
      if (!slideCount) return undefined;
      return {
        id,
        ...(label ? { label } : {}),
        ...(createdAt ? { createdAt } : {}),
        slideCount,
        slides,
        ...(heading ? { heading } : {}),
        ...(sources.length ? { sources } : {}),
      };
    })
    .filter((value): value is ArtifactRevisionFacts => !!value);
}
