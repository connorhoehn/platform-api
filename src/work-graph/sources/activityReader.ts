/**
 * Reads the v2 activity facts from each source's own authoritative record.
 *
 * It performs no authorization of its own: the orchestrator has already
 * decided which nodes this viewer may see at `details` disclosure, and only
 * those node identities reach these lookups. Every method returns `null` when
 * the source has nothing to say, so an absent record discloses nothing rather
 * than producing a plausible-looking placeholder.
 */
import { GetCommand, QueryCommand, ScanCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { tableName } from '../../lib/ddb-table-name';
import type {
  ArtifactFacts,
  RevisionPreview,
  DocumentAttentionFacts,
  MeetingFacts,
  PipelineRunFacts,
  TranscriptFacts,
  WorkActivityReader,
} from '../activity';
import { parsePipelineStepResourceId } from './mapPipeline';

const PIPELINE_RUNS_TABLE = tableName('pipeline-runs');
const RECORDINGS_TABLE = tableName('video-call-recordings');
const DOCUMENTS_TABLE = tableName(process.env.DYNAMODB_DOCUMENTS_TABLE || 'crdt-documents');
const DOCUMENT_COMMENTS_TABLE = tableName('document-comments');
const DOCUMENT_LIFECYCLE_TABLE = tableName('document-lifecycle');
const PIPELINE_DEFINITIONS_TABLE = tableName('pipeline-definitions');
const PROFILES_TABLE = tableName('social-profiles');

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

type Row = Record<string, unknown>;

function text(value: unknown, limit: number): string | undefined {
  if (typeof value !== 'string') return undefined;
  const trimmed = value.trim().replace(/\s+/g, ' ');
  if (!trimmed) return undefined;
  return trimmed.length <= limit ? trimmed : `${trimmed.slice(0, limit - 1)}…`;
}

function iso(value: unknown): string | undefined {
  if (typeof value !== 'string') return undefined;
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? new Date(parsed).toISOString() : undefined;
}

/** Folds the platform's extra lifecycle states onto the five we can describe. */
function runStatus(value: unknown): PipelineRunFacts['status'] {
  switch (String(value)) {
    case 'running':
    case 'resuming':
      return 'running';
    case 'waiting':
    case 'paused':
    case 'pending':
      return 'waiting';
    case 'completed':
    case 'succeeded':
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
  const result = await client.send(new ScanCommand({
    TableName: table,
    FilterExpression: expression,
    ExpressionAttributeValues: values,
    Limit: 200,
  }));
  const rows = (result.Items ?? []) as Row[];
  // More than one match is ambiguous provenance, so nothing is disclosed.
  return rows.length === 1 ? rows[0] : undefined;
}

export class DynamoWorkActivityReader implements WorkActivityReader {
  constructor(private readonly client: DynamoDBDocumentClient) {}

  async pipelineRun(resource: string): Promise<PipelineRunFacts | null> {
    const { runId: id, stepId } = parsePipelineStepResourceId(resource);
    const row = await scanOne(this.client, PIPELINE_RUNS_TABLE, 'runId = :run', { ':run': id });
    if (!row) return null;
    if (stepId) return this.pipelineStep(id, stepId, row);
    const steps = row.stepsSummary && typeof row.stepsSummary === 'object' && !Array.isArray(row.stepsSummary)
      ? (row.stepsSummary as Row)
      : {};
    // The tools are the steps this run actually recorded, in execution order.
    const tools = Object.entries(steps)
      .map(([stepId, summary]) => ({
        stepId,
        startedAt: Date.parse(iso((summary as Row | undefined)?.startedAt) ?? '') || 0,
      }))
      .sort((left, right) => left.startedAt - right.startedAt || left.stepId.localeCompare(right.stepId))
      .map((step) => text(step.stepId, 60))
      .filter((value): value is string => !!value);
    const startedAt = iso(row.startedAt);
    if (!startedAt) return null;
    // The definition's own name, so the card says "Presentation agent" rather
    // than an internal pipeline id.
    // The definitions table is keyed (userId, pipelineId): the owner is the
    // person who triggered this run, which is the same person the graph is of.
    const definition = await this.client.send(new GetCommand({
      TableName: PIPELINE_DEFINITIONS_TABLE,
      Key: { userId: String(row.triggeredBy ?? ''), pipelineId: String(row.pipelineId ?? '') },
    })).catch(() => undefined);
    const agentLabel = text((definition?.Item as Row | undefined)?.name, 80);
    const attempts = Object.values(steps)
      .map((summary) => (Array.isArray((summary as Row | undefined)?.attempts) ? ((summary as Row).attempts as unknown[]).length : 1));
    return {
      runId: id,
      status: runStatus(row.status),
      attempt: Math.max(1, ...attempts),
      startedAt,
      updatedAt: iso(row.lastEventAt) ?? iso(row.completedAt) ?? startedAt,
      tools: [...new Set(tools)],
      ...(agentLabel ? { agentLabel } : {}),
      ...(text(row.error, ERROR_SUMMARY_LENGTH) ? { error: text(row.error, ERROR_SUMMARY_LENGTH) as string } : {}),
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
    return {
      runId,
      status: runStatus(summary.status),
      attempt: Math.max(1, attempts),
      startedAt,
      updatedAt: iso(summary.completedAt) ?? iso(row.lastEventAt) ?? startedAt,
      tools: [],
      step,
      ...(text(summary.error, ERROR_SUMMARY_LENGTH) ? { error: text(summary.error, ERROR_SUMMARY_LENGTH) as string } : {}),
    };
  }

  async artifact(documentId: string): Promise<ArtifactFacts | null> {
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

    const comments = await this.client.send(new QueryCommand({
      TableName: DOCUMENT_COMMENTS_TABLE,
      KeyConditionExpression: 'documentId = :doc',
      ExpressionAttributeValues: { ':doc': documentId },
      Select: 'COUNT',
    })).catch(() => undefined);

    const pageCount = typeof row.pageCount === 'number' && Number.isSafeInteger(row.pageCount) && row.pageCount > 0
      ? row.pageCount
      : anchors.length || undefined;
    return {
      ...(text(row.title, 120) ? { name: text(row.title, 120) as string } : {}),
      mediaKind,
      ...(pageCount ? { pageCount } : {}),
      ...(anchors.length ? { anchors } : {}),
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
  async revisionPreview(documentId: string, revisionId: string): Promise<RevisionPreview | null> {
    const row = (await this.client.send(new GetCommand({
      TableName: DOCUMENTS_TABLE,
      Key: { documentId },
    }))).Item as Row | undefined;
    const previews = row?.revisionPreviews;
    if (!previews || typeof previews !== 'object' || Array.isArray(previews)) return null;
    const entry = (previews as Row)[revisionId] as Row | undefined;
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

  async transcript(transcriptId: string): Promise<TranscriptFacts | null> {
    const id = transcriptId.replace(/^transcript:/, '');
    const row = await scanOne(
      this.client,
      RECORDINGS_TABLE,
      'transcriptId = :id OR recordingId = :id',
      { ':id': id },
    );
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
    const row = await scanOne(this.client, RECORDINGS_TABLE, 'callId = :id', { ':id': id });
    if (!row) return null;
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
