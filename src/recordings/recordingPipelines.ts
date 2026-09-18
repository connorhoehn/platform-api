// What happens after a call is recorded, as PIPELINES.
//
// Until now the LVS `recording.completed` webhook did everything inline —
// resolve the call, write the row, enqueue transcription on a message queue
// whose worker only exists under Tilt — and when any of that failed the
// recording had no transcript and nobody could see why. Now the webhook
// starts a run and answers 204; the run does the work in steps the run store
// persists and the pipeline UI shows, with retries where the outside world is
// slow (LVS composing the file) and a card in the conversation that moves.
//
//   recording-finalize:  trigger → resolve → persist → await-compose → announce → start-transcription
//   call-transcription:  trigger → transcribe → merge-reactions → store → summarize → store-summary → apply → publish
//
// The meeting-notes summary is a step of the transcription run (a light model
// over the speaker-tagged transcript, then one row write). It used to be a
// second writer — an MQ consumer on `transcription:transcript.ready` fed by a
// Tilt-only worker — racing this pipeline for the same row fields; that
// consumer is gone, and the row has exactly one writer per field again.
//
// Transcription runs in-process against the composed file with the local
// speech engine, so a laptop with LVS and platform-api produces a transcript
// with no queue and no second container — the same step a worker fleet would
// run, behind the same step kind.
import { execFile } from 'child_process';
import { promises as fs } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import { randomUUID } from 'crypto';
import { QueryCommand } from '@aws-sdk/client-dynamodb';
import type { StepHandler, StepOutcome, PipelineNode } from 'distributed-core/applications/pipeline';
import { callRecordingRepo, videoSessionRepo, callReactionRepo, type CallRecordingParticipant } from '../repositories';
import type { CallRecording, ComposeStatus } from '../repositories/CallRecordingRepository';
import { deriveMediaDurationMs, reconcileParticipants, type TruthTrack } from '../lib/recording-truth';
import { mergeReactionsIntoTranscript } from '../lib/transcript-reactions';
import { callSfu } from '../lib/sfu-breaker';
import { getRedisClient } from '../lib/redis-client';
import { VIDEO_API_URL } from '../lib/video-auth';
import { tableName } from '../lib/ddb-table-name';
import { ddbClient } from '../lib/aws-clients';
import { getPipelineBridge } from '../pipeline/bridge-types';
import { getRunPersistenceService } from '../pipeline/runPersistence';
import { postChatMessage, PIPELINE_IDENTITY } from '../gateway/GatewayServiceClient';
import { VIDEO_PUBLIC_URL } from '../lib/video-auth';
import { DOCUMENT_APPLY_KIND } from '../documents/documentAgentPipeline';
import { resolveRecordingProfile } from '@connorhoehn/event-catalog';
import type { DescribedStepHandler } from '../pipeline/customStepHandlers';
import log from '../lib/logger';

export const RECORDING_FINALIZE_PIPELINE_ID = 'recording-finalize';
export const CALL_TRANSCRIPTION_PIPELINE_ID = 'call-transcription';

/** Presence identity the transcript is written under. */
export const TRANSCRIPT_AGENT = { userId: PIPELINE_IDENTITY.userId, displayName: 'Transcript', color: '#0ea5e9' };

/** The document operations a transcript becomes: a heading, then one paragraph per segment. */
export function transcriptOps(segments: Array<{ text: string }>): Array<Record<string, unknown>> {
  const spoken = segments.filter((s) => s.text && s.text.trim());
  if (spoken.length === 0) return [];
  return [
    { op: 'appendBlock', kind: 'heading', level: 2, text: 'Call transcript' },
    ...spoken.map((s) => ({ op: 'appendBlock', kind: 'paragraph', text: s.text.trim() })),
  ];
}

export const KINDS = {
  resolve: 'recording:resolve',
  persist: 'recording:persist',
  awaitCompose: 'recording:await-compose',
  announce: 'recording:announce',
  startTranscription: 'transcription:start',
  transcribe: 'transcription:transcribe',
  mergeReactions: 'transcript:merge-reactions',
  store: 'transcript:store',
  storeSummary: 'transcript:store-summary',
  publish: 'transcript:publish',
} as const;

/** The transcript as the summarising model reads it: one speaker-tagged, timestamped line per segment. Empty for silence. */
export function transcriptText(segments: Array<{ startMs?: number; speakerLabel?: string; text: string }>): string {
  return segments
    .filter((s) => s.text && s.text.trim())
    .map((s) => {
      const startMs = Number(s.startMs ?? 0);
      const min = Math.floor(startMs / 60000); const sec = Math.floor((startMs % 60000) / 1000);
      return `[${String(min).padStart(2, '0')}:${String(sec).padStart(2, '0')}] ${s.speakerLabel || 'Call'}: ${s.text.trim()}`;
    })
    .join('\n');
}

/** What the light model is told; the `summarize` agent step interpolates it from the run context. */
export const CALL_SUMMARY_PROMPT = [
  'You are an organizational knowledge analyst. You will be given a meeting transcript with speaker labels and timestamps. Produce a concise, well-structured markdown summary suitable for participants reviewing the call later.',
  'If the transcript below is empty or missing, answer with exactly `(no speech)` and nothing else.',
  '',
  'Output STRICTLY this markdown structure (omit a section only if its content would be empty after a genuine scan of the transcript):',
  '',
  '## Summary',
  'A 2-4 sentence overview of what the meeting was about and the outcome.',
  '',
  '## Topics Discussed',
  '- Bullet list of distinct topics, each with a one-line summary.',
  '',
  '## Key Decisions',
  '- Bullet list of decisions reached during the call. Quote the deciding speaker by their label when the decision is attributable.',
  '',
  '## Action Items',
  '| Owner | Action | Deadline |',
  '|-------|--------|----------|',
  '| Name  | What they committed to do | When (or "TBD") |',
  '',
  '## Open Questions',
  '- Anything raised but not resolved. Cite the asker by speaker label.',
  '',
  'Rules:',
  '- Use the speaker labels verbatim — do not rename participants.',
  '- Quote sparingly. Paraphrase technical content.',
  '- If a section has no content, omit the section heading entirely.',
  '- Do not add a preamble, a signature or a code fence. Start directly with "## Summary".',
  '',
  'Transcript:',
  '{{ context.transcriptText }}',
].join('\n');

/** A model answer with any surrounding ```fence``` removed. */
export function stripFences(text: string): string {
  const t = String(text ?? '').trim();
  const m = /^```[a-zA-Z0-9_-]*\s*\n([\s\S]*?)\n?```$/.exec(t);
  return (m ? m[1] : t).trim();
}

/** The document operations a summary becomes: a heading, then one paragraph per non-empty line. */
export function summaryOps(summaryMarkdown: string): Array<Record<string, unknown>> {
  const lines = String(summaryMarkdown ?? '').split('\n').map((l) => l.trim()).filter(Boolean);
  if (lines.length === 0) return [];
  return [
    { op: 'appendBlock', kind: 'heading', level: 2, text: 'Call summary' },
    ...lines.map((text) => ({ op: 'appendBlock', kind: 'paragraph', text })),
  ];
}

const SETTLED_COMPOSE: ReadonlySet<string> = new Set(['ok', 'degraded', 'failed', 'skipped']);
const lvsAuth = () => ({ Authorization: `Bearer ${process.env.LVS_ADMIN_TOKEN ?? process.env.VIDEO_ADMIN_TOKEN ?? 'admin'}` });

// The lobby → conversation rule lives in ./lobbyChannel (it now covers
// document calls too); re-exported so existing importers keep their path.
import { chatChannelForLobby } from './lobbyChannel';
export { chatChannelForLobby };

/** A call started from a document page uses the document id as its lobby. */
export function documentIdForLobby(lobby: string | null | undefined): string | null {
  if (!lobby) return null;
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(lobby) ? lobby : null;
}

const pos = (x: number) => ({ x, y: 0 });
/** Every step here talks to LVS, DDB, the gateway or a document; the executor retries each from this. */
const IO_RETRY = { maxAttempts: 3, backoffMs: 1500, backoffMultiplier: 2, retryUnknown: true } as const;
const edge = (a: string, b: string) => ({ id: `${a}->${b}`, source: a, sourceHandle: 'out', target: b, targetHandle: 'in' });

export function buildRecordingFinalizeDefinition(): Record<string, unknown> {
  return {
    id: RECORDING_FINALIZE_PIPELINE_ID,
    name: 'Recording finalize',
    description: 'After a call ends: find the call, save the recording, wait for the composed file, tell the conversation, start transcription.',
    version: 1, status: 'published', publishedVersion: 1,
    nodes: [
      { id: 'trigger', type: 'trigger', position: pos(0), data: { type: 'trigger', triggerType: 'webhook', label: 'LVS recording.completed' } },
      { id: 'resolve', type: KINDS.resolve, position: pos(200), data: { type: KINDS.resolve, label: 'Find the call', retryPolicy: IO_RETRY } },
      { id: 'persist', type: KINDS.persist, position: pos(400), data: { type: KINDS.persist, label: 'Save the recording', retryPolicy: IO_RETRY } },
      { id: 'await-compose', type: KINDS.awaitCompose, position: pos(600), data: { type: KINDS.awaitCompose, label: 'Wait for the composed file', retryPolicy: { maxAttempts: 12, backoffMs: 5000, backoffMultiplier: 1.2, retryUnknown: true } } },
      { id: 'announce', type: KINDS.announce, position: pos(800), data: { type: KINDS.announce, label: 'Post the recording card', retryPolicy: IO_RETRY } },
      { id: 'start-transcription', type: KINDS.startTranscription, position: pos(1000), data: { type: KINDS.startTranscription, label: 'Start transcription', retryPolicy: IO_RETRY } },
    ],
    edges: [edge('trigger', 'resolve'), edge('resolve', 'persist'), edge('persist', 'announce'), edge('announce', 'await-compose'), edge('await-compose', 'start-transcription')],
  };
}

const SUMMARY_RETRY = { maxAttempts: 3, backoffMs: 2000, backoffMultiplier: 2, retryUnknown: true } as const;

export function buildCallTranscriptionDefinition(model = process.env.DOCUMENT_AGENT_MODEL || 'haiku'): Record<string, unknown> {
  return {
    id: CALL_TRANSCRIPTION_PIPELINE_ID,
    name: 'Call transcription',
    description: 'Transcribe the composed recording, merge in-call reactions by time, store the transcript, summarise it with a light model, publish it to the conversation and the call\'s document.',
    version: 1, status: 'published', publishedVersion: 1,
    nodes: [
      { id: 'trigger', type: 'trigger', position: pos(0), data: { type: 'trigger', triggerType: 'platform', label: 'Recording ready' } },
      { id: 'transcribe', type: KINDS.transcribe, position: pos(200), data: { type: KINDS.transcribe, label: 'Transcribe the audio', retryPolicy: { maxAttempts: 3, backoffMs: 3000, backoffMultiplier: 2, retryUnknown: true } } },
      { id: 'merge-reactions', type: KINDS.mergeReactions, position: pos(400), data: { type: KINDS.mergeReactions, label: 'Merge reactions', retryPolicy: IO_RETRY } },
      { id: 'store', type: KINDS.store, position: pos(600), data: { type: KINDS.store, label: 'Store the transcript', retryPolicy: IO_RETRY } },
      // The meeting notes: a light model over the speaker-tagged transcript
      // (`transcriptText`, empty for silence), then one row write. The agent
      // step always runs; store-summary skips the row's summary when nothing
      // was said and never reads the model's answer in that case.
      { id: 'summarize', type: 'agent', position: pos(800), data: { type: 'agent', label: 'Summarise the call (Haiku)', prompt: CALL_SUMMARY_PROMPT, model, allowedTools: [], maxTurns: 1, retryPolicy: SUMMARY_RETRY } },
      { id: 'store-summary', type: KINDS.storeSummary, position: pos(1000), data: { type: KINDS.storeSummary, label: 'Store the summary', retryPolicy: SUMMARY_RETRY } },
      // The one shared write path into a document — the same step kind /doc and
      // Transform use. Optional: a room call has no document to write into.
      { id: 'apply', type: DOCUMENT_APPLY_KIND, position: pos(1200), data: { type: DOCUMENT_APPLY_KIND, label: 'Write the summary and transcript into the call document', opsFrom: 'transcriptOps', optional: true, agent: TRANSCRIPT_AGENT, retryPolicy: IO_RETRY } },
      { id: 'publish', type: KINDS.publish, position: pos(1400), data: { type: KINDS.publish, label: 'Publish to the conversation', retryPolicy: IO_RETRY } },
    ],
    edges: [edge('trigger', 'transcribe'), edge('transcribe', 'merge-reactions'), edge('merge-reactions', 'store'), edge('store', 'summarize'), edge('summarize', 'store-summary'), edge('store-summary', 'apply'), edge('apply', 'publish')],
  };
}

// ---------------------------------------------------------------------------
// Shared bits
// ---------------------------------------------------------------------------

async function resolveCallIdByLobby(lobbyName: string): Promise<string | null> {
  try {
    const out = await ddbClient.send(new QueryCommand({
      TableName: tableName('call-session-bindings'),
      IndexName: 'lobbyName-index',
      KeyConditionExpression: 'lobbyName = :l',
      ExpressionAttributeValues: { ':l': { S: lobbyName } },
      Limit: 1,
      ScanIndexForward: false,
    }));
    return (out.Items ?? [])[0]?.callId?.S ?? null;
  } catch (e) {
    log.warn({ err: e instanceof Error ? e.message : String(e), lobbyName }, '[recording-pipeline] callId lookup failed');
    return null;
  }
}

function str(v: unknown): string | undefined { return typeof v === 'string' && v ? v : undefined; }
const fail = (error: string): StepOutcome => ({ status: 'failed', error });
const ok = (output: Record<string, unknown>): StepOutcome => ({ status: 'completed', output });

/** Start a platform-owned run and register it; the trigger route does the same for /doc. */
export async function startSystemRun(pipelineId: string, definition: Record<string, unknown>, triggerPayload: Record<string, unknown>, userId: string): Promise<string> {
  const bridge = getPipelineBridge();
  if (!bridge?.trigger) throw new Error('pipeline engine is not available');
  const out = await Promise.resolve(bridge.trigger({ pipelineId, definition: definition as never, triggerPayload, triggeredBy: { userId } } as never));
  const runId = (out as { runId: string }).runId;
  getRunPersistenceService()?.registerTrigger({ runId, pipelineId, userId, triggerType: 'manual', at: new Date().toISOString() });
  return runId;
}

/** Post the run's card into the conversation, if the lobby has one. Never fatal. */
async function postRunCard(input: { lobbyName: string | null; runId: string; pipelineId: string; command: string; instruction: string; extra?: Record<string, unknown> }): Promise<boolean> {
  const channel = chatChannelForLobby(input.lobbyName);
  if (!channel) {
    // Not an error — an ad-hoc lobby has no thread — but not silence either:
    // this was the only trace when document calls' cards vanished for weeks.
    log.info({ lobbyName: input.lobbyName, runId: input.runId, command: input.command, reason: 'lobby has no conversation' }, '[recording-pipeline] run card not posted');
    return false;
  }
  try {
    await postChatMessage({
      channel,
      message: `${input.command} ${input.instruction}`,
      metadata: { kind: 'agent-run', runId: input.runId, pipelineId: input.pipelineId, command: input.command, instruction: input.instruction, agentName: PIPELINE_IDENTITY.displayName, ...(input.extra ?? {}) },
    });
    return true;
  } catch (e) {
    log.warn({ err: e instanceof Error ? e.message : String(e), channel }, '[recording-pipeline] could not post the run card');
    return false;
  }
}

// ---------------------------------------------------------------------------
// recording-finalize steps
// ---------------------------------------------------------------------------

function resolveHandler(): DescribedStepHandler {
  return {
    kind: KINDS.resolve,
    description:
      'Finds the video session behind a channel ARN — via the Redis index, falling back to a scan — and works out which call it belongs to.',
    reads: 'arn and recordingId.',
    emits:
      'lobbyName, callId, sessionId, startedBy, the participant list, and the chat channel and document the lobby maps to.',
    async execute(_n: PipelineNode, ctx: Record<string, unknown>): Promise<StepOutcome> {
      const arn = str(ctx.arn); const recordingId = str(ctx.recordingId);
      if (!arn || !recordingId) return fail('arn and recordingId are required');
      let session: any = null;
      try {
        const redis = await getRedisClient();
        const raw = redis ? await redis.get(`video:arn-index:${arn}`) : null;
        if (raw) {
          const idx = JSON.parse(raw) as { sessionId: string; lobbyName: string };
          session = await videoSessionRepo.getSession?.(idx.lobbyName, idx.sessionId).catch(() => null);
        }
      } catch { /* fall through to the scan */ }
      if (!session) {
        try { const matches = await videoSessionRepo.findByChannelArn(arn); if (matches.length > 0) session = matches[0]; } catch { /* no session */ }
      }
      const lobbyName: string = session?.documentId ?? '';
      const callId = (lobbyName ? await resolveCallIdByLobby(lobbyName) : null) ?? `unknown-${arn}-${recordingId}`;
      let participants: CallRecordingParticipant[] = (session?.participants ?? []).map((p: any) => ({ userId: p.userId, displayName: p.displayName, participantId: p.participantId, joinedAt: p.joinedAt }));
      if (participants.length === 0 && lobbyName.startsWith('dm:')) {
        const ids = lobbyName.slice(3).split(':').filter(Boolean);
        if (ids.length >= 2) participants = ids.map((userId) => ({ userId }));
      }
      return ok({ lobbyName, callId, sessionId: session?.sessionId ?? null, startedBy: session?.startedBy ?? participants[0]?.userId ?? null, participants, chatChannel: chatChannelForLobby(lobbyName), documentId: documentIdForLobby(lobbyName) });
    },
  };
}

function persistHandler(): DescribedStepHandler {
  return {
    kind: KINDS.persist,
    description:
      'Writes the recording row: reconciles the participant list against the media tracks, derives the true media duration, and stores the segments, destination and thumbnail.',
    reads: 'The webhook detail, plus the callId and participants the resolve step worked out.',
    emits:
      'startedAt, endedAt, mediaDurationMs and participantCount.',
    async execute(_n: PipelineNode, ctx: Record<string, unknown>): Promise<StepOutcome> {
      const detail = (ctx.detail ?? {}) as Record<string, any>;
      const arn = str(ctx.arn)!; const recordingId = str(ctx.recordingId)!;
      const rawTracks: TruthTrack[] = Array.isArray(detail.tracks) ? detail.tracks : [];
      const sessionDurationMs = Number(detail.durationMs ?? 0);
      const startedAt: string = detail.startedAt ?? new Date(Date.now() - sessionDurationMs).toISOString();
      const endedAt: string = detail.endedAt ?? new Date(Date.parse(startedAt) + sessionDurationMs).toISOString();
      const startedBy = str(ctx.startedBy);
      const reconciled = reconcileParticipants({ participants: (ctx.participants as CallRecordingParticipant[]) ?? [], tracks: rawTracks, startedBy, startedAt, endedAt });
      const mediaDurationMs = deriveMediaDurationMs({ tracks: rawTracks, sessionDurationMs });
      await callRecordingRepo.putRecording({
        callId: str(ctx.callId)!, recordingId, channelArn: arn, lobbyName: str(ctx.lobbyName) ?? '', sessionId: str(ctx.sessionId),
        startedAt, endedAt, durationMs: mediaDurationMs, sessionDurationMs,
        totalBytes: Number(detail.totalSize ?? detail.totalBytes ?? 0),
        segments: Array.isArray(detail.segments) ? detail.segments : [],
        destination: detail.destination === 's3' ? 's3' : 'local',
        participants: reconciled.participants, participantCount: reconciled.participantCount, startedBy,
        thumbnailUrl: str(detail.thumbnailUrl), composedUrl: str(detail.composedUrl), tracks: rawTracks.length ? (rawTracks as unknown as CallRecording['tracks']) : undefined,
      });
      return ok({ startedAt, endedAt, mediaDurationMs, participantCount: reconciled.participantCount });
    },
  };
}

/** One attempt per execution; the node's retryPolicy turns "still composing" into a wait. */
function awaitComposeHandler(): DescribedStepHandler {
  return {
    kind: KINDS.awaitCompose,
    description:
      'Asks LVS whether the composed file is ready yet. Fails while composing is still running, so the node\'s retry policy turns this into a wait rather than an error.',
    reads: 'arn, recordingId and callId.',
    emits:
      'composeStatus, and the composed file\'s key and size once there is one.',
    async execute(_n: PipelineNode, ctx: Record<string, unknown>): Promise<StepOutcome> {
      const arn = str(ctx.arn)!; const recordingId = str(ctx.recordingId)!; const callId = str(ctx.callId)!;
      const res = await callSfu(`${VIDEO_API_URL}/api/channels/${encodeURIComponent(arn)}/recordings`, { method: 'GET', headers: lvsAuth() });
      if (!res.ok) return fail(`LVS listed recordings with ${res.status}`);
      const body = (await res.json()) as { recordings?: Array<Record<string, unknown>> } | Array<Record<string, unknown>>;
      const rows = Array.isArray(body) ? body : (body.recordings ?? []);
      const live = rows.find((r) => r?.recordingId === recordingId);
      const composeStatus = (live?.composeStatus as ComposeStatus | undefined) ?? 'queued';
      if (!SETTLED_COMPOSE.has(composeStatus)) return fail(`compose is still ${composeStatus}`);
      const seg = live?.composedSegment as { key?: string; url?: string; size?: number } | undefined;
      const composedSegment = seg ? { key: seg.key ?? seg.url, size: seg.size } : undefined;
      await callRecordingRepo.updateComposeState(callId, recordingId, { composeStatus, ...(composedSegment ? { composedSegment } : {}) });
      return ok({ composeStatus, composedKey: composedSegment?.key ?? null, composedBytes: composedSegment?.size ?? null });
    },
  };
}

/** "12m 4s" — the length as the card's corner badge wants it. */
function formatDurationMs(ms: number): string {
  const total = Math.round(ms / 1000);
  const h = Math.floor(total / 3600); const m = Math.floor((total % 3600) / 60); const s = total % 60;
  if (h > 0) return m > 0 ? `${h}h ${m}m` : `${h}h`;
  if (m > 0) return s > 0 ? `${m}m ${s}s` : `${m}m`;
  return `${s}s`;
}

/** LVS hands the thumbnail back as a path on its own API; the card needs a URL a browser can fetch. */
function absoluteMediaUrl(u: string | undefined): string | undefined {
  if (!u) return undefined;
  if (/^https?:\/\//i.test(u)) return u;
  if (u.startsWith('/')) return VIDEO_PUBLIC_URL ? `${VIDEO_PUBLIC_URL}${u}` : u;
  return undefined;
}

/**
 * The recording as a card in the conversation — the artifact itself, the way
 * Teams posts it: thumbnail, length, who it is by. `metadata.kind:
 * 'recording'` is what ChatPanel renders as a RecordingActivityCard. Posted
 * whether compose succeeded or not: a failed recording is still a fact the
 * room should see, and the card says so rather than staying silent.
 */
export function recordingCardMetadata(ctx: Record<string, unknown>, runId: string): { message: string; metadata: Record<string, unknown> } {
  const durationMs = Number(ctx.mediaDurationMs ?? 0);
  const detail = (ctx.detail ?? {}) as Record<string, unknown>;
  const participants = (ctx.participants as Array<{ userId?: string; displayName?: string }> | undefined) ?? [];
  const startedBy = str(ctx.startedBy);
  const recordedBy = participants.find((p) => p.userId === startedBy)?.displayName ?? startedBy;
  // Posted the moment the recording is saved — before compose, before the
  // transcript — the way Teams drops the chiclet in while it says "Saving".
  // The card reads its LIVE stage from GET /api/recordings/:id, so what is
  // stamped here is only what a client with no API would still want to show.
  const status = ctx.composeStatus === undefined ? 'processing' : ctx.composeStatus === 'ok' || ctx.composeStatus === 'degraded' ? 'ready' : 'failed';
  const metadata: Record<string, unknown> = {
    kind: 'recording',
    recordingId: ctx.recordingId,
    callId: ctx.callId,
    lobbyName: ctx.lobbyName ?? null,
    startedAt: ctx.startedAt ?? null,
    durationMs,
    participantCount: Number(ctx.participantCount ?? participants.length) || undefined,
    participants: participants.slice(0, 12).map((p) => ({ userId: p.userId, displayName: p.displayName })),
    status,
    stage: status === 'processing' ? 'saving' : status,
    runId,
    pipelineId: RECORDING_FINALIZE_PIPELINE_ID,
  };
  if (recordedBy) metadata.recordedBy = recordedBy;
  const thumbnailUrl = absoluteMediaUrl(str(detail.thumbnailUrl));
  if (thumbnailUrl) metadata.thumbnailUrl = thumbnailUrl;
  const message = status === 'failed'
    ? `Recording could not be processed (compose ${String(ctx.composeStatus)})`
    : `Recording${durationMs > 0 ? ` — ${formatDurationMs(durationMs)}` : ''}`;
  return { message, metadata };
}

function announceHandler(): DescribedStepHandler {
  return {
    kind: KINDS.announce,
    description:
      'Posts the recording card into the lobby\'s conversation. A lobby with no conversation is not a failure — it reports that nothing was posted and the run carries on.',
    reads: 'lobbyName, and the recording details the card shows.',
    emits:
      'Whether a card was posted, and to which channel.',
    async execute(_n: PipelineNode, ctx: Record<string, unknown>, helpers): Promise<StepOutcome> {
      const channel = chatChannelForLobby(str(ctx.lobbyName));
      if (!channel) {
        log.info({ lobbyName: ctx.lobbyName, runId: helpers.runId, reason: 'lobby has no conversation' }, '[recording-pipeline] recording card not posted');
        return ok({ posted: false, channel: null });
      }
      const card = recordingCardMetadata(ctx, helpers.runId);
      try {
        await postChatMessage({ channel, ...card });
        return ok({ posted: true, channel });
      } catch (e) {
        log.warn({ err: e instanceof Error ? e.message : String(e), channel }, '[recording-pipeline] could not post the recording card');
        return ok({ posted: false, channel });
      }
    },
  };
}

export interface RecordingPipelineDeps {
  /** How a child run is started — injectable so tests can record instead of execute. */
  startRun?: typeof startSystemRun;
}

function startTranscriptionHandler(deps: RecordingPipelineDeps = {}): DescribedStepHandler {
  const startRun = deps.startRun ?? startSystemRun;
  return {
    kind: KINDS.startTranscription,
    description:
      'Decides whether this recording should be transcribed at all — from the recording profile, or TRANSCRIBE_EVERY_RECORDING — and starts the call-transcription pipeline as a child run.',
    reads: 'The recording profile in the webhook detail, and composeStatus.',
    emits:
      'Whether transcription started, and the child run\'s id. On failure the row records why, because "pending" forever is the one answer nobody can act on.',
    async execute(_n: PipelineNode, ctx: Record<string, unknown>): Promise<StepOutcome> {
      const detail = (ctx.detail ?? {}) as Record<string, any>;
      const webhookSaysTranscribe = detail.transcribe === true || detail.transcribe === 'true';
      const profile = resolveRecordingProfile({ recordingProfile: str(detail.recordingProfile), transcribe: webhookSaysTranscribe, composePerPeer: detail.composePerPeer === true || detail.composePerPeer === 'true', overrides: webhookSaysTranscribe ? { transcribe: true } : undefined });
      const always = process.env.TRANSCRIBE_EVERY_RECORDING === 'true';
      if (!profile.transcribe && !always) return ok({ started: false, reason: 'profile does not transcribe' });
      if (ctx.composeStatus !== 'ok' && ctx.composeStatus !== 'degraded') return ok({ started: false, reason: `nothing to transcribe (compose ${String(ctx.composeStatus)})` });
      const callId = str(ctx.callId)!; const recordingId = str(ctx.recordingId)!;
      await callRecordingRepo.updateTranscriptStatus(callId, recordingId, { status: 'pending' });
      try {
        const runId = await startRun(CALL_TRANSCRIPTION_PIPELINE_ID, buildCallTranscriptionDefinition(), {
          recordingId, callId, arn: ctx.arn, lobbyName: ctx.lobbyName, composedKey: ctx.composedKey, startedAt: ctx.startedAt, documentId: ctx.documentId ?? null,
        }, 'system');
        return ok({ started: true, transcriptionRunId: runId });
      } catch (err) {
        // The row must say why there is no transcript; "pending" forever is the
        // one answer nobody can act on.
        const reason = err instanceof Error ? err.message : String(err);
        await callRecordingRepo.updateTranscriptStatus(callId, recordingId, { status: 'failed', transcriptError: `enqueue failed: ${reason}` }).catch(() => undefined);
        return fail(`could not start transcription: ${reason}`);
      }
    },
  };
}

// ---------------------------------------------------------------------------
// call-transcription steps
// ---------------------------------------------------------------------------

export interface TranscriptSegment { startMs: number; endMs: number; speakerId: string; speakerLabel: string; text: string }

/** Run the local speech engine on a file. Whisper JSON is `{ text }`; lines become segments spread over the media length. */
/**
 * Whisper never returns nothing: on near-silence it hallucinates a stock
 * phrase ("you", "Thank you.", "so so I'm", "Thanks for watching"). A
 * recording seconds long that yields a single stock phrase — or one or two
 * words for ten-plus seconds of audio — is treated as no speech, so the
 * conversation gets "No speech was found" rather than a transcript reading "you".
 */
const WHISPER_SILENCE_PHRASES = new Set(['you', 'thank you', 'thanks', 'thank you for watching', 'thanks for watching', 'bye', 'so so im', 'the', 'and', 'uh', 'um', 'hmm', 'okay', 'oh', 'yeah', 'subtitles by the amaraorg community', 'thank you for listening', 'thank you very much']);
export function looksLikeSilence(text: string, durationMs: number): boolean {
  const norm = text.toLowerCase().replace(/[^a-z0-9\s]/g, '').replace(/\s+/g, ' ').trim();
  if (!norm) return true;
  if (WHISPER_SILENCE_PHRASES.has(norm)) return true;
  const tokens = norm.split(' ');
  const words = tokens.length;
  // Two words across ten seconds is not a conversation.
  if (durationMs >= 10_000 && words <= 2) return true;
  // "you you you you you" — one or two tokens repeated is the engine looping on
  // noise, however many of them there are.
  const distinct = new Set(tokens).size;
  if (words >= 3 && distinct <= 2) return true;
  const counts = new Map<string, number>();
  for (const t of tokens) counts.set(t, (counts.get(t) ?? 0) + 1);
  const top = Math.max(...counts.values());
  return words >= 6 && top / words >= 0.6;
}

export async function transcribeFile(file: string, durationMs: number): Promise<TranscriptSegment[]> {
  const bin = process.env.SPEECH_CLI ?? '/Applications/OpenSuperWhisper.app/Contents/MacOS/opensuperwhisper';
  const out = await new Promise<string>((resolve, reject) => {
    execFile(bin, ['transcribe', file, '--json'], { maxBuffer: 16 * 1024 * 1024, timeout: Number(process.env.SPEECH_TIMEOUT_MS) || 600_000 }, (err, stdout, stderr) => {
      if (err) reject(new Error(`speech engine failed: ${err.message}${stderr ? ` — ${String(stderr).slice(0, 200)}` : ''}`));
      else resolve(String(stdout));
    });
  });
  const start = out.indexOf('{'); const end = out.lastIndexOf('}');
  const parsed = JSON.parse(out.slice(start, end + 1)) as { text?: string };
  const lines = String(parsed.text ?? '').split('\n').map((l) => l.trim()).filter(Boolean);
  if (lines.length === 0) return [];
  const per = Math.max(1, Math.floor(durationMs / lines.length));
  return lines.map((text, i) => ({ startMs: i * per, endMs: Math.min(durationMs, (i + 1) * per), speakerId: 'call', speakerLabel: 'Call', text }));
}

function transcribeHandler(): DescribedStepHandler {
  return {
    kind: KINDS.transcribe,
    description:
      'Downloads the composed recording from LVS and runs it through the transcription engine. A stock phrase returned for silence is discarded rather than stored as speech.',
    reads: 'arn, recordingId, callId and the composed file\'s key.',
    emits:
      'The transcript segments and a word count; an empty segment list when there was no speech.',
    async execute(_n: PipelineNode, ctx: Record<string, unknown>): Promise<StepOutcome> {
      const arn = str(ctx.arn); const recordingId = str(ctx.recordingId); const callId = str(ctx.callId);
      if (!arn || !recordingId || !callId) return fail('arn, recordingId and callId are required');
      const rec = await callRecordingRepo.getRecording(callId, recordingId);
      const key = str(ctx.composedKey) ?? rec?.composedSegment?.key;
      if (!key) return fail('no composed file to transcribe yet');
      const filename = key.split('/').pop()!;
      await callRecordingRepo.updateTranscriptStatus(callId, recordingId, { status: 'processing' });
      const res = await callSfu(`${VIDEO_API_URL}/api/channels/${encodeURIComponent(arn)}/recordings/${encodeURIComponent(recordingId)}/segments/${encodeURIComponent(filename)}`, { method: 'GET', headers: lvsAuth() });
      if (!res.ok) return fail(`LVS served the composed file with ${res.status}`);
      const dir = await fs.mkdtemp(join(tmpdir(), 'transcribe-'));
      const file = join(dir, filename);
      await fs.writeFile(file, Buffer.from(await res.arrayBuffer()));
      try {
        const durationMs = rec?.durationMs ?? 0;
        const heard = await transcribeFile(file, durationMs);
        const text = heard.map((s) => s.text).join(' ');
        if (looksLikeSilence(text, durationMs)) {
          return ok({ segments: [], words: 0, discarded: text.slice(0, 80), reason: 'no speech — the engine returned a stock phrase for silence' });
        }
        return ok({ segments: heard, words: heard.reduce((n, s) => n + s.text.split(/\s+/).filter(Boolean).length, 0) });
      } finally {
        await fs.rm(dir, { recursive: true, force: true }).catch(() => undefined);
      }
    },
  };
}

function mergeReactionsHandler(): DescribedStepHandler {
  return {
    kind: KINDS.mergeReactions,
    description:
      'Folds the emoji reactions people left during the call into the transcript\'s timeline, so a reaction sits next to what was being said.',
    reads: 'The transcript segments and the lobby the call ran in.',
    emits:
      'The merged transcript and how many reactions went into it.',
    async execute(_n: PipelineNode, ctx: Record<string, unknown>): Promise<StepOutcome> {
      const segments = (ctx.segments as TranscriptSegment[]) ?? [];
      const lobbyName = str(ctx.lobbyName); const startedAt = str(ctx.startedAt) ?? new Date().toISOString();
      let reactions: Array<Record<string, unknown>> = [];
      if (lobbyName) {
        try { reactions = (await (callReactionRepo as unknown as { listByLobby: (l: string, o?: unknown) => Promise<unknown> }).listByLobby(lobbyName)) as Array<Record<string, unknown>> ?? []; } catch { reactions = []; }
        if (!Array.isArray(reactions)) reactions = ((reactions as unknown as { items?: Array<Record<string, unknown>> }).items) ?? [];
      }
      const merged = mergeReactionsIntoTranscript({ segments }, reactions as never, startedAt);
      return ok({ transcript: merged, reactions: reactions.length });
    },
  };
}

function storeHandler(): DescribedStepHandler {
  return {
    kind: KINDS.store,
    description:
      'Stores the transcript on the recording row and prepares the document operations that write it into a page. Refuses a transcript over 350KB rather than truncating one.',
    reads: 'callId, recordingId and the merged transcript.',
    emits:
      'How it was stored, the byte and segment counts, the transcript as text, whether any speech was found, and the document operations for the apply step.',
    async execute(_n: PipelineNode, ctx: Record<string, unknown>): Promise<StepOutcome> {
      const callId = str(ctx.callId)!; const recordingId = str(ctx.recordingId)!;
      const transcript = (ctx.transcript ?? { segments: ctx.segments ?? [] }) as { segments: TranscriptSegment[] };
      const json = JSON.stringify(transcript);
      if (json.length > 350 * 1024) return fail(`transcript too large to store inline (${json.length} bytes)`);
      // The summary is the next two steps' business; say it is underway so a
      // reader can tell "not yet" from "this row predates the summary".
      await callRecordingRepo.updateTranscriptStatus(callId, recordingId, { status: 'ready', transcriptKey: 'inline', transcriptJson: json, summaryStatus: 'pending' } as never);
      const text = transcriptText(transcript.segments);
      // `transcriptOps` is what the shared document:apply step writes; the
      // store-summary step puts the summary in front of it when there is one.
      return ok({ stored: 'inline', bytes: json.length, segments: transcript.segments.length, transcriptText: text, hasSpeech: text.length > 0, transcriptOps: transcriptOps(transcript.segments) });
    },
  };
}

/** Persist the summarising model's answer on the row; for a silent recording, record that there was nothing to summarise without reading the model. */
function storeSummaryHandler(): DescribedStepHandler {
  return {
    kind: KINDS.storeSummary,
    description:
      'Persists the summarising model\'s answer against the recording. A silent recording is recorded as skipped without reading the model at all.',
    reads: 'hasSpeech, and the summarize step\'s answer.',
    emits:
      'The summary status and length, and the document operations with the summary placed above the transcript.',
    async execute(_n: PipelineNode, ctx: Record<string, unknown>): Promise<StepOutcome> {
      const callId = str(ctx.callId); const recordingId = str(ctx.recordingId);
      if (!callId || !recordingId) return fail('callId and recordingId are required');
      const now = new Date().toISOString();
      if (ctx.hasSpeech !== true) {
        await callRecordingRepo.updateTranscriptStatus(callId, recordingId, { status: 'ready', summaryStatus: 'skipped', summaryError: null, transcriptGeneratedAt: now });
        return ok({ summaryStatus: 'skipped', summaryChars: 0 });
      }
      const answer = (ctx.steps as Record<string, { text?: unknown }> | undefined)?.summarize?.text;
      const summaryMarkdown = stripFences(typeof answer === 'string' ? answer : '');
      if (!summaryMarkdown || summaryMarkdown === '(no speech)') {
        // The transcript is stored and stays ready; only the notes over it are
        // missing, and the row says so on its own axis.
        const summaryError = summaryMarkdown ? 'the model found no speech in a transcript that has some' : 'the model returned nothing';
        await callRecordingRepo.updateTranscriptStatus(callId, recordingId, { status: 'ready', summaryStatus: 'failed', summaryError, transcriptGeneratedAt: now });
        return ok({ summaryStatus: 'failed', summaryChars: 0, summaryError });
      }
      await callRecordingRepo.updateTranscriptStatus(callId, recordingId, { status: 'ready', summaryStatus: 'ready', summaryMarkdown, summaryError: null, transcriptGeneratedAt: now });
      const existing = Array.isArray(ctx.transcriptOps) ? (ctx.transcriptOps as Array<Record<string, unknown>>) : [];
      return ok({ summaryStatus: 'ready', summaryChars: summaryMarkdown.length, transcriptOps: [...summaryOps(summaryMarkdown), ...existing] });
    },
  };
}

function publishHandler(): DescribedStepHandler {
  return {
    kind: KINDS.publish,
    description:
      'Posts the finished transcript back to the lobby as a run card, with a word count and the opening line as a preview.',
    reads: 'The merged transcript, lobbyName, recordingId and callId.',
    emits:
      'Whether the card was posted.',
    async execute(_n: PipelineNode, ctx: Record<string, unknown>, helpers): Promise<StepOutcome> {
      const transcript = (ctx.transcript ?? { segments: [] }) as { segments: TranscriptSegment[] };
      const words = transcript.segments.reduce((n, s) => n + s.text.split(/\s+/).filter(Boolean).length, 0);
      const preview = transcript.segments.map((s) => s.text).join(' ').slice(0, 160);
      const posted = await postRunCard({
        lobbyName: str(ctx.lobbyName) ?? null, runId: helpers.runId, pipelineId: CALL_TRANSCRIPTION_PIPELINE_ID,
        command: 'transcript', instruction: words ? `${words} words — “${preview}${preview.length >= 160 ? '…' : ''}”` : 'No speech was found in the recording',
        extra: { recordingId: ctx.recordingId, callId: ctx.callId },
      });
      // The transcript reached the call's document (when there is one) in the
      // document:apply step before this one — the same write path /doc uses.
      const applied = ((ctx.steps as Record<string, { applied?: number }> | undefined)?.apply?.applied) ?? 0;
      return ok({ posted, appended: applied, words });
    },
  };
}

export function buildRecordingPipelineHandlers(deps: RecordingPipelineDeps = {}): Map<string, StepHandler> {
  const m = new Map<string, StepHandler>();
  for (const h of [resolveHandler(), persistHandler(), awaitComposeHandler(), announceHandler(), startTranscriptionHandler(deps), transcribeHandler(), mergeReactionsHandler(), storeHandler(), storeSummaryHandler(), publishHandler()]) m.set(h.kind, h);
  return m;
}
