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
//   call-transcription:  trigger → transcribe → merge-reactions → store → publish
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
import { AgentCollaborator } from '../documents/AgentCollaborator';
import { applyOps } from '../documents/documentAgentOps';
import { resolveRecordingProfile } from '@connorhoehn/event-catalog';
import log from '../lib/logger';

export const RECORDING_FINALIZE_PIPELINE_ID = 'recording-finalize';
export const CALL_TRANSCRIPTION_PIPELINE_ID = 'call-transcription';

export const KINDS = {
  resolve: 'recording:resolve',
  persist: 'recording:persist',
  awaitCompose: 'recording:await-compose',
  announce: 'recording:announce',
  startTranscription: 'transcription:start',
  transcribe: 'transcription:transcribe',
  mergeReactions: 'transcript:merge-reactions',
  store: 'transcript:store',
  publish: 'transcript:publish',
} as const;

const SETTLED_COMPOSE: ReadonlySet<string> = new Set(['ok', 'degraded', 'failed', 'skipped']);
const lvsAuth = () => ({ Authorization: `Bearer ${process.env.LVS_ADMIN_TOKEN ?? process.env.VIDEO_ADMIN_TOKEN ?? 'admin'}` });

/** A recording's home in chat: DM lobbies and rooms have one; a bare lobby does not. */
export function chatChannelForLobby(lobby: string | null | undefined): string | null {
  if (!lobby) return null;
  if (lobby.startsWith('dmg:') || lobby.startsWith('dm:')) return `chat:${lobby}`;
  if (lobby.startsWith('room:')) return lobby;
  return null;
}

/** A call started from a document page uses the document id as its lobby. */
export function documentIdForLobby(lobby: string | null | undefined): string | null {
  if (!lobby) return null;
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(lobby) ? lobby : null;
}

const pos = (x: number) => ({ x, y: 0 });
const edge = (a: string, b: string) => ({ id: `${a}->${b}`, source: a, sourceHandle: 'out', target: b, targetHandle: 'in' });

export function buildRecordingFinalizeDefinition(): Record<string, unknown> {
  return {
    id: RECORDING_FINALIZE_PIPELINE_ID,
    name: 'Recording finalize',
    description: 'After a call ends: find the call, save the recording, wait for the composed file, tell the conversation, start transcription.',
    version: 1, status: 'published', publishedVersion: 1,
    nodes: [
      { id: 'trigger', type: 'trigger', position: pos(0), data: { type: 'trigger', triggerType: 'webhook', label: 'LVS recording.completed' } },
      { id: 'resolve', type: KINDS.resolve, position: pos(200), data: { type: KINDS.resolve, label: 'Find the call' } },
      { id: 'persist', type: KINDS.persist, position: pos(400), data: { type: KINDS.persist, label: 'Save the recording' } },
      { id: 'await-compose', type: KINDS.awaitCompose, position: pos(600), data: { type: KINDS.awaitCompose, label: 'Wait for the composed file', retryPolicy: { maxAttempts: 12, backoffMs: 5000, backoffMultiplier: 1.2, retryUnknown: true } } },
      { id: 'announce', type: KINDS.announce, position: pos(800), data: { type: KINDS.announce, label: 'Post to the conversation' } },
      { id: 'start-transcription', type: KINDS.startTranscription, position: pos(1000), data: { type: KINDS.startTranscription, label: 'Start transcription' } },
    ],
    edges: [edge('trigger', 'resolve'), edge('resolve', 'persist'), edge('persist', 'await-compose'), edge('await-compose', 'announce'), edge('announce', 'start-transcription')],
  };
}

export function buildCallTranscriptionDefinition(): Record<string, unknown> {
  return {
    id: CALL_TRANSCRIPTION_PIPELINE_ID,
    name: 'Call transcription',
    description: 'Transcribe the composed recording, merge in-call reactions by time, store the transcript, publish it to the conversation and the call\'s document.',
    version: 1, status: 'published', publishedVersion: 1,
    nodes: [
      { id: 'trigger', type: 'trigger', position: pos(0), data: { type: 'trigger', triggerType: 'manual', label: 'Recording ready' } },
      { id: 'transcribe', type: KINDS.transcribe, position: pos(200), data: { type: KINDS.transcribe, label: 'Transcribe the audio', retryPolicy: { maxAttempts: 3, backoffMs: 3000, backoffMultiplier: 2, retryUnknown: true } } },
      { id: 'merge-reactions', type: KINDS.mergeReactions, position: pos(400), data: { type: KINDS.mergeReactions, label: 'Merge reactions' } },
      { id: 'store', type: KINDS.store, position: pos(600), data: { type: KINDS.store, label: 'Store the transcript' } },
      { id: 'publish', type: KINDS.publish, position: pos(800), data: { type: KINDS.publish, label: 'Publish to the conversation' } },
    ],
    edges: [edge('trigger', 'transcribe'), edge('transcribe', 'merge-reactions'), edge('merge-reactions', 'store'), edge('store', 'publish')],
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
  if (!channel) return false;
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

function resolveHandler(): StepHandler {
  return {
    kind: KINDS.resolve,
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

function persistHandler(): StepHandler {
  return {
    kind: KINDS.persist,
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
function awaitComposeHandler(): StepHandler {
  return {
    kind: KINDS.awaitCompose,
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

function announceHandler(): StepHandler {
  return {
    kind: KINDS.announce,
    async execute(_n: PipelineNode, ctx: Record<string, unknown>, helpers): Promise<StepOutcome> {
      const mins = Math.round(Number(ctx.mediaDurationMs ?? 0) / 60000);
      const posted = await postRunCard({
        lobbyName: str(ctx.lobbyName) ?? null, runId: helpers.runId, pipelineId: RECORDING_FINALIZE_PIPELINE_ID,
        command: 'recording', instruction: `Call recording${mins ? ` (${mins} min)` : ''} — ${ctx.composeStatus === 'ok' ? 'ready to watch' : `compose ${String(ctx.composeStatus)}`}`,
        extra: { recordingId: ctx.recordingId, callId: ctx.callId },
      });
      return ok({ posted, channel: chatChannelForLobby(str(ctx.lobbyName)) });
    },
  };
}

export interface RecordingPipelineDeps {
  /** How a child run is started — injectable so tests can record instead of execute. */
  startRun?: typeof startSystemRun;
}

function startTranscriptionHandler(deps: RecordingPipelineDeps = {}): StepHandler {
  const startRun = deps.startRun ?? startSystemRun;
  return {
    kind: KINDS.startTranscription,
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

function transcribeHandler(): StepHandler {
  return {
    kind: KINDS.transcribe,
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
        const segments = await transcribeFile(file, rec?.durationMs ?? 0);
        return ok({ segments, words: segments.reduce((n, s) => n + s.text.split(/\s+/).filter(Boolean).length, 0) });
      } finally {
        await fs.rm(dir, { recursive: true, force: true }).catch(() => undefined);
      }
    },
  };
}

function mergeReactionsHandler(): StepHandler {
  return {
    kind: KINDS.mergeReactions,
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

function storeHandler(): StepHandler {
  return {
    kind: KINDS.store,
    async execute(_n: PipelineNode, ctx: Record<string, unknown>): Promise<StepOutcome> {
      const callId = str(ctx.callId)!; const recordingId = str(ctx.recordingId)!;
      const transcript = (ctx.transcript ?? { segments: ctx.segments ?? [] }) as { segments: TranscriptSegment[] };
      const json = JSON.stringify(transcript);
      if (json.length > 350 * 1024) return fail(`transcript too large to store inline (${json.length} bytes)`);
      await callRecordingRepo.updateTranscriptStatus(callId, recordingId, { status: 'ready', transcriptKey: 'inline', transcriptJson: json } as never);
      return ok({ stored: 'inline', bytes: json.length, segments: transcript.segments.length });
    },
  };
}

function publishHandler(): StepHandler {
  return {
    kind: KINDS.publish,
    async execute(_n: PipelineNode, ctx: Record<string, unknown>, helpers): Promise<StepOutcome> {
      const transcript = (ctx.transcript ?? { segments: [] }) as { segments: TranscriptSegment[] };
      const words = transcript.segments.reduce((n, s) => n + s.text.split(/\s+/).filter(Boolean).length, 0);
      const preview = transcript.segments.map((s) => s.text).join(' ').slice(0, 160);
      const posted = await postRunCard({
        lobbyName: str(ctx.lobbyName) ?? null, runId: helpers.runId, pipelineId: CALL_TRANSCRIPTION_PIPELINE_ID,
        command: 'transcript', instruction: words ? `${words} words — “${preview}${preview.length >= 160 ? '…' : ''}”` : 'No speech was found in the recording',
        extra: { recordingId: ctx.recordingId, callId: ctx.callId },
      });
      // A call started from a document gets its transcript appended there, by
      // the same collaborator that applies every other document side effect.
      let appended = 0;
      const documentId = str(ctx.documentId);
      if (documentId && transcript.segments.length > 0) {
        const agent = new AgentCollaborator({ documentId, identity: { userId: PIPELINE_IDENTITY.userId, displayName: 'Transcript', color: '#0ea5e9' } });
        try {
          await agent.join(); agent.announce();
          const r = applyOps(agent.doc, [
            { op: 'appendBlock', kind: 'heading', level: 2, text: 'Call transcript' },
            ...transcript.segments.map((s) => ({ op: 'appendBlock' as const, kind: 'paragraph' as const, text: s.text })),
          ], 'agent');
          appended = r.applied;
          void agent.leave(2000).catch(() => undefined);
        } catch (e) {
          await agent.leave(0).catch(() => undefined);
          log.warn({ err: e instanceof Error ? e.message : String(e), documentId }, '[transcription] could not append the transcript to the document');
        }
      }
      return ok({ posted, appended, words });
    },
  };
}

export function buildRecordingPipelineHandlers(deps: RecordingPipelineDeps = {}): Map<string, StepHandler> {
  const m = new Map<string, StepHandler>();
  for (const h of [resolveHandler(), persistHandler(), awaitComposeHandler(), announceHandler(), startTranscriptionHandler(deps), transcribeHandler(), mergeReactionsHandler(), storeHandler(), publishHandler()]) m.set(h.kind, h);
  return m;
}
