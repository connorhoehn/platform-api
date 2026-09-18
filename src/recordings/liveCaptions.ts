// Live captions as a PIPELINE RUN per call.
//
// The caption DATA PLANE is unchanged: LVS mirrors each participant's audio to
// the live-captions sidecar through a `webhook-pcm` tap, the sidecar publishes
// lines on Redis `captions:<arn>`, the gateway's caption-relay fans them onto
// the WS channel the call's browsers subscribe to. What changes is who owns
// the tap's LIFECYCLE: it used to be a route (`POST /api/video/captions`) with
// tap ids in a process-local Map — so a platform-api restart forgot every
// tap, and nothing showed that a call was being captioned at all. Now the
// route starts a run and the run owns the tap:
//
//   live-captions:  trigger → arm → stream → disarm → publish
//
//   arm      creates (or finds — idempotent) the tap and outputs its ids
//   stream   subscribes to the caption channel and stays in flight for the
//            WHOLE call, counting final lines and words; it resolves when the
//            session row says `ended` (the same write every end path makes —
//            hang-up, the sendBeacon fallback, the stale reaper), when the
//            toggle asks it to stop, or at LIVE_CAPTIONS_MAX_MS. Executor
//            cancellation is honoured too, and then the step removes the tap
//            itself because no later step will run.
//   disarm   removes the taps arm created (plus any pointing at our sink)
//   publish  leaves an agent-run card in the call's conversation
//
// Turning captions OFF asks the stream step to stop rather than cancelling the
// run: the run then finishes through disarm and publish and shows `completed`
// with a card, which is what a normal toggle-off is. `POST /api/pipelines/
// :runId/cancel` still works and still removes the tap.
//
// The pipeline is visible in /pipelines like every other run. The executor
// heartbeats a non-LLM step every 10s on its own (`pipeline:step:heartbeat`),
// so a four-hour stream step is never "stuck"; the live line/word counts are
// kept here and overlaid onto the run detail as `steps.stream.progress`.
import type { StepHandler, StepOutcome, PipelineNode, StepHandlerHelpers } from 'distributed-core/applications/pipeline';
import { getRedisClient } from '../lib/redis-client';
import { VIDEO_API_URL } from '../lib/video-auth';
import { videoSessionRepo } from '../repositories';
import { getPipelineBridge } from '../pipeline/bridge-types';
import { postRunCardToChannel } from '../documents/chatRunCard';
import { startSystemRun } from './recordingPipelines';
import { chatChannelForLobby } from './lobbyChannel';
import type { DescribedStepHandler } from '../pipeline/customStepHandlers';
import log from '../lib/logger';

export const LIVE_CAPTIONS_PIPELINE_ID = 'live-captions';

export const CAPTION_KINDS = {
  arm: 'captions:arm',
  stream: 'captions:stream',
  disarm: 'captions:disarm',
  publish: 'captions:publish',
} as const;

/** Where LVS posts the PCM: the live-captions sidecar. */
export function captionsSinkUrl(): string {
  return process.env.LIVE_CAPTIONS_SINK_URL || 'http://localhost:3915/pcm';
}

const DEFAULT_MAX_MS = 4 * 60 * 60 * 1000;
/** How long a call may be captioned before the run ends it on its own. */
export function liveCaptionsMaxMs(): number {
  const n = Number(process.env.LIVE_CAPTIONS_MAX_MS);
  return Number.isFinite(n) && n > 0 ? n : DEFAULT_MAX_MS;
}

const lvsAuth = () => ({ Authorization: `Bearer ${process.env.VIDEO_ADMIN_TOKEN ?? process.env.LVS_ADMIN_TOKEN ?? 'admin'}` });
const tapsUrl = (channelArn: string) => `${VIDEO_API_URL}/api/channels/${encodeURIComponent(channelArn)}/taps`;

// ---------------------------------------------------------------------------
// The tap: arm / disarm, shared by the steps and by the legacy fallback
// ---------------------------------------------------------------------------

export interface TapDeps {
  /** Injectable so tests never reach LVS. */
  fetch?: typeof fetch;
  sinkUrl?: string;
}

interface LvsTap { tapId?: string; id?: string; destination?: { url?: string } }

/** The ids of every tap on the channel that points at OUR sink. Empty when LVS cannot list. */
export async function listCaptionTaps(channelArn: string, deps: TapDeps = {}): Promise<string[]> {
  const f = deps.fetch ?? fetch; const sink = deps.sinkUrl ?? captionsSinkUrl();
  try {
    const res = await f(tapsUrl(channelArn), { headers: lvsAuth() });
    if (!res.ok) return [];
    const body = (await res.json()) as { taps?: LvsTap[] } | LvsTap[];
    const taps = Array.isArray(body) ? body : (body.taps ?? []);
    return taps.filter((t) => t.destination?.url === sink).map((t) => t.tapId ?? t.id ?? '').filter(Boolean);
  } catch {
    return [];
  }
}

export interface ArmResult {
  tapIds: string[];
  created: boolean;
  /** Set when LVS could not tap yet because nobody in the channel publishes audio; try again once someone does. */
  waitingFor?: 'audio';
}

/**
 * Make sure the channel has a per-participant PCM tap pointed at the sidecar.
 * Idempotent: a tap that is already there is reused, so a resumed run (or a
 * second toggle) never stacks taps. Throws when LVS refuses to create one —
 * except "no audio producer", which is a tap for later (see `waitingFor`).
 */
export async function armCaptionTap(channelArn: string, deps: TapDeps = {}): Promise<ArmResult> {
  const f = deps.fetch ?? fetch; const sink = deps.sinkUrl ?? captionsSinkUrl();
  const existing = await listCaptionTaps(channelArn, deps);
  if (existing.length > 0) return { tapIds: existing, created: false };
  const res = await f(tapsUrl(channelArn), {
    method: 'POST',
    headers: { ...lvsAuth(), 'Content-Type': 'application/json' },
    body: JSON.stringify({ destination: { type: 'webhook-pcm', url: sink, perParticipant: true, chunkMs: 200 } }),
  });
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    // Captions turned on before the first audio track is up (a toggle that
    // beat the WHIP handshake, a listener-only room so far): not a failure,
    // a tap for later. The stream step re-arms on every poll until it takes.
    if (res.status === 409 && /no audio producer/i.test(text)) return { tapIds: [], created: false, waitingFor: 'audio' };
    throw new Error(`LVS tap create failed (${res.status}): ${text.slice(0, 200)}`);
  }
  const body = (await res.json()) as { tapId?: string; id?: string };
  const tapId = body.tapId ?? body.id ?? '';
  return { tapIds: tapId ? [tapId] : [], created: true };
}

export interface DisarmResult {
  /** Deleted, or already gone. */
  removed: string[];
  /**
   * Still listed by LVS but undeletable because LVS no longer knows the
   * CHANNEL (its answer is a 403 "invalid credentials" — the same it gives
   * any unknown arn). Seen after every hang-up: the channel is torn down
   * with its last participant and the tap registry outlives it. Inert — no
   * channel, no producer, nothing reaches the sidecar.
   */
  orphaned: string[];
  /** Any other refusal, by id — left for the next disarm. */
  failed: Array<{ tapId: string; status: number }>;
}

/**
 * Remove the taps a run created, and any other tap on the channel pointing at
 * our sink (a tap a forgotten process left behind). A 404 is "already gone".
 */
export async function disarmCaptionTaps(channelArn: string, tapIds: readonly string[] = [], deps: TapDeps = {}): Promise<DisarmResult> {
  const f = deps.fetch ?? fetch;
  const ids = new Set<string>([...tapIds.filter(Boolean), ...(await listCaptionTaps(channelArn, deps))]);
  const out: DisarmResult = { removed: [], orphaned: [], failed: [] };
  for (const id of ids) {
    try {
      const res = await f(`${tapsUrl(channelArn)}/${encodeURIComponent(id)}`, { method: 'DELETE', headers: lvsAuth() });
      if (res.ok || res.status === 404) out.removed.push(id);
      else if (res.status === 403) out.orphaned.push(id);
      else out.failed.push({ tapId: id, status: res.status });
    } catch {
      out.failed.push({ tapId: id, status: 0 });
    }
  }
  return out;
}

// ---------------------------------------------------------------------------
// Which call a channel belongs to
// ---------------------------------------------------------------------------

export interface CallRef { sessionId: string; lobbyName: string }

/**
 * The session behind a channel — the ACTIVE one. A room keeps its channel
 * across calls, and the arn-index Redis key the join wrote can name a row the
 * stale reaper has since ended while a newer call on the same channel is
 * live; the index is a hint the table confirms, not the answer.
 */
export async function resolveCallForArn(channelArn: string): Promise<CallRef | null> {
  let hint: CallRef | null = null;
  try {
    const redis = await getRedisClient();
    const raw = redis ? await redis.get(`video:arn-index:${channelArn}`) : null;
    if (raw) {
      const idx = JSON.parse(raw) as Partial<CallRef>;
      if (idx.sessionId && idx.lobbyName) hint = { sessionId: idx.sessionId, lobbyName: idx.lobbyName };
    }
  } catch { /* no hint */ }
  if (hint) {
    try {
      const row = await videoSessionRepo.getSession(hint.lobbyName, hint.sessionId);
      if (row?.status === 'active') return hint;
    } catch { /* cannot confirm; look at the table */ }
  }
  try {
    const rows = await videoSessionRepo.findByChannelArn(channelArn);
    const active = rows.filter((r) => r.status === 'active').sort((a, b) => String(b.startedAt ?? '').localeCompare(String(a.startedAt ?? '')))[0];
    if (active) return { sessionId: active.sessionId, lobbyName: active.documentId };
    // Nothing live: the hint (or any row) still names the call, and the stream
    // step will see it ended and finish at once — the right answer for a
    // toggle that arrived after the hang-up.
    if (hint) return hint;
    if (rows[0]) return { sessionId: rows[0].sessionId, lobbyName: rows[0].documentId };
  } catch { /* no session */ }
  return hint;
}

// ---------------------------------------------------------------------------
// Run lifecycle: start on toggle-on, stop on toggle-off
// ---------------------------------------------------------------------------

const RUN_INDEX_TTL_S = 5 * 60 * 60; // a little past the 4h max
const runKey = (channelArn: string) => `live-captions:run:${channelArn}`;
const stopKey = (runId: string) => `live-captions:stop:${runId}`;

/** In-process fast paths; Redis carries the same facts across a restart. */
const runByArn = new Map<string, string>();
const stopRequested = new Set<string>();

export interface LifecycleDeps {
  startRun?: typeof startSystemRun;
  resolveCall?: typeof resolveCallForArn;
  tap?: TapDeps;
}

export interface StartLiveCaptionsResult {
  /** Null only when the pipeline engine is unavailable and the tap was armed directly instead. */
  runId: string | null;
  pipelineId: typeof LIVE_CAPTIONS_PIPELINE_ID;
  /** True when a run for this channel was already in flight. */
  reused: boolean;
  /** Set on the engine-less fallback only. */
  tapIds?: string[];
}

async function runIsLive(runId: string): Promise<boolean> {
  const bridge = getPipelineBridge();
  const getRun = bridge?.getRun?.bind(bridge);
  if (!getRun) return true; // cannot tell; trust the index
  try {
    const snap = (await Promise.resolve(getRun(runId))) as { status?: string } | null;
    return !!snap && (snap.status === 'running' || snap.status === 'pending' || snap.status === 'awaiting_approval');
  } catch {
    return false;
  }
}

async function findRunForArn(channelArn: string): Promise<string | null> {
  const local = runByArn.get(channelArn);
  if (local) return local;
  try {
    const redis = await getRedisClient();
    return redis ? await redis.get(runKey(channelArn)) : null;
  } catch {
    return null;
  }
}

async function rememberRun(channelArn: string, runId: string): Promise<void> {
  runByArn.set(channelArn, runId);
  try {
    const redis = await getRedisClient();
    if (redis) await redis.set(runKey(channelArn), runId, { EX: RUN_INDEX_TTL_S });
  } catch { /* the in-process map still knows */ }
}

async function forgetRun(channelArn: string): Promise<void> {
  runByArn.delete(channelArn);
  try {
    const redis = await getRedisClient();
    if (redis) await redis.del(runKey(channelArn));
  } catch { /* best effort */ }
}

/**
 * Captions turned ON for a call: start the `live-captions` run (or return the
 * one already running for this channel). Without a pipeline engine the tap is
 * armed directly so captions still work on a bare process.
 */
export async function startLiveCaptions(input: { channelArn: string; userId: string }, deps: LifecycleDeps = {}): Promise<StartLiveCaptionsResult> {
  const { channelArn, userId } = input;
  const existing = await findRunForArn(channelArn);
  if (existing && await runIsLive(existing)) {
    return { runId: existing, pipelineId: LIVE_CAPTIONS_PIPELINE_ID, reused: true };
  }
  const call = await (deps.resolveCall ?? resolveCallForArn)(channelArn);
  const startRun = deps.startRun ?? startSystemRun;
  let runId: string;
  try {
    runId = await startRun(LIVE_CAPTIONS_PIPELINE_ID, buildLiveCaptionsDefinition(), {
      sessionId: call?.sessionId ?? null,
      channelArn,
      lobbyName: call?.lobbyName ?? null,
      startedBy: userId,
    }, userId);
  } catch (err) {
    // No engine (a bare dev process, a test) — captions must still turn on.
    log.warn({ err: err instanceof Error ? err.message : String(err), channelArn }, '[live-captions] no pipeline run; arming the tap directly');
    const { tapIds } = await armCaptionTap(channelArn, deps.tap);
    return { runId: null, pipelineId: LIVE_CAPTIONS_PIPELINE_ID, reused: false, tapIds };
  }
  await rememberRun(channelArn, runId);
  return { runId, pipelineId: LIVE_CAPTIONS_PIPELINE_ID, reused: false };
}

/**
 * Captions turned OFF: ask the run's stream step to stop, so the run finishes
 * through disarm and publish. When no run is known for the channel (a tap
 * armed by an older process, or the engine-less fallback) the tap is removed
 * directly — the toggle must never leave audio flowing to the sidecar.
 */
export async function stopLiveCaptions(input: { channelArn: string }, deps: LifecycleDeps = {}): Promise<{ runId: string | null; stopped: boolean; removed: string[] }> {
  const { channelArn } = input;
  const runId = await findRunForArn(channelArn);
  if (runId) {
    await requestStop(runId);
    await forgetRun(channelArn);
    return { runId, stopped: true, removed: [] };
  }
  const { removed } = await disarmCaptionTaps(channelArn, [], deps.tap);
  return { runId: null, stopped: false, removed };
}

export async function requestStop(runId: string): Promise<void> {
  stopRequested.add(runId);
  try {
    const redis = await getRedisClient();
    if (redis) await redis.set(stopKey(runId), '1', { EX: 60 * 60 });
  } catch { /* the in-process set still knows */ }
}

export async function isStopRequested(runId: string): Promise<boolean> {
  if (stopRequested.has(runId)) return true;
  try {
    const redis = await getRedisClient();
    return redis ? (await redis.exists(stopKey(runId))) > 0 : false;
  } catch {
    return false;
  }
}

async function clearStop(runId: string): Promise<void> {
  stopRequested.delete(runId);
  try {
    const redis = await getRedisClient();
    if (redis) await redis.del(stopKey(runId));
  } catch { /* the key expires on its own */ }
}

/** Test seam. */
export function __resetLiveCaptionsForTests(): void {
  runByArn.clear();
  stopRequested.clear();
  progressByRun.clear();
}

// ---------------------------------------------------------------------------
// Progress: what the stream step has heard so far, overlaid on the run detail
// ---------------------------------------------------------------------------

export interface LiveCaptionsProgress {
  stepId: string;
  lines: number;
  words: number;
  speakers: number;
  startedAt: string;
  lastLineAt: string | null;
  updatedAt: string;
}

const progressByRun = new Map<string, LiveCaptionsProgress>();

export function getLiveCaptionsProgress(runId: string): LiveCaptionsProgress | null {
  const p = progressByRun.get(runId);
  return p ? { ...p } : null;
}

// ---------------------------------------------------------------------------
// The definition
// ---------------------------------------------------------------------------

const pos = (x: number) => ({ x, y: 0 });
const edge = (a: string, b: string) => ({ id: `${a}->${b}`, source: a, sourceHandle: 'out', target: b, targetHandle: 'in' });
/** LVS, Redis, the gateway: each is retried from here. */
const IO_RETRY = { maxAttempts: 3, backoffMs: 1500, backoffMultiplier: 2, retryUnknown: true } as const;

export function buildLiveCaptionsDefinition(): Record<string, unknown> {
  return {
    id: LIVE_CAPTIONS_PIPELINE_ID,
    name: 'Live captions',
    description: 'While a call runs: arm the audio tap, stream captions to the call until it ends, remove the tap, tell the conversation.',
    version: 1, status: 'published', publishedVersion: 1,
    nodes: [
      { id: 'trigger', type: 'trigger', position: pos(0), data: { type: 'trigger', triggerType: 'platform', label: 'Captions turned on' } },
      { id: 'arm', type: CAPTION_KINDS.arm, position: pos(200), data: { type: CAPTION_KINDS.arm, label: 'Arm the audio tap', retryPolicy: IO_RETRY } },
      // One attempt is the whole call; a second only if subscribing failed outright.
      { id: 'stream', type: CAPTION_KINDS.stream, position: pos(400), data: { type: CAPTION_KINDS.stream, label: 'Stream captions while the call runs', retryPolicy: { maxAttempts: 2, backoffMs: 2000, backoffMultiplier: 1, retryUnknown: true } } },
      { id: 'disarm', type: CAPTION_KINDS.disarm, position: pos(600), data: { type: CAPTION_KINDS.disarm, label: 'Remove the audio tap', retryPolicy: IO_RETRY } },
      { id: 'publish', type: CAPTION_KINDS.publish, position: pos(800), data: { type: CAPTION_KINDS.publish, label: 'Post to the conversation', retryPolicy: IO_RETRY } },
    ],
    edges: [edge('trigger', 'arm'), edge('arm', 'stream'), edge('stream', 'disarm'), edge('disarm', 'publish')],
  };
}

// ---------------------------------------------------------------------------
// The steps
// ---------------------------------------------------------------------------

function str(v: unknown): string | undefined { return typeof v === 'string' && v ? v : undefined; }
const fail = (error: string): StepOutcome => ({ status: 'failed', error });
const ok = (output: Record<string, unknown>): StepOutcome => ({ status: 'completed', output });
const strList = (v: unknown): string[] => (Array.isArray(v) ? v.filter((x): x is string => typeof x === 'string' && !!x) : []);

/** A live subscription: resolves with the function that ends it. */
export type CaptionSubscriber = (channel: string, onMessage: (raw: string) => void) => Promise<() => Promise<void>>;

/** Subscribe on a duplicate of the shared client — a subscribing connection can do nothing else. */
export const redisCaptionSubscriber: CaptionSubscriber = async (channel, onMessage) => {
  const shared = await getRedisClient();
  if (!shared) throw new Error('Redis is unavailable — cannot subscribe to captions');
  const sub = shared.duplicate();
  await sub.connect();
  await sub.subscribe(channel, (message) => onMessage(String(message)));
  return async () => {
    try { await sub.unsubscribe(channel); } catch { /* closing anyway */ }
    try { sub.destroy(); } catch { /* already closed */ }
  };
};

export interface StreamDeps {
  subscribe?: CaptionSubscriber;
  /** True once the call is over. Default: the session row says `ended` (or is gone). */
  isSessionEnded?: (call: CallRef) => Promise<boolean>;
  isStopRequested?: (runId: string) => Promise<boolean>;
  /** Re-arm when the tap vanished under a resumed run. Default: `armCaptionTap`. */
  ensureArmed?: (channelArn: string, tapIds: string[]) => Promise<string[]>;
  disarm?: (channelArn: string, tapIds: string[]) => Promise<unknown>;
  now?: () => number;
  /** How often the end signals are checked. Default 5s. */
  pollMs?: number;
  /** How often an ARMED tap is verified. Default 60s. Unarmed, it is retried every poll. */
  verifyEveryMs?: number;
  maxMs?: () => number;
}

async function sessionEnded(call: CallRef): Promise<boolean> {
  try {
    const row = await videoSessionRepo.getSession(call.lobbyName, call.sessionId);
    return !row || row.status === 'ended';
  } catch {
    return false; // a read blip is not the call ending
  }
}

const countWords = (text: string) => text.split(/\s+/).filter(Boolean).length;

export type StreamEndReason = 'call-ended' | 'stopped' | 'max-duration' | 'cancelled';

function armHandler(tap: TapDeps = {}): DescribedStepHandler {
  return {
    kind: CAPTION_KINDS.arm,
    description:
      'Asks LVS to attach a caption tap to the channel, so spoken lines start arriving on the caption feed.',
    reads: 'channelArn.',
    emits:
      'The tap ids, whether a tap is live yet, the sink URL, and the caption channel later steps subscribe to.',
    async execute(_n: PipelineNode, ctx: Record<string, unknown>): Promise<StepOutcome> {
      const channelArn = str(ctx.channelArn);
      if (!channelArn) return fail('channelArn is required');
      const { tapIds, created, waitingFor } = await armCaptionTap(channelArn, tap);
      if (tapIds.length === 0 && !waitingFor) return fail('LVS created a tap without an id');
      return ok({ tapIds, created, armed: tapIds.length > 0, ...(waitingFor ? { waitingFor } : {}), sinkUrl: tap.sinkUrl ?? captionsSinkUrl(), captionChannel: `captions:${channelArn}` });
    },
  };
}

function streamHandler(deps: StreamDeps = {}): DescribedStepHandler {
  const subscribe = deps.subscribe ?? redisCaptionSubscriber;
  const isEnded = deps.isSessionEnded ?? sessionEnded;
  const stopAsked = deps.isStopRequested ?? isStopRequested;
  const ensureArmed = deps.ensureArmed ?? (async (arn: string) => (await armCaptionTap(arn)).tapIds);
  const disarm = deps.disarm ?? ((arn: string, ids: string[]) => disarmCaptionTaps(arn, ids));
  const now = deps.now ?? Date.now;
  const pollMs = deps.pollMs ?? 5_000;
  const verifyEveryMs = deps.verifyEveryMs ?? 60_000;
  const maxMs = deps.maxMs ?? liveCaptionsMaxMs;

  return {
    kind: CAPTION_KINDS.stream,
    description:
      'The long-running step: subscribes to the caption feed and counts lines, words and speakers until the call ends, the maximum duration is reached, someone asks it to stop, or the run is cancelled. Interim (partial) lines are ignored so a sentence is not counted twice as it is revised.',
    reads: 'channelArn, and the sessionId and lobbyName that identify the call to watch.',
    emits:
      'The line, word and speaker counts, how long it ran, and why it stopped.',
    async execute(_n: PipelineNode, ctx: Record<string, unknown>, helpers: StepHandlerHelpers): Promise<StepOutcome> {
      const channelArn = str(ctx.channelArn);
      if (!channelArn) return fail('channelArn is required');
      const sessionId = str(ctx.sessionId); const lobbyName = str(ctx.lobbyName);
      const call: CallRef | null = sessionId && lobbyName ? { sessionId, lobbyName } : null;
      let tapIds = strList(ctx.tapIds);

      const startedMs = now();
      let lines = 0; let words = 0; let lastLineAt: string | null = null;
      const speakers = new Set<string>();
      const progress: LiveCaptionsProgress = { stepId: helpers.stepId, lines: 0, words: 0, speakers: 0, startedAt: new Date(startedMs).toISOString(), lastLineAt: null, updatedAt: new Date(startedMs).toISOString() };
      progressByRun.set(helpers.runId, progress);

      const onLine = (raw: string) => {
        let msg: { text?: unknown; interim?: unknown; participantId?: unknown; at?: unknown };
        try { msg = JSON.parse(raw); } catch { return; }
        if (msg.interim === true) return;
        const text = typeof msg.text === 'string' ? msg.text.trim() : '';
        if (!text) return;
        lines += 1; words += countWords(text);
        if (typeof msg.participantId === 'string') speakers.add(msg.participantId);
        lastLineAt = typeof msg.at === 'string' ? msg.at : new Date(now()).toISOString();
        Object.assign(progress, { lines, words, speakers: speakers.size, lastLineAt, updatedAt: new Date(now()).toISOString() });
      };

      // Subscribe BEFORE the loop: a line spoken while we look at the row must count.
      const unsubscribe = await subscribe(`captions:${channelArn}`, onLine);

      let reason: StreamEndReason | null = null;
      let lastVerify = startedMs;
      try {
        while (reason === null) {
          if (helpers.isCancelled()) { reason = 'cancelled'; break; }
          if (await stopAsked(helpers.runId)) { reason = 'stopped'; break; }
          if (call && await isEnded(call)) { reason = 'call-ended'; break; }
          const t = now();
          if (t - startedMs >= maxMs()) { reason = 'max-duration'; break; }
          if (t - lastVerify >= (tapIds.length === 0 ? pollMs : verifyEveryMs)) {
            // Unarmed (captions turned on before anyone published audio) the
            // tap is retried every poll; armed, it is verified once a minute —
            // a resumed run, or a tap LVS dropped, gets it back rather than
            // captioning silence for the rest of the call.
            lastVerify = t;
            try { tapIds = await ensureArmed(channelArn, tapIds); } catch (err) { log.warn({ err: err instanceof Error ? err.message : String(err), channelArn }, '[live-captions] tap verify failed'); }
          }
          await helpers.sleep(pollMs);
          if (helpers.isCancelled()) { reason = 'cancelled'; break; }
        }
      } finally {
        await unsubscribe().catch(() => undefined);
        progressByRun.delete(helpers.runId);
        await clearStop(helpers.runId);
        if (reason === 'cancelled') {
          // Nothing downstream will run on a cancelled run: the tap goes now.
          await Promise.resolve(disarm(channelArn, tapIds)).catch((err: unknown) => log.warn({ err: err instanceof Error ? err.message : String(err), channelArn }, '[live-captions] disarm after cancel failed'));
          runByArn.delete(channelArn);
        }
      }

      const output = { lines, words, speakers: speakers.size, durationMs: now() - startedMs, endedBecause: reason, lastLineAt, tapIds, armed: tapIds.length > 0 };
      if (reason === 'cancelled') return { status: 'cancelled', output };
      return ok(output);
    },
  };
}

function disarmHandler(tap: TapDeps = {}): DescribedStepHandler {
  return {
    kind: CAPTION_KINDS.disarm,
    description:
      'Removes the caption taps. A refusal LVS might reconsider fails the step so the node\'s retry policy can try again; a tap whose channel is already gone is reported as orphaned instead, since no retry can change that.',
    reads: 'channelArn and the tap ids the arm step created.',
    emits:
      'Which taps were removed and which were already gone.',
    async execute(_n: PipelineNode, ctx: Record<string, unknown>): Promise<StepOutcome> {
      const channelArn = str(ctx.channelArn);
      if (!channelArn) return fail('channelArn is required');
      const result = await disarmCaptionTaps(channelArn, strList(ctx.tapIds), tap);
      runByArn.delete(channelArn);
      // A refusal LVS might reconsider is retried from the node's retryPolicy;
      // an orphan (channel already gone) is not something a retry can change.
      if (result.failed.length > 0) return fail(`LVS refused to delete ${result.failed.map((x) => `${x.tapId} (${x.status || 'network error'})`).join(', ')}`);
      return ok({ removed: result.removed, orphaned: result.orphaned });
    },
  };
}

function publishHandler(): DescribedStepHandler {
  return {
    kind: CAPTION_KINDS.publish,
    description:
      'Posts a run card back to the lobby saying how many lines were captioned. A lobby with no conversation is not a failure — it records that nothing was posted.',
    reads: 'lobbyName and the counts from the stream step.',
    emits:
      'Whether a card was posted, and to which channel.',
    async execute(_n: PipelineNode, ctx: Record<string, unknown>, helpers: StepHandlerHelpers): Promise<StepOutcome> {
      const lobbyName = str(ctx.lobbyName) ?? null;
      const channel = chatChannelForLobby(lobbyName);
      const lines = Number(ctx.lines ?? 0);
      const instruction = `${lines} line${lines === 1 ? '' : 's'} captioned`;
      const posted = await postRunCardToChannel({
        channel, runId: helpers.runId, pipelineId: LIVE_CAPTIONS_PIPELINE_ID, command: 'captions', instruction,
        extra: { sessionId: ctx.sessionId ?? null, channelArn: ctx.channelArn, words: Number(ctx.words ?? 0), durationMs: Number(ctx.durationMs ?? 0), endedBecause: ctx.endedBecause ?? null },
        context: ctx,
      });
      if (!channel) log.info({ lobbyName, runId: helpers.runId }, '[live-captions] run card not posted — lobby has no conversation');
      return ok({ posted, channel, instruction });
    },
  };
}

export interface LiveCaptionsHandlerDeps { tap?: TapDeps; stream?: StreamDeps }

export function buildLiveCaptionsHandlers(deps: LiveCaptionsHandlerDeps = {}): Map<string, StepHandler> {
  const m = new Map<string, StepHandler>();
  for (const h of [armHandler(deps.tap), streamHandler(deps.stream), disarmHandler(deps.tap), publishHandler()]) m.set(h.kind, h);
  return m;
}
