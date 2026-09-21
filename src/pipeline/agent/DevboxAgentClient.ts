// platform-api/src/pipeline/agent/DevboxAgentClient.ts
//
// The `runtime: 'devbox'` agent client: instead of shelling out to
// `claude -p` inside this pod (ClaudeCodeAgentClient), it drives an
// agent-core microVM "computer lab" session through the middleware's
// INTERNAL HMAC router — the same envelope the work-graph
// compute-capabilities call already signs (src/work-graph/runtime.ts),
// so there is no second secret and no second trust story.
//
// ─── WHY A SESSION PER RUN, NOT PER STEP ───────────────────────────────
//
// A generated agent loop is several steps against ONE workspace: the
// step that researches leaves notes the step that writes the deck reads.
// A session per step would give each of them an empty box. So the first
// devbox step of a run opens the session and reports it (`session`
// event); the step handler puts it in the run context as `lab`, every
// later step of the run gets it back through `opts.lab` and resumes it,
// and bootstrap deletes it once the run reaches a terminal state.
//
// ─── WHY A LOOP AND NOT ONE CALL ───────────────────────────────────────
//
// "continuously" in the user's ask. One `/task` call is one agent turn
// budget; the loop re-posts `{ resume: true }` until the agent says it
// is finished. It says so by ending its reply with the single word DONE
// on the last line — asked for in the acceptance sentence the planner
// writes into every prompt. Every other exit is a BOUND, recorded as
// such rather than dressed up as success:
//
//   'complete'  the agent said DONE
//   'budget'    the session's context reached contextBudgetTokens
//   'max_turns' the loop hit maxTurns iterations
//   'timeout'   wall clock hit timeoutMs
//   'error'     the stream or the middleware said so
//
// ─── EVERY BUFFER IS BOUNDED ───────────────────────────────────────────
//
// This reads a stream from a machine that can talk for twenty minutes.
// Text, the tool trace, each tool payload and each SSE frame all have a
// cap, for the same reason the local client caps its trace: runPersistence
// replaces an over-large step output WHOLESALE with `{truncated,size}`,
// which would lose the whole trace rather than the oversized part of it.

import { SERVICE_AUTH_HEADER, signEnvelope } from '@connorhoehn/service-runtime';
import type {
  AgentClient,
  AgentEvent,
  AgentLabSession,
  AgentRunOptions,
  AgentStopReason,
  AgentToolCall,
} from './types';
import { DEFAULT_MAX_TURNS, DEFAULT_TIMEOUT_MS } from './types';

/** Mount point of the middleware's internal (service-principal) devbox router. */
export const DEVBOX_BASE_PATH = '/internal/devbox';

/** What the loop tells the agent when it has not said DONE yet. */
export const DEVBOX_CONTINUE_PROMPT =
  'Continue the same task. When the acceptance is met, reply with the deliverable summary and the single word DONE on the last line.';

/** Bounds. Text and trace are what land in the persisted step output. */
export const MAX_TEXT_CHARS = 40_000;
export const MAX_TOOL_CALLS = 200;
export const MAX_TOOL_PAYLOAD_CHARS = 4_000;
/** One SSE frame. A box that streams a 5 MB file into one frame gets cut. */
export const MAX_FRAME_CHARS = 256 * 1024;
/** Session open / delete / shell-url are small calls; the stream is not. */
export const CONTROL_TIMEOUT_MS = 15_000;

/**
 * What every caller gets when the middleware has no devbox router — an
 * older build, or the URL pointing somewhere else. An honest sentence,
 * never a silent fall back to the local runtime.
 */
export const DEVBOX_UNAVAILABLE = 'devbox runtime unavailable';

export class DevboxUnavailableError extends Error {
  constructor(detail: string) {
    super(`${DEVBOX_UNAVAILABLE}: ${detail}`);
    this.name = 'DevboxUnavailableError';
  }
}

/**
 * Base URL of the middleware. `AGENTCORE_DEVBOX_URL` first; it defaults to
 * `WORK_GRAPH_COMPUTE_SOURCE_URL` because under Tilt they are the same
 * process (http://host.docker.internal:14030) and asking operators to set
 * the same host twice is how one of them ends up stale.
 */
export function devboxBaseUrl(): string | null {
  const raw = (process.env.AGENTCORE_DEVBOX_URL || process.env.WORK_GRAPH_COMPUTE_SOURCE_URL || '').trim();
  return raw ? raw.replace(/\/+$/, '') : null;
}

/** Same secret and same fallback order as the compute-capabilities call. */
export function devboxSecret(): string | null {
  const raw = (process.env.WORK_GRAPH_SERVICE_AUTH_SECRET || process.env.SERVICE_AUTH_SECRET || '').trim();
  return raw || null;
}

export type FetchLike = (input: string, init?: RequestInit) => Promise<Response>;

export interface DevboxAgentClientOpts {
  baseUrl?: string;
  secret?: string;
  fetchImpl?: FetchLike;
  /** Injected by tests so the loop's clock is not the wall clock. */
  now?: () => number;
  maxToolPayloadChars?: number;
}

/** One frame off the re-framed stream-json SSE. Fields beyond `type` are optional by design. */
export interface DevboxFrame {
  type?: string;
  [k: string]: unknown;
}

const str = (v: unknown): string => (typeof v === 'string' ? v : '');
const num = (v: unknown): number => (typeof v === 'number' && Number.isFinite(v) ? v : 0);

/** `input + cache_read + cache_creation` of the LAST turn — what the model is carrying. */
export function contextTokensOf(usage: unknown): number {
  if (!usage || typeof usage !== 'object') return 0;
  const u = usage as Record<string, unknown>;
  return num(u.input_tokens) + num(u.cache_read_input_tokens) + num(u.cache_creation_input_tokens);
}

/** DONE, alone on the last non-empty line, case-insensitive. Nothing else counts. */
export function saysDone(text: string): boolean {
  const lines = String(text ?? '').split('\n');
  for (let i = lines.length - 1; i >= 0; i -= 1) {
    const line = lines[i].trim();
    if (!line) continue;
    return /^done[.!]?$/i.test(line);
  }
  return false;
}

/** Bound one payload so a big Read cannot sink the trace. Mirrors AgentStepHandler.capToolPayload. */
export function capPayload(value: unknown, maxChars: number): unknown {
  if (value === undefined || value === null) return value;
  if (typeof value === 'string') {
    return value.length > maxChars ? `${value.slice(0, maxChars)}…[truncated ${value.length - maxChars} chars]` : value;
  }
  let serialized: string;
  try {
    serialized = JSON.stringify(value) ?? '';
  } catch {
    return '[unserializable]';
  }
  if (serialized.length <= maxChars) return value;
  return { truncated: true, size: serialized.length, preview: serialized.slice(0, maxChars) };
}

/** The assistant text carried by an `assistant` frame, however it is shaped. */
export function assistantTextOf(frame: DevboxFrame): string {
  if (typeof frame.text === 'string') return frame.text;
  const message = frame.message as { content?: unknown } | undefined;
  const content = message?.content ?? frame.content;
  if (typeof content === 'string') return content;
  if (!Array.isArray(content)) return '';
  return content
    .map((b) => (b && typeof b === 'object' && (b as { type?: unknown }).type === 'text' ? str((b as { text?: unknown }).text) : ''))
    .join('');
}

/** The final text of a `result` frame. */
export function resultTextOf(frame: DevboxFrame): string {
  for (const key of ['result', 'text', 'content']) {
    const v = frame[key];
    if (typeof v === 'string' && v) return v;
  }
  return assistantTextOf(frame);
}

/**
 * Split an SSE byte stream into frames. Yields the parsed `data:` JSON of
 * each event; a frame that is not JSON, or is over MAX_FRAME_CHARS, is
 * dropped rather than allowed to grow the buffer without bound.
 */
export async function* readSseFrames(body: ReadableStream<Uint8Array>): AsyncGenerator<DevboxFrame> {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      // A producer that never emits a blank line must not grow the buffer forever.
      if (buffer.length > MAX_FRAME_CHARS * 2) buffer = buffer.slice(-MAX_FRAME_CHARS);
      let split = buffer.indexOf('\n\n');
      while (split !== -1) {
        const chunk = buffer.slice(0, split);
        buffer = buffer.slice(split + 2);
        const frame = parseSseChunk(chunk);
        if (frame) yield frame;
        split = buffer.indexOf('\n\n');
      }
    }
    const tail = parseSseChunk(buffer);
    if (tail) yield tail;
  } finally {
    await reader.cancel().catch(() => undefined);
  }
}

function parseSseChunk(chunk: string): DevboxFrame | null {
  const data = chunk
    .split('\n')
    .filter((line) => line.startsWith('data:'))
    .map((line) => line.slice(5).trim())
    .join('\n');
  if (!data || data === '[DONE]' || data.length > MAX_FRAME_CHARS) return null;
  try {
    const parsed = JSON.parse(data) as unknown;
    return parsed && typeof parsed === 'object' ? (parsed as DevboxFrame) : null;
  } catch {
    return null;
  }
}

/** One iteration's worth of what the stream said. */
interface IterationResult {
  text: string;
  contextTokens: number;
  tokensIn: number;
  tokensOut: number;
  errored: string | null;
}

export class DevboxAgentClient implements AgentClient {
  readonly provider = 'agentcore-devbox';

  private readonly fetchImpl: FetchLike;
  private readonly now: () => number;
  private readonly maxToolPayloadChars: number;

  constructor(private readonly opts: DevboxAgentClientOpts = {}) {
    this.fetchImpl = opts.fetchImpl ?? ((input, init) => fetch(input, init));
    this.now = opts.now ?? (() => Date.now());
    this.maxToolPayloadChars = opts.maxToolPayloadChars ?? MAX_TOOL_PAYLOAD_CHARS;
  }

  private base(): string {
    const url = this.opts.baseUrl ?? devboxBaseUrl();
    if (!url) throw new DevboxUnavailableError('AGENTCORE_DEVBOX_URL is not configured');
    return url.replace(/\/+$/, '');
  }

  private secret(): string {
    const secret = this.opts.secret ?? devboxSecret();
    if (!secret) throw new DevboxUnavailableError('WORK_GRAPH_SERVICE_AUTH_SECRET is not configured');
    return secret;
  }

  /** Signed exactly like the compute-capabilities envelope: body-bound, 30s ttl, serviceId platform-api. */
  private headers(body: Record<string, unknown>): Record<string, string> {
    return {
      'content-type': 'application/json',
      accept: 'text/event-stream',
      [SERVICE_AUTH_HEADER]: signEnvelope(body, { secret: this.secret(), serviceId: 'platform-api' }),
    };
  }

  private async post(path: string, body: Record<string, unknown>, signal?: AbortSignal): Promise<Response> {
    const res = await this.fetchImpl(`${this.base()}${DEVBOX_BASE_PATH}${path}`, {
      method: 'POST',
      headers: this.headers(body),
      body: JSON.stringify(body),
      ...(signal ? { signal } : {}),
    });
    // A middleware without the internal router answers 404 for every path
    // under it. That is a deployment fact, not a task failure — say so.
    if (res.status === 404) throw new DevboxUnavailableError(`middleware has no ${DEVBOX_BASE_PATH}${path}`);
    return res;
  }

  /** Open a session, or hand back the one this run already has. */
  async openSession(args: { requestedBy?: string; label?: string; existing?: AgentLabSession; signal?: AbortSignal }): Promise<{ lab: AgentLabSession; created: boolean }> {
    if (args.existing?.sessionId) return { lab: args.existing, created: false };
    const body: Record<string, unknown> = {
      requestedBy: args.requestedBy ?? 'agent:loop',
      ...(args.label ? { label: args.label.slice(0, 120) } : {}),
      principal: { service: 'platform-api', requestedBy: args.requestedBy ?? 'agent:loop' },
    };
    const res = await this.post('/sessions', body, args.signal ?? AbortSignal.timeout(CONTROL_TIMEOUT_MS));
    if (!res.ok) throw new DevboxUnavailableError(`POST /sessions answered ${res.status}`);
    const json = (await res.json()) as { sessionId?: unknown; boxId?: unknown };
    const sessionId = str(json.sessionId);
    const boxId = str(json.boxId);
    if (!sessionId) throw new DevboxUnavailableError('POST /sessions returned no sessionId');
    return { lab: { sessionId, boxId }, created: true };
  }

  /** Best-effort teardown. A session that will not delete must not fail a run. */
  async deleteSession(sessionId: string): Promise<boolean> {
    if (!sessionId) return false;
    try {
      const body: Record<string, unknown> = { principal: { service: 'platform-api' } };
      const res = await this.fetchImpl(`${this.base()}${DEVBOX_BASE_PATH}/${encodeURIComponent(sessionId)}`, {
        method: 'DELETE',
        headers: this.headers(body),
        body: JSON.stringify(body),
        signal: AbortSignal.timeout(CONTROL_TIMEOUT_MS),
      });
      return res.ok;
    } catch {
      return false;
    }
  }

  /** `POST /:sessionId/task`, returning the open SSE response. */
  async startTask(sessionId: string, body: Record<string, unknown>, signal?: AbortSignal): Promise<Response> {
    const res = await this.post(`/${encodeURIComponent(sessionId)}/task`, body, signal);
    if (!res.ok) throw new DevboxUnavailableError(`POST /${sessionId}/task answered ${res.status}`);
    if (!res.body) throw new DevboxUnavailableError(`POST /${sessionId}/task returned no stream`);
    return res;
  }

  /** `POST /:sessionId/shell-url` — the browser terminal, for the requester only (the route enforces that). */
  async shellUrl(sessionId: string, requestedBy: string): Promise<{ url: string; expiresAt?: string }> {
    const body: Record<string, unknown> = { requestedBy, principal: { service: 'platform-api', requestedBy } };
    const res = await this.post(`/${encodeURIComponent(sessionId)}/shell-url`, body, AbortSignal.timeout(CONTROL_TIMEOUT_MS));
    if (!res.ok) throw new DevboxUnavailableError(`POST /${sessionId}/shell-url answered ${res.status}`);
    const json = (await res.json()) as { url?: unknown; expiresAt?: unknown };
    const url = str(json.url);
    if (!url) throw new DevboxUnavailableError('shell-url returned no url');
    return { url, ...(str(json.expiresAt) ? { expiresAt: str(json.expiresAt) } : {}) };
  }

  async *run(prompt: string, opts: AgentRunOptions = {}): AsyncIterable<AgentEvent> {
    const model = opts.model?.trim() || 'sonnet';
    const maxTurns = opts.maxTurns && opts.maxTurns > 0 ? opts.maxTurns : DEFAULT_MAX_TURNS;
    const timeoutMs = opts.timeoutMs && opts.timeoutMs > 0 ? opts.timeoutMs : DEFAULT_TIMEOUT_MS;
    const budget = opts.contextBudgetTokens && opts.contextBudgetTokens > 0 ? opts.contextBudgetTokens : undefined;
    const startedAt = this.now();
    const deadline = startedAt + timeoutMs;

    const toolCalls: AgentToolCall[] = [];
    let text = '';
    let iterations = 0;
    let contextTokens = 0;
    let tokensIn = 0;
    let tokensOut = 0;
    let stopReason: AgentStopReason = 'max_turns';
    let failure: string | null = null;

    const opened = await this.openSession({
      ...(opts.requestedBy ? { requestedBy: opts.requestedBy } : {}),
      ...(opts.label ? { label: opts.label } : {}),
      ...(opts.lab ? { existing: opts.lab } : {}),
      ...(opts.signal ? { signal: opts.signal } : {}),
    });
    const { sessionId, boxId } = opened.lab;
    yield { type: 'session', sessionId, boxId, created: opened.created };

    // A session an earlier step opened is already warm: the FIRST task
    // against it resumes rather than starts fresh, so the workspace and the
    // conversation carry across steps.
    let resume = !opened.created;

    while (iterations < maxTurns) {
      if (opts.signal?.aborted) { stopReason = 'cancelled'; break; }
      if (this.now() >= deadline) { stopReason = 'timeout'; break; }

      iterations += 1;
      yield { type: 'turn', index: iterations };

      const body: Record<string, unknown> = {
        prompt: iterations === 1 ? prompt : DEVBOX_CONTINUE_PROMPT,
        model,
        resume,
        principal: { service: 'platform-api', requestedBy: opts.requestedBy ?? 'agent:loop' },
      };
      resume = true;

      let iterationOutcome: IterationResult;
      try {
        const res = await this.startTask(sessionId, body, this.taskSignal(opts.signal, deadline));
        const events: AgentEvent[] = [];
        iterationOutcome = await this.consume(res.body!, toolCalls, events);
        for (const ev of events) yield ev;
      } catch (err) {
        if (opts.signal?.aborted) { stopReason = 'cancelled'; break; }
        if (this.now() >= deadline) { stopReason = 'timeout'; break; }
        stopReason = 'error';
        failure = err instanceof Error ? err.message : String(err);
        break;
      }

      if (iterationOutcome.text) {
        text = `${text}${text ? '\n\n' : ''}${iterationOutcome.text}`.slice(-MAX_TEXT_CHARS);
      }
      tokensIn += iterationOutcome.tokensIn;
      tokensOut += iterationOutcome.tokensOut;
      if (iterationOutcome.contextTokens > 0) contextTokens = iterationOutcome.contextTokens;

      yield {
        type: 'iteration',
        iteration: iterations,
        contextTokens,
        ...(budget ? { contextBudgetTokens: budget } : {}),
        model,
      };

      if (iterationOutcome.errored) {
        stopReason = 'error';
        failure = iterationOutcome.errored;
        break;
      }
      // DONE first: an agent that finished ON the budget boundary finished.
      if (saysDone(iterationOutcome.text)) { stopReason = 'complete'; break; }
      if (budget && contextTokens >= budget) { stopReason = 'budget'; break; }
      if (this.now() >= deadline) { stopReason = 'timeout'; break; }
      if (iterations >= maxTurns) { stopReason = 'max_turns'; break; }
    }

    yield {
      type: 'done',
      text: failure && !text ? `${failure}` : text,
      turns: iterations,
      tokensIn,
      tokensOut,
      model,
      stopReason,
      iterations,
      contextTokens,
      sessionId,
      boxId,
      toolCalls,
    };
  }

  /** The signal one `/task` call runs under: the caller's cancel, or the wall clock, whichever first. */
  private taskSignal(signal: AbortSignal | undefined, deadline: number): AbortSignal {
    const remaining = Math.max(1_000, deadline - this.now());
    const clock = AbortSignal.timeout(remaining);
    if (!signal) return clock;
    // AbortSignal.any is Node 20+, the engines floor for this service.
    return AbortSignal.any([signal, clock]);
  }

  /** Drain one task stream into the trace, collecting the events to yield. */
  private async consume(
    body: ReadableStream<Uint8Array>,
    toolCalls: AgentToolCall[],
    out: AgentEvent[],
  ): Promise<IterationResult> {
    const pending = new Map<string, number>();
    let text = '';
    let contextTokens = 0;
    let tokensIn = 0;
    let tokensOut = 0;
    let errored: string | null = null;

    for await (const frame of readSseFrames(body)) {
      const type = str(frame.type);
      if (type === 'assistant') {
        const chunk = assistantTextOf(frame);
        if (chunk) {
          text = `${text}${chunk}`.slice(-MAX_TEXT_CHARS);
          out.push({ type: 'text', text: chunk.slice(0, MAX_TEXT_CHARS) });
        }
      } else if (type === 'tool_use') {
        if (toolCalls.length >= MAX_TOOL_CALLS) continue;
        const id = str(frame.id) || `tool-${toolCalls.length}`;
        const name = str(frame.name) || 'tool';
        const at = str(frame.ts) || new Date().toISOString();
        const callIndex = toolCalls.length;
        toolCalls.push({ name, input: capPayload(frame.input, this.maxToolPayloadChars), ok: false, startedAt: at });
        pending.set(id, callIndex);
        out.push({ type: 'tool_start', id, callIndex, name, input: toolCalls[callIndex].input, at });
      } else if (type === 'tool_result') {
        const id = str(frame.tool_use_id) || str(frame.id);
        const callIndex = pending.get(id);
        if (callIndex === undefined) continue;
        pending.delete(id);
        const at = str(frame.ts) || new Date().toISOString();
        const isError = frame.is_error === true || frame.ok === false;
        const output = capPayload(frame.content ?? frame.output, this.maxToolPayloadChars);
        const call = toolCalls[callIndex];
        call.ok = !isError;
        call.output = output;
        call.endedAt = at;
        if (isError) call.error = typeof output === 'string' ? output.slice(0, 500) : 'tool reported an error';
        out.push({
          type: 'tool_end', id, callIndex, name: call.name, ok: !isError, output, at,
          ...(call.error ? { error: call.error } : {}),
        });
      } else if (type === 'result') {
        const final = resultTextOf(frame);
        if (final) text = final.slice(-MAX_TEXT_CHARS);
        const usage = frame.usage;
        contextTokens = contextTokensOf(usage);
        if (usage && typeof usage === 'object') {
          const u = usage as Record<string, unknown>;
          tokensIn = num(u.input_tokens);
          tokensOut = num(u.output_tokens);
        }
        if (frame.is_error === true) errored = str(frame.error) || 'the devbox task reported an error';
      } else if (type === 'devbox_error') {
        errored = str(frame.error) || str(frame.message) || 'the devbox stream reported an error';
      }
    }

    return { text, contextTokens, tokensIn, tokensOut, errored };
  }
}

// ─── The run → session map bootstrap tears down from ──────────────────
//
// `context.lab` is the durable record (it rides the run's checkpoint, so a
// GET after a restart still knows which box a run used). This map is the
// LIVE one: the terminal-event subscriber has a runId and needs a session
// id without reading DDB on every completed run. Bounded — a leaked entry
// is a few dozen bytes, but an unbounded map on a long-lived process is a
// leak with a nicer name.

const MAX_TRACKED_RUNS = 500;
const RUN_SESSIONS = new Map<string, AgentLabSession>();

export function rememberRunLab(runId: string, lab: AgentLabSession): void {
  if (!runId || !lab?.sessionId) return;
  // Re-insert so the map's insertion order is a crude LRU.
  RUN_SESSIONS.delete(runId);
  RUN_SESSIONS.set(runId, lab);
  while (RUN_SESSIONS.size > MAX_TRACKED_RUNS) {
    const oldest = RUN_SESSIONS.keys().next();
    if (oldest.done) break;
    RUN_SESSIONS.delete(oldest.value);
  }
}

export function recallRunLab(runId: string): AgentLabSession | undefined {
  return RUN_SESSIONS.get(runId);
}

/** Read and drop in one go, so a terminal event can only tear a session down once. */
export function takeRunLab(runId: string): AgentLabSession | undefined {
  const lab = RUN_SESSIONS.get(runId);
  RUN_SESSIONS.delete(runId);
  return lab;
}

/** Test seam. */
export function clearRunLabs(): void {
  RUN_SESSIONS.clear();
}
