// platform-api/src/pipeline/agent/types.ts
//
// Shared contract for the `agent` pipeline step. Mirrors
// scratchpad/agent-step-contract.md v1 exactly — three workstreams
// (this backend, distributed-core's first-class NodeType, and the
// frontend node/renderer) code against these shapes.
//
// WHY HERE AND NOT IN @connorhoehn/pipeline-runtime: that package's
// public surface is run/step *execution* types (PipelineRun,
// PipelineStep, PipelineContext) classified STABLE for v1.0. Node
// `data` shapes are not modelled there at all — `llm`'s node data
// lives in distributed-core. Adding to pipeline-runtime would also
// force the SHA-pin-and-rebuild dance (commit + push + repin +
// reinstall) before platform-api could compile against it, which is
// exactly the loop the agent step was asked to avoid.

/** Author-facing node data for a `agent` step. */
export interface AgentNodeData {
  type: 'agent';
  /** The task. Supports the same `{{ context.x }}` interpolation as `llm`. */
  prompt: string;
  systemPrompt?: string;
  /** Friendly alias ('sonnet' | 'opus' | 'haiku') or explicit id. */
  model?: string;
  /**
   * Tools the agent may use. DEFAULT-DENY: absent or empty means the
   * agent runs with NO tools and is equivalent to a plain `llm` step.
   * Never widen this implicitly.
   *
   * Entries may be a bare built-in tool name (`Read`) or a permission
   * specifier (`Bash(git status:*)`). The bare name gates *availability*
   * (CLI `--tools`); the full specifier narrows *permission* within an
   * available tool (CLI `--allowedTools`).
   */
  allowedTools?: string[];
  /** Hard ceiling on agent turns. Default 8. */
  maxTurns?: number;
  /** Wall-clock ceiling for the whole step. Default 120_000. */
  timeoutMs?: number;
  /**
   * Which agent runtime executes the step.
   *
   *   'local'  (default) — ClaudeCodeAgentClient: `claude -p` in this pod.
   *   'devbox'           — DevboxAgentClient: an agent-core microVM session
   *                        reached through the middleware's internal HMAC
   *                        router, shared by every devbox step of the run.
   *
   * Absent means 'local', so every definition written before the devbox
   * runtime existed keeps running exactly as it did.
   */
  runtime?: 'local' | 'devbox';
  /**
   * Devbox only. The loop stops with `stopReason: 'budget'` once the
   * session's reported context reaches this many tokens. Without one the
   * loop is bounded only by maxTurns and timeoutMs.
   */
  contextBudgetTokens?: number;
  /** What the node is called in the editor and in the Active Now graph. */
  label?: string;
  /** Provenance when the node was generated from an agent-loop plan. */
  loop?: { kind: string; stepId: string };
}

/** One tool invocation, start to finish, as persisted in the trace. */
export interface AgentToolCall {
  name: string;
  input?: unknown;
  output?: unknown;
  ok: boolean;
  startedAt: string; // ISO
  endedAt?: string; // ISO
  /** Present when ok === false. */
  error?: string;
}

export type AgentStopReason =
  | 'complete'
  | 'max_turns'
  | 'timeout'
  | 'error'
  | 'cancelled'
  /** The devbox session's context reached `contextBudgetTokens`. */
  | 'budget';

/** Persisted + rendered step output. */
export interface AgentStepOutput {
  /** Final assistant text. */
  text: string;
  turns: number;
  toolCalls: AgentToolCall[];
  model: string;
  tokensIn: number;
  tokensOut: number;
  stopReason: AgentStopReason;
  /** True when the step ran with no tools allowed (see allowedTools). */
  toolless: boolean;
  /**
   * How many times the step asked the agent to carry on. The local runtime
   * runs one agent process, so this is its turn count; the devbox runtime
   * resumes the same session repeatedly and this counts those resumes.
   */
  iterations: number;
  /** Context the last turn carried — input + cache-read + cache-creation. */
  contextTokens: number;
  /** The ceiling `contextTokens` was compared against, when there was one. */
  contextBudgetTokens?: number;
  runtime: 'local' | 'devbox';
  /** Devbox only: the microVM session and box this step ran in. */
  sessionId?: string;
  boxId?: string;
}

/** The devbox session a run owns, shared by every devbox step in it. */
export interface AgentLabSession {
  sessionId: string;
  boxId: string;
}

// ── Client-facing streaming events ───────────────────────────────────
//
// The agent client yields these instead of bare text tokens, so the
// step handler can build the trace and emit `pipeline:step:agent:tool`
// as the run happens rather than reconstructing it at the end.

export type AgentEvent =
  | { type: 'text'; text: string }
  | { type: 'turn'; index: number }
  /**
   * Devbox only: the session this run's steps share, reported as soon as it
   * is known so the step handler can persist it even if the loop then fails.
   * `created` is false when an earlier step in the run opened it.
   */
  | { type: 'session'; sessionId: string; boxId: string; created: boolean }
  /**
   * Devbox only: one pass of the continue-until-done loop finished. The step
   * handler forwards this as `pipeline:step:agent:iteration`.
   */
  | {
      type: 'iteration';
      iteration: number;
      contextTokens: number;
      contextBudgetTokens?: number;
      model: string;
    }
  | {
      type: 'tool_start';
      /** CLI `tool_use.id`, used to pair with `tool_end`. */
      id: string;
      /** Index into the resulting `toolCalls` array. */
      callIndex: number;
      name: string;
      input: unknown;
      at: string;
    }
  | {
      type: 'tool_end';
      id: string;
      callIndex: number;
      name: string;
      ok: boolean;
      output: unknown;
      error?: string;
      at: string;
    }
  | {
      type: 'done';
      text: string;
      turns: number;
      tokensIn: number;
      tokensOut: number;
      model: string;
      stopReason: AgentStopReason;
      /** Devbox only; see AgentStepOutput for what each means. */
      iterations?: number;
      contextTokens?: number;
      sessionId?: string;
      boxId?: string;
      /**
       * Authoritative trace. Includes calls that never reported back
       * (killed mid-flight by a turn/timeout breach), which by
       * definition produce no `tool_end` event — so a consumer that
       * rebuilt the trace from events alone would silently drop them.
       */
      toolCalls: AgentToolCall[];
    };

export interface AgentRunOptions {
  model?: string;
  systemPrompt?: string;
  allowedTools?: string[];
  maxTurns?: number;
  timeoutMs?: number;
  signal?: AbortSignal;
  /** Working directory for the agent's tools. Defaults to the CLI default. */
  cwd?: string;
  /** The run this step belongs to. Devbox keys its session by it. */
  runId?: string;
  stepId?: string;
  /** Devbox only: stop with `budget` once the session's context reaches this. */
  contextBudgetTokens?: number;
  /**
   * Devbox only: the session an earlier step of the same run already opened
   * (the run context's `lab`). Absent means this step opens one.
   */
  lab?: AgentLabSession;
  /** Who the session is registered to, and what it is called in the fleet. */
  requestedBy?: string;
  label?: string;
  /**
   * Devbox only: context the RUN has already spent in this session, before
   * this step's first task. The session is shared and its context carries
   * across steps, so a step that starts counting from zero can go straight
   * through the run's ceiling on its first task.
   */
  startingContextTokens?: number;
}

/** Anything that can run an agent loop and report structured progress. */
export interface AgentClient {
  readonly provider: string;
  run(prompt: string, opts?: AgentRunOptions): AsyncIterable<AgentEvent>;
}

export const DEFAULT_MAX_TURNS = 8;
export const DEFAULT_TIMEOUT_MS = 120_000;
