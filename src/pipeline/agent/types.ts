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
  | 'cancelled';

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
}

// ── Client-facing streaming events ───────────────────────────────────
//
// The agent client yields these instead of bare text tokens, so the
// step handler can build the trace and emit `pipeline:step:agent:tool`
// as the run happens rather than reconstructing it at the end.

export type AgentEvent =
  | { type: 'text'; text: string }
  | { type: 'turn'; index: number }
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
}

/** Anything that can run an agent loop and report structured progress. */
export interface AgentClient {
  readonly provider: string;
  run(prompt: string, opts?: AgentRunOptions): AsyncIterable<AgentEvent>;
}

export const DEFAULT_MAX_TURNS = 8;
export const DEFAULT_TIMEOUT_MS = 120_000;
