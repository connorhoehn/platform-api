// platform-api/src/pipeline/agent/AgentStepHandler.ts
//
// Custom step handler (the DC-21 `customStepHandlers` seam) that
// dispatches `node.data.type === 'agent'`. Deliberately NOT a new
// NodeType in distributed-core: a DC change means rebuilding its
// committed dist/ AND bumping pipeline-runtime's SHA-pinned peer
// before platform-api can adopt it. This handler works today, and
// when DC lands a first-class `agent` NodeType the built-in dispatch
// takes over — at which point this file's registration is dropped and
// nothing else changes, because the node data and the step output are
// the same contract either way.
//
// Lifecycle per call:
//   1. Interpolate `{{ context.x }}` in prompt + systemPrompt.
//   2. Run the agent loop, bounded by maxTurns / timeoutMs.
//   3. Emit `pipeline:step:agent:tool` (start/end) as tools fire.
//   4. Return AgentStepOutput as the step output, which the executor's
//      `pipeline:step:completed` carries into runPersistence and thus
//      into `GET /api/pipelines/:id/runs/:runId`.

import type {
  PipelineNode,
  StepHandler,
  StepHandlerHelpers,
  StepOutcome,
} from 'distributed-core/applications/pipeline';
import type {
  AgentClient,
  AgentNodeData,
  AgentStepOutput,
  AgentToolCall,
} from './types';
import { DEFAULT_MAX_TURNS, DEFAULT_TIMEOUT_MS } from './types';
import { isToolless } from './ClaudeCodeAgentClient';

/**
 * `pipeline:step:agent:tool` is not in DC's `PipelineEventMap`, which
 * is a `type` alias and so cannot be module-augmented from here. The
 * runtime EventBus accepts arbitrary string topics; this cast is the
 * same bridge JudgeStepHandler and visionHandlerFactory use. It drops
 * without a payload change once DC's map lists the event.
 */
type LooseEmit = (event: string, payload: unknown) => Promise<void>;

export interface AgentStepHandlerOpts {
  agentClient: AgentClient;
  defaultModel?: string;
  /**
   * Working directory handed to the agent's tools. Only meaningful
   * when the node actually allows a filesystem tool.
   */
  cwd?: string;
  /**
   * Per-payload cap for a persisted tool input/output, in characters.
   * runPersistence replaces a step output wholesale with
   * `{truncated:true,size}` once it exceeds 100 KB — which would lose
   * the whole trace, not just the oversized field. Capping each
   * payload here keeps the trace itself intact.
   */
  maxToolPayloadChars?: number;
}

const DEFAULT_MODEL = 'sonnet';
const DEFAULT_MAX_TOOL_PAYLOAD_CHARS = 4_000;

/** DC's own `{{ context.x }}` semantics, matched exactly (see PipelineExecutor.interpolate). */
export function interpolateContext(
  template: string,
  context: Record<string, unknown>,
): string {
  return template.replace(/\{\{\s*context\.([\w.]+)\s*\}\}/g, (_m, path: string) => {
    const segments = path.split('.');
    let value: unknown = context;
    for (const seg of segments) {
      if (value === null || typeof value !== 'object' || !(seg in (value as object))) {
        return '';
      }
      value = (value as Record<string, unknown>)[seg];
    }
    return value === undefined || value === null ? '' : String(value);
  });
}

/** Bound one tool payload so a big Read can't sink the whole trace. */
export function capToolPayload(value: unknown, maxChars: number): unknown {
  if (value === undefined || value === null) return value;
  if (typeof value === 'string') {
    return value.length > maxChars
      ? `${value.slice(0, maxChars)}…[truncated ${value.length - maxChars} chars]`
      : value;
  }
  let serialized: string;
  try {
    serialized = JSON.stringify(value) ?? '';
  } catch {
    return '[unserializable]';
  }
  if (serialized.length <= maxChars) return value;
  return {
    truncated: true,
    size: serialized.length,
    preview: serialized.slice(0, maxChars),
  };
}

export function createAgentStepHandler(opts: AgentStepHandlerOpts): StepHandler {
  const defaultModel = opts.defaultModel ?? DEFAULT_MODEL;
  const maxChars = opts.maxToolPayloadChars ?? DEFAULT_MAX_TOOL_PAYLOAD_CHARS;

  return {
    kind: 'agent',
    async execute(
      node: PipelineNode,
      context: Record<string, unknown>,
      helpers: StepHandlerHelpers,
    ): Promise<StepOutcome> {
      const emit = helpers.emit as unknown as LooseEmit;
      const data = node.data as unknown as AgentNodeData;

      // A bad subscriber must never take down the step.
      const safeEmit = async (payload: Record<string, unknown>): Promise<void> => {
        try {
          await emit('pipeline:step:agent:tool', payload);
        } catch {
          /* ignore */
        }
      };

      const rawPrompt = typeof data.prompt === 'string' ? data.prompt : '';
      if (!rawPrompt.trim()) {
        return { status: 'failed', error: 'agent step requires a non-empty prompt' };
      }
      const prompt = interpolateContext(rawPrompt, context);
      const systemPrompt = data.systemPrompt
        ? interpolateContext(data.systemPrompt, context)
        : undefined;

      const model = data.model?.trim() || defaultModel;
      const maxTurns = Number.isFinite(data.maxTurns) && (data.maxTurns as number) > 0
        ? (data.maxTurns as number)
        : DEFAULT_MAX_TURNS;
      const timeoutMs = Number.isFinite(data.timeoutMs) && (data.timeoutMs as number) > 0
        ? (data.timeoutMs as number)
        : DEFAULT_TIMEOUT_MS;

      // DEFAULT-DENY. Absent/empty allowlist means no tools at all, and
      // the output says so via `toolless` rather than looking like a
      // run that simply chose not to use one.
      const allowedTools = Array.isArray(data.allowedTools) ? data.allowedTools : [];
      const toolless = isToolless(allowedTools);

      let toolCalls: AgentToolCall[] = [];
      let text = '';
      let turns = 0;
      let tokensIn = 0;
      let tokensOut = 0;
      let reportedModel = model;
      let stopReason: AgentStepOutput['stopReason'] = 'error';

      const buildOutput = (): AgentStepOutput => ({
        text,
        turns,
        toolCalls: toolCalls.map((c) => ({
          ...c,
          input: capToolPayload(c.input, maxChars),
          output: capToolPayload(c.output, maxChars),
        })),
        model: reportedModel,
        tokensIn,
        tokensOut,
        stopReason,
        toolless,
      });

      try {
        const stream = opts.agentClient.run(prompt, {
          model,
          ...(systemPrompt ? { systemPrompt } : {}),
          allowedTools,
          maxTurns,
          timeoutMs,
          signal: helpers.abortSignal,
          ...(opts.cwd ? { cwd: opts.cwd } : {}),
        });

        for await (const ev of stream) {
          if (ev.type === 'tool_start') {
            await safeEmit({
              runId: helpers.runId,
              stepId: helpers.stepId,
              callIndex: ev.callIndex,
              name: ev.name,
              phase: 'start',
              at: ev.at,
            });
          } else if (ev.type === 'tool_end') {
            await safeEmit({
              runId: helpers.runId,
              stepId: helpers.stepId,
              callIndex: ev.callIndex,
              name: ev.name,
              phase: 'end',
              ok: ev.ok,
              at: ev.at,
            });
          } else if (ev.type === 'done') {
            text = ev.text;
            turns = ev.turns;
            tokensIn = ev.tokensIn;
            tokensOut = ev.tokensOut;
            reportedModel = ev.model || model;
            stopReason = ev.stopReason;
            toolCalls = ev.toolCalls;
          }
        }
      } catch (err) {
        const isAbort = (err as Error)?.name === 'AbortError' || helpers.isCancelled();
        stopReason = isAbort ? 'cancelled' : 'error';
        const message = (err as Error)?.message ?? String(err);
        return {
          status: isAbort ? 'cancelled' : 'failed',
          output: buildOutput(),
          error: `agent step ${stopReason}: ${message}`,
        };
      }

      const output = buildOutput();

      // Bounded means bounded: a turn/timeout breach is a FAILURE with
      // the reason recorded, not a quietly-short success.
      if (stopReason === 'max_turns' || stopReason === 'timeout' || stopReason === 'error') {
        return {
          status: 'failed',
          output,
          error: `agent step stopped: ${stopReason}`,
        };
      }
      if (stopReason === 'cancelled') {
        return { status: 'cancelled', output };
      }

      return { status: 'completed', output };
    },
  };
}
