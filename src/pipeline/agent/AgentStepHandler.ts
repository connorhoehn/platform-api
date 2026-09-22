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
  AgentLabSession,
  AgentNodeData,
  AgentStepOutput,
  AgentToolCall,
} from './types';
import { DEFAULT_MAX_TURNS, DEFAULT_TIMEOUT_MS } from './types';
import { isToolless } from './ClaudeCodeAgentClient';
import { rememberRunLab } from './DevboxAgentClient';

/**
 * `pipeline:step:agent:tool` is not in DC's `PipelineEventMap`, which
 * is a `type` alias and so cannot be module-augmented from here. The
 * runtime EventBus accepts arbitrary string topics; this cast is the
 * same bridge JudgeStepHandler and visionHandlerFactory use. It drops
 * without a payload change once DC's map lists the event.
 */
type LooseEmit = (event: string, payload: unknown) => Promise<void>;

export interface AgentStepHandlerOpts {
  /** `runtime: 'local'` (the default, and every pre-devbox definition). */
  agentClient: AgentClient;
  /**
   * `runtime: 'devbox'`. Absent means the devbox runtime is not configured
   * here: a node that asks for it fails saying so rather than silently
   * running somewhere the author did not choose.
   */
  devboxAgentClient?: AgentClient;
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

/**
 * The devbox session an earlier step of this run opened. Step outputs merge
 * into the run context, so the first devbox step's `lab` is simply there for
 * the next one — and rides the checkpoint, so it survives a restart.
 */
export function labFromContext(context: Record<string, unknown>): AgentLabSession | undefined {
  const lab = context.lab;
  if (!lab || typeof lab !== 'object') return undefined;
  const { sessionId, boxId } = lab as { sessionId?: unknown; boxId?: unknown };
  if (typeof sessionId !== 'string' || !sessionId) return undefined;
  return { sessionId, boxId: typeof boxId === 'string' ? boxId : '' };
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
      const safeEmit = async (event: string, payload: Record<string, unknown>): Promise<void> => {
        try {
          await emit(event, payload);
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

      // Which runtime runs it. Absent means 'local', so nothing written
      // before the devbox runtime existed changes behaviour.
      const runtime: 'local' | 'devbox' = data.runtime === 'devbox' ? 'devbox' : 'local';
      const client = runtime === 'devbox' ? opts.devboxAgentClient : opts.agentClient;
      if (!client) {
        return { status: 'failed', error: 'devbox runtime unavailable: no devbox agent client is configured' };
      }

      // THE RUN'S BUDGET IS THE RUN'S, AND THE SESSION IS SHARED.
      //
      // Every devbox step of a run resumes ONE session, so the context it
      // reports is cumulative: step 2 starts wherever step 1 finished. Once
      // some step has spent the run's `contextBudgetTokens`, there is no room
      // left for the ones behind it — and making them each burn an iteration
      // to rediscover that (and fail) is how a three-step loop ended up
      // dead-lettered on step 2 with nothing to show.
      //
      // So a run that has hit its budget marks the context, and later agent
      // steps stop before they start, saying so. No tokens, no turns, no
      // pretending the step ran.
      if (runtime === 'devbox' && context.budgetExhausted === true) {
        return {
          status: 'completed',
          output: {
            text: '', turns: 0, toolCalls: [], model: data.model?.trim() || defaultModel,
            tokensIn: 0, tokensOut: 0, stopReason: 'budget' as const, toolless: false,
            iterations: 0, contextTokens: 0, runtime,
            skipped: true,
            skippedReason: 'the run reached its context budget before this step started',
          } satisfies AgentStepOutput & { skipped: boolean; skippedReason: string },
        };
      }

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
      let iterations = 0;
      let contextTokens = 0;
      let lab: AgentLabSession | undefined;

      const contextBudgetTokens = Number.isFinite(data.contextBudgetTokens) && (data.contextBudgetTokens as number) > 0
        ? (data.contextBudgetTokens as number)
        : undefined;

      const buildOutput = (): AgentStepOutput & { lab?: AgentLabSession } => ({
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
        iterations: iterations || turns,
        contextTokens: contextTokens || tokensIn,
        ...(contextBudgetTokens ? { contextBudgetTokens } : {}),
        runtime,
        ...(lab?.sessionId ? { sessionId: lab.sessionId, boxId: lab.boxId } : {}),
        // Merged into the run context by the executor, so the NEXT devbox
        // step of this run reuses the same box instead of opening its own.
        ...(lab?.sessionId ? { lab } : {}),
      });

      try {
        const stream = client.run(prompt, {
          model,
          ...(systemPrompt ? { systemPrompt } : {}),
          allowedTools,
          maxTurns,
          timeoutMs,
          signal: helpers.abortSignal,
          ...(opts.cwd ? { cwd: opts.cwd } : {}),
          ...(runtime === 'devbox'
            ? {
                runId: helpers.runId,
                stepId: helpers.stepId,
                ...(contextBudgetTokens ? { contextBudgetTokens } : {}),
                ...(labFromContext(context) ? { lab: labFromContext(context)! } : {}),
                ...(typeof context.requestedBy === 'string' && context.requestedBy
                  ? { requestedBy: context.requestedBy }
                  : {}),
                ...(data.label ? { label: data.label } : {}),
              }
            : {}),
        });

        for await (const ev of stream) {
          if (ev.type === 'session') {
            lab = { sessionId: ev.sessionId, boxId: ev.boxId };
            rememberRunLab(helpers.runId, lab);
          } else if (ev.type === 'iteration') {
            iterations = ev.iteration;
            contextTokens = ev.contextTokens;
            await safeEmit('pipeline:step:agent:iteration', {
              runId: helpers.runId,
              stepId: helpers.stepId,
              iteration: ev.iteration,
              contextTokens: ev.contextTokens,
              ...(ev.contextBudgetTokens ? { contextBudgetTokens: ev.contextBudgetTokens } : {}),
              model: ev.model,
            });
          } else if (ev.type === 'tool_start') {
            await safeEmit('pipeline:step:agent:tool', {
              runId: helpers.runId,
              stepId: helpers.stepId,
              callIndex: ev.callIndex,
              name: ev.name,
              phase: 'start',
              at: ev.at,
            });
          } else if (ev.type === 'tool_end') {
            await safeEmit('pipeline:step:agent:tool', {
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
            if (typeof ev.iterations === 'number') iterations = ev.iterations;
            if (typeof ev.contextTokens === 'number') contextTokens = ev.contextTokens;
            if (ev.sessionId) {
              lab = { sessionId: ev.sessionId, boxId: ev.boxId ?? '' };
              rememberRunLab(helpers.runId, lab);
            }
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

      // A BUDGET STOP IS A BOUND, NOT A FAULT.
      //
      // The others in the list below are failures: the agent was still
      // working and we cut it off. Running out of the context budget is the
      // loop doing exactly what it was told — it is the ceiling the person
      // typed (`200k`), reached. Treating it as a step failure failed the
      // whole run, sent it to the dead-letter queue as an UnknownError, and
      // tore the session down, for a run that had simply spent its budget.
      //
      // So it completes, carrying `stopReason: 'budget'` for anything that
      // renders it, and sets `budgetExhausted` in the context (step outputs
      // merge into it) so the steps behind it stop before they start.
      if (stopReason === 'budget') {
        return {
          status: 'completed',
          output: { ...output, budgetExhausted: true },
        };
      }

      // Bounded means bounded: a turn/timeout breach is a FAILURE with the
      // reason recorded, not a quietly-short success.
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
