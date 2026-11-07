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
import { effectiveAgentSettings } from '../runOverrides';

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

/**
 * Keep the whole step output comfortably under runPersistence's 100 KB cap.
 *
 * That cap does not trim the output — it REPLACES it with
 * `{truncated:true,size}`. So an agent step with a real tool trace lost
 * everything on the way to storage, and a page reload after the run showed
 * empty `iterations`/`contextTokens`/`stopReason` and read as a failure. The
 * trace is the expendable part; the numbers are not.
 */
const MAX_PERSISTED_OUTPUT_CHARS = 60_000;

/**
 * How much of the earlier steps' work a later prompt may carry.
 *
 * A deck step drafts from what the run has already done, so each agent step
 * appends its final text to `deckPriorWork` in the run context. Capped from
 * the FRONT (the most recent work is the relevant work) so a long run cannot
 * grow a prompt without bound.
 */
export const MAX_PRIOR_WORK_CHARS = 6_000;

/** Earlier steps' output, newest last, bounded. */
export function appendPriorWork(existing: unknown, label: string, text: string): string {
  const before = typeof existing === 'string' && !existing.startsWith('(') ? existing : '';
  const addition = text.trim() ? `## ${label}\n${text.trim()}` : '';
  if (!addition) return before || '(the earlier steps produced no text)';
  const joined = before ? `${before}\n\n${addition}` : addition;
  return joined.length <= MAX_PRIOR_WORK_CHARS ? joined : joined.slice(-MAX_PRIOR_WORK_CHARS);
}

/**
 * The few numbers the lab is built from, small enough that nothing will ever
 * truncate them. Mirrored into the run CONTEXT under `loopSteps` so they
 * survive independently of the step output — see buildOutput.
 */
export interface AgentLoopStepFacts {
  iterations: number;
  contextTokens: number;
  contextBudgetTokens?: number;
  stopReason: AgentStepOutput['stopReason'];
  tokensIn: number;
  tokensOut: number;
  model: string;
  runtime: 'local' | 'devbox';
  sessionId?: string;
  boxId?: string;
  skipped?: boolean;
}

/**
 * The highest context any earlier step of this run reported.
 *
 * The devbox session is the RUN's and its context accumulates across steps,
 * but a client is constructed per step and would otherwise start counting at
 * zero — which is how a run ended at 215.8k against a 200k budget.
 */
export function spentContextTokens(context: Record<string, unknown>): number {
  let spent = 0;
  for (const facts of Object.values(loopStepsFromContext(context))) {
    const value = (facts as { contextTokens?: unknown })?.contextTokens;
    if (typeof value === 'number' && Number.isFinite(value) && value > spent) spent = value;
  }
  return spent;
}

/** The `loopSteps` map as it stands in the run context. */
export function loopStepsFromContext(context: Record<string, unknown>): Record<string, AgentLoopStepFacts> {
  const raw = context.loopSteps;
  if (!raw || typeof raw !== 'object') return {};
  return { ...(raw as Record<string, AgentLoopStepFacts>) };
}

/**
 * Drop the tool payloads (keeping every call's name, outcome and timing) when
 * the output would otherwise be replaced wholesale on its way to storage.
 */
export function fitPersistedOutput<T extends { toolCalls: AgentToolCall[] }>(output: T): T {
  let serialized: string;
  try {
    serialized = JSON.stringify(output) ?? '';
  } catch {
    return output;
  }
  if (serialized.length <= MAX_PERSISTED_OUTPUT_CHARS) return output;
  return {
    ...output,
    toolCalls: output.toolCalls.map((call) => ({
      ...call,
      input: undefined,
      output: '[dropped: the step output was too large to persist whole]',
    })),
  };
}

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

      // DEFAULT-DENY. Absent/empty allowlist means no tools at all, and
      // the output says so via `toolless` rather than looking like a
      // run that simply chose not to use one.
      const allowedTools = Array.isArray(data.allowedTools) ? data.allowedTools : [];
      const toolless = isToolless(allowedTools);

      // TOOLS RUN IN THE MICROVM, NEVER IN THIS POD. A local agent step is
      // `claude -p` inside platform-api, next to its secrets and credentials;
      // a Bash tool there was a shell in the service (security review,
      // 6 November 2026). So a step that asks for tools runs on the devbox
      // runtime or not at all.
      if (runtime === 'local' && !toolless) {
        return {
          status: 'failed',
          error: 'agent step requests tools, and tool-using agent steps run only on the devbox runtime (set runtime: \'devbox\')',
        };
      }
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
      // THE RUN'S OVERRIDES WIN. A trigger that chose a model or a budget for
      // this run (a run draft's dispatch) is honoured here: the model outright,
      // the budget as a cap under the step's own (runOverrides.ts).
      const effective = effectiveAgentSettings(
        { model: data.model, contextBudgetTokens: data.contextBudgetTokens as number | undefined },
        context,
        defaultModel,
      );

      if (runtime === 'devbox' && context.budgetExhausted === true) {
        const model = effective.model;
        const facts: AgentLoopStepFacts = {
          iterations: 0, contextTokens: 0, stopReason: 'budget',
          tokensIn: 0, tokensOut: 0, model, runtime, skipped: true,
        };
        return {
          status: 'completed',
          output: {
            text: '', turns: 0, toolCalls: [], model,
            tokensIn: 0, tokensOut: 0, stopReason: 'budget' as const, toolless: false,
            iterations: 0, contextTokens: 0, runtime,
            skipped: true,
            skippedReason: 'the run reached its context budget before this step started',
            loopSteps: { ...loopStepsFromContext(context), [helpers.stepId]: facts },
          },
        };
      }

      const model = effective.model;
      const maxTurns = Number.isFinite(data.maxTurns) && (data.maxTurns as number) > 0
        ? (data.maxTurns as number)
        : DEFAULT_MAX_TURNS;
      const timeoutMs = Number.isFinite(data.timeoutMs) && (data.timeoutMs as number) > 0
        ? (data.timeoutMs as number)
        : DEFAULT_TIMEOUT_MS;

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

      const contextBudgetTokens = effective.contextBudgetTokens;

      const stepFacts = (): AgentLoopStepFacts => ({
        iterations: iterations || turns,
        contextTokens: contextTokens || tokensIn,
        ...(contextBudgetTokens ? { contextBudgetTokens } : {}),
        stopReason,
        tokensIn,
        tokensOut,
        model: reportedModel,
        runtime,
        ...(lab?.sessionId ? { sessionId: lab.sessionId, boxId: lab.boxId } : {}),
      });

      const buildOutput = (): AgentStepOutput & { lab?: AgentLabSession; loopSteps?: Record<string, AgentLoopStepFacts> } => fitPersistedOutput({
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
        // THE NUMBERS, KEPT OUT OF HARM'S WAY.
        //
        // Step outputs merge into the run context, and this map is a few
        // dozen bytes per step — so it rides the persisted checkpoint whole
        // even when the step's own output is replaced by a truncation marker
        // on the way to storage. Without it, a reload after a run with a real
        // tool trace showed no iterations, no context and no stop reason, and
        // the lab read `failed` for a run that had finished.
        loopSteps: { ...loopStepsFromContext(context), [helpers.stepId]: stepFacts() },
        // What a later deck step drafts from.
        deckPriorWork: appendPriorWork(context.deckPriorWork, data.label ?? helpers.stepId, text),
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
                // What earlier steps already spent in this shared session.
                ...(spentContextTokens(context) > 0 ? { startingContextTokens: spentContextTokens(context) } : {}),
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
