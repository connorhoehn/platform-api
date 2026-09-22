// Starting an agent loop WITHOUT making the composer wait for the planner.
//
// The planner is a real model call in a real CLI: measured at 21-24s in the
// deployed pod, capped at 90s. Running it inside the POST meant the chat box
// sat on an open HTTP request for the whole of it — a composer cannot do
// that, and a curl with `-m 60` simply never came back.
//
// So the POST does the only two things it can do quickly: it names the loop,
// and it writes that name down. Everything slow happens after the 202.
//
//   POST  → allocate loopId, persist a PENDING row, answer 202 with
//           `card.lab.planSource = 'pending'` and no steps yet
//   later → plan, build the definition, trigger the real run, and record the
//           executor's run id against the loop
//
// TWO IDS, ONE THAT THE CLIENT EVER SEES. The executor allocates its own run
// id at trigger time and will not take one from us, but the client already
// has a URL it is polling by the time planning finishes. So the loop id is
// the public one, the executor id is recorded beside it, and
// `/api/agent-loops/:runId*` resolves either (see defaultLoadRun in
// routes/agentLoops.ts, which follows `agentLoopRunId`).
//
// NEVER A LOST RUN. `planAgentLoop` does not throw — a planner that is down
// yields the single-step fallback. What can still fail is the trigger, and
// that failure lands on the persisted row as `failed` with the reason on it,
// so a loop that never started says why instead of spinning on 'planning'
// until someone gives up.
import { randomUUID } from 'crypto';
import { AppError } from '../middleware/error-handler';
import { pipelineRunRepo } from '../repositories';
import type { PipelineRunItem } from '../repositories/PipelineRunRepository';
import type { ChatCommandRunInput } from './chatCommandRegistry';
import {
  CHAT_AGENT_LOOP_PIPELINE_ID, buildAgentLoopRequest,
  parseAgentLoopCommand, planAgentLoop, type AgentLoopRequest, type LoopPlan,
} from './agentLoopPlanner';
import log from '../lib/logger';

/** The plan a loop has before its planner has answered. */
export const PENDING_PLAN: LoopPlan = { source: 'pending' as unknown as LoopPlan['source'], steps: [] };

/**
 * `planning` is not a `RunItemStatus`, and inventing one would leak a
 * vocabulary the rest of the pipeline machinery does not share. The row is
 * `pending` — which is exactly what it is — and the LOOP view derives
 * 'planning' from the pending plan. One status word, two audiences.
 */
export const PENDING_ROW_STATUS = 'pending' as PipelineRunItem['status'];

export interface AgentLoopRunnerDeps {
  /** Persist the pending row and its later transitions. */
  putRun: (item: PipelineRunItem) => Promise<void>;
  getRun: (runId: string) => Promise<PipelineRunItem | null>;
  /** The planner. Injected so a test can make it slow, or make it fail. */
  plan: (request: AgentLoopRequest) => Promise<LoopPlan>;
  /**
   * Re-enter the ordinary chat-command path with the finished plan, and
   * return the executor's run id. Deliberately NOT a direct `bridge.trigger`
   * call: the shared path also does the organization join and the work-graph
   * projection, and a second copy of that here would drift from it.
   */
  rerun: (input: ChatCommandRunInput) => Promise<string>;
  now: () => Date;
}

const nowIso = (d: Date): string => d.toISOString();

/** The row a loop has while it is being planned. Readable by every GET on the way. */
export function pendingRow(loopId: string, request: AgentLoopRequest, at: Date): PipelineRunItem {
  return {
    pipelineId: CHAT_AGENT_LOOP_PIPELINE_ID,
    runId: loopId,
    status: PENDING_ROW_STATUS,
    triggeredBy: request.requestedBy,
    triggerType: 'manual',
    startedAt: nowIso(at),
    checkpoint: {
      sequenceNumber: 0,
      at: nowIso(at),
      context: {
        loopId,
        instruction: request.instruction,
        hints: request.hints,
        model: request.model,
        contextBudgetTokens: request.contextBudgetTokens,
        chatChannel: request.chatChannel,
        requestedBy: request.requestedBy,
        ...(request.requestedByName ? { requestedByName: request.requestedByName } : {}),
        plan: PENDING_PLAN,
      },
      currentStepIds: [],
    },
  } as unknown as PipelineRunItem;
}

export function defaultRunnerDeps(): AgentLoopRunnerDeps {
  return {
    putRun: (item) => pipelineRunRepo.put(item),
    getRun: (runId) => pipelineRunRepo.get(CHAT_AGENT_LOOP_PIPELINE_ID, runId),
    plan: async (request) => {
      // Imported lazily: the planner client is a CLI process, and nothing
      // should construct one just by loading this module.
      const { getAgentLoopPlannerClient } = await import('./builtinChatCommands.js');
      return planAgentLoop(request, getAgentLoopPlannerClient());
    },
    rerun: async (input) => {
      const { runChatCommand } = await import('./chatCommandRegistry.js');
      const out = await runChatCommand(input);
      return out.runId;
    },
    now: () => new Date(),
  };
}

/** The command as parsed, with who asked and where. */
export function requestFromInput(input: ChatCommandRunInput): AgentLoopRequest {
  const request = buildAgentLoopRequest(
    parseAgentLoopCommand(input.argumentText, input.extra ?? {}),
    {
      chatChannel: input.channel,
      requestedBy: input.requestedBy,
      ...(input.requestedByName ? { requestedByName: input.requestedByName } : {}),
      ...(input.documentId ? { references: { documentId: input.documentId } } : {}),
    },
  );
  if (!request.instruction) throw new AppError(400, '/agent needs something to do');
  return request;
}

/** Put a loop out of its misery, with the reason on the row. */
export async function markLoopFailed(
  loopId: string,
  request: AgentLoopRequest | null,
  reason: string,
  deps: AgentLoopRunnerDeps,
): Promise<void> {
  const existing = await deps.getRun(loopId);
  const row = existing ?? (request ? pendingRow(loopId, request, deps.now()) : null);
  if (!row) return;
  await deps.putRun({
    ...row,
    status: 'failed' as PipelineRunItem['status'],
    error: reason,
    completedAt: nowIso(deps.now()),
  });
}

/**
 * Plan, then start the real run through the ordinary command path. Runs AFTER
 * the 202 has been sent, so nothing here may throw into a caller — there is
 * no caller left. Every outcome ends on the persisted row.
 */
export async function planAndTrigger(
  loopId: string,
  request: AgentLoopRequest,
  input: ChatCommandRunInput,
  deps: AgentLoopRunnerDeps,
): Promise<void> {
  try {
    const plan = await deps.plan(request);
    // The plan in `extra` is what makes the start hook DECLINE on this pass,
    // so the real run goes down the same path as every other command.
    const executorRunId = await deps.rerun({
      ...input,
      clientPostsCard: true,
      extra: { ...input.extra, agentLoop: { request, plan }, agentLoopId: loopId },
    });

    const row = (await deps.getRun(loopId)) ?? pendingRow(loopId, request, deps.now());
    await deps.putRun({
      ...row,
      agentLoopRunId: executorRunId,
      checkpoint: {
        ...row.checkpoint!,
        context: { ...row.checkpoint?.context, plan, executorRunId },
      },
    } as PipelineRunItem);
    log.info({ loopId, executorRunId, planSource: plan.source, steps: plan.steps.length }, 'agent loop planned and started');
  } catch (err) {
    const reason = err instanceof Error ? err.message : String(err);
    log.error({ loopId, err: reason }, 'agent loop failed to start');
    await markLoopFailed(loopId, request, `the agent loop could not be started: ${reason}`, deps)
      .catch(() => undefined);
  }
}

export async function startAgentLoop(
  input: ChatCommandRunInput,
  deps: AgentLoopRunnerDeps = defaultRunnerDeps(),
  onPlanned?: (p: Promise<void>) => void,
): Promise<{ runId: string; request: AgentLoopRequest }> {
  const request = requestFromInput(input);
  const loopId = randomUUID();
  // Written BEFORE the 202. A loop the client has been told about must never
  // 404 on the URL it was handed, and the only way to guarantee that is for
  // the record to exist before the id leaves this process.
  await deps.putRun(pendingRow(loopId, request, deps.now()));
  log.info({ loopId, channel: request.chatChannel, userId: request.requestedBy }, 'agent loop accepted, planning in the background');

  const planning = planAndTrigger(loopId, request, input, deps).catch(() => undefined);
  if (onPlanned) onPlanned(planning);
  return { runId: loopId, request };
}

/**
 * Loops that were mid-plan when this process died.
 *
 * The planner runs in memory: a restart — a deploy, or a Tilt live_update
 * SIGTERM — takes it with no trace, leaving a row that says 'pending' for
 * ever and a devbox session nobody will ever tear down. There is nothing to
 * resume: the plan was never written and the executor never heard of the run.
 * So the honest ending is a failure with the reason, plus stopping the box so
 * it does not keep costing money for a run that is not happening.
 *
 * Only loops still waiting on their planner are touched. One that reached the
 * executor has an `agentLoopRunId` and is an ordinary run — the existing
 * orphan/resume machinery owns it, and failing it here would be wrong.
 */
export async function failOrphanedLoops(args: {
  listPending: () => Promise<PipelineRunItem[]>;
  putRun: (item: PipelineRunItem) => Promise<void>;
  stopSession?: (sessionId: string) => Promise<unknown>;
  now?: () => Date;
}): Promise<{ failed: string[]; sessionsStopped: number }> {
  const now = args.now ?? (() => new Date());
  const failed: string[] = [];
  let sessionsStopped = 0;

  const rows = await args.listPending();
  for (const row of rows) {
    if (row.pipelineId !== CHAT_AGENT_LOOP_PIPELINE_ID) continue;
    if (row.agentLoopRunId) continue;
    const context = (row.checkpoint?.context ?? {}) as Record<string, unknown>;
    const plan = (context.plan ?? {}) as { source?: unknown };
    if (plan.source !== 'pending') continue;

    await args.putRun({
      ...row,
      status: 'failed' as PipelineRunItem['status'],
      error: 'the agent loop could not be started: platform restarted while it was being planned',
      completedAt: now().toISOString(),
    });
    failed.push(row.runId);

    const lab = (context.lab ?? {}) as { sessionId?: unknown };
    if (typeof lab.sessionId === 'string' && lab.sessionId && args.stopSession) {
      try { await args.stopSession(lab.sessionId); sessionsStopped += 1; } catch { /* best effort */ }
    }
  }
  if (failed.length) log.warn({ loops: failed, sessionsStopped }, 'agent loops orphaned by a restart were failed');
  return { failed, sessionsStopped };
}
