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
  CHAT_AGENT_LOOP_PIPELINE_ID, MAX_SOURCE_TITLES, MAX_TRANSCRIPT_PROMPT_CHARS,
  buildAgentLoopRequest, parseAgentLoopCommand, planAgentLoop,
  type AgentLoopRequest, type AgentLoopSource, type LoopPlan,
} from './agentLoopPlanner';
import log from '../lib/logger';
import { getLocalReplica } from '../lib/replicaLiveness';

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
        // Which process is planning (NFR #26): the orphan sweep leaves the
        // loop alone while this replica's liveness lease says it is up.
        ...(getLocalReplica() ? { plannerNodeId: getLocalReplica()!.nodeId } : {}),
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

/**
 * Documents the conversation named, which a deck step may cite. Read from
 * what the composer sent (`extra.documents`, the `#`-referenced documents)
 * and the conversation's recordings; NEVER invented, because a citation the
 * user did not choose is a citation to something they cannot check.
 */
export function sourcesFromInput(input: ChatCommandRunInput): AgentLoopSource[] {
  const out: AgentLoopSource[] = [];
  const seen = new Set<string>();
  const push = (raw: unknown, kind?: string): void => {
    if (!raw || typeof raw !== 'object') return;
    const r = raw as Record<string, unknown>;
    const documentId = typeof r.documentId === 'string' ? r.documentId : (typeof r.id === 'string' ? r.id : '');
    const title = typeof r.title === 'string' ? r.title : (typeof r.name === 'string' ? r.name : '');
    if (!documentId || seen.has(documentId) || out.length >= MAX_SOURCE_TITLES) return;
    seen.add(documentId);
    out.push({ documentId, title: (title || documentId).slice(0, 160), ...(kind ? { kind } : {}) });
  };

  const documents = input.extra?.documents;
  if (Array.isArray(documents)) for (const d of documents) push(d);
  const references = input.extra?.references;
  if (Array.isArray(references)) for (const r of references) push(r);
  const transcripts = input.extra?.transcripts;
  if (Array.isArray(transcripts)) for (const t of transcripts) push(t, 'transcript');
  if (input.documentId) push({ documentId: input.documentId, title: 'The open document' });
  return out;
}

/** Transcript text the deck may quote, capped before it ever reaches a prompt. */
export function transcriptFromInput(input: ChatCommandRunInput): string | undefined {
  const raw = input.extra?.transcript;
  if (typeof raw !== 'string' || !raw.trim()) return undefined;
  return raw.trim().slice(0, MAX_TRANSCRIPT_PROMPT_CHARS);
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
  const sources = sourcesFromInput(input);
  const transcript = transcriptFromInput(input);
  return {
    ...request,
    ...(sources.length ? { sources } : {}),
    ...(transcript ? { transcript } : {}),
  };
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
    // The model is unavailable (breaker open): the loop fails now, with the
    // reason, rather than starting a single step that asks the same model.
    if (plan.unavailable) {
      log.warn({ loopId, reason: plan.unavailable }, 'agent loop not started: model unavailable');
      await markLoopFailed(loopId, request, plan.unavailable, deps).catch(() => undefined);
      return;
    }
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
  const at = deps.now();
  await deps.putRun(pendingRow(loopId, request, at));
  // The Computer Lab tab finds this loop through here, not through a GSI the
  // table does not have. Best-effort: a missing pointer costs a listing row,
  // and must never cost the run itself.
  try {
    await deps.putRun(personDayIndexRow(loopId, request, at));
  } catch (err) {
    log.warn({ loopId, err: err instanceof Error ? err.message : String(err) }, 'agent loop person-day index row failed');
  }
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
  /**
   * Is the replica planning this loop still running it? A loop whose planner
   * is alive is NOT orphaned, however long planning takes (NFR #26 / #11:
   * with two replicas, the other pod's boot must not fail our loops). Omitted
   * → the old single-process meaning: every pending loop is an orphan.
   */
  isPlannerLive?: (plannerNodeId: string | undefined, startedAtMs: number) => Promise<boolean>;
  now?: () => Date;
}): Promise<{ failed: string[]; sessionsStopped: number; skippedLive: number }> {
  const now = args.now ?? (() => new Date());
  const failed: string[] = [];
  let sessionsStopped = 0;
  let skippedLive = 0;

  const rows = await args.listPending();
  for (const row of rows) {
    if (row.pipelineId !== CHAT_AGENT_LOOP_PIPELINE_ID) continue;
    if (row.agentLoopRunId) continue;
    const context = (row.checkpoint?.context ?? {}) as Record<string, unknown>;
    const plan = (context.plan ?? {}) as { source?: unknown };
    if (plan.source !== 'pending') continue;
    if (args.isPlannerLive) {
      const owner = typeof context.plannerNodeId === 'string' ? context.plannerNodeId : undefined;
      if (await args.isPlannerLive(owner, Date.parse(row.startedAt))) { skippedLive += 1; continue; }
    }

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
  if (failed.length) log.warn({ loops: failed, sessionsStopped, skippedLive }, 'agent loops orphaned by a restart were failed');
  return { failed, sessionsStopped, skippedLive };
}

// ─── Finding a person's loops without a GSI ───────────────────────────
//
// The obvious query — "runs this person triggered, newest first" — wants the
// `triggeredBy-startedAt-index` GSI. The deployed table does not have it
// (local DDB answered `The table does not have the specified index`), and a
// scan is not an answer: it reads every run of every pipeline to find one
// person's day.
//
// So the write side pays instead. Each loop writes ONE extra row into the
// same table under a synthetic partition key, and the read side queries that
// partition directly. Both sides are a single Query on the table's own key.
//
// PARTITIONED BY UTC DAY, NOT LOCAL DAY. The requester's timezone is not
// known when the row is written — the chat command does not carry one — and
// guessing it would file runs under a day nobody asked about. A UTC day is a
// fact at write time; the reader knows the timezone, asks for the one to
// three UTC days its local window touches, and filters precisely.
//
// These rows are NOT runs. `status: 'index'` keeps them out of every
// status-keyed query in the service (the orphan scans ask for 'pending' and
// 'running'), and the partition key is not a pipeline id anybody triggers.

/** `loops-by-person#dev-frank#2026-09-21` — the partition one person's day lives in. */
export function personDayPartition(userId: string, utcDay: string): string {
  return `loops-by-person#${userId}#${utcDay}`;
}

/** `YYYY-MM-DD` of an ISO instant, in UTC. */
export function utcDayOf(iso: string): string {
  return iso.slice(0, 10);
}

/** Every UTC day a local-day window touches. One, two, or (at extreme offsets) three. */
export function utcDaysSpanned(startIso: string, endIso: string): string[] {
  const days: string[] = [];
  const start = Date.parse(startIso);
  const end = Date.parse(endIso);
  if (!Number.isFinite(start) || !Number.isFinite(end)) return days;
  // The window is half-open, so the last instant that can appear in it is
  // one millisecond before `end` — using `end` itself would pull in a day
  // that can hold nothing.
  for (let t = start; t <= end - 1; t += 24 * 3_600_000) {
    days.push(new Date(t).toISOString().slice(0, 10));
  }
  const lastDay = new Date(end - 1).toISOString().slice(0, 10);
  if (!days.includes(lastDay)) days.push(lastDay);
  return days;
}

/** The pointer row. Its `runId` is the loop id; everything else is bookkeeping. */
export function personDayIndexRow(loopId: string, request: AgentLoopRequest, at: Date): PipelineRunItem {
  return {
    pipelineId: personDayPartition(request.requestedBy, utcDayOf(nowIso(at))),
    runId: loopId,
    status: 'index' as PipelineRunItem['status'],
    triggeredBy: request.requestedBy,
    triggerType: 'agent-loop-index',
    startedAt: nowIso(at),
    // 90 days, matching the run rows these point at.
    ttl: Math.floor(at.getTime() / 1000) + 90 * 24 * 60 * 60,
  } as unknown as PipelineRunItem;
}

/** Loop ids this person started on the UTC days the window touches, newest first. */
export async function loopIdsForPersonDays(
  userId: string,
  startIso: string,
  endIso: string,
  listPartition: (partition: string, limit: number) => Promise<PipelineRunItem[]>,
  limit = 200,
): Promise<string[]> {
  const days = utcDaysSpanned(startIso, endIso);
  const rows: PipelineRunItem[] = [];
  for (const day of days) {
    rows.push(...await listPartition(personDayPartition(userId, day), limit));
  }
  return rows
    .sort((a, b) => Date.parse(b.startedAt ?? '') - Date.parse(a.startedAt ?? ''))
    .map((r) => r.runId);
}
