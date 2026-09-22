// /api/agent-loops — what the chat box needs to BE the computer lab.
//
//   GET  /:runId             the loop: status, model, budget, steps, plan, lab
//   GET  /:runId/tail        SSE, proxied straight off the box (replay + live)
//   POST /:runId/instruct    a line typed at the `›` prompt, into the session
//   POST /:runId/shell-url   a browser terminal on the box (requester only)
//   POST /:runId/stop        cancel the run and stop the session
//
// AUTH, the same shape as /api/chat/commands: the person who typed the
// command, or anyone who can see the conversation it was typed in. The one
// exception is `shell-url`, which is a live shell on a machine holding the
// requester's work — that stays the requester's alone, because "can read the
// channel" is not the same permission as "can type into the box".
//
// WHERE THE ANSWER COMES FROM: the live executor while the run is in this
// process, the persisted run row after that. The lab (`{ sessionId, boxId }`)
// rides the run CONTEXT — the devbox step returns it as output and outputs
// merge into context — so it lands in the checkpoint that runPersistence
// already writes, and a GET after a restart still knows which box to tail.
import { Router, type Request, type Response } from 'express';
import { GetCommand } from '@aws-sdk/lib-dynamodb';
import { asyncHandler, AppError, NotFoundError, ValidationError } from '../middleware/error-handler';
import { getPipelineBridge } from '../pipeline/bridge-types';
import { pipelineRunRepo } from '../repositories';
import { docClient } from '../lib/aws-clients';
import { tableName } from '../lib/ddb-table-name';
import {
  DevboxAgentClient, DevboxUnavailableError, DEVBOX_BASE_PATH, DEVBOX_UNAVAILABLE,
  devboxBaseUrl, devboxSecret, recallRunLab, type DevboxFileEntry,
} from '../pipeline/agent/DevboxAgentClient';
import { localDayBounds, todayIn } from '../chat/localDay';
import { loopIdsForPersonDays } from '../chat/agentLoopRunner';
import { computeCost } from '../pipeline/llm-pricing';
import type { PipelineRunItem } from '../repositories/PipelineRunRepository';
import { SERVICE_AUTH_HEADER, signEnvelope } from '@connorhoehn/service-runtime';
import { CHAT_AGENT_LOOP_PIPELINE_ID, type LoopPlan } from '../chat/agentLoopPlanner';
import { agentLoopNodeId, deckLoopNodeId, deckRenderTitle } from '../chat/agentLoopPlanner';
import log from '../lib/logger';

/**
 * `Response` in this module is Express's. The fetch one still has to be
 * nameable for the tail proxy's transport — without this alias the deps
 * signature silently means "an Express response", which compiles nowhere.
 */
type FetchResponse = globalThis.Response;

const CHAT_MEMBERS_TABLE = tableName(process.env.DYNAMODB_CHAT_MEMBERS_TABLE || 'chat-members');

/** What a person may type at the prompt in one go. */
export const MAX_INSTRUCT_CHARS = 4_000;
/** A downloadable artifact, not a disk image. */
export const MAX_FILE_BYTES = 10 * 1024 * 1024;
/** Rows the Computer Lab tab shows for one day. */
export const MAX_LIST_RESULTS = 50;
/** Rows read before the day filter narrows them. */
export const MAX_LIST_SCAN = 200;

export interface AgentLoopStepView {
  id: string;
  nodeId: string;
  title: string;
  kind: string;
  status: string;
  iterations: number;
  contextTokens: number;
  tokensIn: number;
  tokensOut: number;
  stopReason?: string;
  /** Why the step failed, in the executor's own words. The UI shows this. */
  error?: string;
  /** The executor's raw message, when `error` is the loop's plainer wording. */
  errorDetail?: string;
  /** True when the run's budget was gone before this step started. */
  skipped?: boolean;
  /** What a `deck` step produced, once it has. */
  result?: { documentId: string; revisionId?: string; slideCount?: number; title?: string };
}

/** A document a run made: the deck it rendered, for the card and the pane. */
export interface AgentLoopProduced {
  documentId: string;
  title: string;
  revisionId?: string;
  slideCount?: number;
}

/** One file the run left in its workspace, as the Files tab lists it. */
export interface AgentLoopArtifact {
  id: string;
  name: string;
  sizeBytes?: number;
  updatedAt?: string;
}

/** A task the run says it is about. Only ever echoed, never inferred. */
export interface AgentLoopWorkItem {
  key: string;
  title: string;
  status: string;
  tags: string[];
}

export interface AgentLoopView {
  runId: string;
  pipelineId: string;
  status: string;
  /** The box, named: its boxId, else the pipeline and a short run id. */
  label: string;
  project?: string;
  startedAt?: string;
  /** Now − started while running; finished − started once it is over. */
  elapsedMs?: number;
  /** Omitted entirely when the run recorded no cost — never shown as $0. */
  costUsd?: number;
  /** 'devbox:task' once a session exists, 'local:agent' otherwise. */
  environment: string;
  model: string;
  contextBudgetTokens: number;
  contextTokens: number;
  iterations: number;
  lab: { sessionId?: string; boxId?: string; runtime: string };
  steps: AgentLoopStepView[];
  plan: { source: string; steps: Array<{ id: string; title: string; kind: string }>; notice?: string };
  artifacts: AgentLoopArtifact[];
  /** Why `artifacts` is empty, when the reason is not "there are none". */
  artifactsNotice?: string;
  workItem?: AgentLoopWorkItem;
  /** Documents this run made — a rendered deck, today. Empty until it has. */
  produced: AgentLoopProduced[];
}

/** One row of the Computer Lab tab: a person's loop, summarised. */
export interface AgentLoopSummary {
  runId: string;
  pipelineId: string;
  status: string;
  label: string;
  title: string;
  model: string;
  startedAt?: string;
  completedAt?: string;
  elapsedMs?: number;
  costUsd?: number;
  environment: string;
  contextTokens: number;
  contextBudgetTokens: number;
  steps: number;
  chatChannel?: string;
  produced: AgentLoopProduced[];
}

/** The run as this route needs it, from wherever it is still known. */
export interface LoadedAgentLoop {
  runId: string;
  pipelineId: string;
  status: string;
  context: Record<string, unknown>;
  steps: Record<string, { status?: string; output?: unknown; error?: string }>;
  startedAt?: string;
  completedAt?: string;
  /** From the run row's own cost fields. Absent means the run recorded none. */
  costUsd?: number;
  /**
   * Set when `runId` is a LOOP id: the executor run the planner started under
   * it. Cancelling, and anything else the executor owns, uses this.
   */
  executorRunId?: string;
}

export interface AgentLoopsDeps {
  loadRun: (runId: string) => Promise<LoadedAgentLoop | null>;
  isChannelViewer: (channel: string, userId: string) => Promise<boolean>;
  devbox: () => DevboxAgentClient;
  cancelRun: (runId: string, userId: string) => Promise<void>;
  /** The tail proxy's transport, so a test can stand in a stream. */
  fetchImpl: (input: string, init?: RequestInit) => Promise<FetchResponse>;
  /** The loops this person started inside the half-open window, newest first. */
  listRunsForDay: (userId: string, startIso: string, endIso: string, limit: number) => Promise<LoadedAgentLoop[]>;
  now: () => number;
}

const str = (v: unknown): string => (typeof v === 'string' ? v : '');
const num = (v: unknown): number => (typeof v === 'number' && Number.isFinite(v) ? v : 0);
const obj = (v: unknown): Record<string, unknown> => (v && typeof v === 'object' ? v as Record<string, unknown> : {});

/**
 * The run's own cost, from the fields runPersistence already writes.
 *
 * `estimatedCostUsd` when the run has one, else priced from its per-model
 * token usage. Returns undefined — NOT zero — when the run recorded neither:
 * a loop of agent steps emits no `llm:response` events, so "no cost data" is
 * the normal case, and showing it as `$0.0000` would be a made-up number.
 */
export function costOfRun(item: Pick<PipelineRunItem, 'estimatedCostUsd' | 'tokenUsage'>): number | undefined {
  if (typeof item.estimatedCostUsd === 'number' && Number.isFinite(item.estimatedCostUsd)) {
    return item.estimatedCostUsd;
  }
  const usage = item.tokenUsage;
  if (!usage || Object.keys(usage).length === 0) return undefined;
  let total = 0;
  let priced = false;
  for (const [model, u] of Object.entries(usage)) {
    const cost = computeCost(model, u.tokensIn ?? 0, u.tokensOut ?? 0);
    if (typeof cost === 'number' && Number.isFinite(cost) && cost > 0) { total += cost; priced = true; }
  }
  return priced ? total : undefined;
}

/** A persisted run row in the shape the routes read. */
export function fromRunItem(item: PipelineRunItem): LoadedAgentLoop {
  const steps: LoadedAgentLoop['steps'] = {};
  for (const [nodeId, summary] of Object.entries(item.stepsSummary ?? {})) {
    steps[nodeId] = {
      ...(summary.status ? { status: summary.status } : {}),
      ...(summary.error ? { error: summary.error } : {}),
      output: summary.output,
    };
  }
  const costUsd = costOfRun(item);
  return {
    runId: item.runId,
    pipelineId: item.pipelineId,
    status: item.status,
    context: obj(item.checkpoint?.context),
    steps,
    ...(item.startedAt ? { startedAt: item.startedAt } : {}),
    ...(item.completedAt ? { completedAt: item.completedAt } : {}),
    ...(costUsd !== undefined ? { costUsd } : {}),
  };
}

/**
 * Live executor for the in-flight step statuses, the durable row for the
 * things only it records (cost, and the timestamps after a restart). Both,
 * because the detail pane wants a running run's live steps AND its cost, and
 * neither source has both.
 */
async function defaultLoadRun(runId: string): Promise<LoadedAgentLoop | null> {
  const first = await loadOne(runId);
  // A LOOP id: the row written at 202 time, pointing at the run its planner
  // eventually started. The client keeps polling the loop id it was given, so
  // the executor's state is folded in under that id rather than swapped for it.
  if (first?.executorRunId && first.executorRunId !== runId) {
    const executor = await loadOne(first.executorRunId);
    if (executor) {
      return {
        ...executor,
        runId,                                   // what the client asked for
        executorRunId: first.executorRunId,      // what to cancel
        // The loop row holds the plan and the command; the executor row holds
        // the live context. The executor's wins where it has anything.
        context: { ...first.context, ...executor.context },
        ...(first.startedAt ? { startedAt: first.startedAt } : {}),
      };
    }
  }
  return first;
}

async function loadOne(runId: string): Promise<LoadedAgentLoop | null> {
  const bridge = getPipelineBridge();
  const [live, item] = await Promise.all([
    (async () => {
      if (!bridge?.getRun) return null;
      try { return await Promise.resolve(bridge.getRun(runId)) as Record<string, unknown> | null; } catch { return null; }
    })(),
    // Keyed by (pipelineId, runId); an agent loop is always this pipeline.
    pipelineRunRepo.get(CHAT_AGENT_LOOP_PIPELINE_ID, runId).catch(() => null),
  ]);
  if (!live && !item) return null;

  const stored = item ? fromRunItem(item) : null;
  const executorRunId = item?.agentLoopRunId
    || str(obj(item?.checkpoint?.context).executorRunId)
    || undefined;
  if (!live) return stored ? { ...stored, ...(executorRunId ? { executorRunId } : {}) } : null;
  return {
    runId,
    pipelineId: str(live.pipelineId) || stored?.pipelineId || CHAT_AGENT_LOOP_PIPELINE_ID,
    status: str(live.status) || stored?.status || 'unknown',
    context: Object.keys(obj(live.context)).length ? obj(live.context) : (stored?.context ?? {}),
    steps: Object.keys(obj(live.steps)).length ? obj(live.steps) as LoadedAgentLoop['steps'] : (stored?.steps ?? {}),
    ...(str(live.startedAt) || stored?.startedAt ? { startedAt: str(live.startedAt) || stored!.startedAt! } : {}),
    ...(str(live.completedAt) || stored?.completedAt ? { completedAt: str(live.completedAt) || stored!.completedAt! } : {}),
    ...(stored?.costUsd !== undefined ? { costUsd: stored.costUsd } : {}),
    ...(executorRunId ? { executorRunId } : {}),
  };
}

async function defaultIsChannelViewer(channel: string, userId: string): Promise<boolean> {
  if (!channel || !userId) return false;
  try {
    const res = await docClient.send(new GetCommand({ TableName: CHAT_MEMBERS_TABLE, Key: { channel, userId } }));
    return !!res.Item && res.Item.removedAt === undefined;
  } catch {
    // Fail CLOSED: an unreadable membership table is not permission.
    return false;
  }
}

const defaultDeps: AgentLoopsDeps = {
  loadRun: defaultLoadRun,
  isChannelViewer: defaultIsChannelViewer,
  devbox: () => new DevboxAgentClient(),
  cancelRun: async (runId, userId) => {
    const bridge = getPipelineBridge();
    if (!bridge?.cancelRun) throw new AppError(503, 'pipeline engine is not available');
    await Promise.resolve(bridge.cancelRun(runId, userId));
  },
  fetchImpl: (input, init) => fetch(input, init),
  /**
   * NOT `listByTriggeredBy`: that wants a `triggeredBy-startedAt-index` GSI
   * the deployed table does not have (it answered "The table does not have
   * the specified index" with a 500). The loop writes a pointer row per
   * person per UTC day at 202 time instead; this reads those partitions and
   * then the runs they name. Two key-only Queries and N bounded Gets — no
   * scan, and no index to provision.
   */
  listRunsForDay: async (userId, startIso, endIso, limit) => {
    const loopIds = await loopIdsForPersonDays(
      userId, startIso, endIso,
      (partition, cap) => pipelineRunRepo.listByPipeline(partition, { limit: cap }),
    );
    const wanted = loopIds.slice(0, limit);
    const rows = await Promise.all(wanted.map((id) => pipelineRunRepo.get(CHAT_AGENT_LOOP_PIPELINE_ID, id).catch(() => null)));
    return rows.filter((r): r is NonNullable<typeof r> => !!r).map(fromRunItem);
  },
  now: () => Date.now(),
};

/** The plan the trigger payload left in the run context. */
export function planOf(context: Record<string, unknown>): LoopPlan {
  const raw = obj(context.plan);
  const steps = Array.isArray(raw.steps) ? raw.steps : [];
  return {
    source: (['fallback', 'pending'] as string[]).includes(str(raw.source))
      ? (str(raw.source) as LoopPlan['source'])
      : 'planner',
    ...(str(raw.notice) ? { notice: str(raw.notice) } : {}),
    steps: steps.map((s) => obj(s)).map((s) => ({
      id: str(s.id), title: str(s.title), kind: str(s.kind) || 'other',
      prompt: str(s.prompt), dependsOn: Array.isArray(s.dependsOn) ? s.dependsOn.map(str) : [], done: str(s.done),
    })),
  } as LoopPlan;
}

/** The lab the run opened: the context is authoritative, the live map is the fallback. */
export function labOf(run: LoadedAgentLoop): { sessionId?: string; boxId?: string; runtime: string } {
  const fromContext = obj(run.context.lab);
  const sessionId = str(fromContext.sessionId) || recallRunLab(run.runId)?.sessionId || '';
  const boxId = str(fromContext.boxId) || recallRunLab(run.runId)?.boxId || '';
  return {
    ...(sessionId ? { sessionId } : {}),
    ...(boxId ? { boxId } : {}),
    // 'devbox' once a session exists; until then the run has not told us.
    runtime: sessionId ? 'devbox' : 'pending',
  };
}

/** The box, named. Its own id when it has one, else the pipeline and a short run id. */
export function labelOf(run: LoadedAgentLoop): string {
  const explicit = str(obj(run.context.lab).label) || str(run.context.labLabel);
  if (explicit) return explicit.slice(0, 120);
  const boxId = labOf(run).boxId;
  if (boxId) return boxId;
  return `${run.pipelineId || CHAT_AGENT_LOOP_PIPELINE_ID} · ${run.runId.slice(0, 8)}`;
}

/** Only what the run itself named. A project nobody recorded is omitted, not guessed. */
export function projectOf(run: LoadedAgentLoop): string | undefined {
  const project = str(run.context.project) || str(run.context.projectId)
    || str(obj(run.context.references).projectId);
  return project ? project.slice(0, 120) : undefined;
}

/** Wall clock: now − started while running, finished − started once it is over. */
export function elapsedMsOf(run: LoadedAgentLoop, now: number): number | undefined {
  if (!run.startedAt) return undefined;
  const started = Date.parse(run.startedAt);
  if (!Number.isFinite(started)) return undefined;
  const ended = run.completedAt ? Date.parse(run.completedAt) : NaN;
  const finish = Number.isFinite(ended) ? ended : now;
  return Math.max(0, finish - started);
}

/**
 * Where the work actually ran. A session means the devbox; anything else is
 * the in-pod agent, and saying 'devbox:task' for it would be a lie on a pane
 * whose whole job is to say which machine a person is looking at.
 */
export function environmentOf(run: LoadedAgentLoop): string {
  if (labOf(run).sessionId) return 'devbox:task';
  const anyDevboxStep = Object.values(run.steps)
    .some((step) => str(obj(step.output).runtime) === 'devbox');
  return anyDevboxStep ? 'devbox:task' : 'local:agent';
}

/**
 * The task this run is about, if the run SAID so. Read straight out of the
 * context the trigger payload left; never looked up, never inferred from a
 * title that happens to look like a ticket key.
 */
export function workItemOf(run: LoadedAgentLoop): AgentLoopWorkItem | undefined {
  const raw = obj(run.context.workItem);
  const key = str(raw.key) || str(raw.id);
  const title = str(raw.title);
  if (!key || !title) return undefined;
  const tags = Array.isArray(raw.tags) ? raw.tags.map(str).filter(Boolean).slice(0, 10) : [];
  return { key: key.slice(0, 80), title: title.slice(0, 200), status: str(raw.status) || 'unknown', tags };
}

/** The instruction, for a row that has no room for a plan. */
export function titleOf(run: LoadedAgentLoop): string {
  const instruction = str(run.context.instruction).trim();
  if (instruction) return instruction.slice(0, 200);
  const plan = planOf(run.context);
  return plan.steps[0]?.title ?? labelOf(run);
}

/** The whole view, assembled from the plan and whatever each step has reported. */
export function viewOf(run: LoadedAgentLoop, now: number = Date.now()): AgentLoopView {
  const plan = planOf(run.context);
  const budget = num(run.context.contextBudgetTokens);
  const model = str(run.context.model) || 'sonnet';

  let contextTokens = 0;
  let iterations = 0;
  let sawBudgetStop = false;

  // THE NUMBERS COME FROM WHICHEVER SOURCE STILL HAS THEM.
  //
  // runPersistence does not trim an over-large step output, it REPLACES it
  // with `{truncated:true,size}` — so after a run with a real tool trace, a
  // reload found no iterations, no context and no stop reason, and the lab
  // read `failed` for a run that had finished. The agent step therefore also
  // mirrors those few numbers into the run context under `loopSteps`, which
  // is small enough to survive, and they are read from there whenever the
  // step's own output no longer carries them.
  const facts = obj(run.context.loopSteps);

  const produced: AgentLoopProduced[] = [];

  const steps: AgentLoopStepView[] = plan.steps.flatMap((step) => {
    const nodeId = agentLoopNodeId(step.id);
    const execution = run.steps[nodeId] ?? {};
    const rawOutput = obj(execution.output);
    const fallback = obj(facts[nodeId]);
    // A truncated output keeps none of its fields; prefer it only where it
    // actually still has the value.
    const output: Record<string, unknown> = { ...fallback, ...rawOutput };
    if (rawOutput.truncated === true) Object.assign(output, fallback);
    const stepIterations = num(output.iterations);
    const stepContext = num(output.contextTokens);
    iterations += stepIterations;
    if (stepContext > contextTokens) contextTokens = stepContext;
    if (str(output.stopReason) === 'budget') sawBudgetStop = true;
    const view: AgentLoopStepView = {
      id: step.id,
      nodeId,
      title: step.kind === 'deck' ? `Draft slides for ${step.title}`.slice(0, 120) : step.title,
      kind: step.kind,
      status: str(execution.status) || 'pending',
      iterations: stepIterations,
      contextTokens: stepContext,
      tokensIn: num(output.tokensIn),
      tokensOut: num(output.tokensOut),
      ...(str(output.stopReason) ? { stopReason: str(output.stopReason) } : {}),
      // A step that failed must say why on the pane, not just go red.
      ...(str(execution.error) ? { error: str(execution.error) } : {}),
      ...(output.skipped === true ? { skipped: true } : {}),
    };

    // A deck step is TWO nodes. The draft is the agent row above; the render
    // is its own row, because "the model did not answer with a spec" and "the
    // renderer fell over" are different problems with different fixes.
    if (step.kind !== 'deck') return [view];

    const renderNodeId = deckLoopNodeId(step.id);
    const render = run.steps[renderNodeId] ?? {};
    const renderOutput = obj(render.output);
    const documentId = str(renderOutput.documentId);
    if (documentId) {
      produced.push({
        documentId,
        title: str(renderOutput.title) || step.title,
        ...(str(renderOutput.revisionId) ? { revisionId: str(renderOutput.revisionId) } : {}),
        ...(num(renderOutput.slideCount) ? { slideCount: num(renderOutput.slideCount) } : {}),
      });
    }
    const renderError = str(render.error);
    const renderView: AgentLoopStepView = {
      id: `${step.id}:render`,
      nodeId: renderNodeId,
      title: deckRenderTitle(step),
      kind: 'deck',
      status: str(render.status) || 'pending',
      iterations: 0,
      contextTokens: 0,
      tokensIn: 0,
      tokensOut: 0,
      ...(renderError ? deckErrorFields(renderError) : {}),
      ...(documentId
        ? {
            result: {
              documentId,
              ...(str(renderOutput.revisionId) ? { revisionId: str(renderOutput.revisionId) } : {}),
              ...(num(renderOutput.slideCount) ? { slideCount: num(renderOutput.slideCount) } : {}),
              ...(str(renderOutput.title) ? { title: str(renderOutput.title) } : {}),
            },
          }
        : {}),
    };
    return [view, renderView];
  });

  const elapsedMs = elapsedMsOf(run, now);
  const project = projectOf(run);
  const workItem = workItemOf(run);

  // The row says 'pending' because that is a status the pipeline machinery
  // understands. What a person is looking at is a planner still thinking, and
  // 'planning' is the word for that — derived here, not invented in storage.
  //
  // 'budget' likewise: a run whose steps stopped because the context ceiling
  // was reached did not fail and did not simply finish — it ran out of what
  // it was given, which is its own outcome and the one the lab renders.
  const terminal = ['completed', 'succeeded', 'failed', 'cancelled', 'canceled', 'rejected'].includes(run.status);
  const status = plan.source === 'pending' && !run.completedAt
    ? 'planning'
    : (sawBudgetStop && terminal ? 'budget' : run.status);

  return {
    runId: run.runId,
    pipelineId: run.pipelineId,
    status,
    label: labelOf(run),
    ...(project ? { project } : {}),
    ...(run.startedAt ? { startedAt: run.startedAt } : {}),
    ...(elapsedMs !== undefined ? { elapsedMs } : {}),
    ...(run.costUsd !== undefined ? { costUsd: run.costUsd } : {}),
    environment: environmentOf(run),
    model,
    contextBudgetTokens: budget,
    contextTokens,
    iterations,
    lab: labOf(run),
    steps,
    plan: {
      source: plan.source,
      ...(plan.notice ? { notice: plan.notice } : {}),
      steps: plan.steps.map((s) => ({ id: s.id, title: s.title, kind: s.kind })),
    },
    // Filled in by the route when the middleware answers; an empty list with
    // no notice means the workspace genuinely has no files.
    artifacts: [],
    ...(workItem ? { workItem } : {}),
    produced,
  };
}

/** One row of the Computer Lab tab. */
export function summaryOf(run: LoadedAgentLoop, now: number): AgentLoopSummary {
  const view = viewOf(run, now);
  return {
    runId: run.runId,
    pipelineId: run.pipelineId,
    status: view.status,
    label: view.label,
    title: titleOf(run),
    model: view.model,
    ...(run.startedAt ? { startedAt: run.startedAt } : {}),
    ...(run.completedAt ? { completedAt: run.completedAt } : {}),
    ...(view.elapsedMs !== undefined ? { elapsedMs: view.elapsedMs } : {}),
    ...(view.costUsd !== undefined ? { costUsd: view.costUsd } : {}),
    environment: view.environment,
    contextTokens: view.contextTokens,
    contextBudgetTokens: view.contextBudgetTokens,
    steps: view.steps.length,
    ...(str(run.context.chatChannel) ? { chatChannel: str(run.context.chatChannel) } : {}),
    produced: view.produced,
  };
}

/**
 * What the lab shows when a deck did not render.
 *
 * Under `MOCK_AGENTCORE` the drafting agent answers with the mock's prose,
 * which is not a DeckSpec, and the generator refuses it — correctly: a deck
 * rendered from nothing would be a fabricated document with a download
 * button. The refusal deserves a sentence a person can act on ("the draft
 * step did not produce a spec") rather than the generator's internals, so the
 * raw message is kept beside it rather than thrown away.
 */
export const DRAFT_NOT_A_SPEC = 'draft was not a DeckSpec';

export function deckErrorFields(rawError: string): { error: string; errorDetail?: string } {
  const notASpec = /no DeckSpec found|not an object|slides|must cite|expected/i.test(rawError);
  if (!notASpec) return { error: rawError };
  return { error: DRAFT_NOT_A_SPEC, errorDetail: rawError };
}

/** A run started inside the half-open window `[startIso, endIso)`. */
export function startedWithin(run: LoadedAgentLoop, startIso: string, endIso: string): boolean {
  if (!run.startedAt) return false;
  const at = Date.parse(run.startedAt);
  if (!Number.isFinite(at)) return false;
  return at >= Date.parse(startIso) && at < Date.parse(endIso);
}

/** A step the executor has started and not finished — the loop is talking to the box right now. */
export function isMidTask(run: LoadedAgentLoop): boolean {
  return Object.values(run.steps).some((s) => str(s.status) === 'running' || str(s.status) === 'in_progress');
}

export function createAgentLoopsRouter(deps: AgentLoopsDeps = defaultDeps): Router {
  const router = Router();

  /** Load + authorize in one place: every route below needs exactly this. */
  const authorize = async (req: Request, opts: { requesterOnly?: boolean } = {}): Promise<LoadedAgentLoop> => {
    const runId = String(req.params.runId ?? '').trim();
    if (!runId) throw new ValidationError('runId is required');
    const run = await deps.loadRun(runId);
    if (!run) throw new NotFoundError('agent loop not found');
    const userId = req.user!.sub;
    if (str(run.context.requestedBy) === userId) return run;
    if (opts.requesterOnly) throw new AppError(403, 'only the person who started this loop can open its shell');
    const channel = str(run.context.chatChannel);
    if (channel && await deps.isChannelViewer(channel, userId)) return run;
    // Not "403 on a run you cannot see" — a run you cannot see does not exist.
    throw new NotFoundError('agent loop not found');
  };

  /** The session id, or the honest reason there is none. */
  const sessionOf = (run: LoadedAgentLoop): string => {
    const sessionId = labOf(run).sessionId;
    if (!sessionId) throw new AppError(409, 'this loop has no cloud workspace yet');
    return sessionId;
  };

  /**
   * The person's loops for one local day — the Computer Lab tab.
   *
   * Registered BEFORE `/:runId`: Express matches in registration order, and
   * a bare `/api/agent-loops` must not be read as a run called "".
   */
  router.get('/', asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const viewer = req.user!.sub;
    const person = String(req.query.person ?? '').trim() || viewer;
    const timezone = String(req.query.timezone ?? '').trim() || 'UTC';
    const now = deps.now();
    const day = String(req.query.day ?? '').trim() || todayIn(timezone, new Date(now));
    const bounds = localDayBounds(day, timezone, new Date(now));

    // The lookup is already day-scoped (by UTC day); this narrows the edges
    // to the person's actual local window.
    const runs = await deps.listRunsForDay(person, bounds.startIso, bounds.endIso, MAX_LIST_SCAN);
    const onDay = runs.filter((run) => startedWithin(run, bounds.startIso, bounds.endIso));

    // Someone else's loops are visible only where the viewer can see the
    // conversation each one was started in — decided per run, because one
    // person's day can span channels the viewer is in and channels they are not.
    const visible: LoadedAgentLoop[] = [];
    if (person === viewer) {
      visible.push(...onDay);
    } else {
      const decided = new Map<string, boolean>();
      for (const run of onDay) {
        const channel = str(run.context.chatChannel);
        if (!channel) continue;
        if (!decided.has(channel)) decided.set(channel, await deps.isChannelViewer(channel, viewer));
        if (decided.get(channel)) visible.push(run);
      }
    }

    visible.sort((a, b) => Date.parse(b.startedAt ?? '') - Date.parse(a.startedAt ?? ''));
    res.json({
      person, day: bounds.day, timezone: bounds.timeZone,
      from: bounds.startIso, to: bounds.endIso,
      loops: visible.slice(0, MAX_LIST_RESULTS).map((run) => summaryOf(run, now)),
    });
  }));

  router.get('/:runId', asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const run = await authorize(req);
    const view = viewOf(run, deps.now());
    const sessionId = labOf(run).sessionId;

    // The workspace listing is best-effort by design: a finished run's box is
    // torn down, and an older middleware has no files route at all. Neither
    // is a reason to fail the pane — but the empty list has to say which.
    if (sessionId) {
      try {
        const files: DevboxFileEntry[] = await deps.devbox().listFiles(sessionId);
        view.artifacts = files;
      } catch (err) {
        view.artifactsNotice = err instanceof DevboxUnavailableError
          ? 'Workspace files unavailable · the runtime did not answer'
          : 'Workspace files could not be read';
        log.warn({ runId: run.runId, err: err instanceof Error ? err.message : String(err) }, 'agent loop artifacts unavailable');
      }
    } else {
      view.artifactsNotice = 'No cloud workspace for this run';
    }
    res.json(view);
  }));

  /**
   * One artifact's bytes, through platform auth rather than a middleware URL
   * handed to the browser — the box's router trusts a service signature, not
   * a person, so the only safe way to let someone download a file is to be
   * the one that fetches it.
   */
  router.get('/:runId/files/:fileId', asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const run = await authorize(req);
    const fileId = String(req.params.fileId ?? '').trim();
    if (!fileId) throw new ValidationError('fileId is required');
    const sessionId = sessionOf(run);

    let upstream: FetchResponse;
    try {
      upstream = await deps.devbox().fetchFile(sessionId, fileId);
    } catch (err) {
      if (err instanceof DevboxUnavailableError) throw new AppError(503, err.message);
      throw err;
    }

    // Refuse on the declared size before reading a byte, so an oversized file
    // costs one header rather than a 10 MB transfer.
    const declared = Number(upstream.headers.get('content-length') ?? '');
    if (Number.isFinite(declared) && declared > MAX_FILE_BYTES) {
      throw new AppError(413, `file is ${declared} bytes, over the ${MAX_FILE_BYTES} byte limit`);
    }

    res.status(200);
    res.setHeader('content-type', upstream.headers.get('content-type') || 'application/octet-stream');
    res.setHeader('content-disposition', `attachment; filename="${fileId.replace(/[^\w.\-]/g, '_')}"`);
    res.setHeader('cache-control', 'private, no-store');

    if (!upstream.body) { res.end(); return; }
    const reader = upstream.body.getReader();
    let sent = 0;
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        sent += value.byteLength;
        // A stream that lied about (or omitted) its length is still bounded.
        if (sent > MAX_FILE_BYTES) {
          await reader.cancel().catch(() => undefined);
          log.warn({ runId: run.runId, fileId, sent }, 'agent loop file exceeded the size cap mid-stream');
          break;
        }
        res.write(Buffer.from(value));
      }
    } finally {
      res.end();
    }
  }));

  // Replay + live, straight off the box. Frames pass through UNCHANGED: the
  // lab renders the same stream-json the middleware re-framed, and a proxy
  // that reshaped it would be a second contract to keep in step.
  router.get('/:runId/tail', asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const run = await authorize(req);
    const boxId = labOf(run).boxId;
    if (!boxId) throw new AppError(409, 'this loop has no cloud workspace yet');
    const base = devboxBaseUrl();
    const secret = devboxSecret();
    if (!base || !secret) throw new AppError(503, `${DEVBOX_UNAVAILABLE}: AGENTCORE_DEVBOX_URL is not configured`);

    // WHAT IS SIGNED HERE IS THE boxId, NOT A PRINCIPAL ENVELOPE.
    //
    // The envelope binds the signature to the request payload, and a GET has
    // no body — so the middleware's internal router verifies the tail against
    // `{ boxId }` (aws-agentcore middleware/src/routes/internal-devbox.ts:
    // "No request body on a GET, so the signed payload is the boxId itself").
    // Signing a principal object here instead produced a valid signature over
    // the wrong payload and the tail answered 401 for every run. The POST and
    // DELETE calls, which do carry a body, keep signing that body.
    const body: Record<string, unknown> = { boxId };
    const url = `${base.replace(/\/+$/, '')}${DEVBOX_BASE_PATH}/boxes/${encodeURIComponent(boxId)}/tail`;
    const upstream = await deps.fetchImpl(url, {
      method: 'GET',
      headers: {
        accept: 'text/event-stream',
        [SERVICE_AUTH_HEADER]: signEnvelope(body, { secret, serviceId: 'platform-api' }),
      },
    });
    if (upstream.status === 404) throw new AppError(503, `${DEVBOX_UNAVAILABLE}: middleware has no tail for this box`);
    if (!upstream.ok || !upstream.body) throw new AppError(502, `the devbox tail answered ${upstream.status}`);

    res.status(200);
    res.setHeader('content-type', 'text/event-stream');
    res.setHeader('cache-control', 'no-cache, no-transform');
    res.setHeader('connection', 'keep-alive');
    // Nothing between here and the box may buffer: a lab that shows the
    // whole run at the end is not a lab.
    res.setHeader('x-accel-buffering', 'no');
    if (typeof (res as { flushHeaders?: () => void }).flushHeaders === 'function') {
      (res as unknown as { flushHeaders: () => void }).flushHeaders();
    }

    const reader = upstream.body.getReader();
    let closed = false;
    req.on('close', () => { closed = true; reader.cancel().catch(() => undefined); });
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done || closed) break;
        res.write(Buffer.from(value));
      }
    } catch (err) {
      log.warn({ runId: run.runId, err: err instanceof Error ? err.message : String(err) }, 'agent loop tail ended early');
    } finally {
      if (!closed) res.end();
    }
  }));

  // A line typed at the `›` prompt. Fire-and-forget: the answer arrives on
  // the tail, not here. 409 while a step is mid-task — two prompts into one
  // session at once is how a workspace gets two half-done jobs in it.
  router.post('/:runId/instruct', asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const run = await authorize(req);
    const text = str((req.body as { text?: unknown } | undefined)?.text).trim();
    if (!text) throw new ValidationError('text is required');
    if (isMidTask(run)) throw new AppError(409, 'a step is running — wait for it to finish, or stop the loop');
    const sessionId = sessionOf(run);

    const client = deps.devbox();
    try {
      const body: Record<string, unknown> = {
        prompt: text.slice(0, MAX_INSTRUCT_CHARS),
        resume: true,
        principal: { service: 'platform-api', requestedBy: req.user!.sub },
      };
      const upstream = await client.startTask(sessionId, body);
      // The caller is not waiting on the stream; the tail carries the answer.
      upstream.body?.cancel().catch(() => undefined);
    } catch (err) {
      if (err instanceof DevboxUnavailableError) throw new AppError(503, err.message);
      throw err;
    }
    res.status(202).json({ accepted: true, runId: run.runId, sessionId });
  }));

  router.post('/:runId/shell-url', asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const run = await authorize(req, { requesterOnly: true });
    const sessionId = sessionOf(run);
    try {
      res.json(await deps.devbox().shellUrl(sessionId, req.user!.sub));
    } catch (err) {
      if (err instanceof DevboxUnavailableError) throw new AppError(503, err.message);
      throw err;
    }
  }));

  // Stop the run first: a session torn down under a running step would make
  // the step fail with a transport error instead of a cancellation.
  router.post('/:runId/stop', asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const run = await authorize(req);
    // The executor knows the run by ITS id; the loop id is the client's.
    await deps.cancelRun(run.executorRunId ?? run.runId, req.user!.sub);
    const sessionId = labOf(run).sessionId;
    let sessionStopped = false;
    if (sessionId) sessionStopped = await deps.devbox().deleteSession(sessionId).catch(() => false);
    res.status(202).json({ stopped: true, runId: run.runId, sessionStopped });
  }));

  return router;
}

export const agentLoopsRouter = createAgentLoopsRouter();
