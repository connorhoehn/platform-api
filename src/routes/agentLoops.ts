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
  devboxBaseUrl, devboxSecret, recallRunLab,
} from '../pipeline/agent/DevboxAgentClient';
import { SERVICE_AUTH_HEADER, signEnvelope } from '@connorhoehn/service-runtime';
import { CHAT_AGENT_LOOP_PIPELINE_ID, type LoopPlan } from '../chat/agentLoopPlanner';
import { agentLoopNodeId } from '../chat/agentLoopPlanner';
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
}

export interface AgentLoopView {
  runId: string;
  pipelineId: string;
  status: string;
  model: string;
  contextBudgetTokens: number;
  contextTokens: number;
  iterations: number;
  lab: { sessionId?: string; boxId?: string; runtime: string };
  steps: AgentLoopStepView[];
  plan: { source: string; steps: Array<{ id: string; title: string; kind: string }>; notice?: string };
}

/** The run as this route needs it, from wherever it is still known. */
export interface LoadedAgentLoop {
  runId: string;
  pipelineId: string;
  status: string;
  context: Record<string, unknown>;
  steps: Record<string, { status?: string; output?: unknown }>;
}

export interface AgentLoopsDeps {
  loadRun: (runId: string) => Promise<LoadedAgentLoop | null>;
  isChannelViewer: (channel: string, userId: string) => Promise<boolean>;
  devbox: () => DevboxAgentClient;
  cancelRun: (runId: string, userId: string) => Promise<void>;
  /** The tail proxy's transport, so a test can stand in a stream. */
  fetchImpl: (input: string, init?: RequestInit) => Promise<FetchResponse>;
}

const str = (v: unknown): string => (typeof v === 'string' ? v : '');
const num = (v: unknown): number => (typeof v === 'number' && Number.isFinite(v) ? v : 0);
const obj = (v: unknown): Record<string, unknown> => (v && typeof v === 'object' ? v as Record<string, unknown> : {});

/** Live executor first (it has the in-flight step statuses), then the durable row. */
async function defaultLoadRun(runId: string): Promise<LoadedAgentLoop | null> {
  const bridge = getPipelineBridge();
  if (bridge?.getRun) {
    try {
      const snap = await Promise.resolve(bridge.getRun(runId)) as Record<string, unknown> | null;
      if (snap) {
        return {
          runId,
          pipelineId: str(snap.pipelineId) || CHAT_AGENT_LOOP_PIPELINE_ID,
          status: str(snap.status) || 'unknown',
          context: obj(snap.context),
          steps: obj(snap.steps) as LoadedAgentLoop['steps'],
        };
      }
    } catch { /* fall through to the row */ }
  }
  // Keyed by (pipelineId, runId); an agent loop is always this pipeline.
  const item = await pipelineRunRepo.get(CHAT_AGENT_LOOP_PIPELINE_ID, runId);
  if (!item) return null;
  const steps: LoadedAgentLoop['steps'] = {};
  for (const [nodeId, summary] of Object.entries(item.stepsSummary ?? {})) {
    steps[nodeId] = { ...(summary.status ? { status: summary.status } : {}), output: summary.output };
  }
  return {
    runId,
    pipelineId: item.pipelineId,
    status: item.status,
    context: obj(item.checkpoint?.context),
    steps,
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
};

/** The plan the trigger payload left in the run context. */
export function planOf(context: Record<string, unknown>): LoopPlan {
  const raw = obj(context.plan);
  const steps = Array.isArray(raw.steps) ? raw.steps : [];
  return {
    source: str(raw.source) === 'fallback' ? 'fallback' : 'planner',
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

/** The whole view, assembled from the plan and whatever each step has reported. */
export function viewOf(run: LoadedAgentLoop): AgentLoopView {
  const plan = planOf(run.context);
  const budget = num(run.context.contextBudgetTokens);
  const model = str(run.context.model) || 'sonnet';

  let contextTokens = 0;
  let iterations = 0;
  const steps: AgentLoopStepView[] = plan.steps.map((step) => {
    const nodeId = agentLoopNodeId(step.id);
    const execution = run.steps[nodeId] ?? {};
    const output = obj(execution.output);
    const stepIterations = num(output.iterations);
    const stepContext = num(output.contextTokens);
    iterations += stepIterations;
    if (stepContext > contextTokens) contextTokens = stepContext;
    return {
      id: step.id,
      nodeId,
      title: step.title,
      kind: step.kind,
      status: str(execution.status) || 'pending',
      iterations: stepIterations,
      contextTokens: stepContext,
      tokensIn: num(output.tokensIn),
      tokensOut: num(output.tokensOut),
      ...(str(output.stopReason) ? { stopReason: str(output.stopReason) } : {}),
    };
  });

  return {
    runId: run.runId,
    pipelineId: run.pipelineId,
    status: run.status,
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
  };
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

  router.get('/:runId', asyncHandler(async (req: Request, res: Response): Promise<void> => {
    res.json(viewOf(await authorize(req)));
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

    const body: Record<string, unknown> = { principal: { service: 'platform-api', requestedBy: req.user!.sub } };
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
    await deps.cancelRun(run.runId, req.user!.sub);
    const sessionId = labOf(run).sessionId;
    let sessionStopped = false;
    if (sessionId) sessionStopped = await deps.devbox().deleteSession(sessionId).catch(() => false);
    res.status(202).json({ stopped: true, runId: run.runId, sessionStopped });
  }));

  return router;
}

export const agentLoopsRouter = createAgentLoopsRouter();
