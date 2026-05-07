// Pipeline HTTP router factory.
//
// IMPORTANT — module-shaped artifact:
//   This file is the seam where the pipeline HTTP surface stops being
//   "social-api code" and becomes "PipelineModule code that happens to be
//   hosted in social-api today". It's structured so that, when the module
//   relocates into distributed-core, the file itself is `git mv`ed
//   without modification. The host (today: social-api/src/app.ts) injects
//   everything that varies between hosts via the `PipelineRouterDeps`
//   options bag — auth, persistence lookup, the bridge into the runtime.
//
//   Constraints:
//     - Only `express` is imported. No social-api middleware, no Cognito,
//       no DDB clients, no distributed-core internals.
//     - All cross-process knowledge (who the user is, where definitions
//       live, how to call the runtime) flows through `deps`.
//     - When this file is ported into distributed-core, the host wires
//       its own deps (DB-backed `getDefinition`, its own auth shape, etc.)
//       and the route handlers themselves are unchanged.
//
// Refs:
//   - .planning/HANDOFF-distributed-core-social-api-absorption.md
//     §"Reframe item 1", §"Item 2 is the most important to get right"

import {
  Router,
  type NextFunction,
  type Request,
  type Response,
  type RequestHandler,
} from 'express';
import type {
  AuthContext,
  InlinePipelineDefinition,
  PipelineErrorBody,
  PipelineRunDetailResponse,
  PipelineTriggerRequest,
  PipelineTriggerResponse,
} from './contracts';

// ---------------------------------------------------------------------------
// Host-injected dependencies.
// ---------------------------------------------------------------------------

export interface TriggerArgs {
  pipelineId: string;
  definition?: InlinePipelineDefinition;
  triggeredBy: { userId: string };
  triggerPayload?: Record<string, unknown>;
  breakpoints?: readonly string[];
}

export interface PipelineRouterDeps {
  /**
   * Resolve the requesting principal from the Express request. The host
   * is responsible for whatever transport produces it — Cognito JWT,
   * dummy dev-user, header-based service-to-service, etc.
   */
  getAuthContext: (req: Request) => AuthContext;

  /**
   * Look up the canonical pipeline definition. Called when the request
   * body lacks one — frontends are encouraged not to ship inline copies
   * since the server is the source of truth.
   * Return `null` for unknown pipelines.
   */
  getDefinition: (
    userId: string,
    pipelineId: string,
  ) => Promise<InlinePipelineDefinition | null>;

  /**
   * Bridge into the PipelineModule runtime. Returns the new `runId`.
   * Throws on bridge-not-wired or framework-side validation errors —
   * the router catches and wraps as 5xx/`PipelineErrorBody`.
   */
  trigger: (args: TriggerArgs) => Promise<{ runId: string }>;

  /**
   * Optional: pre-populate run-persistence state so terminal events can
   * find the run even when the framework's `pipeline:run:started` lands
   * after the terminal event (validation failures, fast-fail pipelines).
   */
  registerTrigger?: (args: {
    runId: string;
    pipelineId: string;
    userId: string;
    triggerType: string;
    at: string;
  }) => void;

  /**
   * Live, in-memory run state read straight from the runtime. Has full
   * step input/output for runs the runtime is currently executing or
   * has recently completed. Returns `null` when the runtime has no
   * record of `runId` (already evicted, never reached this node, etc.).
   *
   * The router calls this first — it's the freshest source.
   */
  getLiveRun?: (
    runId: string,
  ) => Promise<PipelineRunDetailResponse | null> | PipelineRunDetailResponse | null;

  /**
   * Durable run snapshot from the host's persistent store. Used as a
   * fallback when the live runtime no longer has the row in memory.
   * Required for the run-detail endpoint to serve historical runs;
   * if absent, the endpoint can still serve live runs but 404s on
   * anything older.
   */
  getStoredRun?: (
    pipelineId: string,
    runId: string,
  ) => Promise<PipelineRunDetailResponse | null>;
}

// ---------------------------------------------------------------------------
// Internal helpers — no `express` middleware imported from social-api.
// ---------------------------------------------------------------------------

/** Local async-handler wrapper so we don't depend on social-api's. */
function wrap(
  fn: (req: Request, res: Response, next: NextFunction) => Promise<void>,
): RequestHandler {
  return (req, res, next) => {
    Promise.resolve(fn(req, res, next)).catch(next);
  };
}

function badRequest(
  res: Response<PipelineErrorBody>,
  action: string,
  message: string,
): void {
  res.status(400).json({ action, error: message });
}

function bridgeError(
  res: Response<PipelineErrorBody>,
  action: string,
  err: unknown,
): void {
  const message = err instanceof Error ? err.message : String(err);
  res.status(502).json({ action, error: message });
}

// ---------------------------------------------------------------------------
// Router factory.
// ---------------------------------------------------------------------------

export function createPipelineRouter(deps: PipelineRouterDeps): Router {
  const router = Router();

  // POST /pipelines/:pipelineId/runs — trigger a new run.
  router.post(
    '/pipelines/:pipelineId/runs',
    wrap(async (req, res) => {
      const { pipelineId } = req.params as { pipelineId: string };
      const { userId } = deps.getAuthContext(req);
      const body = (req.body ?? {}) as PipelineTriggerRequest;

      if (body.definition && body.definition.id !== pipelineId) {
        badRequest(
          res,
          'trigger',
          `definition.id (${body.definition.id}) does not match :pipelineId (${pipelineId})`,
        );
        return;
      }

      // Server-side definition resolution (canonical store wins when the
      // body has none). The frontend can stop shipping the definition at
      // all once every caller is updated; the inline path stays for
      // unsaved-edit previews from the editor.
      let resolvedDefinition = body.definition;
      if (!resolvedDefinition) {
        const fromStore = await deps.getDefinition(userId, pipelineId);
        if (fromStore) resolvedDefinition = fromStore;
      }

      let runId: string;
      try {
        const out = await deps.trigger({
          pipelineId,
          definition: resolvedDefinition,
          triggeredBy: { userId },
          triggerPayload: body.triggerPayload,
          breakpoints: body.breakpoints,
        });
        runId = out.runId;
      } catch (err) {
        bridgeError(res, 'trigger', err);
        return;
      }

      const at = new Date().toISOString();

      // Pre-populate persistence so terminal-only runs (e.g. validation
      // failures) still land a row in the operator's run history.
      deps.registerTrigger?.({
        runId,
        pipelineId,
        userId,
        triggerType: 'manual',
        at,
      });

      const response: PipelineTriggerResponse = {
        runId,
        pipelineId,
        triggeredBy: { userId, triggerType: 'manual' },
        at,
      };
      res.status(202).json(response);
    }),
  );

  // GET /pipelines/:pipelineId/runs/:runId — single-run snapshot.
  //
  // Live-then-store fallback: the runtime's in-memory snapshot is the
  // freshest (full step inputs/outputs while a run is active), but it
  // gets evicted once the run leaves the active set. The persistent
  // store is the authoritative long-term home. Return whichever
  // succeeds first; only 404 when both miss.
  router.get(
    '/pipelines/:pipelineId/runs/:runId',
    wrap(async (req, res) => {
      const { pipelineId, runId } = req.params as {
        pipelineId: string;
        runId: string;
      };
      // AuthContext extracted so the host can audit if it wants; the
      // handler itself is read-only and doesn't gate on userId.
      void deps.getAuthContext(req);

      const live = deps.getLiveRun
        ? await Promise.resolve(deps.getLiveRun(runId))
        : null;
      if (live) {
        res.json(live);
        return;
      }

      const stored = deps.getStoredRun
        ? await deps.getStoredRun(pipelineId, runId)
        : null;
      if (stored) {
        res.json(stored);
        return;
      }

      res.status(404).json({ action: 'getRun', error: 'run not found' });
    }),
  );

  return router;
}
