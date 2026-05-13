// platform-api/src/pipeline/worker-router.ts
//
// Phase M4.2a — the handler half of the bridge wire. Mounts an Express
// Router that exposes every easy PipelineBridge method as
// `POST /internal/pipeline/<methodName>` using the same `{args}` /
// `{result}` JSON envelope that HTTPPipelineBridge (the client side)
// produces.
//
// Lives in platform-api today so the round-trip is end-to-end testable
// against the local in-process bridge. When the pipeline-worker sibling
// repo materializes (Phase M4.2b), it imports this module and mounts the
// returned Router on its Express app. The bridge object passed in is the
// SAME shape platform-api uses today — no schema rewrite.
//
// PipelineBridge methods are almost all optional in the interface
// (`trigger?`, `cancelRun?`, etc.). At request time we check that the
// method exists on the concrete bridge before calling; absent methods
// surface as 501 Not Implemented rather than a 500-on-undefined-call. The
// in-process bridge from createBridge.ts implements every method we
// route here, so 501 is reserved for misconfiguration / stub bridges.
//
// What's covered: the 15 request/response methods plus 6 inspector
// sub-routes. DLQ is intentionally NOT exposed — its long-running
// redrive semantics need a different transport.
//
// What's NOT covered: PublicBusEvent fan-out (pipeline.run.reassigned
// etc.). The worker will publish those to event-catalog directly.

import { Router, type Request, type Response, type NextFunction } from 'express';
import type { PipelineBridge } from './bridge-types';
import { verifyEnvelope } from '@connorhoehn/service-runtime';
import type { Logger } from 'pino';

export interface WorkerRouterOptions {
  /**
   * Optional HMAC verification. When set, every request must carry a valid
   * `X-Service-Auth` header signed over the canonicalized body. Unsigned
   * requests are rejected with 401. When omitted, the router runs in
   * dev-mode (no auth) — production deployments MUST configure this.
   */
  verifyOpts?: {
    secret: string | string[];
    allowedServices: string[];
    toleranceSec?: number;
  };
  log?: Logger;
}

interface RpcRequestBody {
  args?: unknown[];
}

// Dispatch table — maps method-name on the wire to a function that pulls
// the bridge method and invokes it with the parsed args. The function is
// allowed to throw — the route wrapper turns thrown errors into 500s.
// Each entry expresses its own arg-shape contract via the args[] index
// destructuring; this is type-loose by necessity because the wire is
// JSON. The route wrapper guarantees the bridge method exists before
// dispatch.
type Dispatcher = (bridge: PipelineBridge, args: unknown[]) => Promise<unknown>;

const DISPATCH: Record<string, { method: keyof PipelineBridge; call: Dispatcher }> = {
  trigger: {
    method: 'trigger',
    call: async (b, args) => b.trigger!(args[0] as Parameters<NonNullable<PipelineBridge['trigger']>>[0]),
  },
  resolveBreakpoint: {
    method: 'resolveBreakpoint',
    call: async (b, args) => {
      b.resolveBreakpoint!(
        args[0] as string,
        args[1] as string,
        args[2] as 'continue' | 'step_over' | 'resume_all' | undefined,
      );
      return undefined;
    },
  },
  resumeFromStep: {
    method: 'resumeFromStep',
    call: async (b, args) =>
      b.resumeFromStep!(
        args[0] as string,
        args[1] as string,
        args[2] as Parameters<NonNullable<PipelineBridge['resumeFromStep']>>[2],
      ),
  },
  patchPausedContext: {
    method: 'patchPausedContext',
    call: async (b, args) =>
      b.patchPausedContext!(
        args[0] as string,
        args[1] as string,
        args[2] as Parameters<NonNullable<PipelineBridge['patchPausedContext']>>[2],
        args[3] as string | undefined,
      ),
  },
  revertContextPatch: {
    method: 'revertContextPatch',
    call: async (b, args) => {
      await b.revertContextPatch!(args[0] as string, args[1] as string, args[2] as string);
      return undefined;
    },
  },
  getRun: {
    method: 'getRun',
    call: async (b, args) => b.getRun(args[0] as string),
  },
  getHistory: {
    method: 'getHistory',
    call: async (b, args) => b.getHistory(args[0] as string, args[1] as number),
  },
  resolveApproval: {
    method: 'resolveApproval',
    call: async (b, args) => {
      await b.resolveApproval(
        args[0] as string,
        args[1] as string,
        args[2] as string,
        args[3] as 'approve' | 'reject',
        args[4] as string | undefined,
      );
      return undefined;
    },
  },
  listRuns: {
    method: 'listRuns',
    call: async (b, args) =>
      b.listRuns!(args[0] as string, args[1] as { limit: number; cursor?: string }),
  },
  listActiveRuns: {
    method: 'listActiveRuns',
    call: async (b) => b.listActiveRuns!(),
  },
  cancelRun: {
    method: 'cancelRun',
    call: async (b, args) => {
      await b.cancelRun!(args[0] as string, args[1] as string);
      return undefined;
    },
  },
  getPendingApprovals: {
    method: 'getPendingApprovals',
    call: async (b) => b.getPendingApprovals!(),
  },
  getLastEventAt: {
    method: 'getLastEventAt',
    call: async (b) => b.getLastEventAt!(),
  },
  getMetrics: {
    method: 'getMetrics',
    call: async (b) => b.getMetrics!(),
  },
  getDashboardData: {
    method: 'getDashboardData',
    call: async (b) => b.getDashboardData!(),
  },
};

// Inspector verbs — same dispatch shape but routes through getInspector().
const INSPECTOR_VERBS = [
  'paginatePending',
  'peekPending',
  'listInflight',
  'findStuckLeases',
  'summarizeQueue',
  'aggregateFailures',
] as const;

/**
 * Construct the worker Router. The bridge is invoked per-request — no
 * caching. Errors thrown by bridge methods land as 500 with
 * `{error: {message}}`; client side maps these to `BridgeRpcError`.
 */
export function createWorkerRouter(
  bridge: PipelineBridge,
  opts: WorkerRouterOptions = {},
): Router {
  const router = Router();
  const log = opts.log;

  if (opts.verifyOpts) {
    router.use(authMiddleware(opts.verifyOpts, log));
  }

  // Easy methods
  for (const [methodName, entry] of Object.entries(DISPATCH)) {
    router.post(`/${methodName}`, async (req: Request, res: Response) => {
      if (typeof bridge[entry.method] !== 'function') {
        res.status(501).json({
          error: { message: `bridge does not implement ${methodName}` },
        });
        return;
      }
      const args = parseArgs(req);
      try {
        const result = await entry.call(bridge, args);
        res.json({ result });
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        res.status(500).json({ error: { message } });
      }
    });
  }

  // Inspector sub-routes — bridge.getInspector() optional; absent → 501
  for (const verb of INSPECTOR_VERBS) {
    router.post(`/inspector.${verb}`, async (req: Request, res: Response) => {
      if (typeof bridge.getInspector !== 'function') {
        res.status(501).json({
          error: { message: `bridge does not implement getInspector` },
        });
        return;
      }
      const inspector = bridge.getInspector() as unknown as Record<
        string,
        ((arg: unknown) => unknown) | undefined
      >;
      const fn = inspector?.[verb];
      if (typeof fn !== 'function') {
        res.status(501).json({
          error: { message: `inspector does not implement ${verb}` },
        });
        return;
      }
      const args = parseArgs(req);
      try {
        const result = await fn(args[0]);
        res.json({ result });
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        res.status(500).json({ error: { message } });
      }
    });
  }

  return router;
}

function parseArgs(req: Request): unknown[] {
  const body = req.body as RpcRequestBody | undefined;
  return Array.isArray(body?.args) ? body.args : [];
}

/**
 * Service-auth verification middleware. Re-serializes the request body
 * via the canonical form HTTPPipelineBridge uses on the client side
 * (`JSON.stringify({args})`) and verifies the X-Service-Auth header
 * against it. Mismatched / stale / missing headers → 401.
 */
function authMiddleware(
  verifyOpts: NonNullable<WorkerRouterOptions['verifyOpts']>,
  log?: Logger,
): (req: Request, res: Response, next: NextFunction) => void {
  return (req, res, next) => {
    const header = req.get('x-service-auth');
    if (!header) {
      res.status(401).json({ error: { message: 'missing X-Service-Auth header' } });
      return;
    }
    const canonicalBody = JSON.stringify({ args: parseArgs(req) });
    const verdict = verifyEnvelope(header, {
      ...verifyOpts,
      payload: canonicalBody,
    });
    if (!verdict.ok) {
      log?.warn(
        { reason: verdict.reason, path: req.path },
        'pipeline worker-router rejected request — auth verification failed',
      );
      res.status(401).json({ error: { message: `auth: ${verdict.reason}` } });
      return;
    }
    next();
  };
}
