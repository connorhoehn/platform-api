// platform-api/src/pipeline/wire-bridge.ts
//
// Phase M4.1 — chooses between the in-process and remote PipelineBridge at
// startup based on `PIPELINE_WORKER_URL`. When the env var is set, this
// platform-api process delegates the pipeline runtime to a sibling
// pipeline-worker over HTTP (HTTPPipelineBridge from Block 1). When unset
// (default), the local distributed-core PipelineModule is bootstrapped
// in-process exactly as it has been historically.
//
// Why split this out of `src/index.ts`:
//   1. The env-var branching is pure decision logic — easier to test in
//      isolation than via the boot script.
//   2. Future env-var knobs (sign-opts, timeout, fallback policy) can
//      grow here without making index.ts harder to read.
//   3. The shutdown handle the helper returns abstracts over "nothing to
//      shut down" (remote mode) and "stop the local module" (in-process
//      mode), so the caller treats both modes uniformly.

import { setPipelineBridge } from '../routes/pipelineTriggers';
import { bootstrapPipeline } from './bootstrap';
import { createBridge } from './createBridge';
import { HTTPPipelineBridge } from './http-bridge';
import type { PipelineRunRepository } from '../repositories/PipelineRunRepository';
import type { DLQRepository } from '../repositories/DLQRepository';
import type { Logger } from 'pino';

export interface WirePipelineBridgeDeps {
  pipelineRunRepository?: PipelineRunRepository;
  dlqRepository?: DLQRepository;
  log: Logger;
  env?: NodeJS.ProcessEnv;
}

export interface WirePipelineBridgeResult {
  mode: 'remote' | 'in-process';
  /** Resolves when the bridge is wired and `setPipelineBridge` has been called. */
  ready: Promise<void>;
  /** Idempotent shutdown for the bridge's owned resources. No-op in remote mode. */
  shutdown: () => Promise<void>;
}

/**
 * Wire the PipelineBridge singleton. Returns immediately with a `ready`
 * promise the caller can await (or fire-and-forget). Failures in remote
 * mode throw synchronously from this function; failures in in-process
 * mode reject `ready` but DO NOT throw synchronously — matching the
 * historical fire-and-forget contract.
 */
export function wirePipelineBridge(deps: WirePipelineBridgeDeps): WirePipelineBridgeResult {
  const env = deps.env ?? process.env;
  const workerUrl = env.PIPELINE_WORKER_URL?.trim();

  if (workerUrl) {
    const signSecret = env.PIPELINE_WORKER_SECRET?.trim();
    const signOpts = signSecret
      ? { secret: signSecret, serviceId: env.PIPELINE_WORKER_SERVICE_ID?.trim() || 'platform-api' }
      : undefined;
    const timeoutMs = Number(env.PIPELINE_WORKER_TIMEOUT_MS) || undefined;

    const bridge = new HTTPPipelineBridge({
      workerBaseUrl: workerUrl,
      timeoutMs,
      signOpts,
    });
    setPipelineBridge(bridge);
    deps.log.info(
      { workerUrl, signed: Boolean(signOpts), timeoutMs: timeoutMs ?? 5000 },
      'PipelineBridge wired to remote pipeline-worker (PIPELINE_WORKER_URL set)',
    );
    return {
      mode: 'remote',
      ready: Promise.resolve(),
      shutdown: async () => {
        // HTTPPipelineBridge holds no owned resources — fetch is stateless.
      },
    };
  }

  // In-process mode: bootstrap distributed-core PipelineModule locally.
  let resolvedShutdown: (() => Promise<void>) | null = null;
  const ready = bootstrapPipeline({
    pipelineRunRepository: deps.pipelineRunRepository,
    dlqRepository: deps.dlqRepository,
  })
    .then(({ module, nodeId, dlq, shutdown }) => {
      resolvedShutdown = shutdown;
      setPipelineBridge(createBridge(module, dlq, deps.pipelineRunRepository));
      deps.log.info(
        {
          nodeId,
          runPersistence: Boolean(deps.pipelineRunRepository),
          dlqPersistence: Boolean(deps.dlqRepository),
        },
        'PipelineModule bootstrapped (in-process)',
      );
    })
    .catch((err: unknown) => {
      const e = err as { message?: string; stack?: string; name?: string };
      deps.log.error(
        {
          errName: e?.name,
          errMessage: e?.message,
          errStack: e?.stack,
        },
        'PipelineModule bootstrap FAILED — pipeline bridge is NOT wired; ' +
          'all triggers will fall back to in-memory stub and runs will NOT persist',
      );
    });

  return {
    mode: 'in-process',
    ready,
    shutdown: async () => {
      if (resolvedShutdown) await resolvedShutdown();
    },
  };
}
