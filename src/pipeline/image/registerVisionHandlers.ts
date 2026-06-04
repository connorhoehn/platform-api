// platform-api/src/pipeline/image/registerVisionHandlers.ts
//
// Boot-time wiring for the image:vision-analyze custom step handler.
// Called from `bootstrap.ts` to produce the Map<string, StepHandler>
// that gets forwarded into `PipelineModule` via
// `buildPipelineModuleConfig`. Returned map merges with the judge
// handlers map at the boot site.
//
// Lazy + defensive: when ANTHROPIC_API_KEY is missing, the vision
// handler is still constructed (it surfaces a clean dispatch-time
// failure via VisionClient's non-throwing semantics —
// failureReason='http-error'). Diagnostics > silent omission.
//
// Code parity with registerJudgeHandlers.ts.

import type { Logger } from 'pino';
import type { StepHandler } from 'distributed-core/applications/pipeline';
import { createVisionStepHandler } from './VisionStepHandler';
import { getImageBlobStore, getVisionClient, getVisionResultRepo } from './visionServices';

/** Mutable reference cell — bootstrap patches `current` after `new PipelineModule(...)`. */
export interface PipelineModuleRef {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  current: any | null;
}

export interface BuildVisionStepHandlersOpts {
  /**
   * Mutable reference to the PipelineModule. Same chicken-and-egg
   * pattern as judge handlers: handlers are passed INTO the module
   * config, so the module doesn't exist yet when we build the map.
   * Bootstrap patches `.current` after the module is constructed.
   */
  moduleRef: PipelineModuleRef;
  logger?: Logger;
}

export function buildVisionStepHandlers(
  opts: BuildVisionStepHandlersOpts,
): Map<string, StepHandler> {
  const handlers = new Map<string, StepHandler>();

  // Anthropic Messages API auth: ANTHROPIC_API_KEY env var (set by
  // operator — see docs/vision-PROVISIONING.md). Unlike judge, vision
  // can't use Claude Code OAuth because the CLI strips image content
  // blocks (see VisionClient.ts header).
  //
  // We still register the handler unconditionally so missing-key
  // surfaces as a clean dispatch-time failure rather than "unreachable
  // pipeline node type". Diagnostics > silent omission.
  const visionClient = getVisionClient();
  const imageBlobStore = getImageBlobStore();
  const repository = getVisionResultRepo();

  const visionHandler: StepHandler = createVisionStepHandler({
    visionClient,
    imageBlobStore,
    repository,
    pipelineIdLookup: (runId: string) => {
      const mod = opts.moduleRef.current;
      if (!mod || typeof mod.getRun !== 'function') return undefined;
      try {
        const run = mod.getRun(runId);
        return run?.pipelineId;
      } catch {
        return undefined;
      }
    },
  });

  handlers.set('image:vision-analyze', visionHandler);
  return handlers;
}
