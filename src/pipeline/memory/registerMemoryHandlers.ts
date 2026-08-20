// platform-api/src/pipeline/memory/registerMemoryHandlers.ts
//
// Builds the custom step handlers for the `memory` node, mirroring
// registerVisionHandlers / registerJudgeHandlers so bootstrap can merge them
// into the one customStepHandlers map.

import type { StepHandler } from 'distributed-core/applications/pipeline';
import { createMemoryStepHandler } from './MemoryStepHandler';
import { getMemoryStore } from './memoryServices';
import type { MemoryStore } from './MemoryStore';

export interface BuildMemoryStepHandlersOpts {
  /**
   * Backing store. Injected so the AWS-backed implementation is a HOST choice
   * rather than something the pipeline layer hardwires - the same reason the
   * editor keeps vendor names out of its node panels. Defaults to the
   * env-selected store from memoryServices (in-process unless
   * PIPELINE_MEMORY_MODE=ddb).
   */
  store?: MemoryStore;

  /**
   * Mutable ref to the PipelineModule, patched after construction. Needed to
   * resolve a runId to its pipelineId for 'long-term' scope - the same
   * arrangement the judge handler uses.
   */
  moduleRef: { current: { getRun?: (runId: string) => { pipelineId?: string } | undefined } | null };
}

export function buildMemoryStepHandlers(
  opts: BuildMemoryStepHandlersOpts,
): Map<string, StepHandler> {
  const handlers = new Map<string, StepHandler>();

  const handler = createMemoryStepHandler({
    store: opts.store ?? getMemoryStore(),
    pipelineIdLookup: (runId: string) => {
      const mod = opts.moduleRef.current;
      if (!mod || typeof mod.getRun !== 'function') return undefined;
      try {
        return mod.getRun(runId)?.pipelineId;
      } catch {
        return undefined;
      }
    },
  });

  handlers.set('memory', handler);
  return handlers;
}
