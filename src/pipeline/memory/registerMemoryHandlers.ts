// platform-api/src/pipeline/memory/registerMemoryHandlers.ts
//
// Builds the custom step handlers for the `memory` node, mirroring
// registerVisionHandlers / registerJudgeHandlers so bootstrap can merge them
// into the one customStepHandlers map.

import type { StepHandler } from 'distributed-core/applications/pipeline';
import { createMemoryStepHandler } from './MemoryStepHandler';
import { getMemoryStore } from './memoryServices';

export interface BuildMemoryStepHandlersOpts {
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
    store: getMemoryStore(),
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
