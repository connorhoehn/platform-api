// platform-api/src/pipeline/judge/registerJudgeHandlers.ts
//
// Boot-time wiring for the llm:judge custom step handler. Called from
// `bootstrap.ts` to produce the Map<string, StepHandler> that gets
// forwarded into `PipelineModule` via `buildPipelineModuleConfig`.
//
// Lazy + defensive: when `ANTHROPIC_API_KEY` is missing, the judge
// handler is still constructed (it'll surface a clear failure at
// dispatch time via JudgeClient's non-throwing semantics). When new
// custom step types are added later (image:vision-analyze, etc.) they
// register here too — keep the single Map small and explicit so the
// registry contents stay grep-friendly.

import type { Logger } from 'pino';
import type { StepHandler } from 'distributed-core/applications/pipeline';
import { createJudgeStepHandler } from './JudgeStepHandler';
import { JudgeClient } from './JudgeClient';

/** Mutable reference cell — bootstrap patches `current` after `new PipelineModule(...)`. */
export interface PipelineModuleRef {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  current: any | null;
}

export interface BuildJudgeStepHandlersOpts {
  /**
   * Mutable reference to the PipelineModule. Chicken-and-egg: handlers
   * are passed INTO the module config, so the module doesn't exist yet
   * when we build the handler map. Bootstrap patches `.current` after
   * the module is constructed. When `.current` is null at dispatch time
   * (cold-start / misuse), pipelineId resolves to `'unknown'` and the
   * step still completes — observability stays honest.
   */
  moduleRef: PipelineModuleRef;
  logger?: Logger;
}

export function buildJudgeStepHandlers(
  opts: BuildJudgeStepHandlersOpts,
): Map<string, StepHandler> {
  const handlers = new Map<string, StepHandler>();

  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) {
    if (opts.logger) {
      opts.logger.warn(
        'ANTHROPIC_API_KEY not set — llm:judge step will fail at dispatch ' +
          '(set ANTHROPIC_API_KEY to enable judge runs)',
      );
    }
    // Still register the handler — it'll surface a clean failure rather
    // than the executor throwing "unreachable pipeline node type" for the
    // llm:judge case. Diagnostics > silent omission.
  }

  const judgeClient = new JudgeClient({ apiKey });
  const judgeHandler: StepHandler = createJudgeStepHandler({
    judgeClient,
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

  handlers.set('llm:judge', judgeHandler);
  return handlers;
}
