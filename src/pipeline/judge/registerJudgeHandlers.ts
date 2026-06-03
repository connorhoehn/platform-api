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
import { JudgeBudgetGuard } from './JudgeBudgetGuard';
import { getJudgmentRepo } from './judgeServices';

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

  // Claude Code CLI authentication: OAuth tokens from the host's Keychain,
  // mounted at /root/.claude/.credentials.json in containerized deploys
  // (see ClaudeCodeLLMClient.ts and the Tilt local_resource that extracts
  // them). No ANTHROPIC_API_KEY required — same auth path the existing
  // `llm` NodeType uses.
  //
  // We still register the handler unconditionally so a misconfigured
  // claude binary surfaces as a clean dispatch-time failure rather than
  // "unreachable pipeline node type". Diagnostics > silent omission.
  const judgeClient = new JudgeClient();

  // Persistence: shared singleton (see judgeServices.ts). Soft-fail at the
  // handler — JudgmentResult is already on the EventBus so live observers
  // don't depend on the write. v2 rawText spillover is configured once at
  // the service layer via JUDGE_RAW_TEXT_ENABLED (DDB second table).
  const repository = getJudgmentRepo();

  // Daily budget: $10/day default, override via JUDGE_DAILY_BUDGET_USD_DEFAULT.
  // Soft-fails on Redis outage (returns allow) so a Redis blip doesn't
  // silently disable judge calls.
  const budgetGuard = new JudgeBudgetGuard();

  const judgeHandler: StepHandler = createJudgeStepHandler({
    judgeClient,
    repository,
    budgetGuard,
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
