// platform-api/src/pipeline/resumeInterruptedRuns.ts
//
// DC-25 — boot-time durable resume. The second half of the recovery story:
// `scanOrphanedRuns` (hub#1282) marks dead-pod runs `running → interrupted`;
// this module CLAIMS those runs and continues them on the fresh process,
// under the SAME runId, from the checkpoint persisted on the run row.
//
// Recovery contract per run:
//   1. Eligibility — status `interrupted`, within the resume window, and the
//      row carries a `checkpoint`. A run with NO checkpoint has no resumable
//      frontier: it is LEFT interrupted (the UI's "Resume"/retry affordance
//      remains the operator path) — a clean skip, never a guessed restart.
//   2. Definition — `definitionSnapshot` on the row (captured from the first
//      checkpoint; the exact graph the run started with) is authoritative.
//      Fallback: the definitions repo keyed by (triggeredBy, pipelineId) —
//      may be newer than the graph that produced the run, which is why it is
//      the fallback and not the source.
//   3. Claim — a conditional DDB write (`status = interrupted` → `running`,
//      resumeClaimedBy/resumedAt/resumeCount stamped). Two pods racing to
//      resume: exactly one wins; the loser skips. Single-node today, but the
//      guard costs one conditional write.
//   4. Resume — RunPersistenceService is re-seeded from the row (so the
//      resumed executor's events merge onto the EXISTING step history —
//      pre-crash steps keep their original timestamps), then
//      PipelineModule.resumeRun() continues the run. Completion flows
//      through the normal event → persistence path.

import type { PipelineModule, PipelineResumeState } from 'distributed-core/applications/pipeline';
import type { PipelineDefinition } from 'distributed-core/applications/pipeline';
import type {
  PipelineRunRepository,
  PipelineRunItem,
} from '../repositories/PipelineRunRepository';
import type { RunPersistenceService } from './runPersistence';
import { withContext } from '../lib/logger';

const log = withContext({ service: 'resumeInterruptedRuns' });

export interface ResumeInterruptedRunsOptions {
  repository: PipelineRunRepository;
  module: PipelineModule;
  runPersistence: RunPersistenceService | null;
  /** Fallback definition source when the row has no definitionSnapshot. */
  definitionsRepo: {
    get(userId: string, pipelineId: string): Promise<unknown | null>;
  };
  /** This process's cluster node id — stamped as resumeClaimedBy. */
  nodeId: string;
  /** Only resume runs interrupted within this window. Default: 4h. */
  resumeWindowMs?: number;
  /** Override `now` for tests. */
  now?: () => number;
}

export interface ResumeInterruptedRunsResult {
  scanned: number;
  resumed: number;
  skippedNoCheckpoint: number;
  skippedNoDefinition: number;
  skippedOutsideWindow: number;
  claimLost: number;
  failed: number;
}

const DEFAULT_RESUME_WINDOW_MS = 4 * 60 * 60 * 1000; // 4h — matches scanOrphanedRuns

/**
 * Find `interrupted` runs and resume the recoverable ones. Per-row errors
 * are logged and counted, never thrown — bootstrap must not fail because
 * one row is broken.
 */
export async function resumeInterruptedRuns(
  options: ResumeInterruptedRunsOptions,
): Promise<ResumeInterruptedRunsResult> {
  const { repository, module, runPersistence, definitionsRepo, nodeId } = options;
  const resumeWindowMs = options.resumeWindowMs ?? DEFAULT_RESUME_WINDOW_MS;
  const now = options.now ?? (() => Date.now());

  const result: ResumeInterruptedRunsResult = {
    scanned: 0,
    resumed: 0,
    skippedNoCheckpoint: 0,
    skippedNoDefinition: 0,
    skippedOutsideWindow: 0,
    claimLost: 0,
    failed: 0,
  };

  let interrupted: PipelineRunItem[];
  try {
    interrupted = await repository.listByStatus('interrupted', { limit: 200 });
  } catch (err) {
    log.error({ err: String(err) }, 'resume-scan: listByStatus failed');
    return result;
  }
  result.scanned = interrupted.length;

  const nowMs = now();
  const cutoffMs = nowMs - resumeWindowMs;

  for (const item of interrupted) {
    const markedAtMs = Date.parse(item.interruptedAt ?? item.startedAt);
    if (!Number.isFinite(markedAtMs) || markedAtMs < cutoffMs) {
      result.skippedOutsideWindow++;
      continue;
    }

    if (!item.checkpoint) {
      // Clean skip — never guess a frontier for a run that has no durable one.
      result.skippedNoCheckpoint++;
      log.info(
        { runId: item.runId, pipelineId: item.pipelineId },
        'resume-scan: interrupted run has no checkpoint — leaving interrupted',
      );
      continue;
    }

    const definition = (item.definitionSnapshot
      ?? (await definitionsRepo
        .get(item.triggeredBy, item.pipelineId)
        .catch(() => null))) as PipelineDefinition | null | undefined;
    if (!definition) {
      result.skippedNoDefinition++;
      log.warn(
        { runId: item.runId, pipelineId: item.pipelineId, triggeredBy: item.triggeredBy },
        'resume-scan: no definitionSnapshot and definitions repo miss — leaving interrupted',
      );
      continue;
    }

    try {
      // Double-resume guard: conditional claim. Loser skips.
      const claimed = await repository.claimForResume(
        item.pipelineId,
        item.runId,
        nodeId,
        new Date(nowMs).toISOString(),
      );
      if (!claimed) {
        result.claimLost++;
        log.info({ runId: item.runId }, 'resume-scan: claim lost — another process owns this resume');
        continue;
      }

      // Seed the persistence cache from the post-claim row so the resumed
      // executor's events merge onto the existing history (original
      // pre-crash step timestamps preserved).
      runPersistence?.seedResumedRun(claimed);

      const cp = item.checkpoint;

      // Re-attach display outputs onto the slimmed step records where the
      // stepsSummary still has them (checkpoint steps are persisted without
      // outputs; execution truth travels in cp.context.steps).
      const steps: Record<string, unknown> = {};
      for (const [stepId, raw] of Object.entries(cp.steps ?? {})) {
        const summary = item.stepsSummary?.[stepId];
        const rec = { ...(raw as Record<string, unknown>) };
        if (rec.output === undefined && summary?.output !== undefined) {
          rec.output = summary.output;
        }
        steps[stepId] = rec;
      }

      const resumeState = {
        runId: item.runId,
        context: cp.context,
        currentStepIds: cp.currentStepIds,
        steps,
        joinStates: cp.joinStates,
        firedJoins: cp.firedJoins,
        completedStepCount: cp.completedStepCount,
        sequenceNumber: cp.sequenceNumber,
        startedAt: item.startedAt,
        triggeredBy: {
          ...(item.triggeredBy && item.triggeredBy !== 'system' ? { userId: item.triggeredBy } : {}),
          triggerType: item.triggerType ?? 'manual',
          payload: {},
        },
        replayAttempts: claimed.resumeCount,
      } as unknown as PipelineResumeState;

      await module.resumeRun({ definition, resumeState });
      result.resumed++;
      log.info(
        {
          runId: item.runId,
          pipelineId: item.pipelineId,
          fromSequence: cp.sequenceNumber,
          frontier: cp.currentStepIds,
          resumeCount: claimed.resumeCount,
        },
        'resume-scan: run RESUMED from checkpoint',
      );
    } catch (err) {
      result.failed++;
      log.error({ runId: item.runId, err: String(err) }, 'resume-scan: resume attempt failed');
    }
  }

  if (result.scanned > 0) {
    log.info({ ...result, resumeWindowMs }, 'resume-scan complete');
  }
  return result;
}
