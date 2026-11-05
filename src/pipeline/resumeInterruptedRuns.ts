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
//
// Steps 3–4 run on pipeline-runtime's `recoverInterruptedRuns` (`/durable`):
// the vetted rows go in through `RunRecordRecoveryStore` (runRecordStore.ts),
// whose `claim` is the conditional write above, and `launch` seeds and
// resumes. The resume state is `resumeStateFromRecord` over the mapped row.
// Steps 1–2 stay here because they are async (terminal evidence, the
// definitions repo) and leave rows untouched rather than interrupting them.
// A run whose launch throws keeps its claimed row (`onUnrecoverable: 'skip'`
// and a no-op `interrupt`): the cluster sweep re-interrupts it, as before.

import type { PipelineModule, PipelineDefinition } from 'distributed-core/applications/pipeline';
import {
  recoverInterruptedRuns,
  resumeStateFromRecord,
  type DurableRunRecord,
} from '@connorhoehn/pipeline-runtime/durable';
import type {
  RunRecordRepository,
  PipelineRunItem,
} from '../repositories/PipelineRunRepository';
import type { RunPersistenceService } from './runPersistence';
import { withContext } from '../lib/logger';

import { finalizedRow, type TerminalEvidenceCheck } from './runTerminalEvidence';
import {
  RunRecordRecoveryStore,
  claimedRowOf,
  rowOf,
  toDurableRunRecord,
} from './runRecordStore';

const log = withContext({ service: 'resumeInterruptedRuns' });

export interface ResumeInterruptedRunsOptions {
  repository: RunRecordRepository;
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
  /**
   * NFR #103: proof the run already finished (checkpoint frontier, idempotent
   * side effect, audit). Such a run is written terminal, never resumed.
   */
  terminalEvidence?: TerminalEvidenceCheck;
}

export interface ResumeInterruptedRunsResult {
  /** Interrupted rows whose run had already finished — finalized, not resumed. */
  skippedFinished: number;
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
    skippedFinished: 0,
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

  // Vet each row: in the window, not already finished, a checkpoint, a definition.
  const eligible: DurableRunRecord[] = [];
  for (const item of interrupted) {
    const markedAtMs = Date.parse(item.interruptedAt ?? item.startedAt);
    if (!Number.isFinite(markedAtMs) || markedAtMs < cutoffMs) {
      result.skippedOutsideWindow++;
      continue;
    }

    const evidence = options.terminalEvidence ? await options.terminalEvidence(item) : null;
    if (evidence) {
      try {
        await repository.put(finalizedRow(item, evidence, new Date(nowMs).toISOString()));
        result.skippedFinished++;
        log.info(
          { runId: item.runId, pipelineId: item.pipelineId, status: evidence.status, source: evidence.source, detail: evidence.detail },
          'resume-scan: run already finished — finalized, not resumed',
        );
      } catch (err) {
        result.failed++;
        log.warn({ runId: item.runId, err: String(err) }, 'resume-scan: failed to finalize finished run');
      }
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

    eligible.push(toDurableRunRecord(item, definition));
  }
  if (eligible.length === 0) {
    if (result.scanned > 0) log.info({ ...result, resumeWindowMs }, 'resume-scan complete');
    return result;
  }

  // Claim (double-resume guard: the loser skips) and resume each vetted run.
  const store = new RunRecordRecoveryStore(repository, eligible, () => nowMs);
  const recovered = await recoverInterruptedRuns({
    store,
    owner: nodeId,
    now: () => nowMs,
    statuses: ['interrupted'],
    onUnrecoverable: 'skip',
    launch: async (record) => {
      const item = rowOf(record);
      const claimed = claimedRowOf(record);
      // Seed the persistence cache from the post-claim row so the resumed
      // executor's events merge onto the existing history (original
      // pre-crash step timestamps preserved).
      runPersistence?.seedResumedRun(claimed);
      await module.resumeRun({ definition: record.definition, resumeState: resumeStateFromRecord(record) });
      log.info(
        {
          runId: item.runId,
          pipelineId: item.pipelineId,
          fromSequence: item.checkpoint?.sequenceNumber,
          frontier: item.checkpoint?.currentStepIds,
          resumeCount: claimed.resumeCount,
        },
        'resume-scan: run RESUMED from checkpoint',
      );
    },
    // The claimed row stays as it is (the cluster sweep re-interrupts it); count and log.
    interrupt: async (record, reason) => {
      result.failed++;
      log.error({ runId: record.id, err: reason }, 'resume-scan: resume attempt failed');
    },
  });

  result.resumed = recovered.resumed.length;
  for (const runId of recovered.claimLost) {
    const claimError = store.claimErrors.get(runId);
    if (claimError !== undefined) {
      result.failed++;
      log.error({ runId, err: claimError }, 'resume-scan: resume attempt failed');
    } else {
      result.claimLost++;
      log.info({ runId }, 'resume-scan: claim lost — another process owns this resume');
    }
  }

  if (result.scanned > 0) {
    log.info({ ...result, resumeWindowMs }, 'resume-scan complete');
  }
  return result;
}
