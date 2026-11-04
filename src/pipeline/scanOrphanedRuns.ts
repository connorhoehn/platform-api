// platform-api/src/pipeline/scanOrphanedRuns.ts
//
// Hub task #1282 — single-node restart recovery. Per ADR-DURABLE-PIPELINE-EXECUTION D2.
//
// Process restart leaves any in-flight runs stuck at status='running' with no
// terminal event ever arriving. This scan runs once at bootstrap, finds those
// runs, and transitions them to status='interrupted' with `interruptedAt=now`
// so the operator UI can surface a "Resume" affordance (the existing retry
// route at POST /:runId/retry/:nodeId is the recovery path).
//
// Constraints (from ADR + constitution):
//   - Single-node only. We do NOT fence by ownerNodeId today because the
//     constitution's path-(a) means only one process ever owns a run.
//     Multi-node failover would steal/lease runs differently (out of scope).
//   - Bounded by `resumeWindowMs` (default 4h). Older `running` rows are
//     left alone — they're already-known-broken state from a previous era;
//     silently stomping them on every restart would be surprising.
//   - Side-effect-free re: pipeline events. We update DDB only; no
//     pipeline:run:cancelled is published. The interrupted state is purely a
//     marker so the UI can render appropriately.

import type { RunRecordRepository, PipelineRunItem } from '../repositories/PipelineRunRepository';
import { withContext } from '../lib/logger';
import type { PipelineEventMap } from 'distributed-core/applications/pipeline';
import { getRunRollupSubscriber, type RunRollupSubscriber } from './run-rollup-subscriber';
import { normalizeRunStatus } from './run-rollup';

import { finalizedRow, type TerminalEvidenceCheck } from './runTerminalEvidence';

const log = withContext({ service: 'scanOrphanedRuns' });

export interface ScanOrphanedRunsOptions {
  /**
   * NFR #103: proof the run already finished elsewhere (checkpoint frontier,
   * idempotent side effect, audit). A row with proof is written terminal
   * instead of `interrupted`, so the resume scan never re-runs it.
   */
  terminalEvidence?: TerminalEvidenceCheck;
  repository: RunRecordRepository;
  /** Only transition runs whose `startedAt` is within this window. Default: 4h. */
  resumeWindowMs?: number;
  /** Override `now` for tests. */
  now?: () => number;
  /**
   * NFR #26: is the process executing this run still alive? A run whose owner
   * replica holds its liveness lease is not orphaned — with two replicas the
   * other pod's run is `running` in DDB and must be left alone (interrupting
   * it here made the resume path execute it a second time). Omitted → the
   * original single-node meaning: every `running` row is an orphan.
   */
  isOwnerLive?: (item: PipelineRunItem) => Promise<boolean>;
  /**
   * NFR loop 18: this sweep runs on the LEADER, which never saw the run start
   * (the executor's bus is per process), and it used to write the run table
   * alone — so the pipeline's rollup row, the audit trail and the browser all
   * said `running` until the run was next touched. Each transition is now also
   * merged into the rollup row (`rollup`, default: the process-wide subscriber;
   * `null` = off) and put on the bus as `pipeline:run:orphaned` (`publish`),
   * the event the executor would have raised. Both are best-effort: a failure
   * is logged and the run-table write stands.
   */
  rollup?: Pick<RunRollupSubscriber, 'applyResolved'> | null;
  publish?: <K extends keyof PipelineEventMap>(type: K, payload: PipelineEventMap[K]) => Promise<unknown>;
}

export interface ScanOrphanedRunsResult {
  /** Rows that said `running` but whose run had already finished (NFR #103). */
  finalized?: number;
  scanned: number;
  transitioned: number;
  skippedOutsideWindow: number;
  /** Rows left alone because their owner replica is alive. */
  skippedLive?: number;
}

const DEFAULT_RESUME_WINDOW_MS = 4 * 60 * 60 * 1000; // 4 hours

/**
 * Find runs in `status='running'` and mark recent ones as `interrupted`.
 * Idempotent: re-running on a clean DDB does nothing. Errors per-row are
 * logged but do NOT abort the whole scan — bootstrap should not fail because
 * one row's update threw.
 */
export async function scanOrphanedRuns(
  options: ScanOrphanedRunsOptions,
): Promise<ScanOrphanedRunsResult> {
  const { repository } = options;
  const rollup = options.rollup === undefined ? getRunRollupSubscriber() : options.rollup;
  const resumeWindowMs = options.resumeWindowMs ?? DEFAULT_RESUME_WINDOW_MS;
  const now = options.now ?? (() => Date.now());

  const nowMs = now();
  const cutoffMs = nowMs - resumeWindowMs;

  let running: PipelineRunItem[];
  try {
    // 200 is the repository's max page size; orphan-scans should normally find
    // 0–single-digits of rows, so paging is unnecessary in practice.
    running = await repository.listByStatus('running', { limit: 200 });
  } catch (err) {
    log.error({ err: String(err) }, 'orphan-scan: listByStatus failed');
    return { scanned: 0, transitioned: 0, skippedOutsideWindow: 0 };
  }

  let transitioned = 0;
  let skippedOutsideWindow = 0;
  let skippedLive = 0;
  let finalized = 0;

  for (const item of running) {
    const startedAtMs = Date.parse(item.startedAt);
    if (!Number.isFinite(startedAtMs)) {
      log.warn({ runId: item.runId, startedAt: item.startedAt }, 'orphan-scan: invalid startedAt; skipping');
      continue;
    }
    if (startedAtMs < cutoffMs) {
      skippedOutsideWindow++;
      continue;
    }
    if (options.isOwnerLive && (await options.isOwnerLive(item))) {
      skippedLive++;
      continue;
    }

    const evidence = options.terminalEvidence ? await options.terminalEvidence(item) : null;
    if (evidence) {
      try {
        const row = finalizedRow(item, evidence, new Date(nowMs).toISOString());
        await repository.put(row);
        finalized++;
        log.info(
          { runId: item.runId, pipelineId: item.pipelineId, status: evidence.status, source: evidence.source, detail: evidence.detail },
          'orphan-scan: row said running but the run had finished — written terminal, not interrupted',
        );
        // The owner's terminal event was lost or never persisted; tell the
        // rollup what the row now says. No bus event: the run did finish
        // where it ran, and a synthesized `completed` has no duration to give.
        const at = row.completedAt ?? new Date(nowMs).toISOString();
        await mergeRollup(rollup, {
          pipelineId: item.pipelineId,
          runId: item.runId,
          status: normalizeRunStatus(row.status) ?? 'completed',
          at,
          completedAt: at,
          ...(item.startedAt ? { startedAt: item.startedAt } : {}),
        });
      } catch (err) {
        log.warn({ runId: item.runId, err: String(err) }, 'orphan-scan: failed to finalize run');
      }
      continue;
    }

    const interruptedAt = new Date(nowMs).toISOString();
    const updated: PipelineRunItem = {
      ...item,
      status: 'interrupted',
      interruptedAt,
    };
    try {
      await repository.put(updated);
      transitioned++;
      log.info(
        { runId: item.runId, pipelineId: item.pipelineId, ageMs: nowMs - startedAtMs },
        'orphan-scan: transitioned running → interrupted',
      );
      await mergeRollup(rollup, {
        pipelineId: item.pipelineId,
        runId: item.runId,
        status: 'interrupted',
        at: interruptedAt,
        ...(item.startedAt ? { startedAt: item.startedAt } : {}),
      });
      if (options.publish) {
        try {
          // `pipelineId` rides along for consumers that never indexed this run
          // (the audit subscriber on the leader); the declared shape is dc's.
          await options.publish('pipeline:run:orphaned', {
            runId: item.runId,
            previousOwner: item.ownerNodeId ?? 'unknown',
            at: interruptedAt,
            pipelineId: item.pipelineId,
          } as PipelineEventMap['pipeline:run:orphaned']);
        } catch (err) {
          log.warn({ runId: item.runId, err: String(err) }, 'orphan-scan: pipeline:run:orphaned not published');
        }
      }
    } catch (err) {
      log.warn(
        { runId: item.runId, err: String(err) },
        'orphan-scan: failed to update run',
      );
    }
  }

  // Periodic now (NFR #26 cluster sweeper): stay quiet when nothing moved.
  if (transitioned > 0 || skippedLive > 0 || finalized > 0) {
    log.info(
      { scanned: running.length, transitioned, finalized, skippedOutsideWindow, skippedLive, resumeWindowMs },
      'orphan-scan complete',
    );
  }

  return {
    scanned: running.length,
    transitioned,
    skippedOutsideWindow,
    ...(options.isOwnerLive ? { skippedLive } : {}),
    ...(options.terminalEvidence ? { finalized } : {}),
  };
}

/** Best-effort: the run-table write is the source of truth; the rollup heals on read if this fails. */
async function mergeRollup(
  rollup: Pick<RunRollupSubscriber, 'applyResolved'> | null,
  event: Parameters<RunRollupSubscriber['applyResolved']>[0],
): Promise<void> {
  if (!rollup) return;
  try {
    await rollup.applyResolved(event);
  } catch (err) {
    log.warn({ runId: event.runId, pipelineId: event.pipelineId, status: event.status, err: String(err) }, 'orphan-scan: rollup not updated');
  }
}
