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

import type { PipelineRunRepository, PipelineRunItem } from '../repositories/PipelineRunRepository';
import { withContext } from '../lib/logger';

const log = withContext({ service: 'scanOrphanedRuns' });

export interface ScanOrphanedRunsOptions {
  repository: PipelineRunRepository;
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
}

export interface ScanOrphanedRunsResult {
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
    } catch (err) {
      log.warn(
        { runId: item.runId, err: String(err) },
        'orphan-scan: failed to update run',
      );
    }
  }

  // Periodic now (NFR #26 cluster sweeper): stay quiet when nothing moved.
  if (transitioned > 0 || skippedLive > 0) {
    log.info(
      { scanned: running.length, transitioned, skippedOutsideWindow, skippedLive, resumeWindowMs },
      'orphan-scan complete',
    );
  }

  return {
    scanned: running.length,
    transitioned,
    skippedOutsideWindow,
    ...(options.isOwnerLive ? { skippedLive } : {}),
  };
}
