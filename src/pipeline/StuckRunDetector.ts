// platform-api/src/pipeline/StuckRunDetector.ts
//
// Hub task #1285 — detect pipeline runs whose steps have gone silent past a
// configurable threshold. Strategy: RunPersistenceService tracks `lastEventAt`
// in-memory for each active run; this detector queries that in-process state
// every `checkIntervalMs` and sets `stuckAt` on runs that haven't emitted any
// event in `stuckThresholdMs`.
//
// "Stuck" means status stays `running` — the run is not cancelled. The
// `stuckAt` flag surfaces a badge in the operator UI and enables the
// force-fail endpoint. On process restart, `scanOrphanedRuns` handles the
// cleanup path for runs left in `running` status.
//
// Tunables (env):
//   PIPELINE_STUCK_CHECK_INTERVAL_MS  — how often the scan runs (default 60s)
//   PIPELINE_STUCK_THRESHOLD_MS       — silence window before a run is stuck (default 5 min)

import type { PipelineRunRepository } from '../repositories/PipelineRunRepository';
import type { RunPersistenceService } from './runPersistence';
import { withContext } from '../lib/logger';
import type { PipelineEventMap } from 'distributed-core/applications/pipeline';
import { getRunRollupSubscriber, type RunRollupSubscriber } from './run-rollup-subscriber';

const log = withContext({ service: 'StuckRunDetector' });

export interface StuckRunDetectorOptions {
  runPersistence: RunPersistenceService;
  repository: PipelineRunRepository;
  checkIntervalMs?: number;
  stuckThresholdMs?: number;
  /** Override `Date.now()` for tests. */
  now?: () => number;
  /**
   * NFR loop 18: `stuckAt` used to reach the run table only. The transition is
   * now also merged into the pipeline's rollup row (`rollup`, default: the
   * process-wide subscriber; `null` = off) and raised on the bus as
   * `pipeline:run:stuck` (`publish`) for the audit trail and the browser.
   * Best-effort; the run-table write stands if either fails.
   */
  rollup?: Pick<RunRollupSubscriber, 'applyResolved'> | null;
  publish?: <K extends keyof PipelineEventMap>(type: K, payload: PipelineEventMap[K]) => Promise<unknown>;
}

export class StuckRunDetector {
  private readonly opts: StuckRunDetectorOptions;
  private timer: ReturnType<typeof setInterval> | null = null;

  constructor(opts: StuckRunDetectorOptions) {
    this.opts = opts;
  }

  static resolveIntervalMs(): number {
    const raw = process.env.PIPELINE_STUCK_CHECK_INTERVAL_MS;
    if (raw) {
      const n = Number(raw);
      if (Number.isFinite(n) && n > 0) return n;
    }
    return 60_000;
  }

  static resolveThresholdMs(): number {
    const raw = process.env.PIPELINE_STUCK_THRESHOLD_MS;
    if (raw) {
      const n = Number(raw);
      if (Number.isFinite(n) && n > 0) return n;
    }
    return 5 * 60_000; // 5 minutes
  }

  start(): void {
    if (this.timer !== null) return;
    const interval = this.opts.checkIntervalMs ?? StuckRunDetector.resolveIntervalMs();
    this.timer = setInterval(() => {
      void this.check().catch((err) => {
        log.error({ err: String(err) }, 'stuck-run check failed');
      });
    }, interval);
    // Don't hold Node.js open if everything else has shut down.
    this.timer.unref();
    log.info({ intervalMs: interval }, 'StuckRunDetector started');
  }

  stop(): void {
    if (this.timer !== null) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  async check(): Promise<void> {
    const threshold = this.opts.stuckThresholdMs ?? StuckRunDetector.resolveThresholdMs();
    const now = this.opts.now?.() ?? Date.now();
    const cutoff = now - threshold;

    const stale = this.opts.runPersistence.getStaleRunIds(cutoff);
    const rollup = this.opts.rollup === undefined ? getRunRollupSubscriber() : this.opts.rollup;
    for (const { runId, pipelineId } of stale) {
      try {
        const existing = await this.opts.repository.get(pipelineId, runId);
        if (!existing || existing.stuckAt) continue; // already marked or gone
        const stuckAt = new Date(now).toISOString();
        await this.opts.repository.put({ ...existing, stuckAt });
        log.warn({ runId, pipelineId, stuckAt, thresholdMs: threshold }, 'run marked stuck');
        if (rollup) {
          try {
            await rollup.applyResolved({ pipelineId, runId, status: 'stuck', at: stuckAt, ...(existing.startedAt ? { startedAt: existing.startedAt } : {}) });
          } catch (err) {
            log.warn({ runId, pipelineId, err: String(err) }, 'stuck-run: rollup not updated');
          }
        }
        if (this.opts.publish) {
          try {
            // `pipelineId` rides along for consumers that never indexed this run.
            await this.opts.publish('pipeline:run:stuck', { runId, lastHeartbeatAt: existing.lastEventAt ?? null, at: stuckAt, pipelineId } as PipelineEventMap['pipeline:run:stuck']);
          } catch (err) {
            log.warn({ runId, pipelineId, err: String(err) }, 'stuck-run: pipeline:run:stuck not published');
          }
        }
      } catch (err) {
        log.warn({ runId, pipelineId, err: String(err) }, 'failed to mark run stuck');
      }
    }
  }
}
