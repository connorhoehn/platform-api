// platform-api/src/pipeline/judge/JudgeBudgetGuard.ts
//
// Daily spend cap for `llm:judge` LLM calls, backed by Redis-INCR.
//
// Per audit rule #3: budget gating happens BEFORE the LLM call, not
// after. JudgeStepHandler calls `isExhausted(pipelineId, estCostUsd)`
// with a conservative pre-call cost estimate; the handler skips the
// call (and emits `pipeline:judge:budget-exceeded`) when this returns
// true. After a successful call it calls `record(pipelineId, actualUsd)`
// to debit the running counter.
//
// Storage shape:
//   key: judge:budget:{pipelineId}:{yyyy-mm-dd}
//   value: float (USD)
//   ttl:   48h (set on first INCR via PEXPIRE; double-day for clock skew
//          tolerance across regions / midnight crossings)
//
// Cap is per-pipeline-per-UTC-day, with an env-configurable default and
// optional per-pipeline overrides. The class is intentionally simple: a
// single counter, no warning thresholds, no notification side effects.
// (UI can derive utilization% from the underlying counter via the
// admin route in a future wave.)
//
// Soft-fail posture: if Redis is unavailable, `isExhausted` returns
// `false` — judge calls go through. A reliability outage in the budget
// substrate must not silently disable the LLM observability feature. We
// log a warning so the soft-fail is observable.

import { getRedisClient } from '../../lib/redis-client';
import log from '../../lib/logger';

/**
 * Default per-pipeline daily budget cap in USD. Override via env
 * `JUDGE_DAILY_BUDGET_USD_DEFAULT` (read once at construction). Pipelines
 * may further override via the per-pipeline overrides map.
 */
const DEFAULT_DAILY_BUDGET_USD = 10;

/** Two-day TTL on the Redis key — survives midnight and clock skew. */
const KEY_TTL_SECONDS = 48 * 60 * 60;

export interface JudgeBudgetGuardOptions {
  /** Fallback when env var is unset or invalid. */
  defaultDailyBudgetUsd?: number;
  /** Per-pipeline budget overrides (e.g. high-volume eval pipelines). */
  perPipelineBudgetUsd?: Record<string, number>;
  /**
   * Wall-clock override for tests. Returns ms-since-epoch. Used to derive
   * the yyyy-mm-dd day bucket in UTC.
   */
  now?: () => number;
}

export class JudgeBudgetGuard {
  private readonly defaultDailyBudgetUsd: number;
  private readonly perPipelineBudgetUsd: Record<string, number>;
  private readonly now: () => number;

  constructor(options: JudgeBudgetGuardOptions = {}) {
    const envDefault = Number.parseFloat(process.env.JUDGE_DAILY_BUDGET_USD_DEFAULT ?? '');
    this.defaultDailyBudgetUsd = Number.isFinite(envDefault) && envDefault > 0
      ? envDefault
      : options.defaultDailyBudgetUsd ?? DEFAULT_DAILY_BUDGET_USD;
    this.perPipelineBudgetUsd = options.perPipelineBudgetUsd ?? {};
    this.now = options.now ?? (() => Date.now());
  }

  /**
   * True when `currentSpend + estCost > cap`. Returns false (allow) on
   * Redis errors / unavailability — soft-fail.
   */
  async isExhausted(pipelineId: string, estCostUsd: number): Promise<boolean> {
    const cap = this.capFor(pipelineId);
    const currentSpend = await this.readCurrentSpend(pipelineId);
    if (currentSpend === null) return false; // Redis down → allow
    return currentSpend + estCostUsd > cap;
  }

  /**
   * Debits the running daily total for the pipeline. Sets the 48h TTL on
   * the key if not already set. Best-effort: persistence errors are
   * logged but not surfaced to the caller (the judge call already
   * completed successfully).
   */
  async record(pipelineId: string, actualCostUsd: number): Promise<void> {
    if (actualCostUsd <= 0) return;
    const redis = await getRedisClient();
    if (!redis) return; // Soft-fail — already logged by getRedisClient
    const key = this.dailyKey(pipelineId);
    try {
      await redis.incrByFloat(key, actualCostUsd);
      await redis.expire(key, KEY_TTL_SECONDS);
    } catch (err) {
      log.warn({ err, key }, '[judge-budget-guard] record failed');
    }
  }

  /**
   * Current debited USD for the pipeline's day. Returns null when Redis
   * is unavailable (so callers know to soft-fail) and 0 when the key
   * doesn't exist yet.
   */
  private async readCurrentSpend(pipelineId: string): Promise<number | null> {
    const redis = await getRedisClient();
    if (!redis) return null;
    const key = this.dailyKey(pipelineId);
    try {
      const raw = await redis.get(key);
      if (raw === null) return 0;
      const parsed = Number.parseFloat(raw);
      return Number.isFinite(parsed) ? parsed : 0;
    } catch (err) {
      log.warn({ err, key }, '[judge-budget-guard] read failed');
      return null;
    }
  }

  private capFor(pipelineId: string): number {
    return this.perPipelineBudgetUsd[pipelineId] ?? this.defaultDailyBudgetUsd;
  }

  private dailyKey(pipelineId: string): string {
    const day = this.utcDayBucket(this.now());
    return `judge:budget:${pipelineId}:${day}`;
  }

  /** yyyy-mm-dd in UTC. */
  private utcDayBucket(epochMs: number): string {
    const d = new Date(epochMs);
    const y = d.getUTCFullYear();
    const m = String(d.getUTCMonth() + 1).padStart(2, '0');
    const day = String(d.getUTCDate()).padStart(2, '0');
    return `${y}-${m}-${day}`;
  }
}
