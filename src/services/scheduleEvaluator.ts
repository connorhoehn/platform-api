// platform-api/src/services/scheduleEvaluator.ts
//
// Polls a server-side store of pipeline definitions (Phase 1: same in-memory
// stub as the definitions endpoint) once per minute and fires any pipelines
// whose cron schedule matches the current minute.
//
// Cron grammar (intentionally minimal — covers the 90% case):
//   5-field expression: `minute hour day-of-month month day-of-week`
//   Per field: `*` | `N` | `N,M,...` | `* /N`            (no spaces inside `* /N`)
//             | `A-B` (single dash range; e.g. `1-5` for weekdays)
//   Anything more exotic (`L`, `W`, `?`, multi-step like `1-30/2`) is rejected.
//
// Phase 4 hookup: `trigger()` wires into pipeline ResourceRouter; for now it's
// a caller-supplied side-effect (logging in production startup, capturing in
// tests).

export interface PipelineForSchedule {
  id: string;
  status: string;
  triggerBinding?: { event: string; schedule?: string };
}

export interface ScheduleEvaluatorOptions {
  /** Default 60_000ms. */
  pollIntervalMs?: number;
  /** Side-effect to invoke when a pipeline's schedule matches. */
  trigger: (
    pipelineId: string,
    payload: Record<string, unknown>,
  ) => Promise<void>;
  /** Snapshot of current pipelines on each tick. */
  listPipelines: () => Iterable<PipelineForSchedule>;
  /** For deterministic tests — defaults to `() => new Date()`. */
  now?: () => Date;
  /**
   * Phase 54.6.1 — leader-lock for multi-replica safety. When set, the
   * timer-driven tick acquires the lock first; if another replica holds
   * it, this replica skips the tick. When unset, every replica's timer
   * fires every tick (legacy single-node behavior).
   */
  leaderLock?: SchedulerLeaderLock;
}

import type { SchedulerLeaderLock } from './scheduler-leader-lock';

interface ParsedField {
  // Either '*' or an explicit Set<number> of valid values.
  match: '*' | Set<number>;
  // Step value for `* /N`; only meaningful when match === '*'.
  step?: number;
}

interface ParsedCron {
  minute: ParsedField;
  hour: ParsedField;
  dayOfMonth: ParsedField;
  month: ParsedField;
  dayOfWeek: ParsedField;
}

const STEP_RE = /^\*\/(\d+)$/;
const RANGE_RE = /^(\d+)-(\d+)$/;
const WHITESPACE_RE = /\s+/;

const FIELD_RANGES: Array<{ name: keyof ParsedCron; min: number; max: number }> = [
  { name: 'minute',     min: 0, max: 59 },
  { name: 'hour',       min: 0, max: 23 },
  { name: 'dayOfMonth', min: 1, max: 31 },
  { name: 'month',      min: 1, max: 12 },
  { name: 'dayOfWeek',  min: 0, max: 6  }, // 0 = Sunday
];

function parseField(
  raw: string,
  min: number,
  max: number,
): ParsedField | null {
  const trimmed = raw.trim();
  if (trimmed === '') return null;

  // Step on wildcard: `*/N`
  const stepMatch = STEP_RE.exec(trimmed);
  if (stepMatch) {
    const step = Number(stepMatch[1]);
    if (!Number.isFinite(step) || step <= 0) return null;
    return { match: '*', step };
  }

  // Plain wildcard
  if (trimmed === '*') return { match: '*' };

  // Range: `A-B`
  const rangeMatch = RANGE_RE.exec(trimmed);
  if (rangeMatch) {
    const a = Number(rangeMatch[1]);
    const b = Number(rangeMatch[2]);
    if (!Number.isFinite(a) || !Number.isFinite(b)) return null;
    if (a < min || b > max || a > b) return null;
    const set = new Set<number>();
    for (let i = a; i <= b; i++) set.add(i);
    return { match: set };
  }

  // Comma list: `N,M,...`
  if (trimmed.includes(',')) {
    const parts = trimmed.split(',');
    const set = new Set<number>();
    for (const p of parts) {
      const n = Number(p.trim());
      if (!Number.isFinite(n) || !Number.isInteger(n)) return null;
      if (n < min || n > max) return null;
      set.add(n);
    }
    if (set.size === 0) return null;
    return { match: set };
  }

  // Single integer
  const n = Number(trimmed);
  if (!Number.isFinite(n) || !Number.isInteger(n)) return null;
  if (n < min || n > max) return null;
  return { match: new Set([n]) };
}

export function parseCronExpression(expr: string): ParsedCron | null {
  if (typeof expr !== 'string') return null;
  const fields = expr.trim().split(WHITESPACE_RE);
  if (fields.length !== 5) return null;
  const out: Partial<ParsedCron> = {};
  for (let i = 0; i < 5; i++) {
    const spec = FIELD_RANGES[i];
    const parsed = parseField(fields[i], spec.min, spec.max);
    if (!parsed) return null;
    out[spec.name] = parsed;
  }
  return out as ParsedCron;
}

function fieldMatches(field: ParsedField, value: number, fieldMin: number): boolean {
  if (field.match === '*') {
    if (!field.step || field.step === 1) return true;
    return ((value - fieldMin) % field.step) === 0;
  }
  return field.match.has(value);
}

export function cronMatches(cron: ParsedCron, date: Date): boolean {
  const minute     = date.getMinutes();
  const hour       = date.getHours();
  const dayOfMonth = date.getDate();
  const month      = date.getMonth() + 1; // JS months are 0-indexed
  const dayOfWeek  = date.getDay();        // 0 = Sunday

  return (
    fieldMatches(cron.minute,     minute,     0) &&
    fieldMatches(cron.hour,       hour,       0) &&
    fieldMatches(cron.dayOfMonth, dayOfMonth, 1) &&
    fieldMatches(cron.month,      month,      1) &&
    fieldMatches(cron.dayOfWeek,  dayOfWeek,  0)
  );
}

// ---------------------------------------------------------------------------
// Evaluator
// ---------------------------------------------------------------------------

export class ScheduleEvaluator {
  private timer: ReturnType<typeof setInterval> | null = null;
  private readonly pollIntervalMs: number;
  private readonly opts: ScheduleEvaluatorOptions;
  /** Tracks `${pipelineId}:${minuteEpoch}` to avoid double-firing in same minute. */
  private readonly fired = new Set<string>();
  private readonly nowFn: () => Date;

  constructor(opts: ScheduleEvaluatorOptions) {
    this.opts = opts;
    this.pollIntervalMs = opts.pollIntervalMs ?? 60_000;
    this.nowFn = opts.now ?? (() => new Date());
  }

  start(): void {
    if (this.timer) return;
    this.timer = setInterval(() => {
      this.runProtectedTick().catch(err => {
        console.error('[scheduleEvaluator] tick error', err);
      });
    }, this.pollIntervalMs);
    // Don't keep the event loop alive solely for this timer (test ergonomics +
    // graceful shutdown — match the convention used by gateway timers).
    if (typeof this.timer === 'object' && this.timer && 'unref' in this.timer) {
      (this.timer as unknown as { unref: () => void }).unref();
    }
  }

  /**
   * Phase 54.6.1 — leader-gated tick. The timer calls this on every
   * interval; it acquires the leader lock (if configured) and runs
   * `tick()` only if leadership is granted. Returns true if the tick
   * actually executed (acquired leader) or false if skipped (another
   * replica holds the lock).
   *
   * When no `leaderLock` is configured, this degenerates to a direct
   * tick() call — preserving the legacy single-node behavior.
   */
  async runProtectedTick(now: Date = this.nowFn()): Promise<boolean> {
    if (!this.opts.leaderLock) {
      this.tick(now);
      return true;
    }
    // TTL: 2x the poll interval. Gives the tick body time to complete
    // (typically << 1s) plus a buffer; auto-expires before the next tick
    // so failover is bounded to a single skipped interval.
    const ttlMs = this.pollIntervalMs * 2;
    const acquired = await this.opts.leaderLock.acquire(ttlMs);
    if (!acquired) {
      return false;
    }
    try {
      this.tick(now);
      return true;
    } finally {
      await this.opts.leaderLock.release().catch(() => {});
    }
  }

  stop(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  /** Evaluate a single tick at `now`. Public for tests. */
  tick(now: Date = this.nowFn()): void {
    const ids = this.evaluateOnce(now);
    const minuteEpoch = Math.floor(now.getTime() / 60_000);
    for (const id of ids) {
      const key = `${id}:${minuteEpoch}`;
      if (this.fired.has(key)) continue;
      this.fired.add(key);
      void this.opts.trigger(id, {
        triggerType: 'schedule',
        firedAt: now.toISOString(),
      }).catch(err => {
        console.error('[scheduleEvaluator] trigger failed', id, err);
      });
    }
    // Bound the dedupe set: keep only entries from the last ~10 minutes.
    if (this.fired.size > 1024) {
      const cutoff = minuteEpoch - 10;
      for (const k of this.fired) {
        const m = Number(k.split(':').pop());
        if (Number.isFinite(m) && m < cutoff) this.fired.delete(k);
      }
    }
  }

  /**
   * Pure-ish: returns the pipeline ids whose schedule matches `now`.
   * Does NOT consult or update the dedupe set.
   */
  evaluateOnce(now: Date): string[] {
    const out: string[] = [];
    for (const p of this.opts.listPipelines()) {
      if (p.status !== 'published') continue;
      if (!p.triggerBinding || p.triggerBinding.event !== 'schedule') continue;
      const expr = p.triggerBinding.schedule;
      if (!expr) continue;
      const cron = parseCronExpression(expr);
      if (!cron) {
        console.error('[scheduleEvaluator] invalid cron, skipping', p.id, expr);
        continue;
      }
      if (cronMatches(cron, now)) out.push(p.id);
    }
    return out;
  }

  /**
   * Fire a pipeline immediately, ignoring schedule. Useful for tests + the
   * "Run now" UI affordance Phase 4 will surface on scheduled triggers.
   */
  triggerNow(pipelineId: string): void {
    void this.opts.trigger(pipelineId, {
      triggerType: 'schedule',
      firedAt: this.nowFn().toISOString(),
      manual: true,
    }).catch(err => {
      console.error('[scheduleEvaluator] manual trigger failed', pipelineId, err);
    });
  }
}

export function createScheduleEvaluator(
  opts: ScheduleEvaluatorOptions,
): ScheduleEvaluator {
  return new ScheduleEvaluator(opts);
}
