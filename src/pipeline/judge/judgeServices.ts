// platform-api/src/pipeline/judge/judgeServices.ts
//
// Single source of construction for the judge repositories + stores so
// the step handler (registerJudgeHandlers.ts) and the HTTP routes
// (routes/judgments.ts) share the exact same instances + env wiring.
//
// Lazy construction: the `get*` accessors instantiate on first call.
// This matters for tests + envs where env vars are read at runtime,
// and keeps the boot cost paid only by the consumer that actually
// uses the service.

import { JudgmentRepository } from './JudgmentRepository';
import { JudgmentAggregateRepository } from './JudgmentAggregateRepository';
import { DdbRawTextStore, type RawTextStore } from './RawTextStore';

let _rawTextStore: RawTextStore | undefined | null = null;
let _judgmentRepo: JudgmentRepository | null = null;
let _judgmentAggregateRepo: JudgmentAggregateRepository | null = null;

/**
 * The configured RawTextStore, or undefined when JUDGE_RAW_TEXT_ENABLED
 * is not 'true'. Cached after first read.
 *
 * The store writes to the `pipeline-judgment-rawtext` DDB table — see
 * docs/judge-PROVISIONING.md for the schema. The application code does
 * NOT auto-create the table. When the table is missing, the store's
 * writes fail and JudgmentRepository's existing soft-fail path drops
 * the body (same behavior as JUDGE_RAW_TEXT_ENABLED=false).
 */
export function getRawTextStore(): RawTextStore | undefined {
  if (_rawTextStore === null) {
    const enabled =
      (process.env.JUDGE_RAW_TEXT_ENABLED ?? 'true').trim().toLowerCase() !== 'false';
    _rawTextStore = enabled ? new DdbRawTextStore() : undefined;
  }
  return _rawTextStore;
}

/** Singleton JudgmentRepository wired with the configured RawTextStore. */
export function getJudgmentRepo(): JudgmentRepository {
  if (!_judgmentRepo) {
    const store = getRawTextStore();
    _judgmentRepo = new JudgmentRepository(undefined, store ? { rawTextStore: store } : {});
  }
  return _judgmentRepo;
}

/** Singleton JudgmentAggregateRepository. */
export function getJudgmentAggregateRepo(): JudgmentAggregateRepository {
  if (!_judgmentAggregateRepo) {
    _judgmentAggregateRepo = new JudgmentAggregateRepository();
  }
  return _judgmentAggregateRepo;
}

/**
 * Test-only — drop every cached singleton so the next `get*` call
 * rebuilds with current env vars. Used by integration tests that
 * toggle JUDGE_RAW_TEXT_ENABLED mid-run.
 */
export function __resetJudgeServicesForTests(): void {
  _rawTextStore = null;
  _judgmentRepo = null;
  _judgmentAggregateRepo = null;
}
