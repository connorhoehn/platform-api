// platform-api/src/pipeline/judge/judgeServices.ts
//
// Single source of construction for the judge repositories + stores so
// that the step handler (registerJudgeHandlers.ts) and the HTTP routes
// (routes/judgments.ts) share the exact same instances + env wiring.
//
// Without this, the route would construct its own JudgmentRepository
// with no rawTextStore — `getRawText` would always return null for
// spilled-to-S3 bodies. By centralizing here, both paths see the same
// store and the same retention configuration.
//
// Lazy construction: the `get*` accessors instantiate on first call.
// This matters for tests + envs where env vars are read at runtime, and
// keeps the boot cost paid only by the consumer that actually uses the
// service.

import { JudgmentRepository } from './JudgmentRepository';
import { JudgmentAggregateRepository } from './JudgmentAggregateRepository';
import { S3RawTextStore, type RawTextStore } from './RawTextStore';

let _rawTextStore: RawTextStore | undefined | null = null;
let _judgmentRepo: JudgmentRepository | null = null;
let _judgmentAggregateRepo: JudgmentAggregateRepository | null = null;

/**
 * The configured RawTextStore, or undefined when JUDGE_RAW_TEXT_BUCKET
 * is unset. Cached after first read so multiple consumers share one
 * S3Client. Returns `undefined` (not `null`) for ergonomic feature-flag
 * patterns at the call site.
 */
export function getRawTextStore(): RawTextStore | undefined {
  if (_rawTextStore === null) {
    const bucket = process.env.JUDGE_RAW_TEXT_BUCKET?.trim();
    _rawTextStore = bucket ? new S3RawTextStore({ bucket }) : undefined;
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
 * Test-only — drop every cached singleton so the next `get*` call rebuilds
 * with current env vars. Used by integration tests that toggle
 * JUDGE_RAW_TEXT_BUCKET mid-run.
 */
export function __resetJudgeServicesForTests(): void {
  _rawTextStore = null;
  _judgmentRepo = null;
  _judgmentAggregateRepo = null;
}
