// platform-api/src/pipeline/judge/JudgmentAggregateRepository.ts
//
// v2 — per-rubric/per-pipeline/per-day rollups of judge results, used
// for trend sparklines + cost summaries in the UI.
//
// Storage shape (table `pipeline-judgment-aggregates`):
//   PK: aggregateKey (S) — `${pipelineId}#${rubricId}#${dayBucket}`
//   Attributes (numeric — atomically ADD-incremented per judgment):
//     numJudgments, numPassed, numFailed,
//     sumOverallScore, sumTokensIn, sumTokensOut, sumCostUsd,
//     minOverallScore (SET if smaller; tracked best-effort),
//     maxOverallScore (SET if larger; tracked best-effort)
//   Plus de-normalized scalar fields for query convenience:
//     pipelineId, rubricId, dayBucket, firstJudgedAt, lastJudgedAt
//   GSI `pipeline-day-index`:
//     PK: pipelineId (S)
//     SK: dayBucket (S) — yyyy-mm-dd UTC
//     Projection: ALL
//   GSI `rubric-day-index`:
//     PK: rubricId (S)
//     SK: dayBucket (S)
//     Projection: ALL
//   TTL attribute: `ttlEpoch` (N, unix-seconds) — DDB drops rows older
//     than the configured retention window (default 365 days — aggregates
//     live an order of magnitude longer than per-judgment rows since they
//     ARE the trend substrate).
//
// recordJudgment() does ONE UpdateItem per call — atomic, idempotent under
// at-most-once semantics. It deliberately does not de-dupe by judgmentId
// (the per-judgment table already enforces uniqueness; double-fire across
// process restarts would just double-count, but the executor's EventBus
// is in-process so that's not a real concern).
//
// min/max are tracked best-effort via UpdateItem expressions — DDB has no
// native MIN/MAX so we use a conditional update that swaps the value only
// when the new candidate is smaller (or larger). Two extra round-trips
// per record in the worst case (when the value updates), one when it
// doesn't. Acceptable for the 1-2 judgments per pipeline run rate.

import {
  DynamoDBDocumentClient,
  QueryCommand,
  UpdateCommand,
} from '@aws-sdk/lib-dynamodb';
import { docClient as defaultDocClient } from '../../lib/aws-clients';
import { tableName } from '../../lib/ddb-table-name';
import type { JudgmentResult } from './types';

const PIPELINE_JUDGMENT_AGGREGATES_TABLE = tableName('pipeline-judgment-aggregates');
const PIPELINE_DAY_INDEX = 'pipeline-day-index';
const RUBRIC_DAY_INDEX = 'rubric-day-index';

const DEFAULT_TTL_DAYS = 365;
const SECONDS_PER_DAY = 24 * 60 * 60;

export interface JudgmentAggregateRepositoryOptions {
  ttlDays?: number;
  now?: () => number;
}

/** Persisted aggregate row shape (what queries return). */
export interface AggregateRow {
  aggregateKey: string;
  pipelineId: string;
  rubricId: string;
  /** yyyy-mm-dd in UTC. */
  dayBucket: string;
  numJudgments: number;
  numPassed: number;
  numFailed: number;
  sumOverallScore: number;
  sumTokensIn: number;
  sumTokensOut: number;
  sumCostUsd: number;
  /** Best-effort min/max — tracked via conditional UpdateItem. */
  minOverallScore?: number;
  maxOverallScore?: number;
  firstJudgedAt: string;
  lastJudgedAt: string;
  ttlEpoch: number;
}

export interface ListAggregatesOptions {
  /** Inclusive lower bound on dayBucket. */
  since?: string;
  /** Inclusive upper bound on dayBucket. */
  until?: string;
  limit?: number;
}

export interface ListAggregatesResult {
  items: AggregateRow[];
  nextCursor?: string;
}

/**
 * yyyy-mm-dd in UTC. Pure helper — exported for symmetry with
 * JudgeBudgetGuard's day-bucket scheme (same convention).
 */
export function utcDayBucket(epochMs: number): string {
  const d = new Date(epochMs);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  const day = String(d.getUTCDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
}

export function buildAggregateKey(
  pipelineId: string,
  rubricId: string,
  dayBucket: string,
): string {
  return `${pipelineId}#${rubricId}#${dayBucket}`;
}

export class JudgmentAggregateRepository {
  private ttlSeconds: number;
  private now: () => number;

  constructor(
    private docClient: DynamoDBDocumentClient = defaultDocClient,
    options: JudgmentAggregateRepositoryOptions = {},
  ) {
    this.ttlSeconds = (options.ttlDays ?? DEFAULT_TTL_DAYS) * SECONDS_PER_DAY;
    this.now = options.now ?? (() => Date.now());
  }

  /**
   * Atomically increments the aggregate row for the given judgment's
   * (pipelineId, rubricId, dayBucket) tuple. ONE round-trip — DDB
   * UpdateItem with ADD performs the upsert.
   *
   * Pass-through best-effort min/max updates happen on a separate
   * round-trip; failures there are swallowed (the count/sum invariants
   * are the load-bearing ones for trend rendering).
   */
  async recordJudgment(result: JudgmentResult): Promise<void> {
    const judgedAtMs = Date.parse(result.judgedAt);
    const dayBucket = utcDayBucket(
      Number.isFinite(judgedAtMs) ? judgedAtMs : this.now(),
    );
    const aggregateKey = buildAggregateKey(result.pipelineId, result.rubricId, dayBucket);
    const ttlEpoch = Math.floor(this.now() / 1000) + this.ttlSeconds;
    const passed = result.overallPassed ? 1 : 0;
    const failed = result.overallPassed ? 0 : 1;

    // Primary UpdateItem — ADD all counters, SET de-normalized scalars +
    // lastJudgedAt. `if_not_exists` initializes firstJudgedAt + the
    // identity scalars only on first write.
    await this.docClient.send(
      new UpdateCommand({
        TableName: PIPELINE_JUDGMENT_AGGREGATES_TABLE,
        Key: { aggregateKey },
        UpdateExpression: [
          'ADD numJudgments :one, numPassed :passed, numFailed :failed,',
          'sumOverallScore :score, sumTokensIn :tin, sumTokensOut :tout,',
          'sumCostUsd :cost',
          'SET pipelineId = if_not_exists(pipelineId, :pid),',
          'rubricId = if_not_exists(rubricId, :rid),',
          'dayBucket = if_not_exists(dayBucket, :day),',
          'firstJudgedAt = if_not_exists(firstJudgedAt, :judgedAt),',
          'lastJudgedAt = :judgedAt,',
          'ttlEpoch = :ttl',
        ].join(' '),
        ExpressionAttributeValues: {
          ':one': 1,
          ':passed': passed,
          ':failed': failed,
          ':score': result.overallScore,
          ':tin': result.tokensIn,
          ':tout': result.tokensOut,
          ':cost': result.costUsd,
          ':pid': result.pipelineId,
          ':rid': result.rubricId,
          ':day': dayBucket,
          ':judgedAt': result.judgedAt,
          ':ttl': ttlEpoch,
        },
      }),
    );

    // Best-effort min/max — conditional SETs that only fire when the new
    // candidate beats the stored value. We accept the small cost of two
    // extra round-trips for the trend extremes.
    await Promise.all([
      this.updateExtreme(aggregateKey, 'minOverallScore', result.overallScore, '<'),
      this.updateExtreme(aggregateKey, 'maxOverallScore', result.overallScore, '>'),
    ]).catch(() => {
      // Min/max are best-effort; the row's count/sum invariants are
      // correct regardless. Swallow conditional-check failures (they
      // mean "no update was needed", which is the expected path most
      // of the time).
    });
  }

  /**
   * List aggregates by pipeline, newest day first. Optional date-range
   * filter via since/until (inclusive, ISO yyyy-mm-dd).
   */
  async listByPipeline(
    pipelineId: string,
    options: ListAggregatesOptions = {},
  ): Promise<ListAggregatesResult> {
    return this.queryGsi(PIPELINE_DAY_INDEX, 'pipelineId', pipelineId, options);
  }

  /** List aggregates by rubric across pipelines, newest day first. */
  async listByRubric(
    rubricId: string,
    options: ListAggregatesOptions = {},
  ): Promise<ListAggregatesResult> {
    return this.queryGsi(RUBRIC_DAY_INDEX, 'rubricId', rubricId, options);
  }

  private async queryGsi(
    indexName: string,
    pkAttr: string,
    pkValue: string,
    options: ListAggregatesOptions,
  ): Promise<ListAggregatesResult> {
    const { limit = 30, since, until } = options;
    const keyExprParts = ['#pk = :pk'];
    const values: Record<string, unknown> = { ':pk': pkValue };
    const names: Record<string, string> = { '#pk': pkAttr };

    if (since && until) {
      keyExprParts.push('#sk BETWEEN :since AND :until');
      values[':since'] = since;
      values[':until'] = until;
      names['#sk'] = 'dayBucket';
    } else if (since) {
      keyExprParts.push('#sk >= :since');
      values[':since'] = since;
      names['#sk'] = 'dayBucket';
    } else if (until) {
      keyExprParts.push('#sk <= :until');
      values[':until'] = until;
      names['#sk'] = 'dayBucket';
    }

    const result = await this.docClient.send(
      new QueryCommand({
        TableName: PIPELINE_JUDGMENT_AGGREGATES_TABLE,
        IndexName: indexName,
        KeyConditionExpression: keyExprParts.join(' AND '),
        ExpressionAttributeNames: names,
        ExpressionAttributeValues: values,
        Limit: limit,
        ScanIndexForward: false, // newest day first
      }),
    );

    return {
      items: (result.Items ?? []) as AggregateRow[],
      nextCursor: undefined,
    };
  }

  private async updateExtreme(
    aggregateKey: string,
    attr: 'minOverallScore' | 'maxOverallScore',
    candidate: number,
    op: '<' | '>',
  ): Promise<void> {
    try {
      await this.docClient.send(
        new UpdateCommand({
          TableName: PIPELINE_JUDGMENT_AGGREGATES_TABLE,
          Key: { aggregateKey },
          UpdateExpression: `SET #attr = :candidate`,
          ConditionExpression: `attribute_not_exists(#attr) OR :candidate ${op} #attr`,
          ExpressionAttributeNames: { '#attr': attr },
          ExpressionAttributeValues: { ':candidate': candidate },
        }),
      );
    } catch (err) {
      // Conditional check failure means the existing extreme is already
      // better — expected outcome on the steady-state path.
      if ((err as { name?: string })?.name !== 'ConditionalCheckFailedException') {
        throw err;
      }
    }
  }
}
