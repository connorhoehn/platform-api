// platform-api/src/pipeline/judge/JudgmentRepository.ts
//
// DDB-backed persistence for `JudgmentResult` records produced by the
// JudgeStepHandler. Append-only — judgments are immutable historical
// observations. Re-running the judge produces a new judgmentId.
//
// Storage shape (table `pipeline-judgments`):
//   PK: judgmentId (S) — caller-supplied UUID (set in JudgeStepHandler)
//   GSI `pipeline-time-index`:
//     PK: pipelineId (S)
//     SK: judgedAt (S, ISO 8601)
//     Projection: ALL
//   GSI `run-time-index`:
//     PK: runId (S)
//     SK: judgedAt (S, ISO 8601)
//     Projection: ALL
//   GSI `rubric-time-index`:
//     PK: rubricId (S)
//     SK: judgedAt (S, ISO 8601)
//     Projection: ALL
//   TTL attribute: `ttlEpoch` (N, unix-seconds) — DDB drops rows older
//     than the configured retention window (default 90 days; judgments
//     live longer than audit rows because they're the substrate for
//     trend analysis + rubric tuning).
//
// rawText spillover: when `rawModelText` is ≥32KB we strip it from the
// primary DDB item to stay under the 400KB cap. v2: if a RawTextStore
// is wired (DdbRawTextStore in production — writes to a second DDB
// table), we persist the full body and store the resulting key on
// `rawTextKey`. Without a store, we just set `rawTextTruncated:true`
// and the body is dropped at persist time (still available on the live
// `pipeline:judge:scored` event for session observers).
// `getRawText()` re-hydrates the full body lazily on demand.
//
// `persistJudgment` matches the `JudgmentRepositoryLike` interface in
// JudgeStepHandler.ts — pass an instance of this class into the
// handler's `repository` option and you get DDB persistence for free.

import {
  DynamoDBDocumentClient,
  QueryCommand,
} from '@aws-sdk/lib-dynamodb';
import { BaseRepository } from '../../repositories/BaseRepository';
import { docClient as defaultDocClient } from '../../lib/aws-clients';
import { tableName } from '../../lib/ddb-table-name';
import type { JudgmentResult } from './types';
import type { RawTextStore } from './RawTextStore';

const PIPELINE_JUDGMENTS_TABLE = tableName('pipeline-judgments');
const PIPELINE_TIME_INDEX = 'pipeline-time-index';
const RUN_TIME_INDEX = 'run-time-index';
const RUBRIC_TIME_INDEX = 'rubric-time-index';

const DEFAULT_TTL_DAYS = 90;
const SECONDS_PER_DAY = 24 * 60 * 60;

/**
 * rawText size cap for inline DDB storage. Above this, the field is
 * stripped and `rawTextTruncated:true` is set. Tuned to keep total item
 * size comfortably under the 400KB DDB cap with rubric snapshot + 30
 * criteria + rationales.
 */
const RAW_TEXT_INLINE_CAP_BYTES = 32 * 1024;

export interface JudgmentRepositoryOptions {
  ttlDays?: number;
  now?: () => number;
  /**
   * Optional v2 spillover for >32KB rawModelText bodies. When provided,
   * oversized bodies are uploaded to the store and the resulting key is
   * persisted on `rawTextKey`; `getRawText()` re-hydrates lazily.
   * When omitted, oversized bodies are dropped and only the
   * `rawTextTruncated:true` flag is set.
   */
  rawTextStore?: RawTextStore;
}

/** Persistence row — adds DDB-only fields on top of JudgmentResult. */
export interface StoredJudgment extends JudgmentResult {
  ttlEpoch: number;
  /** True when rawModelText exceeded RAW_TEXT_INLINE_CAP_BYTES and was stripped. */
  rawTextTruncated?: boolean;
  /** Spillover storage key for the full body (set when a rawTextStore was wired at write). */
  rawTextKey?: string;
}

export interface ListJudgmentsOptions {
  limit?: number;
  since?: string;
  until?: string;
  cursor?: string;
}

export interface ListJudgmentsResult {
  items: StoredJudgment[];
  nextCursor?: string;
}

function encodeCursor(key: Record<string, unknown> | undefined): string | undefined {
  if (!key) return undefined;
  return Buffer.from(JSON.stringify(key), 'utf8').toString('base64');
}

function decodeCursor(token: string | undefined): Record<string, unknown> | undefined {
  if (!token) return undefined;
  try {
    return JSON.parse(Buffer.from(token, 'base64').toString('utf8')) as Record<string, unknown>;
  } catch {
    return undefined;
  }
}

export class JudgmentRepository {
  private store: BaseRepository;
  private ttlSeconds: number;
  private now: () => number;
  private rawTextStore: RawTextStore | undefined;

  constructor(
    private docClient: DynamoDBDocumentClient = defaultDocClient,
    options: JudgmentRepositoryOptions = {},
  ) {
    this.store = new BaseRepository(PIPELINE_JUDGMENTS_TABLE, docClient);
    this.ttlSeconds = (options.ttlDays ?? DEFAULT_TTL_DAYS) * SECONDS_PER_DAY;
    this.now = options.now ?? (() => Date.now());
    this.rawTextStore = options.rawTextStore;
  }

  /**
   * Implements `JudgmentRepositoryLike.persistJudgment`. Idempotent on
   * `judgmentId` — overwriting an existing row replaces it (the handler
   * generates a fresh UUID per call so collisions are not expected).
   */
  async persistJudgment(result: JudgmentResult): Promise<void> {
    const ttlEpoch = Math.floor(this.now() / 1000) + this.ttlSeconds;
    const rawTextBytes = result.rawModelText
      ? Buffer.byteLength(result.rawModelText, 'utf8')
      : 0;
    const truncate = rawTextBytes >= RAW_TEXT_INLINE_CAP_BYTES;

    // v2 spillover: write to the spillover store BEFORE the primary DDB
    // row so a successful primary always has a valid rawTextKey pointing
    // at a real object. Spillover failures don't fail the primary write
    // — we fall back to the v1 behavior (`rawTextTruncated:true`, body
    // dropped) and log. Persistence integrity > spillover completeness.
    let rawTextKey: string | undefined;
    if (truncate && this.rawTextStore && result.rawModelText) {
      try {
        rawTextKey = await this.rawTextStore.put(result.judgmentId, result.rawModelText);
      } catch (err) {
        console.warn('JudgmentRepository: rawText spillover upload failed', err);
      }
    }

    const item: StoredJudgment = {
      ...result,
      // strip the heavy field rather than rejecting the write — analytics
      // care about scores + criteria; rawModelText is just for audit drill-in
      ...(truncate
        ? {
            rawModelText: undefined,
            rawTextTruncated: true,
            ...(rawTextKey ? { rawTextKey } : {}),
          }
        : {}),
      ttlEpoch,
    };

    await this.store.putItem(item as unknown as Record<string, unknown>);
  }

  /**
   * Lazily fetch the full rawModelText for a judgment. Returns the body
   * from the spillover store when the row was spilled, the inline body
   * when it wasn't truncated, or null when the body is unavailable (no
   * store configured at write, or the spillover fetch failed).
   */
  async getRawText(judgmentId: string): Promise<string | null> {
    const row = await this.getById(judgmentId);
    if (!row) return null;
    if (row.rawModelText) return row.rawModelText;
    if (row.rawTextKey && this.rawTextStore) {
      const body = await this.rawTextStore.get(row.rawTextKey);
      if (body === null) {
        // The row CLAIMS a spillover key exists but the store can't
        // serve it — most commonly because the spillover table was
        // truncated, the write originally failed silently, or the
        // store impl returned null after a transient DDB error
        // (DdbRawTextStore logs the underlying cause).
        // eslint-disable-next-line no-console
        console.warn(
          '[JudgmentRepository] spillover key set but body unavailable',
          { judgmentId, rawTextKey: row.rawTextKey },
        );
      }
      return body;
    }
    return null;
  }

  async getById(judgmentId: string): Promise<StoredJudgment | null> {
    return this.store.getItem<StoredJudgment>({ judgmentId });
  }

  async listByPipeline(
    pipelineId: string,
    options: ListJudgmentsOptions = {},
  ): Promise<ListJudgmentsResult> {
    return this.queryGsi(PIPELINE_TIME_INDEX, 'pipelineId', pipelineId, options);
  }

  async listByRun(
    runId: string,
    options: ListJudgmentsOptions = {},
  ): Promise<ListJudgmentsResult> {
    return this.queryGsi(RUN_TIME_INDEX, 'runId', runId, options);
  }

  async listByRubric(
    rubricId: string,
    options: ListJudgmentsOptions = {},
  ): Promise<ListJudgmentsResult> {
    return this.queryGsi(RUBRIC_TIME_INDEX, 'rubricId', rubricId, options);
  }

  private async queryGsi(
    indexName: string,
    pkAttr: string,
    pkValue: string,
    options: ListJudgmentsOptions,
  ): Promise<ListJudgmentsResult> {
    const { limit = 100, since, until, cursor } = options;
    const keyExprParts = ['#pk = :pk'];
    const values: Record<string, unknown> = { ':pk': pkValue };
    const names: Record<string, string> = { '#pk': pkAttr };

    if (since && until) {
      keyExprParts.push('#sk BETWEEN :since AND :until');
      values[':since'] = since;
      values[':until'] = until;
      names['#sk'] = 'judgedAt';
    } else if (since) {
      keyExprParts.push('#sk >= :since');
      values[':since'] = since;
      names['#sk'] = 'judgedAt';
    } else if (until) {
      keyExprParts.push('#sk <= :until');
      values[':until'] = until;
      names['#sk'] = 'judgedAt';
    }

    const result = await this.docClient.send(
      new QueryCommand({
        TableName: PIPELINE_JUDGMENTS_TABLE,
        IndexName: indexName,
        KeyConditionExpression: keyExprParts.join(' AND '),
        ExpressionAttributeNames: names,
        ExpressionAttributeValues: values,
        Limit: limit,
        ScanIndexForward: false, // newest first
        ExclusiveStartKey: decodeCursor(cursor),
      }),
    );

    return {
      items: (result.Items ?? []) as StoredJudgment[],
      nextCursor: encodeCursor(result.LastEvaluatedKey),
    };
  }
}
