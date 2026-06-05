// platform-api/src/pipeline/image/VisionResultRepository.ts
//
// DDB-backed persistence for `VisionResult` records produced by the
// VisionStepHandler. Append-only — vision results are immutable
// historical observations. Re-running the step produces a new visionId.
//
// Storage shape (table `pipeline-vision-results`):
//   PK: visionId (S) — caller-supplied UUID (set in VisionStepHandler)
//   GSI `pipeline-time-index`:
//     PK: pipelineId (S)
//     SK: analyzedAt (S, ISO 8601)
//     Projection: ALL
//   GSI `run-time-index`:
//     PK: runId (S)
//     SK: analyzedAt (S, ISO 8601)
//     Projection: ALL
//   TTL attribute: `ttlEpoch` (N, unix-seconds) — DDB drops rows older
//     than the configured retention window (default 30 days, matching
//     the image-blob TTL; vision results without their source images
//     have limited audit value).
//
// rawText spillover: when `rawModelText` is ≥32KB we strip it from the
// primary DDB item to stay under the 400KB cap. If a RawTextStore is
// wired (DdbRawTextStore pointing at `pipeline-vision-rawtext`), we
// persist the full body and store the key on `rawTextKey`. Without a
// store, the body is dropped at persist time (still available on the
// live `pipeline:image:vision:analyzed` event). `getRawText()`
// re-hydrates lazily.
//
// Code parity with platform-api/src/pipeline/judge/JudgmentRepository.ts.

import {
  DynamoDBDocumentClient,
  QueryCommand,
} from '@aws-sdk/lib-dynamodb';
import { BaseRepository } from '../../repositories/BaseRepository';
import { docClient as defaultDocClient } from '../../lib/aws-clients';
import { tableName } from '../../lib/ddb-table-name';
import type { VisionResult } from './types';
import type { RawTextStore } from '../judge/RawTextStore';

const PIPELINE_VISION_RESULTS_TABLE = tableName('pipeline-vision-results');
const PIPELINE_TIME_INDEX = 'pipeline-time-index';
const RUN_TIME_INDEX = 'run-time-index';

const DEFAULT_TTL_DAYS = 30;
const SECONDS_PER_DAY = 24 * 60 * 60;

/**
 * rawText size cap for inline DDB storage. Above this, the field is
 * stripped and `rawTextTruncated:true` is set. Tuned to keep total item
 * size comfortably under the 400KB DDB cap with extracted JSON + caption.
 */
const RAW_TEXT_INLINE_CAP_BYTES = 32 * 1024;

export interface VisionResultRepositoryOptions {
  ttlDays?: number;
  now?: () => number;
  /**
   * Optional spillover for >32KB rawModelText bodies. When provided,
   * oversized bodies are uploaded to the store and the resulting key
   * is persisted on `rawTextKey`; `getRawText()` re-hydrates lazily.
   * When omitted, oversized bodies are dropped on persist (only the
   * `rawTextTruncated:true` flag is preserved).
   */
  rawTextStore?: RawTextStore;
}

/** Persistence row — adds DDB-only fields on top of VisionResult. */
export interface StoredVisionResult extends VisionResult {
  ttlEpoch: number;
  /** True when rawModelText exceeded RAW_TEXT_INLINE_CAP_BYTES and was stripped. */
  rawTextTruncated?: boolean;
  /** Spillover storage key for the full body (set when a rawTextStore was wired at write). */
  rawTextKey?: string;
}

export interface ListVisionResultsOptions {
  limit?: number;
  since?: string;
  until?: string;
  cursor?: string;
}

export interface ListVisionResultsResult {
  items: StoredVisionResult[];
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

export class VisionResultRepository {
  private store: BaseRepository;
  private ttlSeconds: number;
  private now: () => number;
  private rawTextStore: RawTextStore | undefined;

  constructor(
    private docClient: DynamoDBDocumentClient = defaultDocClient,
    options: VisionResultRepositoryOptions = {},
  ) {
    this.store = new BaseRepository(PIPELINE_VISION_RESULTS_TABLE, docClient);
    this.ttlSeconds = (options.ttlDays ?? DEFAULT_TTL_DAYS) * SECONDS_PER_DAY;
    this.now = options.now ?? (() => Date.now());
    this.rawTextStore = options.rawTextStore;
  }

  /**
   * Implements `VisionResultRepositoryLike.persistVisionResult`. Idempotent
   * on `visionId` — overwriting an existing row replaces it (the handler
   * generates a fresh UUID per call so collisions are not expected).
   */
  async persistVisionResult(result: VisionResult): Promise<void> {
    const ttlEpoch = Math.floor(this.now() / 1000) + this.ttlSeconds;
    const rawTextBytes = result.rawModelText
      ? Buffer.byteLength(result.rawModelText, 'utf8')
      : 0;
    const truncate = rawTextBytes >= RAW_TEXT_INLINE_CAP_BYTES;

    // Spillover: write to the spillover store BEFORE the primary DDB
    // row so a successful primary always has a valid rawTextKey
    // pointing at a real object. Spillover failures don't fail the
    // primary write — fall back to drop-the-body. Persistence
    // integrity > spillover completeness.
    let rawTextKey: string | undefined;
    if (truncate && this.rawTextStore && result.rawModelText) {
      try {
        rawTextKey = await this.rawTextStore.put(result.visionId, result.rawModelText);
      } catch (err) {
        console.warn('VisionResultRepository: rawText spillover upload failed', err);
      }
    }

    const item: StoredVisionResult = {
      ...result,
      ...(truncate
        ? {
            rawTextTruncated: true,
            ...(rawTextKey ? { rawTextKey } : {}),
          }
        : {}),
      ttlEpoch,
    };
    // When truncating, explicitly delete the key so DDB never sees
    // `rawModelText: undefined` (which some marshallers serialize as a
    // NULL attribute instead of omitting the field entirely).
    if (truncate) {
      delete item.rawModelText;
    }

    await this.store.putItem(item as unknown as Record<string, unknown>);
  }

  /**
   * Lazily fetch the full rawModelText for a vision result. Returns the
   * body from the spillover store when the row was spilled, the inline
   * body when it wasn't truncated, or null when the body is unavailable
   * (no store configured at write, or the spillover fetch failed).
   */
  async getRawText(visionId: string): Promise<string | null> {
    const row = await this.getById(visionId);
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
          '[VisionResultRepository] spillover key set but body unavailable',
          { visionId, rawTextKey: row.rawTextKey },
        );
      }
      return body;
    }
    return null;
  }

  async getById(visionId: string): Promise<StoredVisionResult | null> {
    return this.store.getItem<StoredVisionResult>({ visionId });
  }

  async listByPipeline(
    pipelineId: string,
    options: ListVisionResultsOptions = {},
  ): Promise<ListVisionResultsResult> {
    return this.queryGsi(PIPELINE_TIME_INDEX, 'pipelineId', pipelineId, options);
  }

  async listByRun(
    runId: string,
    options: ListVisionResultsOptions = {},
  ): Promise<ListVisionResultsResult> {
    return this.queryGsi(RUN_TIME_INDEX, 'runId', runId, options);
  }

  private async queryGsi(
    indexName: string,
    pkAttr: string,
    pkValue: string,
    options: ListVisionResultsOptions,
  ): Promise<ListVisionResultsResult> {
    const { limit = 100, since, until, cursor } = options;
    const keyExprParts = ['#pk = :pk'];
    const values: Record<string, unknown> = { ':pk': pkValue };
    const names: Record<string, string> = { '#pk': pkAttr };

    if (since && until) {
      keyExprParts.push('#sk BETWEEN :since AND :until');
      values[':since'] = since;
      values[':until'] = until;
      names['#sk'] = 'analyzedAt';
    } else if (since) {
      keyExprParts.push('#sk >= :since');
      values[':since'] = since;
      names['#sk'] = 'analyzedAt';
    } else if (until) {
      keyExprParts.push('#sk <= :until');
      values[':until'] = until;
      names['#sk'] = 'analyzedAt';
    }

    const result = await this.docClient.send(
      new QueryCommand({
        TableName: PIPELINE_VISION_RESULTS_TABLE,
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
      items: (result.Items ?? []) as StoredVisionResult[],
      nextCursor: encodeCursor(result.LastEvaluatedKey),
    };
  }
}
