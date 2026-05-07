// Hub task #197 — persist pipeline run history to DynamoDB.
//
// Queryable index of pipeline run history. The Raft state machine + WAL remain
// the source of truth for replay; this repository backs the
// `/api/pipelines/:pipelineId/runs` operator-facing surface.

import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { BaseRepository } from './BaseRepository';
import { tableName } from '../lib/ddb-table-name';

export interface TokenUsageByModel {
  tokensIn: number;
  tokensOut: number;
}

/**
 * One execution attempt of a step. Hub task #1280.
 * Mirrors `frontend/src/types/pipeline.ts:StepAttempt`. Kept duplicated rather
 * than imported to avoid a frontend→backend type-import cycle.
 */
export interface StepAttempt {
  attemptNumber: number;
  startedAt: string;
  completedAt?: string;
  durationMs?: number;
  input?: unknown;
  output?: unknown;
  error?: string;
  errorCategory?: string;
  retryAfterMs?: number;
  idempotencyKey: string;
}

/**
 * Per-step persistence shape. Top-level fields reflect the LATEST attempt
 * (back-compat with pre-#1280 rows that had no `attempts[]`). `attempts[]`
 * carries the full history when present.
 */
export interface StepSummary {
  status?: string;
  startedAt?: string;
  completedAt?: string;
  durationMs?: number;
  output?: unknown;
  error?: string;
  attempts?: StepAttempt[];
}

export interface PipelineRunItem {
  pipelineId: string;
  runId: string;
  status: string; // 'pending' | 'running' | 'succeeded' | 'failed' | 'canceled' | 'interrupted'
  triggeredBy: string; // userId or 'system'
  triggerType: string; // 'manual' | 'schedule' | 'webhook' | 'document-event'
  startedAt: string; // ISO timestamp
  completedAt?: string; // ISO timestamp
  /** ISO timestamp set by the orphan-scan when transitioning `running` → `interrupted`. */
  interruptedAt?: string;
  /** ISO timestamp set by StuckRunDetector when a heartbeat threshold is exceeded. */
  stuckAt?: string;
  /** ISO timestamp of the last pipeline event observed for this run. Used by StuckRunDetector. */
  lastEventAt?: string;
  stepsSummary?: Record<string, StepSummary>;
  error?: string;
  /** Per-model token usage accumulated from pipeline:llm:response events. */
  tokenUsage?: Record<string, TokenUsageByModel>;
  /** Estimated total cost in USD based on the pricing table. */
  estimatedCostUsd?: number;
  /** Epoch seconds — set by application for auto-expiry of old runs. */
  ttl?: number;
}

/**
 * Read helper: synthesise a single-attempt history for legacy rows that lack
 * `attempts[]`. Used by readers that want a uniform per-attempt view (UI,
 * retry-from-step). Returns `[]` if the step has no recorded state at all.
 */
export function readAttempts(summary: StepSummary | undefined, runId: string, nodeId: string): StepAttempt[] {
  if (!summary) return [];
  if (summary.attempts && summary.attempts.length > 0) return summary.attempts;
  // Synthetic single attempt from latest-only fields (pre-#1280 rows).
  if (!summary.startedAt && !summary.completedAt && summary.status === undefined) return [];
  return [{
    attemptNumber: 1,
    startedAt: summary.startedAt ?? summary.completedAt ?? new Date(0).toISOString(),
    completedAt: summary.completedAt,
    durationMs: summary.durationMs,
    output: summary.output,
    error: summary.error,
    idempotencyKey: `${runId}:${nodeId}:1`,
  }];
}

export class PipelineRunRepository extends BaseRepository {
  constructor(docClient: DynamoDBDocumentClient) {
    super(tableName('pipeline-runs'), docClient);
  }

  /** Upsert a pipeline run. */
  async put(item: PipelineRunItem): Promise<void> {
    return this.putItem(item as unknown as Record<string, unknown>);
  }

  /** Fetch a single run by composite key. */
  async get(pipelineId: string, runId: string): Promise<PipelineRunItem | null> {
    return this.getItem<PipelineRunItem>({ pipelineId, runId });
  }

  /** List runs for a pipeline, most recent first by runId. */
  async listByPipeline(
    pipelineId: string,
    opts: { limit?: number } = {},
  ): Promise<PipelineRunItem[]> {
    const limit = Math.max(1, Math.min(200, opts.limit ?? 50));
    return this.query<PipelineRunItem>({
      KeyConditionExpression: 'pipelineId = :pipelineId',
      ExpressionAttributeValues: { ':pipelineId': pipelineId },
      ScanIndexForward: false,
      Limit: limit,
    });
  }

  /** Query the status-startedAt GSI. Most recent first. */
  async listByStatus(
    status: string,
    opts: { limit?: number } = {},
  ): Promise<PipelineRunItem[]> {
    const limit = Math.max(1, Math.min(200, opts.limit ?? 50));
    return this.query<PipelineRunItem>({
      IndexName: 'status-startedAt-index',
      KeyConditionExpression: '#status = :status',
      ExpressionAttributeNames: { '#status': 'status' },
      ExpressionAttributeValues: { ':status': status },
      ScanIndexForward: false,
      Limit: limit,
    });
  }

  /** Query the triggeredBy-startedAt GSI. Most recent first. */
  async listByTriggeredBy(
    triggeredBy: string,
    opts: { limit?: number } = {},
  ): Promise<PipelineRunItem[]> {
    const limit = Math.max(1, Math.min(200, opts.limit ?? 50));
    return this.query<PipelineRunItem>({
      IndexName: 'triggeredBy-startedAt-index',
      KeyConditionExpression: 'triggeredBy = :triggeredBy',
      ExpressionAttributeValues: { ':triggeredBy': triggeredBy },
      ScanIndexForward: false,
      Limit: limit,
    });
  }

  /** Remove a run. */
  async delete(pipelineId: string, runId: string): Promise<void> {
    return this.deleteItem({ pipelineId, runId });
  }
}
