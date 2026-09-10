// Hub task #197 — persist pipeline run history to DynamoDB.
//
// Queryable index of pipeline run history. The Raft state machine + WAL remain
// the source of truth for replay; this repository backs the
// `/api/pipelines/:pipelineId/runs` operator-facing surface.

import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import type { PipelineRunStatus, PipelineStepStatus } from '@connorhoehn/pipeline-runtime';
import { isTerminalRunStatus } from '@connorhoehn/pipeline-runtime';
import { BaseRepository } from './BaseRepository';
import { tableName } from '../lib/ddb-table-name';

/**
 * How long a FINISHED run stays queryable, in days.
 *
 * Runs accumulated without bound until 2026-08: the `ttl` field below was
 * declared but never written, and `pipeline-runs` was never registered with
 * `ensureTableTTL`. Either gap alone is enough to keep every row forever, so
 * both had to be closed — a ttl value on a table with TTL disabled expires
 * nothing, and an enabled table expires nothing if no row carries a ttl.
 */
const DEFAULT_RUN_RETENTION_DAYS = 30;

function runRetentionDays(): number {
  const raw = Number(process.env['PIPELINE_RUN_RETENTION_DAYS']);
  return Number.isFinite(raw) && raw > 0 ? raw : DEFAULT_RUN_RETENTION_DAYS;
}

/**
 * Platform-api run status as it may be **read back** from the table.
 *
 * Canonical `PipelineRunStatus`, plus one platform-api-specific lifecycle
 * state and the two legacy aliases that survive on historical rows:
 *   - `interrupted` — orphan-scan transition: `running` with no heartbeat after restart
 *   - `succeeded`   — legacy alias for `completed`, written before the vocabulary fix
 *   - `canceled`    — legacy alias for `cancelled` (US spelling), same vintage
 *
 * The aliases are READ-ONLY. `runPersistence` writes canonical spellings, and
 * every read boundary that serves this value to a client must fold it through
 * `normalizeRunStatus` — otherwise the runs list and runs detail endpoints
 * disagree for the same run, which is exactly the bug this shape once caused.
 * Do not write `succeeded`/`canceled` from new code.
 */
export type RunItemStatus = PipelineRunStatus | 'interrupted' | 'succeeded' | 'canceled';

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
  status?: PipelineStepStatus;
  startedAt?: string;
  completedAt?: string;
  durationMs?: number;
  output?: unknown;
  error?: string;
  attempts?: StepAttempt[];
}

/**
 * DC-25 durable resume: the latest executor checkpoint, persisted from
 * `pipeline:run:checkpoint` events onto the run row so it survives pod
 * replacement (the file WAL lives on the pod's ephemeral fs and does not).
 *
 * `steps` records are SLIMMED before persistence (no input/output/llm/agent
 * payloads — outputs already live in `context.steps` and in `stepsSummary`),
 * which keeps the row bounded. When even the slimmed checkpoint exceeds the
 * size budget, the largest `context.steps` entries are replaced with
 * `{truncated:true,size}` markers and listed in `droppedContextStepKeys`.
 */
export interface PersistedCheckpoint {
  sequenceNumber: number;
  at: string;
  context: Record<string, unknown>;
  currentStepIds: string[];
  steps?: Record<string, unknown>;
  joinStates?: Record<string, unknown[]>;
  firedJoins?: string[];
  completedStepCount?: number;
  /** context.steps keys whose values were dropped by size compaction. */
  droppedContextStepKeys?: string[];
}

export interface PipelineRunItem {
  pipelineId: string;
  runId: string;
  /** Canonical run status. `interrupted` and `stuck` are platform-api lifecycle extensions. */
  status: RunItemStatus;
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
  /**
   * Set when `status === 'rejected'` — who rejected the run's approval gate,
   * when, and why. Persisted so the runs LIST can explain a rejection without
   * a second lookup into the (possibly evicted) live executor.
   */
  rejection?: { stepId: string; userId: string; comment?: string; at: string };
  /** Per-model token usage accumulated from pipeline:llm:response events. */
  tokenUsage?: Record<string, TokenUsageByModel>;
  /** Estimated total cost in USD based on the pricing table. */
  estimatedCostUsd?: number;
  /** Epoch seconds — set by application for auto-expiry of old runs. */
  ttl?: number;
  /**
   * DC-25 durable resume: latest executor checkpoint (see PersistedCheckpoint).
   * Written by RunPersistenceService from `pipeline:run:checkpoint` events.
   * Its presence is what makes an `interrupted` run resumable at boot.
   */
  checkpoint?: PersistedCheckpoint;
  /**
   * The exact PipelineDefinition graph this run executes — captured from the
   * FIRST checkpoint event (the executor includes it at sequenceNumber 1).
   * Authoritative over the definitions repo for resume: the live definition
   * may have been edited since the run started.
   */
  definitionSnapshot?: unknown;
  /** ISO timestamp of the most recent boot-time resume of this run. */
  resumedAt?: string;
  /** Number of times this run has been resumed after an interruption. */
  resumeCount?: number;
  /** Node id that claimed the interrupted run for resume (double-resume guard). */
  resumeClaimedBy?: string;
  /**
   * DLQ redrive lineage. `redriveOf` names the failed run this one replaces
   * (lifted from the trigger payload the redrive injects); `redrivenBy` on
   * the failed run lists every run started to replace it, oldest first.
   */
  redriveOf?: string;
  redrivenBy?: string[];
  /**
   * A person's verdict on what this run SUGGESTED in a document (tracked
   * changes under `suggestionKey`). Written by POST /runs/:runId/review after
   * the run is terminal; the run's status does not change — the edit landed as
   * a proposal, and this is what became of it.
   */
  review?: RunReview;
}

export interface RunReview {
  decision: 'accept' | 'reject';
  /** `${user}@${date}` of the suggestion marks the run wrote. */
  suggestionKey: string;
  documentId: string;
  by: string;
  byName?: string;
  at: string;
  comment?: string;
  /** Set when the decision was applied to the document server-side, by this run. */
  appliedByRunId?: string;
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

/**
 * One image artifact produced during a run, with optional parent linkage.
 * Emitted by getRunImageLineage() for every step whose output carries an
 * imageId. Source images (image-source nodes) have parentImageId undefined.
 */
export interface ImageLineageEntry {
  imageId: string;
  producedByStepId: string;
  producedByNodeKind: string;
  parentImageId?: string;
  width: number;
  height: number;
  mimeType: string;
  bbox?: { x: number; y: number; width: number; height: number };
  at: string;
}

// Image-producing node kinds. Keyed on the node data.type / handler kind values
// used by the image step handlers in platform-api/src/pipeline/image/.
const IMAGE_PRODUCING_KINDS = new Set([
  'image-source',
  'image:crop',
  'image:adjust-bw',
  'image:adjust-layer',
  'image:save',
  'image:resize',
  'image:transform',
  'image:format-convert',
  'image:sharpen',
  'image:watermark',
  'image:generate',
  'image:variation',
  'image:edit-prompt',
  'image:vision-analyze',
  'image:vision-classify',
  'image:vision-describe',
  'image:vision-tag',
  'image:vision-extract-text',
]);

/**
 * Infer the node kind from a step key. Step keys in stepsSummary are the
 * nodeId, optionally suffixed with `:2`, `:3`, … when the same node executed
 * multiple times (fork arms). Strip the numeric suffix to get the raw nodeId,
 * then look up the nodeKind from the step output when available.
 */
function inferNodeKind(stepOutput: unknown): string {
  if (!stepOutput || typeof stepOutput !== 'object') return 'unknown';
  const o = stepOutput as Record<string, unknown>;
  // Step handlers store their node kind in the output for lineage consumers.
  // Fall back to 'unknown' if the output doesn't carry it.
  if (typeof o['nodeKind'] === 'string') return o['nodeKind'];
  // Detect image-source by the absence of parentImageId (it never transforms).
  if (o['imageId'] && !o['parentImageId']) return 'image-source';
  // Detect crop by the presence of bbox.
  if (o['imageId'] && o['bbox']) return 'image:crop';
  // Generic image-producing node.
  if (o['imageId']) return 'image';
  return 'unknown';
}

export class PipelineRunRepository extends BaseRepository {
  constructor(docClient: DynamoDBDocumentClient) {
    super(tableName('pipeline-runs'), docClient);
  }

  /**
   * Upsert a pipeline run, stamping an expiry once the run is FINISHED.
   *
   * Only terminal runs get a ttl. A `pending`, `running` or — critically —
   * `interrupted` run must never expire: `resumeInterruptedRuns` reads exactly
   * those rows at boot, so expiring one would silently destroy work that was
   * going to be resumed. `isTerminalRunStatus` is the shared predicate rather
   * than a local list, so this cannot drift from the canonical vocabulary the
   * way the `succeeded`/`canceled` aliases once did (it folds those in).
   *
   * An explicit ttl already on the item wins, so a caller can pin a specific
   * expiry without fighting this default.
   */
  async put(item: PipelineRunItem): Promise<void> {
    const stamped =
      item.ttl === undefined && isTerminalRunStatus(item.status)
        ? { ...item, ttl: Math.floor(Date.now() / 1000) + runRetentionDays() * 86_400 }
        : item;
    return this.putItem(stamped as unknown as Record<string, unknown>);
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

  /**
   * DC-25: idempotent claim of an `interrupted` run for boot-time resume.
   *
   * A conditional update — `status` must still be `interrupted` — so two
   * processes racing to resume the same run cannot both win: the loser's
   * write fails the condition and this returns `null`. The winner's row
   * transitions to `running` with `resumeClaimedBy`/`resumedAt` stamped and
   * `resumeCount` incremented (a second crash re-interrupts it and the next
   * boot can claim it again).
   *
   * Returns the post-claim item on success, `null` when the claim was lost
   * (already claimed, or the run reached a terminal state meanwhile).
   */
  async claimForResume(
    pipelineId: string,
    runId: string,
    nodeId: string,
    at: string = new Date().toISOString(),
  ): Promise<PipelineRunItem | null> {
    try {
      const attributes = await this.updateItem({
        Key: { pipelineId, runId },
        UpdateExpression:
          'SET #status = :running, resumeClaimedBy = :nodeId, resumedAt = :at, ' +
          'resumeCount = if_not_exists(resumeCount, :zero) + :one',
        ConditionExpression: '#status = :interrupted',
        ExpressionAttributeNames: { '#status': 'status' },
        ExpressionAttributeValues: {
          ':running': 'running',
          ':interrupted': 'interrupted',
          ':nodeId': nodeId,
          ':at': at,
          ':zero': 0,
          ':one': 1,
        },
        ReturnValues: 'ALL_NEW',
      });
      return (attributes as unknown as PipelineRunItem) ?? null;
    } catch (err) {
      if ((err as { name?: string }).name === 'ConditionalCheckFailedException') {
        return null;
      }
      throw err;
    }
  }

  /**
   * Record that `newRunId` was started to replace the run `(pipelineId, runId)`.
   * A `list_append` update so two redrives racing on the same run both land,
   * and a full-row put from run persistence cannot have wiped an earlier
   * one — the original run is terminal by the time anyone redrives it, so
   * persistence has already flushed and forgotten it. Conditional on the row
   * existing: a redrive of a run this table never saw must not create a
   * phantom row with no status. Returns false when the row was missing.
   */
  async appendRedrivenBy(pipelineId: string, runId: string, newRunId: string): Promise<boolean> {
    try {
      await this.updateItem({
        Key: { pipelineId, runId },
        UpdateExpression: 'SET redrivenBy = list_append(if_not_exists(redrivenBy, :empty), :new)',
        ConditionExpression: 'attribute_exists(runId)',
        ExpressionAttributeValues: { ':empty': [], ':new': [newRunId] },
      });
      return true;
    } catch (err) {
      if ((err as { name?: string }).name === 'ConditionalCheckFailedException') return false;
      throw err;
    }
  }

  /** Record a person's verdict on the run's suggestions. False when the run row does not exist. */
  async setReview(pipelineId: string, runId: string, review: RunReview): Promise<boolean> {
    try {
      await this.updateItem({
        Key: { pipelineId, runId },
        UpdateExpression: 'SET review = :review',
        ConditionExpression: 'attribute_exists(runId)',
        ExpressionAttributeValues: { ':review': review },
      });
      return true;
    } catch (err) {
      if ((err as { name?: string }).name === 'ConditionalCheckFailedException') return false;
      throw err;
    }
  }

  /**
   * Walk all steps in a stored run and emit one ImageLineageEntry per image
   * artifact produced. Image-source nodes get parentImageId undefined; all
   * other image-producing steps carry the parent imageId from their output.
   * Steps with multi-image output (images[]) emit one entry per image.
   *
   * Returns [] when the run has no stepsSummary or no image-producing steps.
   * Returns null when the run does not exist.
   */
  async getRunImageLineage(
    pipelineId: string,
    runId: string,
  ): Promise<ImageLineageEntry[] | null> {
    const run = await this.get(pipelineId, runId);
    if (!run) return null;

    const summary = run.stepsSummary ?? {};
    const completedAt = run.completedAt ?? run.startedAt ?? new Date(0).toISOString();
    const entries: ImageLineageEntry[] = [];

    for (const [rawStepKey, step] of Object.entries(summary)) {
      if (!step.output) continue;
      const output = step.output as Record<string, unknown>;
      const at = step.completedAt ?? completedAt;

      // Strip fork suffix (e.g. "node-crop:2" → "node-crop")
      const stepId = rawStepKey.replace(/:\d+$/, '');

      // Determine producing node kind from the step output.
      const nodeKind = inferNodeKind(output);

      // Check if this step is an image producer.
      // A step is an image producer when its output has an imageId field
      // that resolves to a non-empty string.
      const topImageId = typeof output['imageId'] === 'string' && output['imageId']
        ? output['imageId']
        : null;

      // Multi-image steps (e.g. scatter arms) can emit images[] array
      const imagesArr = Array.isArray(output['images'])
        ? (output['images'] as unknown[])
        : null;

      if (topImageId) {
        // Single-image output shape: { imageId, parentImageId?, width, height, mimeType, bbox? }
        const entry: ImageLineageEntry = {
          imageId: topImageId,
          producedByStepId: stepId,
          producedByNodeKind: nodeKind,
          parentImageId:
            typeof output['parentImageId'] === 'string' && output['parentImageId']
              ? output['parentImageId']
              : undefined,
          width: typeof output['width'] === 'number' ? output['width'] : 0,
          height: typeof output['height'] === 'number' ? output['height'] : 0,
          mimeType: typeof output['mimeType'] === 'string' ? output['mimeType'] : 'image/jpeg',
          bbox: isBBox(output['bbox']) ? (output['bbox'] as ImageLineageEntry['bbox']) : undefined,
          at,
        };
        entries.push(entry);
      } else if (imagesArr) {
        // Multi-image shape from scatter-arm steps: output.images[].imageId
        for (const img of imagesArr) {
          if (!img || typeof img !== 'object') continue;
          const imgObj = img as Record<string, unknown>;
          const imgId = typeof imgObj['imageId'] === 'string' ? imgObj['imageId'] : null;
          if (!imgId) continue;
          entries.push({
            imageId: imgId,
            producedByStepId: stepId,
            producedByNodeKind: nodeKind,
            parentImageId:
              typeof imgObj['parentImageId'] === 'string' && imgObj['parentImageId']
                ? imgObj['parentImageId']
                : undefined,
            width: typeof imgObj['width'] === 'number' ? imgObj['width'] : 0,
            height: typeof imgObj['height'] === 'number' ? imgObj['height'] : 0,
            mimeType: typeof imgObj['mimeType'] === 'string' ? imgObj['mimeType'] : 'image/jpeg',
            bbox: isBBox(imgObj['bbox']) ? (imgObj['bbox'] as ImageLineageEntry['bbox']) : undefined,
            at,
          });
        }
      }
    }

    return entries;
  }
}

function isBBox(v: unknown): boolean {
  if (!v || typeof v !== 'object') return false;
  const b = v as Record<string, unknown>;
  return (
    typeof b['x'] === 'number' &&
    typeof b['y'] === 'number' &&
    typeof b['width'] === 'number' &&
    typeof b['height'] === 'number'
  );
}
