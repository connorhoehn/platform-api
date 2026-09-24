import {
  DescribeTimeToLiveCommand,
  UpdateTimeToLiveCommand,
} from '@aws-sdk/client-dynamodb';
import { ddbClient } from './aws-clients';
import { tableName } from './ddb-table-name';
import { ensureOutboxPendingIndex } from './outbox-pending';
import log from './logger';

const OUTBOX_TABLE = tableName('social-outbox');
const VIDEO_SESSIONS_TABLE = tableName('document-video-sessions');
const PIPELINE_RUNS_TABLE = tableName('pipeline-runs');
const PIPELINE_AUDIT_TABLE = tableName('pipeline-audit');
const PIPELINE_DLQ_TABLE = tableName('pipeline-dlq');

/**
 * Idempotent: enable the DDB native TTL on a table if it isn't already.
 * Used by the bootstrap helpers below. Extracted so adding more tables is
 * a one-liner.
 */
async function ensureTableTTL(tableNameStr: string, attributeName = 'ttl'): Promise<void> {
  const desc = await ddbClient.send(
    new DescribeTimeToLiveCommand({ TableName: tableNameStr }),
  );
  const status = desc.TimeToLiveDescription?.TimeToLiveStatus;
  if (status === 'ENABLED' || status === 'ENABLING') return;

  await ddbClient.send(
    new UpdateTimeToLiveCommand({
      TableName: tableNameStr,
      TimeToLiveSpecification: {
        Enabled: true,
        AttributeName: attributeName,
      },
    }),
  );
  log.info({ table: tableNameStr, attributeName }, 'TTL enabled on table');
}

export async function ensureOutboxTTL(): Promise<void> {
  await ensureTableTTL(OUTBOX_TABLE);
  // The drainer's pending-index (NFR #34c) — additive UpdateTable only; the
  // drainer walks the table until the index is ACTIVE, so this never gates boot.
  const index = await ensureOutboxPendingIndex(ddbClient, OUTBOX_TABLE);
  if (index === 'created') log.info({ table: OUTBOX_TABLE }, 'outbox pending-index created');
}

/**
 * Enable TTL on the document-video-sessions table. Called at startup so
 * `endSession()` writes get garbage-collected after the grace window
 * (default 7 days; see VIDEO_SESSION_TTL_DAYS). Safe to call against a
 * table that already has TTL enabled.
 */
export async function ensureVideoSessionsTTL(): Promise<void> {
  await ensureTableTTL(VIDEO_SESSIONS_TABLE);
}

/**
 * Enable TTL on the pipeline-runs table.
 *
 * Runs grew without bound because BOTH halves of expiry were missing: this
 * table was never registered here, and `PipelineRunRepository` never wrote the
 * `ttl` attribute it declared. Each gap independently defeats the other, so
 * fixing one alone changes nothing — which is why the rows stacked up
 * unnoticed for so long.
 *
 * Only FINISHED runs carry a ttl (see PipelineRunRepository.put), so enabling
 * TTL here cannot expire a run that is still pending, running, or interrupted
 * and awaiting resume.
 */
export async function ensurePipelineRunsTTL(): Promise<void> {
  await ensureTableTTL(PIPELINE_RUNS_TABLE);
}

/**
 * realtime-examples NFR #226 (Loop 38): pipeline-audit rows carry `ttlEpoch`
 * (+30 d) and pipeline-dlq rows `ttl` (+30 d), but neither table enabled TTL,
 * so in AWS nothing reaped them and in dev only a per-table sweep did. Both
 * are pure expiry stamps (an audit trail and failed-run dead letters), never
 * a "last active" field, so enabling TTL removes exactly what the writers
 * already meant to drop. It also brings both under the local TTL sweep.
 */
export async function ensurePipelineAuditTTL(): Promise<void> {
  await ensureTableTTL(PIPELINE_AUDIT_TABLE, 'ttlEpoch');
}

export async function ensurePipelineDlqTTL(): Promise<void> {
  await ensureTableTTL(PIPELINE_DLQ_TABLE);
}
