// DynamoDB Local does not enforce TTL (NFR #73). In AWS the social-outbox's
// PROCESSED rows vanish about a day after the drainer stamps their `ttl`; in
// local dev they stayed forever — 14,773 rows / 17.5 MB, 1,923 of them past
// their ttl, on 2026-09-23 — and every one is still scanned, indexed and paged
// by the DynamoDB-Local JVM that got OOM-killed twice that day.
//
// This is the dev-only stand-in for the TTL reaper, and it is deliberately
// narrow: it deletes a row only when its status is PROCESSED AND its `ttl` has
// passed, with both re-checked in the delete's own condition (a row that was
// re-stamped or changed status between scan and delete is left alone). It runs
// only when LOCAL_TTL_SWEEP=true and never under NODE_ENV=production, where
// the real TTL does this job. Counts are logged every pass.
//
// NFR #77 extends it to the work-graph projection's `WG#` rows (1,176 past
// their ttl on 2026-09-23): same rule, ttl passed re-checked per delete.
import { DeleteCommand, ScanCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';

export interface LocalTtlSweepResult {
  scanned: number;
  expired: number;
  deleted: number;
  skipped: number;
}

export function localTtlSweepEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  return env.LOCAL_TTL_SWEEP === 'true' && env.NODE_ENV !== 'production';
}

/**
 * The generic reaper: deletes rows of `tableName` whose `ttl` has passed AND
 * that match `where`, re-checking both in each delete's condition. `keyAttrs`
 * is the table's full primary key (hash, or hash + range).
 */
export async function sweepExpiredRows(opts: {
  docClient: Pick<DynamoDBDocumentClient, 'send'>;
  tableName: string;
  keyAttrs: string[];
  where?: { expression: string; names: Record<string, string>; values: Record<string, unknown> };
  nowSec?: number;
  /** Stop after this many deletes; the next pass continues. */
  maxDeletes?: number;
}): Promise<LocalTtlSweepResult> {
  const nowSec = opts.nowSec ?? Math.floor(Date.now() / 1000);
  const maxDeletes = opts.maxDeletes ?? 2000;
  const out: LocalTtlSweepResult = { scanned: 0, expired: 0, deleted: 0, skipped: 0 };
  const condition = `#ttl < :now${opts.where ? ` AND ${opts.where.expression}` : ''}`;
  const names: Record<string, string> = { '#ttl': 'ttl', ...(opts.where?.names ?? {}) };
  const values: Record<string, unknown> = { ':now': nowSec, ...(opts.where?.values ?? {}) };
  const keyNames: Record<string, string> = Object.fromEntries(opts.keyAttrs.map((k, i) => [`#k${i}`, k]));
  let cursor: Record<string, unknown> | undefined;
  do {
    const page = await opts.docClient.send(new ScanCommand({
      TableName: opts.tableName,
      FilterExpression: condition,
      ProjectionExpression: Object.keys(keyNames).join(', '),
      ExpressionAttributeNames: { ...names, ...keyNames },
      ExpressionAttributeValues: values,
      ExclusiveStartKey: cursor,
    }));
    out.scanned += page.ScannedCount ?? 0;
    for (const item of page.Items ?? []) {
      if (out.deleted >= maxDeletes) return out;
      out.expired += 1;
      try {
        await opts.docClient.send(new DeleteCommand({
          TableName: opts.tableName,
          Key: Object.fromEntries(opts.keyAttrs.map((k) => [k, item[k]])),
          ConditionExpression: condition,
          ExpressionAttributeNames: names,
          ExpressionAttributeValues: values,
        }));
        out.deleted += 1;
      } catch (err: unknown) {
        if ((err as { name?: string })?.name !== 'ConditionalCheckFailedException') throw err;
        out.skipped += 1;
      }
    }
    cursor = page.LastEvaluatedKey as Record<string, unknown> | undefined;
  } while (cursor);
  return out;
}

/** social-outbox: PROCESSED rows past their ttl (NFR #73). */
export async function sweepExpiredProcessedRows(opts: {
  docClient: Pick<DynamoDBDocumentClient, 'send'>;
  tableName: string;
  keyAttr: string;
  status?: string;
  nowSec?: number;
  maxDeletes?: number;
}): Promise<LocalTtlSweepResult> {
  return sweepExpiredRows({
    docClient: opts.docClient,
    tableName: opts.tableName,
    keyAttrs: [opts.keyAttr],
    where: { expression: '#status = :status', names: { '#status': 'status' }, values: { ':status': opts.status ?? 'PROCESSED' } },
    nowSec: opts.nowSec,
    maxDeletes: opts.maxDeletes,
  });
}

/**
 * work-graph: the projection's `WG#` rows (revision deltas) past their ttl
 * (NFR #77). Grants (`WGGRANT#`) are not touched here.
 */
export async function sweepExpiredWorkGraphRows(opts: {
  docClient: Pick<DynamoDBDocumentClient, 'send'>;
  tableName: string;
  nowSec?: number;
  maxDeletes?: number;
}): Promise<LocalTtlSweepResult> {
  return sweepExpiredRows({
    docClient: opts.docClient,
    tableName: opts.tableName,
    keyAttrs: ['PK', 'SK'],
    where: { expression: 'begins_with(#pk, :wg)', names: { '#pk': 'PK' }, values: { ':wg': 'WG#' } },
    nowSec: opts.nowSec,
    maxDeletes: opts.maxDeletes,
  });
}
