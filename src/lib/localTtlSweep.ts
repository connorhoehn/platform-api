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

export async function sweepExpiredProcessedRows(opts: {
  docClient: Pick<DynamoDBDocumentClient, 'send'>;
  tableName: string;
  keyAttr: string;
  status?: string;
  nowSec?: number;
  /** Stop after this many deletes; the next pass continues. */
  maxDeletes?: number;
}): Promise<LocalTtlSweepResult> {
  const status = opts.status ?? 'PROCESSED';
  const nowSec = opts.nowSec ?? Math.floor(Date.now() / 1000);
  const maxDeletes = opts.maxDeletes ?? 2000;
  const out: LocalTtlSweepResult = { scanned: 0, expired: 0, deleted: 0, skipped: 0 };
  const cond = {
    ExpressionAttributeNames: { '#status': 'status', '#ttl': 'ttl' },
    ExpressionAttributeValues: { ':status': status, ':now': nowSec },
  };
  let cursor: Record<string, unknown> | undefined;
  do {
    const page = await opts.docClient.send(new ScanCommand({
      TableName: opts.tableName,
      FilterExpression: '#status = :status AND #ttl < :now',
      ProjectionExpression: '#k',
      ...cond,
      ExpressionAttributeNames: { ...cond.ExpressionAttributeNames, '#k': opts.keyAttr },
      ExclusiveStartKey: cursor,
    }));
    out.scanned += page.ScannedCount ?? 0;
    for (const item of page.Items ?? []) {
      if (out.deleted >= maxDeletes) return out;
      out.expired += 1;
      try {
        await opts.docClient.send(new DeleteCommand({
          TableName: opts.tableName,
          Key: { [opts.keyAttr]: item[opts.keyAttr] },
          ConditionExpression: '#status = :status AND #ttl < :now',
          ...cond,
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
