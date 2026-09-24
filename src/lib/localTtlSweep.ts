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
//
// NFR #227 (Loop 37) generalises it: `sweepTtlTables` finds every table whose
// OWN TimeToLive is ENABLED, reads that attribute name from the table, and
// deletes only rows whose value is a Number below now — exactly the rows AWS's
// reaper would remove, nothing else. It runs as one ClusterSweeper task (one
// replica, under the sweep-leader lease), pages the scan small, paces deletes
// and stops the moment leadership is lost. Before it, both replicas scanned
// the whole 100k-row outbox every 10 minutes, unleased.
import { DescribeTableCommand, DescribeTimeToLiveCommand, ListTablesCommand, type DynamoDBClient } from '@aws-sdk/client-dynamodb';
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
  /** The table's TTL attribute (default `ttl`). */
  ttlAttr?: string;
  /** Count what would be deleted; delete nothing. */
  dryRun?: boolean;
  /** Rows examined per Scan page (keeps each page small in the DynamoDB-Local JVM). */
  pageLimit?: number;
  /** Pace deletes to at most this many per second (0 = unpaced). */
  deletesPerSecond?: number;
  /** Checked between pages and deletes; false stops the pass (e.g. lease lost). */
  shouldContinue?: () => boolean;
  sleep?: (ms: number) => Promise<void>;
}): Promise<LocalTtlSweepResult> {
  const nowSec = opts.nowSec ?? Math.floor(Date.now() / 1000);
  const maxDeletes = opts.maxDeletes ?? 2000;
  const gapMs = opts.deletesPerSecond && opts.deletesPerSecond > 0 ? Math.ceil(1000 / opts.deletesPerSecond) : 0;
  const sleep = opts.sleep ?? ((ms: number) => new Promise<void>((r) => setTimeout(r, ms)));
  const keepGoing = opts.shouldContinue ?? (() => true);
  const out: LocalTtlSweepResult = { scanned: 0, expired: 0, deleted: 0, skipped: 0 };
  const condition = `#ttl < :now${opts.where ? ` AND ${opts.where.expression}` : ''}`;
  const names: Record<string, string> = { '#ttl': opts.ttlAttr ?? 'ttl', ...(opts.where?.names ?? {}) };
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
      ...(opts.pageLimit ? { Limit: opts.pageLimit } : {}),
    }));
    out.scanned += page.ScannedCount ?? 0;
    for (const item of page.Items ?? []) {
      if (out.deleted >= maxDeletes || !keepGoing()) return out;
      out.expired += 1;
      if (opts.dryRun) continue;
      if (gapMs > 0 && out.deleted + out.skipped > 0) {
        await sleep(gapMs);
        if (!keepGoing()) return out;
      }
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
  } while (cursor && keepGoing());
  return out;
}

export type TtlSweepWhere = { expression: string; names: Record<string, string>; values: Record<string, unknown> };

/** Narrower rules for tables that share their TTL attribute with rows the reaper must not touch. */
export function defaultTtlSweepOverrides(tableName: (base: string) => string): Record<string, TtlSweepWhere> {
  return {
    // Only terminal delivery rows; lease rows share this table (NFR #73).
    [tableName('social-outbox')]: {
      expression: '#status IN (:processed, :failed)',
      names: { '#status': 'status' },
      values: { ':processed': 'PROCESSED', ':failed': 'FAILED' },
    },
    // Only projection revision deltas, never grants (NFR #77).
    [tableName('work-graph')]: { expression: 'begins_with(#pk, :wg)', names: { '#pk': 'PK' }, values: { ':wg': 'WG#' } },
  };
}

export interface TtlTableSweep extends LocalTtlSweepResult {
  table: string;
  ttlAttr: string;
  ms: number;
}

/**
 * Every table (under `prefix`) whose own TimeToLive is ENABLED: delete rows
 * whose TTL attribute is a Number below now, re-checked in each delete's
 * condition. A String or millisecond value never matches, as in AWS.
 */
export async function sweepTtlTables(opts: {
  ddb: Pick<DynamoDBClient, 'send'>;
  docClient: Pick<DynamoDBDocumentClient, 'send'>;
  prefix?: string;
  overrides?: Record<string, TtlSweepWhere>;
  dryRun?: boolean;
  maxDeletesPerTable?: number;
  deletesPerSecond?: number;
  pageLimit?: number;
  shouldContinue?: () => boolean;
  nowSec?: number;
  sleep?: (ms: number) => Promise<void>;
}): Promise<TtlTableSweep[]> {
  const keepGoing = opts.shouldContinue ?? (() => true);
  const names: string[] = [];
  let start: string | undefined;
  do {
    const page = await opts.ddb.send(new ListTablesCommand({ ExclusiveStartTableName: start }));
    names.push(...(page.TableNames ?? []));
    start = page.LastEvaluatedTableName;
  } while (start);
  const results: TtlTableSweep[] = [];
  for (const table of names) {
    if (!keepGoing()) break;
    if (opts.prefix && !table.startsWith(opts.prefix)) continue;
    const ttl = (await opts.ddb.send(new DescribeTimeToLiveCommand({ TableName: table }))).TimeToLiveDescription;
    if (ttl?.TimeToLiveStatus !== 'ENABLED' || !ttl.AttributeName) continue;
    const keySchema = (await opts.ddb.send(new DescribeTableCommand({ TableName: table }))).Table?.KeySchema ?? [];
    const keyAttrs = [...keySchema].sort((a) => (a.KeyType === 'HASH' ? -1 : 1)).map((k) => k.AttributeName as string);
    if (keyAttrs.length === 0) continue;
    const startedAt = Date.now();
    const swept = await sweepExpiredRows({
      docClient: opts.docClient,
      tableName: table,
      keyAttrs,
      ttlAttr: ttl.AttributeName,
      where: opts.overrides?.[table],
      dryRun: opts.dryRun,
      maxDeletes: opts.maxDeletesPerTable ?? 2000,
      deletesPerSecond: opts.deletesPerSecond ?? 50,
      pageLimit: opts.pageLimit ?? 200,
      shouldContinue: keepGoing,
      nowSec: opts.nowSec,
      sleep: opts.sleep,
    });
    results.push({ table, ttlAttr: ttl.AttributeName, ...swept, ms: Date.now() - startedAt });
  }
  return results;
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
