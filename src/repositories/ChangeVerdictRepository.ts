/**
 * ChangeVerdictRepository — the cache behind `pass / concern / fail`.
 *
 * Table: `document-change-verdicts`
 *   - PK: `changeKey` (S) = `${documentId}#${itemId}` — WHICH change.
 *   - SK: `forRevision` (S) = the hash of that change's content
 *     (`computeChangeRevision`, lib/changeVerdict.ts) — WHICH VERSION of it.
 *   - Attributes: verdict, headline, reasoning, producedAt, contextDigest,
 *     model, ttl (30-day epoch seconds).
 *
 * The content hash is half the primary key, and that is the whole invalidation
 * story: a verdict is addressable only by the exact text it was computed
 * against, so editing a change does not make its old verdict wrong — it makes
 * it UNREACHABLE. Nothing has to remember to clear anything.
 *
 * A MISS IS ORDINARY, NOT EXCEPTIONAL
 *
 * The local DDB substrate is ephemeral: it runs with no volume, and every pod
 * restart wipes every table. So this repository treats "the table is not there"
 * as a normal Tuesday — it creates it and carries on — and treats any other read
 * failure as a miss rather than an error. A cache that can fail a request is
 * worse than no cache, because the expensive path (asking the model) still
 * works perfectly well without it.
 *
 * Table creation lives here rather than in the shared provisioning script
 * because that script is in another repository, and a cache table that only
 * exists if someone remembered to run a script in a different repo would spend
 * most of its life not existing.
 */

import {
  DynamoDBDocumentClient,
  GetCommand,
  QueryCommand,
} from '@aws-sdk/lib-dynamodb';
import {
  CreateTableCommand,
  DescribeTableCommand,
  ResourceNotFoundException,
  ResourceInUseException,
  UpdateTimeToLiveCommand,
} from '@aws-sdk/client-dynamodb';
import { BaseRepository } from './BaseRepository';
import { ddbClient } from '../lib/aws-clients';
import { tableName } from '../lib/ddb-table-name';
import { describeError } from '../lib/changeVerdict';
import type { ChangeReviewVerdict, ChangeVerdict } from '../types/changeVerdict';
import log from '../lib/logger';

export const CHANGE_VERDICTS_TABLE = tableName('document-change-verdicts');

const RETENTION_DAYS = 30;

export interface ChangeVerdictRow {
  /** `${documentId}#${itemId}` */
  changeKey: string;
  /** Content hash of the change. Sort key. */
  forRevision: string;
  documentId: string;
  itemId: string;
  verdict: ChangeVerdict;
  headline: string;
  reasoning?: string;
  producedAt: string;
  /**
   * Hash of the document context the verdict was reasoned from.
   *
   * Not part of the key — the key is the change, because that is what the UI
   * reasons about. But a row whose context digest no longer matches is
   * recomputed instead of served: "does this contradict the document" is only
   * answerable against the document it was answered for.
   */
  contextDigest: string;
  model: string;
  ttl: number;
}

export function changeKey(documentId: string, itemId: string): string {
  return `${documentId}#${itemId}`;
}

/**
 * "That table is not here."
 *
 * Not just `instanceof`: the local substrate is a DynamoDB-compatible
 * reimplementation, not DynamoDB, and an error it returns by name rather than
 * in the shape the SDK models would slip past a bare instanceof check — and
 * then the table would never be created, so every write would fail and every
 * read would miss, forever, while the endpoint still looked healthy.
 */
function isMissingTable(err: unknown): boolean {
  return (
    err instanceof ResourceNotFoundException ||
    (err as { name?: string } | null)?.name === 'ResourceNotFoundException'
  );
}

/** Strip the storage columns back down to the shared contract. */
export function toVerdict(row: ChangeVerdictRow): ChangeReviewVerdict {
  return {
    verdict: row.verdict,
    headline: row.headline,
    ...(row.reasoning ? { reasoning: row.reasoning } : {}),
    producedAt: row.producedAt,
    forRevision: row.forRevision,
  };
}

export class ChangeVerdictRepository extends BaseRepository {
  /** Set once the table is known to exist, so the describe happens at most once. */
  private ensured = false;

  constructor(docClient: DynamoDBDocumentClient) {
    super(CHANGE_VERDICTS_TABLE, docClient);
  }

  /**
   * Create the table if it is not there. Idempotent, and safe to lose a race
   * with another replica doing the same thing (ResourceInUse means it won).
   */
  async ensureTable(): Promise<void> {
    if (this.ensured) return;
    try {
      await ddbClient.send(new DescribeTableCommand({ TableName: CHANGE_VERDICTS_TABLE }));
      this.ensured = true;
      return;
    } catch (err) {
      if (!isMissingTable(err)) {
        log.warn(
          { event: 'change.verdict.cache.describe_failed', table: CHANGE_VERDICTS_TABLE, err: describeError(err) },
          'could not describe the verdict cache table; continuing without cache',
        );
        return;
      }
    }

    try {
      await ddbClient.send(
        new CreateTableCommand({
          TableName: CHANGE_VERDICTS_TABLE,
          AttributeDefinitions: [
            { AttributeName: 'changeKey', AttributeType: 'S' },
            { AttributeName: 'forRevision', AttributeType: 'S' },
          ],
          KeySchema: [
            { AttributeName: 'changeKey', KeyType: 'HASH' },
            { AttributeName: 'forRevision', KeyType: 'RANGE' },
          ],
          BillingMode: 'PAY_PER_REQUEST',
        }),
      );
      log.info({ table: CHANGE_VERDICTS_TABLE }, 'created the change-verdict cache table');
    } catch (err) {
      if (!(err instanceof ResourceInUseException)) {
        log.warn(
          { event: 'change.verdict.cache.create_failed', table: CHANGE_VERDICTS_TABLE, err: describeError(err) },
          'could not create the verdict cache table; continuing without cache',
        );
        return;
      }
    }

    // Best-effort: the local substrate does not implement TTL, and rows here
    // are small and capped by the 30-day `ttl` attribute they already carry.
    // Failing to enable it must not stop a verdict being served.
    try {
      await ddbClient.send(
        new UpdateTimeToLiveCommand({
          TableName: CHANGE_VERDICTS_TABLE,
          TimeToLiveSpecification: { Enabled: true, AttributeName: 'ttl' },
        }),
      );
    } catch (err) {
      log.debug(
        { table: CHANGE_VERDICTS_TABLE, err: describeError(err) },
        'TTL not enabled on the verdict cache table (substrate may not support it)',
      );
    }
    this.ensured = true;
  }

  /**
   * The verdict for this exact revision, or null.
   *
   * Never throws. Every failure here is reported as a miss, because the caller's
   * fallback for a miss — ask the model — is correct in every one of those cases.
   */
  async get(documentId: string, itemId: string, forRevision: string): Promise<ChangeVerdictRow | null> {
    try {
      const out = await this.docClient.send(
        new GetCommand({
          TableName: CHANGE_VERDICTS_TABLE,
          Key: { changeKey: changeKey(documentId, itemId), forRevision },
        }),
      );
      return (out.Item as ChangeVerdictRow | undefined) ?? null;
    } catch (err) {
      if (isMissingTable(err)) {
        await this.ensureTable();
        return null;
      }
      log.warn(
        { event: 'change.verdict.cache.read_failed', documentId, itemId, err: describeError(err) },
        'verdict cache read failed; treating as a miss',
      );
      return null;
    }
  }

  /** Every verdict held for one change, newest first. Never throws. */
  async listForChange(documentId: string, itemId: string): Promise<ChangeVerdictRow[]> {
    try {
      const out = await this.docClient.send(
        new QueryCommand({
          TableName: CHANGE_VERDICTS_TABLE,
          KeyConditionExpression: 'changeKey = :k',
          ExpressionAttributeValues: { ':k': changeKey(documentId, itemId) },
          Limit: 25,
        }),
      );
      const rows = (out.Items ?? []) as ChangeVerdictRow[];
      // The sort key is a hash, so DDB's ordering carries no meaning. Order by
      // the only thing that does.
      return rows.sort((a, b) => (a.producedAt < b.producedAt ? 1 : -1));
    } catch (err) {
      if (isMissingTable(err)) {
        await this.ensureTable();
        return [];
      }
      log.warn(
        { event: 'change.verdict.cache.query_failed', documentId, itemId, err: describeError(err) },
        'verdict cache query failed; treating as empty',
      );
      return [];
    }
  }

  /**
   * Store a verdict. Never throws: a verdict that was produced but not cached is
   * still a perfectly good verdict, and failing the request would throw away the
   * model call we just paid for.
   */
  async put(row: Omit<ChangeVerdictRow, 'ttl' | 'changeKey'>): Promise<void> {
    const item: ChangeVerdictRow = {
      ...row,
      changeKey: changeKey(row.documentId, row.itemId),
      ttl: Math.floor(Date.now() / 1000) + RETENTION_DAYS * 86400,
    };
    try {
      await this.putItem(item as unknown as Record<string, unknown>);
    } catch (err) {
      if (isMissingTable(err)) {
        await this.ensureTable();
        try {
          await this.putItem(item as unknown as Record<string, unknown>);
          return;
        } catch (retryErr) {
          log.warn(
            { event: 'change.verdict.cache.write_failed', ...item, err: describeError(retryErr) },
            'verdict cache write failed after creating the table; the verdict was still served',
          );
          return;
        }
      }
      log.warn(
        { event: 'change.verdict.cache.write_failed', documentId: row.documentId, itemId: row.itemId, err: describeError(err) },
        'verdict cache write failed; the verdict was still served',
      );
    }
  }
}
