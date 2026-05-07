// platform-api/src/ingest/repositories/IngestAuditRepository.ts
//
// Persistence for the ingest audit log (hub#2213 — foundation slice for
// the audit-viewer feature). Every mapping decision, source change, and
// write-back attempt lands here via `appendAudit`. Read-side surfaces
// (route + page) are slice 2/3 (hub#2214); the call-site instrumentation
// is slice 3/3 (hub#2215).
//
// Key schema:
//   PK: auditId (HASH)
// GSIs:
//   targetId-createdAt-index — list every audit row for a target (source /
//                              item / document) in chronological order
//   actor-createdAt-index    — list every action by a given operator /
//                              system actor
//
// The viewer page's time-range + event-type filters scan with a filter
// expression for now; once the row count grows past dev-scale we'll add
// a dedicated GSI keyed on createdAt.

import { DynamoDBDocumentClient, ScanCommand } from '@aws-sdk/lib-dynamodb';
import { BaseRepository } from '../../repositories/BaseRepository';
import { tableName } from '../../lib/ddb-table-name';
import type { IngestAuditEntry, IngestAuditEventType } from '../types';

export interface ListAuditOptions {
  /** Cap; default 100, max 500. */
  limit?: number;
  /** Inclusive lower bound (ISO timestamp). */
  since?: string;
  /** Exclusive upper bound (ISO timestamp). */
  until?: string;
  /** Optional event-type filter. */
  eventType?: IngestAuditEventType;
}

export class IngestAuditRepository extends BaseRepository {
  constructor(docClient: DynamoDBDocumentClient) {
    super(tableName('ingest-audit'), docClient);
  }

  async put(entry: IngestAuditEntry): Promise<void> {
    return super.putItem(entry as unknown as Record<string, unknown>);
  }

  async getById(auditId: string): Promise<IngestAuditEntry | null> {
    return this.getItem<IngestAuditEntry>({ auditId });
  }

  async listByTarget(targetId: string, limit = 100): Promise<IngestAuditEntry[]> {
    return this.query<IngestAuditEntry>({
      IndexName: 'targetId-createdAt-index',
      KeyConditionExpression: 'targetId = :targetId',
      ExpressionAttributeValues: { ':targetId': targetId },
      ScanIndexForward: false,
      Limit: Math.min(limit, 500),
    });
  }

  async listByActor(actor: string, limit = 100): Promise<IngestAuditEntry[]> {
    return this.query<IngestAuditEntry>({
      IndexName: 'actor-createdAt-index',
      KeyConditionExpression: 'actor = :actor',
      ExpressionAttributeValues: { ':actor': actor },
      ScanIndexForward: false,
      Limit: Math.min(limit, 500),
    });
  }

  /**
   * Scan-based listing for the cross-source audit-viewer page. Filters
   * applied via FilterExpression — fine at v1 dev scale (≤ a few thousand
   * rows). Production scale would add a createdAt-keyed GSI.
   */
  async listAll(opts: ListAuditOptions = {}): Promise<IngestAuditEntry[]> {
    const filterParts: string[] = [];
    const names: Record<string, string> = {};
    const values: Record<string, unknown> = {};

    if (opts.since) {
      filterParts.push('createdAt >= :since');
      values[':since'] = opts.since;
    }
    if (opts.until) {
      filterParts.push('createdAt < :until');
      values[':until'] = opts.until;
    }
    if (opts.eventType) {
      filterParts.push('#eventType = :eventType');
      names['#eventType'] = 'eventType';
      values[':eventType'] = opts.eventType;
    }

    const result = await this.docClient.send(
      new ScanCommand({
        TableName: this.tableName,
        ...(filterParts.length > 0
          ? {
              FilterExpression: filterParts.join(' AND '),
              ...(Object.keys(names).length > 0 ? { ExpressionAttributeNames: names } : {}),
              ExpressionAttributeValues: values,
            }
          : {}),
        Limit: Math.min(opts.limit ?? 100, 500),
      }),
    );
    const rows = (result.Items ?? []) as IngestAuditEntry[];
    // Stable ordering — newest first. Scan returns un-ordered rows.
    return rows.sort((a, b) => b.createdAt.localeCompare(a.createdAt));
  }
}
