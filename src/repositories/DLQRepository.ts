// Pipeline dead-letter queue persistence — durable backing store for failed events.
//
// The in-memory InMemoryDeadLetterQueue remains the fast-path cache; this
// repository provides durable persistence so DLQ entries survive restarts.

import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { BaseRepository } from './BaseRepository';
import { tableName } from '../lib/ddb-table-name';

export interface DLQEntry {
  messageId: string;
  /**
   * Constant `'DLQ'` — the hash key of the `failedAt-index` GSI that `list`
   * queries. Every row must carry it or it is invisible to `list` (rows
   * written before this field existed are exactly that: present in the table,
   * never listed, never rehydrated).
   */
  partitionKey?: 'DLQ';
  eventType: string;
  eventPayload: unknown;
  lastError: string;
  failedAt: string;
  totalAttempts: number;
  /** Epoch seconds — set for auto-expiry after 30 days. */
  ttl: number;
}

const DLQ_PARTITION_KEY = 'DLQ' as const;

export class DLQRepository extends BaseRepository {
  constructor(docClient: DynamoDBDocumentClient) {
    super(tableName('pipeline-dlq'), docClient);
  }

  async create(entry: DLQEntry): Promise<void> {
    return this.putItem({ ...entry, partitionKey: DLQ_PARTITION_KEY } as unknown as Record<string, unknown>);
  }

  async get(messageId: string): Promise<DLQEntry | null> {
    return this.getItem<DLQEntry>({ messageId });
  }

  async delete(messageId: string): Promise<void> {
    return this.deleteItem({ messageId });
  }

  /** List all DLQ entries (newest first), optionally filtered by error kind. */
  async list(opts: {
    limit?: number;
    errorKind?: string;
    exclusiveStartKey?: Record<string, unknown>;
  } = {}): Promise<{ entries: DLQEntry[]; lastEvaluatedKey?: Record<string, unknown> }> {
    const limit = Math.max(1, Math.min(100, opts.limit ?? 50));

    // Use the failedAt GSI for chronological listing.
    const result = await this.queryWithPagination<DLQEntry>({
      IndexName: 'failedAt-index',
      KeyConditionExpression: 'partitionKey = :pk',
      ExpressionAttributeValues: { ':pk': DLQ_PARTITION_KEY },
      ScanIndexForward: false, // newest first
      Limit: limit,
      ExclusiveStartKey: opts.exclusiveStartKey,
    });

    // Filter by error kind in-memory if specified.
    const filtered = opts.errorKind
      ? result.items.filter((e) => e.lastError.startsWith(opts.errorKind!))
      : result.items;

    return { entries: filtered, lastEvaluatedKey: result.lastEvaluatedKey };
  }
}
