// Hub task #199 — DDB-backed persistence for pipeline dead-letter queue entries.
//
// Failed EventBus events land here for inspection, retry, or discard.
// The `failedAt-index` GSI (PK: status, SK: failedAt) enables efficient
// listing of pending entries in reverse-chronological order.

import { DynamoDBDocumentClient, ScanCommand } from '@aws-sdk/lib-dynamodb';
import { BaseRepository } from './BaseRepository';
import { tableName } from '../lib/ddb-table-name';

export interface PipelineDLQItem {
  messageId: string;
  topic: string;
  payload: Record<string, unknown>;
  error: string;
  status: string; // 'pending' | 'retried' | 'discarded'
  failedAt: string; // ISO timestamp
  retriedAt?: string; // ISO timestamp, set on retry
  retryCount: number;
  ttl?: number; // epoch seconds
}

export class PipelineDLQRepository extends BaseRepository {
  constructor(docClient: DynamoDBDocumentClient) {
    super(tableName('pipeline-dlq'), docClient);
  }

  async put(item: PipelineDLQItem): Promise<void> {
    return this.putItem(item as unknown as Record<string, unknown>);
  }

  async get(messageId: string): Promise<PipelineDLQItem | null> {
    return this.getItem<PipelineDLQItem>({ messageId });
  }

  async listPending(opts?: { limit?: number }): Promise<PipelineDLQItem[]> {
    const limit = opts?.limit ?? 50;
    return this.query<PipelineDLQItem>({
      IndexName: 'failedAt-index',
      KeyConditionExpression: '#status = :status',
      ExpressionAttributeNames: { '#status': 'status' },
      ExpressionAttributeValues: { ':status': 'pending' },
      ScanIndexForward: false,
      Limit: Math.min(limit, 200),
    });
  }

  async listAll(opts?: { limit?: number }): Promise<PipelineDLQItem[]> {
    const limit = opts?.limit ?? 50;
    const result = await this.docClient.send(
      new ScanCommand({
        TableName: this.tableName,
        Limit: Math.min(limit, 200),
      }),
    );
    const items = (result.Items ?? []) as PipelineDLQItem[];
    // Sort most recent failedAt first
    return items.sort((a, b) => b.failedAt.localeCompare(a.failedAt));
  }

  async markRetried(messageId: string): Promise<void> {
    await this.updateItem({
      Key: { messageId },
      UpdateExpression: 'SET #status = :status, retriedAt = :retriedAt',
      ExpressionAttributeNames: { '#status': 'status' },
      ExpressionAttributeValues: {
        ':status': 'retried',
        ':retriedAt': new Date().toISOString(),
      },
    });
  }

  async markDiscarded(messageId: string): Promise<void> {
    await this.updateItem({
      Key: { messageId },
      UpdateExpression: 'SET #status = :status',
      ExpressionAttributeNames: { '#status': 'status' },
      ExpressionAttributeValues: { ':status': 'discarded' },
    });
  }

  async delete(messageId: string): Promise<void> {
    return this.deleteItem({ messageId });
  }
}
