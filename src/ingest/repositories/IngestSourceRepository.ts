// Ingest subsystem — source configuration persistence.
//
// Each source represents a configured ingest endpoint (local directory, S3
// bucket, Google Drive, etc.). Key schema: sourceId (HASH).

import { DynamoDBDocumentClient, ScanCommand } from '@aws-sdk/lib-dynamodb';
import { BaseRepository } from '../../repositories/BaseRepository';
import { tableName } from '../../lib/ddb-table-name';
import { IngestSource } from '../types';

export class IngestSourceRepository extends BaseRepository {
  constructor(docClient: DynamoDBDocumentClient) {
    super(tableName('ingest-sources'), docClient);
  }

  async getById(sourceId: string): Promise<IngestSource | null> {
    return this.getItem<IngestSource>({ sourceId });
  }

  async putSource(source: IngestSource): Promise<void> {
    return this.putItem(source as unknown as Record<string, unknown>);
  }

  async updateSource(
    sourceId: string,
    updates: Partial<Omit<IngestSource, 'sourceId'>>,
  ): Promise<void> {
    const names: Record<string, string> = {};
    const values: Record<string, unknown> = {};
    const parts: string[] = [];

    Object.entries(updates).forEach(([key, value]) => {
      const safeKey = `#${key}`;
      const safeVal = `:${key}`;
      names[safeKey] = key;
      values[safeVal] = value;
      parts.push(`${safeKey} = ${safeVal}`);
    });

    if (parts.length === 0) return;

    await this.updateItem({
      Key: { sourceId },
      UpdateExpression: `SET ${parts.join(', ')}`,
      ExpressionAttributeNames: names,
      ExpressionAttributeValues: values,
    });
  }

  async deleteSource(sourceId: string): Promise<void> {
    return this.deleteItem({ sourceId });
  }

  async listAll(limit = 100): Promise<IngestSource[]> {
    const result = await this.docClient.send(
      new ScanCommand({
        TableName: this.tableName,
        Limit: Math.min(limit, 500),
      }),
    );
    return (result.Items ?? []) as IngestSource[];
  }

  async listByStatus(status: IngestSource['status'], limit = 100): Promise<IngestSource[]> {
    const result = await this.docClient.send(
      new ScanCommand({
        TableName: this.tableName,
        FilterExpression: '#status = :status',
        ExpressionAttributeNames: { '#status': 'status' },
        ExpressionAttributeValues: { ':status': status },
        Limit: Math.min(limit, 500),
      }),
    );
    return (result.Items ?? []) as IngestSource[];
  }
}
