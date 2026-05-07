// Ingest subsystem — ingest item persistence.
//
// Each item tracks a single file through the ingest pipeline (queued → mapping
// → materializing → triggering → done/failed). Key schema: ingestId (HASH).
// GSI: sourceId-status-index (sourceId HASH, status RANGE).

import { DynamoDBDocumentClient, ScanCommand } from '@aws-sdk/lib-dynamodb';
import { BaseRepository } from '../../repositories/BaseRepository';
import { tableName } from '../../lib/ddb-table-name';
import { IngestItem, IngestItemStatus } from '../types';

export class IngestItemRepository extends BaseRepository {
  constructor(docClient: DynamoDBDocumentClient) {
    super(tableName('ingest-items'), docClient);
  }

  async getById(ingestId: string): Promise<IngestItem | null> {
    return this.getItem<IngestItem>({ ingestId });
  }

  async put(item: IngestItem): Promise<void> {
    return super.putItem(item as unknown as Record<string, unknown>);
  }

  async updateStatus(
    ingestId: string,
    status: IngestItemStatus,
    extra?: Partial<Omit<IngestItem, 'ingestId' | 'status'>>,
  ): Promise<void> {
    const names: Record<string, string> = { '#status': 'status', '#updatedAt': 'updatedAt' };
    const values: Record<string, unknown> = {
      ':status': status,
      ':updatedAt': new Date().toISOString(),
    };
    const parts: string[] = ['#status = :status', '#updatedAt = :updatedAt'];

    if (extra) {
      Object.entries(extra).forEach(([key, value]) => {
        const safeKey = `#${key}`;
        const safeVal = `:${key}`;
        names[safeKey] = key;
        values[safeVal] = value;
        parts.push(`${safeKey} = ${safeVal}`);
      });
    }

    await this.updateItem({
      Key: { ingestId },
      UpdateExpression: `SET ${parts.join(', ')}`,
      ExpressionAttributeNames: names,
      ExpressionAttributeValues: values,
    });
  }

  async listBySource(
    sourceId: string,
    status?: IngestItemStatus,
  ): Promise<IngestItem[]> {
    if (status) {
      return this.query<IngestItem>({
        IndexName: 'sourceId-status-index',
        KeyConditionExpression: 'sourceId = :sourceId AND #status = :status',
        ExpressionAttributeNames: { '#status': 'status' },
        ExpressionAttributeValues: { ':sourceId': sourceId, ':status': status },
      });
    }
    return this.query<IngestItem>({
      IndexName: 'sourceId-status-index',
      KeyConditionExpression: 'sourceId = :sourceId',
      ExpressionAttributeValues: { ':sourceId': sourceId },
    });
  }

  /** Scan with filter — acceptable at demo scale; no GSI with status as PK. */
  async listByStatus(
    status: IngestItemStatus,
    limit?: number,
  ): Promise<IngestItem[]> {
    const result = await this.docClient.send(
      new ScanCommand({
        TableName: this.tableName,
        FilterExpression: '#status = :status',
        ExpressionAttributeNames: { '#status': 'status' },
        ExpressionAttributeValues: { ':status': status },
        ...(limit ? { Limit: Math.min(limit, 200) } : {}),
      }),
    );
    return (result.Items ?? []) as IngestItem[];
  }

  async delete(ingestId: string): Promise<void> {
    return super.deleteItem({ ingestId });
  }
}
