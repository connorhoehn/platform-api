// Ingest subsystem — mapping needs-review queue persistence.
//
// One entry per item whose mapping confidence fell below the per-source
// threshold. Operators triage these in the UI; clearing one promotes or
// rejects the LLM's suggested mapping.
//
// Key schema:
//   PK: ingestId (HASH)         — exactly one open review per item
// GSI:
//   sourceId-status-index       — list pending reviews for a source
//   status-createdAt-index      — global queue dashboard, oldest-first

import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { BaseRepository } from '../../repositories/BaseRepository';
import { tableName } from '../../lib/ddb-table-name';
import type { ReviewQueueEntry } from '../types';
import type { MappingReviewQueueRepository as MappingReviewQueueRepositoryContract } from '../mapping/mapping-pipeline';

export class MappingReviewQueueRepository extends BaseRepository
  implements MappingReviewQueueRepositoryContract {
  constructor(docClient: DynamoDBDocumentClient) {
    super(tableName('ingest-mapping-review-queue'), docClient);
  }

  async put(entry: ReviewQueueEntry): Promise<void> {
    return super.putItem(entry as unknown as Record<string, unknown>);
  }

  async getById(ingestId: string): Promise<ReviewQueueEntry | null> {
    return this.getItem<ReviewQueueEntry>({ ingestId });
  }

  async listPendingBySource(sourceId: string): Promise<ReviewQueueEntry[]> {
    return this.query<ReviewQueueEntry>({
      IndexName: 'sourceId-status-index',
      KeyConditionExpression: 'sourceId = :sourceId AND #status = :status',
      ExpressionAttributeNames: { '#status': 'status' },
      ExpressionAttributeValues: { ':sourceId': sourceId, ':status': 'pending' },
    });
  }

  async listAllPending(limit = 100): Promise<ReviewQueueEntry[]> {
    return this.query<ReviewQueueEntry>({
      IndexName: 'status-createdAt-index',
      KeyConditionExpression: '#status = :status',
      ExpressionAttributeNames: { '#status': 'status' },
      ExpressionAttributeValues: { ':status': 'pending' },
      Limit: Math.min(limit, 500),
    });
  }

  async resolve(
    ingestId: string,
    resolution: 'approved' | 'rejected',
    reviewedBy: string,
  ): Promise<void> {
    await this.updateItem({
      Key: { ingestId },
      UpdateExpression: 'SET #status = :s, reviewedAt = :now, reviewedBy = :by',
      ExpressionAttributeNames: { '#status': 'status' },
      ExpressionAttributeValues: {
        ':s': resolution,
        ':now': new Date().toISOString(),
        ':by': reviewedBy,
      },
    });
  }
}
