import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { BaseRepository } from './BaseRepository';
import { tableName } from '../lib/ddb-table-name';

export interface SectionReview {
  documentId: string;
  reviewKey: string;
  sectionId: string;
  userId: string;
  displayName: string;
  status: string;
  timestamp: string;
  comment?: string;
}

const TABLE_NAME = tableName('section-reviews');

export class SectionReviewRepository extends BaseRepository {
  constructor(docClient: DynamoDBDocumentClient) {
    super(TABLE_NAME, docClient);
  }

  async submitReview(review: {
    documentId: string;
    sectionId: string;
    userId: string;
    displayName: string;
    status: string;
    timestamp: string;
    comment?: string;
  }): Promise<SectionReview> {
    const reviewKey = `${review.sectionId}:${review.userId}`;
    const item: SectionReview = {
      ...review,
      reviewKey,
    };
    await this.putItem(item as unknown as Record<string, unknown>);
    return item;
  }

  async getReviewsForDocument(documentId: string): Promise<SectionReview[]> {
    return this.query<SectionReview>({
      KeyConditionExpression: 'documentId = :docId',
      ExpressionAttributeValues: { ':docId': documentId },
    });
  }

  async getReviewsForSection(
    documentId: string,
    sectionId: string,
  ): Promise<SectionReview[]> {
    return this.query<SectionReview>({
      KeyConditionExpression:
        'documentId = :docId AND begins_with(reviewKey, :prefix)',
      ExpressionAttributeValues: {
        ':docId': documentId,
        ':prefix': `${sectionId}:`,
      },
    });
  }

  async getUserReviews(userId: string): Promise<SectionReview[]> {
    return this.query<SectionReview>({
      IndexName: 'userId-documentId-index',
      KeyConditionExpression: 'userId = :uid',
      ExpressionAttributeValues: { ':uid': userId },
    });
  }

  async deleteReview(documentId: string, reviewKey: string): Promise<void> {
    await this.deleteItem({ documentId, reviewKey });
  }
}
