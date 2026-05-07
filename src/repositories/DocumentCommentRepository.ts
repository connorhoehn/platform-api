import { ulid } from 'ulid';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { BaseRepository } from './BaseRepository';
import { tableName } from '../lib/ddb-table-name';

export interface DocumentComment {
  documentId: string;
  commentId: string;
  sectionId: string;
  text: string;
  userId: string;
  displayName: string;
  color: string;
  timestamp: string;
  parentCommentId?: string;
  resolved?: boolean;
  resolvedBy?: string;
  resolvedAt?: string;
}

const TABLE_NAME = tableName('document-comments');

export class DocumentCommentRepository extends BaseRepository {
  constructor(docClient: DynamoDBDocumentClient) {
    super(TABLE_NAME, docClient);
  }

  async createComment(comment: {
    documentId: string;
    sectionId: string;
    text: string;
    userId: string;
    displayName: string;
    color: string;
    timestamp: string;
    parentCommentId?: string;
  }): Promise<DocumentComment> {
    const commentId = ulid();
    const item: DocumentComment = {
      ...comment,
      commentId,
    };
    await this.putItem(item as unknown as Record<string, unknown>);
    return item;
  }

  async getCommentsForDocument(
    documentId: string,
    limit?: number,
    lastKey?: Record<string, unknown>,
  ): Promise<{ items: DocumentComment[]; lastEvaluatedKey?: Record<string, unknown> }> {
    return this.queryWithPagination<DocumentComment>({
      KeyConditionExpression: 'documentId = :docId',
      ExpressionAttributeValues: { ':docId': documentId },
      ScanIndexForward: true,
      ...(limit ? { Limit: limit } : {}),
      ...(lastKey ? { ExclusiveStartKey: lastKey } : {}),
    });
  }

  async getCommentsForSection(
    sectionId: string,
    limit?: number,
    lastKey?: Record<string, unknown>,
  ): Promise<{ items: DocumentComment[]; lastEvaluatedKey?: Record<string, unknown> }> {
    return this.queryWithPagination<DocumentComment>({
      IndexName: 'sectionId-timestamp-index',
      KeyConditionExpression: 'sectionId = :sid',
      ExpressionAttributeValues: { ':sid': sectionId },
      ScanIndexForward: true,
      ...(limit ? { Limit: limit } : {}),
      ...(lastKey ? { ExclusiveStartKey: lastKey } : {}),
    });
  }

  async resolveThread(
    documentId: string,
    commentId: string,
    resolvedBy: string,
    resolvedAt: string,
  ): Promise<void> {
    await this.updateItem({
      Key: { documentId, commentId },
      UpdateExpression: 'SET resolved = :r, resolvedBy = :by, resolvedAt = :at',
      ExpressionAttributeValues: {
        ':r': true,
        ':by': resolvedBy,
        ':at': resolvedAt,
      },
    });
  }

  async unresolveThread(documentId: string, commentId: string): Promise<void> {
    await this.updateItem({
      Key: { documentId, commentId },
      UpdateExpression: 'REMOVE resolved, resolvedBy, resolvedAt',
    });
  }

  async deleteComment(documentId: string, commentId: string): Promise<void> {
    await this.deleteItem({ documentId, commentId });
  }
}
