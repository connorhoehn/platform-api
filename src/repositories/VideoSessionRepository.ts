import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { BaseRepository } from './BaseRepository';
import { tableName } from '../lib/ddb-table-name';

export interface VideoSessionParticipant {
  userId: string;
  displayName: string;
  joinedAt: string;
}

export interface VideoSessionRecord {
  documentId: string;
  sessionId: string;
  vnlSessionId: string;
  status: 'active' | 'ended';
  startedAt: string;
  endedAt?: string;
  startedBy: string;
  participants: VideoSessionParticipant[];
  transcriptStatus?: 'pending' | 'processing' | 'available' | 'failed';
  transcript?: string;
  aiSummary?: string;
}

const TABLE_NAME = tableName('document-video-sessions');

export class VideoSessionRepository extends BaseRepository {
  constructor(docClient: DynamoDBDocumentClient) {
    super(TABLE_NAME, docClient);
  }

  async createSession(record: VideoSessionRecord): Promise<VideoSessionRecord> {
    await this.putItem(record as unknown as Record<string, unknown>);
    return record;
  }

  async endSession(
    documentId: string,
    sessionId: string,
    endedAt: string,
  ): Promise<void> {
    await this.updateItem({
      Key: { documentId, sessionId },
      UpdateExpression: 'SET #status = :status, endedAt = :endedAt',
      ExpressionAttributeNames: { '#status': 'status' },
      ExpressionAttributeValues: {
        ':status': 'ended',
        ':endedAt': endedAt,
      },
    });
  }

  async addParticipant(
    documentId: string,
    sessionId: string,
    participant: VideoSessionParticipant,
  ): Promise<void> {
    await this.updateItem({
      Key: { documentId, sessionId },
      UpdateExpression: 'SET participants = list_append(if_not_exists(participants, :empty), :participant)',
      ExpressionAttributeValues: {
        ':participant': [participant],
        ':empty': [],
      },
    });
  }

  async getSession(
    documentId: string,
    sessionId: string,
  ): Promise<VideoSessionRecord | null> {
    return this.getItem<VideoSessionRecord>({ documentId, sessionId });
  }

  async getSessionsByDocument(documentId: string): Promise<VideoSessionRecord[]> {
    const results = await this.query<VideoSessionRecord>({
      KeyConditionExpression: 'documentId = :docId',
      ExpressionAttributeValues: { ':docId': documentId },
    });
    // Sort by startedAt descending (newest first) since sessionId (UUID) is not temporal
    return results.sort((a, b) => (b.startedAt ?? '').localeCompare(a.startedAt ?? ''));
  }
}
