// Phase 51 / hub#62 — server-side approval log persistence.
//
// Mirrors the decisions-renderer append flow into the dedicated
// `approval-workflows` DDB table (provisioned by the Tiltfile). Each
// approval entry is a single row keyed by (documentId, workflowId);
// the GSI `status-index` (workflowStatus + createdAt) supports
// audit queries like "all pending approvals across all docs."
//
// Storage shape (matches the Tiltfile schema):
//   pk: documentId (HASH)
//   sk: workflowId (RANGE)
//   GSI status-index: workflowStatus (HASH) + createdAt (RANGE)

import { DynamoDBDocumentClient, QueryCommand } from '@aws-sdk/lib-dynamodb';
import { BaseRepository } from './BaseRepository';
import { tableName } from '../lib/ddb-table-name';

export type ApprovalDecision = 'approved' | 'rejected' | 'changes_requested';

export interface ApprovalEntry {
  documentId: string;
  workflowId: string;       // unique per entry; client-generated
  sectionId: string;        // section the approval is for
  reviewerId: string;
  reviewerName?: string;    // denormalized for audit readability
  decision: ApprovalDecision;
  comment?: string;
  workflowStatus: ApprovalDecision; // mirrors decision; required for the GSI
  createdAt: string;        // ISO
}

export class ApprovalRepository extends BaseRepository {
  constructor(docClient: DynamoDBDocumentClient) {
    super(tableName('approval-workflows'), docClient);
  }

  async create(entry: ApprovalEntry): Promise<void> {
    return this.putItem(entry as unknown as Record<string, unknown>);
  }

  async listByDocument(documentId: string, limit = 100): Promise<ApprovalEntry[]> {
    const result = await this.docClient.send(
      new QueryCommand({
        TableName: this.tableName,
        KeyConditionExpression: 'documentId = :did',
        ExpressionAttributeValues: { ':did': documentId },
        Limit: Math.min(limit, 500),
      }),
    );
    return (result.Items ?? []) as ApprovalEntry[];
  }

  async listByStatus(status: ApprovalDecision, limit = 100): Promise<ApprovalEntry[]> {
    const result = await this.docClient.send(
      new QueryCommand({
        TableName: this.tableName,
        IndexName: 'status-index',
        KeyConditionExpression: 'workflowStatus = :s',
        ExpressionAttributeValues: { ':s': status },
        ScanIndexForward: false, // newest first
        Limit: Math.min(limit, 500),
      }),
    );
    return (result.Items ?? []) as ApprovalEntry[];
  }
}
