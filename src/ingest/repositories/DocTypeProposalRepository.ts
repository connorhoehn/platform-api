// Ingest subsystem — DocType-proposal queue persistence.
//
// Auto-discovered document type proposals from the generative tier. Sit
// here until a human promotes them into a real DocType (or rejects them).
// The clustering signature lets the engine count "similar" low-confidence
// items already proposed for a source so it doesn't spam duplicates.
//
// Key schema:
//   PK: proposalId (HASH)
// GSIs:
//   sourceId-status-index       — list pending proposals per source
//   sourceId-signature-index    — fast lookup for "similar already proposed?"

import { DynamoDBDocumentClient, ScanCommand } from '@aws-sdk/lib-dynamodb';
import { BaseRepository } from '../../repositories/BaseRepository';
import { tableName } from '../../lib/ddb-table-name';
import type { DocTypeProposalEntry } from '../types';
import type { DocTypeProposalRepository as DocTypeProposalRepositoryContract } from '../mapping/mapping-pipeline';

export class DocTypeProposalRepository extends BaseRepository
  implements DocTypeProposalRepositoryContract {
  constructor(docClient: DynamoDBDocumentClient) {
    super(tableName('ingest-doctype-proposals'), docClient);
  }

  async put(entry: DocTypeProposalEntry): Promise<void> {
    return super.putItem(entry as unknown as Record<string, unknown>);
  }

  async getById(proposalId: string): Promise<DocTypeProposalEntry | null> {
    return this.getItem<DocTypeProposalEntry>({ proposalId });
  }

  async listPendingBySource(sourceId: string): Promise<DocTypeProposalEntry[]> {
    return this.query<DocTypeProposalEntry>({
      IndexName: 'sourceId-status-index',
      KeyConditionExpression: 'sourceId = :sourceId AND #status = :status',
      ExpressionAttributeNames: { '#status': 'status' },
      ExpressionAttributeValues: { ':sourceId': sourceId, ':status': 'pending' },
    });
  }

  async listPendingBySignature(
    sourceId: string,
    signature: string,
  ): Promise<DocTypeProposalEntry[]> {
    const all = await this.query<DocTypeProposalEntry>({
      IndexName: 'sourceId-signature-index',
      KeyConditionExpression: 'sourceId = :sourceId AND signature = :sig',
      ExpressionAttributeValues: { ':sourceId': sourceId, ':sig': signature },
    });
    return all.filter((p) => p.status === 'pending');
  }

  /**
   * Scan-based list of every pending proposal across all sources. Used by
   * the operator UI's promotion-queue page when no source filter is set.
   * v1 scale (≤ a few hundred proposals) — swap to a paged scan once scale
   * warrants it.
   */
  async listAllPending(limit = 200): Promise<DocTypeProposalEntry[]> {
    const result = await this.docClient.send(
      new ScanCommand({
        TableName: this.tableName,
        FilterExpression: '#status = :status',
        ExpressionAttributeNames: { '#status': 'status' },
        ExpressionAttributeValues: { ':status': 'pending' },
        Limit: Math.min(limit, 500),
      }),
    );
    return (result.Items ?? []) as DocTypeProposalEntry[];
  }

  async resolve(
    proposalId: string,
    resolution: 'promoted' | 'rejected',
    reviewedBy: string,
  ): Promise<void> {
    await this.updateItem({
      Key: { proposalId },
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
