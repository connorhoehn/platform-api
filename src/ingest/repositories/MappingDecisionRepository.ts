// Ingest subsystem — mapping-decision audit log.
//
// One row per mapping attempt (rule | llm-match | llm-generate). Stored
// alongside the item so operators can replay the decision rationale.
//
// Key schema:
//   PK: decisionId (HASH)
// GSIs:
//   ingestId-createdAt-index   — list all decisions for a single item
//   sourceId-createdAt-index   — list all decisions for a source (timeline)

import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { BaseRepository } from '../../repositories/BaseRepository';
import { tableName } from '../../lib/ddb-table-name';
import type {
  MappingDecision,
} from '../types';
import type { MappingDecisionRepository as MappingDecisionRepositoryContract } from '../mapping/mapping-pipeline';

export class MappingDecisionRepository extends BaseRepository
  implements MappingDecisionRepositoryContract {
  constructor(docClient: DynamoDBDocumentClient) {
    super(tableName('ingest-mapping-decisions'), docClient);
  }

  async put(decision: MappingDecision): Promise<void> {
    return super.putItem(decision as unknown as Record<string, unknown>);
  }

  async getById(decisionId: string): Promise<MappingDecision | null> {
    return this.getItem<MappingDecision>({ decisionId });
  }

  async listByIngestId(ingestId: string): Promise<MappingDecision[]> {
    return this.query<MappingDecision>({
      IndexName: 'ingestId-createdAt-index',
      KeyConditionExpression: 'ingestId = :ingestId',
      ExpressionAttributeValues: { ':ingestId': ingestId },
      ScanIndexForward: false,    // newest first
    });
  }

  async listBySource(sourceId: string, limit = 100): Promise<MappingDecision[]> {
    return this.query<MappingDecision>({
      IndexName: 'sourceId-createdAt-index',
      KeyConditionExpression: 'sourceId = :sourceId',
      ExpressionAttributeValues: { ':sourceId': sourceId },
      ScanIndexForward: false,
      Limit: Math.min(limit, 500),
    });
  }
}
