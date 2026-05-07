// Ingest subsystem — sync cursor / state persistence.
//
// Tracks per-file state for each source so incremental syncs can detect
// changes (via file hash comparison). Key schema: sourceId (HASH),
// filePath (RANGE).

import { DynamoDBDocumentClient, BatchWriteCommand, BatchWriteCommandInput } from '@aws-sdk/lib-dynamodb';
import { BaseRepository } from '../../repositories/BaseRepository';
import { tableName } from '../../lib/ddb-table-name';
import { SyncCursorEntry } from '../types';

export class SyncCursorRepository extends BaseRepository {
  constructor(docClient: DynamoDBDocumentClient) {
    super(tableName('ingest-sync-state'), docClient);
  }

  async get(sourceId: string, filePath: string): Promise<SyncCursorEntry | null> {
    return this.getItem<SyncCursorEntry>({ sourceId, filePath });
  }

  async put(entry: SyncCursorEntry): Promise<void> {
    return this.putItem(entry as unknown as Record<string, unknown>);
  }

  async listBySource(sourceId: string): Promise<SyncCursorEntry[]> {
    return this.query<SyncCursorEntry>({
      KeyConditionExpression: 'sourceId = :sourceId',
      ExpressionAttributeValues: { ':sourceId': sourceId },
    });
  }

  async deleteBySource(sourceId: string): Promise<void> {
    // Fetch all entries for the source, then batch-delete them.
    const entries = await this.listBySource(sourceId);
    if (entries.length === 0) return;

    const chunks = this._chunkArray(entries, 25);
    for (const chunk of chunks) {
      const requestItems: BatchWriteCommandInput['RequestItems'] = {
        [this.tableName]: chunk.map((entry) => ({
          DeleteRequest: { Key: { sourceId: entry.sourceId, filePath: entry.filePath } },
        })),
      };
      await this.docClient.send(new BatchWriteCommand({ RequestItems: requestItems }));
    }
  }

  private _chunkArray<T>(array: T[], size: number): T[][] {
    const chunks: T[][] = [];
    for (let i = 0; i < array.length; i += size) {
      chunks.push(array.slice(i, i + size));
    }
    return chunks;
  }
}
