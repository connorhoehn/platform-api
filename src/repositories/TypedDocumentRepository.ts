// Phase 51 Phase A — DDB-backed persistence for TypedDocument instances.
//
// Each instance conforms to a DocumentType schema; the route layer enforces
// shape (required fields present, cardinality matches).

import {
  DynamoDBDocumentClient,
  ScanCommand,
  BatchWriteCommand,
  BatchWriteCommandInput,
} from '@aws-sdk/lib-dynamodb';
import { BaseRepository } from './BaseRepository';
import { tableName } from '../lib/ddb-table-name';

// Value shapes Phase B understands. Stored verbatim in DDB; the route
// layer enforces (fieldType, cardinality) → value-shape correspondence
// before persisting.
export type TypedDocumentValue =
  | string
  | string[]
  | number
  | number[]
  | boolean;

export interface TypedDocumentItem {
  documentId: string;
  typeId: string;
  values: Record<string, TypedDocumentValue>;
  createdBy: string;
  createdAt: string;
  updatedAt: string;
}

export class TypedDocumentRepository extends BaseRepository {
  constructor(docClient: DynamoDBDocumentClient) {
    super(tableName('typed-documents'), docClient);
  }

  async create(item: TypedDocumentItem): Promise<void> {
    return this.putItem(item as unknown as Record<string, unknown>);
  }

  async get(documentId: string): Promise<TypedDocumentItem | null> {
    return this.getItem<TypedDocumentItem>({ documentId });
  }

  // Phase A — Scan + filter on typeId. Flagged in the planning doc: a GSI on
  // typeId is the right shape but adding it requires IaC — operator work.
  // Acceptable while document instance counts stay demo-scale.
  async listByType(typeId: string, limit = 50): Promise<TypedDocumentItem[]> {
    const result = await this.docClient.send(
      new ScanCommand({
        TableName: this.tableName,
        FilterExpression: 'typeId = :typeId',
        ExpressionAttributeValues: { ':typeId': typeId },
        Limit: Math.min(limit, 200),
      }),
    );
    return (result.Items ?? []) as TypedDocumentItem[];
  }

  async update(documentId: string, values: Record<string, TypedDocumentValue>): Promise<TypedDocumentItem | null> {
    const existing = await this.get(documentId);
    if (!existing) return null;
    const updated: TypedDocumentItem = {
      ...existing,
      values: { ...existing.values, ...values },
      updatedAt: new Date().toISOString(),
    };
    await this.putItem(updated as unknown as Record<string, unknown>);
    return updated;
  }

  async delete(documentId: string): Promise<void> {
    return this.deleteItem({ documentId });
  }

  // Bulk operations for Phase 51 bulk import/update/delete endpoints.
  // DDB BatchWriteItem supports up to 25 items per request; chunk if needed.
  async bulkCreate(items: TypedDocumentItem[]): Promise<void> {
    const chunks = this._chunkArray(items, 25);
    for (const chunk of chunks) {
      const requestItems: BatchWriteCommandInput['RequestItems'] = {
        [this.tableName]: chunk.map((item) => ({
          PutRequest: { Item: item as unknown as Record<string, unknown> },
        })),
      };
      await this.docClient.send(new BatchWriteCommand({ RequestItems: requestItems }));
    }
  }

  async bulkUpdate(
    updates: Array<{ documentId: string; values: Record<string, TypedDocumentValue>; updatedAt: string }>,
  ): Promise<void> {
    // DDB BatchWriteItem only supports Put/Delete, not Update. For bulk updates,
    // we need to fetch existing items, merge values, and Put them back.
    // Chunked to 25 items per batch (DDB limit).
    const chunks = this._chunkArray(updates, 25);
    for (const chunk of chunks) {
      // Fetch existing items
      const documentIds = chunk.map((u) => u.documentId);
      const existing = new Map<string, TypedDocumentItem>();
      for (const documentId of documentIds) {
        const item = await this.get(documentId);
        if (item) existing.set(documentId, item);
      }

      // Merge and put back
      const putItems = chunk
        .filter((u) => existing.has(u.documentId))
        .map((u) => {
          const item = existing.get(u.documentId)!;
          return {
            ...item,
            values: { ...item.values, ...u.values },
            updatedAt: u.updatedAt,
          };
        });

      if (putItems.length > 0) {
        const requestItems: BatchWriteCommandInput['RequestItems'] = {
          [this.tableName]: putItems.map((item) => ({
            PutRequest: { Item: item as unknown as Record<string, unknown> },
          })),
        };
        await this.docClient.send(new BatchWriteCommand({ RequestItems: requestItems }));
      }
    }
  }

  async bulkDelete(documentIds: string[]): Promise<void> {
    const chunks = this._chunkArray(documentIds, 25);
    for (const chunk of chunks) {
      const requestItems: BatchWriteCommandInput['RequestItems'] = {
        [this.tableName]: chunk.map((documentId) => ({
          DeleteRequest: { Key: { documentId } },
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
