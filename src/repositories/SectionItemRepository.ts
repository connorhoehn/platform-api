import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { ulid } from 'ulid';
import { BaseRepository } from './BaseRepository';
import { tableName } from '../lib/ddb-table-name';

export interface SectionItemFields {
  sectionKey: string;
  itemId: string;
  documentId: string;
  sectionId: string;
  text: string;
  assignee?: string;
  priority?: string;
  status?: string;
  dueDate?: string;
  category?: string;
  notes?: string;
  ackedBy?: string;
  ackedAt?: string;
  createdAt: string;
  updatedAt: string;
}

export type CreateSectionItemInput = {
  documentId: string;
  sectionId: string;
  itemId?: string;
  text: string;
  assignee?: string;
  priority?: string;
  status?: string;
  dueDate?: string;
  category?: string;
  notes?: string;
};

const TABLE_NAME = tableName('section-items');

export class SectionItemRepository extends BaseRepository {
  constructor(docClient: DynamoDBDocumentClient) {
    super(TABLE_NAME, docClient);
  }

  private sectionKey(documentId: string, sectionId: string): string {
    return `${documentId}:${sectionId}`;
  }

  async createItem(input: CreateSectionItemInput): Promise<SectionItemFields> {
    const now = new Date().toISOString();
    const itemId = input.itemId ?? ulid();
    const item: SectionItemFields = {
      sectionKey: this.sectionKey(input.documentId, input.sectionId),
      itemId,
      documentId: input.documentId,
      sectionId: input.sectionId,
      text: input.text,
      ...(input.assignee !== undefined && { assignee: input.assignee }),
      ...(input.priority !== undefined && { priority: input.priority }),
      status: input.status ?? 'open',
      ...(input.dueDate !== undefined && { dueDate: input.dueDate }),
      ...(input.category !== undefined && { category: input.category }),
      ...(input.notes !== undefined && { notes: input.notes }),
      createdAt: now,
      updatedAt: now,
    };
    await this.putItem(item as unknown as Record<string, unknown>);
    return item;
  }

  async getItemsForSection(
    documentId: string,
    sectionId: string,
    limit?: number,
  ): Promise<SectionItemFields[]> {
    return this.query<SectionItemFields>({
      KeyConditionExpression: 'sectionKey = :sk',
      ExpressionAttributeValues: { ':sk': this.sectionKey(documentId, sectionId) },
      ...(limit !== undefined && { Limit: limit }),
    });
  }

  async updateItemFields(
    documentId: string,
    sectionId: string,
    itemId: string,
    updates: Partial<Omit<SectionItemFields, 'sectionKey' | 'itemId' | 'documentId' | 'sectionId' | 'createdAt'>>,
  ): Promise<Record<string, unknown> | undefined> {
    const now = new Date().toISOString();
    const fields = { ...updates, updatedAt: now };
    const names: Record<string, string> = {};
    const values: Record<string, unknown> = {};
    const parts: string[] = [];

    for (const [key, val] of Object.entries(fields)) {
      if (val === undefined) continue;
      const placeholder = `#${key}`;
      const valPlaceholder = `:${key}`;
      names[placeholder] = key;
      values[valPlaceholder] = val;
      parts.push(`${placeholder} = ${valPlaceholder}`);
    }

    if (parts.length === 0) return undefined;

    return this.updateItem({
      Key: { sectionKey: this.sectionKey(documentId, sectionId), itemId },
      UpdateExpression: `SET ${parts.join(', ')}`,
      ExpressionAttributeNames: names,
      ExpressionAttributeValues: values,
      ReturnValues: 'ALL_NEW',
    });
  }

  async deleteItemById(documentId: string, sectionId: string, itemId: string): Promise<void> {
    await this.deleteItem({
      sectionKey: this.sectionKey(documentId, sectionId),
      itemId,
    });
  }

  async getItemsByAssignee(assignee: string, status?: string): Promise<SectionItemFields[]> {
    if (status) {
      return this.query<SectionItemFields>({
        IndexName: 'assignee-status-index',
        KeyConditionExpression: 'assignee = :a AND #status = :s',
        ExpressionAttributeNames: { '#status': 'status' },
        ExpressionAttributeValues: { ':a': assignee, ':s': status },
      });
    }
    return this.query<SectionItemFields>({
      IndexName: 'assignee-status-index',
      KeyConditionExpression: 'assignee = :a',
      ExpressionAttributeValues: { ':a': assignee },
    });
  }

  async getItemsForDocument(documentId: string): Promise<SectionItemFields[]> {
    return this.query<SectionItemFields>({
      IndexName: 'documentId-index',
      KeyConditionExpression: 'documentId = :d',
      ExpressionAttributeValues: { ':d': documentId },
    });
  }

  async ackItem(
    documentId: string,
    sectionId: string,
    itemId: string,
    ackedBy: string,
    ackedAt: string,
  ): Promise<Record<string, unknown> | undefined> {
    return this.updateItem({
      Key: { sectionKey: this.sectionKey(documentId, sectionId), itemId },
      UpdateExpression: 'SET ackedBy = :ab, ackedAt = :at, updatedAt = :u',
      ExpressionAttributeValues: {
        ':ab': ackedBy,
        ':at': ackedAt,
        ':u': new Date().toISOString(),
      },
      ReturnValues: 'ALL_NEW',
    });
  }
}
