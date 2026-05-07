import { DynamoDBDocumentClient, BatchWriteCommand } from '@aws-sdk/lib-dynamodb';
import { ulid } from 'ulid';
import { BaseRepository } from './BaseRepository';
import { tableName } from '../lib/ddb-table-name';

export interface DocumentSectionFields {
  documentId: string;
  sectionId: string;
  type: string;
  title: string;
  sectionType?: string;
  sortOrder: number;
  metadata?: Record<string, unknown>;
  placeholder?: string;
  createdAt: string;
  updatedAt: string;
}

export type CreateDocumentSectionInput = {
  documentId: string;
  sectionId?: string;
  type: string;
  title: string;
  sectionType?: string;
  sortOrder: number;
  metadata?: Record<string, unknown>;
  placeholder?: string;
};

const TABLE_NAME = tableName('document-sections');

export class DocumentSectionRepository extends BaseRepository {
  private docClientRef: DynamoDBDocumentClient;

  constructor(docClient: DynamoDBDocumentClient) {
    super(TABLE_NAME, docClient);
    this.docClientRef = docClient;
  }

  async createSection(input: CreateDocumentSectionInput): Promise<DocumentSectionFields> {
    const now = new Date().toISOString();
    const section: DocumentSectionFields = {
      documentId: input.documentId,
      sectionId: input.sectionId ?? ulid(),
      type: input.type,
      title: input.title,
      ...(input.sectionType !== undefined && { sectionType: input.sectionType }),
      sortOrder: input.sortOrder,
      ...(input.metadata !== undefined && { metadata: input.metadata }),
      ...(input.placeholder !== undefined && { placeholder: input.placeholder }),
      createdAt: now,
      updatedAt: now,
    };
    await this.putItem(section as unknown as Record<string, unknown>);
    return section;
  }

  async getSectionsForDocument(documentId: string): Promise<DocumentSectionFields[]> {
    const sections = await this.query<DocumentSectionFields>({
      KeyConditionExpression: 'documentId = :d',
      ExpressionAttributeValues: { ':d': documentId },
    });
    return sections.sort((a, b) => a.sortOrder - b.sortOrder);
  }

  async updateSectionFields(
    documentId: string,
    sectionId: string,
    updates: Partial<Omit<DocumentSectionFields, 'documentId' | 'sectionId' | 'createdAt'>>,
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
      Key: { documentId, sectionId },
      UpdateExpression: `SET ${parts.join(', ')}`,
      ExpressionAttributeNames: names,
      ExpressionAttributeValues: values,
      ReturnValues: 'ALL_NEW',
    });
  }

  async deleteSectionById(documentId: string, sectionId: string): Promise<void> {
    await this.deleteItem({ documentId, sectionId });
  }

  async reorderSections(documentId: string, sectionIds: string[]): Promise<void> {
    const now = new Date().toISOString();

    // DynamoDB BatchWrite supports max 25 items per call
    const batches: { PutRequest: { Item: Record<string, unknown> } }[][] = [];
    const existing = await this.getSectionsForDocument(documentId);
    const sectionMap = new Map(existing.map((s) => [s.sectionId, s]));

    const writeRequests: { PutRequest: { Item: Record<string, unknown> } }[] = [];
    for (let i = 0; i < sectionIds.length; i++) {
      const section = sectionMap.get(sectionIds[i]);
      if (!section) continue;
      writeRequests.push({
        PutRequest: {
          Item: {
            ...section,
            sortOrder: i,
            updatedAt: now,
          } as unknown as Record<string, unknown>,
        },
      });
    }

    // Split into batches of 25
    for (let i = 0; i < writeRequests.length; i += 25) {
      batches.push(writeRequests.slice(i, i + 25));
    }

    for (const batch of batches) {
      await this.docClientRef.send(
        new BatchWriteCommand({
          RequestItems: {
            [TABLE_NAME]: batch,
          },
        }),
      );
    }
  }
}
