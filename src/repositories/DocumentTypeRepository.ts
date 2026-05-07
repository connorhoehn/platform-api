// Phase 51 Phase A — DDB-backed persistence for DocumentType schemas.
//
// Schema definitions for the document-types feature live in the
// `document-types` table (PK: typeId). The shape mirrors the frontend
// `DocumentType` TS interface so the API consumer can deserialize without
// translation.

import { DynamoDBDocumentClient, ScanCommand } from '@aws-sdk/lib-dynamodb';
import { BaseRepository } from './BaseRepository';
import { tableName } from '../lib/ddb-table-name';

export type DocumentTypeFieldKind =
  | 'text'
  | 'long_text'
  | 'number'
  | 'date'
  | 'boolean'
  | 'enum'
  | 'reference';

export type DocumentTypeFieldWidget =
  | 'text_field'
  | 'textarea'
  | 'number_input'
  | 'date_picker'
  | 'checkbox'
  | 'select'
  | 'reference_picker';

export type DocumentTypeFieldCardinality = 1 | 'unlimited';

export interface DocumentTypeFieldValidation {
  /** Numbers: minimum value. Strings: minimum length (per entry, including each entry of an unlimited array). */
  min?: number;
  /** Numbers: maximum value. Strings: maximum length. */
  max?: number;
  /** text/long_text only: each value must match. Server validates regex parses at type-creation time. */
  regex?: string;
  /** boolean only: value must be true. Resolves the "must be checked" use case. */
  requireTrue?: boolean;
}

export interface DocumentTypeFieldShowWhen {
  fieldId: string;
  equals: string | number | boolean;
}

/** Phase E — opt-in visibility per display context.
 *  Default semantics when absent (or `full` unset): visible in full only. */
export interface DocumentTypeFieldDisplayModes {
  full?: boolean;
  teaser?: boolean;
  list?: boolean;
}

export interface DocumentTypeFieldItem {
  fieldId: string;
  name: string;
  fieldType: DocumentTypeFieldKind;
  widget: DocumentTypeFieldWidget;
  cardinality: DocumentTypeFieldCardinality;
  required: boolean;
  helpText: string;
  /** Phase C — controlled vocabulary for fieldType='enum'. Required when fieldType='enum'. */
  options?: string[];
  /** Phase C — target DocumentType for fieldType='reference'. Required when fieldType='reference'. */
  referenceTypeId?: string;
  /** Phase D — additive constraints applied after primitive shape validation. */
  validation?: DocumentTypeFieldValidation;
  /** Phase D — only show this field when another field on the type equals a value. */
  showWhen?: DocumentTypeFieldShowWhen;
  /** Phase E — per-context visibility (full / teaser / list). */
  displayModes?: DocumentTypeFieldDisplayModes;
}

export interface DocumentTypeItem {
  typeId: string;
  name: string;
  description: string;
  icon: string;
  fields: DocumentTypeFieldItem[];
  createdBy: string;
  createdAt: string;
  updatedAt: string;
  version?: number;
  previousVersions?: DocumentTypeVersionSnapshot[];
}

export interface DocumentTypeVersionSnapshot {
  version: number;
  name: string;
  description: string;
  icon: string;
  fields: DocumentTypeFieldItem[];
  updatedAt: string;
}

export class DocumentTypeRepository extends BaseRepository {
  constructor(docClient: DynamoDBDocumentClient) {
    super(tableName('document-types'), docClient);
  }

  async create(item: DocumentTypeItem): Promise<void> {
    return this.putItem(item as unknown as Record<string, unknown>);
  }

  async get(typeId: string): Promise<DocumentTypeItem | null> {
    return this.getItem<DocumentTypeItem>({ typeId });
  }

  async delete(typeId: string): Promise<void> {
    return this.deleteItem({ typeId });
  }

  // Phase A — full-table Scan, capped. Acceptable for the demo cardinality
  // (hand-crafted document types, not user-generated content). A GSI keyed
  // on `createdBy` would be the right move once tenancy lands.
  async list(limit = 50): Promise<DocumentTypeItem[]> {
    const result = await this.docClient.send(
      new ScanCommand({ TableName: this.tableName, Limit: Math.min(limit, 200) }),
    );
    return (result.Items ?? []) as DocumentTypeItem[];
  }

  async update(
    typeId: string,
    patch: Partial<Pick<DocumentTypeItem, 'name' | 'description' | 'icon' | 'fields'>>,
    existingItem?: DocumentTypeItem,
  ): Promise<DocumentTypeItem> {
    const expressions: string[] = [];
    const exprNames: Record<string, string> = {};
    const exprValues: Record<string, unknown> = {};

    if (patch.name !== undefined) {
      expressions.push('#name = :name');
      exprNames['#name'] = 'name';
      exprValues[':name'] = patch.name;
    }
    if (patch.description !== undefined) {
      expressions.push('description = :description');
      exprValues[':description'] = patch.description;
    }
    if (patch.icon !== undefined) {
      expressions.push('icon = :icon');
      exprValues[':icon'] = patch.icon;
    }
    if (patch.fields !== undefined) {
      expressions.push('#fields = :fields');
      exprNames['#fields'] = 'fields';
      exprValues[':fields'] = patch.fields;
    }

    const now = new Date().toISOString();
    expressions.push('updatedAt = :updatedAt');
    exprValues[':updatedAt'] = now;

    // Schema versioning: bump version and snapshot the previous state.
    const currentVersion = existingItem?.version ?? 0;
    const nextVersion = currentVersion + 1;
    expressions.push('version = :version');
    exprValues[':version'] = nextVersion;

    if (existingItem) {
      const snapshot: DocumentTypeVersionSnapshot = {
        version: currentVersion || 1,
        name: existingItem.name,
        description: existingItem.description,
        icon: existingItem.icon,
        fields: existingItem.fields,
        updatedAt: existingItem.updatedAt,
      };
      const history = existingItem.previousVersions ?? [];
      const trimmed = history.length >= 20 ? history.slice(-19) : history;
      expressions.push('previousVersions = :previousVersions');
      exprValues[':previousVersions'] = [...trimmed, snapshot];
    }

    const result = await this.updateItem({
      Key: { typeId },
      UpdateExpression: 'SET ' + expressions.join(', '),
      ...(Object.keys(exprNames).length ? { ExpressionAttributeNames: exprNames } : {}),
      ExpressionAttributeValues: exprValues,
      ReturnValues: 'ALL_NEW',
    });

    return result as unknown as DocumentTypeItem;
  }
}
