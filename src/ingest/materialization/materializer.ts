// Ingest subsystem — document materializer.
//
// Orchestrates creating/updating TypedDocuments from ingested files.
// Accepts repository dependencies via constructor injection.

import { randomUUID } from 'crypto';
import { IngestItem, FileContent, MappingResult } from '../types';
import { TypedDocumentRepository, TypedDocumentItem, TypedDocumentValue } from '../../repositories/TypedDocumentRepository';
import { extractFields, ExtractionResult } from './field-extractor';

export interface MaterializationResult {
  documentId: string;
  created: boolean; // true if new, false if updated existing
  warnings: string[]; // extraction warnings passed through
}

export interface MaterializeOptions {
  /** If a document already exists for this source+filePath, pass its ID here. */
  existingDocumentId: string | null;
  /** User/system identity to stamp as createdBy on new documents. */
  createdBy: string;
}

/**
 * Event emitter for document lifecycle events. Compatible with EventBus or
 * EventEmitter. Used to trigger pipeline workflows via document.created/updated.
 */
export interface DocumentEventEmitter {
  emit(event: { type: 'document.created' | 'document.updated'; documentId: string; typeId: string; timestamp: string }): void;
}

export class DocumentMaterializer {
  constructor(
    private readonly typedDocumentRepo: TypedDocumentRepository,
    private readonly eventEmitter?: DocumentEventEmitter,
  ) {}

  /**
   * Materializes a TypedDocument from an ingested file.
   *
   * 1. Uses field-extractor to pull structured data from the content
   * 2. If existingDocumentId is set: updates the existing document's values
   * 3. Otherwise: creates a new typed document
   * 4. Returns the documentId and whether it was created or updated
   */
  async materialize(
    item: IngestItem,
    content: FileContent,
    mapping: MappingResult,
    options: MaterializeOptions,
  ): Promise<MaterializationResult> {
    // Extract fields from raw content
    const extraction: ExtractionResult = extractFields(
      content,
      mapping.fieldMapping,
      item.mimeType,
    );

    const now = new Date().toISOString();
    const values = extraction.fields as Record<string, TypedDocumentValue>;

    if (options.existingDocumentId) {
      // Update existing document
      const existing = await this.typedDocumentRepo.get(options.existingDocumentId);
      if (existing) {
        const updated: TypedDocumentItem = {
          ...existing,
          values: { ...existing.values, ...values },
          updatedAt: now,
        };
        await this.typedDocumentRepo.create(updated); // PutItem overwrites

        // Emit document.updated event for pipeline triggering
        if (this.eventEmitter) {
          this.eventEmitter.emit({
            type: 'document.updated',
            documentId: options.existingDocumentId,
            typeId: mapping.targetTypeId,
            timestamp: now,
          });
        }

        return {
          documentId: options.existingDocumentId,
          created: false,
          warnings: extraction.warnings,
        };
      }
      // If the document was deleted between lookup and now, fall through to create
    }

    // Create new document
    const documentId = randomUUID();
    const newDoc: TypedDocumentItem = {
      documentId,
      typeId: mapping.targetTypeId,
      values,
      createdBy: options.createdBy,
      createdAt: now,
      updatedAt: now,
    };
    await this.typedDocumentRepo.create(newDoc);

    // Emit document.created event for pipeline triggering
    if (this.eventEmitter) {
      this.eventEmitter.emit({
        type: 'document.created',
        documentId,
        typeId: mapping.targetTypeId,
        timestamp: now,
      });
    }

    return {
      documentId,
      created: true,
      warnings: extraction.warnings,
    };
  }
}
