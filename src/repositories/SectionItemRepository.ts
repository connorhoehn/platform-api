import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { ulid } from 'ulid';
import { BaseRepository } from './BaseRepository';
import { tableName } from '../lib/ddb-table-name';
import type { ReattachEvent, WorkItemProvenance } from '../types/workItem';

/**
 * A section item, optionally carrying work-item provenance.
 *
 * The provenance half is spread in rather than nested so that every existing
 * reader — the task list, `GET /api/items/mine`, the WS fan-out — keeps working
 * unchanged on rows that have it. See src/types/workItem.ts for why a work item
 * is a section item and not a table of its own.
 */
export interface SectionItemFields extends WorkItemProvenance {
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

export type CreateSectionItemInput = WorkItemProvenance & {
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

/** Keys that are part of the primary key and can never be patched in place. */
const IMMUTABLE_FIELDS = new Set(['sectionKey', 'itemId', 'documentId', 'sectionId', 'createdAt']);

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
      // Provenance. Undefined keys are dropped rather than written as null so a
      // hand-created task stays byte-identical to what it was before work items
      // existed.
      ...(input.source !== undefined && { source: input.source }),
      ...(input.utterance !== undefined && { utterance: input.utterance }),
      ...(input.utteranceId !== undefined && { utteranceId: input.utteranceId }),
      ...(input.captureId !== undefined && { captureId: input.captureId }),
      ...(input.capturedContext !== undefined && { capturedContext: input.capturedContext }),
      ...(input.confidenceTier !== undefined && { confidenceTier: input.confidenceTier }),
      ...(input.attachState !== undefined && { attachState: input.attachState }),
      ...(input.candidates !== undefined && { candidates: input.candidates }),
      ...(input.reattachHistory !== undefined && { reattachHistory: input.reattachHistory }),
      ...(input.createdBy !== undefined && { createdBy: input.createdBy }),
      ...(input.proposal !== undefined && { proposal: input.proposal }),
      ...(input.proposalStatus !== undefined && { proposalStatus: input.proposalStatus }),
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
      // A caller who patches `sectionId` would leave the row's own idea of where
      // it lives disagreeing with the partition key it is stored under, and the
      // section query would stop returning it. Moving is `moveItem`.
      if (IMMUTABLE_FIELDS.has(key)) continue;
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

  /**
   * Find an item by document + itemId, without knowing which section holds it.
   *
   * Section is half the partition key, so the obvious REST shape would put it in
   * the URL — and then every URL for a work item would break the moment the item
   * was re-attached somewhere else. Since re-attachment is a first-class
   * operation here rather than a repair, the item's own id has to be enough to
   * address it. The document-wide GSI makes that a single query over a bounded
   * set (a document has tens of items, not thousands).
   */
  async findByItemId(documentId: string, itemId: string): Promise<SectionItemFields | null> {
    const rows = await this.query<SectionItemFields>({
      IndexName: 'documentId-index',
      KeyConditionExpression: 'documentId = :d',
      FilterExpression: 'itemId = :i',
      ExpressionAttributeValues: { ':d': documentId, ':i': itemId },
    });
    return rows[0] ?? null;
  }

  /** One item by its full key, or null. */
  async getOne(
    documentId: string,
    sectionId: string,
    itemId: string,
  ): Promise<SectionItemFields | null> {
    const row = await this.getItem<SectionItemFields>({
      sectionKey: this.sectionKey(documentId, sectionId),
      itemId,
    });
    return row ?? null;
  }

  /**
   * Move an item to a different section, preserving its identity and history.
   *
   * The section is half the partition key, so a move is physically a write to a
   * new row and a delete of the old one — but it must not READ as a new item.
   * The prior art here is uniformly bad: Granola's only remedy for a misfiled
   * note is to delete it and start over, and Otter's reassign is one-way and
   * drops the original association. So:
   *
   *   * `itemId` is carried over, which keeps every existing reference valid.
   *   * `reattachHistory` is APPENDED to, never replaced, so where an item has
   *     been is always recoverable.
   *   * `createdAt` and the provenance block are untouched — the utterance did
   *     not happen again just because we filed it somewhere else.
   *
   * The put precedes the delete on purpose. If the process dies between them the
   * item exists twice, which is visible and repairable; the other order loses it.
   */
  async moveItem(params: {
    documentId: string;
    fromSectionId: string;
    toSectionId: string;
    itemId: string;
    by: string;
    via: ReattachEvent['via'];
    /** Set when the destination came from healing, cleared on a human move. */
    attachState: SectionItemFields['attachState'];
  }): Promise<SectionItemFields | null> {
    const { documentId, fromSectionId, toSectionId, itemId, by, via, attachState } = params;
    if (fromSectionId === toSectionId) {
      return this.getOne(documentId, fromSectionId, itemId);
    }

    const existing = await this.getOne(documentId, fromSectionId, itemId);
    if (!existing) return null;

    const event: ReattachEvent = {
      at: new Date().toISOString(),
      by,
      fromSectionId,
      toSectionId,
      via,
    };

    const moved: SectionItemFields = {
      ...existing,
      sectionKey: this.sectionKey(documentId, toSectionId),
      sectionId: toSectionId,
      attachState,
      reattachHistory: [...(existing.reattachHistory ?? []), event],
      updatedAt: event.at,
    };
    // A moved item is no longer a guess, so the ranked alternatives that were
    // offered while it sat in the inbox are stale. Leaving them would invite a
    // second "attach here" click that moves it somewhere nobody chose.
    delete moved.candidates;

    await this.putItem(moved as unknown as Record<string, unknown>);
    await this.deleteItem({
      sectionKey: this.sectionKey(documentId, fromSectionId),
      itemId,
    });
    return moved;
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
