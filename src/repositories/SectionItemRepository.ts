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
  /**
   * The HUMAN LABEL for the assignee — "Alice Chen". This is what renders in
   * the assignee badge, and it is deliberately NOT the query key: keying the
   * cross-document query on it is what left `GET /api/items/mine` structurally
   * empty (the app assigns by display name, the endpoint asked for a Cognito
   * sub, and the two halves never agreed on what a person is called).
   */
  assignee?: string;
  /**
   * The IDENTITY of the assignee — the auth subject (`dev-alice`, a Cognito
   * sub in production). The only thing `GET /api/items/mine` may be keyed on.
   *
   * Optional because rows written before this field existed do not have it,
   * and DynamoDB drops an item missing a GSI key attribute from that index —
   * so a name-only row is simply absent from the id index rather than wrong in
   * it. See `getItemsAssignedTo` for what is done about those rows.
   */
  assigneeId?: string;
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
  assigneeId?: string;
  priority?: string;
  status?: string;
  dueDate?: string;
  category?: string;
  notes?: string;
};

/**
 * A patch to one item. `undefined` leaves a field alone; `null` REMOVEs it.
 *
 * The two are not interchangeable — see `updateItemFields`.
 */
export type SectionItemPatch = {
  [K in keyof Omit<
    SectionItemFields,
    'sectionKey' | 'itemId' | 'documentId' | 'sectionId' | 'createdAt'
  >]?: SectionItemFields[K] | null;
};

/** Keys that are part of the primary key and can never be patched in place. */
const IMMUTABLE_FIELDS = new Set(['sectionKey', 'itemId', 'documentId', 'sectionId', 'createdAt']);

const TABLE_NAME = tableName('section-items');

/** Keyed on `assigneeId` — the identity index. What "my items" is answered from. */
const ASSIGNEE_ID_INDEX = 'assigneeId-status-index';

/**
 * Keyed on `assignee` — the label index, kept only for rows written before
 * `assigneeId` existed. See `getItemsAssignedTo`.
 */
const LEGACY_ASSIGNEE_INDEX = 'assignee-status-index';

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
      // Written only when non-empty: it is a GSI hash key, and DynamoDB rejects
      // an empty string in one. An absent id means "not findable by assignee",
      // which is the truth for a row assigned by name alone.
      ...(input.assigneeId ? { assigneeId: input.assigneeId } : {}),
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

  /**
   * Patch fields on one item.
   *
   * `null` means REMOVE the attribute, and it is not a nicety: `assignee` and
   * `assigneeId` are GSI hash keys, and DynamoDB refuses an empty string in a
   * key attribute. Writing `''` to unassign would have failed the whole update;
   * skipping it (what the route used to do, by testing truthiness) left the row
   * assigned to whoever had it. Removing the attribute is the only way to say
   * "nobody" — it also drops the row out of the index, which is correct.
   */
  async updateItemFields(
    documentId: string,
    sectionId: string,
    itemId: string,
    updates: SectionItemPatch,
  ): Promise<Record<string, unknown> | undefined> {
    const now = new Date().toISOString();
    const fields: SectionItemPatch = { ...updates, updatedAt: now };
    const names: Record<string, string> = {};
    const values: Record<string, unknown> = {};
    const setParts: string[] = [];
    const removeParts: string[] = [];

    for (const [key, val] of Object.entries(fields)) {
      if (val === undefined) continue;
      // A caller who patches `sectionId` would leave the row's own idea of where
      // it lives disagreeing with the partition key it is stored under, and the
      // section query would stop returning it. Moving is `moveItem`.
      if (IMMUTABLE_FIELDS.has(key)) continue;
      const placeholder = `#${key}`;
      names[placeholder] = key;
      if (val === null) {
        removeParts.push(placeholder);
        continue;
      }
      const valPlaceholder = `:${key}`;
      values[valPlaceholder] = val;
      setParts.push(`${placeholder} = ${valPlaceholder}`);
    }

    if (setParts.length === 0 && removeParts.length === 0) return undefined;

    const expression = [
      setParts.length > 0 ? `SET ${setParts.join(', ')}` : '',
      removeParts.length > 0 ? `REMOVE ${removeParts.join(', ')}` : '',
    ]
      .filter(Boolean)
      .join(' ');

    return this.updateItem({
      Key: { sectionKey: this.sectionKey(documentId, sectionId), itemId },
      UpdateExpression: expression,
      ExpressionAttributeNames: names,
      ...(setParts.length > 0 ? { ExpressionAttributeValues: values } : {}),
      ReturnValues: 'ALL_NEW',
    });
  }

  async deleteItemById(documentId: string, sectionId: string, itemId: string): Promise<void> {
    await this.deleteItem({
      sectionKey: this.sectionKey(documentId, sectionId),
      itemId,
    });
  }

  /** One assignee-keyed GSI query, optionally narrowed to a status. */
  private queryAssigneeIndex(
    indexName: string,
    keyAttribute: 'assignee' | 'assigneeId',
    value: string,
    status?: string,
  ): Promise<SectionItemFields[]> {
    return this.query<SectionItemFields>({
      IndexName: indexName,
      KeyConditionExpression: status
        ? `#k = :a AND #status = :s`
        : `#k = :a`,
      ExpressionAttributeNames: {
        '#k': keyAttribute,
        ...(status ? { '#status': 'status' } : {}),
      },
      ExpressionAttributeValues: {
        ':a': value,
        ...(status ? { ':s': status } : {}),
      },
    });
  }

  /**
   * Everything assigned to one person, across every document.
   *
   * TWO index queries, not one, and not a scan:
   *
   *   * `assigneeId-status-index` — the real answer. Every row written since
   *     the label/identity split carries `assigneeId`.
   *   * `assignee-status-index` — the legacy index, asked for the SAME id.
   *     It is not asked for a name. Voice work items and the summary projector
   *     have always written a real userId into `assignee`, so those rows do
   *     answer this query today, and dropping the second read would have taken
   *     a working lane away while fixing a broken one.
   *
   * What is deliberately NOT recovered: rows whose `assignee` holds a display
   * name and nothing else. Those are unfindable and stay unfindable. There is
   * no name→identity directory on this side — the directory is a frontend
   * constant — so a backfill would be guesswork, and they have never been
   * returned by this query in the first place, so nothing regresses. Any such
   * row heals the next time somebody sets its assignee from the app, which now
   * writes both fields.
   */
  async getItemsAssignedTo(userId: string, status?: string): Promise<SectionItemFields[]> {
    if (!userId) return [];

    const [byId, byLegacyKey] = await Promise.all([
      this.queryAssigneeIndex(ASSIGNEE_ID_INDEX, 'assigneeId', userId, status),
      this.queryAssigneeIndex(LEGACY_ASSIGNEE_INDEX, 'assignee', userId, status),
    ]);

    // A row can legitimately appear in both (assignee and assigneeId are the
    // same id on a self-assigned work item). Identity is the full primary key.
    const seen = new Set<string>();
    const merged: SectionItemFields[] = [];
    for (const row of [...byId, ...byLegacyKey]) {
      const key = `${row.sectionKey} ${row.itemId}`;
      if (seen.has(key)) continue;
      seen.add(key);
      merged.push(row);
    }
    return merged;
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
