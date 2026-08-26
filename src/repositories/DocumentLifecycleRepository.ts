/**
 * Server-side document lifecycle state.
 *
 * One row per document. Before this table the only lifecycle signal lived in
 * CRDT metadata the frontend edited directly, which is why "who may unlock an
 * approved document" had no answer. Ownership and status now live here, where
 * the server can enforce them.
 *
 * Storage shape:
 *   pk: documentId (HASH)
 *   GSI status-updatedAt-index: status (HASH) + updatedAt (RANGE)
 *     → "every document currently in_review, oldest first" for a review queue.
 *
 * The stored `status` may be the legacy value 'final'. Reads normalize it to
 * 'approved'; writes rewrite it, so the legacy value drains out over time
 * instead of needing a migration job.
 */

import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { BaseRepository } from './BaseRepository';
import { tableName } from '../lib/ddb-table-name';
import {
  normalizeStatus,
  type DocumentStatus,
  type LifecycleAction,
} from '../lib/document-lifecycle';

/** One entry in the per-document audit trail. */
export interface LifecycleHistoryEntry {
  from: string;
  to: string;
  action: LifecycleAction;
  actor: string;
  at: string;
  reason?: string;
}

export interface DocumentLifecycleItem {
  documentId: string;
  /** Normalized on read — never 'final' when it comes out of this repository. */
  status: DocumentStatus;
  ownerId: string;
  createdAt: string;
  updatedAt: string;
  updatedBy: string;
  /** Monotonic counter; bumped on every applied transition. */
  version: number;
  lastAction?: LifecycleAction;
  lastReason?: string;
  /** Most recent transitions, newest last, capped at HISTORY_LIMIT. */
  history: LifecycleHistoryEntry[];
  /** Present only when the row still carried the legacy 'final' value. */
  legacyStatus?: string;
}

const TABLE_NAME = tableName('document-lifecycle');

/** Keeps the item comfortably inside the 400KB DDB item ceiling. */
export const HISTORY_LIMIT = 50;

/** Thrown when a compare-and-swap loses to a concurrent transition. */
export class LifecycleConcurrencyError extends Error {
  constructor(message = 'Document lifecycle changed concurrently') {
    super(message);
    this.name = 'LifecycleConcurrencyError';
  }
}

function isConditionalCheckFailure(err: unknown): boolean {
  const name = (err as { name?: string } | undefined)?.name;
  return name === 'ConditionalCheckFailedException';
}

export class DocumentLifecycleRepository extends BaseRepository {
  constructor(docClient: DynamoDBDocumentClient) {
    super(TABLE_NAME, docClient);
  }

  /**
   * Read the lifecycle row, normalizing the legacy 'final' status. Returns
   * null when the document has never transitioned — callers treat that as an
   * implicit 'draft' with no recorded owner.
   */
  async get(documentId: string): Promise<DocumentLifecycleItem | null> {
    const raw = await this.getItem<Record<string, unknown>>({ documentId });
    if (!raw) return null;
    return hydrate(raw);
  }

  /**
   * Create the row for a document that has never had one, claiming ownership.
   *
   * Returns the created item, or null when a row already existed (the
   * conditional put lost the race). Callers re-read on null.
   */
  async initialize(
    documentId: string,
    ownerId: string,
    status: DocumentStatus = 'draft',
  ): Promise<DocumentLifecycleItem | null> {
    const now = new Date().toISOString();
    const item: DocumentLifecycleItem = {
      documentId,
      status,
      ownerId,
      createdAt: now,
      updatedAt: now,
      updatedBy: ownerId,
      version: 0,
      history: [],
    };
    try {
      await this.putItemConditional(
        item as unknown as Record<string, unknown>,
        'attribute_not_exists(#documentId)',
        { '#documentId': 'documentId' },
      );
      return item;
    } catch (err) {
      if (isConditionalCheckFailure(err)) return null;
      throw err;
    }
  }

  /**
   * Apply a decided transition with a compare-and-swap on the current status,
   * so two concurrent approvals cannot both land.
   *
   * `expectedFrom` is the NORMALIZED status the caller decided against. The
   * condition also accepts the legacy value when that normalizes to the same
   * state, which is how 'final' rows get migrated to 'approved' on write.
   */
  async applyTransition(params: {
    documentId: string;
    expectedFrom: DocumentStatus;
    /** Legacy alias also accepted by the CAS, e.g. 'final' for 'approved'. */
    legacyFrom?: string;
    to: DocumentStatus;
    action: LifecycleAction;
    actor: string;
    at: string;
    reason?: string;
    history: LifecycleHistoryEntry[];
  }): Promise<DocumentLifecycleItem> {
    const entry: LifecycleHistoryEntry = {
      from: params.expectedFrom,
      to: params.to,
      action: params.action,
      actor: params.actor,
      at: params.at,
      ...(params.reason !== undefined ? { reason: params.reason } : {}),
    };
    const history = [...params.history, entry].slice(-HISTORY_LIMIT);

    const names: Record<string, string> = {
      '#documentId': 'documentId',
      '#status': 'status',
      '#version': 'version',
    };
    const values: Record<string, unknown> = {
      ':from': params.expectedFrom,
      ':to': params.to,
      ':updatedAt': params.at,
      ':updatedBy': params.actor,
      ':action': params.action,
      ':history': history,
      ':one': 1,
      ':zero': 0,
    };

    // The CAS accepts either the normalized status or its legacy alias.
    let condition = 'attribute_exists(#documentId) AND #status = :from';
    if (params.legacyFrom) {
      values[':legacyFrom'] = params.legacyFrom;
      condition =
        'attribute_exists(#documentId) AND (#status = :from OR #status = :legacyFrom)';
    }

    const sets = [
      '#status = :to',
      'updatedAt = :updatedAt',
      'updatedBy = :updatedBy',
      'lastAction = :action',
      'history = :history',
      '#version = if_not_exists(#version, :zero) + :one',
    ];
    if (params.reason !== undefined) {
      names['#lastReason'] = 'lastReason';
      values[':reason'] = params.reason;
      sets.push('#lastReason = :reason');
    }

    try {
      const attrs = await this.updateItem({
        Key: { documentId: params.documentId },
        UpdateExpression: `SET ${sets.join(', ')}`,
        ConditionExpression: condition,
        ExpressionAttributeNames: names,
        ExpressionAttributeValues: values,
        ReturnValues: 'ALL_NEW',
      });
      if (!attrs) {
        throw new LifecycleConcurrencyError();
      }
      return hydrate(attrs);
    } catch (err) {
      if (isConditionalCheckFailure(err)) {
        throw new LifecycleConcurrencyError(
          `Document ${params.documentId} is no longer in state '${params.expectedFrom}'`,
        );
      }
      throw err;
    }
  }

  /** Documents currently in a given lifecycle state, oldest update first. */
  async listByStatus(
    status: DocumentStatus,
    limit = 50,
  ): Promise<DocumentLifecycleItem[]> {
    const rows = await this.query<Record<string, unknown>>({
      IndexName: 'status-updatedAt-index',
      KeyConditionExpression: '#status = :s',
      ExpressionAttributeNames: { '#status': 'status' },
      ExpressionAttributeValues: { ':s': status },
      Limit: Math.min(limit, 200),
    });
    return rows.map(hydrate);
  }
}

function hydrate(raw: Record<string, unknown>): DocumentLifecycleItem {
  const stored = raw['status'] as string | undefined;
  const normalized = normalizeStatus(stored);
  const item: DocumentLifecycleItem = {
    documentId: String(raw['documentId'] ?? ''),
    status: normalized,
    ownerId: String(raw['ownerId'] ?? ''),
    createdAt: String(raw['createdAt'] ?? ''),
    updatedAt: String(raw['updatedAt'] ?? ''),
    updatedBy: String(raw['updatedBy'] ?? ''),
    version: typeof raw['version'] === 'number' ? (raw['version'] as number) : 0,
    history: Array.isArray(raw['history'])
      ? (raw['history'] as LifecycleHistoryEntry[])
      : [],
  };
  if (raw['lastAction'] !== undefined) {
    item.lastAction = raw['lastAction'] as LifecycleAction;
  }
  if (raw['lastReason'] !== undefined) {
    item.lastReason = String(raw['lastReason']);
  }
  if (stored !== undefined && stored !== normalized) {
    item.legacyStatus = stored;
  }
  return item;
}
