/**
 * Named review requests with due dates.
 *
 * A review REQUEST ("Alice, please review section 3 by Friday") is distinct
 * from a review RESPONSE (`SectionReviewRepository` — "Alice said approved").
 * The request is what makes someone a designated reviewer, what shows up in
 * `GET /api/reviews/mine`, and what gates document approval.
 *
 * Key design deliberately mirrors SectionReviewRepository:
 *   pk: documentId (HASH)
 *   sk: requestKey (RANGE)  — `${scopeId}:${reviewerId}`
 *   GSI reviewerId-createdAt-index: reviewerId (HASH) + createdAt (RANGE)
 *
 * `scopeId` is the sectionId for a section-scoped request, or DOC_SCOPE for a
 * whole-document request. Making the key deterministic (rather than a ulid)
 * means "ask Alice to review section 3" is idempotent — re-requesting updates
 * the existing row instead of piling up duplicates — exactly how
 * SectionReviewRepository keys `${sectionId}:${userId}`.
 *
 * TTL habit: only WITHDRAWN requests get a `ttl`. A withdrawn request is
 * noise with a short audit half-life, so it self-cleans after
 * WITHDRAWN_RETENTION_DAYS. Pending/approved/changes_requested rows are the
 * audit trail and are never expired.
 */

import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { ulid } from 'ulid';
import { BaseRepository } from './BaseRepository';
import { tableName } from '../lib/ddb-table-name';

export type ReviewRequestState =
  | 'pending'
  | 'approved'
  | 'changes_requested'
  | 'withdrawn';

export const REVIEW_REQUEST_STATES: readonly ReviewRequestState[] = [
  'pending',
  'approved',
  'changes_requested',
  'withdrawn',
] as const;

/** States a reviewer may move a request into by responding to it. */
export const REVIEWER_RESPONSE_STATES: readonly ReviewRequestState[] = [
  'approved',
  'changes_requested',
] as const;

/** Scope marker used in the sort key for whole-document requests. */
export const DOC_SCOPE = '_doc';

export const WITHDRAWN_RETENTION_DAYS = 30;

export interface DocumentReviewRequest {
  documentId: string;
  requestKey: string;
  requestId: string;
  scope: 'document' | 'section';
  sectionId?: string;
  reviewerId: string;
  reviewerName?: string;
  requestedBy: string;
  requestedByName?: string;
  state: ReviewRequestState;
  /** Required requests gate approval. Optional ones are advisory. */
  required: boolean;
  dueDate?: string;
  note?: string;
  /** The reviewer's comment when they responded. */
  comment?: string;
  createdAt: string;
  updatedAt: string;
  respondedAt?: string;
  /** Epoch seconds; set only on withdrawn requests. */
  ttl?: number;
}

export interface CreateReviewRequestInput {
  documentId: string;
  reviewerId: string;
  reviewerName?: string;
  requestedBy: string;
  requestedByName?: string;
  sectionId?: string;
  dueDate?: string;
  note?: string;
  required?: boolean;
}

const TABLE_NAME = tableName('document-review-requests');

/** Builds the deterministic sort key for a (scope, reviewer) pair. */
export function buildRequestKey(
  reviewerId: string,
  sectionId?: string,
): string {
  return `${sectionId ?? DOC_SCOPE}:${reviewerId}`;
}

/**
 * A request is OUTSTANDING when it is required and has not been satisfied.
 * Satisfied means the reviewer approved, or the requester withdrew the ask.
 * A `changes_requested` request is answered but NOT satisfied — it must block
 * approval, otherwise "reviewer asked for changes" would be a no-op.
 */
export function isReviewOutstanding(req: DocumentReviewRequest): boolean {
  if (!req.required) return false;
  return req.state === 'pending' || req.state === 'changes_requested';
}

/** True when a pending request's due date is in the past. */
export function isOverdue(
  req: DocumentReviewRequest,
  now: Date = new Date(),
): boolean {
  if (req.state !== 'pending' || !req.dueDate) return false;
  const due = Date.parse(req.dueDate);
  return Number.isFinite(due) && due < now.getTime();
}

export class DocumentReviewRequestRepository extends BaseRepository {
  constructor(docClient: DynamoDBDocumentClient) {
    super(TABLE_NAME, docClient);
  }

  /**
   * Create (or re-issue) a review request. Idempotent on
   * (documentId, sectionId, reviewerId): re-requesting resets the row to
   * pending with a fresh due date, which is what "nudge Alice again" means.
   */
  async createRequest(
    input: CreateReviewRequestInput,
  ): Promise<DocumentReviewRequest> {
    const now = new Date().toISOString();
    const requestKey = buildRequestKey(input.reviewerId, input.sectionId);
    const existing = await this.getRequest(input.documentId, requestKey);

    const item: DocumentReviewRequest = {
      documentId: input.documentId,
      requestKey,
      requestId: existing?.requestId ?? ulid(),
      scope: input.sectionId ? 'section' : 'document',
      ...(input.sectionId !== undefined ? { sectionId: input.sectionId } : {}),
      reviewerId: input.reviewerId,
      ...(input.reviewerName !== undefined
        ? { reviewerName: input.reviewerName }
        : {}),
      requestedBy: input.requestedBy,
      ...(input.requestedByName !== undefined
        ? { requestedByName: input.requestedByName }
        : {}),
      state: 'pending',
      required: input.required ?? true,
      ...(input.dueDate !== undefined ? { dueDate: input.dueDate } : {}),
      ...(input.note !== undefined ? { note: input.note } : {}),
      createdAt: existing?.createdAt ?? now,
      updatedAt: now,
    };

    await this.putItem(item as unknown as Record<string, unknown>);
    return item;
  }

  async getRequest(
    documentId: string,
    requestKey: string,
  ): Promise<DocumentReviewRequest | null> {
    return this.getItem<DocumentReviewRequest>({ documentId, requestKey });
  }

  async getRequestsForDocument(
    documentId: string,
  ): Promise<DocumentReviewRequest[]> {
    return this.query<DocumentReviewRequest>({
      KeyConditionExpression: 'documentId = :docId',
      ExpressionAttributeValues: { ':docId': documentId },
    });
  }

  async getRequestsForSection(
    documentId: string,
    sectionId: string,
  ): Promise<DocumentReviewRequest[]> {
    return this.query<DocumentReviewRequest>({
      KeyConditionExpression:
        'documentId = :docId AND begins_with(requestKey, :prefix)',
      ExpressionAttributeValues: {
        ':docId': documentId,
        ':prefix': `${sectionId}:`,
      },
    });
  }

  /**
   * Every request addressed to a user, newest first. The GSI range key is
   * createdAt rather than dueDate on purpose: dueDate is optional, and DDB
   * silently omits items missing a GSI key attribute — indexing on dueDate
   * would make requests without a due date invisible to `/reviews/mine`.
   */
  async getRequestsForReviewer(
    reviewerId: string,
    state?: string,
  ): Promise<DocumentReviewRequest[]> {
    const rows = await this.query<DocumentReviewRequest>({
      IndexName: 'reviewerId-createdAt-index',
      KeyConditionExpression: 'reviewerId = :rid',
      ExpressionAttributeValues: { ':rid': reviewerId },
      ScanIndexForward: false,
    });
    return state ? rows.filter((r) => r.state === state) : rows;
  }

  /**
   * Move a request to a new state. Conditional on the row existing so a
   * response to a deleted request surfaces rather than resurrecting it.
   */
  async setState(params: {
    documentId: string;
    requestKey: string;
    state: ReviewRequestState;
    actor: string;
    comment?: string;
    at?: string;
  }): Promise<DocumentReviewRequest | null> {
    const at = params.at ?? new Date().toISOString();
    const names: Record<string, string> = {
      '#documentId': 'documentId',
      '#state': 'state',
    };
    const values: Record<string, unknown> = {
      ':state': params.state,
      ':updatedAt': at,
      ':respondedAt': at,
      ':actor': params.actor,
    };
    const sets = [
      '#state = :state',
      'updatedAt = :updatedAt',
      'respondedAt = :respondedAt',
      'respondedBy = :actor',
    ];
    if (params.comment !== undefined) {
      values[':comment'] = params.comment;
      sets.push('#comment = :comment');
      names['#comment'] = 'comment';
    }
    if (params.state === 'withdrawn') {
      values[':ttl'] = Math.floor(
        Date.parse(at) / 1000 + WITHDRAWN_RETENTION_DAYS * 86400,
      );
      names['#ttl'] = 'ttl';
      sets.push('#ttl = :ttl');
    }

    try {
      const attrs = await this.updateItem({
        Key: { documentId: params.documentId, requestKey: params.requestKey },
        UpdateExpression: `SET ${sets.join(', ')}`,
        ConditionExpression: 'attribute_exists(#documentId)',
        ExpressionAttributeNames: names,
        ExpressionAttributeValues: values,
        ReturnValues: 'ALL_NEW',
      });
      return (attrs as DocumentReviewRequest | undefined) ?? null;
    } catch (err) {
      if ((err as { name?: string }).name === 'ConditionalCheckFailedException') {
        return null;
      }
      throw err;
    }
  }

  /** Change the due date without disturbing the request's state. */
  async setDueDate(
    documentId: string,
    requestKey: string,
    dueDate: string | null,
    at: string = new Date().toISOString(),
  ): Promise<DocumentReviewRequest | null> {
    const expr =
      dueDate === null
        ? { UpdateExpression: 'REMOVE dueDate SET updatedAt = :updatedAt' }
        : {
            UpdateExpression: 'SET dueDate = :dueDate, updatedAt = :updatedAt',
          };
    const values: Record<string, unknown> = { ':updatedAt': at };
    if (dueDate !== null) values[':dueDate'] = dueDate;

    try {
      const attrs = await this.updateItem({
        Key: { documentId, requestKey },
        ...expr,
        ConditionExpression: 'attribute_exists(#documentId)',
        ExpressionAttributeNames: { '#documentId': 'documentId' },
        ExpressionAttributeValues: values,
        ReturnValues: 'ALL_NEW',
      });
      return (attrs as DocumentReviewRequest | undefined) ?? null;
    } catch (err) {
      if ((err as { name?: string }).name === 'ConditionalCheckFailedException') {
        return null;
      }
      throw err;
    }
  }

  async deleteRequest(documentId: string, requestKey: string): Promise<void> {
    await this.deleteItem({ documentId, requestKey });
  }
}
