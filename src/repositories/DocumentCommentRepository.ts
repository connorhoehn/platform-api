import { ulid } from 'ulid';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { BaseRepository } from './BaseRepository';
import { tableName } from '../lib/ddb-table-name';

/**
 * WHERE a comment points, in a canvas document.
 *
 * A Y.js relative position, encoded. The two obvious alternatives are both
 * wrong: a character offset goes stale the moment anyone types above the
 * comment and fails SILENTLY (the comment does not disappear — it starts
 * pointing at someone else's sentence), and a pixel offset goes stale when a
 * font loads. A relative position names the position by the identity of the
 * character it sits beside, so ten paragraphs inserted above it do not move it.
 *
 * Opaque to this service. The client encodes it and the client resolves it;
 * nothing on the server ever decodes these strings, because doing so would put
 * a Y.Doc in an API process to answer a question no API caller asks. What the
 * server owes is durability and shape — hence the size caps in the route.
 *
 * Mirrors `CanvasAnchor` in realtime-modules structurally rather than importing
 * it: that package is a browser/CRDT library and this is an Express service.
 * The field names are deliberately short — they are repeated on every comment
 * row in DynamoDB, where attribute names are billed alongside their values.
 */
export interface DocumentCommentAnchor {
  /** Encoding version. Written by the client; the server never interprets it. */
  v: number;
  /** The Y.js root the anchor is relative to (`'body'` for a canvas page). */
  key: string;
  /** base64 of the encoded relative position for the start of the range. */
  start: string;
  /** base64 of the encoded relative position for the end of the range. */
  end: string;
  /**
   * The text the range covered when the comment was written.
   *
   * Stored so an ORPHANED thread — one whose anchored text has since been
   * deleted — can still say what it was about. Never used to re-find the text:
   * a fuzzy re-match is exactly the confident wrong answer this design avoids.
   */
  quote: string;
}

export interface DocumentComment {
  documentId: string;
  commentId: string;
  /**
   * The legacy position: which SECTION the comment was left on.
   *
   * Optional since canvas documents, which have no sections. A comment carries
   * a `sectionId`, an `anchor`, or a `parentCommentId` (a reply is positioned
   * by the thread it belongs to) — see the POST route. Absent here means
   * absent from the item, so the `sectionId-timestamp-index` GSI is sparse and
   * anchored comments simply do not appear in a by-section query, which is the
   * honest answer to "which section is this in?" for a document with none.
   */
  scope?: 'document';
  sectionId?: string;
  text: string;
  userId: string;
  displayName: string;
  color: string;
  timestamp: string;
  parentCommentId?: string;
  /**
   * Where in the prose this comment points. Absent on every comment written
   * before anchors existed, and on replies, which inherit their root's
   * position. An absent anchor is a real state, not a missing migration: those
   * comments genuinely never referred to a span of text, and inventing one for
   * them would be fabricating evidence.
   */
  anchor?: DocumentCommentAnchor;
  resolved?: boolean;
  resolvedBy?: string;
  resolvedAt?: string;
  /**
   * WS-J lane B: the agent run this ROOT comment was handed to ("Ask agent").
   * `pending` until the run settles; the settle is a compare-and-set on
   * (`runId`, `pending`), so two replicas observing the same completion write
   * one reply between them.
   */
  agentRequest?: DocumentCommentAgentRequest;
}

/**
 * `pending` → the run is going. A run that wrote SUGGESTIONS settles to
 * `suggested` (the change is on the page, awaiting a person's verdict) and then
 * `accepted` / `declined` from the suggestion review; a direct edit settles to
 * `addressed`. `refused` / `failed` leave the thread open.
 */
export type CommentAgentRequestState = 'pending' | 'suggested' | 'accepted' | 'declined' | 'addressed' | 'refused' | 'failed';

export interface DocumentCommentAgentRequest {
  runId: string;
  pipelineId: string;
  state: CommentAgentRequestState;
  /** Resolve the thread when the run lands its edit ("Resolve when applied"). */
  resolveOnApply: boolean;
  requestedBy: string;
  requestedByName: string;
  at: string;
  settledAt?: string;
  /** The reply the settle wrote — pre-allocated so the claim names it. */
  replyId?: string;
  /** `suggested`: the `${user}@${bucket}` key the run's marks carry — what an Accept / Reject in the margin names. */
  suggestionKey?: string;
  /** `accepted` / `declined`: who gave the verdict, and when. */
  reviewedBy?: string;
  reviewedAt?: string;
  reviewReplyId?: string;
}

const TABLE_NAME = tableName('document-comments');

export class DocumentCommentRepository extends BaseRepository {
  constructor(docClient: DynamoDBDocumentClient) {
    super(TABLE_NAME, docClient);
  }

  async createComment(comment: {
    documentId: string;
    /** Omit for a canvas comment — it has an `anchor` instead. */
    scope?: 'document';
    sectionId?: string;
    text: string;
    userId: string;
    displayName: string;
    color: string;
    timestamp: string;
    parentCommentId?: string;
    anchor?: DocumentCommentAnchor;
    /** Pre-allocated id (an agent's settle reply); default a fresh ulid. */
    commentId?: string;
  }): Promise<DocumentComment> {
    const commentId = comment.commentId ?? ulid();
    // Spread, not assignment, so an absent optional stays ABSENT from the item.
    // Writing `sectionId: undefined` into a DynamoDB item is not the same as
    // omitting it — it either throws or persists a null, and a null sectionId
    // in the GSI is a different thing from no sectionId at all.
    const item: DocumentComment = {
      ...comment,
      commentId,
    };
    await this.putItem(item as unknown as Record<string, unknown>);
    return item;
  }

  async getCommentsForDocument(
    documentId: string,
    limit?: number,
    lastKey?: Record<string, unknown>,
  ): Promise<{ items: DocumentComment[]; lastEvaluatedKey?: Record<string, unknown> }> {
    return this.queryWithPagination<DocumentComment>({
      KeyConditionExpression: 'documentId = :docId',
      ExpressionAttributeValues: { ':docId': documentId },
      ScanIndexForward: true,
      ...(limit ? { Limit: limit } : {}),
      ...(lastKey ? { ExclusiveStartKey: lastKey } : {}),
    });
  }

  async getCommentsForSection(
    documentId: string,
    sectionId: string,
    limit?: number,
    lastKey?: Record<string, unknown>,
  ): Promise<{ items: DocumentComment[]; lastEvaluatedKey?: Record<string, unknown> }> {
    return this.queryWithPagination<DocumentComment>({
      KeyConditionExpression: 'documentId = :docId',
      FilterExpression: 'sectionId = :sid',
      ExpressionAttributeValues: { ':docId': documentId, ':sid': sectionId },
      ScanIndexForward: true,
      ...(limit ? { Limit: limit } : {}),
      ...(lastKey ? { ExclusiveStartKey: lastKey } : {}),
    });
  }

  async getComment(documentId: string, commentId: string): Promise<DocumentComment | null> {
    return this.getItem<DocumentComment>({ documentId, commentId });
  }

  async editComment(documentId: string, commentId: string, text: string): Promise<DocumentComment> {
    const result = await this.updateItem({
      Key: { documentId, commentId },
      ConditionExpression: 'attribute_exists(commentId)',
      UpdateExpression: 'SET #text = :text, editedAt = :at',
      ExpressionAttributeNames: { '#text': 'text' },
      ExpressionAttributeValues: { ':text': text, ':at': new Date().toISOString() },
      ReturnValues: 'ALL_NEW',
    });
    return result as unknown as DocumentComment;
  }

  /**
   * Re-points a ROOT comment at a span of text (WS-J). Drops `scope`, because a
   * comment is on the whole document or on a span, never both. The caller has
   * validated the anchor and checked who may move it.
   */
  async setAnchor(documentId: string, commentId: string, anchor: DocumentCommentAnchor): Promise<DocumentComment> {
    const result = await this.updateItem({
      Key: { documentId, commentId },
      ConditionExpression: 'attribute_exists(commentId)',
      UpdateExpression: 'SET anchor = :anchor, anchoredAt = :at REMOVE #scope',
      ExpressionAttributeNames: { '#scope': 'scope' },
      ExpressionAttributeValues: { ':anchor': anchor, ':at': new Date().toISOString() },
      ReturnValues: 'ALL_NEW',
    });
    return result as unknown as DocumentComment;
  }

  async resolveThread(
    documentId: string,
    commentId: string,
    resolvedBy: string,
    resolvedAt: string,
  ): Promise<void> {
    await this.updateItem({
      Key: { documentId, commentId },
      ConditionExpression: 'attribute_exists(commentId)',
      UpdateExpression: 'SET resolved = :r, resolvedBy = :by, resolvedAt = :at',
      ExpressionAttributeValues: {
        ':r': true,
        ':by': resolvedBy,
        ':at': resolvedAt,
      },
    });
  }

  async unresolveThread(documentId: string, commentId: string): Promise<void> {
    await this.updateItem({
      Key: { documentId, commentId },
      ConditionExpression: 'attribute_exists(commentId)',
      UpdateExpression: 'REMOVE resolved, resolvedBy, resolvedAt',
    });
  }

  /** Hands a root comment to an agent run (replaces an earlier, settled or not). */
  async setAgentRequest(documentId: string, commentId: string, request: DocumentCommentAgentRequest): Promise<DocumentComment> {
    const result = await this.updateItem({
      Key: { documentId, commentId },
      ConditionExpression: 'attribute_exists(commentId)',
      UpdateExpression: 'SET agentRequest = :req',
      ExpressionAttributeValues: { ':req': request },
      ReturnValues: 'ALL_NEW',
    });
    return result as unknown as DocumentComment;
  }

  /**
   * Settles the agent request for `runId` exactly once: true when THIS caller
   * moved it off `pending`, false when it was already settled (the other
   * replica's copy of the event, a redelivery) or re-asked under another run.
   */
  async settleAgentRequest(
    documentId: string,
    commentId: string,
    runId: string,
    state: Exclude<CommentAgentRequestState, 'pending'>,
    replyId: string,
    settledAt: string,
    suggestionKey?: string,
  ): Promise<boolean> {
    try {
      await this.updateItem({
        Key: { documentId, commentId },
        ConditionExpression: 'agentRequest.runId = :run AND agentRequest.#state = :pending',
        UpdateExpression: `SET agentRequest.#state = :state, agentRequest.settledAt = :at, agentRequest.replyId = :reply${suggestionKey ? ', agentRequest.suggestionKey = :key' : ''}`,
        ExpressionAttributeNames: { '#state': 'state' },
        ExpressionAttributeValues: {
          ':run': runId, ':pending': 'pending', ':state': state, ':at': settledAt, ':reply': replyId,
          ...(suggestionKey ? { ':key': suggestionKey } : {}),
        },
      });
      return true;
    } catch (err) {
      if ((err as { name?: string })?.name === 'ConditionalCheckFailedException') return false;
      throw err;
    }
  }

  /**
   * Records a person's verdict on the run's suggestion exactly once: true when
   * THIS caller moved the request from `suggested` (or still `pending` — a
   * verdict can outrun the run's own terminal event) to `accepted` / `declined`.
   */
  async reviewAgentRequest(
    documentId: string,
    commentId: string,
    runId: string,
    state: 'accepted' | 'declined',
    reviewedBy: string,
    replyId: string,
    reviewedAt: string,
  ): Promise<boolean> {
    try {
      await this.updateItem({
        Key: { documentId, commentId },
        ConditionExpression: 'agentRequest.runId = :run AND agentRequest.#state IN (:pending, :suggested)',
        UpdateExpression: 'SET agentRequest.#state = :state, agentRequest.reviewedBy = :by, agentRequest.reviewedAt = :at, agentRequest.reviewReplyId = :reply',
        ExpressionAttributeNames: { '#state': 'state' },
        ExpressionAttributeValues: {
          ':run': runId, ':pending': 'pending', ':suggested': 'suggested', ':state': state, ':by': reviewedBy, ':at': reviewedAt, ':reply': replyId,
        },
      });
      return true;
    } catch (err) {
      if ((err as { name?: string })?.name === 'ConditionalCheckFailedException') return false;
      throw err;
    }
  }

  async deleteComment(documentId: string, commentId: string): Promise<void> {
    await this.deleteItem({ documentId, commentId });
  }
}
