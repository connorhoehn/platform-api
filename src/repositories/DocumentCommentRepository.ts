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
  }): Promise<DocumentComment> {
    const commentId = ulid();
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

  async deleteComment(documentId: string, commentId: string): Promise<void> {
    await this.deleteItem({ documentId, commentId });
  }
}
