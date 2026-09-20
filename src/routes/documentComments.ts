import { Router, Request, Response } from 'express';
import { requireAuth } from '../middleware/auth';
import { publishSignal } from '../lib/event-catalog';
import { documentCommentRepo } from '../repositories';
import type { DocumentCommentAnchor } from '../repositories/DocumentCommentRepository';
import {
  asyncHandler,
  ValidationError,
  NotFoundError,
  ForbiddenError,
} from '../middleware/error-handler';
import log from '../lib/logger';

// ---------------------------------------------------------------------------
// Anchors
//
// An anchor is an OPAQUE payload: the client encodes a Y.js relative position
// and the client decodes it. Nothing here interprets `start`/`end`, because
// interpreting them would mean loading the document's CRDT into an API process
// to answer a question no API caller is asking.
//
// What the server does owe is a shape and a size. This item goes into DynamoDB
// beside the comment body and comes back out on every read, so an unbounded
// blob written once is a cost paid on every page load forever — and an
// unvalidated field is a place for a client bug to store something that only
// fails much later, in the reader, where nobody will connect it to the write.
// ---------------------------------------------------------------------------

/**
 * Generous next to a real anchor and still a hard ceiling.
 *
 * An encoded relative position is on the order of 40 bytes, so ~56 base64
 * characters; 2048 leaves several orders of magnitude of headroom for a future
 * encoding while refusing anything that is clearly not a position.
 */
const MAX_ANCHOR_POSITION_CHARS = 2048;

/**
 * The quote is a snippet for an orphaned thread to show, not the document.
 * Truncated rather than refused: a user who commented on four paragraphs did
 * nothing wrong, and losing their comment over the length of its context would
 * be a worse answer than showing them the first 500 characters of it.
 */
const MAX_ANCHOR_QUOTE_CHARS = 500;

/**
 * Validates and narrows an anchor off the wire.
 *
 * Returns `undefined` for an absent anchor — the overwhelmingly common case,
 * since every section comment and every reply has none — and throws for one
 * that is present but malformed, because a client that computed a bad anchor
 * wants to hear about it now rather than store a comment that can never be
 * placed.
 */
function parseAnchor(value: unknown): DocumentCommentAnchor | undefined {
  if (value === undefined || value === null) return undefined;
  if (typeof value !== 'object' || Array.isArray(value)) {
    throw new ValidationError('anchor must be an object');
  }

  const raw = value as Record<string, unknown>;
  const { v, key, start, end, quote } = raw;

  if (typeof v !== 'number' || !Number.isInteger(v) || v < 1) {
    throw new ValidationError('anchor.v must be a positive integer version');
  }
  if (typeof key !== 'string' || !key.trim()) {
    throw new ValidationError('anchor.key is required');
  }
  for (const [name, position] of [['start', start], ['end', end]] as const) {
    if (typeof position !== 'string' || !position) {
      throw new ValidationError(`anchor.${name} is required`);
    }
    if (position.length > MAX_ANCHOR_POSITION_CHARS) {
      throw new ValidationError(`anchor.${name} is too long`);
    }
  }
  if (typeof quote !== 'string') {
    throw new ValidationError('anchor.quote must be a string');
  }

  return {
    v,
    key: key.trim(),
    start: start as string,
    end: end as string,
    quote: quote.slice(0, MAX_ANCHOR_QUOTE_CHARS),
  };
}

// documentCommentsRouter is mounted at /documents — mergeParams:true exposes :documentId
export const documentCommentsRouter = Router({ mergeParams: true });

// POST /api/documents/:documentId/comments — create a comment or threaded reply
documentCommentsRouter.post('/:documentId/comments', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { documentId } = req.params;
  const { sectionId, text, parentCommentId, anchor, scope } = req.body as {
    sectionId?: string;
    text?: string;
    parentCommentId?: string;
    anchor?: unknown;
    scope?: unknown;
  };
  const userId = req.user!.sub;

  const trimmedSectionId =
    typeof sectionId === 'string' && sectionId.trim() ? sectionId.trim() : undefined;
  const parsedAnchor = parseAnchor(anchor);

  if (scope !== undefined && scope !== 'document') {
    throw new ValidationError('scope must be document when supplied');
  }
  if (scope === 'document' && (trimmedSectionId || parsedAnchor || parentCommentId)) {
    throw new ValidationError('document scope cannot also specify a section, anchor or parent');
  }
  if (!trimmedSectionId && !parsedAnchor && !parentCommentId && scope !== 'document') {
    throw new ValidationError('one of sectionId, anchor, parentCommentId or scope:document is required');
  }
  if (parentCommentId !== undefined) {
    if (typeof parentCommentId !== 'string' || !parentCommentId.trim()) {
      throw new ValidationError('parentCommentId must be a non-empty string');
    }
    const parent = await documentCommentRepo.getComment(documentId, parentCommentId);
    if (!parent || parent.documentId !== documentId) throw new NotFoundError('Parent comment not found in this document');
  }

  const trimmedText = typeof text === 'string' ? text.trim() : '';
  if (!trimmedText || trimmedText.length > 10000) {
    throw new ValidationError('text is required (max 10000 chars)');
  }

  const now = new Date().toISOString();
  const comment = await documentCommentRepo.createComment({
    documentId,
    text: trimmedText,
    userId,
    displayName: req.user!.email ?? userId,
    color: '#4A90D9',
    timestamp: now,
    ...(trimmedSectionId ? { sectionId: trimmedSectionId } : {}),
    ...(parentCommentId ? { parentCommentId } : {}),
    ...(parsedAnchor ? { anchor: parsedAnchor } : {}),
    ...(scope === 'document' ? { scope: 'document' as const } : {}),
  });

  publishSignal(`doc-comments:${documentId}`, 'doc:comment_added', {
    documentId,
    comment,
  }).catch((err: unknown) => {
    log.error({ event: 'doc:comment_added', err: err instanceof Error ? err.message : String(err) }, 'signal publish failed');
  });

  res.status(201).json({ comment });
}));

// GET /api/documents/:documentId/comments — list comments, optionally filtered by sectionId
documentCommentsRouter.get('/:documentId/comments', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { documentId } = req.params;
  const sectionId = req.query['sectionId'] as string | undefined;
  const limit = Math.min(parseInt(req.query['limit'] as string ?? '50', 10) || 50, 200);

  let cursor: Record<string, unknown> | undefined;
  if (req.query['cursor'] !== undefined) {
    try {
      const raw = req.query['cursor'];
      if (typeof raw !== 'string' || raw.length > 2048) throw new Error('invalid');
      const parsed = JSON.parse(Buffer.from(raw, 'base64url').toString()) as Record<string, unknown>;
      if (parsed.documentId !== documentId || typeof parsed.commentId !== 'string') throw new Error('invalid');
      cursor = { documentId, commentId: parsed.commentId };
    } catch { throw new ValidationError('Invalid comments cursor'); }
  }
  let result;
  if (sectionId) {
    result = await documentCommentRepo.getCommentsForSection(documentId, sectionId, limit, cursor);
  } else {
    result = await documentCommentRepo.getCommentsForDocument(documentId, limit, cursor);
  }

  res.status(200).json({ comments: result.items, ...(result.lastEvaluatedKey ? { nextCursor: Buffer.from(JSON.stringify(result.lastEvaluatedKey)).toString('base64url') } : {}) });
}));

// PATCH /api/documents/:documentId/comments/:commentId — resolve/unresolve a thread
documentCommentsRouter.patch('/:documentId/comments/:commentId', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { documentId, commentId } = req.params;
  const { resolved, text } = req.body as { resolved?: boolean; text?: unknown };
  const userId = req.user!.sub;

  const existing = await documentCommentRepo.getComment(documentId, commentId);
  if (!existing) throw new NotFoundError('Comment not found');
  if (text !== undefined) {
    if (existing.userId !== userId) throw new ForbiddenError('Only the author may edit comment text');
    if (typeof text !== 'string' || !text.trim() || text.trim().length > 10000) {
      throw new ValidationError('text is required (max 10000 chars)');
    }
    if (resolved !== undefined) throw new ValidationError('Edit text and resolve in separate requests');
    const comment = await documentCommentRepo.editComment(documentId, commentId, text.trim());
    publishSignal(`doc-comments:${documentId}`, 'doc:comment_updated', { documentId, comment })
      .catch((err: unknown) => log.error({ event: 'doc:comment_updated', err: err instanceof Error ? err.message : String(err) }, 'signal publish failed'));
    res.status(200).json({ comment });
    return;
  }
  if (typeof resolved !== 'boolean') {
    throw new ValidationError('resolved (boolean) or text is required');
  }

  const now = new Date().toISOString();
  if (resolved) {
    await documentCommentRepo.resolveThread(documentId, commentId, userId, now);
  } else {
    await documentCommentRepo.unresolveThread(documentId, commentId);
  }

  publishSignal(`doc-comments:${documentId}`, 'doc:comment_resolved', {
    documentId,
    commentId,
    resolved,
    ...(resolved ? { resolvedBy: userId, resolvedAt: now } : {}),
  }).catch((err: unknown) => {
    log.error({ event: 'doc:comment_resolved', err: err instanceof Error ? err.message : String(err) }, 'signal publish failed');
  });

  res.status(200).json({
    comment: {
      documentId,
      commentId,
      resolved,
      ...(resolved ? { resolvedBy: userId, resolvedAt: now } : {}),
    },
  });
}));

// DELETE /api/documents/:documentId/comments/:commentId — delete a comment
documentCommentsRouter.delete('/:documentId/comments/:commentId', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { documentId, commentId } = req.params;

  const existing = await documentCommentRepo.getComment(documentId, commentId);
  if (!existing) throw new NotFoundError('Comment not found');
  if (existing.userId !== req.user!.sub) throw new ForbiddenError('Only the author may delete a comment');
  await documentCommentRepo.deleteComment(documentId, commentId);

  publishSignal(`doc-comments:${documentId}`, 'doc:comment_deleted', {
    documentId,
    commentId,
  }).catch((err: unknown) => {
    log.error({ event: 'doc:comment_deleted', err: err instanceof Error ? err.message : String(err) }, 'signal publish failed');
  });

  res.status(204).send();
}));
